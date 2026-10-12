use crate::torrent::Info;
use anyhow::{Context, Result, bail};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

/// One file of the torrent, placed in the concatenated byte stream.
#[derive(Debug)]
struct FileSpan {
    path: PathBuf,
    /// Offset of the file's first byte in the torrent's byte stream.
    offset: u64,
    length: u64,
}

/// Maps the torrent's byte stream onto files on disk.
#[derive(Debug)]
pub struct Storage {
    files: Vec<FileSpan>,
    piece_length: u64,
    total_length: u64,
}

impl Storage {
    /// Creates (or opens) every file with its final size. Single-file
    /// torrents go in `out_dir/name`, multi-file ones under `out_dir/name/`.
    pub fn new(info: &Info, out_dir: &Path) -> Result<Self> {
        check_component(&info.name)?;
        let root = out_dir.join(&info.name);

        let mut files = Vec::new();
        let mut offset = 0u64;
        match (&info.length, &info.files) {
            (Some(length), None) => files.push(FileSpan {
                path: root,
                offset: 0,
                length: *length,
            }),
            (None, Some(entries)) => {
                for entry in entries {
                    if entry.path.is_empty() {
                        bail!("torrent contains a file with an empty path");
                    }
                    let mut path = root.clone();
                    for component in &entry.path {
                        check_component(component)?;
                        path.push(component);
                    }
                    files.push(FileSpan {
                        path,
                        offset,
                        length: entry.length,
                    });
                    offset += entry.length;
                }
            }
            _ => bail!("torrent must have exactly one of `length` or `files`"),
        }

        for file in &files {
            if let Some(parent) = file.path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            // Keep existing contents so a future resume can reuse them.
            let handle = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&file.path)
                .with_context(|| format!("failed to create {}", file.path.display()))?;
            handle.set_len(file.length)?;
        }

        let total_length = files.iter().map(|f| f.length).sum();
        Ok(Self {
            files,
            piece_length: info.piece_length,
            total_length,
        })
    }

    /// Writes a verified piece to disk on the blocking thread pool.
    pub async fn write_piece(self: &Arc<Self>, index: usize, data: Vec<u8>) -> Result<()> {
        let storage = Arc::clone(self);
        let offset = index as u64 * self.piece_length;
        tokio::task::spawn_blocking(move || storage.write_at(offset, &data)).await?
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.check_range(offset, data.len())?;
        for (file, file_offset, range) in self.segments(offset, data.len()) {
            let mut handle = OpenOptions::new().write(true).open(&file.path)?;
            handle.seek(SeekFrom::Start(file_offset))?;
            handle
                .write_all(&data[range])
                .with_context(|| format!("failed to write {}", file.path.display()))?;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_range(offset, len)?;
        let mut data = vec![0u8; len];
        for (file, file_offset, range) in self.segments(offset, len) {
            let mut handle = File::open(&file.path)?;
            handle.seek(SeekFrom::Start(file_offset))?;
            handle.read_exact(&mut data[range])?;
        }
        Ok(data)
    }

    fn check_range(&self, offset: u64, len: usize) -> Result<()> {
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > self.total_length)
        {
            bail!("range {}+{} is outside the torrent data", offset, len);
        }
        Ok(())
    }

    /// Splits `offset..offset + len` into per-file pieces: the file, the
    /// position within that file, and the matching range of the buffer.
    fn segments(
        &self,
        offset: u64,
        len: usize,
    ) -> impl Iterator<Item = (&FileSpan, u64, std::ops::Range<usize>)> {
        let end = offset + len as u64;
        self.files.iter().filter_map(move |file| {
            let start = offset.max(file.offset);
            let stop = end.min(file.offset + file.length);
            (start < stop).then(|| {
                let buf_start = (start - offset) as usize;
                let buf_end = (stop - offset) as usize;
                (file, start - file.offset, buf_start..buf_end)
            })
        })
    }
}

/// Rejects path components that could escape the output directory.
fn check_component(component: &str) -> Result<()> {
    let mut parts = Path::new(component).components();
    match (parts.next(), parts.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => bail!("unsafe path component {:?} in torrent", component),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::FileInfo;
    use serde_bytes::ByteBuf;

    fn multi_file_info(files: &[(&[&str], u64)]) -> Info {
        Info {
            name: "root".into(),
            piece_length: 4,
            pieces: ByteBuf::new(),
            length: None,
            files: Some(
                files
                    .iter()
                    .map(|(path, length)| FileInfo {
                        path: path.iter().map(|s| s.to_string()).collect(),
                        length: *length,
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn piece_spanning_files() {
        let dir = tempfile::tempdir().unwrap();
        let info = multi_file_info(&[(&["a"], 3), (&["sub", "empty"], 0), (&["sub", "b"], 6)]);
        let storage = Storage::new(&info, dir.path()).unwrap();

        storage.write_at(0, b"abcd").unwrap();
        storage.write_at(4, b"efgh").unwrap();
        storage.write_at(8, b"i").unwrap();

        let root = dir.path().join("root");
        assert_eq!(std::fs::read(root.join("a")).unwrap(), b"abc");
        assert_eq!(std::fs::read(root.join("sub/empty")).unwrap(), b"");
        assert_eq!(std::fs::read(root.join("sub/b")).unwrap(), b"defghi");
        assert_eq!(storage.read_at(2, 5).unwrap(), b"cdefg");
        assert!(storage.write_at(8, b"jk").is_err());
        assert!(storage.read_at(u64::MAX, 1).is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        for bad in [&["..", "x"][..], &["/etc"], &["a/b"], &[""], &["."], &[]] {
            let info = multi_file_info(&[(bad, 1)]);
            assert!(
                Storage::new(&info, dir.path()).is_err(),
                "accepted {:?}",
                bad
            );
        }
        let mut info = multi_file_info(&[(&["ok"], 1)]);
        info.name = "..".into();
        assert!(Storage::new(&info, dir.path()).is_err());
    }
}
