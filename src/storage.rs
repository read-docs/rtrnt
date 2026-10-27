use crate::torrent::Info;
use anyhow::{Context, Result, bail};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

/// Most files kept open at once. A torrent can have thousands of files, and
/// macOS's default soft limit is 256 descriptors shared with peer sockets.
const MAX_OPEN_FILES: usize = 64;

/// One file of the torrent, placed in the concatenated byte stream.
#[derive(Debug)]
pub struct FileSpan {
    pub path: PathBuf,
    /// Offset of the file's first byte in the torrent's byte stream.
    pub offset: u64,
    pub length: u64,
    /// The file's size when the storage was opened, before any resizing, or
    /// `None` if it did not exist.
    pub found: Option<u64>,
}

impl FileSpan {
    /// Bytes at the start of the file that were there before we opened it.
    fn prior_len(&self) -> u64 {
        self.found.unwrap_or(0)
    }
}

/// Maps the torrent's byte stream onto files on disk.
#[derive(Debug)]
pub struct Storage {
    files: Vec<FileSpan>,
    piece_length: u64,
    total_length: u64,
    writable: bool,
    handles: Mutex<HandleCache>,
}

/// Open handles by index into `Storage::files`, least recently used first.
/// I/O goes through positional reads and writes, so a handle is shared
/// without a cursor and the lock is held only to look it up.
#[derive(Debug, Default)]
struct HandleCache {
    open: Vec<(usize, Arc<File>)>,
}

impl HandleCache {
    fn get(&mut self, index: usize, path: &Path, writable: bool) -> Result<Arc<File>> {
        if let Some(pos) = self.open.iter().position(|(i, _)| *i == index) {
            let entry = self.open.remove(pos);
            let handle = Arc::clone(&entry.1);
            self.open.push(entry);
            return Ok(handle);
        }
        let handle = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(writable)
                .open(path)
                .with_context(|| format!("failed to open {}", path.display()))?,
        );
        if self.open.len() >= MAX_OPEN_FILES {
            // A handle still in use elsewhere closes when that I/O finishes.
            self.open.remove(0);
        }
        self.open.push((index, Arc::clone(&handle)));
        Ok(handle)
    }
}

impl Storage {
    /// Where each file of the torrent will be, as keys for telling whether
    /// two torrents share a file: under `out_dir` made absolute (creating
    /// it if need be), and lowercased where the usual file systems ignore
    /// case. Nothing else is created.
    pub fn paths(info: &Info, out_dir: &Path) -> Result<Vec<PathBuf>> {
        std::fs::create_dir_all(out_dir)
            .with_context(|| format!("failed to create {}", out_dir.display()))?;
        let out_dir = out_dir
            .canonicalize()
            .with_context(|| format!("failed to resolve {}", out_dir.display()))?;
        let files = layout(info, &out_dir)?;
        Ok(files
            .into_iter()
            .map(|file| fold_case(&file.path))
            .collect())
    }

    /// Creates (or opens) every file with its final size. Single-file
    /// torrents go in `out_dir/name`, multi-file ones under `out_dir/name/`.
    pub fn new(info: &Info, out_dir: &Path) -> Result<Self> {
        let mut files = layout(info, out_dir)?;
        for file in &mut files {
            if let Some(parent) = file.path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("failed to create {}", parent.display()))?;
            }
            // Keep existing contents so a resume can reuse them.
            let handle = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&file.path)
                .with_context(|| format!("failed to create {}", file.path.display()))?;
            let found = handle.metadata()?.len();
            file.found = Some(found);
            // Resizing to the same size would still bump the modification
            // time, which the resume file goes by.
            if found != file.length {
                handle.set_len(file.length)?;
            }
        }
        Ok(Self::with_files(info, files, true))
    }

    /// Opens whatever files of the torrent are already on disk, read-only,
    /// without creating or resizing anything. Reading a range that is not on
    /// disk fails; see `is_on_disk`.
    pub fn open_existing(info: &Info, out_dir: &Path) -> Result<Self> {
        let mut files = layout(info, out_dir)?;
        for file in &mut files {
            file.found = match std::fs::metadata(&file.path) {
                Ok(meta) if meta.is_file() => Some(meta.len()),
                Ok(_) => bail!("{} is not a file", file.path.display()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => None,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("failed to read {}", file.path.display()));
                }
            };
        }
        Ok(Self::with_files(info, files, false))
    }

    fn with_files(info: &Info, files: Vec<FileSpan>, writable: bool) -> Self {
        let total_length = files.iter().map(|f| f.length).sum();
        Self {
            files,
            piece_length: info.piece_length,
            total_length,
            writable,
            handles: Mutex::default(),
        }
    }

    /// The torrent's files, in order.
    pub fn files(&self) -> &[FileSpan] {
        &self.files
    }

    /// Writes a verified piece to disk on the blocking thread pool.
    pub async fn write_piece(self: &Arc<Self>, index: usize, data: Vec<u8>) -> Result<()> {
        let storage = Arc::clone(self);
        let offset = index as u64 * self.piece_length;
        tokio::task::spawn_blocking(move || storage.write_at(offset, &data)).await?
    }

    /// Reads `length` bytes from offset `begin` in piece `index` on the
    /// blocking thread pool, to answer a peer's request.
    pub async fn read_block(
        self: &Arc<Self>,
        index: usize,
        begin: u32,
        length: u32,
    ) -> Result<Vec<u8>> {
        let storage = Arc::clone(self);
        let offset = index as u64 * self.piece_length + u64::from(begin);
        tokio::task::spawn_blocking(move || storage.read_at(offset, length as usize)).await?
    }

    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
        if !self.writable {
            bail!("storage was opened read-only");
        }
        self.check_range(offset, data.len())?;
        for (index, file_offset, range) in self.segments(offset, data.len()) {
            let path = &self.files[index].path;
            let handle = self.handle(index)?;
            write_all_at(&handle, &data[range], file_offset)
                .with_context(|| format!("failed to write {}", path.display()))?;
        }
        Ok(())
    }

    pub fn read_at(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        self.check_range(offset, len)?;
        let mut data = vec![0u8; len];
        for (index, file_offset, range) in self.segments(offset, len) {
            let path = &self.files[index].path;
            let handle = self.handle(index)?;
            read_exact_at(&handle, &mut data[range], file_offset)
                .with_context(|| format!("failed to read {}", path.display()))?;
        }
        Ok(data)
    }

    /// Whether `offset..offset + len` overlaps data that was on disk before
    /// the storage was opened. Ranges that do not are all zeros, never a
    /// finished piece from an earlier run.
    pub fn may_hold_prior_data(&self, offset: u64, len: usize) -> bool {
        self.segments(offset, len)
            .any(|(index, file_offset, _)| file_offset < self.files[index].prior_len())
    }

    /// Whether every byte of `offset..offset + len` was on disk before the
    /// storage was opened.
    pub fn is_on_disk(&self, offset: u64, len: usize) -> bool {
        self.segments(offset, len)
            .all(|(index, file_offset, range)| {
                file_offset + range.len() as u64 <= self.files[index].prior_len()
            })
    }

    fn handle(&self, index: usize) -> Result<Arc<File>> {
        self.handles
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(index, &self.files[index].path, self.writable)
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

    /// Splits `offset..offset + len` into per-file pieces: the file's index,
    /// the position within that file, and the matching range of the buffer.
    fn segments(
        &self,
        offset: u64,
        len: usize,
    ) -> impl Iterator<Item = (usize, u64, std::ops::Range<usize>)> {
        let end = offset + len as u64;
        self.files
            .iter()
            .enumerate()
            .filter_map(move |(index, file)| {
                let start = offset.max(file.offset);
                let stop = end.min(file.offset + file.length);
                (start < stop).then(|| {
                    let buf_start = (start - offset) as usize;
                    let buf_end = (stop - offset) as usize;
                    (index, start - file.offset, buf_start..buf_end)
                })
            })
    }
}

#[cfg(unix)]
fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::write_all_at(file, buf, offset)
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

// `seek_write`/`seek_read` also move the cursor, which nothing here relies on.
#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `path` as it is compared with others: lowercased on macOS and Windows,
/// whose default file systems treat names that differ only in case as one
/// file.
fn fold_case(path: &Path) -> PathBuf {
    if cfg!(any(target_os = "macos", windows)) {
        PathBuf::from(path.to_string_lossy().to_lowercase())
    } else {
        path.to_path_buf()
    }
}

/// Where each file of the torrent goes: `out_dir/name` for a single file,
/// under `out_dir/name/` for several. Fails if two files share a path.
/// Nothing is touched on disk.
fn layout(info: &Info, out_dir: &Path) -> Result<Vec<FileSpan>> {
    check_component(&info.name)?;
    let root = out_dir.join(&info.name);

    let mut files = Vec::new();
    let mut offset = 0u64;
    match (&info.length, &info.files) {
        (Some(length), None) => files.push(FileSpan {
            path: root,
            offset: 0,
            length: *length,
            found: None,
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
                    found: None,
                });
                offset += entry.length;
            }
        }
        _ => bail!("torrent must have exactly one of `length` or `files`"),
    }
    let mut seen = HashSet::new();
    for file in &files {
        if !seen.insert(fold_case(&file.path)) {
            bail!("torrent lists {} twice", file.path.display());
        }
    }
    Ok(files)
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
mod tests;
