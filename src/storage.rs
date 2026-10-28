use crate::selection::{FileSelection, Priority, priorities_of};
use crate::torrent::Info;
use anyhow::{Context, Result, bail};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use unicode_normalization::UnicodeNormalization;

pub mod verify;

/// Most files kept open at once. A torrent can have thousands of files, and
/// macOS's default soft limit is 256 descriptors shared with peer sockets.
const MAX_OPEN_FILES: usize = 64;

/// Reading or writing the torrent's files failed, which is the disk's fault
/// and no peer's. The cause is in the error's chain.
#[derive(Debug)]
pub struct StorageError;

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("disk error")
    }
}

impl std::error::Error for StorageError {}

/// One file of the torrent, placed in the concatenated byte stream.
#[derive(Debug)]
pub struct FileSpan {
    pub path: PathBuf,
    /// Offset of the file's first byte in the torrent's byte stream.
    pub offset: u64,
    pub length: u64,
    /// The file's size when the storage was opened, before any resizing, or
    /// `None` if it did not exist. A file that was skipped and not needed
    /// by a wanted piece is not created, so it stays `None`.
    pub found: Option<u64>,
}

impl FileSpan {
    /// Whether the file is on disk, as far as the storage knows: it was
    /// there or was created when the storage was opened.
    pub fn exists(&self) -> bool {
        self.found.is_some()
    }

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
    /// Which files are wanted, as the files were created by.
    selection: FileSelection,
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

    /// Creates (or opens) every file with its final size, except a file
    /// that `selection` skips and no wanted piece touches, which is left
    /// alone (not created, not resized). A skipped file that shares a piece
    /// with a wanted one is created with its full length (sparse where the
    /// file system allows). Single-file torrents go in `out_dir/name`,
    /// multi-file ones under `out_dir/name/`.
    ///
    /// `selection` lists a priority for each file, or none at all, which
    /// means all are `Normal`.
    pub fn new(info: &Info, out_dir: &Path, selection: &FileSelection) -> Result<Self> {
        let mut files = layout(info, out_dir)?;
        if !selection.is_empty() && selection.priorities().len() != files.len() {
            bail!(
                "selection lists {} files, the torrent has {}",
                selection.priorities().len(),
                files.len()
            );
        }
        let touched = touched_by_wanted_pieces(info, &files, selection);
        for (index, file) in files.iter_mut().enumerate() {
            if selection.priority(index) == Priority::Skip && !touched[index] {
                // Whatever is there from an earlier run stays as it is.
                file.found = std::fs::metadata(&file.path)
                    .ok()
                    .filter(|meta| meta.is_file())
                    .map(|meta| meta.len());
                continue;
            }
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
        let mut storage = Self::with_files(info, files, true);
        storage.selection = selection.clone();
        Ok(storage)
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

    /// `paths` on the blocking thread pool, so that creating the directory
    /// does not hold up the runtime.
    pub async fn paths_async(info: &Info, out_dir: &Path) -> Result<Vec<PathBuf>> {
        let (info, out_dir) = (info.clone(), out_dir.to_path_buf());
        tokio::task::spawn_blocking(move || Self::paths(&info, &out_dir)).await?
    }

    /// `new` on the blocking thread pool, so that creating and resizing the
    /// files does not hold up the runtime.
    pub async fn new_async(info: &Info, out_dir: &Path, selection: &FileSelection) -> Result<Self> {
        let (info, out_dir, selection) = (info.clone(), out_dir.to_path_buf(), selection.clone());
        tokio::task::spawn_blocking(move || Self::new(&info, &out_dir, &selection)).await?
    }

    /// `open_existing` on the blocking thread pool.
    pub async fn open_existing_async(info: &Info, out_dir: &Path) -> Result<Self> {
        let (info, out_dir) = (info.clone(), out_dir.to_path_buf());
        tokio::task::spawn_blocking(move || Self::open_existing(&info, &out_dir)).await?
    }

    fn with_files(info: &Info, files: Vec<FileSpan>, writable: bool) -> Self {
        let total_length = files.iter().map(|f| f.length).sum();
        Self {
            files,
            piece_length: info.piece_length,
            total_length,
            writable,
            selection: FileSelection::default(),
            handles: Mutex::default(),
        }
    }

    /// The torrent's files, in order.
    pub fn files(&self) -> &[FileSpan] {
        &self.files
    }

    /// The selection the files were created by: all `Normal` for a storage
    /// opened with `open_existing`.
    pub fn selection(&self) -> &FileSelection {
        &self.selection
    }

    /// Whether every file holding bytes of `offset..offset + len` is on
    /// disk.
    pub fn files_exist(&self, offset: u64, len: usize) -> bool {
        self.segments(offset, len)
            .all(|(index, _, _)| self.files[index].exists())
    }

    /// Checks the data of piece `index` against its `hash`, and writes it to
    /// disk if it matches, all on the blocking thread pool. Returns false,
    /// writing nothing, if it does not.
    pub async fn verify_and_write(
        self: &Arc<Self>,
        index: usize,
        data: Vec<u8>,
        hash: [u8; 20],
    ) -> Result<bool> {
        let storage = Arc::clone(self);
        let offset = index as u64 * self.piece_length;
        tokio::task::spawn_blocking(move || {
            let actual: [u8; 20] = Sha1::digest(&data).into();
            if actual != hash {
                return Ok(false);
            }
            storage
                .write_at(offset, &data)
                .map(|()| true)
                .context(StorageError)
        })
        .await?
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
        tokio::task::spawn_blocking(move || storage.read_at(offset, length as usize))
            .await?
            .context(StorageError)
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

    /// Indexes of the files holding bytes of `range`, in order. Empty files
    /// hold none.
    fn files_in(&self, range: std::ops::Range<u64>) -> impl Iterator<Item = usize> {
        files_between(&self.files, range.start, range.end)
    }

    /// Splits `offset..offset + len` into per-file pieces: the file's index,
    /// the position within that file, and the matching range of the buffer.
    fn segments(
        &self,
        offset: u64,
        len: usize,
    ) -> impl Iterator<Item = (usize, u64, std::ops::Range<usize>)> {
        let end = offset + len as u64;
        self.files_in(offset..end).filter_map(move |index| {
            let file = &self.files[index];
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

/// Indexes of the files holding bytes of `start..end`, which are in order in
/// `files`. Empty files hold none.
pub(crate) fn files_between(
    files: &[FileSpan],
    start: u64,
    end: u64,
) -> impl Iterator<Item = usize> {
    let first = files.partition_point(|file| file.offset + file.length <= start);
    (first..files.len())
        .take_while(move |&file| files[file].offset < end)
        .filter(move |&file| files[file].length > 0)
}

/// Which files hold bytes of a piece that `selection` wants.
fn touched_by_wanted_pieces(
    info: &Info,
    files: &[FileSpan],
    selection: &FileSelection,
) -> Vec<bool> {
    let priorities = priorities_of(files, info.piece_length, info.pieces.len() / 20, selection);
    let mut touched = vec![false; files.len()];
    for (index, priority) in priorities.iter().enumerate() {
        if priority.is_some() {
            let start = index as u64 * info.piece_length;
            let end = start + info.piece_length;
            for file in files_between(files, start, end) {
                touched[file] = true;
            }
        }
    }
    touched
}

/// `path` as it is compared with others: normalized to NFC and lowercased
/// on macOS and Windows, whose default file systems treat names that differ
/// only in case (or, on macOS, in how an accent is composed) as one file.
fn fold_case(path: &Path) -> PathBuf {
    if cfg!(any(target_os = "macos", windows)) {
        PathBuf::from(
            path.to_string_lossy()
                .nfc()
                .collect::<String>()
                .to_lowercase(),
        )
    } else {
        path.to_path_buf()
    }
}

/// Where each file of the torrent goes: `out_dir/name` for a single file,
/// under `out_dir/name/` for several. Fails if two files share a path.
/// Nothing is touched on disk.
pub(crate) fn layout(info: &Info, out_dir: &Path) -> Result<Vec<FileSpan>> {
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

/// Deletes the torrent's files from `out_dir`, then the directories they
/// leave empty, up to the torrent's own (never `out_dir`). Keeps going past
/// a failure, and returns every one; a file that is not there is no
/// failure.
///
/// `layout` keeps every path under `out_dir`, since it rejects `..` and
/// absolute components. So that nothing outside is reached through a
/// symbolic link either, only regular files are removed, and only when
/// none of their directories under `out_dir` is a link: a link is left
/// alone, and so is what it points to. Directories are removed only if
/// empty.
pub fn delete_files(info: &Info, out_dir: &Path) -> Vec<anyhow::Error> {
    let files = match layout(info, out_dir) {
        Ok(files) => files,
        Err(e) => return vec![e],
    };
    let mut failures = Vec::new();
    // Every directory of the torrent's, deepest first once sorted.
    let mut dirs = Vec::new();
    for file in &files {
        let mut parents: Vec<&Path> = file
            .path
            .ancestors()
            .skip(1)
            .take_while(|dir| *dir != out_dir)
            .collect();
        // From the torrent's own directory down, to find a link on the way
        // before anything under it is touched.
        parents.reverse();
        match delete_file(&file.path, &parents) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                failures.push(
                    anyhow::Error::new(e)
                        .context(format!("failed to delete {}", file.path.display())),
                );
            }
        }
        dirs.extend(parents.into_iter().map(Path::to_path_buf));
    }
    dirs.sort_by_key(|dir| std::cmp::Reverse(dir.components().count()));
    dirs.dedup();
    for dir in dirs {
        let removed = match std::fs::symlink_metadata(&dir) {
            Ok(meta) if meta.is_dir() => std::fs::remove_dir(&dir),
            Ok(_) => continue,
            Err(e) => Err(e),
        };
        match removed {
            Ok(()) => {}
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(e) => {
                failures.push(
                    anyhow::Error::new(e).context(format!("failed to delete {}", dir.display())),
                );
            }
        }
    }
    failures
}

/// Removes `path` if it is a regular file in real directories `parents`.
fn delete_file(path: &Path, parents: &[&Path]) -> io::Result<()> {
    for dir in parents.iter().copied().chain([path]) {
        let meta = std::fs::symlink_metadata(dir)?;
        let real = if dir == path {
            meta.is_file()
        } else {
            meta.is_dir()
        };
        if !real {
            let what = if dir == path { "file" } else { "directory" };
            return Err(io::Error::other(format!(
                "{} is a link or not a {what}, so it was left alone",
                dir.display()
            )));
        }
    }
    std::fs::remove_file(path)
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
