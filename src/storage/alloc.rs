//! Reserving a file's disk space up front.

use std::fs::File;
use std::io;

/// Zeros written at a time where the file system cannot reserve space.
const ZERO_CHUNK: usize = 1024 * 1024;

/// Whether files are given their disk space when created.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize, clap::ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum Preallocate {
    /// Files are set to their full size and the file system gives them
    /// blocks as data arrives, so the space is used as the torrent
    /// downloads. Holes read as zeros.
    #[default]
    Sparse,
    /// Each wanted file's space is reserved when it is created, so a full
    /// disk shows at the start, and the file is not scattered by the pieces
    /// arriving in random order.
    Full,
}

impl Preallocate {
    /// The setting's name, as written on the command line and in the
    /// config file.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sparse => "sparse",
            Self::Full => "full",
        }
    }
}

/// Reserves disk space for `file` up to `length` bytes, and leaves it
/// exactly that long. `current` is its size now. Data already in the file
/// is kept. A disk without room fails with `ErrorKind::StorageFull`.
pub(super) fn reserve(file: &File, current: u64, length: u64) -> io::Result<()> {
    if length > 0 && !reserve_natively(file, current, length)? {
        file.set_len(length)?;
        if current == 0 {
            zero_fill(file, 0, length)?;
        } else {
            // An existing sparse file can already have its final logical
            // length. Materialize its holes without changing verified data.
            let mut buffer = vec![0; ZERO_CHUNK];
            let mut offset = 0;
            while offset < length {
                let count = (length - offset).min(ZERO_CHUNK as u64) as usize;
                super::read_exact_at(file, &mut buffer[..count], offset)?;
                super::write_all_at(file, &buffer[..count], offset)?;
                offset += count as u64;
            }
        }
    }
    if file.metadata()?.len() != length {
        file.set_len(length)?;
    }
    Ok(())
}

/// Asks the operating system to reserve the space. `Ok(false)` if it cannot
/// here, so the caller falls back to writing zeros.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn reserve_natively(file: &File, _current: u64, length: u64) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    let length = libc::off_t::try_from(length).map_err(|_| io::Error::other("file too large"))?;
    // Reserves the holes below `length` too and leaves data as it is. It
    // returns the error number itself, not -1.
    // SAFETY: the descriptor is open for as long as `file` is.
    match unsafe { libc::posix_fallocate(file.as_raw_fd(), 0, length) } {
        0 => Ok(true),
        libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS => Ok(false),
        errno => Err(io::Error::from_raw_os_error(errno)),
    }
}

#[cfg(target_os = "macos")]
fn reserve_natively(file: &File, current: u64, length: u64) -> io::Result<bool> {
    use std::os::fd::AsRawFd;
    if current > 0 {
        // F_PREALLOCATE reserves beyond physical EOF, rather than filling
        // existing holes. Skip work only when the file is already allocated;
        // otherwise use the data-preserving fallback above.
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        return Ok(current <= length && metadata.blocks().saturating_mul(512) >= length);
    }
    // The empty file's entire extent can be reserved natively.
    let want =
        libc::off_t::try_from(length - current).map_err(|_| io::Error::other("file too large"))?;
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATEALL,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: want,
        fst_bytesalloc: 0,
    };
    // SAFETY: `store` is a valid `fstore_t` that the call reads and updates.
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &mut store) } == -1 {
        let e = io::Error::last_os_error();
        return match e.raw_os_error() {
            Some(libc::ENOTSUP | libc::EINVAL) => Ok(false),
            _ => Err(e),
        };
    }
    // The blocks are reserved; the size follows.
    file.set_len(length)?;
    Ok(true)
}

#[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
fn reserve_natively(_: &File, _: u64, _: u64) -> io::Result<bool> {
    Ok(false)
}

/// Writes zeros over `from..to`, which a file's new tail reads as anyway:
/// it is what makes the file system hand out the blocks.
fn zero_fill(file: &File, from: u64, to: u64) -> io::Result<()> {
    let zeros = vec![0u8; ZERO_CHUNK];
    let mut at = from;
    while at < to {
        let n = usize::try_from(to - at).map_or(ZERO_CHUNK, |n| n.min(ZERO_CHUNK));
        super::write_all_at(file, &zeros[..n], at)?;
        at += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
