//! Checks data already on disk against the torrent's piece hashes, so an
//! interrupted download can resume where it left off.

use crate::storage::Storage;
use crate::torrent::TorrentFile;
use anyhow::{Result, anyhow};
use sha1::Digest;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Most threads hashing at once. Reading from disk is usually the limit, and
/// each thread holds a whole piece in memory.
const MAX_THREADS: usize = 4;

/// Pieces that may hold data from an earlier run: those overlapping data
/// that was on disk before `Storage::new` created and resized the files.
pub fn candidates(torrent: &TorrentFile, storage: &Storage) -> Result<Vec<usize>> {
    pieces_where(torrent, |offset, len| {
        storage.may_hold_prior_data(offset, len)
    })
}

/// Pieces whose every byte is on disk, so they can be read and hashed.
pub fn present(torrent: &TorrentFile, storage: &Storage) -> Result<Vec<usize>> {
    pieces_where(torrent, |offset, len| storage.is_on_disk(offset, len))
}

fn pieces_where(torrent: &TorrentFile, keep: impl Fn(u64, usize) -> bool) -> Result<Vec<usize>> {
    let mut pieces = Vec::new();
    for index in 0..torrent.num_pieces() {
        let (offset, len) = piece_range(torrent, index)?;
        if keep(offset, len) {
            pieces.push(index);
        }
    }
    Ok(pieces)
}

/// Hashes `pieces` on disk and returns, for every piece of the torrent,
/// whether it is there and correct. Pieces not in `pieces` count as missing.
///
/// Blocks while reading; `on_checked` gets each piece's length once it has
/// been hashed. An I/O error stops the check.
pub fn verify(
    torrent: &TorrentFile,
    storage: &Storage,
    pieces: &[usize],
    on_checked: &(dyn Fn(u64) + Sync),
) -> Result<Vec<bool>> {
    let next = AtomicUsize::new(0);
    let worker = || -> Result<Vec<usize>> {
        let mut good = Vec::new();
        while let Some(&index) = pieces.get(next.fetch_add(1, Ordering::Relaxed)) {
            match check_piece(torrent, storage, index) {
                Ok((true, len)) => {
                    good.push(index);
                    on_checked(len);
                }
                Ok((false, len)) => on_checked(len),
                Err(e) => {
                    // Stop the other threads at their next piece.
                    next.store(pieces.len(), Ordering::Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(good)
    };

    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(MAX_THREADS)
        .min(pieces.len());
    let results: Vec<Result<Vec<usize>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads).map(|_| scope.spawn(worker)).collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(anyhow!("hashing thread panicked")))
            })
            .collect()
    });

    let mut have = vec![false; torrent.num_pieces()];
    for result in results {
        for index in result? {
            have[index] = true;
        }
    }
    Ok(have)
}

/// The outcome of checking a download against its torrent.
#[derive(Debug, PartialEq, Eq)]
pub struct Report {
    pub good_pieces: usize,
    pub total_pieces: usize,
    /// Files with something wrong, in torrent order.
    pub problems: Vec<(PathBuf, FileProblem)>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FileProblem {
    Missing,
    WrongSize {
        expected: u64,
        found: u64,
    },
    /// Pieces overlapping the file that did not match their hash. A piece
    /// spanning two files counts against both.
    BadPieces(usize),
}

impl Report {
    /// Compares the files on disk and the pieces that verified (`have`)
    /// against the torrent.
    pub fn new(torrent: &TorrentFile, storage: &Storage, have: &[bool]) -> Self {
        let piece_length = torrent.info.piece_length;
        let problems = storage
            .files()
            .iter()
            .filter_map(|file| {
                let problem = match file.found {
                    None => FileProblem::Missing,
                    Some(found) if found != file.length => FileProblem::WrongSize {
                        expected: file.length,
                        found,
                    },
                    // An empty file of the right size overlaps no piece.
                    Some(_) if file.length == 0 => return None,
                    Some(_) => {
                        let first = file.offset / piece_length;
                        let last = (file.offset + file.length - 1) / piece_length;
                        let bad = (first..=last)
                            .filter(|&i| !have.get(i as usize).copied().unwrap_or(false))
                            .count();
                        if bad == 0 {
                            return None;
                        }
                        FileProblem::BadPieces(bad)
                    }
                };
                Some((file.path.clone(), problem))
            })
            .collect();
        Self {
            good_pieces: have.iter().filter(|&&have| have).count(),
            total_pieces: have.len(),
            problems,
        }
    }

    /// Whether every piece verified and every file has the right size.
    pub fn is_complete(&self) -> bool {
        self.good_pieces == self.total_pieces && self.problems.is_empty()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{} of {} pieces OK", self.good_pieces, self.total_pieces)?;
        for (path, problem) in &self.problems {
            let path = path.display();
            match problem {
                FileProblem::Missing => writeln!(f, "missing     {path}")?,
                FileProblem::WrongSize { expected, found } => {
                    writeln!(f, "wrong size  {path}: {found} bytes, expected {expected}")?
                }
                FileProblem::BadPieces(1) => writeln!(f, "corrupt     {path}: 1 bad piece")?,
                FileProblem::BadPieces(n) => writeln!(f, "corrupt     {path}: {n} bad pieces")?,
            }
        }
        Ok(())
    }
}

/// Whether piece `index` on disk matches its hash, and the piece's length.
fn check_piece(torrent: &TorrentFile, storage: &Storage, index: usize) -> Result<(bool, u64)> {
    let (offset, len) = piece_range(torrent, index)?;
    let data = storage.read_at(offset, len)?;
    let actual: [u8; 20] = sha1::Sha1::digest(&data).into();
    Ok((actual == torrent.piece_hash(index)?, len as u64))
}

/// Byte offset and length of piece `index` in the torrent's byte stream.
fn piece_range(torrent: &TorrentFile, index: usize) -> Result<(u64, usize)> {
    let len = usize::try_from(torrent.piece_len(index)?)?;
    Ok((index as u64 * torrent.info.piece_length, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::torrent::{FileInfo, Info};
    use serde_bytes::ByteBuf;
    use std::sync::atomic::AtomicU64;

    const PIECE_LENGTH: usize = 4;

    /// A torrent of two files, "a" (6 bytes) and "b" (5 bytes), over `data`.
    fn torrent(data: &[u8]) -> TorrentFile {
        let pieces: Vec<u8> = data
            .chunks(PIECE_LENGTH)
            .flat_map(|chunk| sha1::Sha1::digest(chunk).to_vec())
            .collect();
        TorrentFile {
            announce: None,
            announce_list: None,
            info: Info {
                name: "root".into(),
                piece_length: PIECE_LENGTH as u64,
                pieces: ByteBuf::from(pieces),
                length: None,
                files: Some(vec![
                    FileInfo {
                        path: vec!["a".into()],
                        length: 6,
                    },
                    FileInfo {
                        path: vec!["b".into()],
                        length: 5,
                    },
                ]),
            },
        }
    }

    #[test]
    fn finds_intact_pieces_and_skips_fresh_files() {
        let data = b"abcdefghijk";
        let torrent = torrent(data);
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        // "a" is complete from an earlier run except for a corrupt byte in
        // piece 1, which spans both files; "b" does not exist yet.
        std::fs::write(root.join("a"), b"abcdeX").unwrap();

        let storage = Storage::new(&torrent.info, dir.path()).unwrap();
        // Piece 2 lies entirely in the new, empty "b".
        let candidates = candidates(&torrent, &storage).unwrap();
        assert_eq!(candidates, [0, 1]);

        let checked = AtomicU64::new(0);
        let have = verify(&torrent, &storage, &candidates, &|len| {
            checked.fetch_add(len, Ordering::Relaxed);
        })
        .unwrap();
        assert_eq!(have, [true, false, false]);
        assert_eq!(checked.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn complete_data_verifies_every_piece() {
        let data = b"abcdefghijk";
        let torrent = torrent(data);
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a"), &data[..6]).unwrap();
        std::fs::write(root.join("b"), &data[6..]).unwrap();

        let storage = Storage::new(&torrent.info, dir.path()).unwrap();
        let candidates = candidates(&torrent, &storage).unwrap();
        assert_eq!(candidates, [0, 1, 2]);
        let have = verify(&torrent, &storage, &candidates, &|_| {}).unwrap();
        assert_eq!(have, [true, true, true]);
    }

    #[test]
    fn nothing_to_check_in_a_new_download() {
        let torrent = torrent(b"abcdefghijk");
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::new(&torrent.info, dir.path()).unwrap();
        assert!(candidates(&torrent, &storage).unwrap().is_empty());
        let have = verify(&torrent, &storage, &[], &|_| {}).unwrap();
        assert_eq!(have, [false, false, false]);
    }

    /// Writes the files, checks them read-only and returns the report.
    fn check(a: Option<&[u8]>, b: Option<&[u8]>) -> (Report, PathBuf) {
        let torrent = torrent(b"abcdefghijk");
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).unwrap();
        for (name, contents) in [("a", a), ("b", b)] {
            if let Some(contents) = contents {
                std::fs::write(root.join(name), contents).unwrap();
            }
        }
        let storage = Storage::open_existing(&torrent.info, dir.path()).unwrap();
        let pieces = present(&torrent, &storage).unwrap();
        let have = verify(&torrent, &storage, &pieces, &|_| {}).unwrap();
        (Report::new(&torrent, &storage, &have), root)
    }

    #[test]
    fn report_on_a_complete_download() {
        let (report, _) = check(Some(b"abcdef"), Some(b"ghijk"));
        assert!(report.is_complete());
        assert_eq!(report.to_string(), "3 of 3 pieces OK\n");
    }

    #[test]
    fn report_on_a_missing_file() {
        let (report, root) = check(Some(b"abcdef"), None);
        assert!(!report.is_complete());
        // Piece 1 spans both files, so "a" cannot be fully verified either.
        assert_eq!(
            report.problems,
            [
                (root.join("a"), FileProblem::BadPieces(1)),
                (root.join("b"), FileProblem::Missing),
            ]
        );
        assert_eq!(report.good_pieces, 1);
    }

    #[test]
    fn report_on_corrupt_and_resized_files() {
        let (report, root) = check(Some(b"abcdeX"), Some(b"ghijk!"));
        assert_eq!(
            report.problems,
            [
                (root.join("a"), FileProblem::BadPieces(1)),
                (
                    root.join("b"),
                    FileProblem::WrongSize {
                        expected: 5,
                        found: 6
                    }
                ),
            ]
        );
        assert_eq!(report.good_pieces, 2);
        let text = report.to_string();
        assert!(text.starts_with("2 of 3 pieces OK\n"), "{text}");
        assert!(text.contains("corrupt     "), "{text}");
        assert!(text.contains(": 6 bytes, expected 5\n"), "{text}");
    }
}
