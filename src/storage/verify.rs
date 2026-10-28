//! Checks data already on disk against the torrent's piece hashes, so an
//! interrupted download can resume where it left off.

use super::Storage;
use crate::torrent::TorrentFile;
use anyhow::{Result, anyhow};
use sha1::Digest;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Most threads hashing at once. Reading from disk is usually the limit, and
/// each thread holds a whole piece in memory.
const MAX_THREADS: usize = 4;

/// Pieces that may hold data from an earlier run: those overlapping data
/// that was on disk before `Storage::new` created and resized the files,
/// and whose files are all there (a skipped file may not be).
pub fn candidates(torrent: &TorrentFile, storage: &Storage) -> Result<Vec<usize>> {
    pieces_where(torrent, |offset, len| {
        storage.may_hold_prior_data(offset, len) && storage.files_exist(offset, len)
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
/// been hashed. An I/O error stops the check, and so does `cancel` being set,
/// which is looked at between pieces and makes the result an error.
pub fn verify(
    torrent: &TorrentFile,
    storage: &Storage,
    pieces: &[usize],
    cancel: &AtomicBool,
    on_checked: &(dyn Fn(u64) + Sync),
) -> Result<Vec<bool>> {
    let next = AtomicUsize::new(0);
    let worker = || -> Result<Vec<usize>> {
        let mut good = Vec::new();
        while !cancel.load(Ordering::Relaxed)
            && let Some(&index) = pieces.get(next.fetch_add(1, Ordering::Relaxed))
        {
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
    if cancel.load(Ordering::Relaxed) {
        return Err(anyhow!("check cancelled"));
    }
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
mod tests;
