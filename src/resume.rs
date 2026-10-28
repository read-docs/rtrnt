//! The resume file: which pieces of a torrent were verified and written, and
//! the size and modification time of each file when that was saved. A later
//! run trusts the pieces that lie only in files that still look the same,
//! and hashes the rest as usual.
//!
//! There is one file per torrent, `<info hash in hex>.resume` in the state
//! directory (see `default_dir`), outside the download directory.

use crate::bencode;
use crate::download::{self, DownloadConfig, Session};
use crate::selection::{self, FileSelection, Priority};
use crate::state::{self, write_atomically};
use crate::stats::TorrentState;
use crate::storage::verify;
use crate::storage::{FileSpan, Storage, files_between};
use crate::torrent::TorrentFile;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;
use tokio::sync::Semaphore;
use tracing::{info, warn};

/// The format of the file, bumped when it changes incompatibly.
const VERSION: i64 = 1;

/// A file's size and modification time in nanoseconds since the epoch, or
/// `(0, -1)` for one that cannot be read. That never matches a file that is
/// there, so a missing file is never clean.
type Stamp = (u64, i64);

/// Where one torrent's resume file is, and what it must match.
#[derive(Debug, Clone)]
pub struct Target {
    path: PathBuf,
    info_hash: [u8; 20],
    /// The download directory, made absolute: the file is ignored if a run
    /// uses another one.
    output_dir: PathBuf,
}

impl Target {
    /// The resume file of the torrent with `info_hash` as `config` has it, or
    /// `None` if there is no `resume_dir`.
    pub fn new(config: &DownloadConfig, info_hash: [u8; 20]) -> Option<Self> {
        let dir = config.resume_dir.as_ref()?;
        Some(Self {
            path: dir.join(format!("{}.resume", hex(&info_hash))),
            info_hash,
            output_dir: std::path::absolute(&config.output_dir)
                .unwrap_or_else(|_| config.output_dir.clone()),
        })
    }

    /// Where the resume file is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Saves the resume file for the pieces marked in `have` and the files
    /// as they are now, on the blocking thread pool. The file is replaced
    /// atomically. A failure is logged, and the download goes on without a
    /// fresh resume file.
    ///
    /// `have` must be taken before this is called, so that it never claims a
    /// piece whose write had not finished when the files were looked at.
    pub async fn save(&self, storage: &Arc<Storage>, have: Vec<bool>) {
        let target = self.clone();
        let storage = Arc::clone(storage);
        let saved = tokio::task::spawn_blocking(move || {
            let saved = Saved {
                version: VERSION,
                info_hash: ByteBuf::from(target.info_hash.to_vec()),
                output_dir: ByteBuf::from(target.output_dir.as_os_str().as_encoded_bytes()),
                pieces: ByteBuf::from(pack(&have)),
                files: storage.files().iter().map(|f| stamp(&f.path)).collect(),
                priorities: priorities_to_save(storage.selection()),
            };
            write_atomically(&target.path, &serde_bencode::to_bytes(&saved)?)
        })
        .await;
        match saved {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("cannot save the resume file: {e:#}"),
            Err(e) => warn!("cannot save the resume file: {e}"),
        }
    }
}

impl Target {
    /// Saves `selection` in the resume file of a torrent that is not
    /// running, for the next run to take, leaving the rest of the file as
    /// it is. Without a usable file (none, or for another torrent or
    /// download directory), one is made that vouches for no piece, so the
    /// next run hashes what is on disk. The file is replaced atomically.
    pub async fn save_selection(
        &self,
        selection: &FileSelection,
        file_count: usize,
        num_pieces: usize,
    ) -> Result<()> {
        let (target, selection) = (self.clone(), selection.clone());
        tokio::task::spawn_blocking(move || {
            let kept = std::fs::read(&target.path)
                .ok()
                .and_then(|data| parse(&data, &target, file_count, num_pieces).ok());
            let priorities = priorities_to_save(&selection);
            let saved = match kept {
                Some(saved) => Saved {
                    priorities,
                    ..saved
                },
                None => Saved {
                    version: VERSION,
                    info_hash: ByteBuf::from(target.info_hash.to_vec()),
                    output_dir: ByteBuf::from(target.output_dir.as_os_str().as_encoded_bytes()),
                    pieces: ByteBuf::from(pack(&vec![false; num_pieces])),
                    files: vec![(0, -1); file_count],
                    priorities,
                },
            };
            write_atomically(&target.path, &serde_bencode::to_bytes(&saved)?)
        })
        .await?
    }
}

/// Where resume files go when no directory is given: `resume` in the state
/// directory (see `state::state_dir`), so `$XDG_STATE_HOME/rtrnt/resume` or
/// `~/.local/state/rtrnt/resume`. `None` if neither directory is known.
pub fn default_dir(xdg_state_home: Option<OsString>, home: Option<&Path>) -> Option<PathBuf> {
    Some(state::state_dir(xdg_state_home, home)?.join("resume"))
}

/// The contents of a resume file.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Saved {
    version: i64,
    #[serde(rename = "info hash")]
    info_hash: ByteBuf,
    #[serde(rename = "output dir")]
    output_dir: ByteBuf,
    /// A bitfield of the pieces verified and written, as in the peer wire
    /// protocol: the first piece is the high bit of the first byte.
    pieces: ByteBuf,
    /// Each file of the torrent, in order, as it was when saved.
    files: Vec<Stamp>,
    /// The priority of each file (see `Priority::to_wire`), when any is not
    /// normal. Files from before priorities existed have none, and so have
    /// those saved with every file normal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    priorities: Vec<i64>,
}

/// The priorities of `selection` as stored: none if all are normal.
fn priorities_to_save(selection: &FileSelection) -> Vec<i64> {
    if selection.is_all_normal() {
        return Vec::new();
    }
    selection
        .priorities()
        .iter()
        .map(|priority| priority.to_wire())
        .collect()
}

/// Which pieces of the torrent are on disk and intact, for a run that is
/// about to start: those the resume file vouches for, plus those that had
/// to be hashed.
///
/// A file is clean if its size and modification time are what the resume
/// file recorded. A piece set in the resume file that lies only in clean
/// files is done without hashing. A piece that touches a file that is not
/// clean is hashed if it overlaps data that was on disk before (see
/// `verify::candidates`). Pieces in clean files that the resume file does
/// not list are not hashed: they are zeros we pre-allocated.
///
/// Without a usable resume file (none, unreadable, for another torrent or
/// download directory, or `recheck`), every piece that may hold data
/// from an earlier run is hashed. Either way the resume file is saved
/// afterwards.
///
/// Hashing waits for `hashing`'s one permit (see `download::hash_pieces`),
/// and is stopped by `shutdown`, which makes the result an `Interrupted`
/// error.
pub async fn initial_pieces(
    session: &Arc<Session>,
    recheck: bool,
    hashing: &Semaphore,
    shutdown: impl Future<Output = ()>,
) -> Result<Vec<bool>> {
    let torrent = &session.torrent;
    let priorities = session.piece_priorities();
    session.stats.set_selection(
        session
            .storage
            .selection()
            .for_files(session.storage.files().len()),
        selection::wanted_len(torrent, &priorities)?,
    );
    let candidates = verify::candidates(torrent, &session.storage)?;
    let saved = match &session.resume {
        Some(target) if !recheck => load(target, session).await,
        _ => None,
    };
    let have = match saved {
        None => download::hash_pieces(session, candidates, hashing, shutdown).await?,
        Some(saved) => {
            let storage = Arc::clone(&session.storage);
            let stamps = tokio::task::spawn_blocking(move || {
                storage
                    .files()
                    .iter()
                    .map(|f| stamp(&f.path))
                    .collect::<Vec<_>>()
            })
            .await?;
            let files = session.storage.files();
            let clean = clean_files(files, &saved.files, &stamps);
            let (mut have, to_hash) = split(torrent, files, &clean, &saved.pieces, &candidates)?;
            let dirty = clean.iter().filter(|&&clean| !clean).count();
            info!(
                files = files.len(),
                dirty,
                trusted = have.iter().filter(|&&have| have).count(),
                to_check = to_hash.len(),
                "using the resume file"
            );
            let hashed = download::hash_pieces(session, to_hash, hashing, shutdown).await?;
            for (have, hashed) in have.iter_mut().zip(hashed) {
                *have |= hashed;
            }
            have
        }
    };
    if let Some(target) = &session.resume {
        target.save(&session.storage, have.clone()).await;
    }
    session
        .stats
        .set_verified(selection::wanted_done(torrent, &priorities, &have)?);
    session.stats.set_state(TorrentState::Downloading);
    Ok(have)
}

/// Reads and checks the resume file of `session`, or says in the log why
/// there is none to use.
async fn load(target: &Target, session: &Session) -> Option<Saved> {
    let data = match tokio::fs::read(&target.path).await {
        Ok(data) => data,
        Err(e) => {
            info!(path = %target.path.display(), "no resume file to use: {e}");
            return None;
        }
    };
    let (file_count, num_pieces) = (session.storage.files().len(), session.torrent.num_pieces());
    match parse(&data, target, file_count, num_pieces) {
        Ok(saved) => Some(saved),
        Err(e) => {
            info!(path = %target.path.display(), "ignoring the resume file: {e:#}");
            None
        }
    }
}

/// The file selection saved for a torrent of `file_count` files, if the
/// resume file is there, is for this torrent and download directory, and
/// has one. Called before the files are created, since which of them are
/// depends on it. Without priorities in the file, all are `Normal`, which
/// is the same as none.
pub fn load_selection(target: &Target, file_count: usize) -> Option<FileSelection> {
    let data = std::fs::read(&target.path).ok()?;
    let saved = match decode(&data, target) {
        Ok(saved) => saved,
        Err(e) => {
            info!(path = %target.path.display(), "ignoring the resume file: {e:#}");
            return None;
        }
    };
    selection_of(&saved, file_count)
}

/// The selection `saved` holds, if it has one for `file_count` files.
fn selection_of(saved: &Saved, file_count: usize) -> Option<FileSelection> {
    if saved.priorities.is_empty() {
        return None;
    }
    let priorities: Option<Vec<Priority>> = saved
        .priorities
        .iter()
        .map(|&wire| Priority::from_wire(wire))
        .collect();
    priorities
        .filter(|priorities| priorities.len() == file_count)
        .map(FileSelection::new)
}

/// Decodes a resume file and checks that it is for this torrent and this
/// download directory.
fn decode(data: &[u8], target: &Target) -> Result<Saved> {
    // Bounds the nesting, so that decoding the value is safe.
    let len = bencode::value_len(data)?;
    let saved: Saved = serde_bencode::from_bytes(&data[..len]).context("not a resume file")?;
    if saved.version != VERSION {
        bail!("version {} is not {VERSION}", saved.version);
    }
    if saved.info_hash.as_slice() != target.info_hash {
        bail!("it is for another torrent");
    }
    if saved.output_dir.as_slice() != target.output_dir.as_os_str().as_encoded_bytes() {
        bail!("it is for another download directory");
    }
    Ok(saved)
}

/// `decode`, and checks that it is for a torrent of `file_count` files and
/// `num_pieces` pieces.
fn parse(data: &[u8], target: &Target, file_count: usize, num_pieces: usize) -> Result<Saved> {
    let saved = decode(data, target)?;
    if saved.files.len() != file_count {
        bail!("it lists {} files, not {file_count}", saved.files.len());
    }
    if saved.pieces.len() != num_pieces.div_ceil(8) {
        bail!(
            "it has {} bytes of pieces, not {}",
            saved.pieces.len(),
            num_pieces.div_ceil(8)
        );
    }
    Ok(saved)
}

/// Which of `files` are clean: still the size they were made, with the
/// modification time `saved` recorded, given their `stamps` now.
fn clean_files(files: &[FileSpan], saved: &[Stamp], stamps: &[Stamp]) -> Vec<bool> {
    files
        .iter()
        .zip(saved.iter().zip(stamps))
        .map(|(file, (saved, now))| saved == now && saved.1 >= 0 && saved.0 == file.length)
        .collect()
}

/// Settles the pieces of `torrent` given which `files` are `clean` and the
/// `saved` bitfield: returns those that are done, and those to hash, which
/// are the `candidates` that touch a file that is not clean.
fn split(
    torrent: &TorrentFile,
    files: &[FileSpan],
    clean: &[bool],
    saved: &[u8],
    candidates: &[usize],
) -> Result<(Vec<bool>, Vec<usize>)> {
    let mut candidate = vec![false; torrent.num_pieces()];
    for &index in candidates {
        candidate[index] = true;
    }
    let mut have = vec![false; torrent.num_pieces()];
    let mut to_hash = Vec::new();
    for index in 0..torrent.num_pieces() {
        let start = index as u64 * torrent.info.piece_length;
        let end = start + torrent.piece_len(index)?;
        if files_between(files, start, end).all(|file| clean[file]) {
            have[index] = is_set(saved, index);
        } else if candidate[index] {
            to_hash.push(index);
        }
    }
    Ok((have, to_hash))
}

/// `have` as a bitfield, first piece in the high bit.
fn pack(have: &[bool]) -> Vec<u8> {
    let mut bits = vec![0u8; have.len().div_ceil(8)];
    for index in (0..have.len()).filter(|&index| have[index]) {
        bits[index / 8] |= 0x80 >> (index % 8);
    }
    bits
}

/// Whether piece `index` is set in the bitfield `bits`.
fn is_set(bits: &[u8], index: usize) -> bool {
    bits.get(index / 8)
        .is_some_and(|byte| byte & (0x80 >> (index % 8)) != 0)
}

/// The size and modification time of the file at `path`.
fn stamp(path: &Path) -> Stamp {
    let read = || -> Option<Stamp> {
        let meta = std::fs::metadata(path).ok()?;
        let since_epoch = meta.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        Some((meta.len(), i64::try_from(since_epoch.as_nanos()).ok()?))
    };
    read().unwrap_or((0, -1))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests;
