//! Which files of a torrent to download, and which first.
//!
//! A file's priority carries over to the pieces it touches: a piece is
//! wanted if any non-empty file in it is, and takes the highest priority
//! among them. A piece shared by a skipped file and a wanted one is still
//! downloaded, so the skipped file may be created (see `Storage::new`).

use crate::storage::{FileSpan, files_between};
use crate::torrent::{Info, TorrentFile};
use anyhow::Result;

/// How much a file is wanted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Not downloaded, unless a piece it shares with another file is.
    Skip,
    #[default]
    Normal,
    /// Downloaded before the rest.
    High,
}

impl Priority {
    /// The number stored in the resume file.
    pub(crate) fn to_wire(self) -> i64 {
        match self {
            Priority::Skip => 0,
            Priority::Normal => 1,
            Priority::High => 2,
        }
    }

    /// The priority stored as `value`, if it is one.
    pub(crate) fn from_wire(value: i64) -> Option<Self> {
        match value {
            0 => Some(Priority::Skip),
            1 => Some(Priority::Normal),
            2 => Some(Priority::High),
            _ => None,
        }
    }
}

impl Priority {
    /// The next higher priority, or the same for `High`.
    pub fn raised(self) -> Self {
        match self {
            Priority::Skip => Priority::Normal,
            Priority::Normal | Priority::High => Priority::High,
        }
    }

    /// The next lower priority, or the same for `Skip`.
    pub fn lowered(self) -> Self {
        match self {
            Priority::Skip | Priority::Normal => Priority::Skip,
            Priority::High => Priority::Normal,
        }
    }

    /// `Normal` for a skipped file, and `Skip` for any other.
    pub fn toggled(self) -> Self {
        match self {
            Priority::Skip => Priority::Normal,
            Priority::Normal | Priority::High => Priority::Skip,
        }
    }
}

/// The priority of each file of a torrent, in order. The empty selection
/// means every file is `Normal`, whatever their number.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileSelection(Vec<Priority>);

impl FileSelection {
    pub fn new(priorities: Vec<Priority>) -> Self {
        Self(priorities)
    }

    /// `count` files, all `Normal`.
    pub fn all_normal(count: usize) -> Self {
        Self(vec![Priority::Normal; count])
    }

    /// The priority of file `index`; `Normal` for one the selection does
    /// not list.
    pub fn priority(&self, index: usize) -> Priority {
        self.0.get(index).copied().unwrap_or_default()
    }

    /// The priorities as listed, none for the empty selection.
    pub fn priorities(&self) -> &[Priority] {
        &self.0
    }

    /// The priorities of `count` files, listed or not.
    pub fn for_files(&self, count: usize) -> Vec<Priority> {
        (0..count).map(|index| self.priority(index)).collect()
    }

    /// Whether every file is `Normal`.
    pub fn is_all_normal(&self) -> bool {
        self.0.iter().all(|&priority| priority == Priority::Normal)
    }

    /// Whether the selection lists files and skips every one.
    pub fn is_all_skipped(&self) -> bool {
        !self.0.is_empty() && self.0.iter().all(|&priority| priority == Priority::Skip)
    }

    /// Whether the selection lists no file, which means all are `Normal`.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// The files named on the command line (`--select-file 1,3-5`): numbered
/// from 1, as aria2 does, in ranges that may overlap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilePicks(Vec<(usize, usize)>);

impl FilePicks {
    /// Parses a comma-separated list of file numbers and ranges (`3`,
    /// `3-5`), each from 1.
    pub fn parse(text: &str) -> Result<Self, String> {
        let number = |part: &str| match part.trim().parse::<usize>() {
            Ok(number) if number >= 1 => Ok(number),
            _ => Err(format!("{part:?} is not a file number, counting from 1")),
        };
        let mut ranges = Vec::new();
        for item in text.split(',') {
            let (first, last) = match item.split_once('-') {
                Some((first, last)) => (number(first)?, number(last)?),
                None => (number(item)?, number(item)?),
            };
            if first > last {
                return Err(format!("{item:?} is a range that ends before it starts"));
            }
            ranges.push((first, last));
        }
        Ok(Self(ranges))
    }

    /// The highest file number picked.
    pub fn last(&self) -> usize {
        self.0.iter().map(|&(_, last)| last).max().unwrap_or(0)
    }

    /// The selection of a torrent of `count` files that downloads the ones
    /// picked and skips the rest. Numbers past the last file pick nothing.
    pub fn selection(&self, count: usize) -> FileSelection {
        let picked = |index: usize| {
            self.0
                .iter()
                .any(|&(first, last)| (first..=last).contains(&(index + 1)))
        };
        FileSelection::new(
            (0..count)
                .map(|index| {
                    if picked(index) {
                        Priority::Normal
                    } else {
                        Priority::Skip
                    }
                })
                .collect(),
        )
    }
}

/// Bytes in the pieces that `selection` wants of `torrent`.
pub fn wanted_of(torrent: &TorrentFile, selection: &FileSelection) -> Result<u64> {
    let files = crate::storage::layout(&torrent.info, std::path::Path::new(""))?;
    wanted_len(torrent, &piece_priorities(torrent, &files, selection))
}

/// How many files `info` lists.
pub fn file_count(info: &Info) -> usize {
    info.files.as_ref().map_or(1, Vec::len)
}

/// The priority of each piece of `torrent`, given where its `files` lie:
/// the highest of the non-empty files it touches, or `None` if they are all
/// skipped.
pub fn piece_priorities(
    torrent: &TorrentFile,
    files: &[FileSpan],
    selection: &FileSelection,
) -> Vec<Option<Priority>> {
    priorities_of(
        files,
        torrent.info.piece_length,
        torrent.num_pieces(),
        selection,
    )
}

/// `piece_priorities` for a torrent of `num_pieces` pieces of
/// `piece_length` bytes, the last perhaps shorter.
pub(crate) fn priorities_of(
    files: &[FileSpan],
    piece_length: u64,
    num_pieces: usize,
    selection: &FileSelection,
) -> Vec<Option<Priority>> {
    let total = files.last().map_or(0, |file| file.offset + file.length);
    (0..num_pieces)
        .map(|index| {
            let start = index as u64 * piece_length;
            let end = (start + piece_length).min(total);
            files_between(files, start, end)
                .map(|file| selection.priority(file))
                .max()
                .filter(|&priority| priority != Priority::Skip)
        })
        .collect()
}

/// Total length of the wanted pieces.
pub fn wanted_len(torrent: &TorrentFile, priorities: &[Option<Priority>]) -> Result<u64> {
    sum_pieces(torrent, priorities, |_| true)
}

/// Length of the wanted pieces marked in `have`.
pub fn wanted_done(
    torrent: &TorrentFile,
    priorities: &[Option<Priority>],
    have: &[bool],
) -> Result<u64> {
    sum_pieces(torrent, priorities, |index| {
        have.get(index).copied().unwrap_or(false)
    })
}

fn sum_pieces(
    torrent: &TorrentFile,
    priorities: &[Option<Priority>],
    keep: impl Fn(usize) -> bool,
) -> Result<u64> {
    priorities
        .iter()
        .enumerate()
        .filter(|&(index, priority)| priority.is_some() && keep(index))
        .map(|(index, _)| torrent.piece_len(index))
        .sum()
}

#[cfg(test)]
mod tests;
