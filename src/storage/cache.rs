//! The write cache: verified pieces held in memory until a flush writes
//! them to the files, in file-offset order and with adjacent pieces as one
//! write.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Most bytes joined into one write by a flush. Joining copies, so this
/// bounds the extra memory a flush uses.
const MAX_RUN: usize = 4 * 1024 * 1024;

/// A piece held in the cache: where it starts in the torrent's byte stream,
/// and its bytes.
pub(super) type Cached = (u64, Arc<Vec<u8>>);

/// Verified data not yet on disk, by offset in the torrent's byte stream.
///
/// An entry stays in the cache, and so stays visible to reads, until its
/// flush has written it (see `retire`).
#[derive(Debug)]
pub(super) struct WriteCache {
    /// Bytes held at which a flush is due; 0 for no cache.
    limit: usize,
    state: Mutex<State>,
    /// Held by a flush from start to end, so that two do not write the same
    /// entries at once.
    flushing: Mutex<()>,
}

#[derive(Debug, Default)]
struct State {
    entries: BTreeMap<u64, Arc<Vec<u8>>>,
    /// The total length of `entries`.
    bytes: usize,
}

impl WriteCache {
    /// A cache that holds `limit` bytes before a flush is due, or none (and
    /// does nothing) for 0.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::default(),
            flushing: Mutex::default(),
        }
    }

    /// Whether there is a cache at all.
    pub(super) fn enabled(&self) -> bool {
        self.limit > 0
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the right to flush, until the guard is dropped.
    pub(super) fn begin_flush(&self) -> MutexGuard<'_, ()> {
        self.flushing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Bytes held now.
    pub(super) fn bytes(&self) -> usize {
        self.state().bytes
    }

    /// Holds `data`, which starts at `offset`, in place of whatever was held
    /// at `offset`. Returns whether the cache is now at its limit, so that a
    /// flush is due.
    pub(super) fn insert(&self, offset: u64, data: Vec<u8>) -> bool {
        let mut state = self.state();
        state.bytes += data.len();
        if let Some(old) = state.entries.insert(offset, Arc::new(data)) {
            state.bytes -= old.len();
        }
        state.bytes >= self.limit
    }

    /// The entries that overlap `range`.
    pub(super) fn overlapping(&self, range: Range<u64>) -> Vec<Cached> {
        let state = self.state();
        // Entries do not overlap one another, so the one starting before
        // `range` that reaches into it is the last one that does.
        let before = state.entries.range(..range.start).next_back();
        let within = state.entries.range(range.start..range.end);
        before
            .into_iter()
            .chain(within)
            .filter(|(start, data)| **start + data.len() as u64 > range.start)
            .map(|(start, data)| (*start, Arc::clone(data)))
            .collect()
    }

    /// Every entry held, in offset order, in runs of entries that follow one
    /// another with no gap, none longer than `MAX_RUN` unless a single
    /// entry is. Each run is one write.
    pub(super) fn runs(&self) -> Vec<Vec<Cached>> {
        let state = self.state();
        let mut runs: Vec<Vec<Cached>> = Vec::new();
        let mut len = 0;
        let mut end = 0;
        for (start, data) in &state.entries {
            let joins = runs
                .last()
                .is_some_and(|_| *start == end && len + data.len() <= MAX_RUN);
            if joins {
                len += data.len();
            } else {
                runs.push(Vec::new());
                len = data.len();
            }
            end = *start + data.len() as u64;
            if let Some(run) = runs.last_mut() {
                run.push((*start, Arc::clone(data)));
            }
        }
        runs
    }

    /// Drops `entry` once it is on disk. An entry replaced meanwhile by
    /// newer data at its offset stays.
    pub(super) fn retire(&self, entry: &Cached) {
        let mut state = self.state();
        let same = state
            .entries
            .get(&entry.0)
            .is_some_and(|held| Arc::ptr_eq(held, &entry.1));
        if same {
            state.entries.remove(&entry.0);
            state.bytes -= entry.1.len();
        }
    }
}

/// The bytes of `run`, as one buffer.
pub(super) fn join(run: &[Cached]) -> Vec<u8> {
    let mut joined = Vec::with_capacity(run.iter().map(|(_, data)| data.len()).sum());
    for (_, data) in run {
        joined.extend_from_slice(data);
    }
    joined
}

#[cfg(test)]
mod tests;
