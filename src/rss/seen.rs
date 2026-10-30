//! The items already taken from the feeds, so that a restart does not add
//! them again.
//!
//! An item is remembered by the SHA-1 of its feed's URL and its id (see
//! `feed`), so the file stays small whatever the ids look like. It is
//! `rss-seen.json` in the state directory, replaced atomically, and holds at
//! most `MAX_SEEN` items: when more come, the oldest are forgotten. That is
//! far more than a feed lists at a time, so a forgotten item has long left
//! its feed.

use crate::magnet::hex;
use crate::state::write_atomically;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use tracing::warn;
use url::Url;

/// The most items remembered.
pub const MAX_SEEN: usize = 10_000;
/// The format of the file, bumped when it changes incompatibly.
const VERSION: u32 = 1;

/// The file as it is written: the oldest item first.
#[derive(Serialize, Deserialize)]
struct Stored {
    version: u32,
    seen: Vec<String>,
}

/// The items taken so far.
#[derive(Debug)]
pub struct Seen {
    /// Where it is kept; `None` keeps it in memory only.
    path: Option<PathBuf>,
    /// The oldest first.
    order: VecDeque<String>,
    set: HashSet<String>,
    limit: usize,
    /// Whether it has changed since it was read or saved.
    dirty: bool,
}

impl Seen {
    /// An empty cache kept in `path`, if any, holding at most `MAX_SEEN`.
    pub fn new(path: Option<PathBuf>) -> Self {
        Self {
            path,
            order: VecDeque::new(),
            set: HashSet::new(),
            limit: MAX_SEEN,
            dirty: false,
        }
    }

    /// The same, holding at most `limit` items.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit.max(1);
        self.trim();
        self
    }

    /// The cache in `path`. A missing file is an empty cache; one that
    /// cannot be read is set aside as `<name>.bad` with a warning, and the
    /// cache starts empty.
    pub fn load(path: Option<PathBuf>) -> Self {
        let mut seen = Self::new(path.clone());
        let Some(path) = path else {
            return seen;
        };
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return seen,
            Err(e) => {
                warn!(
                    "starting without the seen RSS items: cannot read {}: {e}",
                    path.display()
                );
                return seen;
            }
        };
        match serde_json::from_slice::<Stored>(&data) {
            Ok(stored) if stored.version == VERSION => {
                for key in stored.seen {
                    seen.add(key);
                }
                seen.dirty = false;
            }
            Ok(_) | Err(_) => {
                warn!(
                    "starting without the seen RSS items: {} cannot be used",
                    path.display()
                );
                let mut bad = path.as_os_str().to_owned();
                bad.push(".bad");
                if let Err(e) = std::fs::rename(&path, PathBuf::from(bad)) {
                    warn!("cannot set {} aside: {e}", path.display());
                }
            }
        }
        seen
    }

    /// What identifies the item `id` of the feed `feed`.
    fn key(feed: &Url, id: &str) -> String {
        let mut hash = Sha1::new();
        hash.update(feed.as_str().as_bytes());
        hash.update([0]);
        hash.update(id.as_bytes());
        hex(&hash.finalize())
    }

    /// Whether the item `id` of `feed` was taken.
    pub fn contains(&self, feed: &Url, id: &str) -> bool {
        self.set.contains(&Self::key(feed, id))
    }

    /// Remembers the item `id` of `feed`.
    pub fn insert(&mut self, feed: &Url, id: &str) {
        self.add(Self::key(feed, id));
    }

    fn add(&mut self, key: String) {
        if self.set.insert(key.clone()) {
            self.order.push_back(key);
            self.dirty = true;
            self.trim();
        }
    }

    /// Forgets the oldest items past the limit.
    fn trim(&mut self) {
        while self.order.len() > self.limit {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
                self.dirty = true;
            }
        }
    }

    /// How many items are remembered.
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Whether none is.
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Writes the cache if it has changed and has a file.
    pub fn save(&mut self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if !self.dirty {
            return Ok(());
        }
        let stored = Stored {
            version: VERSION,
            seen: self.order.iter().cloned().collect(),
        };
        let data = serde_json::to_vec(&stored)?;
        write_atomically(path, &data).with_context(|| "cannot save the seen RSS items")?;
        self.dirty = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
