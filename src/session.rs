//! The session file: the torrents the terminal UI had, in the table's order,
//! and the rate limits set from it, so that the next run starts where this
//! one left off.
//!
//! It is `session` in the state directory (see `state::state_dir`), bencoded
//! and replaced atomically. Each torrent is its info hash, its source and
//! whether it was paused. The source is a magnet URI, or the path of a copy
//! of the `.torrent` in the `torrents` directory next to the file, made when
//! the torrent was added, so a `.torrent` the user moves or deletes later
//! still loads.

use crate::magnet::MagnetLink;
use crate::source::Source;
use crate::state::write_atomically;
use crate::torrent;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use std::path::{Path, PathBuf};
use tracing::warn;

/// The format of the file, bumped when it changes incompatibly.
const VERSION: i64 = 1;

/// One torrent of a saved session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedTorrent {
    pub info_hash: [u8; 20],
    /// A magnet link, or the copy of the `.torrent` in the store.
    pub source: Source,
    /// Whether the torrent was paused, rather than running or finished.
    pub paused: bool,
}

/// Download and upload limits in KiB/s, 0 for none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedLimits {
    pub download: u64,
    pub upload: u64,
}

/// What the session file holds.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SavedSession {
    /// In the order of the torrent table.
    pub torrents: Vec<SavedTorrent>,
    /// The limits set from the UI, if any ever was.
    pub rate_limits: Option<SavedLimits>,
}

/// The file as it is written.
#[derive(Serialize, Deserialize)]
struct Stored {
    version: i64,
    torrents: Vec<StoredTorrent>,
    #[serde(
        rename = "rate limits",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    rate_limits: Option<SavedLimits>,
}

#[derive(Serialize, Deserialize)]
struct StoredTorrent {
    #[serde(rename = "info hash")]
    info_hash: ByteBuf,
    /// A magnet URI, or a path.
    source: String,
    /// 1 if paused (bencode has no booleans).
    paused: i64,
}

/// Just the version, read first so that a file of another version is
/// reported as such rather than as malformed.
#[derive(Deserialize)]
struct Versioned {
    version: i64,
}

impl SavedSession {
    /// Encodes the session as the file holds it. A torrent whose copy has
    /// a path that is not UTF-8 is left out, with a warning.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let torrents = self
            .torrents
            .iter()
            .filter_map(|torrent| {
                let source = match &torrent.source {
                    Source::Magnet(link) => link.to_uri(),
                    Source::File(path) => match path.to_str() {
                        Some(path) => path.to_string(),
                        None => {
                            warn!(path = %path.display(), "not saving a torrent whose path is not UTF-8");
                            return None;
                        }
                    },
                };
                Some(StoredTorrent {
                    info_hash: ByteBuf::from(torrent.info_hash.to_vec()),
                    source,
                    paused: i64::from(torrent.paused),
                })
            })
            .collect();
        let stored = Stored {
            version: VERSION,
            torrents,
            rate_limits: self.rate_limits,
        };
        Ok(serde_bencode::to_bytes(&stored)?)
    }

    /// Reads what `encode` wrote. A torrent whose entry makes no sense is
    /// left out, with a warning; a file of another version, or one that is
    /// not a session file, is an error.
    pub fn decode(data: &[u8]) -> Result<Self> {
        let Versioned { version } =
            serde_bencode::from_bytes(data).context("not a session file")?;
        if version != VERSION {
            bail!("session file version {version}, this program reads {VERSION}");
        }
        let stored: Stored = serde_bencode::from_bytes(data).context("malformed session file")?;
        let torrents = stored
            .torrents
            .into_iter()
            .filter_map(|entry| {
                let Ok(info_hash) = <[u8; 20]>::try_from(entry.info_hash.as_slice()) else {
                    warn!("skipping a torrent of the session file with a bad info hash");
                    return None;
                };
                let source = if crate::magnet::is_magnet(&entry.source) {
                    match MagnetLink::parse(&entry.source) {
                        Ok(link) => Source::Magnet(Box::new(link)),
                        Err(e) => {
                            warn!("skipping a torrent of the session file: {e:#}");
                            return None;
                        }
                    }
                } else {
                    Source::File(PathBuf::from(entry.source))
                };
                Some(SavedTorrent {
                    info_hash,
                    source,
                    paused: entry.paused != 0,
                })
            })
            .collect();
        Ok(Self {
            torrents,
            rate_limits: stored.rate_limits,
        })
    }
}

/// Where the session file and the copies of `.torrent` files are, and how
/// they are read and written. Tests, and anyone who wants another
/// directory, give their own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStore {
    dir: PathBuf,
}

impl SessionStore {
    /// A store in `dir`, which is made when something is first written.
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// The store in the state directory (see `state::default_state_dir`),
    /// or `None` if that is not known.
    pub fn in_state_dir() -> Option<Self> {
        crate::state::default_state_dir().map(Self::new)
    }

    /// The session file.
    pub fn path(&self) -> PathBuf {
        self.dir.join("session")
    }

    /// The directory of the copied `.torrent` files.
    pub fn torrents_dir(&self) -> PathBuf {
        self.dir.join("torrents")
    }

    /// Where the copy of the `.torrent` of the torrent with `info_hash` is.
    pub fn torrent_path(&self, info_hash: [u8; 20]) -> PathBuf {
        let name: String = info_hash.iter().map(|b| format!("{b:02x}")).collect();
        self.torrents_dir().join(format!("{name}.torrent"))
    }

    /// Whether `path` is the copy of a torrent in this store.
    pub fn holds(&self, path: &Path) -> bool {
        path.parent() == Some(self.torrents_dir().as_path())
    }

    /// Reads the session file; a missing one is an empty session.
    pub fn load(&self) -> Result<SavedSession> {
        let path = self.path();
        let data = match std::fs::read(&path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(SavedSession::default());
            }
            Err(e) => return Err(e).with_context(|| format!("cannot read {}", path.display())),
        };
        SavedSession::decode(&data).with_context(|| format!("cannot use {}", path.display()))
    }

    /// Writes the session file, replacing the old one atomically.
    pub fn save(&self, session: &SavedSession) -> Result<()> {
        write_atomically(&self.path(), &session.encode()?)
    }

    /// Moves a session file that cannot be used to `session.bad`, so that
    /// the next save does not destroy it. Best effort.
    pub fn set_aside(&self) {
        let path = self.path();
        let mut bad = path.as_os_str().to_owned();
        bad.push(".bad");
        if let Err(e) = std::fs::rename(&path, PathBuf::from(bad)) {
            warn!("cannot set the session file aside: {e}");
        }
    }

    /// Copies the `.torrent` at `from`, which is the torrent with
    /// `info_hash`, into the store, and returns the path of the copy. The
    /// copy is written atomically; one that is there already is replaced. A
    /// path that is the copy itself is returned as it is. This is what the
    /// watch directory uses to keep a torrent it renames or deletes.
    pub fn store_torrent(&self, from: &Path, info_hash: [u8; 20]) -> Result<PathBuf> {
        let to = self.torrent_path(info_hash);
        if from == to {
            return Ok(to);
        }
        let data =
            std::fs::read(from).with_context(|| format!("failed to read {}", from.display()))?;
        write_atomically(&to, &data)?;
        Ok(to)
    }

    /// Deletes the copy of the torrent with `info_hash`, if there is one.
    pub fn forget_torrent(&self, info_hash: [u8; 20]) -> Result<()> {
        let path = self.torrent_path(info_hash);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("failed to delete {}", path.display())),
        }
    }
}

/// The info hash of `source`, reading a `.torrent` file for it; `None` if
/// that does not load.
pub async fn info_hash_of(source: &Source) -> Option<[u8; 20]> {
    match source {
        Source::Magnet(link) => Some(link.info_hash),
        Source::File(path) => torrent::load_metainfo(path)
            .await
            .ok()
            .map(|metainfo| metainfo.info_hash),
    }
}

/// Merges the torrents given on the command line into the saved ones:
/// `saved` first, then the command line's, each with its info hash if it
/// has one, leaving out one the session has already. One the session has
/// paused is resumed. Returns the saved torrents to restore and the
/// command-line sources still to start.
pub fn merge(
    mut saved: Vec<SavedTorrent>,
    cli: Vec<(Source, Option<[u8; 20]>)>,
) -> (Vec<SavedTorrent>, Vec<Source>) {
    let mut start = Vec::new();
    for (source, info_hash) in cli {
        let known = info_hash.and_then(|hash| saved.iter_mut().find(|t| t.info_hash == hash));
        match known {
            Some(torrent) => torrent.paused = false,
            None => start.push(source),
        }
    }
    (saved, start)
}

#[cfg(test)]
mod tests;
