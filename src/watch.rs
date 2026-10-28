//! The watch directory: `.torrent` files, and `.magnet` files holding a
//! magnet link, dropped there are added to the session.
//!
//! The directory is polled every two seconds rather than watched through the
//! file system's notifications, which keeps it simple and works on network
//! shares; a few seconds of delay does not matter. A file is taken once its
//! size and modification time are the same on two polls in a row, so a
//! download a browser has not finished writing is left alone.
//!
//! What is taken is parsed first. A file that does not parse is renamed to
//! `<name>.invalid`, with a notice. Otherwise the torrent is added through
//! the manager's command channel and the file renamed to `<name>.added` (as
//! rTorrent does), or deleted with `delete`. A `.torrent` is first copied
//! into the session store (see `SessionStore::store_torrent`), and the
//! torrent added from the copy, so that the rename or the delete does not
//! take the torrent's file from under it, and the torrent survives a restart.
//! The store is the session's, or else the state directory's even when the
//! session is not kept; without any (no state directory) the torrent is
//! added from the file itself, which is then left where it is, so that it
//! can still be loaded.

use crate::magnet::{self, MagnetLink};
use crate::manager::Command;
use crate::session::SessionStore;
use crate::source::Source;
use crate::stats::SessionStats;
use crate::torrent;
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

/// How often the directory is looked at.
pub const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// How a file looked when it was seen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Stamp {
    len: u64,
    modified: Option<SystemTime>,
}

/// A file seen on the last poll.
#[derive(Debug, Clone, Copy)]
struct Seen {
    stamp: Stamp,
    /// Whether it was dealt with but could not be moved away, so it is not
    /// to be added again until it changes.
    taken: bool,
}

/// What a file holds.
enum Parsed {
    Magnet(Box<MagnetLink>),
    /// The info hash of a `.torrent`.
    Torrent([u8; 20]),
}

/// Watches one directory for torrents.
pub struct Watcher {
    dir: PathBuf,
    delete: bool,
    store: Option<SessionStore>,
    commands: mpsc::UnboundedSender<Command>,
    stats: Arc<SessionStats>,
    seen: HashMap<PathBuf, Seen>,
    /// Whether the last poll could not read the directory, so that the
    /// failure is logged once rather than every poll.
    unreadable: bool,
}

impl Watcher {
    /// A watcher of `dir` that sends the torrents it finds to `commands`,
    /// and tells the user of trouble through `stats`. Taken files are
    /// deleted rather than renamed if `delete`. `.torrent` files are copied
    /// into `store`, if there is one.
    pub fn new(
        dir: PathBuf,
        delete: bool,
        store: Option<SessionStore>,
        commands: mpsc::UnboundedSender<Command>,
        stats: Arc<SessionStats>,
    ) -> Self {
        Self {
            dir,
            delete,
            store,
            commands,
            stats,
            seen: HashMap::new(),
            unreadable: false,
        }
    }

    /// Polls every `POLL_INTERVAL` until `shutdown` resolves or the
    /// command channel closes, then drops the watcher, and with it its
    /// sender, so the manager's run can end.
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let mut tick = tokio::time::interval(POLL_INTERVAL);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                () = &mut shutdown => break,
                _ = tick.tick() => {
                    if !self.poll().await {
                        break;
                    }
                }
            }
        }
    }

    /// Looks at the directory once: takes the files that look as they did
    /// on the last poll. Returns false once the command channel is closed.
    pub async fn poll(&mut self) -> bool {
        let files = match self.list().await {
            Ok(files) => {
                self.unreadable = false;
                files
            }
            Err(e) => {
                if !self.unreadable {
                    warn!(
                        "cannot read the watch directory {}: {e}",
                        self.dir.display()
                    );
                }
                self.unreadable = true;
                self.seen.clear();
                return true;
            }
        };
        let mut now = HashMap::new();
        for (path, stamp) in files {
            let previous = self.seen.remove(&path).filter(|seen| seen.stamp == stamp);
            let mut taken = false;
            match previous {
                Some(seen) if seen.taken => taken = true,
                Some(_) => match self.take(&path).await {
                    None => return false,
                    Some(true) => continue,
                    Some(false) => taken = true,
                },
                None => {}
            }
            now.insert(path, Seen { stamp, taken });
        }
        self.seen = now;
        true
    }

    /// The files of the directory that may be torrents, and how they look.
    async fn list(&self) -> std::io::Result<Vec<(PathBuf, Stamp)>> {
        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if extension(&path).is_none() {
                continue;
            }
            // A file that vanishes meanwhile is just not there.
            let Ok(metadata) = tokio::fs::metadata(&path).await else {
                continue;
            };
            if metadata.is_file() {
                let stamp = Stamp {
                    len: metadata.len(),
                    modified: metadata.modified().ok(),
                };
                files.push((path, stamp));
            }
        }
        Ok(files)
    }

    /// Adds the torrent in `path` and moves the file away. Returns whether
    /// the file is gone from its place, or `None` if the command channel is
    /// closed.
    async fn take(&self, path: &Path) -> Option<bool> {
        let name = path
            .file_name()
            .map_or_else(String::new, |name| name.to_string_lossy().into_owned());
        let source = match parse(path).await {
            Ok(Parsed::Magnet(link)) => Source::Magnet(link),
            Ok(Parsed::Torrent(info_hash)) => Source::File(self.keep(path, info_hash).await),
            Err(e) => {
                warn!("{name}: {e:#}");
                self.stats.notice(format!("{name}: {e:#}, set aside"));
                return Some(self.move_away(path, "invalid").await);
            }
        };
        let in_place = matches!(&source, Source::File(file) if file == path);
        self.commands.send(Command::Add(source)).ok()?;
        info!("{name}: taken from the watch directory");
        if in_place {
            return Some(false);
        }
        Some(if self.delete {
            self.remove(path).await
        } else {
            self.move_away(path, "added").await
        })
    }

    /// Where the torrent of `path` can be loaded from once the file is
    /// moved: its copy in the store, or else `path` itself.
    async fn keep(&self, path: &Path, info_hash: [u8; 20]) -> PathBuf {
        let Some(store) = self.store.clone() else {
            return path.to_path_buf();
        };
        let from = path.to_path_buf();
        let copied = task::spawn_blocking(move || store.store_torrent(&from, info_hash)).await;
        match copied {
            Ok(Ok(copy)) => copy,
            Ok(Err(e)) => {
                warn!("cannot copy {} to the session store: {e:#}", path.display());
                path.to_path_buf()
            }
            Err(e) => {
                warn!("cannot copy {} to the session store: {e}", path.display());
                path.to_path_buf()
            }
        }
    }

    /// Renames `path` to the same with `.suffix` added. Returns whether
    /// that worked; if not, a notice says so.
    async fn move_away(&self, path: &Path, suffix: &str) -> bool {
        let mut to = path.as_os_str().to_owned();
        to.push(".");
        to.push(suffix);
        match tokio::fs::rename(path, PathBuf::from(to)).await {
            Ok(()) => true,
            Err(e) => self.cannot_move(path, &e),
        }
    }

    /// Deletes `path`, with the same result as `move_away`.
    async fn remove(&self, path: &Path) -> bool {
        match tokio::fs::remove_file(path).await {
            Ok(()) => true,
            Err(e) => self.cannot_move(path, &e),
        }
    }

    fn cannot_move(&self, path: &Path, error: &std::io::Error) -> bool {
        warn!("cannot move {} away: {error}", path.display());
        self.stats
            .notice(format!("cannot move {} away: {error}", path.display()));
        false
    }
}

/// The kinds of file the directory takes.
#[derive(PartialEq)]
enum Kind {
    Torrent,
    Magnet,
}

/// What kind of file `path` is by its name, if one the directory takes.
fn extension(path: &Path) -> Option<Kind> {
    let ext = path.extension().and_then(OsStr::to_str)?;
    if ext.eq_ignore_ascii_case("torrent") {
        Some(Kind::Torrent)
    } else if ext.eq_ignore_ascii_case("magnet") {
        Some(Kind::Magnet)
    } else {
        None
    }
}

/// Reads the torrent in `path`: a `.magnet` file holds a link on its first
/// line, anything else is a `.torrent`.
async fn parse(path: &Path) -> Result<Parsed> {
    if extension(path) != Some(Kind::Magnet) {
        let metainfo = torrent::load_metainfo(path).await?;
        return Ok(Parsed::Torrent(metainfo.info_hash));
    }
    let text = tokio::fs::read_to_string(path).await?;
    let first = text.trim_start_matches('\u{feff}').lines().next();
    let line = first.unwrap_or("").trim();
    if !magnet::is_magnet(line) {
        bail!("the first line is not a magnet link");
    }
    Ok(Parsed::Magnet(Box::new(MagnetLink::parse(line)?)))
}

#[cfg(test)]
mod tests;
