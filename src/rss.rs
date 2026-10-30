//! RSS auto-download: feeds named in the config file are polled, and the
//! torrents of the items whose titles match are added to the session, as
//! the watch directory and the UI's `a` key add them.
//!
//! One task (`Poller`) goes through the feeds, each when its interval is
//! up, one at a time; a feed that fails is asked again after a doubling
//! delay, up to six hours, until it answers. For each item whose title the
//! feed's filter accepts and that is not in the seen cache (`Seen`):
//!
//! - a magnet link is added as it is;
//! - a `.torrent` URL is downloaded, size-capped, checked to be a torrent
//!   and copied into the session's store, like the watch directory's
//!   files, and added from the copy;
//! - an item that points to nothing usable, or whose torrent is not one,
//!   is logged and remembered as seen, so it is not tried over and over.
//!   One that could not be downloaded because the server or the network
//!   failed is tried again at the next poll.
//!
//! Every add and every feed error is logged, and shown as a notice in the
//! UI. The cache is written after each poll of a feed, so a restart adds
//! nothing twice.

mod feed;
mod fetch;
mod seen;
mod subscription;

pub use feed::{FeedItem, Target, parse_feed};
pub use fetch::{Fetch, FetchError, HttpFetcher};
pub use seen::{MAX_SEEN, Seen};
pub use subscription::{
    DEFAULT_INTERVAL_MINUTES, Filter, MIN_INTERVAL_MINUTES, Subscription, to_toml,
};

use crate::magnet::{MagnetLink, hex};
use crate::manager::Command;
use crate::session::SessionStore;
use crate::source::Source;
use crate::state::{default_state_dir, write_atomically};
use crate::stats::SessionStats;
use crate::torrent;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tokio::time::Instant;
use tracing::{debug, info, warn};
use url::Url;

/// The largest feed document taken.
pub const FEED_LIMIT: usize = 10 * 1024 * 1024;
/// The largest `.torrent` taken.
pub const TORRENT_LIMIT: usize = 10 * 1024 * 1024;
/// The longest wait before asking a failing feed again, unless its
/// interval is longer.
pub const MAX_BACKOFF: Duration = Duration::from_secs(6 * 60 * 60);

/// Where the seen cache is kept: in the state directory, or `None` if
/// there is none.
pub fn seen_path() -> Option<PathBuf> {
    default_state_dir().map(|dir| dir.join("rss-seen.json"))
}

/// How long to wait before the next poll of a feed with this `interval`
/// that has failed `failures` times in a row: the interval, doubled for each
/// failure after the first, up to `MAX_BACKOFF` (or the interval itself, if
/// that is longer).
pub fn backoff(interval: Duration, failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    interval
        .saturating_mul(1 << doublings)
        .min(MAX_BACKOFF.max(interval))
}

/// A feed and when it is next due.
struct Slot {
    subscription: Subscription,
    due: Instant,
    /// Polls that failed in a row.
    failures: u32,
}

/// Polls the feeds and adds what they offer.
pub struct Poller<F> {
    slots: Vec<Slot>,
    fetcher: F,
    /// Where downloaded `.torrent` files are kept; `None` if there is no
    /// such place, in which case only magnet links can be taken.
    store: Option<SessionStore>,
    seen: Seen,
    commands: mpsc::UnboundedSender<Command>,
    stats: Arc<SessionStats>,
}

impl<F: Fetch> Poller<F> {
    /// A poller of `subscriptions`, all due at once, that gets documents
    /// with `fetcher`, keeps the `.torrent` files it downloads in `store`,
    /// remembers items in `seen`, sends the torrents to `commands` and
    /// tells the user of trouble through `stats`.
    pub fn new(
        subscriptions: Vec<Subscription>,
        fetcher: F,
        store: Option<SessionStore>,
        seen: Seen,
        commands: mpsc::UnboundedSender<Command>,
        stats: Arc<SessionStats>,
    ) -> Self {
        let now = Instant::now();
        Self {
            slots: subscriptions
                .into_iter()
                .map(|subscription| Slot {
                    subscription,
                    due: now,
                    failures: 0,
                })
                .collect(),
            fetcher,
            store,
            seen,
            commands,
            stats,
        }
    }

    /// Polls each feed when it is due until `shutdown` resolves or the
    /// command channel closes, then drops the poller, and with it its
    /// sender, so the manager's run can end.
    pub async fn run(mut self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            let Some(index) = self.next_due() else {
                // No feeds: nothing to do but wait to be told to stop.
                shutdown.await;
                return;
            };
            let due = self.slots[index].due;
            let open = tokio::select! {
                () = &mut shutdown => return,
                () = tokio::time::sleep_until(due) => {
                    tokio::select! {
                        () = &mut shutdown => return,
                        open = self.poll(index) => open,
                    }
                }
            };
            if !open {
                return;
            }
        }
    }

    /// The feed that is due first.
    fn next_due(&self) -> Option<usize> {
        (0..self.slots.len()).min_by_key(|&index| self.slots[index].due)
    }

    /// Polls feed number `index` now and sets when it is next due. Returns
    /// false once the command channel is closed.
    async fn poll(&mut self, index: usize) -> bool {
        let subscription = self.slots[index].subscription.clone();
        let url = &subscription.url;
        let host = host_of(url);
        let items = match self.read_feed(url).await {
            Ok(items) => items,
            Err(e) => {
                let slot = &mut self.slots[index];
                slot.failures = slot.failures.saturating_add(1);
                let wait = backoff(subscription.interval, slot.failures);
                slot.due = Instant::now() + wait;
                warn!(feed = %host, "cannot read the feed, trying again in {}s: {e}", wait.as_secs());
                // One notice, not one per failed poll.
                if slot.failures == 1 {
                    self.stats.notice(format!("rss: cannot read {host}: {e}"));
                }
                return true;
            }
        };
        let slot = &mut self.slots[index];
        if slot.failures > 0 {
            info!(feed = %host, "the feed answers again");
        }
        slot.failures = 0;
        slot.due = Instant::now() + subscription.interval;
        let open = self.take(&subscription, items).await;
        if let Err(e) = self.seen.save() {
            warn!("{e:#}");
        }
        open
    }

    /// Gets and reads the feed at `url`.
    async fn read_feed(&self, url: &Url) -> Result<Vec<FeedItem>, String> {
        let data = self
            .fetcher
            .get(url, FEED_LIMIT)
            .await
            .map_err(|e| e.to_string())?;
        parse_feed(&data, Some(url)).map_err(|e| format!("{e:#}"))
    }

    /// Adds the items `subscription` takes that were not taken before.
    /// Returns false once the command channel is closed.
    async fn take(&mut self, subscription: &Subscription, items: Vec<FeedItem>) -> bool {
        let url = &subscription.url;
        let host = host_of(url);
        for item in items {
            if !subscription.filter.accepts(&item.title) {
                continue;
            }
            let Some(id) = item.id.as_deref() else {
                debug!(feed = %host, title = %item.title, "an item with no id or link is skipped");
                continue;
            };
            if self.seen.contains(url, id) {
                continue;
            }
            match self.source_of(&item).await {
                Ok(source) => {
                    let command = if let Some(dir) = &subscription.download_dir {
                        Command::AddIn {
                            source,
                            paused: subscription.paused,
                            dir: dir.clone(),
                        }
                    } else if subscription.paused {
                        Command::AddPaused(source)
                    } else {
                        Command::Add(source)
                    };
                    if self.commands.send(command).is_err() {
                        return false;
                    }
                    self.seen.insert(url, id);
                    info!(feed = %host, title = %item.title, "taken from the feed");
                    self.stats.notice(format!("rss: added {}", item.title));
                }
                Err(e) => {
                    warn!(feed = %host, title = %item.title, "cannot add: {e}");
                    // A failure that may pass is tried again at the next
                    // poll; the rest is not worth trying again.
                    if !e.retry {
                        self.seen.insert(url, id);
                        self.stats
                            .notice(format!("rss: cannot add {}: {e}", item.title));
                    }
                }
            }
        }
        true
    }

    /// What to add for `item`: its magnet link, or its `.torrent` fetched
    /// and kept in the store.
    async fn source_of(&self, item: &FeedItem) -> Result<Source, FetchError> {
        match &item.target {
            None => Err(FetchError::new("the item has no torrent link", false)),
            Some(Target::Magnet(uri)) => MagnetLink::parse(uri)
                .map(|link| Source::Magnet(Box::new(link)))
                .map_err(|e| FetchError::new(format!("{e:#}"), false)),
            Some(Target::Torrent(url)) => {
                let Some(store) = self.store.clone() else {
                    return Err(FetchError::new(
                        "there is no state directory to keep the .torrent in",
                        false,
                    ));
                };
                let data = self.fetcher.get(url, TORRENT_LIMIT).await?;
                let metainfo = torrent::parse_metainfo(&data)
                    .map_err(|e| FetchError::new(format!("{e:#}"), false))?;
                let path = store.torrent_path(metainfo.info_hash);
                let kept = {
                    let path = path.clone();
                    task::spawn_blocking(move || write_atomically(&path, &data)).await
                };
                match kept {
                    Ok(Ok(())) => Ok(Source::File(path)),
                    Ok(Err(e)) => Err(FetchError::new(
                        format!("cannot keep {}: {e:#}", hex(&metainfo.info_hash)),
                        true,
                    )),
                    Err(e) => Err(FetchError::new(e.to_string(), true)),
                }
            }
        }
    }
}

/// The host of `url`, which is all of it that logs and notices show: the
/// path and query may hold a key (private trackers' feeds do).
fn host_of(url: &Url) -> String {
    url.host_str().unwrap_or("the feed").to_string()
}

#[cfg(test)]
mod tests;
