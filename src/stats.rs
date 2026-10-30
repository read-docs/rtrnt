//! Live numbers about a session and its torrents, for the terminal UI.
//!
//! The engine publishes cheap counters here (relaxed atomics, and short
//! locks taken at most once per piece or per announce, never per block),
//! and a reader pulls them on its own schedule. Rates and history are the
//! reader's business: nothing here keeps time beyond a few timestamps.

use crate::client::ConnectionSlots;
use crate::dht::Dht;
use crate::portmap::{Portmapper, Status as PortmapStatus};
use crate::rate_limit::RateLimits;
use crate::torrent_limits::{TorrentLimits, TorrentSettings};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Instant;

mod peer;
mod torrent;

pub use peer::{PeerFlags, PeerStats, WebSeedState, WebSeedStats};
pub(crate) use torrent::PieceSource;
pub use torrent::{
    Layout, PeerSource, PieceSnapshot, Previous, TorrentState, TorrentStats, TrackerResult,
    TrackerStatus,
};

/// What every torrent of a `Client` shares, and the torrents themselves.
pub struct SessionStats {
    torrents: Mutex<Vec<Arc<TorrentStats>>>,
    /// What each torrent overrides of the session's settings, by info hash.
    limits: Mutex<HashMap<[u8; 20], Arc<TorrentLimits>>>,
    /// The tags of each torrent that has any (see `tags`), by info hash.
    tags: Mutex<HashMap<[u8; 20], Vec<String>>>,
    /// Set once the DHT node is started.
    dht: OnceLock<Dht>,
    /// Set once the client has one.
    portmap: OnceLock<Portmapper>,
    /// Set once the listening port is open.
    port: OnceLock<Option<u16>>,
    /// The directory watched for torrents, if there is one.
    watch_dir: OnceLock<PathBuf>,
    rate_limits: Arc<RateLimits>,
    /// Whether the alternative speed limits are on (see `altspeed`).
    alt_speed: AtomicBool,
    slots: ConnectionSlots,
    /// The latest notice, and when it came.
    notice: Mutex<Option<(Instant, String)>>,
}

impl SessionStats {
    /// Stats for a session with the rate limits `rate_limits` whose
    /// connections hold `slots`.
    pub fn new(rate_limits: Arc<RateLimits>, slots: ConnectionSlots) -> Self {
        Self {
            torrents: Mutex::new(Vec::new()),
            limits: Mutex::new(HashMap::new()),
            tags: Mutex::new(HashMap::new()),
            dht: OnceLock::new(),
            portmap: OnceLock::new(),
            port: OnceLock::new(),
            watch_dir: OnceLock::new(),
            rate_limits,
            alt_speed: AtomicBool::new(false),
            slots,
            notice: Mutex::new(None),
        }
    }

    /// The torrents of the session, in the order they were added.
    pub fn torrents(&self) -> Vec<Arc<TorrentStats>> {
        lock(&self.torrents).clone()
    }

    /// The settings of the torrent with `info_hash`, which it keeps through
    /// pauses and runs; made, with nothing overridden, if it has none yet.
    pub fn torrent_limits(&self, info_hash: [u8; 20]) -> Arc<TorrentLimits> {
        Arc::clone(lock(&self.limits).entry(info_hash).or_default())
    }

    /// The settings of the torrent with `info_hash`, none overridden if it
    /// has none.
    pub fn torrent_settings(&self, info_hash: [u8; 20]) -> TorrentSettings {
        lock(&self.limits)
            .get(&info_hash)
            .map(|limits| limits.settings())
            .unwrap_or_default()
    }

    /// The settings that apply to the torrent with `info_hash`, its own over
    /// its tags', none overridden if it has none.
    pub fn effective_settings(&self, info_hash: [u8; 20]) -> TorrentSettings {
        lock(&self.limits)
            .get(&info_hash)
            .map(|limits| limits.effective())
            .unwrap_or_default()
    }

    /// Drops the settings and tags of a torrent removed from the session.
    pub fn forget_torrent_limits(&self, info_hash: [u8; 20]) {
        lock(&self.limits).remove(&info_hash);
        lock(&self.tags).remove(&info_hash);
    }

    /// The tags of the torrent with `info_hash`, none if it has none.
    pub fn torrent_tags(&self, info_hash: [u8; 20]) -> Vec<String> {
        lock(&self.tags)
            .get(&info_hash)
            .cloned()
            .unwrap_or_default()
    }

    /// Gives the torrent with `info_hash` these tags, in place of its
    /// earlier ones.
    pub fn set_torrent_tags(&self, info_hash: [u8; 20], tags: Vec<String>) {
        let mut all = lock(&self.tags);
        if tags.is_empty() {
            all.remove(&info_hash);
        } else {
            all.insert(info_hash, tags);
        }
    }

    /// Adds a torrent called `name` (until its metadata says otherwise),
    /// saved under `save_path`.
    ///
    /// A torrent run again, after a pause or once it ended, takes the place
    /// of its earlier run's stats, so it keeps its row, when it was added,
    /// and what the earlier runs downloaded and uploaded and how long they
    /// ran. One with the same info hash still running is left alone.
    pub fn add_torrent(
        &self,
        info_hash: [u8; 20],
        name: &str,
        save_path: &Path,
    ) -> Arc<TorrentStats> {
        let mut torrents = lock(&self.torrents);
        let earlier = torrents
            .iter_mut()
            .find(|torrent| torrent.info_hash() == info_hash && torrent.finished_at().is_some());
        match earlier {
            Some(earlier) => {
                let previous = earlier.so_far(Instant::now());
                let torrent = Arc::new(TorrentStats::again(
                    info_hash,
                    name,
                    save_path,
                    earlier.added(),
                    previous,
                ));
                torrent.inherit_sequential(earlier);
                *earlier = Arc::clone(&torrent);
                torrent
            }
            None => {
                let torrent = Arc::new(TorrentStats::new(info_hash, name, save_path));
                torrents.push(Arc::clone(&torrent));
                torrent
            }
        }
    }

    /// Puts the torrents in the order of `info_hashes`, which is the
    /// queue's. One not listed goes after the listed ones, in the order it
    /// had.
    pub fn arrange(&self, info_hashes: &[[u8; 20]]) {
        let mut torrents = lock(&self.torrents);
        let place = |torrent: &Arc<TorrentStats>| {
            info_hashes
                .iter()
                .position(|hash| *hash == torrent.info_hash())
                .unwrap_or(usize::MAX)
        };
        torrents.sort_by_key(place);
    }

    /// Drops the row of `torrent`, removed from the session.
    pub fn remove_torrent(&self, torrent: &Arc<TorrentStats>) {
        lock(&self.torrents).retain(|kept| !Arc::ptr_eq(kept, torrent));
    }

    pub(crate) fn set_dht(&self, dht: Dht) {
        let _ = self.dht.set(dht);
    }

    pub(crate) fn set_portmap(&self, portmap: Portmapper) {
        let _ = self.portmap.set(portmap);
    }

    pub(crate) fn set_port(&self, port: Option<u16>) {
        let _ = self.port.set(port);
    }

    /// Records that `dir` is watched for torrents (see `watch`).
    pub fn set_watch_dir(&self, dir: PathBuf) {
        let _ = self.watch_dir.set(dir);
    }

    /// The directory watched for torrents, if there is one.
    pub fn watch_dir(&self) -> Option<PathBuf> {
        self.watch_dir.get().cloned()
    }

    /// Nodes in the DHT routing table, or `None` while no node runs.
    pub fn dht_nodes(&self) -> Option<usize> {
        self.dht.get().map(Dht::node_count)
    }

    /// How mapping the listening port on the router is going.
    pub fn portmap(&self) -> PortmapStatus {
        self.portmap
            .get()
            .map(Portmapper::status)
            .unwrap_or_default()
    }

    /// Where we accept peers, once known.
    pub fn port(&self) -> Option<u16> {
        self.port.get().copied().flatten()
    }

    /// Download and upload limits in KiB/s, 0 for none, as they are now.
    pub fn rate_limits(&self) -> (u64, u64) {
        self.rate_limits.kib()
    }

    /// Records whether the alternative speed limits are on.
    pub fn set_alt_speed(&self, on: bool) {
        self.alt_speed.store(on, Ordering::Relaxed);
    }

    /// Whether the alternative speed limits are on.
    pub fn alt_speed(&self) -> bool {
        self.alt_speed.load(Ordering::Relaxed)
    }

    /// Peer connections open now, across every torrent.
    pub fn connections(&self) -> usize {
        self.slots.in_use()
    }

    /// Most peer connections open at once.
    pub fn max_connections(&self) -> usize {
        self.slots.limit()
    }

    /// Tells the user `text`: something the UI should show, such as the
    /// result of a command, that it cannot wait for. A later notice
    /// replaces it.
    pub fn notice(&self, text: impl Into<String>) {
        *lock(&self.notice) = Some((Instant::now(), text.into()));
    }

    /// The latest notice, and when it came.
    pub fn last_notice(&self) -> Option<(Instant, String)> {
        lock(&self.notice).clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
