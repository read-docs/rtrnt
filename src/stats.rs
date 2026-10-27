//! Live numbers about a session and its torrents, for the terminal UI.
//!
//! The engine publishes cheap counters here (relaxed atomics, and short
//! locks taken at most once per piece or per announce, never per block),
//! and a reader pulls them on its own schedule. Rates and history are the
//! reader's business: nothing here keeps time beyond a few timestamps.

use crate::client::ConnectionSlots;
use crate::dht::Dht;
use crate::rate_limit::RateLimits;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Instant;

mod peer;
mod torrent;

pub use peer::{PeerFlags, PeerStats};
pub(crate) use torrent::PieceSource;
pub use torrent::{
    Layout, PeerSource, PieceSnapshot, Previous, TorrentState, TorrentStats, TrackerResult,
    TrackerStatus,
};

/// What every torrent of a `Client` shares, and the torrents themselves.
pub struct SessionStats {
    torrents: Mutex<Vec<Arc<TorrentStats>>>,
    /// Set once the DHT node is started.
    dht: OnceLock<Dht>,
    /// Set once the listening port is open.
    port: OnceLock<Option<u16>>,
    rate_limits: Arc<RateLimits>,
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
            dht: OnceLock::new(),
            port: OnceLock::new(),
            rate_limits,
            slots,
            notice: Mutex::new(None),
        }
    }

    /// The torrents of the session, in the order they were added.
    pub fn torrents(&self) -> Vec<Arc<TorrentStats>> {
        lock(&self.torrents).clone()
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

    pub(crate) fn set_dht(&self, dht: Dht) {
        let _ = self.dht.set(dht);
    }

    pub(crate) fn set_port(&self, port: Option<u16>) {
        let _ = self.port.set(port);
    }

    /// Nodes in the DHT routing table, or `None` while no node runs.
    pub fn dht_nodes(&self) -> Option<usize> {
        self.dht.get().map(Dht::node_count)
    }

    /// Where we accept peers, once known.
    pub fn port(&self) -> Option<u16> {
        self.port.get().copied().flatten()
    }

    /// Download and upload limits in KiB/s, 0 for none, as they are now.
    pub fn rate_limits(&self) -> (u64, u64) {
        self.rate_limits.kib()
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
