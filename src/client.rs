//! What the torrents of one session share: the settings, our peer id, the
//! listening port, the DHT node, the rate limits, the connection slots, the
//! hashing queue and the progress bars.

use crate::dht::Dht;
use crate::download::DownloadConfig;
use crate::listener::Listener;
use crate::peer;
use crate::rate_limit::RateLimits;
use anyhow::{Result, bail};
use indicatif::MultiProgress;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::{Instrument, Span, info, info_span, warn};

/// Runs any number of torrents at once, each through `download_file` or
/// `download_magnet`, sharing one listening port, one DHT node and one pair
/// of rate limits, and holding no more than `max_connections` peer
/// connections between them. Torrents check the data on disk one at a time.
/// The port and the DHT node are only opened once a torrent needs them.
pub struct Client {
    config: DownloadConfig,
    peer_id: [u8; 20],
    rate_limits: Arc<RateLimits>,
    slots: ConnectionSlots,
    /// One permit: held by the torrent hashing the data on disk.
    hashing: Semaphore,
    bars: MultiProgress,
    listener: OnceCell<Arc<Listener>>,
    /// `None` if the DHT is off or could not be started.
    dht: OnceCell<Option<Dht>>,
    /// Info hashes of the torrents running, so none runs twice.
    torrents: Mutex<HashSet<[u8; 20]>>,
}

impl Client {
    /// A client with the settings in `config`, drawing its progress bars in
    /// `bars`.
    pub fn new(config: DownloadConfig, bars: MultiProgress) -> Self {
        if config.max_download_rate > 0 || config.max_upload_rate > 0 {
            info!(
                download = config.max_download_rate,
                upload = config.max_upload_rate,
                "rate limits in KiB/s (0: unlimited)"
            );
        }
        let rate_limits = RateLimits::from_kib(config.max_download_rate, config.max_upload_rate);
        let slots = ConnectionSlots::new(config.max_connections);
        Self {
            config,
            peer_id: peer::generate_peer_id(),
            rate_limits: Arc::new(rate_limits),
            slots,
            hashing: Semaphore::new(1),
            bars,
            listener: OnceCell::new(),
            dht: OnceCell::new(),
            torrents: Mutex::new(HashSet::new()),
        }
    }

    pub fn config(&self) -> &DownloadConfig {
        &self.config
    }

    /// Our peer id, the same for every torrent.
    pub fn peer_id(&self) -> [u8; 20] {
        self.peer_id
    }

    /// How fast block data may move each way, across every peer of every
    /// torrent.
    pub fn rate_limits(&self) -> Arc<RateLimits> {
        Arc::clone(&self.rate_limits)
    }

    /// The peer connections left to open, across every torrent.
    pub fn slots(&self) -> ConnectionSlots {
        self.slots.clone()
    }

    /// Held by the torrent that is checking the data on disk, so that
    /// torrents check it one at a time (see `download::hash_pieces`).
    pub fn hashing(&self) -> &Semaphore {
        &self.hashing
    }

    pub fn bars(&self) -> &MultiProgress {
        &self.bars
    }

    /// Where peers connect to us, listening on the configured port (see
    /// `Listener::bind`) from the first call on.
    pub async fn listener(&self) -> &Arc<Listener> {
        let bind = Listener::bind(self.config.listen_port);
        self.listener
            .get_or_init(|| bind.instrument(session_span()))
            .await
    }

    /// Our DHT node, started on the first call (trying the listening port
    /// for UDP too), unless the DHT is off. A failure to start it is logged
    /// once, and the session goes on without it.
    pub async fn dht(&self) -> Option<Dht> {
        if !self.config.dht {
            return None;
        }
        let port = self.config.listen_port;
        let start = async move {
            Dht::bind(port)
                .await
                .inspect_err(|e| warn!("cannot start the DHT: {e:#}"))
                .ok()
        };
        self.dht
            .get_or_init(|| start.instrument(session_span()))
            .await
            .clone()
    }

    /// Marks the torrent with `info_hash` as running until the returned
    /// claim is dropped. Fails if it is running already.
    pub fn claim(&self, info_hash: [u8; 20]) -> Result<Claim<'_>> {
        if !self.lock_torrents().insert(info_hash) {
            bail!("this torrent is already being downloaded in this session");
        }
        Ok(Claim {
            client: self,
            info_hash,
        })
    }

    fn lock_torrents(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.torrents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A span for work done for every torrent, such as opening the listening
/// port, so its logs do not go under the torrent that happened to need it
/// first. Outside any torrent's span it is no span at all.
fn session_span() -> Span {
    if Span::current().is_none() {
        Span::none()
    } else {
        info_span!(parent: None, "session")
    }
}

/// The peer connections a session may have open at once, shared by all its
/// torrents. Every connection, whether we opened it or the peer did, holds a
/// `SlotPermit` until it is closed.
#[derive(Clone)]
pub struct ConnectionSlots {
    semaphore: Arc<Semaphore>,
    /// Notified whenever a permit comes back.
    freed: Arc<Notify>,
}

impl ConnectionSlots {
    /// `limit` slots.
    pub fn new(limit: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))),
            freed: Arc::new(Notify::new()),
        }
    }

    /// Takes a slot, or `None` if they are all in use.
    pub fn try_acquire(&self) -> Option<SlotPermit> {
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;
        Some(SlotPermit {
            permit: Some(permit),
            freed: Arc::clone(&self.freed),
        })
    }

    /// Resolves once a slot comes back after this call. Created before
    /// `try_acquire` is tried, it does not miss a slot freed in between.
    pub fn freed(&self) -> Notified<'_> {
        self.freed.notified()
    }
}

/// A slot in a `ConnectionSlots`, given back when dropped.
pub struct SlotPermit {
    /// Taken in `drop`, so that the slot is back before anyone is woken.
    permit: Option<OwnedSemaphorePermit>,
    freed: Arc<Notify>,
}

impl Drop for SlotPermit {
    fn drop(&mut self) {
        self.permit.take();
        self.freed.notify_waiters();
    }
}

/// A torrent running in a `Client`.
pub struct Claim<'a> {
    client: &'a Client,
    info_hash: [u8; 20],
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.client.lock_torrents().remove(&self.info_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::SeedLimits;

    fn client(dht: bool) -> Client {
        let config = DownloadConfig {
            output_dir: ".".into(),
            max_peers: 10,
            max_connections: 100,
            listen_port: 0,
            seed: SeedLimits::default(),
            dht,
            max_download_rate: 64,
            max_upload_rate: 0,
            resume_dir: None,
            recheck: false,
        };
        Client::new(config, MultiProgress::new())
    }

    #[test]
    fn a_torrent_runs_once_at_a_time() {
        let client = client(false);
        let claim = client.claim([1; 20]).unwrap();
        let _other = client.claim([2; 20]).unwrap();
        assert!(client.claim([1; 20]).is_err());
        drop(claim);
        assert!(client.claim([1; 20]).is_ok());
    }

    #[test]
    fn connection_slots_run_out_and_come_back() {
        let slots = ConnectionSlots::new(2);
        let first = slots.try_acquire().unwrap();
        let _second = slots.try_acquire().unwrap();
        assert!(slots.try_acquire().is_none());
        drop(first);
        assert!(slots.try_acquire().is_some());
    }

    #[test]
    fn the_client_hands_out_one_set_of_slots() {
        let client = client(false);
        let held: Vec<_> = (0..100)
            .map(|_| client.slots().try_acquire().unwrap())
            .collect();
        assert!(client.slots().try_acquire().is_none());
        drop(held);
        assert!(client.slots().try_acquire().is_some());
    }

    #[tokio::test]
    async fn freed_wakes_when_a_slot_comes_back() {
        let slots = ConnectionSlots::new(1);
        let permit = slots.try_acquire().unwrap();
        let mut freed = std::pin::pin!(slots.freed());
        // Nothing came back yet.
        assert!(futures_now_or_never(freed.as_mut()).is_none());
        drop(permit);
        freed.await;
        assert!(slots.try_acquire().is_some());
    }

    /// Polls `future` once.
    fn futures_now_or_never<F: Future>(future: F) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        match future.as_mut().poll(&mut cx) {
            std::task::Poll::Ready(output) => Some(output),
            std::task::Poll::Pending => None,
        }
    }

    #[tokio::test]
    async fn torrents_share_one_listener_and_one_rate_limit() {
        let client = client(false);
        let first = client.listener().await;
        assert!(Arc::ptr_eq(first, client.listener().await));
        assert!(first.port().is_some());
        assert!(Arc::ptr_eq(&client.rate_limits(), &client.rate_limits()));
        assert!(client.rate_limits().download.is_limited());
        assert!(!client.rate_limits().upload.is_limited());
    }

    #[tokio::test]
    async fn no_dht_when_it_is_off() {
        assert!(client(false).dht().await.is_none());
    }
}
