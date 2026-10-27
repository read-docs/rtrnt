//! What the torrents of one session share: the settings, our peer id, the
//! listening port, the DHT node, the rate limits, the connection slots, the
//! hashing queue, the files the torrents write and the progress bars.

use crate::dht::Dht;
use crate::download::DownloadConfig;
use crate::listener::Listener;
use crate::peer;
use crate::rate_limit::RateLimits;
use anyhow::{Result, bail};
use indicatif::MultiProgress;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::futures::Notified;
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit, Semaphore};
use tracing::{Instrument, Span, info, info_span, warn};

/// The info hash and name of the torrent that writes a file.
type FileOwner = ([u8; 20], Arc<str>);

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
    /// The files the torrents running write, keyed as `Storage::paths`
    /// gives them, with the info hash and name of the torrent that owns each.
    files: Mutex<HashMap<PathBuf, FileOwner>>,
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
            files: Mutex::new(HashMap::new()),
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
            files: None,
        })
    }

    /// Marks `paths` (see `Storage::paths`) as written by the torrent with
    /// `info_hash` and `name` until the returned claim is dropped. Fails,
    /// claiming nothing, if another torrent holds any of them.
    pub fn claim_files(
        &self,
        info_hash: [u8; 20],
        name: &str,
        paths: &[PathBuf],
    ) -> Result<FileClaim<'_>> {
        let mut files = self.lock_files();
        for path in paths {
            if let Some((owner, other)) = files.get(path)
                && *owner != info_hash
            {
                bail!(
                    "{} is also written by the torrent {other}, which is running in this session",
                    path.display()
                );
            }
        }
        let name: Arc<str> = name.into();
        for path in paths {
            files.insert(path.clone(), (info_hash, Arc::clone(&name)));
        }
        Ok(FileClaim {
            client: self,
            info_hash,
            paths: paths.to_vec(),
        })
    }

    fn lock_torrents(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.torrents.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_files(&self) -> MutexGuard<'_, HashMap<PathBuf, FileOwner>> {
        self.files.lock().unwrap_or_else(PoisonError::into_inner)
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
    /// Set once the torrent's layout is known.
    files: Option<FileClaim<'a>>,
}

impl Claim<'_> {
    /// Claims the files of this torrent, called `name`, for as long as the
    /// claim lasts (see `Client::claim_files`).
    pub fn claim_files(&mut self, name: &str, paths: &[PathBuf]) -> Result<()> {
        self.files = Some(self.client.claim_files(self.info_hash, name, paths)?);
        Ok(())
    }
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.client.lock_torrents().remove(&self.info_hash);
    }
}

/// The files a torrent writes, free for others once this is dropped.
pub struct FileClaim<'a> {
    client: &'a Client,
    info_hash: [u8; 20],
    paths: Vec<PathBuf>,
}

impl Drop for FileClaim<'_> {
    fn drop(&mut self) {
        let mut files = self.client.lock_files();
        for path in &self.paths {
            if files
                .get(path)
                .is_some_and(|(owner, _)| *owner == self.info_hash)
            {
                files.remove(path);
            }
        }
    }
}

#[cfg(test)]
mod tests;
