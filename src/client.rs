//! What the torrents of one session share: the settings, our peer id, the
//! listening port, the DHT node, the rate limits, the connection slots, the
//! hashing queue, the files the torrents write and the progress bars.

use crate::blocklist::Blocklist;
use crate::dht::Dht;
use crate::download::DownloadConfig;
use crate::listener::Listener;
use crate::lsd::{Lsd, LsdPeers};
use crate::peer;
use crate::portmap::{Portmapper, Protocol, Status};
use crate::rate_limit::RateLimits;
use crate::stats::SessionStats;
use anyhow::{Result, bail};
use indicatif::MultiProgress;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
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
    utp: OnceCell<Option<Arc<crate::utp::Endpoint>>>,
    /// Keeps the listening ports mapped on the router.
    portmap: Portmapper,
    /// `None` if the DHT is off or could not be started.
    dht: OnceCell<Option<Dht>>,
    /// `None` if LSD is off or could not be started.
    lsd: OnceCell<Option<Lsd>>,
    /// Info hashes of the torrents running, so none runs twice.
    torrents: Mutex<HashSet<[u8; 20]>>,
    /// The files the torrents running write, keyed as `Storage::paths`
    /// gives them, with the info hash and name of the torrent that owns each.
    files: Mutex<HashMap<PathBuf, FileOwner>>,
    stats: Arc<SessionStats>,
    /// Peers in these ranges are neither connected to nor accepted.
    blocklist: Arc<Blocklist>,
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
        let rate_limits = Arc::new(RateLimits::from_kib(
            config.max_download_rate,
            config.max_upload_rate,
        ));
        let slots = ConnectionSlots::new(config.max_connections);
        let stats = SessionStats::new(Arc::clone(&rate_limits), slots.clone());
        let portmap = Portmapper::new(config.portmap && !crate::network::global().restricted());
        stats.set_portmap(portmap.clone());
        Self {
            config,
            peer_id: peer::generate_peer_id(),
            rate_limits,
            slots,
            hashing: Semaphore::new(1),
            bars,
            listener: OnceCell::new(),
            utp: OnceCell::new(),
            portmap,
            dht: OnceCell::new(),
            lsd: OnceCell::new(),
            torrents: Mutex::new(HashSet::new()),
            files: Mutex::new(HashMap::new()),
            stats: Arc::new(stats),
            blocklist: Arc::default(),
        }
    }

    /// Refuses the peers `blocklist` blocks, from the first torrent on.
    pub fn with_blocklist(mut self, blocklist: Arc<Blocklist>) -> Self {
        self.blocklist = blocklist;
        self
    }

    /// The ranges of addresses we refuse peers from.
    pub fn blocklist(&self) -> &Arc<Blocklist> {
        &self.blocklist
    }

    /// Live numbers about the session and its torrents.
    pub fn stats(&self) -> &Arc<SessionStats> {
        &self.stats
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

    /// Changes the rate limits to `download` and `upload` KiB/s (0:
    /// unlimited) for every torrent, at once. `config` keeps the limits the
    /// session started with, and the config file is left as it is.
    pub fn set_rate_limits(&self, download: u64, upload: u64) {
        info!(download, upload, "rate limits in KiB/s (0: unlimited)");
        self.rate_limits.set(download, upload);
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
        let bind = async {
            let tcp = if self.config.utp_policy == crate::utp::Policy::Require {
                Vec::new()
            } else {
                crate::listener::listen(self.config.listen_port).await
            };
            let port = tcp
                .first()
                .and_then(|socket| socket.local_addr().ok())
                .map_or(self.config.listen_port, |addr| addr.port());
            let utp = self
                .utp
                .get_or_init(|| async {
                    if self.config.utp_policy == crate::utp::Policy::Off
                        || crate::network::global().proxied()
                    {
                        return None;
                    }
                    crate::utp::Endpoint::bind(
                        port,
                        self.config.max_connections,
                        Arc::clone(&self.blocklist),
                    )
                    .await
                    .inspect_err(|e| warn!("cannot start uTP: {e:#}"))
                    .ok()
                })
                .await
                .clone();
            let has_tcp = !tcp.is_empty();
            let listener = Listener::start_with_utp(
                tcp,
                Arc::clone(&self.blocklist),
                self.config.encryption,
                utp.clone(),
            );
            self.stats.set_port(listener.port());
            if has_tcp && let Some(port) = listener.port() {
                self.portmap.map(Protocol::Tcp, port);
            }
            if let Some(endpoint) = utp {
                self.portmap.map(Protocol::Udp, endpoint.port());
            }
            listener
        };
        self.listener
            .get_or_init(|| bind.instrument(session_span()))
            .await
    }

    /// Shared uTP endpoint, initialized with the peer listener when enabled.
    pub async fn utp(&self) -> Option<Arc<crate::utp::Endpoint>> {
        self.listener().await;
        self.utp.get().cloned().flatten()
    }

    /// Our DHT node, started on the first call (trying the port the
    /// listener got for UDP too), unless the DHT is off. A failure to start
    /// it is logged once, and the session goes on without it.
    pub async fn dht(&self) -> Option<Dht> {
        if !self.config.dht || crate::network::global().proxied() {
            return None;
        }
        // Not the configured port: with port 0 the listener picks its own.
        let port = self
            .listener()
            .await
            .port()
            .unwrap_or(self.config.listen_port);
        let stats = &self.stats;
        let portmap = &self.portmap;
        let start = async move {
            let result = if let Some(utp) = self.utp.get().and_then(Option::as_ref) {
                utp.dht()
            } else {
                Dht::bind(port).await
            };
            let dht = result
                .inspect_err(|e| warn!("cannot start the DHT: {e:#}"))
                .ok();
            if let Some(dht) = &dht {
                stats.set_dht(dht.clone());
                portmap.map(Protocol::Udp, dht.port());
                tokio::spawn(tell_dht_external_ip(portmap.clone(), dht.clone()));
            }
            dht
        };
        self.dht
            .get_or_init(|| start.instrument(session_span()))
            .await
            .clone()
    }

    /// Announces the torrent `info_hash` on the local network and returns
    /// the peers found there, once the first call has joined the multicast
    /// group. `None` if LSD is off, the torrent is `private` (BEP 27), we
    /// have no listening port to announce, or the group cannot be joined,
    /// which is logged once.
    pub async fn lsd(&self, info_hash: [u8; 20], private: bool) -> Option<LsdPeers> {
        if !self.config.lsd || crate::network::global().restricted() {
            return None;
        }
        if private {
            info!("private torrent: not using local service discovery");
            return None;
        }
        let port = self.listener().await.port()?;
        let start = async move {
            Lsd::bind(port)
                .inspect_err(|e| warn!("cannot start local service discovery: {e:#}"))
                .ok()
        };
        self.lsd
            .get_or_init(|| start.instrument(session_span()))
            .await
            .as_ref()?
            .register(info_hash, private)
    }

    /// Removes the router's port mappings, if it made any, waiting a few
    /// seconds at most. Called as the session ends.
    pub async fn close_port_mappings(&self) {
        self.portmap.shutdown().await;
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

    /// The name of a running torrent that writes any of `paths` (see
    /// `Storage::paths`), if one does.
    pub fn file_owner(&self, paths: &[PathBuf]) -> Option<Arc<str>> {
        let files = self.lock_files();
        paths
            .iter()
            .find_map(|path| files.get(path).map(|(_, name)| Arc::clone(name)))
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
    limit: usize,
    /// Notified whenever a permit comes back.
    freed: Arc<Notify>,
}

impl ConnectionSlots {
    /// `limit` slots.
    pub fn new(limit: usize) -> Self {
        let limit = limit.min(Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(Semaphore::new(limit)),
            limit,
            freed: Arc::new(Notify::new()),
        }
    }

    /// How many slots there are.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// How many slots are taken.
    pub fn in_use(&self) -> usize {
        self.limit - self.semaphore.available_permits()
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

/// How often, and how many times, the port mapper is asked for the router's
/// outside address.
const EXTERNAL_IP_POLL: Duration = Duration::from_secs(5);
const EXTERNAL_IP_POLLS: u32 = 60;

/// Tells `dht` the outside address the router reports once the port mapper
/// has it, so that the node id can match it (BEP 42) without waiting for
/// other nodes to say. Gives up when the mapping fails or does not come.
async fn tell_dht_external_ip(portmap: Portmapper, dht: Dht) {
    for _ in 0..EXTERNAL_IP_POLLS {
        match portmap.status() {
            Status::Off | Status::Failed(_) => return,
            Status::Mapped {
                external_ip: Some(ip),
                ..
            } => return dht.set_external_ip(ip),
            Status::Searching | Status::Mapped { .. } => {}
        }
        tokio::time::sleep(EXTERNAL_IP_POLL).await;
    }
}

#[cfg(test)]
mod tests;
