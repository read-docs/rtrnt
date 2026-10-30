use super::choker::Choker;
use super::peer_pool::PeerPool;
use crate::blocklist::Blocklist;
use crate::client::{ConnectionSlots, SlotPermit};
use crate::dht::DhtSearch;
use crate::listener::Registration;
use crate::lsd::{LsdPeers, lsd_peers};
use crate::peer::holepunch;
use crate::peer::pex::PeerExchange;
use crate::peer::{self, Incoming, PeerConnection};
use crate::selection;
use crate::stats::{PeerSource, PieceSnapshot, PieceSource, TorrentState, WebSeedStats};
use crate::storage::StorageError;
use crate::tracker::announcer::{Announcer, Transferred};
use crate::webseed::WebSeed;
use anyhow::{Result, bail};
use indicatif::HumanBytes;
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

use super::progress::Progress;
use super::queue::PieceQueue;
use super::smart_ban::SmartBan;
use super::superseed::Offers;
use super::web::web_seed_worker;
use super::worker::peer_worker;
use super::{Interrupted, Misbehaved, Session};

/// How long workers get to wind down, once the download is complete or
/// interrupted, before being aborted.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// How often a seed limit is checked, and the upload count on the progress
/// bar brought up to date, while seeding.
const SEED_CHECK: Duration = Duration::from_secs(1);
/// DHT lookups awaited once we run out of peers, before giving up. One may
/// have started before we ran out.
const DHT_LOOKUPS_WHEN_OUT: u8 = 2;
/// Peers learned through peer exchange join the pool only while fewer than
/// this many wait there, so peers that flood us with addresses cannot grow
/// it without bound.
const MAX_PEX_BACKLOG: usize = 500;
/// How often the resume file is saved while pieces complete.
const RESUME_SAVE_INTERVAL: Duration = Duration::from_secs(30);
/// How often the write cache is flushed to disk even if it is not full.
const CACHE_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// The peers a download starts with.
#[derive(Default)]
pub(crate) struct StartPeers {
    /// Peers to connect to, with those already connected or banned marked.
    pub(crate) pool: PeerPool,
    /// Addresses of incoming peers that misbehaved.
    pub(crate) banned_ips: HashSet<IpAddr>,
    /// Peers on the local network, if we look for them.
    pub(crate) lsd: Option<LsdPeers>,
    /// Connections already open, such as those the metadata of a magnet
    /// link came over, each with whether the peer connected to us and the
    /// connection slot it holds.
    pub(crate) connections: Vec<(PeerConnection, bool, SlotPermit)>,
}

impl StartPeers {
    /// No peers yet, in a pool that leaves out those `blocklist` blocks.
    pub(crate) fn with_blocklist(blocklist: Arc<Blocklist>) -> Self {
        Self {
            pool: PeerPool::with_blocklist(blocklist),
            ..Self::default()
        }
    }
}

/// How many peers a download may be connected to.
pub(crate) struct Limits {
    /// Most peers of this torrent at once.
    pub(crate) max_peers: usize,
    /// Every worker holds one of these while it runs, and no peer is
    /// connected to or taken on while none is free; a slot freed in any
    /// torrent that shares them gets us trying again.
    pub(crate) slots: ConnectionSlots,
}

/// `download_from_peers`, starting with the peers in `start`, some of them
/// perhaps already connected, and taking the peers that connect to us from
/// `incoming`, within `limits`.
pub(crate) async fn download_with(
    session: Arc<Session>,
    have: &[bool],
    start: StartPeers,
    limits: Limits,
    mut incoming: Registration,
    mut announcer: Option<Announcer>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let torrent = &session.torrent;
    let num_pieces = torrent.num_pieces();
    if have.len() != num_pieces {
        bail!("have {} piece flags for {num_pieces} pieces", have.len());
    }
    let Limits { max_peers, slots } = limits;
    // The torrent's own seed settings count, and may change as it runs.
    let torrent_limits = &session.torrent_limits;
    let seed = torrent_limits.seed(session.seed);
    // Private torrents (BEP 27) get their peers from trackers only.
    let (pex, mut pex_peers) = PeerExchange::new(!torrent.is_private());
    let stats = &session.stats;
    let priorities = session.piece_priorities();
    let (smart_ban, mut verdicts) = SmartBan::new();
    let (holepunch, mut introduced) = holepunch::Hub::new(
        !torrent.is_private()
            && session.utp.is_some()
            && session.utp_policy != crate::utp::Policy::Off
            && !crate::network::global().proxied(),
    );
    let swarm = Arc::new(Swarm {
        queue: PieceQueue::new(have, &priorities)
            .seeding(seed.enabled())
            .sequential(stats.sequential_flag(), session.piece_ranks()),
        progress: Progress::new(
            &session.bars,
            &torrent.info.name,
            selection::wanted_len(torrent, &priorities)?,
            selection::wanted_done(torrent, &priorities, have)?,
            Arc::clone(stats),
        ),
        connected: ConnectedPeers::default(),
        choker: Choker::default(),
        pex,
        smart_ban,
        offers: Offers::new(num_pieces),
        holepunch,
    });
    let Swarm {
        queue,
        progress,
        choker,
        ..
    } = &*swarm;
    let source: Arc<dyn PieceSource> = swarm.clone();
    stats.set_piece_source(Arc::downgrade(&source));
    stats.set_state(if queue.is_done() {
        TorrentState::Seeding
    } else {
        TorrentState::Downloading
    });
    // Incoming peers have no address we could ban in the pool.
    let StartPeers {
        mut pool,
        mut banned_ips,
        mut lsd,
        connections,
    } = start;

    // Told to peers in the extension handshake.
    let listen_port = incoming.port();

    let mut workers = JoinSet::new();
    // Starts a worker that connects to `addr`, or, given the socket the peer
    // connected to us on, answers its handshake, or carries on over a
    // connection already open. The worker holds `permit` until it exits.
    let spawn = |workers: &mut JoinSet<WorkerExit>, addr, link: Link, permit: SlotPermit| {
        let session = Arc::clone(&session);
        let swarm = Arc::clone(&swarm);
        let inbound = link.is_inbound();
        let worker = async move {
            let _permit = permit;
            let mut pieces = 0;
            let mut encrypted = None;
            let result = peer_worker(
                &session,
                &swarm,
                addr,
                link,
                listen_port,
                &mut pieces,
                &mut encrypted,
            )
            .await;
            WorkerExit {
                addr,
                inbound,
                pieces,
                encrypted,
                result,
            }
        };
        workers.spawn(worker.in_current_span());
    };
    for (conn, inbound, permit) in connections {
        spawn(
            &mut workers,
            conn.addr,
            Link::Open {
                conn: Box::new(conn),
                inbound,
            },
            permit,
        );
    }
    // Web seeds take pieces next to the peers, for as long as they work.
    let mut web_workers: JoinSet<Result<()>> = JoinSet::new();
    if !queue.is_done() {
        let seeds = WebSeed::of(torrent);
        let shown: Vec<Arc<WebSeedStats>> = seeds
            .iter()
            .map(|seed| {
                let http_seed = seed.kind == crate::webseed::WebSeedKind::Http;
                Arc::new(WebSeedStats::new(&seed.url, http_seed))
            })
            .collect();
        stats.set_web_seeds(shown.clone());
        for (seed, seed_stats) in seeds.into_iter().zip(shown) {
            info!(url = %seed.url, "using web seed");
            let session = Arc::clone(&session);
            let swarm = Arc::clone(&swarm);
            web_workers.spawn(
                async move { web_seed_worker(&session, &swarm, seed, seed_stats).await }
                    .in_current_span(),
            );
        }
    }
    let mut dht = session
        .dht
        .clone()
        .map(|dht| DhtSearch::new(dht, session.info_hash, listen_port));
    // Set once we ran out of peers and asked for more, counting the answers
    // still awaited; cleared when a new connection is started.
    let mut asked: Option<Asked> = None;
    // Upload slots go by download rate, and by upload rate once we seed.
    let mut choking = std::pin::pin!(choker.run(|| queue.is_done()));
    let mut shutdown = std::pin::pin!(shutdown);
    let mut interrupted = false;
    // The disk error that ended the run, if one did.
    let mut disk_error = None;
    // Whether the download finishes this session, rather than having been
    // complete on disk.
    let downloading = !queue.is_done();
    // When we finished downloading and started seeding.
    let mut seeding_since = None;
    // Uploaded by the torrent's earlier runs this session, before a pause:
    // the ratio counts them, so that a pause does not start it over.
    let uploaded_before = stats.previous().uploaded;
    // Pieces completed this session as of the last save of the resume file,
    // and when the next one is due.
    let mut saved = 0;
    let mut next_save = Instant::now() + RESUME_SAVE_INTERVAL;
    let mut next_flush = Instant::now() + CACHE_FLUSH_INTERVAL;
    loop {
        // Made before the seed settings are read, so that a change meanwhile
        // is not missed.
        let mut seed_changed = std::pin::pin!(torrent_limits.seed_changed());
        seed_changed.as_mut().enable();
        let seed = torrent_limits.seed(session.seed);
        queue.set_seeding(seed.enabled());
        if queue.is_done() {
            let since = match seeding_since {
                Some(since) => since,
                None => {
                    if downloading {
                        if let Some(announcer) = announcer.as_mut() {
                            announcer.complete();
                        }
                        if let Some(completion) = &session.completion {
                            completion.fire();
                        }
                    }
                    if !seed.enabled() {
                        break;
                    }
                    info!(ratio = seed.ratio, time = ?seed.time, "download complete, seeding");
                    stats.set_state(TorrentState::Seeding);
                    *seeding_since.insert(Instant::now())
                }
            };
            // Seeding limits set to none while seeding end it, as none at
            // the start would have.
            if !seed.enabled() {
                break;
            }
            let uploaded = progress.transferred.uploaded.load(Ordering::Relaxed);
            progress
                .bar
                .set_message(format!("seeding, {} up", HumanBytes(uploaded)));
            if seed.reached(
                uploaded_before + uploaded,
                torrent.length(),
                since.elapsed(),
            ) {
                info!(uploaded = uploaded_before + uploaded, "seed limit reached");
                break;
            }
        }
        stats.set_pool(pool.len());
        if let Some(dht) = &mut dht {
            dht.set_seeding(queue.is_done());
        }
        // Made before a slot is looked for, so that one freed meanwhile is
        // not missed.
        let mut freed = std::pin::pin!(slots.freed());
        while workers.len() < max_peers {
            let Some(addr) = pool.next(Instant::now()) else {
                break;
            };
            let Some(permit) = slots.try_acquire() else {
                pool.requeue(addr);
                break;
            };
            asked = None;
            let plaintext_first = pool.plaintext_first(addr);
            spawn(
                &mut workers,
                addr,
                Link::Connect { plaintext_first },
                permit,
            );
        }

        let retry_at = pool.next_retry().filter(|_| workers.len() < max_peers);
        // Peers still waiting for a slot are not running out.
        if workers.is_empty()
            && retry_at.is_none()
            && pool.ready_len() == 0
            && seeding_since.is_none()
        {
            match &asked {
                None if announcer.is_some() || dht.is_some() => {
                    info!("out of peers, asking for more");
                    if let Some(announcer) = &mut announcer {
                        announcer.hurry();
                    }
                    if let Some(dht) = &mut dht {
                        dht.hurry();
                    }
                    asked = Some(Asked {
                        tracker: announcer.is_some(),
                        dht_lookups: if dht.is_some() {
                            DHT_LOOKUPS_WHEN_OUT
                        } else {
                            0
                        },
                    });
                }
                Some(asked) if asked.tracker || asked.dht_lookups > 0 => {}
                // A web seed may still deliver the rest.
                _ if !web_workers.is_empty() => {}
                _ => break,
            }
        }
        tokio::select! {
            Some(joined) = workers.join_next() => {
                let WorkerExit { addr, inbound, pieces, encrypted, result } = match joined {
                    Ok(exit) => exit,
                    Err(e) => {
                        warn!("peer task failed: {e}");
                        continue;
                    }
                };
                // So the next connection greets the peer the way that
                // worked.
                if let (false, Some(encrypted)) = (inbound, encrypted) {
                    pool.reached(addr, encrypted);
                }
                match result {
                    Ok(()) => debug!(%addr, "peer finished"),
                    Err(e) if e.is::<Misbehaved>() || e.is::<peer::ProtocolViolation>() => {
                        warn!(%addr, "banning peer: {e:#}");
                        if inbound {
                            banned_ips.insert(addr.ip());
                        } else {
                            pool.ban(addr);
                        }
                    }
                    // The disk is at fault, not the peer, and no other peer
                    // would fare better: the torrent stops.
                    Err(e) if e.is::<StorageError>() => {
                        warn!(%addr, "stopping: {e:#}");
                        disk_error = Some(e);
                        break;
                    }
                    Err(e) => {
                        debug!(%addr, pieces, "peer dropped: {e:#}");
                        // We cannot connect back to the port an incoming
                        // peer came from.
                        if !inbound {
                            pool.failed(addr, pieces > 0, Instant::now());
                            if encrypted.is_none() { swarm.holepunch.failed(addr, Instant::now()); }
                        }
                    }
                }
            }
            Some(addr) = introduced.recv() => {
                if workers.len() < max_peers && pool.allows(addr)
                    && !banned_ips.contains(&addr.ip()) && !swarm.smart_ban.is_banned(addr)
                    && let Some(permit) = slots.try_acquire()
                {
                    debug!(%addr, "connecting through a holepunch introduction");
                    spawn(&mut workers, addr, Link::Holepunch, permit);
                }
            }
            Some(joined) = web_workers.join_next() => {
                match joined {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if e.is::<StorageError>() => {
                        warn!("stopping: {e:#}");
                        disk_error = Some(e);
                        break;
                    }
                    Ok(Err(e)) => warn!("web seed failed: {e:#}"),
                    Err(e) => warn!("web seed task failed: {e}"),
                }
            }
            peer = incoming.next() => {
                let addr = peer.addr;
                if workers.len() >= max_peers {
                    debug!(%addr, "turning away incoming peer: too many peers");
                } else if banned_ips.contains(&addr.ip()) {
                    debug!(%addr, "turning away banned peer");
                } else if let Some(permit) = slots.try_acquire() {
                    asked = None;
                    stats.add_source(PeerSource::Incoming, 1);
                    spawn(&mut workers, addr, Link::Accepted(peer), permit);
                } else {
                    debug!(%addr, "turning away incoming peer: no free connection slot");
                }
            }
            () = &mut freed => {}
            () = sleep_until_some(retry_at) => {}
            peers = reannounce(&mut announcer, &progress.transferred) => {
                stats.add_source(PeerSource::Tracker, peers.len());
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.tracker = false;
                }
            }
            peers = dht_lookup(&mut dht) => {
                stats.add_source(PeerSource::Dht, peers.len());
                if let Some(swarm) = dht.as_ref().and_then(DhtSearch::swarm) {
                    stats.set_dht_swarm(swarm);
                }
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.dht_lookups = asked.dht_lookups.saturating_sub(1);
                }
            }
            peers = lsd_peers(&mut lsd) => {
                stats.add_source(PeerSource::Lsd, peers.len());
                pool.add(peers);
            }
            Some(peers) = pex_peers.recv() => {
                if pool.ready_len() < MAX_PEX_BACKLOG {
                    stats.add_source(PeerSource::Pex, peers.len());
                    pool.add_with_encryption(peers);
                }
            }
            // Found guilty of corrupt blocks by a piece completed elsewhere.
            Some(sender) = verdicts.recv() => {
                if sender.inbound {
                    banned_ips.insert(sender.addr.ip());
                } else {
                    pool.ban(sender.addr);
                }
            }
            () = &mut choking => {}
            () = &mut seed_changed => {}
            () = queue.wait_for(|| queue.is_done()), if seeding_since.is_none() => {}
            () = tokio::time::sleep(SEED_CHECK), if seeding_since.is_some() => {}
            () = tokio::time::sleep_until(next_save), if session.resume.is_some() => {
                save_resume(&session, queue, &mut saved).await;
                next_save = Instant::now() + RESUME_SAVE_INTERVAL;
            }
            () = tokio::time::sleep_until(next_flush), if session.storage.cache_enabled() => {
                next_flush = Instant::now() + CACHE_FLUSH_INTERVAL;
                if let Err(e) = session.storage.flush_async().await {
                    warn!("stopping: {e:#}");
                    disk_error = Some(e);
                    break;
                }
            }
            () = &mut shutdown => {
                info!("interrupted, stopping");
                interrupted = true;
                break;
            }
        }
    }

    // Workers still downloading notice completion or the closed queue and
    // exit (sending Cancel for duplicate endgame requests); ones still
    // connecting are cut short. A worker writing a piece finishes the write
    // first, and even if aborted, the write runs on to the end on the
    // blocking pool. New peers are refused meanwhile.
    drop(incoming);
    queue.close();
    let drain = async {
        while workers.join_next().await.is_some() {}
        while web_workers.join_next().await.is_some() {}
    };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        workers.shutdown().await;
        web_workers.shutdown().await;
    }
    stats.set_web_seeds(Vec::new());

    // Every finished write is in the files now (and with a write cache,
    // flushed there).
    if let Err(e) = session.storage.flush_async().await {
        warn!("cannot write the cached pieces: {e:#}");
        disk_error.get_or_insert(e);
    }
    save_resume(&session, queue, &mut saved).await;
    stats.set_final_pieces(queue.snapshot());

    let remaining = queue.remaining();
    let wanted = queue.wanted();
    if remaining == 0 {
        progress.bar.finish();
    } else {
        // `finish` would fill the bar.
        progress.bar.abandon();
    }
    if let Some(announcer) = announcer {
        announcer.stop(&progress.transferred).await;
    }
    if let Some(e) = disk_error {
        return Err(e);
    }
    if remaining > 0 {
        if interrupted {
            bail!(Interrupted {
                remaining,
                pieces: wanted
            });
        }
        bail!("download incomplete: {remaining} of {wanted} pieces missing and no peers left");
    }
    Ok(())
}

/// Saves the torrent's resume file if a piece was completed since the last
/// save, which had `saved` completions.
async fn save_resume(session: &Session, queue: &PieceQueue, saved: &mut usize) {
    let Some(resume) = &session.resume else {
        return;
    };
    let completed = queue.completed_count();
    if completed == *saved {
        return;
    }
    // Before the files are looked at, so that a piece completed meanwhile
    // is at worst missing from the file.
    resume.save(&session.storage, queue.have_bitfield()).await;
    *saved = completed;
}

/// How a peer worker reaches its peer.
pub(super) enum Link {
    /// By connecting to it, greeting it in plaintext before trying
    /// encryption if `plaintext_first` (see `Encryption::attempts`).
    Connect { plaintext_first: bool },
    /// A negotiated relay introduction always uses the shared uTP endpoint.
    Holepunch,
    /// Over a connection it opened to us, with its handshake read but not
    /// answered.
    Accepted(Incoming),
    /// Over a connection already open, such as one the metadata of a magnet
    /// link came over.
    Open {
        conn: Box<PeerConnection>,
        /// Whether the peer connected to us.
        inbound: bool,
    },
}

impl Link {
    /// Whether the peer connected to us, so we cannot connect back.
    pub(super) fn is_inbound(&self) -> bool {
        match self {
            Link::Connect { .. } | Link::Holepunch => false,
            Link::Accepted(_) => true,
            Link::Open { inbound, .. } => *inbound,
        }
    }
}

/// Sleeps until `at`, or forever if it is `None`.
pub(crate) async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// What the peer workers of a download share.
pub(super) struct Swarm {
    pub(super) queue: PieceQueue,
    pub(super) progress: Progress,
    pub(super) connected: ConnectedPeers,
    /// Decides which peers may download from us.
    pub(super) choker: Choker,
    /// Tells peers about each other (BEP 11).
    pub(super) pex: PeerExchange,
    /// Remembers which peers sent the blocks of pieces that failed their
    /// hash check, to ban them once the piece is completed elsewhere.
    pub(super) smart_ban: SmartBan,
    /// How often each piece was offered while super-seeding.
    pub(super) offers: Offers,
    /// Bounded public-torrent rendezvous coordination.
    pub(super) holepunch: Arc<holepunch::Hub>,
}

impl PieceSource for Swarm {
    fn piece_snapshot(&self) -> PieceSnapshot {
        self.queue.snapshot()
    }
}

/// How a peer worker ended.
struct WorkerExit {
    addr: SocketAddr,
    /// Whether the peer connected to us.
    inbound: bool,
    /// Pieces the peer delivered first.
    pieces: usize,
    /// Whether the connection was encrypted, once the handshake was done.
    encrypted: Option<bool>,
    result: Result<()>,
}

/// Ids of the peers we are connected to, so that a peer we connect to and
/// that also connects to us is only downloaded from once.
#[derive(Default)]
pub(crate) struct ConnectedPeers(Mutex<HashMap<[u8; 20], Arc<ConnectionChoice>>>);

struct ConnectionChoice {
    preferred: bool,
    replaced: tokio::sync::watch::Sender<bool>,
    done: AtomicBool,
    finished: tokio::sync::Notify,
}

impl ConnectedPeers {
    fn lock(&self) -> MutexGuard<'_, HashMap<[u8; 20], Arc<ConnectionChoice>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn insert(
        &self,
        id: [u8; 20],
        preferred: bool,
        replace: bool,
    ) -> Option<(Connected<'_>, Option<Arc<ConnectionChoice>>)> {
        let mut peers = self.lock();
        let old = peers.get(&id).cloned();
        if old
            .as_ref()
            .is_some_and(|old| !replace || old.preferred || !preferred)
        {
            return None;
        }
        let connection = Arc::new(ConnectionChoice {
            preferred,
            replaced: tokio::sync::watch::Sender::new(false),
            done: AtomicBool::new(false),
            finished: tokio::sync::Notify::new(),
        });
        peers.insert(id, Arc::clone(&connection));
        if let Some(old) = &old {
            old.replaced.send_replace(true);
        }
        Some((
            Connected {
                peers: self,
                id,
                connection,
                waiting_for: None,
            },
            old,
        ))
    }

    /// Records a connection until the returned guard drops. Metadata fetches
    /// use this form because their connections are not introduced in pairs.
    pub(crate) fn register(&self, id: [u8; 20]) -> Option<Connected<'_>> {
        self.insert(id, false, false).map(|(guard, _)| guard)
    }

    /// Resolves crossed simultaneous connections consistently on both peers.
    /// The smaller peer ID prefers outbound; the larger prefers inbound.
    /// A preferred stream replaces a nonpreferred one, waiting for its guard
    /// to drop before a new worker can register PEX and relay handles.
    pub(crate) async fn register_directed(
        &self,
        id: [u8; 20],
        preferred: bool,
    ) -> Option<Connected<'_>> {
        let (mut guard, old) = self.insert(id, preferred, true)?;
        if let Some(old) = old {
            guard.waiting_for = Some(Arc::clone(&old));
            loop {
                let mut finished = std::pin::pin!(old.finished.notified());
                finished.as_mut().enable();
                if old.done.load(Ordering::Acquire) {
                    break;
                }
                finished.await;
            }
        }
        guard.waiting_for = None;
        Some(guard)
    }
}

pub(crate) struct Connected<'a> {
    peers: &'a ConnectedPeers,
    id: [u8; 20],
    connection: Arc<ConnectionChoice>,
    waiting_for: Option<Arc<ConnectionChoice>>,
}

impl Connected<'_> {
    /// Watch state remembers a replacement even if it predates this wait.
    pub(crate) async fn replacement(&self) {
        let mut replaced = self.connection.replaced.subscribe();
        let _ = replaced.wait_for(|&replaced| replaced).await;
    }
}

impl Drop for Connected<'_> {
    fn drop(&mut self) {
        let mut peers = self.peers.lock();
        if peers
            .get(&self.id)
            .is_some_and(|current| Arc::ptr_eq(current, &self.connection))
        {
            if let Some(old) = &self.waiting_for
                && !old.done.load(Ordering::Acquire)
            {
                // A canceled replacement must still exclude new workers
                // until the old worker releases its PEX and relay handles.
                peers.insert(self.id, Arc::clone(old));
            } else {
                peers.remove(&self.id);
            }
        }
        self.connection.done.store(true, Ordering::Release);
        self.connection.finished.notify_waiters();
    }
}

/// Runs the next re-announce and returns its peers, or waits forever
/// without a tracker. Cancel-safe.
pub(crate) async fn reannounce(
    announcer: &mut Option<Announcer>,
    transferred: &Transferred,
) -> Vec<SocketAddr> {
    match announcer {
        Some(announcer) => announcer.poll(transferred).await,
        None => std::future::pending().await,
    }
}

/// Runs the next DHT lookup and returns its peers, or waits forever without
/// a DHT. Cancel-safe.
pub(crate) async fn dht_lookup(search: &mut Option<DhtSearch>) -> Vec<SocketAddr> {
    match search {
        Some(search) => search.poll().await,
        None => std::future::pending().await,
    }
}

/// Answers still awaited from the peer sources asked for more peers once we
/// ran out.
struct Asked {
    tracker: bool,
    dht_lookups: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_is_connected_to_once_at_a_time() {
        let peers = ConnectedPeers::default();
        let a = peers.register([1; 20]).unwrap();
        assert!(peers.register([1; 20]).is_none());
        let _b = peers.register([2; 20]).unwrap();
        drop(a);
        assert!(peers.register([1; 20]).is_some());
    }
    #[tokio::test]
    async fn crossed_connections_keep_the_same_stream_on_both_ends() {
        let smaller = Arc::new(ConnectedPeers::default());
        let larger = Arc::new(ConnectedPeers::default());
        // Opposite ends each first select a different outbound stream.
        let smaller_out = smaller.register_directed([2; 20], true).await.unwrap();
        let larger_out = larger.register_directed([1; 20], false).await.unwrap();
        assert!(smaller.register_directed([2; 20], false).await.is_none());
        let accepting = Arc::clone(&larger);
        let preferred = tokio::spawn(async move {
            let guard = accepting.register_directed([1; 20], true).await.unwrap();
            assert!(accepting.register([1; 20]).is_none());
            drop(guard);
        });
        // Replacement state cannot be lost even when subscribed afterward.
        larger_out.replacement().await;
        assert!(
            !preferred.is_finished(),
            "new stream started before old PEX handles ended"
        );
        drop(larger_out);
        preferred.await.unwrap();
        // The smaller side's outbound stream and larger side's inbound are
        // opposite ends of the same surviving connection.
        assert!(smaller.register([2; 20]).is_none());
        drop(smaller_out);
        assert!(smaller.register([2; 20]).is_some());
        assert!(larger.register([1; 20]).is_some());
    }
    #[tokio::test]
    async fn canceling_a_replacement_still_waits_for_old_worker_cleanup() {
        let peers = ConnectedPeers::default();
        let old = peers.register_directed([2; 20], false).await.unwrap();
        let replacement = peers.register_directed([2; 20], true);
        assert!(
            tokio::time::timeout(Duration::ZERO, replacement)
                .await
                .is_err()
        );
        old.replacement().await;
        assert!(peers.register([2; 20]).is_none());
        drop(old);
        assert!(peers.register([2; 20]).is_some());
    }
}
