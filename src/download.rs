use crate::announcer::{Announcer, Transferred};
use crate::choker::Choker;
use crate::client::{Client, ConnectionSlots, SlotPermit};
use crate::dht::{Dht, DhtSearch};
use crate::extension::{self, Advertised};
use crate::fast;
use crate::listener::{Listener, Registration};
use crate::metadata;
use crate::peer::{self, Incoming, PeerConnection, PeerMessage};
use crate::peer_pool::PeerPool;
use crate::pex::{self, PeerExchange};
use crate::rate_limit::{Allowance, RateLimits};
use crate::resume::{self, Target};
use crate::storage::Storage;
use crate::torrent::{self, TorrentFile};
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use crate::verify;
use anyhow::{Result, anyhow, bail};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use rand::Rng;
use sha1::Digest;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

const BLOCK_SIZE: u32 = 16 * 1024;
/// Block requests in flight to a new peer, before its rate is known.
const INITIAL_PIPELINE: usize = 4;
const MIN_PIPELINE: usize = 2;
/// Many clients drop requests beyond a few hundred queued. A peer that sends
/// `reqq` in its extension handshake (BEP 10) gets at most that many.
const MAX_PIPELINE: usize = 250;
/// The pipeline holds about this much of a peer's measured throughput, so
/// the peer never runs dry while our next requests are on their way.
const PIPELINE_QUEUE_TIME: Duration = Duration::from_secs(3);
/// How long the download rate is measured before the depth is resized.
const RATE_WINDOW: Duration = Duration::from_secs(1);
/// Peers that send this many corrupt pieces are dropped.
const MAX_HASH_FAILURES: u32 = 3;
/// A peer that has no piece we need for this long is dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// A peer that keeps us choked this long while we are interested is dropped.
const UNCHOKE_TIMEOUT: Duration = Duration::from_secs(60);
/// How often an idle worker re-checks the queue.
const IDLE_POLL: Duration = Duration::from_secs(2);
/// How long workers get to wind down, once the download is complete or
/// interrupted, before being aborted.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);
/// How long to wait before accepting again after an error, such as running
/// out of file descriptors.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);
/// Longest block a peer may request. 16 KiB is what every client asks for;
/// some serve up to 128 KiB, but nobody needs that.
const MAX_REQUEST_LEN: u32 = BLOCK_SIZE;
/// Requests a peer may have queued with us; more are dropped. Told to peers
/// in the extension handshake (`reqq`).
const MAX_QUEUED_UPLOADS: usize = extension::MAX_QUEUED_REQUESTS as usize;
/// Messages a worker handles from a peer that keeps sending, before it sends
/// the next block that peer asked for.
const MAX_MESSAGES_PER_BLOCK: usize = 16;
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

pub struct DownloadConfig {
    pub output_dir: PathBuf,
    /// Most peers to be connected to at once, for each torrent.
    pub max_peers: usize,
    /// Most peers to be connected to at once, across every torrent of a
    /// `Client`.
    pub max_connections: usize,
    /// Port to listen on for incoming peers; 0 picks a free one.
    pub listen_port: u16,
    pub seed: SeedLimits,
    /// Whether to look for peers in the DHT too (never for private torrents).
    pub dht: bool,
    /// Most block data to download per second, in KiB, across all peers;
    /// 0 for no limit.
    pub max_download_rate: u64,
    /// Most block data to upload per second, in KiB, across all peers; 0
    /// for no limit.
    pub max_upload_rate: u64,
    /// Directory the resume files (see `resume`) are kept in, or `None` to
    /// keep none.
    pub resume_dir: Option<PathBuf>,
    /// Whether to ignore the resume files and hash the data on disk, as
    /// without one.
    pub recheck: bool,
}

/// How long to keep seeding once every piece is done. Seeding stops at
/// whichever limit is reached first; with neither, the run ends as soon as
/// the download is complete.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SeedLimits {
    /// Stop once we have uploaded this many times the torrent's size this
    /// session; 0 for no ratio limit. The torrent's size rather than what we
    /// downloaded, so that a download resumed from disk still gives back
    /// its share.
    pub ratio: f64,
    /// Stop after seeding this long; zero for no time limit.
    pub time: Duration,
}

impl SeedLimits {
    /// Whether to seed at all once the download is complete.
    pub fn enabled(&self) -> bool {
        self.ratio > 0.0 || !self.time.is_zero()
    }

    /// Whether a limit is reached after uploading `uploaded` bytes of a
    /// torrent of `size` bytes, having seeded for `seeded`.
    fn reached(&self, uploaded: u64, size: u64, seeded: Duration) -> bool {
        (self.ratio > 0.0 && uploaded as f64 >= self.ratio * size as f64)
            || (!self.time.is_zero() && seeded >= self.time)
    }
}

/// A torrent to download and seed: what the peer workers need to know about
/// it, and how long to stay once it is complete.
pub struct Session {
    pub torrent: TorrentFile,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub storage: Arc<Storage>,
    pub seed: SeedLimits,
    /// Our DHT node, if any: searched for peers, and told about peers' DHT
    /// nodes, whose ports they send us.
    pub dht: Option<Dht>,
    /// How fast block data may move each way, shared by every peer (of
    /// every torrent in the session).
    pub rate_limits: Arc<RateLimits>,
    /// The torrent's info dictionary exactly as encoded, if we have it, for
    /// peers that fetch it by magnet link (BEP 9).
    pub metadata: Option<Vec<u8>>,
    /// Where the torrent's progress bars go, next to those of the other
    /// torrents in the session.
    pub bars: MultiProgress,
    /// Where the torrent's resume file is saved while it downloads, if it
    /// has one.
    pub resume: Option<Target>,
}

/// Downloads the torrent at `torrent_path` and seeds it for as long as the
/// client's `seed` setting says, stopping early (see `download_from_peers`)
/// if `shutdown` resolves. It is first polled when the data on disk is
/// checked, or, if there is none to check, once the tracker has answered, so
/// a signal handler behind it is installed no later than that. It also ends
/// the wait for the client's turn to check the data, and the check itself,
/// with an `Interrupted` error.
///
/// The torrent shares the client's listening port, DHT node, rate limits,
/// connection slots and turn to check the data on disk with the client's
/// other torrents, and fails if the client is running it already, or is
/// running a torrent that writes one of the same files, before it creates
/// or reads anything.
///
/// A download that is already complete on disk exits at once, unless it is
/// to be seeded.
pub async fn download_file(
    torrent_path: &Path,
    client: &Client,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let config = client.config();
    let torrent::Metainfo {
        torrent,
        info_hash,
        info_bytes,
    } = torrent::load_metainfo(torrent_path).await?;
    info!(
        name = %torrent.info.name,
        size = torrent.length(),
        pieces = torrent.num_pieces(),
        info_hash = %hex(&info_hash),
        "loaded torrent"
    );
    let mut claim = client.claim(info_hash)?;
    // Before anything is created, so a torrent that shares a file with a
    // running one leaves its files alone.
    claim.claim_files(
        &torrent.info.name,
        &Storage::paths(&torrent.info, &config.output_dir)?,
    )?;

    // Before announcing, so a bad output directory does not leave the
    // tracker without a `stopped`.
    let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir)?);
    let dht = start_dht(&torrent, client).await;
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: client.peer_id(),
        storage,
        seed: config.seed,
        dht,
        rate_limits: client.rate_limits(),
        metadata: Some(info_bytes),
        bars: client.bars().clone(),
        resume: Target::new(config, info_hash),
    });

    let mut shutdown = std::pin::pin!(shutdown);
    let have =
        resume::initial_pieces(&session, config, client.hashing(), shutdown.as_mut()).await?;
    let left = session.torrent.length() - verified_bytes(&session.torrent, &have)?;
    if left == 0 {
        info!(output = %config.output_dir.display(), "already complete");
        if !config.seed.enabled() {
            return Ok(());
        }
    }

    let incoming = client.listener().await.register(info_hash)?;
    let port = incoming.port().unwrap_or(config.listen_port);
    let request = AnnounceRequest {
        info_hash,
        peer_id: session.peer_id,
        port,
        uploaded: 0,
        downloaded: 0,
        left,
        event: AnnounceEvent::Started,
    };
    let tiers = session.torrent.tracker_tiers();
    let (announcer, peers) = match Announcer::start(tiers, request).await {
        Ok((announcer, peers)) => (Some(announcer), peers),
        // No trackers, or none answered: the DHT may still find peers.
        Err(e) if session.dht.is_some() => {
            info!("{e:#}; looking for peers in the DHT only");
            (None, Vec::new())
        }
        Err(e) => return Err(e),
    };
    // A seed can wait for peers to connect to it, and the DHT may still find
    // some.
    if peers.is_empty() && left > 0 && session.dht.is_none() {
        if let Some(announcer) = announcer {
            announcer.stop(&Transferred::default()).await;
        }
        bail!("no tracker returned any peers");
    }
    if announcer.is_some() {
        info!(peers = peers.len(), "tracker announce succeeded");
    }

    let mut start = StartPeers::default();
    start.pool.add(peers);
    download_with(
        session,
        &have,
        start,
        Limits {
            max_peers: config.max_peers,
            slots: client.slots(),
        },
        incoming,
        announcer,
        shutdown,
    )
    .await?;
    info!(output = %config.output_dir.display(), "done");
    Ok(())
}

/// The client's DHT node, unless disabled or the torrent is private. It
/// bootstraps once a download looks for peers, or within a minute.
async fn start_dht(torrent: &TorrentFile, client: &Client) -> Option<Dht> {
    if !client.config().dht {
        return None;
    }
    if torrent.is_private() {
        info!("private torrent: not using the DHT");
        return None;
    }
    client.dht().await
}

/// Listens for incoming peers on `port`, or on a free port if that one is
/// taken (say, by another download): over IPv4, and over IPv6 on the port
/// the IPv4 listener got, so trackers can be told a single port. The IPv4
/// listener comes first. Without IPv6 we listen over IPv4 only, and without
/// any listener we still download, over the connections we open ourselves.
pub async fn listen(port: u16) -> Vec<TcpListener> {
    let bind = |port| TcpListener::bind((Ipv4Addr::UNSPECIFIED, port));
    let result = match bind(port).await {
        Err(e) if port != 0 => {
            warn!(port, "cannot listen on port: {e}; using a free one instead");
            bind(0).await
        }
        result => result,
    };
    let v4 = result
        .inspect_err(|e| warn!("cannot listen for peers: {e}"))
        .ok();
    let port = match v4.as_ref().map(TcpListener::local_addr) {
        Some(Ok(addr)) => addr.port(),
        _ => port,
    };
    let v6 = listen_v6(port)
        .inspect_err(|e| info!(port, "cannot listen for IPv6 peers: {e}"))
        .ok();
    v4.into_iter().chain(v6).collect()
}

/// Listens on `[::]:port` for IPv6 only, so that the socket does not also
/// claim the port for IPv4.
fn listen_v6(port: u16) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(true)?;
    // As `TcpListener::bind` does on Unix, so a restart can reuse the port.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
}

/// Verifies a download against its torrent without changing anything on
/// disk, and prints which files are missing, the wrong size or corrupt.
/// Fails unless every piece is intact.
pub async fn check_file(torrent_path: &Path, output_dir: &Path) -> Result<()> {
    let (torrent, info_hash) = torrent::load_file(torrent_path).await?;
    let storage = Arc::new(Storage::open_existing(&torrent.info, output_dir)?);
    let pieces = verify::present(&torrent, &storage)?;
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: peer::generate_peer_id(),
        storage,
        seed: SeedLimits::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: MultiProgress::new(),
        resume: None,
    });
    // Nothing else is hashing, and nothing stops the check.
    let have = hash_pieces(&session, pieces, &Semaphore::new(1), std::future::pending()).await?;

    let report = verify::Report::new(&session.torrent, &session.storage, &have);
    print!("{report}");
    if !report.is_complete() {
        bail!("{} is incomplete", session.torrent.info.name);
    }
    Ok(())
}

/// Hashes `pieces` on disk, showing progress, and returns which pieces of
/// the torrent are there and intact.
///
/// Only the holder of `hashing`'s one permit hashes at a time, so torrents
/// that share it take turns; the bar says `queued for checking` while this
/// one waits. If `shutdown` resolves while it waits or hashes, the check
/// stops at the next piece and the result is an `Interrupted` error.
pub(crate) async fn hash_pieces(
    session: &Arc<Session>,
    pieces: Vec<usize>,
    hashing: &Semaphore,
    shutdown: impl Future<Output = ()>,
) -> Result<Vec<bool>> {
    let torrent = &session.torrent;
    if pieces.is_empty() {
        return Ok(vec![false; torrent.num_pieces()]);
    }
    let total = pieces
        .iter()
        .map(|&index| torrent.piece_len(index))
        .sum::<Result<u64>>()?;
    let bar = ProgressBar::new(total);
    bar.set_prefix(bar_label(&torrent.info.name));
    if let Ok(style) = ProgressStyle::with_template("{prefix} queued for checking") {
        bar.set_style(style);
    }
    let bar = session.bars.add(bar);
    // Drawn now, not at its first update, which only comes with our turn.
    bar.tick();
    let interrupted = || Interrupted {
        remaining: torrent.num_pieces(),
        pieces: torrent.num_pieces(),
    };
    let mut shutdown = std::pin::pin!(shutdown);

    let turn = tokio::select! {
        turn = hashing.acquire() => turn?,
        () = &mut shutdown => {
            bar.finish_and_clear();
            session.bars.remove(&bar);
            bail!(interrupted());
        }
    };
    let style = ProgressStyle::with_template(
        "checking {prefix} {bar:40.cyan/blue} {bytes}/{total_bytes} eta {eta}",
    );
    if let Ok(style) = style {
        bar.set_style(style);
    }
    bar.reset_eta();

    let blocking = Arc::clone(session);
    let check_bar = bar.clone();
    let cancel = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::clone(&cancel);
    let mut check = tokio::task::spawn_blocking(move || {
        verify::verify(
            &blocking.torrent,
            &blocking.storage,
            &pieces,
            &cancelled,
            &|len| check_bar.inc(len),
        )
    });
    let result = tokio::select! {
        result = &mut check => result?,
        () = &mut shutdown => {
            cancel.store(true, Ordering::Relaxed);
            // The threads stop at their next piece.
            let _ = check.await;
            Err(interrupted().into())
        }
    };
    drop(turn);
    bar.finish_and_clear();
    session.bars.remove(&bar);
    let have = result?;

    let done = have.iter().filter(|&&have| have).count();
    info!(done, pieces = have.len(), "checked data on disk");
    Ok(have)
}

/// Total length of the pieces marked in `have`.
pub(crate) fn verified_bytes(torrent: &TorrentFile, have: &[bool]) -> Result<u64> {
    have.iter()
        .enumerate()
        .filter(|(_, have)| **have)
        .map(|(index, _)| torrent.piece_len(index))
        .sum()
}

/// Downloads every piece not marked in `have` from `peers`, connected to at
/// most `max_peers` at once. A peer that drops is replaced by the next one,
/// and failed peers are retried with backoff (see `PeerPool`).
///
/// Peers that connect to us on any of `listeners` are downloaded from too,
/// while there is room under `max_peers`. One that misbehaves is turned away
/// by IP address from then on.
///
/// Every connected peer may download the pieces we have, as far as the
/// choker (see `choker`) unchokes it.
///
/// Once every piece is done, we seed until a limit in `session.seed` is
/// reached (or not at all without one): workers keep serving their peers,
/// new peers are still taken on, and running out of peers no longer ends
/// the run. A peer that is a seed too is let go.
///
/// With an `announcer`, the tracker is re-announced to on its interval and
/// the peers it returns join the pool. If we run out of peers while
/// downloading, the tracker is asked for more once before giving up. The
/// tracker is sent `completed` when the download finishes (not if it was
/// complete from the start) and `stopped` at the end, with the bytes
/// downloaded and uploaded.
///
/// With a DHT node in the session, the DHT is searched for peers at once
/// and then every few minutes (see `DhtSearch`), announcing our listening
/// port, and the peers found join the pool. When we run out of peers, the
/// DHT gets two more lookups (the first may have started earlier) before we
/// give up.
///
/// Peers that support peer exchange (BEP 11) are told which peers we are
/// connected to, and the peers they tell us about join the pool (see
/// `pex`), unless the torrent is private.
///
/// When `shutdown` resolves, no more blocks are requested and workers exit,
/// each finishing any piece write it has started. The tracker is still sent
/// `stopped`, and the result is an `Interrupted` error unless every piece
/// was done.
pub async fn download_from_peers(
    session: Arc<Session>,
    have: &[bool],
    peers: &[SocketAddr],
    max_peers: usize,
    listeners: Vec<TcpListener>,
    announcer: Option<Announcer>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let mut start = StartPeers::default();
    start.pool.add(peers.iter().copied());
    let incoming = Listener::start(listeners).register(session.info_hash)?;
    // Slots of its own, as many as the peers it may have.
    let limits = Limits {
        max_peers,
        slots: ConnectionSlots::new(max_peers),
    };
    download_with(session, have, start, limits, incoming, announcer, shutdown).await
}

/// The peers a download starts with.
#[derive(Default)]
pub(crate) struct StartPeers {
    /// Peers to connect to, with those already connected or banned marked.
    pub(crate) pool: PeerPool,
    /// Addresses of incoming peers that misbehaved.
    pub(crate) banned_ips: HashSet<IpAddr>,
    /// Connections already open, such as those the metadata of a magnet
    /// link came over, each with whether the peer connected to us and the
    /// connection slot it holds.
    pub(crate) connections: Vec<(PeerConnection, bool, SlotPermit)>,
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
    let seed = session.seed;
    // Private torrents (BEP 27) get their peers from trackers only.
    let (pex, mut pex_peers) = PeerExchange::new(!torrent.is_private());
    let swarm = Arc::new(Swarm {
        queue: PieceQueue::new(have).seeding(seed.enabled()),
        progress: Progress::new(
            &session.bars,
            &torrent.info.name,
            torrent.length(),
            verified_bytes(torrent, have)?,
        ),
        connected: ConnectedPeers::default(),
        choker: Choker::default(),
        pex,
    });
    let Swarm {
        queue,
        progress,
        choker,
        ..
    } = &*swarm;
    // Incoming peers have no address we could ban in the pool.
    let StartPeers {
        mut pool,
        mut banned_ips,
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
            let result = peer_worker(&session, &swarm, addr, link, listen_port, &mut pieces).await;
            WorkerExit {
                addr,
                inbound,
                pieces,
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
    // Whether the download finishes this session, rather than having been
    // complete on disk.
    let downloading = !queue.is_done();
    // When we finished downloading and started seeding.
    let mut seeding_since = None;
    // Pieces completed this session as of the last save of the resume file,
    // and when the next one is due.
    let mut saved = 0;
    let mut next_save = Instant::now() + RESUME_SAVE_INTERVAL;
    loop {
        if queue.is_done() {
            let since = match seeding_since {
                Some(since) => since,
                None => {
                    if let Some(announcer) = announcer.as_mut().filter(|_| downloading) {
                        announcer.complete();
                    }
                    if !seed.enabled() {
                        break;
                    }
                    info!(ratio = seed.ratio, time = ?seed.time, "download complete, seeding");
                    *seeding_since.insert(Instant::now())
                }
            };
            let uploaded = progress.transferred.uploaded.load(Ordering::Relaxed);
            progress
                .bar
                .set_message(format!("seeding, {} up", HumanBytes(uploaded)));
            if seed.reached(uploaded, torrent.length(), since.elapsed()) {
                info!(uploaded, "seed limit reached");
                break;
            }
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
            spawn(&mut workers, addr, Link::Connect, permit);
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
                _ => break,
            }
        }
        tokio::select! {
            Some(joined) = workers.join_next() => {
                let WorkerExit { addr, inbound, pieces, result } = match joined {
                    Ok(exit) => exit,
                    Err(e) => {
                        warn!("peer task failed: {e}");
                        continue;
                    }
                };
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
                    Err(e) => {
                        debug!(%addr, pieces, "peer dropped: {e:#}");
                        // We cannot connect back to the port an incoming
                        // peer came from.
                        if !inbound {
                            pool.failed(addr, pieces > 0, Instant::now());
                        }
                    }
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
                    spawn(&mut workers, addr, Link::Accepted(peer), permit);
                } else {
                    debug!(%addr, "turning away incoming peer: no free connection slot");
                }
            }
            () = &mut freed => {}
            () = sleep_until_some(retry_at) => {}
            peers = reannounce(&mut announcer, &progress.transferred) => {
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.tracker = false;
                }
            }
            peers = dht_lookup(&mut dht) => {
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.dht_lookups = asked.dht_lookups.saturating_sub(1);
                }
            }
            Some(peers) = pex_peers.recv() => {
                if pool.ready_len() < MAX_PEX_BACKLOG {
                    pool.add(peers);
                }
            }
            () = &mut choking => {}
            () = queue.wait_for(|| queue.is_done()), if seeding_since.is_none() => {}
            () = tokio::time::sleep(SEED_CHECK), if seeding_since.is_some() => {}
            () = tokio::time::sleep_until(next_save), if session.resume.is_some() => {
                save_resume(&session, queue, &mut saved).await;
                next_save = Instant::now() + RESUME_SAVE_INTERVAL;
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
    let drain = async { while workers.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        workers.shutdown().await;
    }

    // Every finished write is in the files now.
    save_resume(&session, queue, &mut saved).await;

    let remaining = queue.remaining();
    if remaining == 0 {
        progress.bar.finish();
    } else {
        // `finish` would fill the bar.
        progress.bar.abandon();
    }
    if let Some(announcer) = announcer {
        announcer.stop(&progress.transferred).await;
    }
    if remaining > 0 {
        if interrupted {
            bail!(Interrupted {
                remaining,
                pieces: num_pieces
            });
        }
        bail!("download incomplete: {remaining} of {num_pieces} pieces missing and no peers left");
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
enum Link {
    /// By connecting to it.
    Connect,
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
    fn is_inbound(&self) -> bool {
        match self {
            Link::Connect => false,
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

/// Accepts the next incoming connection on any of `listeners`, or waits
/// forever without one. Cancel-safe.
pub(crate) async fn accept(listeners: &[TcpListener]) -> (TcpStream, SocketAddr) {
    let next = || {
        // Start at a random listener, so a busy one cannot starve the rest.
        let start = rand::rng().random_range(0..listeners.len().max(1));
        std::future::poll_fn(move |cx| {
            listeners
                .iter()
                .cycle()
                .skip(start)
                .take(listeners.len())
                .find_map(|listener| match listener.poll_accept(cx) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                })
                .map_or(Poll::Pending, Poll::Ready)
        })
    };
    loop {
        match next().await {
            Ok(incoming) => return incoming,
            Err(e) => {
                warn!("cannot accept a peer: {e}");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

/// What the peer workers of a download share.
struct Swarm {
    queue: PieceQueue,
    progress: Progress,
    connected: ConnectedPeers,
    /// Decides which peers may download from us.
    choker: Choker,
    /// Tells peers about each other (BEP 11).
    pex: PeerExchange,
}

/// How a peer worker ended.
struct WorkerExit {
    addr: SocketAddr,
    /// Whether the peer connected to us.
    inbound: bool,
    /// Pieces the peer delivered first.
    pieces: usize,
    result: Result<()>,
}

/// Ids of the peers we are connected to, so that a peer we connect to and
/// that also connects to us is only downloaded from once.
#[derive(Default)]
pub(crate) struct ConnectedPeers(Mutex<HashSet<[u8; 20]>>);

impl ConnectedPeers {
    fn lock(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a connection to the peer with id `id` until the returned
    /// handle is dropped, or returns `None` if there already is one.
    pub(crate) fn register(&self, id: [u8; 20]) -> Option<Connected<'_>> {
        let new = self.lock().insert(id);
        new.then_some(Connected { peers: self, id })
    }
}

pub(crate) struct Connected<'a> {
    peers: &'a ConnectedPeers,
    id: [u8; 20],
}

impl Drop for Connected<'_> {
    fn drop(&mut self) {
        self.peers.lock().remove(&self.id);
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

/// A peer that broke the protocol or sent bad data, or turned out to be us.
/// It is never retried.
#[derive(Debug)]
pub(crate) struct Misbehaved(pub(crate) &'static str);

impl std::fmt::Display for Misbehaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Misbehaved {}

/// The download was stopped by its shutdown signal before every piece was
/// done. What was verified is on disk for the next run to resume from.
#[derive(Debug)]
pub struct Interrupted {
    /// Pieces still missing.
    pub remaining: usize,
    /// Pieces in the torrent.
    pub pieces: usize,
}

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "download interrupted with {} of {} pieces missing",
            self.remaining, self.pieces
        )
    }
}

impl std::error::Error for Interrupted {}

/// Bytes verified and written, shown on the progress bar, and bytes moved
/// either way, reported to the tracker.
struct Progress {
    bar: ProgressBar,
    transferred: Transferred,
}

impl Progress {
    /// A bar in `bars` for `total` bytes of the torrent called `name`, of
    /// which `done` were already on disk.
    fn new(bars: &MultiProgress, name: &str, total: u64, done: u64) -> Self {
        let bar = bars.add(ProgressBar::new(total));
        let style = ProgressStyle::with_template(
            "{prefix} {bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta} {msg}",
        );
        if let Ok(style) = style {
            bar.set_style(style);
        }
        bar.set_prefix(bar_label(name));
        // Moving the bar records `done` as a step, and resetting the ETA
        // drops it again, so data from disk does not count towards the rate.
        bar.set_position(done);
        bar.reset_eta();
        Self {
            bar,
            transferred: Transferred::default(),
        }
    }

    /// Counts a verified piece.
    fn add(&self, bytes: u64) {
        self.bar.inc(bytes);
        self.transferred
            .downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Counts a block sent to a peer.
    fn uploaded(&self, bytes: u64) {
        self.transferred
            .uploaded
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Downloads from one peer, and uploads to it while the choker unchokes it,
/// until the torrent is complete, the queue is closed or the peer fails.
/// Reaches the peer at `addr` over `link`. `listen_port` is where we accept
/// peers, if anywhere. `pieces_done` counts the pieces this peer delivered
/// first, even on error.
async fn peer_worker(
    session: &Session,
    swarm: &Swarm,
    addr: SocketAddr,
    link: Link,
    listen_port: Option<u16>,
    pieces_done: &mut usize,
) -> Result<()> {
    let Swarm {
        queue,
        progress,
        connected,
        choker,
        pex,
    } = swarm;
    let torrent = &session.torrent;
    let (info_hash, peer_id, num_pieces) =
        (session.info_hash, session.peer_id, torrent.num_pieces());
    let inbound = link.is_inbound();
    // Set for a connection the metadata of a magnet link came over, on
    // which we have already said we had no piece.
    let reopened = matches!(link, Link::Open { .. });
    let advertised = Advertised {
        listen_port,
        metadata_size: session
            .metadata
            .as_ref()
            .and_then(|metadata| u32::try_from(metadata.len()).ok()),
        private: torrent.is_private(),
    };
    let mut conn = match link {
        Link::Accepted(incoming) => {
            PeerConnection::answer(incoming, peer_id, num_pieces, advertised).await?
        }
        Link::Connect => {
            PeerConnection::connect(addr, info_hash, peer_id, num_pieces, advertised).await?
        }
        Link::Open { conn, .. } => *conn,
    };
    if conn.peer_id == peer_id {
        // A tracker listed our own address.
        bail!(Misbehaved("connected to ourselves"));
    }
    let Some(_connected) = connected.register(conn.peer_id) else {
        bail!("already connected to this peer");
    };
    // Whether both sides support the fast extension (BEP 6): then every
    // request gets a Piece or a Reject both ways, and a choke no longer
    // drops requests.
    let fast = conn.supports_fast();
    debug!(%addr, inbound, fast, "connected");
    let mut pex = pex.join(conn.peer_id, addr, !inbound);

    // Our pieces, first thing after the handshakes (see
    // `fast::have_message`), or one Have each on a connection that heard we
    // had none. Pieces completed from now on are announced with Have.
    let (have, mut announced) = if reopened {
        queue.have_messages()
    } else {
        let (have, announced) = queue.have_message(fast);
        (have.into_iter().collect(), announced)
    };
    if !have.is_empty() {
        conn.send_all(&have).await?;
    }
    // BEP 5: tell a peer that runs a DHT node where ours listens.
    if let Some(dht) = &session.dht
        && conn.supports_dht()
    {
        conn.send(&PeerMessage::Port { port: dht.port() }).await?;
    }

    let mut pieces = queue.register_peer();
    let mut slot = choker.register();
    // Pieces we hold a claim on, with requests spread over all of them so the
    // pipeline does not drain at every piece boundary.
    let mut active: Vec<ActivePiece<'_>> = Vec::new();
    let mut pipeline = Pipeline::new(Instant::now());
    let mut hash_failures = 0;
    let mut last_useful = Instant::now();
    // Set while we are interested but choked.
    let mut unchoke_deadline = None;
    let mut has_changed = true;
    // Blocks the peer asked us for, sent one per turn of the loop.
    let mut uploads = Uploads::default();
    // Messages handled since the last block we sent, and whether the last
    // look for one found nothing waiting.
    let mut messages_since_block = 0;
    let mut drained = false;
    // This peer's turns at the session's rate limits: for the blocks we ask
    // it for, and for the blocks it asks us for.
    let mut download_budget = session.rate_limits.download.allowance();
    let mut upload_budget = session.rate_limits.upload.allowance();
    // Fast extension: the allowed fast set we gave the peer, decided when it
    // first tells us its pieces (`None` until then, and empty unless it was
    // just starting out). Its requests for these pieces are served even
    // while we choke it.
    // A connection the metadata of a magnet link came over has had its
    // say on its pieces already.
    let mut offered: Option<Vec<u32>> = if reopened && fast {
        Some(offer_allowed_fast(&mut conn, &info_hash).await?)
    } else {
        None
    };
    // Fast extension: requests we cancelled that the peer has not answered
    // yet. It still answers each, with the block or a Reject.
    let mut cancelled: HashSet<BlockRequest> = HashSet::new();
    loop {
        // Registered before anything is checked, so that a change in the
        // queue from here on still wakes the idle wait below.
        let mut changed = std::pin::pin!(queue.changed.notified());
        changed.as_mut().enable();

        drop_completed(&mut conn, &mut active, fast.then_some(&mut cancelled)).await?;
        announce_pieces(&mut conn, queue, &mut announced).await?;
        if queue.is_finished() {
            return Ok(());
        }
        if has_changed {
            pieces.update(conn.peer_has());
            pex.set_seed(conn.peer_has().iter().all(|&has| has));
            has_changed = false;
        }
        pex.tick(&mut conn, Instant::now()).await?;
        if queue.is_done() && conn.peer_has().iter().all(|&has| has) {
            debug!(%addr, "both seeding, letting go");
            return Ok(());
        }
        // Interested only while the peer has a piece we still need, even if
        // another peer is downloading it right now. Any piece we hold counts.
        let wanted = !active.is_empty() || queue.wants(&pieces);
        conn.set_interested(wanted).await?;
        // The choker hears of interest changes at once, and its answer
        // wakes the waits below.
        slot.set_interested(conn.peer_interested());
        let choking = !slot.is_unchoked();
        conn.set_choking(choking).await?;
        if choking {
            // A choke drops the peer's requests, except those for its
            // allowed fast pieces. With the fast extension each dropped one
            // is rejected, after the Choke as BEP 6 asks; without it there
            // is no way to reject them one by one.
            let allowed = offered.as_deref().unwrap_or_default();
            let dropped = uploads.retain(|request| allowed.contains(&request.index));
            if fast && !dropped.is_empty() {
                let rejects: Vec<PeerMessage> =
                    dropped.into_iter().map(BlockRequest::reject).collect();
                conn.send_all(&rejects).await?;
            }
        }
        let now = Instant::now();
        // Worth keeping while either side wants something from the other.
        if wanted || conn.peer_interested() {
            last_useful = now;
        }
        unchoke_deadline = match unchoke_deadline {
            _ if !wanted || !conn.is_choked() => None,
            Some(deadline) => Some(deadline),
            None => Some(now + UNCHOKE_TIMEOUT),
        };
        if fast && conn.is_choked() {
            release_choked(&mut active, conn.allowed_fast());
        }
        // While choked, the fast extension still lets us ask for the pieces
        // the peer allowed.
        if !conn.is_choked() || (fast && conn.allowed_fast().contains(&true)) {
            request_blocks(
                &mut conn,
                queue,
                &pieces,
                torrent,
                &mut active,
                &mut pipeline,
                &mut download_budget,
            )
            .await?;
        } else {
            // Nothing to ask for until unchoked: the turn goes to others.
            download_budget.release();
        }

        // Send the next block the peer asked for once the messages that had
        // already arrived are handled, so that a Cancel among them is
        // honored, or after `MAX_MESSAGES_PER_BLOCK` of them, so that a peer
        // that keeps sending still gets its blocks. Under an upload limit,
        // the block also waits for this peer's turn.
        if uploads.is_empty() {
            messages_since_block = 0;
            upload_budget.release();
        } else if (drained || messages_since_block >= MAX_MESSAGES_PER_BLOCK)
            && let Some(&request) = uploads.front()
            && upload_budget.try_take(request.length.into(), Instant::now())
        {
            uploads.pop();
            serve_block(&mut conn, &session.storage, request).await?;
            slot.uploaded(request.length.into());
            progress.uploaded(request.length.into());
            messages_since_block = 0;
        }

        // When our next turn at a rate limit comes, if we are waiting for
        // one. A turn that has come but is not used yet (say, the pipeline
        // is full) is not waited for.
        let waiting_since = Instant::now();
        let next_turn = [&download_budget, &upload_budget]
            .into_iter()
            .filter(|budget| budget.is_waiting(waiting_since))
            .filter_map(|budget| budget.ready_at())
            .min();
        // Set if the wait below ends because our turn came, rather than
        // because nothing was left to read.
        let mut turn_came = false;

        // `receive` and `receive_untimed` are cancel-safe, so losing a race
        // loses no data.
        let message = if !uploads.is_empty() && !upload_budget.is_waiting(waiting_since) {
            // More blocks to send: only take a message that is already here.
            tokio::select! {
                biased;
                message = conn.receive_untimed() => Some(message?),
                () = std::future::ready(()) => None,
            }
        } else if active.is_empty() {
            if unchoke_deadline.is_some_and(|deadline| now >= deadline) {
                bail!("peer did not unchoke us");
            }
            if now - last_useful > IDLE_TIMEOUT {
                bail!("neither side has pieces the other needs");
            }
            conn.keep_alive().await?;
            // Wait for an unchoke, for a piece to be returned or completed,
            // for the queue to close, for the peer to announce a new piece or
            // ask for one of ours, or for the choker to (un)choke it.
            tokio::select! {
                () = &mut changed => None,
                message = conn.receive_untimed() => Some(message?),
                () = slot.changed() => None,
                _ = tokio::time::sleep(IDLE_POLL) => None,
                () = sleep_until_some(next_turn) => {
                    turn_came = true;
                    None
                }
            }
        } else {
            conn.keep_alive().await?;
            // A piece completed by any worker is announced to this peer.
            let woken = || {
                queue.is_closed()
                    || queue.completed_count() > announced
                    || active.iter().any(|piece| piece.claim.is_completed())
            };
            // With nothing in flight, we are waiting for our turn at the
            // download limit, so the peer has nothing to send and its
            // silence is no reason to drop it.
            let in_flight: usize = active.iter().map(ActivePiece::in_flight).sum();
            let receive = async {
                if in_flight > 0 {
                    conn.receive().await
                } else {
                    conn.receive_untimed().await
                }
            };
            tokio::select! {
                message = receive => Some(message?),
                () = queue.wait_for(woken) => None,
                () = slot.changed() => None,
                () = sleep_until_some(next_turn) => {
                    turn_came = true;
                    None
                }
            }
        };
        // Messages may have arrived while we waited for our turn, and are
        // handled before the next block goes out.
        drained = message.is_none() && !turn_came;
        let Some(message) = message else {
            continue;
        };
        messages_since_block += 1;

        match message {
            PeerMessage::Have { .. } => has_changed = true,
            PeerMessage::Bitfield { .. } | PeerMessage::HaveAll | PeerMessage::HaveNone => {
                has_changed = true;
                // The peer's first word on its pieces: if it is just
                // starting out, it gets our allowed fast set.
                if fast && offered.is_none() {
                    offered = Some(offer_allowed_fast(&mut conn, &info_hash).await?);
                }
            }
            PeerMessage::Port { port } => {
                if let Some(dht) = &session.dht {
                    dht.add_node(SocketAddr::new(addr.ip(), port));
                }
            }
            PeerMessage::Extended {
                id: pex::ID,
                payload,
            } => pex.received(&payload, Instant::now(), queue.is_done())?,
            // Without the fast extension a choke drops our requests:
            // dropping the claims returns the pieces, and the next turn
            // waits for an unchoke. With it they stand until rejected (see
            // `release_choked`).
            PeerMessage::Choke if !fast && !active.is_empty() => {
                debug!(%addr, pieces = active.len(), "choked mid-download");
                active.clear();
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let answered = BlockRequest {
                    index,
                    begin,
                    length: block.len() as u32,
                };
                // Blocks of pieces we no longer hold are late replies to
                // requests abandoned on a choke or cancelled in endgame.
                let Some(pos) = active.iter().position(|piece| piece.index == index) else {
                    cancelled.remove(&answered);
                    continue;
                };
                if !active[pos].add_block(begin, &block)? {
                    cancelled.remove(&answered);
                    continue;
                }
                pipeline.received(block.len() as u64, Instant::now());
                slot.downloaded(block.len() as u64);
                if !active[pos].is_complete() {
                    continue;
                }
                let index = active[pos].claim.index;
                let Some((claim, data)) = active.swap_remove(pos).verify() else {
                    hash_failures += 1;
                    warn!(%addr, index, "piece failed hash check");
                    if hash_failures >= MAX_HASH_FAILURES {
                        bail!(Misbehaved("too many corrupt pieces"));
                    }
                    continue;
                };
                // In endgame another peer may have finished it meanwhile. If
                // both pass this check, both write the same verified bytes.
                if claim.is_completed() {
                    continue;
                }
                let piece_len = data.len() as u64;
                session.storage.write_piece(index, data).await?;
                if claim.complete() {
                    *pieces_done += 1;
                    progress.add(piece_len);
                    debug!(%addr, index, "piece complete");
                }
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // We only ever advertise pieces we have, so asking for
                // another is a protocol violation.
                if !queue.has(index as usize) {
                    bail!(Misbehaved("peer requested a piece we do not have"));
                }
                request.check(torrent.piece_len(index as usize)?)?;
                let allowed = offered.as_ref().is_some_and(|set| set.contains(&index));
                if conn.am_choking() && !allowed {
                    // Sent before our Choke arrived, which dropped it. With
                    // the fast extension it is rejected.
                    debug!(%addr, ?request, "turning down request from choked peer");
                    if fast {
                        conn.send(&request.reject()).await?;
                    }
                } else if !uploads.push(request) {
                    debug!(%addr, ?request, "dropping request: too many queued");
                    if fast {
                        conn.send(&request.reject()).await?;
                    }
                }
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // With the fast extension even a cancelled request gets an
                // answer: a Reject if its block was not sent yet.
                if uploads.cancel(&request) && fast {
                    conn.send(&request.reject()).await?;
                }
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // The answer to a request we cancelled.
                if cancelled.remove(&request) {
                    continue;
                }
                let rejected = active
                    .iter_mut()
                    .find(|piece| piece.index == index)
                    .is_some_and(|piece| piece.reject(begin, length));
                if !rejected {
                    bail!(Misbehaved("peer rejected a request we did not send"));
                }
                debug!(%addr, ?request, choked = conn.is_choked(), "request rejected");
                // The block is asked for again: at once while unchoked, and
                // by whoever claims the piece next once it is released. A
                // piece the peer allowed us while choked but rejects anyway
                // is not asked for again until it unchokes us, or we would
                // ask over and over.
                if conn.is_choked() {
                    conn.disallow_fast(index);
                }
            }
            // A hint we do without: rarest first picks our pieces.
            PeerMessage::SuggestPiece { .. } => {}
            PeerMessage::Extended {
                id: extension::UT_METADATA_ID,
                payload,
            } => metadata::serve(&mut conn, session.metadata.as_deref(), &payload).await?,
            _ => {}
        }
    }
}

/// Sends Have for every piece completed since the last call, except those
/// the peer has itself (Have suppression, which the spec allows).
async fn announce_pieces(
    conn: &mut PeerConnection,
    queue: &PieceQueue,
    announced: &mut usize,
) -> Result<()> {
    let completed = queue.completed_since(*announced);
    *announced += completed.len();
    let peer_has = conn.peer_has();
    let haves = completed
        .into_iter()
        .filter(|&index| !peer_has.get(index).copied().unwrap_or(false))
        .map(|index| {
            let piece_index = u32::try_from(index).map_err(|_| anyhow!("piece index too large"))?;
            Ok(PeerMessage::Have { piece_index })
        })
        .collect::<Result<Vec<_>>>()?;
    if !haves.is_empty() {
        conn.send_all(&haves).await?;
    }
    Ok(())
}

/// Reads a block the peer asked for from disk and sends it.
async fn serve_block(
    conn: &mut PeerConnection,
    storage: &Arc<Storage>,
    request: BlockRequest,
) -> Result<()> {
    let BlockRequest {
        index,
        begin,
        length,
    } = request;
    let block = storage.read_block(index as usize, begin, length).await?;
    conn.send(&PeerMessage::Piece {
        index,
        begin,
        block,
    })
    .await
}

/// A block request, ours or a peer's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct BlockRequest {
    index: u32,
    begin: u32,
    length: u32,
}

impl BlockRequest {
    /// The Cancel that withdraws this request.
    fn cancel(self) -> PeerMessage {
        let BlockRequest {
            index,
            begin,
            length,
        } = self;
        PeerMessage::Cancel {
            index,
            begin,
            length,
        }
    }

    /// The Reject (fast extension) that turns this request down.
    fn reject(self) -> PeerMessage {
        let BlockRequest {
            index,
            begin,
            length,
        } = self;
        PeerMessage::RejectRequest {
            index,
            begin,
            length,
        }
    }

    /// Checks that the block lies inside its piece, `piece_len` bytes long,
    /// and is no longer than `MAX_REQUEST_LEN`.
    fn check(&self, piece_len: u64) -> Result<()> {
        let end = u64::from(self.begin) + u64::from(self.length);
        if self.length == 0 || self.length > MAX_REQUEST_LEN || end > piece_len {
            bail!(Misbehaved("peer sent an invalid request"));
        }
        Ok(())
    }
}

/// Blocks a peer asked us for that have not been sent yet, oldest first.
#[derive(Default)]
struct Uploads(VecDeque<BlockRequest>);

impl Uploads {
    /// Queues a request, unless it is queued already. Returns false, dropping
    /// it, if `MAX_QUEUED_UPLOADS` requests are queued.
    fn push(&mut self, request: BlockRequest) -> bool {
        if self.0.contains(&request) {
            return true;
        }
        if self.0.len() >= MAX_QUEUED_UPLOADS {
            return false;
        }
        self.0.push_back(request);
        true
    }

    /// Drops a queued request, and returns whether it was queued. A block
    /// already sent cannot be called back, and the peer ignores it.
    fn cancel(&mut self, request: &BlockRequest) -> bool {
        let queued = self.0.len();
        self.0.retain(|queued| queued != request);
        self.0.len() < queued
    }

    fn pop(&mut self) -> Option<BlockRequest> {
        self.0.pop_front()
    }

    /// The request to answer next.
    fn front(&self) -> Option<&BlockRequest> {
        self.0.front()
    }

    /// Drops every queued request that `keep` returns false for, and
    /// returns the dropped ones, oldest first.
    fn retain(&mut self, keep: impl Fn(&BlockRequest) -> bool) -> Vec<BlockRequest> {
        let mut dropped = Vec::new();
        self.0.retain(|request| {
            let kept = keep(request);
            if !kept {
                dropped.push(*request);
            }
            kept
        });
        dropped
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Tops up the requests in flight to the pipeline depth, claiming another
/// piece whenever every block of the held ones has been requested. While
/// the peer chokes us, only its allowed fast pieces (fast extension) are
/// asked for.
///
/// Under a download limit, each request also waits for this peer's turn in
/// `budget`, so the data it asks for is paid for before it is sent and
/// never arrives faster than the limit. A turn not yet come is kept for the
/// next call, and given up if there is nothing left to claim.
async fn request_blocks<'q>(
    conn: &mut PeerConnection,
    queue: &'q PieceQueue,
    pieces: &PeerPieces<'_>,
    torrent: &TorrentFile,
    active: &mut Vec<ActivePiece<'q>>,
    pipeline: &mut Pipeline,
    budget: &mut Allowance<'_>,
) -> Result<()> {
    let in_flight: usize = active.iter().map(ActivePiece::in_flight).sum();
    if in_flight == 0 {
        // Time spent choked or with nothing to ask for says nothing about
        // the peer's speed.
        pipeline.restart(Instant::now());
    }
    // A peer that told us its queue size would drop requests beyond it.
    let depth = match conn.request_limit() {
        Some(limit) => pipeline.depth().min(limit),
        None => pipeline.depth(),
    };
    let allowed = conn.is_choked().then(|| conn.allowed_fast());
    let may_request = |piece: &ActivePiece<'_>| {
        piece.has_unrequested()
            && allowed.is_none_or(|allowed| allowed.get(piece.claim.index) == Some(&true))
    };
    let mut requests = Vec::new();
    while in_flight + requests.len() < depth {
        let pos = match active.iter().position(may_request) {
            Some(pos) => pos,
            None => {
                let held: Vec<usize> = active.iter().map(|piece| piece.claim.index).collect();
                let claim = match allowed {
                    None => queue.claim(pieces, &held),
                    Some(allowed) => queue.claim_allowed(pieces, &held, allowed),
                };
                let Some(claim) = claim else {
                    budget.release();
                    break;
                };
                let piece_len = torrent.piece_len(claim.index)?;
                let hash = torrent.piece_hash(claim.index)?;
                active.push(ActivePiece::new(claim, piece_len, hash)?);
                active.len() - 1
            }
        };
        if !budget.try_take(active[pos].next_block_len().into(), Instant::now()) {
            break;
        }
        requests.push(active[pos].request_next());
    }
    if !requests.is_empty() {
        conn.send_all(&requests).await?;
    }
    Ok(())
}

/// Lets go of held pieces that another worker has completed (endgame),
/// cancelling the requests still outstanding for them. With `cancelled`
/// (fast extension), the cancelled requests are added to it, as the peer
/// still answers them.
async fn drop_completed(
    conn: &mut PeerConnection,
    active: &mut Vec<ActivePiece<'_>>,
    mut cancelled: Option<&mut HashSet<BlockRequest>>,
) -> Result<()> {
    let mut cancels = Vec::new();
    active.retain(|piece| {
        let completed = piece.claim.is_completed();
        if completed {
            debug!(addr = %conn.addr, index = piece.claim.index, "piece completed by another peer");
            for request in piece.outstanding() {
                cancels.push(request.cancel());
                if let Some(cancelled) = cancelled.as_deref_mut() {
                    cancelled.insert(request);
                }
            }
        }
        !completed
    });
    if !cancels.is_empty() {
        conn.send_all(&cancels).await?;
    }
    Ok(())
}

/// While the peer chokes us under the fast extension, our requests stand
/// until it rejects them. Lets go of the held pieces we may not ask it for
/// meanwhile (those not in its allowed fast set) once none of their
/// requests are outstanding, so that other peers can claim them.
fn release_choked(active: &mut Vec<ActivePiece<'_>>, allowed: &[bool]) {
    active.retain(|piece| {
        let keep = piece.in_flight() > 0 || allowed.get(piece.claim.index) == Some(&true);
        if !keep {
            debug!(index = piece.claim.index, "choked: releasing piece");
        }
        keep
    });
}

/// Sends the peer our allowed fast set (fast extension) if it is just
/// starting out (see `fast::is_starting`), and returns the set, empty if
/// the peer did not get one. The set may name pieces we do not have yet;
/// the peer can only ask for those once we announce them.
async fn offer_allowed_fast(conn: &mut PeerConnection, info_hash: &[u8; 20]) -> Result<Vec<u32>> {
    let pieces = conn.peer_has().iter().filter(|&&has| has).count();
    if !fast::is_starting(pieces, conn.peer_has().len()) {
        return Ok(Vec::new());
    }
    let num_pieces = u32::try_from(conn.peer_has().len())?;
    let set = fast::allowed_fast_set(
        conn.addr.ip(),
        info_hash,
        num_pieces,
        fast::ALLOWED_FAST_COUNT,
    );
    let messages: Vec<PeerMessage> = set
        .iter()
        .map(|&piece_index| PeerMessage::AllowedFast { piece_index })
        .collect();
    if !messages.is_empty() {
        conn.send_all(&messages).await?;
    }
    debug!(addr = %conn.addr, ?set, "sent allowed fast set");
    Ok(set)
}

/// Sizes a peer's request pipeline to its measured download rate, so that
/// about `PIPELINE_QUEUE_TIME` worth of blocks is always on its way.
///
/// A deeper pipeline lets more data arrive per round trip, so the measured
/// rate grows until the link is the limit, and the depth follows it: a fast
/// peer gets a deep queue, a slow one a short queue (and so holds fewer
/// pieces hostage).
struct Pipeline {
    depth: usize,
    window_start: Instant,
    window_bytes: u64,
}

impl Pipeline {
    fn new(now: Instant) -> Self {
        Self {
            depth: INITIAL_PIPELINE,
            window_start: now,
            window_bytes: 0,
        }
    }

    fn depth(&self) -> usize {
        self.depth
    }

    /// Starts a fresh measurement window, keeping the current depth.
    fn restart(&mut self, now: Instant) {
        self.window_start = now;
        self.window_bytes = 0;
    }

    /// Counts a received block. Once `RATE_WINDOW` has passed, the depth is
    /// resized to the rate over the window.
    fn received(&mut self, bytes: u64, now: Instant) {
        self.window_bytes += bytes;
        let elapsed = now.saturating_duration_since(self.window_start).as_millis();
        if elapsed < RATE_WINDOW.as_millis() {
            return;
        }
        // rate * queue time / block size, in whole blocks.
        let blocks = (u128::from(self.window_bytes) * PIPELINE_QUEUE_TIME.as_millis())
            .div_ceil(elapsed * u128::from(BLOCK_SIZE));
        self.depth = usize::try_from(blocks)
            .unwrap_or(MAX_PIPELINE)
            .clamp(MIN_PIPELINE, MAX_PIPELINE);
        self.restart(now);
    }
}

/// Where a block of an `ActivePiece` stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BlockState {
    /// Not asked for, or asked for and rejected (fast extension).
    Wanted,
    /// Asked for and not yet answered.
    Requested,
    Received,
}

/// A claimed piece being downloaded. Blocks are requested in order, and a
/// rejected block is asked for again before any later one.
struct ActivePiece<'a> {
    claim: PieceClaim<'a>,
    /// `claim.index` as sent on the wire.
    index: u32,
    hash: [u8; 20],
    data: Vec<u8>,
    blocks: Vec<BlockState>,
    /// The first `Wanted` block, or `blocks.len()` if there is none.
    next_request: usize,
    /// Blocks `Requested`.
    in_flight: usize,
    /// Blocks `Received`.
    done: usize,
}

impl<'a> ActivePiece<'a> {
    fn new(claim: PieceClaim<'a>, piece_len: u64, hash: [u8; 20]) -> Result<Self> {
        let index = u32::try_from(claim.index).map_err(|_| anyhow!("piece index too large"))?;
        let piece_len = u32::try_from(piece_len).map_err(|_| anyhow!("piece too large"))?;
        Ok(Self {
            claim,
            index,
            hash,
            data: vec![0u8; piece_len as usize],
            blocks: vec![BlockState::Wanted; piece_len.div_ceil(BLOCK_SIZE) as usize],
            next_request: 0,
            in_flight: 0,
            done: 0,
        })
    }

    fn block_len(&self, block: usize) -> u32 {
        let begin = block as u32 * BLOCK_SIZE;
        BLOCK_SIZE.min(self.data.len() as u32 - begin)
    }

    /// The request for block `block`.
    fn block_request(&self, block: usize) -> BlockRequest {
        BlockRequest {
            index: self.index,
            begin: block as u32 * BLOCK_SIZE,
            length: self.block_len(block),
        }
    }

    /// The block that starts at `begin`, if one does.
    fn block_at(&self, begin: u32) -> Option<usize> {
        let block = (begin / BLOCK_SIZE) as usize;
        (begin.is_multiple_of(BLOCK_SIZE) && block < self.blocks.len()).then_some(block)
    }

    /// Requests sent and not yet answered.
    fn in_flight(&self) -> usize {
        self.in_flight
    }

    fn has_unrequested(&self) -> bool {
        self.next_request < self.blocks.len()
    }

    /// Length of the block `request_next` asks for. Call only if
    /// `has_unrequested`.
    fn next_block_len(&self) -> u32 {
        self.block_len(self.next_request)
    }

    /// The request for the first wanted block. Call only if
    /// `has_unrequested`.
    fn request_next(&mut self) -> PeerMessage {
        let block = self.next_request;
        self.blocks[block] = BlockState::Requested;
        self.in_flight += 1;
        self.next_request = (block + 1..self.blocks.len())
            .find(|&b| self.blocks[b] == BlockState::Wanted)
            .unwrap_or(self.blocks.len());
        let BlockRequest {
            index,
            begin,
            length,
        } = self.block_request(block);
        PeerMessage::Request {
            index,
            begin,
            length,
        }
    }

    /// Every request still outstanding.
    fn outstanding(&self) -> impl Iterator<Item = BlockRequest> + '_ {
        (0..self.blocks.len())
            .filter(|&block| self.blocks[block] == BlockState::Requested)
            .map(|block| self.block_request(block))
    }

    /// Takes back an outstanding request that the peer rejected (fast
    /// extension), so the block is asked for again. Returns false if no
    /// such request is outstanding.
    fn reject(&mut self, begin: u32, length: u32) -> bool {
        let Some(block) = self.block_at(begin) else {
            return false;
        };
        if self.blocks[block] != BlockState::Requested || length != self.block_len(block) {
            return false;
        }
        self.blocks[block] = BlockState::Wanted;
        self.in_flight -= 1;
        self.next_request = self.next_request.min(block);
        true
    }

    /// Stores a block of this piece. Returns false if it was not one we are
    /// waiting for: a duplicate, or a late reply to a request that was
    /// abandoned on a choke, or rejected, for a block we have not asked for
    /// again yet. Those are ignored rather than dropping an honest peer.
    fn add_block(&mut self, begin: u32, block: &[u8]) -> Result<bool> {
        let index = match self.block_at(begin) {
            Some(index) if block.len() == self.block_len(index) as usize => index,
            _ => bail!(Misbehaved("peer sent an invalid block")),
        };
        if self.blocks[index] != BlockState::Requested {
            return Ok(false);
        }
        let start = begin as usize;
        self.data[start..start + block.len()].copy_from_slice(block);
        self.blocks[index] = BlockState::Received;
        self.in_flight -= 1;
        self.done += 1;
        Ok(true)
    }

    fn is_complete(&self) -> bool {
        self.done == self.blocks.len()
    }

    /// The claim and data of a complete piece, or `None` if the data does
    /// not match the piece hash (dropping the claim).
    fn verify(self) -> Option<(PieceClaim<'a>, Vec<u8>)> {
        let actual: [u8; 20] = sha1::Sha1::digest(&self.data).into();
        (actual == self.hash).then_some((self.claim, self.data))
    }
}

/// Pieces not yet downloaded, shared between all peer workers.
struct PieceQueue {
    state: Mutex<QueueState>,
    /// Signalled when a piece is returned to the queue or completed, and
    /// when the queue is closed.
    changed: Notify,
}

struct QueueState {
    /// Pieces nobody is downloading.
    pending: BTreeSet<usize>,
    /// Pieces being downloaded but not yet completed, with how many workers
    /// hold a claim on each (more than one only in endgame).
    in_flight: BTreeMap<usize, u32>,
    /// Pieces not yet verified and written, whether pending or in flight.
    remaining: usize,
    /// How many connected peers have each piece.
    availability: Vec<u32>,
    /// Pieces verified and on disk, which peers may download from us.
    have: Vec<bool>,
    /// Pieces completed this session, in order, so that each worker can tell
    /// its peer about the ones it has not announced yet.
    completed: Vec<usize>,
    /// Set when the download stops: no more pieces are handed out, and
    /// workers exit.
    closed: bool,
    /// Whether workers keep serving their peers once every piece is done,
    /// until the queue is closed.
    seed: bool,
}

impl PieceQueue {
    /// A queue of every piece not marked in `have`.
    fn new(have: &[bool]) -> Self {
        let pending: BTreeSet<usize> = (0..have.len()).filter(|&i| !have[i]).collect();
        Self {
            state: Mutex::new(QueueState {
                remaining: pending.len(),
                pending,
                in_flight: BTreeMap::new(),
                availability: vec![0; have.len()],
                have: have.to_vec(),
                completed: Vec::new(),
                closed: false,
                seed: false,
            }),
            changed: Notify::new(),
        }
    }

    /// Keeps workers going once every piece is done if `seed` is set, so
    /// they can upload to their peers, until the queue is closed.
    fn seeding(mut self, seed: bool) -> Self {
        self.state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .seed = seed;
        self
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts counting a peer's pieces towards availability. Its pieces are
    /// uncounted again when the returned handle is dropped.
    fn register_peer(&self) -> PeerPieces<'_> {
        PeerPieces {
            queue: self,
            has: vec![false; self.lock().availability.len()],
        }
    }

    /// Takes the rarest pending piece the peer has, breaking ties at random.
    ///
    /// Endgame: once no piece is pending, every remaining piece is already
    /// being downloaded, so this joins the in-flight piece with the fewest
    /// downloaders instead. Whichever copy finishes first wins, so one slow
    /// peer can no longer hold up the end of the download.
    ///
    /// The piece goes back to the queue when its last claim is dropped
    /// without `complete()`. Pieces in `held`, which the caller already
    /// claimed, are never duplicated. A closed queue hands out nothing.
    fn claim(&self, peer: &PeerPieces<'_>, held: &[usize]) -> Option<PieceClaim<'_>> {
        self.claim_from(&peer.has, held)
    }

    /// Like `claim`, but only takes a piece marked in `allowed`: one the
    /// peer lets us download while it chokes us (fast extension).
    fn claim_allowed(
        &self,
        peer: &PeerPieces<'_>,
        held: &[usize],
        allowed: &[bool],
    ) -> Option<PieceClaim<'_>> {
        let has: Vec<bool> = peer
            .has
            .iter()
            .zip(allowed)
            .map(|(&has, &allowed)| has && allowed)
            .collect();
        self.claim_from(&has, held)
    }

    /// `claim` for a peer that has the pieces marked in `has`.
    fn claim_from(&self, has: &[bool], held: &[usize]) -> Option<PieceClaim<'_>> {
        let mut guard = self.lock();
        let state = &mut *guard;
        if state.closed {
            return None;
        }
        let mut rng = rand::rng();
        let index = if state.pending.is_empty() {
            let candidates = state
                .in_flight
                .iter()
                .filter(|(i, _)| !held.contains(i))
                .map(|(&i, &holders)| (i, holders));
            let index = pick_least(candidates, has, &mut rng)?;
            let holders = state.in_flight.entry(index).or_insert(0);
            *holders += 1;
            debug!(index, holders = *holders, "endgame: duplicating piece");
            index
        } else {
            let availability = &state.availability;
            let candidates = state
                .pending
                .iter()
                .map(|&i| (i, availability.get(i).copied().unwrap_or(0)));
            let index = pick_least(candidates, has, &mut rng)?;
            state.pending.remove(&index);
            state.in_flight.insert(index, 1);
            index
        };
        Some(PieceClaim {
            queue: self,
            index,
            completed: false,
        })
    }

    /// Whether the peer has a piece that is not yet completed, whether
    /// pending or in flight.
    fn wants(&self, peer: &PeerPieces<'_>) -> bool {
        let state = self.lock();
        let has = |i: &usize| peer.has.get(*i).copied().unwrap_or(false);
        state.pending.iter().any(has) || state.in_flight.keys().any(has)
    }

    /// Resolves once `done` returns true, checking again whenever a piece is
    /// returned or completed.
    async fn wait_for(&self, mut done: impl FnMut() -> bool) {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            // Register before checking, so a change in between still wakes
            // us.
            changed.as_mut().enable();
            if done() {
                return;
            }
            changed.await;
        }
    }

    /// Whether piece `index` is verified and on disk.
    fn has(&self, index: usize) -> bool {
        self.lock().have.get(index).copied().unwrap_or(false)
    }

    /// The message that tells a peer which pieces we have (see
    /// `fast::have_message`), if any, and how many completions it covers:
    /// pass that to `completed_since` for the pieces completed after it was
    /// taken.
    fn have_message(&self, fast: bool) -> (Option<PeerMessage>, usize) {
        let state = self.lock();
        let message = fast::have_message(&state.have, fast);
        (message, state.completed.len())
    }

    /// A Have for each piece we have, for a peer told earlier that we had
    /// none (one the metadata of a magnet link came from), and how many
    /// completions they cover, as for `have_message`.
    fn have_messages(&self) -> (Vec<PeerMessage>, usize) {
        let state = self.lock();
        let haves = (0..state.have.len())
            .filter(|&index| state.have[index])
            .filter_map(|index| u32::try_from(index).ok())
            .map(|piece_index| PeerMessage::Have { piece_index })
            .collect();
        (haves, state.completed.len())
    }

    /// Pieces completed after the first `seen` completions of the session.
    fn completed_since(&self, seen: usize) -> Vec<usize> {
        self.lock()
            .completed
            .get(seen..)
            .map(<[usize]>::to_vec)
            .unwrap_or_default()
    }

    /// Pieces completed this session.
    fn completed_count(&self) -> usize {
        self.lock().completed.len()
    }

    /// Which pieces are verified and on disk.
    fn have_bitfield(&self) -> Vec<bool> {
        self.lock().have.clone()
    }

    fn remaining(&self) -> usize {
        self.lock().remaining
    }

    fn is_done(&self) -> bool {
        self.remaining() == 0
    }

    /// Stops handing out pieces and wakes every worker so it can exit.
    fn close(&self) {
        self.lock().closed = true;
        self.changed.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Whether workers should exit: the queue is closed, or every piece is
    /// done and we are not seeding.
    fn is_finished(&self) -> bool {
        let state = self.lock();
        state.closed || (state.remaining == 0 && !state.seed)
    }
}

struct PieceClaim<'a> {
    queue: &'a PieceQueue,
    index: usize,
    completed: bool,
}

impl PieceClaim<'_> {
    /// Marks the piece verified and written. Returns false if another worker
    /// completed it first.
    fn complete(mut self) -> bool {
        self.completed = true;
        let first = {
            let mut state = self.queue.lock();
            let first = state.in_flight.remove(&self.index).is_some();
            if first {
                state.remaining -= 1;
                state.have[self.index] = true;
                state.completed.push(self.index);
            }
            first
        };
        self.queue.changed.notify_waiters();
        first
    }

    /// Whether the piece has been completed (by any worker).
    fn is_completed(&self) -> bool {
        // While a claim exists its piece is never pending, so it is either
        // in flight or done.
        !self.queue.lock().in_flight.contains_key(&self.index)
    }
}

impl Drop for PieceClaim<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        {
            let mut state = self.queue.lock();
            // No entry means another worker already completed the piece.
            if let Some(holders) = state.in_flight.get_mut(&self.index) {
                *holders -= 1;
                if *holders == 0 {
                    state.in_flight.remove(&self.index);
                    state.pending.insert(self.index);
                }
            }
        }
        self.queue.changed.notify_waiters();
    }
}

/// One peer's pieces as currently counted in the queue's availability.
struct PeerPieces<'a> {
    queue: &'a PieceQueue,
    has: Vec<bool>,
}

impl PeerPieces<'_> {
    /// Brings the counted pieces in line with what the peer has advertised.
    fn update(&mut self, peer_has: &[bool]) {
        let changed: Vec<usize> = self
            .has
            .iter()
            .zip(peer_has)
            .enumerate()
            .filter(|(_, (counted, has))| counted != has)
            .map(|(i, _)| i)
            .collect();
        if changed.is_empty() {
            return;
        }
        let mut state = self.queue.lock();
        for i in changed {
            let has = peer_has[i];
            let count = &mut state.availability[i];
            *count = if has {
                count.saturating_add(1)
            } else {
                count.saturating_sub(1)
            };
            self.has[i] = has;
        }
    }
}

impl Drop for PeerPieces<'_> {
    fn drop(&mut self) {
        let mut state = self.queue.lock();
        for (count, _) in state
            .availability
            .iter_mut()
            .zip(&self.has)
            .filter(|(_, has)| **has)
        {
            *count = count.saturating_sub(1);
        }
    }
}

/// Picks the `(piece, count)` candidate in `has` with the lowest count. Ties
/// are broken uniformly at random (reservoir sampling) so that peers spread
/// out over equally good pieces instead of all fetching the lowest index.
fn pick_least(
    candidates: impl IntoIterator<Item = (usize, u32)>,
    has: &[bool],
    rng: &mut impl Rng,
) -> Option<usize> {
    let mut best = None;
    let mut ties = 0u32;
    for (i, count) in candidates {
        if !has.get(i).copied().unwrap_or(false) {
            continue;
        }
        match best {
            Some((_, best_count)) if count > best_count => {}
            Some((_, best_count)) if count == best_count => {
                ties += 1;
                if rng.random_range(0..ties) == 0 {
                    best = Some((i, count));
                }
            }
            _ => {
                best = Some((i, count));
                ties = 1;
            }
        }
    }
    best.map(|(i, _)| i)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Width of the torrent name in front of each progress bar.
const BAR_LABEL_WIDTH: usize = 20;

/// The torrent name `name` as shown in front of its progress bars: cut short
/// or padded, so the bars of several torrents line up.
fn bar_label(name: &str) -> String {
    if name.chars().count() <= BAR_LABEL_WIDTH {
        return format!("{name:<BAR_LABEL_WIDTH$}");
    }
    let mut label: String = name.chars().take(BAR_LABEL_WIDTH - 1).collect();
    label.push('…');
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn peer<'a>(queue: &'a PieceQueue, has: &[bool]) -> PeerPieces<'a> {
        let mut peer = queue.register_peer();
        peer.update(has);
        peer
    }

    fn availability(queue: &PieceQueue) -> Vec<u32> {
        queue.lock().availability.clone()
    }

    #[test]
    fn bar_labels_line_up() {
        assert_eq!(bar_label("a.iso"), format!("{:<20}", "a.iso"));
        let long = bar_label("ubuntu-24.04-desktop-amd64.iso");
        assert_eq!(long, "ubuntu-24.04-deskto…");
        assert_eq!(bar_label("żółw-ąęść-łódź-źdźbło-x").chars().count(), 20);
    }

    #[test]
    fn claims_only_pieces_the_peer_has() {
        let queue = PieceQueue::new(&[false; 3]);
        assert!(queue.claim(&peer(&queue, &[false; 3]), &[]).is_none());
        let partial = peer(&queue, &[false, true, true]);
        let claim = queue.claim(&partial, &[]).unwrap();
        let second = queue.claim(&partial, &[]).unwrap();
        let mut claimed = [claim.index, second.index];
        claimed.sort();
        assert_eq!(claimed, [1, 2]);
        assert!(queue.claim(&partial, &[]).is_none());
        drop(second);
        claim.complete();
        assert_eq!(queue.remaining(), 2);
        // Piece 0 is now the rarest: only `full` has it.
        let full = peer(&queue, &[true; 3]);
        assert_eq!(queue.claim(&full, &[]).unwrap().index, 0);
    }

    #[test]
    fn dropped_claim_returns_piece() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        drop(queue.claim(&seed, &[]).unwrap());
        let claim = queue.claim(&seed, &[]).unwrap();
        claim.complete();
        assert!(queue.is_done());
        assert!(queue.claim(&seed, &[]).is_none());
    }

    #[test]
    fn pieces_already_on_disk_are_never_claimed() {
        let queue = PieceQueue::new(&[true, false, true]);
        assert_eq!(queue.remaining(), 1);
        let seed = peer(&queue, &[true; 3]);
        let claim = queue.claim(&seed, &[]).unwrap();
        assert_eq!(claim.index, 1);
        // Endgame only ever offers piece 1 again, never one we already have.
        assert!(queue.claim(&seed, &[1]).is_none());
        claim.complete();
        assert!(queue.is_done());
        assert!(!queue.wants(&seed));
    }

    #[test]
    fn claims_rarest_piece_first() {
        let queue = PieceQueue::new(&[false; 4]);
        let _a = peer(&queue, &[true, true, false, true]);
        let _b = peer(&queue, &[true, false, false, true]);
        let seed = peer(&queue, &[true; 4]);
        assert_eq!(availability(&queue), [3, 2, 1, 3]);
        let first = queue.claim(&seed, &[]).unwrap();
        assert_eq!(first.index, 2);
        let second = queue.claim(&seed, &[]).unwrap();
        assert_eq!(second.index, 1);
    }

    #[test]
    fn availability_follows_peer_updates_and_disconnects() {
        let queue = PieceQueue::new(&[false; 3]);
        let mut a = peer(&queue, &[true, false, false]);
        let b = peer(&queue, &[true, true, false]);
        assert_eq!(availability(&queue), [2, 1, 0]);
        // A Have message, then a (replacement) bitfield that drops a piece.
        a.update(&[true, false, true]);
        assert_eq!(availability(&queue), [2, 1, 1]);
        a.update(&[false, false, true]);
        assert_eq!(availability(&queue), [1, 1, 1]);
        drop(b);
        assert_eq!(availability(&queue), [0, 0, 1]);
        drop(a);
        assert_eq!(availability(&queue), [0, 0, 0]);
    }

    #[test]
    fn wants_pieces_until_they_are_completed() {
        let queue = PieceQueue::new(&[false; 3]);
        let a = peer(&queue, &[true, true, false]);
        let b = peer(&queue, &[false, true, false]);
        assert!(!queue.wants(&peer(&queue, &[false; 3])));
        assert!(queue.wants(&b));

        // In flight with another peer still counts: endgame may need it.
        let claim = queue.claim(&b, &[]).unwrap();
        assert_eq!(claim.index, 1);
        assert!(queue.wants(&b));
        assert!(claim.complete());
        assert!(!queue.wants(&b));
        assert!(queue.wants(&a));
        queue.claim(&a, &[]).unwrap().complete();
        assert!(!queue.wants(&a));
    }

    #[test]
    fn rarest_ties_are_broken_at_random() {
        let candidates = [(0, 1), (1, 5), (2, 1), (3, 1)];
        let has = [true; 4];
        let mut rng = StdRng::seed_from_u64(7);
        let mut seen = [0u32; 4];
        for _ in 0..300 {
            let i = pick_least(candidates, &has, &mut rng).unwrap();
            seen[i] += 1;
        }
        assert_eq!(seen[1], 0, "picked a more common piece");
        for i in [0, 2, 3] {
            assert!(seen[i] > 50, "piece {i} picked {} times: {seen:?}", seen[i]);
        }
    }

    #[test]
    fn pick_least_skips_pieces_the_peer_lacks() {
        let candidates = [(1, 9), (2, 9)];
        let mut rng = StdRng::seed_from_u64(0);
        assert_eq!(
            pick_least(candidates, &[true, false, true], &mut rng),
            Some(2)
        );
        assert_eq!(
            pick_least(candidates, &[true, false, false], &mut rng),
            None
        );
    }

    #[test]
    fn endgame_duplicates_in_flight_pieces() {
        let queue = PieceQueue::new(&[false; 2]);
        let seed = peer(&queue, &[true; 2]);
        let a = queue.claim(&seed, &[]).unwrap();
        let b = queue.claim(&seed, &[]).unwrap();
        // Nothing pending: further claims join in-flight pieces, spreading
        // over the one with the fewest downloaders.
        let c = queue.claim(&seed, &[]).unwrap();
        let d = queue.claim(&seed, &[]).unwrap();
        assert_ne!(c.index, d.index);
        let (first, dup) = if c.index == a.index { (a, c) } else { (b, c) };

        assert!(!dup.is_completed());
        assert!(first.complete());
        assert!(dup.is_completed());
        assert!(!dup.complete(), "a piece must only be completed once");
        assert_eq!(queue.remaining(), 1);
        // The completed piece is never handed out again.
        let e = queue.claim(&seed, &[]).unwrap();
        assert_eq!(e.index, d.index);
    }

    #[test]
    fn endgame_piece_is_requeued_only_when_every_claim_is_dropped() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed, &[]).unwrap();
        let b = queue.claim(&seed, &[]).unwrap();
        assert_eq!(queue.lock().in_flight[&0], 2);
        drop(a);
        assert!(queue.lock().pending.is_empty());
        assert_eq!(queue.lock().in_flight[&0], 1);
        drop(b);
        assert!(queue.lock().pending.contains(&0));
        assert!(queue.lock().in_flight.is_empty());
        assert_eq!(queue.remaining(), 1);
    }

    #[test]
    fn dropping_a_duplicate_after_completion_changes_nothing() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed, &[]).unwrap();
        let b = queue.claim(&seed, &[]).unwrap();
        assert!(a.complete());
        drop(b);
        assert!(queue.is_done());
        assert!(queue.lock().pending.is_empty());
        assert!(queue.claim(&seed, &[]).is_none());
    }

    #[tokio::test]
    async fn closed_queue_hands_out_nothing_and_wakes_workers() {
        let queue = PieceQueue::new(&[false; 2]);
        let seed = peer(&queue, &[true; 2]);
        let held = queue.claim(&seed, &[]).unwrap();
        assert!(!queue.is_finished());

        let close_later = async {
            tokio::task::yield_now().await;
            queue.close();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(queue.wait_for(|| queue.is_closed()), close_later)
        })
        .await
        .expect("not woken by close");
        assert!(queue.is_finished());
        assert!(!queue.is_done());
        assert!(queue.claim(&seed, &[]).is_none());

        // A piece written after the close still counts.
        assert!(held.complete());
        assert_eq!(queue.remaining(), 1);
    }

    /// A piece of two and a half blocks, its hash, and an `ActivePiece` for it.
    fn active<'a>(queue: &'a PieceQueue, seed: &PeerPieces<'_>) -> (Vec<u8>, ActivePiece<'a>) {
        let len = BLOCK_SIZE as usize * 5 / 2;
        let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let hash = sha1::Sha1::digest(&data).into();
        let claim = queue.claim(seed, &[]).unwrap();
        let piece = ActivePiece::new(claim, len as u64, hash).unwrap();
        (data, piece)
    }

    fn block(data: &[u8], block: usize) -> &[u8] {
        let start = block * BLOCK_SIZE as usize;
        &data[start..data.len().min(start + BLOCK_SIZE as usize)]
    }

    #[test]
    fn active_piece_requests_blocks_in_order_and_assembles_them() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (data, mut piece) = active(&queue, &seed);
        let mut lengths = Vec::new();
        while piece.has_unrequested() {
            let PeerMessage::Request { begin, length, .. } = piece.request_next() else {
                panic!("not a request");
            };
            lengths.push((begin, length));
        }
        let half = BLOCK_SIZE / 2;
        assert_eq!(
            lengths,
            [
                (0, BLOCK_SIZE),
                (BLOCK_SIZE, BLOCK_SIZE),
                (2 * BLOCK_SIZE, half)
            ]
        );
        assert_eq!(piece.in_flight(), 3);

        for b in [2, 0, 1] {
            assert!(!piece.is_complete());
            assert!(
                piece
                    .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                    .unwrap()
            );
        }
        assert_eq!(piece.in_flight(), 0);
        assert!(piece.is_complete());
        let (claim, assembled) = piece.verify().expect("hash mismatch");
        assert_eq!(assembled, data);
        assert!(claim.complete());
    }

    #[test]
    fn active_piece_ignores_duplicate_and_unrequested_blocks() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (data, mut piece) = active(&queue, &seed);
        piece.request_next();
        // A late reply to a request abandoned on a choke, for a block we
        // have not asked for again yet.
        assert!(!piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
        assert_eq!(piece.in_flight(), 1);
        assert!(piece.add_block(0, block(&data, 0)).unwrap());
        assert!(!piece.add_block(0, block(&data, 0)).unwrap());
        assert_eq!(piece.in_flight(), 0);
        assert_eq!(piece.done, 1);
    }

    #[test]
    fn active_piece_rejects_invalid_blocks() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (data, mut piece) = active(&queue, &seed);
        while piece.has_unrequested() {
            piece.request_next();
        }
        let full = vec![0u8; BLOCK_SIZE as usize];
        for (begin, block) in [
            (1, block(&data, 0)),
            (0, &full[1..]),
            (2 * BLOCK_SIZE, &full[..]),
            (3 * BLOCK_SIZE, block(&data, 2)),
        ] {
            let err = piece.add_block(begin, block).unwrap_err();
            assert!(err.is::<Misbehaved>(), "begin {begin}: {err:#}");
        }
    }

    #[test]
    fn active_piece_cancels_only_outstanding_requests() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (data, mut piece) = active(&queue, &seed);
        piece.request_next();
        piece.request_next();
        piece.add_block(0, block(&data, 0)).unwrap();
        let cancels: Vec<PeerMessage> = piece.outstanding().map(BlockRequest::cancel).collect();
        assert_eq!(
            cancels,
            [PeerMessage::Cancel {
                index: 0,
                begin: BLOCK_SIZE,
                length: BLOCK_SIZE,
            }]
        );
    }

    #[test]
    fn rejected_blocks_are_requested_again_first() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (data, mut piece) = active(&queue, &seed);
        let request = |block: u32| PeerMessage::Request {
            index: 0,
            begin: block * BLOCK_SIZE,
            length: BLOCK_SIZE.min(data.len() as u32 - block * BLOCK_SIZE),
        };
        assert_eq!(piece.request_next(), request(0));
        assert_eq!(piece.request_next(), request(1));
        // Not outstanding: unrequested, wrong length, not a block start.
        assert!(!piece.reject(2 * BLOCK_SIZE, BLOCK_SIZE / 2));
        assert!(!piece.reject(0, BLOCK_SIZE - 1));
        assert!(!piece.reject(1, BLOCK_SIZE));
        assert!(!piece.reject(9 * BLOCK_SIZE, BLOCK_SIZE));

        assert!(piece.reject(0, BLOCK_SIZE));
        assert!(!piece.reject(0, BLOCK_SIZE), "rejected twice");
        assert_eq!(piece.in_flight(), 1);
        // A block that arrives after all is ignored until asked for again.
        assert!(!piece.add_block(0, block(&data, 0)).unwrap());
        assert_eq!(piece.request_next(), request(0));
        assert_eq!(piece.request_next(), request(2));
        assert!(!piece.has_unrequested());
        let outstanding: Vec<BlockRequest> = piece.outstanding().collect();
        assert_eq!(outstanding.len(), 3);

        assert!(piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
        assert!(!piece.reject(BLOCK_SIZE, BLOCK_SIZE), "already received");
        assert!(piece.reject(2 * BLOCK_SIZE, BLOCK_SIZE / 2));
        assert_eq!(piece.request_next(), request(2));
        for b in [0, 2] {
            assert!(
                piece
                    .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                    .unwrap()
            );
        }
        assert!(piece.is_complete());
        assert_eq!(piece.in_flight(), 0);
        assert!(piece.verify().is_some());
    }

    #[test]
    fn choked_claims_take_only_allowed_fast_pieces() {
        let queue = PieceQueue::new(&[false, false, true, false]);
        let partial = peer(&queue, &[true, true, false, true]);
        let allowed = [false, true, true, false];
        // Piece 2 is allowed, but we have it already.
        let claim = queue.claim_allowed(&partial, &[], &allowed).unwrap();
        assert_eq!(claim.index, 1);
        assert!(queue.claim_allowed(&partial, &[], &allowed).is_none());
        // Endgame duplicates are limited the same way.
        let others = queue.claim(&partial, &[]).unwrap();
        let rest = queue.claim(&partial, &[others.index]).unwrap();
        assert!(queue.lock().pending.is_empty());
        assert!(queue.claim_allowed(&partial, &[1], &allowed).is_none());
        let dup = queue.claim_allowed(&partial, &[], &allowed).unwrap();
        assert_eq!(dup.index, 1);
        drop((claim, others, rest, dup));
    }

    #[test]
    fn a_choke_releases_pieces_with_nothing_outstanding_unless_allowed_fast() {
        let queue = PieceQueue::new(&[false; 3]);
        let seed = peer(&queue, &[true; 3]);
        let piece = |index: usize| {
            let claim = queue
                .claim_from(&[0, 1, 2].map(|i| i == index), &[])
                .unwrap();
            ActivePiece::new(claim, u64::from(2 * BLOCK_SIZE), [0; 20]).unwrap()
        };
        let (mut waiting, idle, allowed) = (piece(0), piece(1), piece(2));
        waiting.request_next();
        let mut active = vec![waiting, idle, allowed];
        release_choked(&mut active, &[false, false, true]);
        let held: Vec<usize> = active.iter().map(|piece| piece.claim.index).collect();
        assert_eq!(held, [0, 2]);
        assert!(queue.lock().pending.contains(&1));

        // Once its request is rejected, the first piece goes too.
        assert!(active[0].reject(0, BLOCK_SIZE));
        release_choked(&mut active, &[false, false, true]);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].claim.index, 2);
        assert!(queue.lock().pending.contains(&0));
        drop(seed);
    }

    #[test]
    fn corrupt_piece_fails_verification_and_is_returned() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let (mut data, mut piece) = active(&queue, &seed);
        data[0] ^= 0xff;
        for b in 0..3 {
            piece.request_next();
            piece
                .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                .unwrap();
        }
        assert!(piece.verify().is_none());
        assert!(queue.lock().pending.contains(&0));
    }

    #[test]
    fn endgame_never_duplicates_a_piece_the_worker_holds() {
        let queue = PieceQueue::new(&[false; 2]);
        let seed = peer(&queue, &[true; 2]);
        let a = queue.claim(&seed, &[]).unwrap();
        let b = queue.claim(&seed, &[]).unwrap();
        assert!(queue.claim(&seed, &[a.index, b.index]).is_none());
        let dup = queue.claim(&seed, &[a.index]).unwrap();
        assert_eq!(dup.index, b.index);
    }

    #[test]
    fn pipeline_follows_the_download_rate() {
        let start = Instant::now();
        let mut pipeline = Pipeline::new(start);
        assert_eq!(pipeline.depth(), INITIAL_PIPELINE);
        let block = u64::from(BLOCK_SIZE);

        // Nothing changes until a whole window has passed.
        pipeline.received(100 * block, start + RATE_WINDOW / 2);
        assert_eq!(pipeline.depth(), INITIAL_PIPELINE);
        // 100 blocks in a second: three seconds' worth.
        pipeline.received(0, start + RATE_WINDOW);
        assert_eq!(pipeline.depth(), 300.min(MAX_PIPELINE));

        // 10 blocks over the next two seconds.
        let later = start + RATE_WINDOW + Duration::from_secs(2);
        pipeline.received(10 * block, later);
        assert_eq!(pipeline.depth(), 15);

        // One block in ten seconds: never below the minimum.
        let much_later = later + Duration::from_secs(10);
        pipeline.received(block, much_later);
        assert_eq!(pipeline.depth(), MIN_PIPELINE);

        // Way past the maximum.
        pipeline.received(10_000 * block, much_later + RATE_WINDOW);
        assert_eq!(pipeline.depth(), MAX_PIPELINE);
    }

    #[test]
    fn pipeline_restart_discards_idle_time() {
        let start = Instant::now();
        let mut pipeline = Pipeline::new(start);
        let block = u64::from(BLOCK_SIZE);
        // Choked for a minute, then 20 blocks in the first second.
        let resumed = start + Duration::from_secs(60);
        pipeline.restart(resumed);
        pipeline.received(20 * block, resumed + RATE_WINDOW);
        assert_eq!(pipeline.depth(), 60);
    }

    #[test]
    fn a_peer_is_connected_to_once_at_a_time() {
        let peers = ConnectedPeers::default();
        let a = peers.register([1; 20]).unwrap();
        assert!(peers.register([1; 20]).is_none());
        let _b = peers.register([2; 20]).unwrap();
        drop(a);
        assert!(peers.register([1; 20]).is_some());
    }

    #[test]
    fn bitfield_and_haves_follow_completed_pieces() {
        let queue = PieceQueue::new(&[false; 3]);
        assert_eq!(queue.have_message(false), (None, 0));
        assert_eq!(queue.have_message(true), (Some(PeerMessage::HaveNone), 0));
        assert!(!queue.has(0));
        let seed = peer(&queue, &[true; 3]);
        let first = queue.claim(&seed, &[]).unwrap();
        let second = queue.claim(&seed, &[]).unwrap();
        let (a, b) = (first.index, second.index);

        assert!(second.complete());
        assert!(queue.has(b));
        let (message, seen) = queue.have_message(false);
        let bitfield = peer::encode_bitfield(&[0, 1, 2].map(|i| i == b));
        assert_eq!(message, Some(PeerMessage::Bitfield { bitfield }));
        assert_eq!(seen, 1);
        // A worker that sent this bitfield announces only what follows.
        assert!(queue.completed_since(seen).is_empty());
        assert!(first.complete());
        assert_eq!(queue.completed_since(seen), [a]);
        assert_eq!(queue.completed_since(0), [b, a]);
        assert_eq!(queue.completed_count(), 2);
        assert_eq!(queue.have_bitfield(), [0, 1, 2].map(|i| i == a || i == b));
        assert!(queue.completed_since(5).is_empty());
        assert!(!queue.has(3));
        drop(seed);
        queue
            .claim(&peer(&queue, &[true; 3]), &[])
            .unwrap()
            .complete();
        assert_eq!(queue.have_message(true), (Some(PeerMessage::HaveAll), 3));
    }

    #[test]
    fn seed_limits_stop_at_whichever_comes_first() {
        let minute = Duration::from_secs(60);
        let none = SeedLimits::default();
        assert!(!none.enabled());
        let ratio = SeedLimits {
            ratio: 1.5,
            time: Duration::ZERO,
        };
        assert!(ratio.enabled());
        assert!(!ratio.reached(149, 100, 1000 * minute));
        assert!(ratio.reached(150, 100, Duration::ZERO));
        let time = SeedLimits {
            ratio: 0.0,
            time: minute,
        };
        assert!(time.enabled());
        assert!(!time.reached(u64::MAX, 100, minute / 2));
        assert!(time.reached(0, 100, minute));
        let both = SeedLimits {
            ratio: 2.0,
            time: minute,
        };
        assert!(both.reached(200, 100, Duration::ZERO));
        assert!(both.reached(0, 100, minute));
        assert!(!both.reached(199, 100, minute / 2));
    }

    #[test]
    fn a_seeding_queue_keeps_workers_until_closed() {
        let queue = PieceQueue::new(&[true, true]);
        assert!(queue.is_done() && queue.is_finished());
        let queue = PieceQueue::new(&[true, true]).seeding(true);
        assert!(queue.is_done() && !queue.is_finished());
        queue.close();
        assert!(queue.is_finished());
    }

    #[test]
    fn pieces_on_disk_are_in_the_bitfield_but_never_announced() {
        let queue = PieceQueue::new(&[true, false, true]);
        assert!(queue.has(0) && !queue.has(1) && queue.has(2));
        let bitfield = vec![0b1010_0000];
        let message = Some(PeerMessage::Bitfield { bitfield });
        assert_eq!(queue.have_message(false), (message.clone(), 0));
        assert_eq!(queue.have_message(true), (message, 0));
        assert!(queue.completed_since(0).is_empty());
    }

    #[test]
    fn request_must_lie_inside_the_piece_and_be_one_block_at_most() {
        let request = |begin, length| BlockRequest {
            index: 0,
            begin,
            length,
        };
        let piece_len = 3 * u64::from(BLOCK_SIZE) - 100;
        for (begin, length) in [
            (0, BLOCK_SIZE),
            (BLOCK_SIZE, BLOCK_SIZE),
            // The short last block, and odd but valid ranges.
            (2 * BLOCK_SIZE, BLOCK_SIZE - 100),
            (7, 1),
            (2 * BLOCK_SIZE - 100, BLOCK_SIZE),
        ] {
            assert!(request(begin, length).check(piece_len).is_ok());
        }
        for (begin, length) in [
            (0, 0),
            (0, BLOCK_SIZE + 1),
            (0, 2 * BLOCK_SIZE),
            (2 * BLOCK_SIZE, BLOCK_SIZE),
            (3 * BLOCK_SIZE, 1),
            (u32::MAX, BLOCK_SIZE),
        ] {
            let err = request(begin, length).check(piece_len).unwrap_err();
            assert!(err.is::<Misbehaved>(), "{begin}+{length}: {err:#}");
        }
    }

    #[test]
    fn uploads_are_served_in_order_cancelled_and_capped() {
        let request = |begin| BlockRequest {
            index: 1,
            begin,
            length: BLOCK_SIZE,
        };
        let mut uploads = Uploads::default();
        assert!(uploads.push(request(0)));
        assert!(uploads.push(request(1)));
        assert!(uploads.push(request(2)));
        // Asked twice, sent once.
        assert!(uploads.push(request(0)));
        assert!(uploads.cancel(&request(1)));
        assert!(!uploads.cancel(&request(1)));
        assert!(!uploads.cancel(&request(9)));
        assert_eq!(uploads.pop(), Some(request(0)));
        assert_eq!(uploads.pop(), Some(request(2)));
        assert_eq!(uploads.pop(), None);

        for begin in 0..MAX_QUEUED_UPLOADS as u32 {
            assert!(uploads.push(request(begin)));
        }
        assert!(!uploads.push(request(u32::MAX)));
        assert_eq!(uploads.retain(|_| false).len(), MAX_QUEUED_UPLOADS);
        assert!(uploads.is_empty());
        assert!(uploads.push(request(u32::MAX)));
    }

    #[test]
    fn retained_uploads_keep_their_order_and_dropped_ones_are_returned() {
        let request = |index| BlockRequest {
            index,
            begin: 0,
            length: BLOCK_SIZE,
        };
        let mut uploads = Uploads::default();
        for index in [3, 1, 4, 1, 5, 9, 2, 6] {
            uploads.push(request(index));
        }
        let dropped = uploads.retain(|request| request.index % 2 == 0);
        assert_eq!(dropped, [3, 1, 5, 9].map(request));
        assert_eq!(uploads.pop(), Some(request(4)));
        assert_eq!(uploads.pop(), Some(request(2)));
        assert_eq!(uploads.pop(), Some(request(6)));
        assert!(uploads.is_empty());
    }

    #[tokio::test]
    async fn wait_for_wakes_on_completion() {
        let queue = PieceQueue::new(&[false; 1]);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed, &[]).unwrap();
        let b = queue.claim(&seed, &[]).unwrap();
        let short = Duration::from_millis(50);
        assert!(
            tokio::time::timeout(short, queue.wait_for(|| b.is_completed()))
                .await
                .is_err(),
            "resolved before the piece was completed"
        );
        let complete_later = async {
            tokio::task::yield_now().await;
            a.complete();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(queue.wait_for(|| b.is_completed()), complete_later)
        })
        .await
        .expect("not woken by completion");
    }
}
