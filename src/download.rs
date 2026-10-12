use crate::announcer::Announcer;
use crate::peer::{self, PeerConnection, PeerMessage};
use crate::peer_pool::PeerPool;
use crate::storage::Storage;
use crate::torrent::{self, TorrentFile};
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use crate::verify;
use anyhow::{Result, anyhow, bail};
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use sha1::Digest;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

const BLOCK_SIZE: u32 = 16 * 1024;
/// Block requests in flight to a new peer, before its rate is known.
const INITIAL_PIPELINE: usize = 4;
const MIN_PIPELINE: usize = 2;
/// Many clients drop requests beyond a few hundred queued. The extension
/// handshake (BEP 10 `reqq`) would tell us a peer's real limit.
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

pub struct DownloadConfig {
    pub output_dir: PathBuf,
    pub max_peers: usize,
    /// Port to listen on for incoming peers; 0 picks a free one.
    pub listen_port: u16,
}

/// Everything a peer worker needs to know about the torrent.
pub struct Session {
    pub torrent: TorrentFile,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub storage: Arc<Storage>,
}

/// Downloads the torrent at `torrent_path`, stopping early (see
/// `download_from_peers`) if `shutdown` resolves. It is first polled once
/// the tracker has answered, so a signal handler behind it is only
/// installed then.
pub async fn download_file(
    torrent_path: &Path,
    config: &DownloadConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let (torrent, info_hash) = torrent::load_file(torrent_path).await?;
    info!(
        name = %torrent.info.name,
        size = torrent.length(),
        pieces = torrent.num_pieces(),
        info_hash = %hex(&info_hash),
        "loaded torrent"
    );

    // Before announcing, so a bad output directory does not leave the
    // tracker without a `stopped`.
    let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir)?);
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: peer::generate_peer_id(),
        storage,
    });

    let candidates = verify::candidates(&session.torrent, &session.storage)?;
    let have = hash_pieces(&session, candidates).await?;
    let left = session.torrent.length() - verified_bytes(&session.torrent, &have)?;
    if left == 0 {
        info!(output = %config.output_dir.display(), "already complete");
        return Ok(());
    }

    let listeners = listen(config.listen_port).await;
    let port = match listeners.first().map(TcpListener::local_addr) {
        Some(Ok(addr)) => {
            info!(port = addr.port(), "listening for peers");
            addr.port()
        }
        _ => config.listen_port,
    };
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
    let (announcer, peers) = Announcer::start(tiers, request).await?;
    if peers.is_empty() {
        announcer.stop(false, 0).await;
        bail!("no tracker returned any peers");
    }
    info!(peers = peers.len(), "tracker announce succeeded");

    download_from_peers(
        session,
        &have,
        &peers,
        config.max_peers,
        listeners,
        Some(announcer),
        shutdown,
    )
    .await?;
    info!(output = %config.output_dir.display(), "download complete");
    Ok(())
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
    });
    let have = hash_pieces(&session, pieces).await?;

    let report = verify::Report::new(&session.torrent, &session.storage, &have);
    print!("{report}");
    if !report.is_complete() {
        bail!("{} is incomplete", session.torrent.info.name);
    }
    Ok(())
}

/// Hashes `pieces` on disk, showing progress, and returns which pieces of
/// the torrent are there and intact.
async fn hash_pieces(session: &Arc<Session>, pieces: Vec<usize>) -> Result<Vec<bool>> {
    let torrent = &session.torrent;
    if pieces.is_empty() {
        return Ok(vec![false; torrent.num_pieces()]);
    }
    let total = pieces
        .iter()
        .map(|&index| torrent.piece_len(index))
        .sum::<Result<u64>>()?;
    let bar = ProgressBar::new(total);
    if let Ok(style) =
        ProgressStyle::with_template("checking {bar:40.cyan/blue} {bytes}/{total_bytes} eta {eta}")
    {
        bar.set_style(style);
    }

    let session = Arc::clone(session);
    let check_bar = bar.clone();
    let have = tokio::task::spawn_blocking(move || {
        verify::verify(&session.torrent, &session.storage, &pieces, &|len| {
            check_bar.inc(len)
        })
    })
    .await??;
    bar.finish_and_clear();

    let done = have.iter().filter(|&&have| have).count();
    info!(done, pieces = have.len(), "checked data on disk");
    Ok(have)
}

/// Total length of the pieces marked in `have`.
fn verified_bytes(torrent: &TorrentFile, have: &[bool]) -> Result<u64> {
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
/// With an `announcer`, the tracker is re-announced to on its interval and
/// the peers it returns join the pool. If we run out of peers, the tracker is
/// asked for more once before giving up. At the end the tracker is sent
/// `completed` (if the download finished) and `stopped`.
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
    mut announcer: Option<Announcer>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let torrent = &session.torrent;
    let num_pieces = torrent.num_pieces();
    if have.len() != num_pieces {
        bail!("have {} piece flags for {num_pieces} pieces", have.len());
    }
    let queue = Arc::new(PieceQueue::new(have));
    let progress = Arc::new(Progress::new(
        torrent.length(),
        verified_bytes(torrent, have)?,
    ));
    let connected = Arc::new(ConnectedPeers::default());
    let mut pool = PeerPool::default();
    pool.add(peers.iter().copied());
    // Incoming peers have no address we could ban in the pool.
    let mut banned_ips = HashSet::new();

    let mut workers = JoinSet::new();
    // Starts a worker that connects to `addr`, or, given the socket the peer
    // connected to us on, answers its handshake.
    let spawn = |workers: &mut JoinSet<WorkerExit>, addr, socket: Option<TcpStream>| {
        let session = Arc::clone(&session);
        let queue = Arc::clone(&queue);
        let progress = Arc::clone(&progress);
        let connected = Arc::clone(&connected);
        let inbound = socket.is_some();
        workers.spawn(async move {
            let mut pieces = 0;
            let result = peer_worker(
                &session,
                &queue,
                &progress,
                &connected,
                addr,
                socket,
                &mut pieces,
            )
            .await;
            WorkerExit {
                addr,
                inbound,
                pieces,
                result,
            }
        });
    };
    // Set once we ran out of peers and asked the tracker for more; cleared
    // when a new connection is started.
    let mut asked_for_peers = false;
    let mut shutdown = std::pin::pin!(shutdown);
    let mut interrupted = false;
    while !queue.is_done() {
        while workers.len() < max_peers {
            let Some(addr) = pool.next(Instant::now()) else {
                break;
            };
            asked_for_peers = false;
            spawn(&mut workers, addr, None);
        }

        let retry_at = pool.next_retry().filter(|_| workers.len() < max_peers);
        if workers.is_empty() && retry_at.is_none() {
            match &mut announcer {
                Some(announcer) if !asked_for_peers => {
                    info!("out of peers, asking the tracker for more");
                    announcer.hurry();
                    asked_for_peers = true;
                }
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
                    Err(e) if e.is::<Misbehaved>() => {
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
            (socket, addr) = accept(&listeners) => {
                if workers.len() >= max_peers {
                    debug!(%addr, "turning away incoming peer: too many peers");
                } else if banned_ips.contains(&addr.ip()) {
                    debug!(%addr, "turning away banned peer");
                } else {
                    asked_for_peers = false;
                    spawn(&mut workers, addr, Some(socket));
                }
            }
            () = sleep_until_some(retry_at) => {}
            peers = reannounce(&mut announcer, &progress.downloaded) => pool.add(peers),
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
    drop(listeners);
    queue.close();
    let drain = async { while workers.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        workers.shutdown().await;
    }

    let remaining = queue.remaining();
    if remaining == 0 {
        progress.bar.finish();
    } else {
        // `finish` would fill the bar.
        progress.bar.abandon();
    }
    if let Some(announcer) = announcer {
        let downloaded = progress.downloaded.load(Ordering::Relaxed);
        announcer.stop(remaining == 0, downloaded).await;
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

/// Sleeps until `at`, or forever if it is `None`.
async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Accepts the next incoming connection on any of `listeners`, or waits
/// forever without one. Cancel-safe.
async fn accept(listeners: &[TcpListener]) -> (TcpStream, SocketAddr) {
    let next = || {
        std::future::poll_fn(|cx| {
            listeners
                .iter()
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
struct ConnectedPeers(Mutex<HashSet<[u8; 20]>>);

impl ConnectedPeers {
    fn lock(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a connection to the peer with id `id` until the returned
    /// handle is dropped, or returns `None` if there already is one.
    fn register(&self, id: [u8; 20]) -> Option<Connected<'_>> {
        let new = self.lock().insert(id);
        new.then_some(Connected { peers: self, id })
    }
}

struct Connected<'a> {
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
async fn reannounce(announcer: &mut Option<Announcer>, downloaded: &AtomicU64) -> Vec<SocketAddr> {
    match announcer {
        Some(announcer) => announcer.poll(downloaded).await,
        None => std::future::pending().await,
    }
}

/// A peer that broke the protocol or sent bad data, or turned out to be us.
/// It is never retried.
#[derive(Debug)]
struct Misbehaved(&'static str);

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

/// Bytes verified and written, shown on the progress bar and reported to the
/// tracker.
struct Progress {
    bar: ProgressBar,
    /// Downloaded this session, not counting data already on disk.
    downloaded: AtomicU64,
}

impl Progress {
    /// A bar for `total` bytes, of which `done` were already on disk.
    fn new(total: u64, done: u64) -> Self {
        let bar = ProgressBar::new(total);
        let style = ProgressStyle::with_template(
            "{bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta} {msg}",
        );
        if let Ok(style) = style {
            bar.set_style(style);
        }
        // Moving the bar records `done` as a step, and resetting the ETA
        // drops it again, so data from disk does not count towards the rate.
        bar.set_position(done);
        bar.reset_eta();
        Self {
            bar,
            downloaded: AtomicU64::new(0),
        }
    }

    fn add(&self, bytes: u64) {
        self.bar.inc(bytes);
        self.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Downloads from one peer until the torrent is complete, the queue is
/// closed or the peer fails. Connects to `addr`, or with a `socket` the peer
/// connected to us on, answers its handshake. `pieces_done` counts the
/// pieces this peer delivered first, even on error.
async fn peer_worker(
    session: &Session,
    queue: &PieceQueue,
    progress: &Progress,
    connected: &ConnectedPeers,
    addr: SocketAddr,
    socket: Option<TcpStream>,
    pieces_done: &mut usize,
) -> Result<()> {
    let torrent = &session.torrent;
    let (info_hash, peer_id, num_pieces) =
        (session.info_hash, session.peer_id, torrent.num_pieces());
    let inbound = socket.is_some();
    let mut conn = match socket {
        Some(socket) => {
            PeerConnection::accept(socket, addr, info_hash, peer_id, num_pieces).await?
        }
        None => PeerConnection::connect(addr, info_hash, peer_id, num_pieces).await?,
    };
    if conn.peer_id == peer_id {
        // A tracker listed our own address.
        bail!(Misbehaved("connected to ourselves"));
    }
    let Some(_connected) = connected.register(conn.peer_id) else {
        bail!("already connected to this peer");
    };
    debug!(%addr, inbound, "connected");

    let mut pieces = queue.register_peer();
    // Pieces we hold a claim on, with requests spread over all of them so the
    // pipeline does not drain at every piece boundary.
    let mut active: Vec<ActivePiece<'_>> = Vec::new();
    let mut pipeline = Pipeline::new(Instant::now());
    let mut hash_failures = 0;
    let mut last_useful = Instant::now();
    // Set while we are interested but choked.
    let mut unchoke_deadline = None;
    let mut has_changed = true;
    loop {
        drop_completed(&mut conn, &mut active).await?;
        if queue.is_finished() {
            return Ok(());
        }
        if has_changed {
            pieces.update(conn.peer_has());
            has_changed = false;
        }
        // Interested only while the peer has a piece we still need, even if
        // another peer is downloading it right now. Any piece we hold counts.
        let wanted = !active.is_empty() || queue.wants(&pieces);
        conn.set_interested(wanted).await?;
        let now = Instant::now();
        if wanted {
            last_useful = now;
        }
        unchoke_deadline = match unchoke_deadline {
            _ if !wanted || !conn.is_choked() => None,
            Some(deadline) => Some(deadline),
            None => Some(now + UNCHOKE_TIMEOUT),
        };
        if !conn.is_choked() {
            request_blocks(
                &mut conn,
                queue,
                &pieces,
                torrent,
                &mut active,
                &mut pipeline,
            )
            .await?;
        }

        // `receive` and `receive_untimed` are cancel-safe, so losing a race
        // loses no data.
        let message = if active.is_empty() {
            if unchoke_deadline.is_some_and(|deadline| now >= deadline) {
                bail!("peer did not unchoke us");
            }
            if now - last_useful > IDLE_TIMEOUT {
                bail!("peer has no pieces we need");
            }
            conn.keep_alive().await?;
            // Wait for an unchoke, for a piece to be returned or completed,
            // for the queue to close, or for the peer to announce a new
            // piece.
            tokio::select! {
                _ = queue.changed.notified() => None,
                message = conn.receive_untimed() => Some(message?),
                _ = tokio::time::sleep(IDLE_POLL) => None,
            }
        } else {
            conn.keep_alive().await?;
            let completed_elsewhere = || active.iter().any(|piece| piece.claim.is_completed());
            tokio::select! {
                message = conn.receive() => Some(message?),
                () = queue.wait_for(|| queue.is_closed() || completed_elsewhere()) => None,
            }
        };
        let Some(message) = message else {
            continue;
        };

        match message {
            PeerMessage::Have { .. } | PeerMessage::Bitfield { .. } => has_changed = true,
            // Dropping the claims returns the pieces; the next turn waits for
            // an unchoke.
            PeerMessage::Choke if !active.is_empty() => {
                debug!(%addr, pieces = active.len(), "choked mid-download");
                active.clear();
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                // Blocks of pieces we no longer hold are late replies to
                // requests abandoned on a choke or cancelled in endgame.
                let Some(pos) = active.iter().position(|piece| piece.index == index) else {
                    continue;
                };
                if !active[pos].add_block(begin, &block)? {
                    continue;
                }
                pipeline.received(block.len() as u64, Instant::now());
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
            _ => {}
        }
    }
}

/// Tops up the requests in flight to the pipeline depth, claiming another
/// piece whenever every block of the held ones has been requested.
async fn request_blocks<'q>(
    conn: &mut PeerConnection,
    queue: &'q PieceQueue,
    pieces: &PeerPieces<'_>,
    torrent: &TorrentFile,
    active: &mut Vec<ActivePiece<'q>>,
    pipeline: &mut Pipeline,
) -> Result<()> {
    let in_flight: usize = active.iter().map(ActivePiece::in_flight).sum();
    if in_flight == 0 {
        // Time spent choked or with nothing to ask for says nothing about
        // the peer's speed.
        pipeline.restart(Instant::now());
    }
    let mut requests = Vec::new();
    while in_flight + requests.len() < pipeline.depth() {
        let pos = match active.iter().position(ActivePiece::has_unrequested) {
            Some(pos) => pos,
            None => {
                let held: Vec<usize> = active.iter().map(|piece| piece.claim.index).collect();
                let Some(claim) = queue.claim(pieces, &held) else {
                    break;
                };
                let piece_len = torrent.piece_len(claim.index)?;
                let hash = torrent.piece_hash(claim.index)?;
                active.push(ActivePiece::new(claim, piece_len, hash)?);
                active.len() - 1
            }
        };
        requests.push(active[pos].request_next());
    }
    if !requests.is_empty() {
        conn.send_all(&requests).await?;
    }
    Ok(())
}

/// Lets go of held pieces that another worker has completed (endgame),
/// cancelling the requests still outstanding for them.
async fn drop_completed(
    conn: &mut PeerConnection,
    active: &mut Vec<ActivePiece<'_>>,
) -> Result<()> {
    let mut cancels = Vec::new();
    active.retain(|piece| {
        let completed = piece.claim.is_completed();
        if completed {
            debug!(addr = %conn.addr, index = piece.claim.index, "piece completed by another peer");
            cancels.extend(piece.cancels());
        }
        !completed
    });
    if !cancels.is_empty() {
        conn.send_all(&cancels).await?;
    }
    Ok(())
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

/// A claimed piece being downloaded. Blocks are requested in order, so the
/// ones before `next_request` have been asked for.
struct ActivePiece<'a> {
    claim: PieceClaim<'a>,
    /// `claim.index` as sent on the wire.
    index: u32,
    hash: [u8; 20],
    data: Vec<u8>,
    received: Vec<bool>,
    next_request: usize,
    /// Blocks received, all before `next_request`.
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
            received: vec![false; piece_len.div_ceil(BLOCK_SIZE) as usize],
            next_request: 0,
            done: 0,
        })
    }

    fn block_len(&self, block: usize) -> u32 {
        let begin = block as u32 * BLOCK_SIZE;
        BLOCK_SIZE.min(self.data.len() as u32 - begin)
    }

    /// Requests sent and not yet answered.
    fn in_flight(&self) -> usize {
        self.next_request - self.done
    }

    fn has_unrequested(&self) -> bool {
        self.next_request < self.received.len()
    }

    /// The request for the next block. Call only if `has_unrequested`.
    fn request_next(&mut self) -> PeerMessage {
        let block = self.next_request;
        self.next_request += 1;
        PeerMessage::Request {
            index: self.index,
            begin: block as u32 * BLOCK_SIZE,
            length: self.block_len(block),
        }
    }

    /// Cancels for every request still outstanding.
    fn cancels(&self) -> impl Iterator<Item = PeerMessage> + '_ {
        (0..self.next_request)
            .filter(|&block| !self.received[block])
            .map(|block| PeerMessage::Cancel {
                index: self.index,
                begin: block as u32 * BLOCK_SIZE,
                length: self.block_len(block),
            })
    }

    /// Stores a block of this piece. Returns false if it was not one we are
    /// waiting for: a duplicate, or a late reply to a request abandoned on a
    /// choke for a block we have not asked for again yet. Those are ignored
    /// rather than dropping an honest peer.
    fn add_block(&mut self, begin: u32, block: &[u8]) -> Result<bool> {
        let index = (begin / BLOCK_SIZE) as usize;
        if !begin.is_multiple_of(BLOCK_SIZE)
            || index >= self.received.len()
            || block.len() != self.block_len(index) as usize
        {
            bail!(Misbehaved("peer sent an invalid block"));
        }
        if index >= self.next_request || self.received[index] {
            return Ok(false);
        }
        let start = begin as usize;
        self.data[start..start + block.len()].copy_from_slice(block);
        self.received[index] = true;
        self.done += 1;
        Ok(true)
    }

    fn is_complete(&self) -> bool {
        self.done == self.received.len()
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
    /// Set when the download stops: no more pieces are handed out, and
    /// workers exit.
    closed: bool,
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
                closed: false,
            }),
            changed: Notify::new(),
        }
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
            let index = pick_least(candidates, &peer.has, &mut rng)?;
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
            let index = pick_least(candidates, &peer.has, &mut rng)?;
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

    /// Whether workers should exit: every piece is done or the queue is
    /// closed.
    fn is_finished(&self) -> bool {
        let state = self.lock();
        state.remaining == 0 || state.closed
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
        let cancels: Vec<PeerMessage> = piece.cancels().collect();
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
