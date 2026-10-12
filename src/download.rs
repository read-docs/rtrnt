use crate::announcer::Announcer;
use crate::peer::{self, PeerConnection, PeerMessage};
use crate::peer_pool::PeerPool;
use crate::storage::Storage;
use crate::torrent::{self, TorrentFile};
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use anyhow::{Result, anyhow, bail};
use indicatif::{ProgressBar, ProgressStyle};
use rand::Rng;
use sha1::Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{debug, info, warn};

const BLOCK_SIZE: u32 = 16 * 1024;
/// Block requests kept in flight per peer.
const PIPELINE: usize = 16;
/// Peers that send this many corrupt pieces are dropped.
const MAX_HASH_FAILURES: u32 = 3;
/// A peer with nothing useful to offer for this long is dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// How often an idle worker re-checks the queue.
const IDLE_POLL: Duration = Duration::from_secs(2);
/// How long finished workers get to wind down before being aborted.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

pub struct DownloadConfig {
    pub output_dir: PathBuf,
    pub max_peers: usize,
}

/// Everything a peer worker needs to know about the torrent.
pub struct Session {
    pub torrent: TorrentFile,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub storage: Arc<Storage>,
}

pub async fn download_file(torrent_path: &Path, config: &DownloadConfig) -> Result<()> {
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

    let peer_id = peer::generate_peer_id();
    let request = AnnounceRequest {
        info_hash,
        peer_id,
        port: peer::LISTEN_PORT,
        uploaded: 0,
        downloaded: 0,
        left: torrent.length(),
        event: AnnounceEvent::Started,
    };
    let (announcer, peers) = Announcer::start(torrent.trackers(), request).await?;
    if peers.is_empty() {
        announcer.stop(false, 0).await;
        bail!("no tracker returned any peers");
    }
    info!(peers = peers.len(), "tracker announce succeeded");

    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id,
        storage,
    });
    download_from_peers(session, &peers, config.max_peers, Some(announcer)).await?;
    info!(output = %config.output_dir.display(), "download complete");
    Ok(())
}

/// Downloads every piece from `peers`, connected to at most `max_peers` at
/// once. A peer that drops is replaced by the next one, and failed peers are
/// retried with backoff (see `PeerPool`).
///
/// With an `announcer`, the tracker is re-announced to on its interval and
/// the peers it returns join the pool. If we run out of peers, the tracker is
/// asked for more once before giving up. At the end the tracker is sent
/// `completed` (if the download finished) and `stopped`.
pub async fn download_from_peers(
    session: Arc<Session>,
    peers: &[SocketAddr],
    max_peers: usize,
    mut announcer: Option<Announcer>,
) -> Result<()> {
    let num_pieces = session.torrent.num_pieces();
    let queue = Arc::new(PieceQueue::new(num_pieces));
    let progress = Arc::new(Progress::new(session.torrent.length()));
    let mut pool = PeerPool::default();
    pool.add(peers.iter().copied());

    let mut workers = JoinSet::new();
    // Set once we ran out of peers and asked the tracker for more; cleared
    // when a new connection is started.
    let mut asked_for_peers = false;
    while !queue.is_done() {
        while workers.len() < max_peers {
            let Some(addr) = pool.next(Instant::now()) else {
                break;
            };
            asked_for_peers = false;
            let session = Arc::clone(&session);
            let queue = Arc::clone(&queue);
            let progress = Arc::clone(&progress);
            workers.spawn(async move {
                let mut pieces = 0;
                let result = peer_worker(&session, &queue, &progress, addr, &mut pieces).await;
                (addr, pieces, result)
            });
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
                let (addr, pieces, result) = match joined {
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
                        pool.ban(addr);
                    }
                    Err(e) => {
                        debug!(%addr, pieces, "peer dropped: {e:#}");
                        pool.failed(addr, pieces > 0, Instant::now());
                    }
                }
            }
            () = sleep_until_some(retry_at) => {}
            peers = reannounce(&mut announcer, &progress.downloaded) => pool.add(peers),
        }
    }

    // Workers still downloading notice completion and exit (sending Cancel
    // for duplicate endgame requests); ones still connecting are cut short.
    let drain = async { while workers.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        workers.shutdown().await;
    }
    progress.bar.finish();

    let remaining = queue.remaining();
    if let Some(announcer) = announcer {
        let downloaded = progress.downloaded.load(Ordering::Relaxed);
        announcer.stop(remaining == 0, downloaded).await;
    }
    if remaining > 0 {
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

/// Runs the next re-announce and returns its peers, or waits forever
/// without a tracker. Cancel-safe.
async fn reannounce(announcer: &mut Option<Announcer>, downloaded: &AtomicU64) -> Vec<SocketAddr> {
    match announcer {
        Some(announcer) => announcer.poll(downloaded).await,
        None => std::future::pending().await,
    }
}

/// A peer that broke the protocol or sent bad data. It is never retried.
#[derive(Debug)]
struct Misbehaved(&'static str);

impl std::fmt::Display for Misbehaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Misbehaved {}

/// Bytes verified and written, shown on the progress bar and reported to the
/// tracker.
struct Progress {
    bar: ProgressBar,
    downloaded: AtomicU64,
}

impl Progress {
    fn new(total: u64) -> Self {
        let bar = ProgressBar::new(total);
        let style = ProgressStyle::with_template(
            "{bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta} {msg}",
        );
        if let Ok(style) = style {
            bar.set_style(style);
        }
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

/// Downloads from one peer until the torrent is complete or the peer fails.
/// `pieces_done` counts the pieces this peer delivered first, even on error.
async fn peer_worker(
    session: &Session,
    queue: &PieceQueue,
    progress: &Progress,
    addr: SocketAddr,
    pieces_done: &mut usize,
) -> Result<()> {
    let torrent = &session.torrent;
    let mut conn = PeerConnection::connect(
        addr,
        session.info_hash,
        session.peer_id,
        torrent.num_pieces(),
    )
    .await?;
    conn.negotiate().await?;
    debug!(%addr, "connected and unchoked");

    let mut pieces = queue.register_peer();
    let mut hash_failures = 0;
    let mut last_useful = Instant::now();
    while !queue.is_done() {
        pieces.update(conn.peer_has());
        let Some(claim) = queue.claim(&pieces) else {
            if last_useful.elapsed() > IDLE_TIMEOUT {
                bail!("peer has no pieces we need");
            }
            // Wait for a piece to be returned or completed, or for the peer
            // to announce a new piece. `receive_untimed` is cancel-safe.
            tokio::select! {
                _ = queue.changed.notified() => {}
                message = conn.receive_untimed() => { message?; }
                _ = tokio::time::sleep(IDLE_POLL) => {}
            }
            continue;
        };
        last_useful = Instant::now();

        let index = claim.index;
        let piece_len = torrent.piece_len(index)?;
        let hash = torrent.piece_hash(index)?;
        match download_piece(&mut conn, &claim, piece_len, &hash).await {
            Ok(data) => {
                // In endgame another peer may have finished it meanwhile. If
                // both pass this check, both write the same verified bytes.
                if claim.is_completed() {
                    continue;
                }
                session.storage.write_piece(index, data).await?;
                if claim.complete() {
                    *pieces_done += 1;
                    progress.add(piece_len);
                    debug!(%addr, index, "piece complete");
                }
            }
            Err(PieceError::CompletedElsewhere) => {
                debug!(%addr, index, "piece completed by another peer");
            }
            Err(PieceError::Choked) => {
                debug!(%addr, index, "choked mid-piece");
                drop(claim);
                conn.wait_unchoke().await?;
            }
            Err(PieceError::HashMismatch) => {
                hash_failures += 1;
                warn!(%addr, index, "piece failed hash check");
                if hash_failures >= MAX_HASH_FAILURES {
                    bail!(Misbehaved("too many corrupt pieces"));
                }
            }
            Err(PieceError::Other(e)) => return Err(e),
        }
    }
    Ok(())
}

#[derive(Debug)]
enum PieceError {
    /// The peer choked us; outstanding requests were discarded.
    Choked,
    /// Another peer completed the piece first (endgame); outstanding
    /// requests were cancelled.
    CompletedElsewhere,
    HashMismatch,
    Other(anyhow::Error),
}

impl From<anyhow::Error> for PieceError {
    fn from(e: anyhow::Error) -> Self {
        PieceError::Other(e)
    }
}

/// Downloads one piece with up to `PIPELINE` block requests in flight.
/// Gives up early, cancelling outstanding requests, if another peer
/// completes the piece first.
async fn download_piece(
    conn: &mut PeerConnection,
    claim: &PieceClaim<'_>,
    piece_len: u64,
    expected_hash: &[u8; 20],
) -> Result<Vec<u8>, PieceError> {
    let piece_index = u32::try_from(claim.index).map_err(|_| anyhow!("piece index too large"))?;
    let piece_len = u32::try_from(piece_len).map_err(|_| anyhow!("piece too large"))?;
    let num_blocks = piece_len.div_ceil(BLOCK_SIZE) as usize;
    let block_len = |block: usize| {
        let begin = block as u32 * BLOCK_SIZE;
        BLOCK_SIZE.min(piece_len - begin)
    };

    let mut data = vec![0u8; piece_len as usize];
    let mut received = vec![false; num_blocks];
    let mut next_request = 0;
    let mut in_flight = 0;
    let mut done = 0;

    while done < num_blocks {
        let mut requests = Vec::new();
        while in_flight < PIPELINE && next_request < num_blocks {
            requests.push(PeerMessage::Request {
                index: piece_index,
                begin: next_request as u32 * BLOCK_SIZE,
                length: block_len(next_request),
            });
            next_request += 1;
            in_flight += 1;
        }
        if !requests.is_empty() {
            conn.send_all(&requests).await?;
        }

        // `receive` is cancel-safe, so losing the race loses no data.
        let message = tokio::select! {
            message = conn.receive() => Some(message?),
            () = claim.completed_elsewhere() => None,
        };
        let Some(message) = message else {
            let cancels: Vec<PeerMessage> = (0..next_request)
                .filter(|&block| !received[block])
                .map(|block| PeerMessage::Cancel {
                    index: piece_index,
                    begin: block as u32 * BLOCK_SIZE,
                    length: block_len(block),
                })
                .collect();
            if !cancels.is_empty() {
                conn.send_all(&cancels).await?;
            }
            return Err(PieceError::CompletedElsewhere);
        };
        match message {
            PeerMessage::Piece {
                index,
                begin,
                block,
            } if index == piece_index => {
                let block_index = (begin / BLOCK_SIZE) as usize;
                if begin % BLOCK_SIZE != 0
                    || block_index >= num_blocks
                    || block.len() != block_len(block_index) as usize
                {
                    return Err(anyhow!(Misbehaved("peer sent an invalid block")).into());
                }
                // A late reply to a request abandoned on a choke, for a block
                // we have not asked for again yet. Ignore it rather than
                // dropping an honest peer.
                if block_index >= next_request {
                    continue;
                }
                // Saturating: after a choke, late replies to the abandoned
                // requests can arrive alongside replies to the new ones.
                in_flight = in_flight.saturating_sub(1);
                if received[block_index] {
                    continue;
                }
                let start = begin as usize;
                data[start..start + block.len()].copy_from_slice(&block);
                received[block_index] = true;
                done += 1;
            }
            PeerMessage::Choke => return Err(PieceError::Choked),
            // Blocks of other pieces are late replies to abandoned requests;
            // Have/Bitfield were already applied by `receive`.
            _ => {}
        }
    }

    let actual: [u8; 20] = sha1::Sha1::digest(&data).into();
    if &actual != expected_hash {
        return Err(PieceError::HashMismatch);
    }
    Ok(data)
}

/// Pieces not yet downloaded, shared between all peer workers.
struct PieceQueue {
    state: Mutex<QueueState>,
    /// Signalled when a piece is returned to the queue or completed.
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
}

impl PieceQueue {
    fn new(num_pieces: usize) -> Self {
        Self {
            state: Mutex::new(QueueState {
                pending: (0..num_pieces).collect(),
                in_flight: BTreeMap::new(),
                remaining: num_pieces,
                availability: vec![0; num_pieces],
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
    /// without `complete()`.
    fn claim(&self, peer: &PeerPieces<'_>) -> Option<PieceClaim<'_>> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let mut rng = rand::rng();
        let index = if state.pending.is_empty() {
            let candidates = state.in_flight.iter().map(|(&i, &holders)| (i, holders));
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

    fn remaining(&self) -> usize {
        self.lock().remaining
    }

    fn is_done(&self) -> bool {
        self.remaining() == 0
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

    /// Resolves once another worker has completed this piece.
    async fn completed_elsewhere(&self) {
        loop {
            let mut changed = std::pin::pin!(self.queue.changed.notified());
            // Register before checking, so a completion in between still
            // wakes us.
            changed.as_mut().enable();
            if self.is_completed() {
                return;
            }
            changed.await;
        }
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

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
        let queue = PieceQueue::new(3);
        assert!(queue.claim(&peer(&queue, &[false; 3])).is_none());
        let partial = peer(&queue, &[false, true, true]);
        let claim = queue.claim(&partial).unwrap();
        let second = queue.claim(&partial).unwrap();
        let mut claimed = [claim.index, second.index];
        claimed.sort();
        assert_eq!(claimed, [1, 2]);
        assert!(queue.claim(&partial).is_none());
        drop(second);
        claim.complete();
        assert_eq!(queue.remaining(), 2);
        // Piece 0 is now the rarest: only `full` has it.
        let full = peer(&queue, &[true; 3]);
        assert_eq!(queue.claim(&full).unwrap().index, 0);
    }

    #[test]
    fn dropped_claim_returns_piece() {
        let queue = PieceQueue::new(1);
        let seed = peer(&queue, &[true]);
        drop(queue.claim(&seed).unwrap());
        let claim = queue.claim(&seed).unwrap();
        claim.complete();
        assert!(queue.is_done());
        assert!(queue.claim(&seed).is_none());
    }

    #[test]
    fn claims_rarest_piece_first() {
        let queue = PieceQueue::new(4);
        let _a = peer(&queue, &[true, true, false, true]);
        let _b = peer(&queue, &[true, false, false, true]);
        let seed = peer(&queue, &[true; 4]);
        assert_eq!(availability(&queue), [3, 2, 1, 3]);
        let first = queue.claim(&seed).unwrap();
        assert_eq!(first.index, 2);
        let second = queue.claim(&seed).unwrap();
        assert_eq!(second.index, 1);
    }

    #[test]
    fn availability_follows_peer_updates_and_disconnects() {
        let queue = PieceQueue::new(3);
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
        let queue = PieceQueue::new(2);
        let seed = peer(&queue, &[true; 2]);
        let a = queue.claim(&seed).unwrap();
        let b = queue.claim(&seed).unwrap();
        // Nothing pending: further claims join in-flight pieces, spreading
        // over the one with the fewest downloaders.
        let c = queue.claim(&seed).unwrap();
        let d = queue.claim(&seed).unwrap();
        assert_ne!(c.index, d.index);
        let (first, dup) = if c.index == a.index { (a, c) } else { (b, c) };

        assert!(!dup.is_completed());
        assert!(first.complete());
        assert!(dup.is_completed());
        assert!(!dup.complete(), "a piece must only be completed once");
        assert_eq!(queue.remaining(), 1);
        // The completed piece is never handed out again.
        let e = queue.claim(&seed).unwrap();
        assert_eq!(e.index, d.index);
    }

    #[test]
    fn endgame_piece_is_requeued_only_when_every_claim_is_dropped() {
        let queue = PieceQueue::new(1);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed).unwrap();
        let b = queue.claim(&seed).unwrap();
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
        let queue = PieceQueue::new(1);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed).unwrap();
        let b = queue.claim(&seed).unwrap();
        assert!(a.complete());
        drop(b);
        assert!(queue.is_done());
        assert!(queue.lock().pending.is_empty());
        assert!(queue.claim(&seed).is_none());
    }

    #[tokio::test]
    async fn ignores_late_block_not_yet_requested_again() {
        // More blocks than fit in the pipeline, so the last one is not
        // requested in the first batch.
        const BLOCKS: u32 = PIPELINE as u32 + 4;
        let piece: Vec<u8> = (0..BLOCKS * BLOCK_SIZE).map(|i| (i % 251) as u8).collect();
        let hash: [u8; 20] = sha1::Sha1::digest(&piece).into();
        let info_hash = [7u8; 20];

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seed_piece = piece.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            let reply = peer::handshake_bytes(&info_hash, &[1u8; 20]);
            socket.write_all(&reply).await.unwrap();
            let block = |b: u32| {
                let start = (b * BLOCK_SIZE) as usize;
                PeerMessage::Piece {
                    index: 0,
                    begin: b * BLOCK_SIZE,
                    block: seed_piece[start..start + BLOCK_SIZE as usize].to_vec(),
                }
            };
            // Still in transit from a request made before a choke.
            socket.write_all(&block(BLOCKS - 1).encode()).await.unwrap();
            loop {
                let Ok(len) = socket.read_u32().await else {
                    return;
                };
                let mut payload = vec![0u8; len as usize];
                if socket.read_exact(&mut payload).await.is_err() {
                    return;
                }
                if let Ok(PeerMessage::Request { begin, .. }) = PeerMessage::decode(&payload) {
                    let reply = block(begin / BLOCK_SIZE).encode();
                    socket.write_all(&reply).await.unwrap();
                }
            }
        });

        let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1)
            .await
            .unwrap();
        let queue = PieceQueue::new(1);
        let seed = peer(&queue, &[true]);
        let claim = queue.claim(&seed).unwrap();
        let data = download_piece(&mut conn, &claim, piece.len() as u64, &hash)
            .await
            .expect("late block dropped the peer");
        assert_eq!(data, piece);
    }

    #[tokio::test]
    async fn completed_elsewhere_wakes_on_completion() {
        let queue = PieceQueue::new(1);
        let seed = peer(&queue, &[true]);
        let a = queue.claim(&seed).unwrap();
        let b = queue.claim(&seed).unwrap();
        let short = Duration::from_millis(50);
        assert!(
            tokio::time::timeout(short, b.completed_elsewhere())
                .await
                .is_err(),
            "resolved before the piece was completed"
        );
        let complete_later = async {
            tokio::task::yield_now().await;
            a.complete();
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(b.completed_elsewhere(), complete_later)
        })
        .await
        .expect("not woken by completion");
    }
}
