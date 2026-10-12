use crate::peer::{self, PeerConnection, PeerMessage};
use crate::storage::Storage;
use crate::torrent::{self, TorrentFile};
use crate::tracker::{self, AnnounceRequest};
use anyhow::{Result, anyhow, bail};
use indicatif::{ProgressBar, ProgressStyle};
use sha1::Digest;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

    let peer_id = peer::generate_peer_id();
    let request = AnnounceRequest {
        info_hash,
        peer_id,
        port: peer::LISTEN_PORT,
        uploaded: 0,
        downloaded: 0,
        left: torrent.length(),
    };
    let response = tracker::announce(&torrent.trackers(), &request).await?;
    info!(peers = response.peers.len(), "tracker announce succeeded");

    let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir)?);
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id,
        storage,
    });
    download_from_peers(session, &response.peers, config.max_peers).await?;
    info!(output = %config.output_dir.display(), "download complete");
    Ok(())
}

/// Downloads every piece from up to `max_peers` of `peers`.
pub async fn download_from_peers(
    session: Arc<Session>,
    peers: &[SocketAddr],
    max_peers: usize,
) -> Result<()> {
    let num_pieces = session.torrent.num_pieces();
    let queue = Arc::new(PieceQueue::new(num_pieces));
    let progress = progress_bar(session.torrent.length());

    let mut workers = JoinSet::new();
    for &addr in peers.iter().take(max_peers) {
        let session = Arc::clone(&session);
        let queue = Arc::clone(&queue);
        let progress = progress.clone();
        workers.spawn(async move {
            match peer_worker(&session, &queue, &progress, addr).await {
                Ok(()) => debug!(%addr, "peer finished"),
                Err(e) => debug!(%addr, "peer dropped: {e:#}"),
            }
        });
    }
    while let Some(result) = workers.join_next().await {
        if let Err(e) = result {
            warn!("peer task failed: {e}");
        }
    }
    progress.finish();

    let remaining = queue.remaining();
    if remaining > 0 {
        bail!("download incomplete: {remaining} of {num_pieces} pieces missing and no peers left");
    }
    Ok(())
}

fn progress_bar(total: u64) -> ProgressBar {
    let bar = ProgressBar::new(total);
    let style = ProgressStyle::with_template(
        "{bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta} {msg}",
    );
    if let Ok(style) = style {
        bar.set_style(style);
    }
    bar
}

async fn peer_worker(
    session: &Session,
    queue: &PieceQueue,
    progress: &ProgressBar,
    addr: SocketAddr,
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

    let mut hash_failures = 0;
    let mut last_useful = Instant::now();
    while !queue.is_done() {
        let Some(claim) = queue.claim(conn.peer_has()) else {
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
        match download_piece(&mut conn, index, piece_len, &hash).await {
            Ok(data) => {
                session.storage.write_piece(index, data).await?;
                claim.complete();
                progress.inc(piece_len);
                debug!(%addr, index, "piece complete");
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
                    bail!("too many corrupt pieces");
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
    HashMismatch,
    Other(anyhow::Error),
}

impl From<anyhow::Error> for PieceError {
    fn from(e: anyhow::Error) -> Self {
        PieceError::Other(e)
    }
}

/// Downloads one piece with up to `PIPELINE` block requests in flight.
async fn download_piece(
    conn: &mut PeerConnection,
    index: usize,
    piece_len: u64,
    expected_hash: &[u8; 20],
) -> Result<Vec<u8>, PieceError> {
    let piece_index = u32::try_from(index).map_err(|_| anyhow!("piece index too large"))?;
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

        match conn.receive().await? {
            PeerMessage::Piece {
                index,
                begin,
                block,
            } if index == piece_index => {
                let block_index = (begin / BLOCK_SIZE) as usize;
                if begin % BLOCK_SIZE != 0
                    || block_index >= next_request
                    || block.len() != block_len(block_index) as usize
                {
                    return Err(anyhow!("peer sent a block we did not request").into());
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
    pending: BTreeSet<usize>,
    /// Pieces not yet verified and written, whether pending or in flight.
    remaining: usize,
}

impl PieceQueue {
    fn new(num_pieces: usize) -> Self {
        Self {
            state: Mutex::new(QueueState {
                pending: (0..num_pieces).collect(),
                remaining: num_pieces,
            }),
            changed: Notify::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Takes the first pending piece the peer has. The piece goes back to
    /// the queue when the claim is dropped without `complete()`.
    fn claim(&self, peer_has: &[bool]) -> Option<PieceClaim<'_>> {
        let mut state = self.lock();
        let index = state
            .pending
            .iter()
            .copied()
            .find(|&i| peer_has.get(i).copied().unwrap_or(false))?;
        state.pending.remove(&index);
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
    fn complete(mut self) {
        self.completed = true;
        self.queue.lock().remaining -= 1;
        self.queue.changed.notify_waiters();
    }
}

impl Drop for PieceClaim<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.queue.lock().pending.insert(self.index);
            self.queue.changed.notify_waiters();
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claims_only_pieces_the_peer_has() {
        let queue = PieceQueue::new(3);
        assert!(queue.claim(&[false, false, false]).is_none());
        let claim = queue.claim(&[false, true, true]).unwrap();
        assert_eq!(claim.index, 1);
        let second = queue.claim(&[false, true, true]).unwrap();
        assert_eq!(second.index, 2);
        assert!(queue.claim(&[false, true, true]).is_none());
        drop(second);
        claim.complete();
        assert_eq!(queue.remaining(), 2);
        assert_eq!(queue.claim(&[true, true, true]).unwrap().index, 0);
    }

    #[test]
    fn dropped_claim_returns_piece() {
        let queue = PieceQueue::new(1);
        drop(queue.claim(&[true]).unwrap());
        let claim = queue.claim(&[true]).unwrap();
        claim.complete();
        assert!(queue.is_done());
        assert!(queue.claim(&[true]).is_none());
    }
}
