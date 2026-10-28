use crate::peer::{BlockRef, PeerConnection, PeerMessage};
use crate::rate_limit::Allowance;
use crate::torrent::TorrentFile;
use anyhow::{Result, anyhow, bail};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::Instant;
use tracing::debug;

use super::queue::{PeerPieces, PieceClaim, PieceQueue};
use super::{BLOCK_SIZE, Misbehaved};

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

/// Tops up the requests in flight to the pipeline depth, claiming another
/// piece whenever every block of the held ones has been requested. While
/// the peer chokes us, only its allowed fast pieces (fast extension) are
/// asked for.
///
/// Under a download limit, each request also waits for this peer's turn in
/// `budget`, so the data it asks for is paid for before it is sent and
/// never arrives faster than the limit. A turn not yet come is kept for the
/// next call, and given up if there is nothing left to claim.
pub(super) async fn request_blocks<'q>(
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
        requests.push(active[pos].request_next(Instant::now()));
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
pub(super) async fn drop_completed(
    conn: &mut PeerConnection,
    active: &mut Vec<ActivePiece<'_>>,
    mut cancelled: Option<&mut HashSet<BlockRef>>,
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

/// Takes back the requests the peer has not answered within `timeout`, and
/// cancels them, so their blocks are asked for again. With `cancelled` (fast
/// extension), the cancelled requests are added to it, as the peer still
/// answers them.
pub(super) async fn expire_requests(
    conn: &mut PeerConnection,
    active: &mut [ActivePiece<'_>],
    now: Instant,
    timeout: Duration,
    mut cancelled: Option<&mut HashSet<BlockRef>>,
) -> Result<()> {
    let mut cancels = Vec::new();
    for piece in active {
        for request in piece.expire(now, timeout) {
            debug!(addr = %conn.addr, ?request, "request timed out");
            cancels.push(request.cancel());
            if let Some(cancelled) = cancelled.as_deref_mut() {
                cancelled.insert(request);
            }
        }
    }
    if !cancels.is_empty() {
        conn.send_all(&cancels).await?;
    }
    Ok(())
}

/// While the peer chokes us under the fast extension, our requests stand
/// until it rejects them. Lets go of the held pieces we may not ask it for
/// meanwhile (those not in its allowed fast set) once none of their
/// requests are outstanding, so that other peers can claim them.
pub(super) fn release_choked(active: &mut Vec<ActivePiece<'_>>, allowed: &[bool]) {
    active.retain(|piece| {
        let keep = piece.in_flight() > 0 || allowed.get(piece.claim.index) == Some(&true);
        if !keep {
            debug!(index = piece.claim.index, "choked: releasing piece");
        }
        keep
    });
}

/// Sizes a peer's request pipeline to its measured download rate, so that
/// about `PIPELINE_QUEUE_TIME` worth of blocks is always on its way.
///
/// A deeper pipeline lets more data arrive per round trip, so the measured
/// rate grows until the link is the limit, and the depth follows it: a fast
/// peer gets a deep queue, a slow one a short queue (and so holds fewer
/// pieces hostage).
pub(super) struct Pipeline {
    depth: usize,
    window_start: Instant,
    window_bytes: u64,
}

impl Pipeline {
    pub(super) fn new(now: Instant) -> Self {
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
    pub(super) fn received(&mut self, bytes: u64, now: Instant) {
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
    /// Asked for at the given time and not yet answered.
    Requested(Instant),
    Received,
}

/// A claimed piece being downloaded. Blocks are requested in order, and a
/// rejected block is asked for again before any later one.
pub(super) struct ActivePiece<'a> {
    pub(super) claim: PieceClaim<'a>,
    /// `claim.index` as sent on the wire.
    pub(super) index: u32,
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
    pub(super) fn new(claim: PieceClaim<'a>, piece_len: u64, hash: [u8; 20]) -> Result<Self> {
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
    fn block_request(&self, block: usize) -> BlockRef {
        BlockRef {
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
    pub(super) fn in_flight(&self) -> usize {
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

    /// The request for the first wanted block, sent at `now`. Call only if
    /// `has_unrequested`.
    fn request_next(&mut self, now: Instant) -> PeerMessage {
        let block = self.next_request;
        self.blocks[block] = BlockState::Requested(now);
        self.in_flight += 1;
        self.next_request = (block + 1..self.blocks.len())
            .find(|&b| self.blocks[b] == BlockState::Wanted)
            .unwrap_or(self.blocks.len());
        self.block_request(block).request()
    }

    /// Every request still outstanding.
    fn outstanding(&self) -> impl Iterator<Item = BlockRef> + '_ {
        (0..self.blocks.len())
            .filter(|&block| matches!(self.blocks[block], BlockState::Requested(_)))
            .map(|block| self.block_request(block))
    }

    /// Takes back an outstanding request that the peer rejected (fast
    /// extension), so the block is asked for again. Returns false if no
    /// such request is outstanding.
    pub(super) fn reject(&mut self, begin: u32, length: u32) -> bool {
        let Some(block) = self.block_at(begin) else {
            return false;
        };
        if !matches!(self.blocks[block], BlockState::Requested(_))
            || length != self.block_len(block)
        {
            return false;
        }
        self.blocks[block] = BlockState::Wanted;
        self.in_flight -= 1;
        self.next_request = self.next_request.min(block);
        true
    }

    /// Takes back the requests that were sent `timeout` or longer before
    /// `now` and are still unanswered, so their blocks are asked for again,
    /// and returns them.
    pub(super) fn expire(&mut self, now: Instant, timeout: Duration) -> Vec<BlockRef> {
        let mut expired = Vec::new();
        for block in 0..self.blocks.len() {
            if let BlockState::Requested(sent) = self.blocks[block]
                && now.saturating_duration_since(sent) >= timeout
            {
                self.blocks[block] = BlockState::Wanted;
                self.in_flight -= 1;
                self.next_request = self.next_request.min(block);
                expired.push(self.block_request(block));
            }
        }
        expired
    }

    /// Stores a block of this piece. Returns false if it was not one we are
    /// waiting for: a duplicate, or a late reply to a request that was
    /// abandoned on a choke, or rejected, for a block we have not asked for
    /// again yet. Those are ignored rather than dropping an honest peer.
    pub(super) fn add_block(&mut self, begin: u32, block: &[u8]) -> Result<bool> {
        let index = match self.block_at(begin) {
            Some(index) if block.len() == self.block_len(index) as usize => index,
            _ => bail!(Misbehaved("peer sent an invalid block")),
        };
        if !matches!(self.blocks[index], BlockState::Requested(_)) {
            return Ok(false);
        }
        let start = begin as usize;
        self.data[start..start + block.len()].copy_from_slice(block);
        self.blocks[index] = BlockState::Received;
        self.in_flight -= 1;
        self.done += 1;
        Ok(true)
    }

    pub(super) fn is_complete(&self) -> bool {
        self.done == self.blocks.len()
    }

    /// The claim and data of a complete piece, and the hash the data must
    /// have, which `Storage::verify_and_write` checks.
    pub(super) fn into_parts(self) -> (PieceClaim<'a>, Vec<u8>, [u8; 20]) {
        (self.claim, self.data, self.hash)
    }
}

#[cfg(test)]
mod tests;
