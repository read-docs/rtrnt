use crate::peer::extension::{self};
use crate::peer::{PeerConnection, PeerMessage};
use crate::storage::Storage;
use anyhow::{Result, bail};
use std::collections::VecDeque;
use std::sync::Arc;

use super::{BLOCK_SIZE, Misbehaved};

/// Longest block a peer may request. 16 KiB is what every client asks for;
/// some serve up to 128 KiB, but nobody needs that.
const MAX_REQUEST_LEN: u32 = BLOCK_SIZE;
/// Requests a peer may have queued with us; more are dropped. Told to peers
/// in the extension handshake (`reqq`).
const MAX_QUEUED_UPLOADS: usize = extension::MAX_QUEUED_REQUESTS as usize;

/// Reads a block the peer asked for from disk and sends it.
pub(super) async fn serve_block(
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
pub(super) struct BlockRequest {
    pub(super) index: u32,
    pub(super) begin: u32,
    pub(super) length: u32,
}

impl BlockRequest {
    /// The Cancel that withdraws this request.
    pub(super) fn cancel(self) -> PeerMessage {
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
    pub(super) fn reject(self) -> PeerMessage {
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
    pub(super) fn check(&self, piece_len: u64) -> Result<()> {
        let end = u64::from(self.begin) + u64::from(self.length);
        if self.length == 0 || self.length > MAX_REQUEST_LEN || end > piece_len {
            bail!(Misbehaved("peer sent an invalid request"));
        }
        Ok(())
    }
}

/// Blocks a peer asked us for that have not been sent yet, oldest first.
#[derive(Default)]
pub(super) struct Uploads(VecDeque<BlockRequest>);

impl Uploads {
    /// Queues a request, unless it is queued already. Returns false, dropping
    /// it, if `MAX_QUEUED_UPLOADS` requests are queued.
    pub(super) fn push(&mut self, request: BlockRequest) -> bool {
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
    pub(super) fn cancel(&mut self, request: &BlockRequest) -> bool {
        let queued = self.0.len();
        self.0.retain(|queued| queued != request);
        self.0.len() < queued
    }

    pub(super) fn pop(&mut self) -> Option<BlockRequest> {
        self.0.pop_front()
    }

    /// The request to answer next.
    pub(super) fn front(&self) -> Option<&BlockRequest> {
        self.0.front()
    }

    /// Drops every queued request that `keep` returns false for, and
    /// returns the dropped ones, oldest first.
    pub(super) fn retain(&mut self, keep: impl Fn(&BlockRequest) -> bool) -> Vec<BlockRequest> {
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

    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests;
