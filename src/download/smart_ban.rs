//! Smart ban: finding out which peers sent corrupt data, and banning them.
//!
//! When a piece fails its hash check, the hash of each of its blocks is kept
//! with whoever sent the block. When the piece is later completed from
//! another source, the blocks of that good copy are compared with the kept
//! ones, and the senders of those that differ are banned, even if they have
//! left by then. Without this, a peer that sends one bad piece and
//! reconnects is only ever dropped, as the strikes are counted per
//! connection and start over.
//!
//! Web seeds count as a source of the good copy, but are never blamed: they
//! have their own backoff.

use super::BLOCK_SIZE;
use sha1::{Digest, Sha1};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tracing::{debug, warn};

/// Failed pieces remembered at once; the oldest is forgotten first.
const MAX_PIECES: usize = 32;
/// Failed copies remembered of one piece; the oldest is forgotten first.
const MAX_ATTEMPTS: usize = 3;
/// Peers marked as banned for the workers still connected to them.
const MAX_BANNED: usize = 1024;

/// The SHA-1 of one block.
pub(super) type BlockHash = [u8; 20];

/// The hash of each block of `data`, a piece, in order.
pub(super) fn block_hashes(data: &[u8]) -> Vec<BlockHash> {
    data.chunks(BLOCK_SIZE as usize)
        .map(|block| Sha1::digest(block).into())
        .collect()
}

/// A peer that sent blocks, as the swarm bans it: by the address we
/// connected to, or by the address alone if it connected to us.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Sender {
    pub(super) addr: SocketAddr,
    /// Whether the peer connected to us.
    pub(super) inbound: bool,
}

/// One failed copy of a piece: the hash of each block, and who sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Attempt {
    senders: Vec<Sender>,
    /// For each block, its hash and the index of its sender in `senders`.
    blocks: Vec<(BlockHash, u16)>,
}

impl Attempt {
    /// A copy that came whole from `sender`.
    pub(super) fn whole(data: &[u8], sender: Sender) -> Self {
        Self {
            senders: vec![sender],
            blocks: block_hashes(data)
                .into_iter()
                .map(|hash| (hash, 0))
                .collect(),
        }
    }

    /// A copy assembled from blocks of several senders, `blocks` giving each
    /// block's hash and sender in order. Blocks of a sender past the 65535th
    /// (more than a piece has blocks) are left out. Every piece comes whole
    /// from one peer today, so only tests build these.
    #[cfg(test)]
    pub(super) fn assembled(blocks: impl IntoIterator<Item = (BlockHash, Sender)>) -> Self {
        let mut senders: Vec<Sender> = Vec::new();
        let mut kept = Vec::new();
        for (hash, sender) in blocks {
            let slot = match senders.iter().position(|known| *known == sender) {
                Some(slot) => slot,
                None => {
                    senders.push(sender);
                    senders.len() - 1
                }
            };
            if let Ok(slot) = u16::try_from(slot) {
                kept.push((hash, slot));
            }
        }
        Self {
            senders,
            blocks: kept,
        }
    }

    /// The senders of the blocks that are not as in `good`, the hashes of
    /// the verified piece, each once. A block `good` lacks counts as
    /// differing.
    fn guilty(&self, good: &[BlockHash]) -> Vec<Sender> {
        let mut guilty: Vec<Sender> = Vec::new();
        for (block, (hash, slot)) in self.blocks.iter().enumerate() {
            if good.get(block) == Some(hash) {
                continue;
            }
            if let Some(&sender) = self.senders.get(usize::from(*slot))
                && !guilty.contains(&sender)
            {
                guilty.push(sender);
            }
        }
        guilty
    }
}

/// The failed copies of pieces that no good copy has been seen of yet.
/// Bounded: at most `MAX_PIECES` pieces with `MAX_ATTEMPTS` copies each.
#[derive(Debug, Default)]
pub(super) struct Suspects {
    pieces: HashMap<usize, Vec<Attempt>>,
    /// The pieces in `pieces`, oldest first.
    order: VecDeque<usize>,
}

impl Suspects {
    /// Remembers a copy of piece `index` that failed its hash check.
    pub(super) fn record(&mut self, index: usize, attempt: Attempt) {
        if !self.pieces.contains_key(&index) {
            if self.order.len() >= MAX_PIECES
                && let Some(oldest) = self.order.pop_front()
            {
                self.pieces.remove(&oldest);
            }
            self.order.push_back(index);
        }
        let attempts = self.pieces.entry(index).or_default();
        if attempts.len() >= MAX_ATTEMPTS {
            attempts.remove(0);
        }
        attempts.push(attempt);
    }

    /// Whether a failed copy of piece `index` is remembered.
    pub(super) fn watching(&self, index: usize) -> bool {
        self.pieces.contains_key(&index)
    }

    /// Piece `index` has been completed, `good` being the hash of each of
    /// its blocks. Forgets its failed copies and returns who sent a block
    /// that differs, each once.
    pub(super) fn resolve(&mut self, index: usize, good: &[BlockHash]) -> Vec<Sender> {
        let Some(attempts) = self.pieces.remove(&index) else {
            return Vec::new();
        };
        self.order.retain(|&i| i != index);
        let mut guilty: Vec<Sender> = Vec::new();
        for sender in attempts.iter().flat_map(|attempt| attempt.guilty(good)) {
            if !guilty.contains(&sender) {
                guilty.push(sender);
            }
        }
        guilty
    }
}

/// What the workers of a swarm share for smart banning: the failed copies,
/// and the peers found guilty.
pub(super) struct SmartBan {
    suspects: Mutex<Suspects>,
    /// Guilty peers, for the swarm to ban.
    verdicts: UnboundedSender<Sender>,
    /// The addresses banned, so that a worker still connected to one drops
    /// it.
    banned: Mutex<HashSet<SocketAddr>>,
}

impl SmartBan {
    /// A fresh `SmartBan`, and the receiver of its verdicts.
    pub(super) fn new() -> (Self, UnboundedReceiver<Sender>) {
        let (verdicts, receiver) = unbounded_channel();
        let smart = Self {
            suspects: Mutex::default(),
            verdicts,
            banned: Mutex::default(),
        };
        (smart, receiver)
    }

    fn suspects(&self) -> MutexGuard<'_, Suspects> {
        self.suspects.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Remembers that `sender` sent `data`, which is piece `index`, and
    /// failed its hash check.
    pub(super) fn failed(&self, index: usize, data: &[u8], sender: Sender) {
        debug!(index, addr = %sender.addr, "remembering a failed piece");
        let attempt = Attempt::whole(data, sender);
        self.suspects().record(index, attempt);
    }

    /// Whether a failed copy of piece `index` is remembered, so that the
    /// good copy is worth `verified`.
    pub(super) fn watching(&self, index: usize) -> bool {
        self.suspects().watching(index)
    }

    /// Piece `index` passed its hash check with `data`: bans the senders of
    /// the failed copies' blocks that differ from it.
    pub(super) fn verified(&self, index: usize, data: &[u8]) {
        let good = block_hashes(data);
        let guilty = self.suspects().resolve(index, &good);
        for sender in guilty {
            warn!(
                addr = %sender.addr,
                index,
                "banning peer: sent corrupt blocks of a piece completed from elsewhere"
            );
            {
                let mut banned = self.banned.lock().unwrap_or_else(PoisonError::into_inner);
                if banned.len() < MAX_BANNED {
                    banned.insert(sender.addr);
                }
            }
            // The receiver is gone once the download ends.
            let _ = self.verdicts.send(sender);
        }
    }

    /// Whether `addr` was found guilty.
    pub(super) fn is_banned(&self, addr: SocketAddr) -> bool {
        self.banned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(&addr)
    }
}

#[cfg(test)]
mod tests;
