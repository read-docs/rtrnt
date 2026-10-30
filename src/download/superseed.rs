//! Super-seeding (BEP 16): when we have the whole torrent and nobody else
//! does, telling every peer all of it lets each one fetch whatever it likes,
//! and many fetch the same pieces from us. Instead a super-seeding worker
//! says it has nothing, and then offers its peer one piece at a time with a
//! Have, the one we have offered the least, so that every piece we upload
//! is a different one until the swarm has them all.
//!
//! A peer is offered its next piece once it holds the last one and another
//! peer has it too, which shows that the peer passes what it gets on, or
//! after `PATIENCE` if nobody else turns up to prove it. Only a torrent we
//! have complete is super-seeded, and only for as long as it stays so.

use super::queue::pick_least;
use rand::Rng;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use tokio::time::Instant;

/// How long a peer that holds its offered piece is waited on to show that
/// another peer has it, before it is offered the next one anyway: it may be
/// the only peer there is.
pub(super) const PATIENCE: Duration = Duration::from_secs(60);
/// How long a new peer is given to tell us which pieces it has before it is
/// offered one, so that we do not offer it a piece it already holds.
pub(super) const HEARING: Duration = Duration::from_secs(3);

/// How often each piece has been offered, to any peer.
pub(super) struct Offers {
    counts: Mutex<Vec<u32>>,
}

impl Offers {
    /// No piece offered yet, of a torrent of `num_pieces`.
    pub(super) fn new(num_pieces: usize) -> Self {
        Self {
            counts: Mutex::new(vec![0; num_pieces]),
        }
    }

    /// Picks the piece to offer a peer that has `peer_has`, and counts the
    /// offer: see `choose_piece`.
    pub(super) fn offer(
        &self,
        peer_has: &[bool],
        have: &[bool],
        availability: &[u32],
        rng: &mut impl Rng,
    ) -> Option<usize> {
        let mut counts = self.counts.lock().unwrap_or_else(PoisonError::into_inner);
        let piece = choose_piece(&counts, availability, peer_has, have, rng)?;
        if let Some(count) = counts.get_mut(piece) {
            *count = count.saturating_add(1);
        }
        Some(piece)
    }
}

/// The piece to offer a peer: one we `have` and it lacks, of those the
/// least offered so far (`offered`), then the least held by the peers
/// connected (`availability`), and of those a random one. `None` if the peer
/// has every piece we do.
pub(super) fn choose_piece(
    offered: &[u32],
    availability: &[u32],
    peer_has: &[bool],
    have: &[bool],
    rng: &mut impl Rng,
) -> Option<usize> {
    let eligible: Vec<bool> = have
        .iter()
        .enumerate()
        .map(|(i, &ours)| ours && !peer_has.get(i).copied().unwrap_or(false))
        .collect();
    let candidates = (0..have.len()).map(|i| {
        let offers = offered.get(i).copied().unwrap_or(0);
        let held = availability.get(i).copied().unwrap_or(0);
        (i, (offers, held))
    });
    pick_least(candidates, &eligible, rng)
}

/// Where one peer stands: the piece it was last offered, and since when it
/// has been seen holding it.
#[derive(Debug, Default)]
pub(super) struct PeerOffer {
    piece: Option<usize>,
    held_since: Option<Instant>,
}

impl PeerOffer {
    /// Whether the peer is due its next piece at `now`: it has been offered
    /// none yet, or it has the last one, and `availability` of that piece
    /// (how many connected peers have it, this one included) shows another
    /// has it too, or `PATIENCE` has passed since it was first seen with it.
    pub(super) fn due(
        &mut self,
        now: Instant,
        peer_has: &[bool],
        availability: impl FnOnce(usize) -> u32,
    ) -> bool {
        let Some(piece) = self.piece else {
            return true;
        };
        if !peer_has.get(piece).copied().unwrap_or(false) {
            return false;
        }
        let since = *self.held_since.get_or_insert(now);
        availability(piece) >= 2 || now.saturating_duration_since(since) >= PATIENCE
    }

    /// Records that the peer was offered `piece`.
    pub(super) fn offered(&mut self, piece: usize) {
        self.piece = Some(piece);
        self.held_since = None;
    }
}

#[cfg(test)]
mod tests;
