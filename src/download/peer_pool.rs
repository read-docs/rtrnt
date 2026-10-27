//! Every peer we know of, and when each may be tried (again).

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

/// A peer that fails this many times in a row is given up on.
const MAX_FAILURES: u32 = 3;
/// Wait before the first retry of a failed peer; doubled after each failure.
const RETRY_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(crate) struct PeerPool {
    /// Peers that can be tried now, in the order they were learned.
    ready: VecDeque<SocketAddr>,
    /// Failed peers waiting out their backoff.
    retrying: Vec<(Instant, SocketAddr)>,
    /// Every peer ever added, so a tracker repeating it does not duplicate it.
    known: HashMap<SocketAddr, PeerState>,
}

#[derive(Default)]
struct PeerState {
    /// Failures since the peer last delivered a piece.
    failures: u32,
    banned: bool,
}

impl PeerPool {
    /// Adds peers not seen before. Known peers, including connected, retrying
    /// and banned ones, are left alone.
    pub(crate) fn add(&mut self, peers: impl IntoIterator<Item = SocketAddr>) {
        for addr in peers {
            if let Entry::Vacant(entry) = self.known.entry(addr) {
                entry.insert(PeerState::default());
                self.ready.push_back(addr);
            }
        }
    }

    /// Takes the next peer to connect to, if any is ready at `now`.
    pub(crate) fn next(&mut self, now: Instant) -> Option<SocketAddr> {
        let (mut due, waiting): (Vec<_>, Vec<_>) =
            self.retrying.drain(..).partition(|&(at, _)| at <= now);
        self.retrying = waiting;
        due.sort();
        self.ready.extend(due.into_iter().map(|(_, addr)| addr));
        self.ready.pop_front()
    }

    /// How many peers wait to be tried for the first time or after a
    /// backoff that is over, not counting those still backing off.
    /// Every peer ever added.
    pub(crate) fn len(&self) -> usize {
        self.known.len()
    }

    pub(crate) fn ready_len(&self) -> usize {
        self.ready.len()
    }

    /// When the next peer in backoff becomes ready.
    pub(crate) fn next_retry(&self) -> Option<Instant> {
        self.retrying.iter().map(|&(at, _)| at).min()
    }

    /// Records that a connection to `addr` ended with an error. `useful`
    /// peers (those that delivered a piece) start counting failures afresh.
    pub(crate) fn failed(&mut self, addr: SocketAddr, useful: bool, now: Instant) {
        let state = self.known.entry(addr).or_default();
        if state.banned {
            return;
        }
        state.failures = if useful { 1 } else { state.failures + 1 };
        if state.failures >= MAX_FAILURES {
            return;
        }
        let backoff = RETRY_BACKOFF * 2u32.pow(state.failures - 1);
        self.retrying.push((now + backoff, addr));
    }

    /// Puts a peer handed out by `next` back at the end of the queue, as if
    /// it had not been tried, unless it is banned.
    pub(crate) fn requeue(&mut self, addr: SocketAddr) {
        let banned = self.known.entry(addr).or_default().banned;
        if !banned && !self.ready.contains(&addr) {
            self.ready.push_back(addr);
        }
    }

    /// Never connects to `addr` again.
    pub(crate) fn ban(&mut self, addr: SocketAddr) {
        self.known.entry(addr).or_default().banned = true;
    }
}

#[cfg(test)]
mod tests;
