//! The choking algorithm: decides which peers may download from us.
//!
//! Every `ROUND` the interested peers are ranked by the rate they sent us
//! data at over the round, or, once we are seeding, the rate we sent them
//! data at, and the fastest `REGULAR_SLOTS` are unchoked (tit-for-tat). One
//! more, picked at random among the other interested peers, is unchoked
//! optimistically and keeps that slot for `OPTIMISTIC_PERIOD`: that is how a
//! new peer gets a chance to show what it can do, and how we find peers
//! faster than the ones we have. Everyone else is choked.
//!
//! Between rounds the slots are dealt again, with the rates of the last
//! round, whenever a peer leaves or its interest changes, so a slot never
//! sits empty for long and a new peer can be served at once.

use rand::Rng;
use rand::seq::IndexedRandom;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::time::Instant;
use tracing::debug;

/// Peers unchoked for their rate.
pub(crate) const REGULAR_SLOTS: usize = 4;
/// How often the rates are measured and the slots dealt again.
pub(crate) const ROUND: Duration = Duration::from_secs(10);
/// How long a peer keeps the optimistic slot.
pub(crate) const OPTIMISTIC_PERIOD: Duration = Duration::from_secs(30);

/// Identifies a connected peer to the choker. Keys grow in connection order.
pub(crate) type PeerKey = u64;

/// A connected peer, as the choker sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) key: PeerKey,
    /// Bytes per second over the last round.
    pub(crate) rate: u64,
    /// Whether the peer wants to download from us.
    pub(crate) interested: bool,
}

/// The peers to unchoke; everyone else is choked.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Choice {
    /// Fastest first.
    pub(crate) regular: Vec<PeerKey>,
    pub(crate) optimistic: Option<PeerKey>,
}

impl Choice {
    pub(crate) fn is_unchoked(&self, key: PeerKey) -> bool {
        self.regular.contains(&key) || self.optimistic == Some(key)
    }
}

/// Deals the unchoke slots among the interested `candidates`: the
/// `REGULAR_SLOTS` fastest get a regular one, and ties go to the peer
/// listed first (connected longest), so equal rates do not shuffle the
/// slots every time.
///
/// The `optimistic` peer keeps its slot unless `rotate` is set, it is gone
/// or no longer interested, or it earned a regular slot. Then the slot goes
/// to another interested peer without a regular one, picked at random (the
/// old one only if nobody else is left).
pub(crate) fn choose(
    candidates: &[Candidate],
    optimistic: Option<PeerKey>,
    rotate: bool,
    rng: &mut impl Rng,
) -> Choice {
    let mut interested: Vec<&Candidate> = candidates.iter().filter(|c| c.interested).collect();
    // Stable, so ties keep the order of `candidates`.
    interested.sort_by_key(|c| Reverse(c.rate));
    let split = interested.len().min(REGULAR_SLOTS);
    let regular = interested[..split].iter().map(|c| c.key).collect();
    let others: Vec<PeerKey> = interested[split..].iter().map(|c| c.key).collect();

    let current = optimistic.filter(|key| others.contains(key));
    let optimistic = match current {
        Some(key) if !rotate => Some(key),
        _ => {
            let fresh: Vec<PeerKey> = others
                .iter()
                .copied()
                .filter(|&key| Some(key) != current)
                .collect();
            fresh.choose(rng).copied().or(current)
        }
    };
    Choice {
        regular,
        optimistic,
    }
}

/// Shared between the peer workers, which report on their peers through a
/// `ChokerSlot`, and `run`, which deals the unchoke slots.
#[derive(Default)]
pub(crate) struct Choker {
    state: Mutex<State>,
    /// Signalled when a peer leaves or its interest changes.
    changed: Notify,
}

#[derive(Default)]
struct State {
    next_key: PeerKey,
    peers: BTreeMap<PeerKey, Entry>,
    optimistic: Option<PeerKey>,
    /// When the optimistic peer got its slot.
    optimistic_since: Option<Instant>,
    /// When the rates were last measured.
    measured: Option<Instant>,
}

struct Entry {
    stats: Arc<PeerStats>,
    unchoke: watch::Sender<bool>,
    /// The `stats` byte counts (downloaded, uploaded) at the last
    /// measurement.
    counted: (u64, u64),
    /// Bytes per second over the last round.
    rate: u64,
}

/// What a worker reports about its peer.
#[derive(Default)]
struct PeerStats {
    /// Block bytes received from the peer.
    downloaded: AtomicU64,
    /// Block bytes sent to the peer.
    uploaded: AtomicU64,
    interested: AtomicBool,
}

impl Choker {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a newly connected peer, choked, until the returned slot is
    /// dropped.
    pub(crate) fn register(&self) -> ChokerSlot<'_> {
        let mut state = self.lock();
        let key = state.next_key;
        state.next_key += 1;
        let stats = Arc::new(PeerStats::default());
        let (unchoke, unchoked) = watch::channel(false);
        state.peers.insert(
            key,
            Entry {
                stats: Arc::clone(&stats),
                unchoke,
                counted: (0, 0),
                rate: 0,
            },
        );
        ChokerSlot {
            choker: self,
            key,
            stats,
            unchoked,
        }
    }

    /// Deals the unchoke slots every `ROUND`, ranking peers by download
    /// rate, or by upload rate while `seeding` returns true, and in between
    /// whenever a peer leaves or its interest changes. Never returns.
    pub(crate) async fn run(&self, seeding: impl Fn() -> bool) {
        let mut next_round = Instant::now();
        loop {
            tokio::select! {
                () = tokio::time::sleep_until(next_round) => {
                    let now = Instant::now();
                    self.measure(now, seeding());
                    next_round = now + ROUND;
                }
                () = self.changed.notified() => {}
            }
            self.rechoke(Instant::now());
        }
    }

    /// Works out each peer's rate since the last measurement: the bytes it
    /// sent us, or while `seeding` the bytes we sent it. A peer that
    /// connected meanwhile is measured over the whole time, which
    /// understates it for one round.
    fn measure(&self, now: Instant, seeding: bool) {
        let mut state = self.lock();
        let elapsed = state
            .measured
            .map_or(Duration::ZERO, |at| now.saturating_duration_since(at));
        state.measured = Some(now);
        for entry in state.peers.values_mut() {
            let counts = (
                entry.stats.downloaded.load(Ordering::Relaxed),
                entry.stats.uploaded.load(Ordering::Relaxed),
            );
            let bytes = if seeding {
                counts.1.saturating_sub(entry.counted.1)
            } else {
                counts.0.saturating_sub(entry.counted.0)
            };
            entry.counted = counts;
            entry.rate = per_second(bytes, elapsed);
        }
    }

    /// Deals the slots with the rates of the last measurement, rotating the
    /// optimistic one once it has been held for `OPTIMISTIC_PERIOD`, and
    /// tells each worker whether its peer is unchoked.
    fn rechoke(&self, now: Instant) {
        let mut state = self.lock();
        let rotate = state
            .optimistic_since
            .is_none_or(|since| now.saturating_duration_since(since) >= OPTIMISTIC_PERIOD);
        let candidates: Vec<Candidate> = state
            .peers
            .iter()
            .map(|(&key, entry)| Candidate {
                key,
                rate: entry.rate,
                interested: entry.stats.interested.load(Ordering::Relaxed),
            })
            .collect();
        let choice = choose(&candidates, state.optimistic, rotate, &mut rand::rng());
        if rotate || choice.optimistic != state.optimistic {
            state.optimistic_since = choice.optimistic.map(|_| now);
        }
        state.optimistic = choice.optimistic;

        let mut changes = 0;
        for (&key, entry) in &state.peers {
            let unchoked = choice.is_unchoked(key);
            entry.unchoke.send_if_modified(|current| {
                let changed = *current != unchoked;
                *current = unchoked;
                changes += usize::from(changed);
                changed
            });
        }
        if changes > 0 {
            debug!(regular = ?choice.regular, optimistic = ?choice.optimistic, "unchoke slots dealt");
        }
    }
}

/// `bytes` over `elapsed`, per second.
fn per_second(bytes: u64, elapsed: Duration) -> u64 {
    let millis = elapsed.as_millis();
    if millis == 0 {
        return 0;
    }
    u64::try_from(u128::from(bytes) * 1000 / millis).unwrap_or(u64::MAX)
}

/// A worker's line to the choker, held while its peer is connected.
pub(crate) struct ChokerSlot<'a> {
    choker: &'a Choker,
    key: PeerKey,
    stats: Arc<PeerStats>,
    unchoked: watch::Receiver<bool>,
}

impl ChokerSlot<'_> {
    /// Counts block bytes received from the peer.
    pub(crate) fn downloaded(&self, bytes: u64) {
        self.stats.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Counts block bytes sent to the peer.
    pub(crate) fn uploaded(&self, bytes: u64) {
        self.stats.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Records whether the peer wants to download from us, dealing the slots
    /// again if that changed.
    pub(crate) fn set_interested(&self, interested: bool) {
        if self.stats.interested.swap(interested, Ordering::Relaxed) != interested {
            self.choker.changed.notify_one();
        }
    }

    /// Whether the peer may download from us, as last dealt.
    pub(crate) fn is_unchoked(&mut self) -> bool {
        *self.unchoked.borrow_and_update()
    }

    /// Resolves once the peer is choked or unchoked after the last
    /// `is_unchoked`. Cancel-safe.
    pub(crate) async fn changed(&mut self) {
        if self.unchoked.changed().await.is_err() {
            // The choker is gone, so nothing changes any more.
            std::future::pending().await
        }
    }
}

impl Drop for ChokerSlot<'_> {
    fn drop(&mut self) {
        self.choker.lock().peers.remove(&self.key);
        self.choker.changed.notify_one();
    }
}

#[cfg(test)]
mod tests;
