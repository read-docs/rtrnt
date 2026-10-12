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
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    fn candidate(key: PeerKey, rate: u64, interested: bool) -> Candidate {
        Candidate {
            key,
            rate,
            interested,
        }
    }

    #[test]
    fn unchokes_the_four_fastest_interested_peers() {
        let mut rng = StdRng::seed_from_u64(1);
        let candidates = [
            candidate(0, 10, true),
            candidate(1, 500, false),
            candidate(2, 40, true),
            candidate(3, 30, true),
            candidate(4, 20, true),
            candidate(5, 50, true),
        ];
        let choice = choose(&candidates, None, true, &mut rng);
        assert_eq!(choice.regular, [5, 2, 3, 4]);
        // The only interested peer left gets the optimistic slot.
        assert_eq!(choice.optimistic, Some(0));
        assert!(!choice.is_unchoked(1), "not interested");
    }

    #[test]
    fn fewer_interested_peers_than_slots_are_all_unchoked() {
        let mut rng = StdRng::seed_from_u64(1);
        let candidates = [candidate(7, 0, true), candidate(8, 0, false)];
        let choice = choose(&candidates, Some(8), false, &mut rng);
        assert_eq!(choice.regular, [7]);
        assert_eq!(choice.optimistic, None);
        assert_eq!(choose(&[], Some(1), true, &mut rng), Choice::default());
    }

    #[test]
    fn equal_rates_favor_the_peer_connected_longest() {
        let mut rng = StdRng::seed_from_u64(1);
        let candidates: Vec<Candidate> = (0..6).map(|key| candidate(key, 0, true)).collect();
        for _ in 0..10 {
            let choice = choose(&candidates, Some(5), false, &mut rng);
            assert_eq!(choice.regular, [0, 1, 2, 3]);
            assert_eq!(choice.optimistic, Some(5));
        }
    }

    #[test]
    fn optimistic_slot_is_kept_until_rotated() {
        let mut rng = StdRng::seed_from_u64(1);
        let candidates: Vec<Candidate> =
            (0..8).map(|key| candidate(key, 100 - key, true)).collect();
        // Peers 4 to 7 compete for the optimistic slot.
        let first = choose(&candidates, None, true, &mut rng)
            .optimistic
            .unwrap();
        assert!((4..8).contains(&first));
        assert_eq!(
            choose(&candidates, Some(first), false, &mut rng).optimistic,
            Some(first)
        );
        // Rotating always moves the slot on while there is someone else.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..100 {
            let next = choose(&candidates, Some(first), true, &mut rng)
                .optimistic
                .unwrap();
            assert_ne!(next, first);
            seen.insert(next);
        }
        assert_eq!(seen.len(), 3, "picked {seen:?}");
        // With nobody else, the old peer keeps it.
        let pair = [candidates[0], candidates[7]];
        assert_eq!(choose(&pair, Some(7), true, &mut rng).optimistic, None);
        let five = &candidates[..5];
        assert_eq!(choose(five, Some(4), true, &mut rng).optimistic, Some(4));
    }

    #[test]
    fn optimistic_slot_moves_when_its_peer_leaves_loses_interest_or_speeds_up() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut candidates: Vec<Candidate> = (0..6).map(|key| candidate(key, 10, true)).collect();
        // Gone.
        let choice = choose(&candidates, Some(9), false, &mut rng);
        assert!(matches!(choice.optimistic, Some(4 | 5)));
        // No longer interested: the slot goes to the other one left.
        candidates[5].interested = false;
        assert_eq!(
            choose(&candidates, Some(5), false, &mut rng).optimistic,
            Some(4)
        );
        // Fast enough for a regular slot, which pushes peer 3 out.
        candidates[5].interested = true;
        candidates[5].rate = 1000;
        let choice = choose(&candidates, Some(5), false, &mut rng);
        assert_eq!(choice.regular, [5, 0, 1, 2]);
        assert!(matches!(choice.optimistic, Some(3 | 4)));
    }

    #[test]
    fn rates_are_bytes_per_second() {
        let second = Duration::from_secs(1);
        assert_eq!(per_second(1000, second), 1000);
        assert_eq!(per_second(1000, 10 * second), 100);
        assert_eq!(per_second(1000, Duration::ZERO), 0);
        assert_eq!(per_second(u64::MAX, Duration::from_millis(1)), u64::MAX);
    }

    #[test]
    fn deals_slots_by_download_rate_then_by_upload_rate_when_seeding() {
        let choker = Choker::default();
        let mut slots: Vec<ChokerSlot<'_>> = (0..6).map(|_| choker.register()).collect();
        let start = Instant::now();
        choker.measure(start, false);
        for (i, slot) in slots.iter().enumerate() {
            slot.set_interested(true);
            // Peer 5 sends us the most, peer 0 the least, and the other way
            // round for what we send them.
            slot.downloaded(1000 * i as u64);
            slot.uploaded(1000 * (5 - i) as u64);
        }
        let unchoked = |slots: &mut [ChokerSlot<'_>]| -> Vec<usize> {
            (0..slots.len())
                .filter(|&i| slots[i].is_unchoked())
                .collect()
        };

        let optimistic = |choker: &Choker| choker.lock().optimistic.unwrap();

        // Peers 2 to 5 sent us the most; 0 or 1 is unchoked optimistically.
        let round = start + ROUND;
        choker.measure(round, false);
        choker.rechoke(round);
        let first = optimistic(&choker);
        assert!(first < 2);
        let mut expected = vec![first as usize, 2, 3, 4, 5];
        expected.sort();
        assert_eq!(unchoked(&mut slots), expected);

        // Seeding: what we send counts, so peers 0 to 3 win. The optimistic
        // peer earned a regular slot, so 4 or 5 gets the optimistic one.
        for (i, slot) in slots.iter().enumerate() {
            slot.uploaded(1000 * (5 - i) as u64);
        }
        let later = round + ROUND;
        choker.measure(later, true);
        choker.rechoke(later);
        let second = optimistic(&choker);
        assert!(second >= 4);
        let mut expected = vec![0, 1, 2, 3, second as usize];
        expected.sort();
        assert_eq!(unchoked(&mut slots), expected);

        // Kept until its time is up, then rotated to the other one.
        choker.rechoke(later + OPTIMISTIC_PERIOD / 2);
        assert_eq!(optimistic(&choker), second);
        let rotated = later + OPTIMISTIC_PERIOD;
        choker.rechoke(rotated);
        assert_eq!(optimistic(&choker), 9 - second);
        assert!(!slots[second as usize].is_unchoked());

        // A peer that leaves frees its slot for the one left waiting.
        slots.remove(0);
        choker.rechoke(rotated);
        assert!(slots.iter_mut().all(|slot| slot.is_unchoked()));
    }
}
