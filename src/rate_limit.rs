//! Upload and download rate limits: one token bucket per direction, shared
//! by every peer of a session.
//!
//! The bucket fills at the limit and holds a small burst (`BURST_TIME`
//! worth, and at least `MIN_BURST` bytes). Bytes are taken from it before
//! they move: a block request before it is sent, so the data it asks for is
//! paid for in advance and arrives no faster than the limit, and a block we
//! upload before it goes out. Only block payloads count; message headers
//! and other protocol traffic do not.
//!
//! Peers take turns. Each one holds at most one reservation at a time (see
//! `Allowance`), and reservations are served in the order they were made,
//! going into debt when the bucket is empty. A peer that wants to send more
//! has to reserve again behind everyone already waiting, so a fast peer
//! cannot starve the others: each gets an equal share, unless it needs less.
//!
//! The bucket is kept as the time it will be full again (the "theoretical
//! arrival time" of the generic cell rate algorithm), so it needs no timer:
//! each reservation just works out when its bytes are paid for.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::time::Instant;

/// How much of the limit can go out at once after a pause.
pub const BURST_TIME: Duration = Duration::from_millis(250);
/// The smallest burst, in bytes: one block, so that a slow limit still lets
/// a block through at once after a pause.
pub const MIN_BURST: u64 = 16 * 1024;

/// A session's rate limits, one per direction.
#[derive(Debug, Default)]
pub struct RateLimits {
    /// Block data we request from peers.
    pub download: RateLimiter,
    /// Block data we send to peers.
    pub upload: RateLimiter,
}

impl RateLimits {
    /// Limits of `download` and `upload` KiB per second; 0 is unlimited.
    pub fn from_kib(download: u64, upload: u64) -> Self {
        Self {
            download: RateLimiter::new(download.saturating_mul(1024)),
            upload: RateLimiter::new(upload.saturating_mul(1024)),
        }
    }
}

/// A token bucket shared by every peer, or no limit at all.
#[derive(Debug, Default)]
pub struct RateLimiter {
    /// `None` when unlimited.
    bucket: Option<Bucket>,
}

impl RateLimiter {
    /// A limit of `bytes_per_sec`; 0 is unlimited.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bucket: (bytes_per_sec > 0).then(|| Bucket::new(bytes_per_sec)),
        }
    }

    /// No limit: every byte may move at once.
    pub fn unlimited() -> Self {
        Self::default()
    }

    /// Whether there is a limit.
    pub fn is_limited(&self) -> bool {
        self.bucket.is_some()
    }

    /// A peer's share of this limit. Every peer gets its own.
    pub fn allowance(&self) -> Allowance<'_> {
        Allowance {
            bucket: self.bucket.as_ref(),
            reserved: None,
        }
    }
}

/// The bucket of a limited `RateLimiter`.
#[derive(Debug)]
struct Bucket {
    /// Bytes per second, more than 0.
    rate: u64,
    /// How long the bucket takes to fill from empty.
    burst: Duration,
    /// When the bucket will be full again if nothing more is taken; in the
    /// past while it is full. Each byte taken moves it on by `1 / rate`.
    full_at: Mutex<Instant>,
}

impl Bucket {
    fn new(rate: u64) -> Self {
        let mut bucket = Self {
            rate,
            burst: Duration::ZERO,
            full_at: Mutex::new(Instant::now()),
        };
        bucket.burst = BURST_TIME.max(bucket.cost(MIN_BURST));
        bucket
    }

    fn lock(&self) -> MutexGuard<'_, Instant> {
        self.full_at.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How long the limit takes to let `bytes` through, rounded up.
    fn cost(&self, bytes: u64) -> Duration {
        let nanos = (u128::from(bytes) * 1_000_000_000).div_ceil(u128::from(self.rate));
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Takes `bytes`, going into debt if there are not enough, and returns
    /// when the debt is paid off: the moment they may move. Reservations
    /// made later are paid off later.
    fn reserve(&self, bytes: u64, now: Instant) -> Instant {
        let mut full_at = self.lock();
        *full_at = (*full_at).max(now) + self.cost(bytes);
        full_at.checked_sub(self.burst).unwrap_or(now).max(now)
    }

    /// Takes `bytes` more at once, without waiting for them.
    fn charge(&self, bytes: u64, now: Instant) {
        let mut full_at = self.lock();
        *full_at = (*full_at).max(now) + self.cost(bytes);
    }

    /// Gives back `bytes` taken but not used. The bucket never holds more
    /// than it can.
    fn refund(&self, bytes: u64, now: Instant) {
        let mut full_at = self.lock();
        *full_at = full_at
            .checked_sub(self.cost(bytes))
            .unwrap_or(now)
            .max(now);
    }
}

/// One peer's share of a `RateLimiter` in one direction. It holds at most
/// one reservation at a time, so peers take turns: one that wants more must
/// queue up behind the others again.
///
/// A reservation that is not used is given back when the allowance is
/// released or dropped.
#[derive(Debug)]
pub struct Allowance<'a> {
    /// `None` when unlimited.
    bucket: Option<&'a Bucket>,
    /// Bytes reserved, and when they may move.
    reserved: Option<(u64, Instant)>,
}

impl Allowance<'_> {
    /// Whether `bytes` may move now; if so, they are taken from the limit
    /// and the caller must send (or request) them. If not, they are
    /// reserved, and `ready_at` says when to ask again. Always true without
    /// a limit.
    ///
    /// A reservation made for a different number of bytes (say, a request
    /// cancelled in the meantime, replaced by a short last block) still
    /// counts: the difference is charged or given back.
    pub fn try_take(&mut self, bytes: u64, now: Instant) -> bool {
        let Some(bucket) = self.bucket else {
            return true;
        };
        let (reserved, ready) = match self.reserved.take() {
            Some(reservation) => reservation,
            None => (bytes, bucket.reserve(bytes, now)),
        };
        if ready > now {
            self.reserved = Some((reserved, ready));
            return false;
        }
        if bytes > reserved {
            bucket.charge(bytes - reserved, now);
        } else if bytes < reserved {
            bucket.refund(reserved - bytes, now);
        }
        true
    }

    /// When the reserved bytes may move, if any are reserved.
    pub fn ready_at(&self) -> Option<Instant> {
        self.reserved.map(|(_, ready)| ready)
    }

    /// Whether bytes are reserved that may not move yet.
    pub fn is_waiting(&self, now: Instant) -> bool {
        self.ready_at().is_some_and(|ready| ready > now)
    }

    /// Gives back the reservation, if any, for when there is nothing to send
    /// any more (say, the peer choked us).
    pub fn release(&mut self) {
        if let (Some(bucket), Some((bytes, _))) = (self.bucket, self.reserved.take()) {
            bucket.refund(bytes, Instant::now());
        }
    }
}

impl Drop for Allowance<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const BLOCK: u64 = 16 * 1024;
    /// 100 KiB/s.
    const RATE: u64 = 100 * 1024;

    /// Takes `bytes` as soon as `allowance` allows, sleeping as needed.
    async fn take(allowance: &mut Allowance<'_>, bytes: u64) {
        while !allowance.try_take(bytes, Instant::now()) {
            tokio::time::sleep_until(allowance.ready_at().unwrap()).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unlimited_never_waits() {
        let limiter = RateLimiter::unlimited();
        assert!(!limiter.is_limited());
        assert!(!RateLimiter::new(0).is_limited());
        let mut allowance = limiter.allowance();
        let now = Instant::now();
        for _ in 0..10_000 {
            assert!(allowance.try_take(BLOCK, now));
        }
        assert_eq!(allowance.ready_at(), None);
        assert!(!allowance.is_waiting(now));
    }

    #[test]
    fn limits_are_in_kib() {
        let limits = RateLimits::from_kib(0, 64);
        assert!(!limits.download.is_limited());
        let bucket = limits.upload.bucket.as_ref().unwrap();
        assert_eq!(bucket.rate, 64 * 1024);
        // Absurd limits saturate instead of overflowing.
        let limits = RateLimits::from_kib(u64::MAX, 1);
        assert_eq!(limits.download.bucket.as_ref().unwrap().rate, u64::MAX);
    }

    #[test]
    fn burst_is_a_quarter_second_and_at_least_a_block() {
        // 100 KiB/s: a quarter second is 25 KiB, more than a block.
        assert_eq!(Bucket::new(RATE).burst, BURST_TIME);
        // 16 KiB/s: a block is a whole second.
        assert_eq!(Bucket::new(BLOCK).burst, Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn burst_goes_at_once_then_paced() {
        let limiter = RateLimiter::new(RATE);
        let mut allowance = limiter.allowance();
        let start = Instant::now();
        // 25 KiB of burst: one block, and the second goes into debt.
        assert!(allowance.try_take(BLOCK, start));
        assert!(!allowance.try_take(BLOCK, start));
        assert!(allowance.is_waiting(start));
        // 32 KiB taken with 25 KiB in the bucket: 7 KiB of debt at 100 KiB/s.
        let ready = allowance.ready_at().unwrap();
        assert_eq!(ready - start, Duration::from_millis(70));
        // Asking again early changes nothing.
        assert!(!allowance.try_take(BLOCK, start + Duration::from_millis(69)));
        assert_eq!(allowance.ready_at(), Some(ready));
        assert!(allowance.try_take(BLOCK, ready));
        assert_eq!(allowance.ready_at(), None);
    }

    #[tokio::test(start_paused = true)]
    async fn sustained_rate_matches_the_limit() {
        let limiter = RateLimiter::new(RATE);
        let mut allowance = limiter.allowance();
        let start = Instant::now();
        // 1000 KiB at 100 KiB/s, less the 25 KiB burst: 9.75 s.
        let blocks = 1000 / 16;
        for _ in 0..blocks {
            take(&mut allowance, BLOCK).await;
        }
        let elapsed = start.elapsed();
        let expected = Duration::from_secs_f64((blocks * BLOCK) as f64 / RATE as f64) - BURST_TIME;
        assert!(
            elapsed.abs_diff(expected) < Duration::from_millis(5),
            "{elapsed:?} vs {expected:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_only_refills_the_burst() {
        let limiter = RateLimiter::new(RATE);
        let mut allowance = limiter.allowance();
        tokio::time::sleep(Duration::from_secs(60)).await;
        let now = Instant::now();
        // A minute idle still allows only the 25 KiB burst at once.
        assert!(allowance.try_take(BLOCK, now));
        assert!(!allowance.try_take(BLOCK, now));
    }

    #[tokio::test(start_paused = true)]
    async fn peers_take_turns() {
        let limiter = RateLimiter::new(RATE);
        let mut a = limiter.allowance();
        let mut b = limiter.allowance();
        let now = Instant::now();
        // Use up the burst, then both wait, `a` first.
        assert!(a.try_take(BLOCK, now));
        assert!(!a.try_take(BLOCK, now));
        assert!(!b.try_take(BLOCK, now));
        let (a_ready, b_ready) = (a.ready_at().unwrap(), b.ready_at().unwrap());
        assert!(a_ready < b_ready);
        // `a` goes, and its next block queues up behind `b`'s.
        assert!(a.try_take(BLOCK, a_ready));
        assert!(!a.try_take(BLOCK, a_ready));
        assert!(a.ready_at().unwrap() > b_ready);
    }

    #[tokio::test(start_paused = true)]
    async fn a_greedy_peer_does_not_starve_the_others() {
        let limiter = Arc::new(RateLimiter::new(RATE));
        let deadline = Instant::now() + Duration::from_secs(10);
        // Three peers that each want everything they can get.
        let tasks: Vec<_> = (0..3)
            .map(|_| {
                let limiter = Arc::clone(&limiter);
                tokio::spawn(async move {
                    let mut allowance = limiter.allowance();
                    let mut bytes = 0;
                    while Instant::now() < deadline {
                        take(&mut allowance, BLOCK).await;
                        bytes += BLOCK;
                    }
                    bytes
                })
            })
            .collect();
        let mut shares = Vec::new();
        for task in tasks {
            shares.push(task.await.unwrap());
        }
        let total: u64 = shares.iter().sum();
        // About 10 s at the limit, plus the burst and the block each peer
        // reserved before the deadline and got after it...
        assert!(total <= 10 * RATE + RATE / 4 + 3 * BLOCK, "{total}");
        assert!(total >= 10 * RATE - 2 * BLOCK, "{total}");
        // ...shared evenly, give or take a block.
        let (min, max) = (shares.iter().min(), shares.iter().max());
        assert!(max.unwrap() - min.unwrap() <= BLOCK, "{shares:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn released_reservations_are_given_back() {
        let limiter = RateLimiter::new(RATE);
        let mut a = limiter.allowance();
        let now = Instant::now();
        assert!(a.try_take(BLOCK, now));
        assert!(!a.try_take(BLOCK, now));
        a.release();
        assert_eq!(a.ready_at(), None);
        // `b` gets the slot `a` gave up rather than queuing behind it.
        let mut b = limiter.allowance();
        assert!(!b.try_take(BLOCK, now));
        assert_eq!(b.ready_at().unwrap() - now, Duration::from_millis(70));
        // Dropping an allowance releases it too.
        drop(b);
        let mut c = limiter.allowance();
        assert!(!c.try_take(BLOCK, now));
        assert_eq!(c.ready_at().unwrap() - now, Duration::from_millis(70));
    }

    #[tokio::test(start_paused = true)]
    async fn a_reservation_covers_a_different_size() {
        let limiter = RateLimiter::new(RATE);
        let mut allowance = limiter.allowance();
        let mut other = limiter.allowance();
        let now = Instant::now();
        assert!(allowance.try_take(BLOCK, now));
        assert!(!allowance.try_take(BLOCK, now));
        let ready = allowance.ready_at().unwrap();
        // The block it was reserved for was cancelled; a short one is sent
        // instead, and the rest is given back.
        assert!(allowance.try_take(BLOCK / 4, ready));
        // Of the 25 KiB burst, 16 + 4 KiB are gone and 7 KiB came back by
        // `ready`, which leaves 12 KiB: a block needs 4 KiB more, 40 ms.
        assert!(!other.try_take(BLOCK, ready));
        assert_eq!(other.ready_at().unwrap() - ready, Duration::from_millis(40));
    }
}
