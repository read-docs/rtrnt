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
//!
//! A limit can change while running (`RateLimiter::set_rate`). Reservations
//! made before the change are made again at the new rate, and peers waiting
//! for their turn are woken (`RateLimiter::changed`) to do so.

use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
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

    /// Changes the limits to `download` and `upload` KiB per second; 0 is
    /// unlimited.
    pub fn set(&self, download: u64, upload: u64) {
        self.download.set_rate(download.saturating_mul(1024));
        self.upload.set_rate(upload.saturating_mul(1024));
    }

    /// The download and upload limits in KiB per second, 0 for none.
    pub fn kib(&self) -> (u64, u64) {
        (self.download.rate() / 1024, self.upload.rate() / 1024)
    }
}

/// A token bucket shared by every peer, or no limit at all.
#[derive(Debug)]
pub struct RateLimiter {
    bucket: Mutex<Bucket>,
    /// Notified whenever the rate changes.
    changed: Notify,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::unlimited()
    }
}

impl RateLimiter {
    /// A limit of `bytes_per_sec`; 0 is unlimited.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            bucket: Mutex::new(Bucket::new(bytes_per_sec, Instant::now())),
            changed: Notify::new(),
        }
    }

    /// No limit: every byte may move at once.
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Whether there is a limit.
    pub fn is_limited(&self) -> bool {
        self.rate() > 0
    }

    /// The limit in bytes per second, 0 for none.
    pub fn rate(&self) -> u64 {
        self.lock().rate
    }

    /// Changes the limit to `bytes_per_sec`; 0 is unlimited. Debt taken on
    /// at the old rate is forgiven beyond one burst, so it does not hold up
    /// the new one, and every reservation is made again (see `Allowance`).
    pub fn set_rate(&self, bytes_per_sec: u64) {
        self.lock().set_rate(bytes_per_sec, Instant::now());
        self.changed.notify_waiters();
    }

    /// Resolves once the limit changes after this call, so that a peer
    /// waiting for its turn can reserve again at the new rate.
    pub fn changed(&self) -> Notified<'_> {
        self.changed.notified()
    }

    /// A peer's share of this limit. Every peer gets its own.
    pub fn allowance(&self) -> Allowance<'_> {
        Allowance {
            limiter: self,
            reserved: None,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Bucket> {
        self.bucket.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The state of a `RateLimiter`.
#[derive(Debug)]
struct Bucket {
    /// Bytes per second, 0 for no limit.
    rate: u64,
    /// How long the bucket takes to fill from empty.
    burst: Duration,
    /// When the bucket will be full again if nothing more is taken; in the
    /// past while it is full. Each byte taken moves it on by `1 / rate`.
    full_at: Instant,
    /// Bumped whenever the rate changes, so reservations made before can
    /// tell.
    generation: u64,
}

impl Bucket {
    fn new(rate: u64, now: Instant) -> Self {
        let mut bucket = Self {
            rate,
            burst: Duration::ZERO,
            full_at: now,
            generation: 0,
        };
        bucket.burst = bucket.burst_for_rate();
        bucket
    }

    /// `BURST_TIME`, or the time `MIN_BURST` takes if that is longer.
    fn burst_for_rate(&self) -> Duration {
        if self.rate == 0 {
            return Duration::ZERO;
        }
        BURST_TIME.max(self.cost(MIN_BURST))
    }

    fn set_rate(&mut self, rate: u64, now: Instant) {
        self.rate = rate;
        self.burst = self.burst_for_rate();
        self.full_at = self.full_at.min(now + self.burst);
        self.generation += 1;
    }

    /// How long the limit takes to let `bytes` through, rounded up. Only
    /// called while there is a limit.
    fn cost(&self, bytes: u64) -> Duration {
        let nanos = (u128::from(bytes) * 1_000_000_000).div_ceil(u128::from(self.rate.max(1)));
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }

    /// Takes `bytes`, going into debt if there are not enough, and returns
    /// when the debt is paid off: the moment they may move. Reservations
    /// made later are paid off later.
    fn reserve(&mut self, bytes: u64, now: Instant) -> Instant {
        self.charge(bytes, now);
        self.full_at.checked_sub(self.burst).unwrap_or(now).max(now)
    }

    /// Takes `bytes` more at once, without waiting for them.
    fn charge(&mut self, bytes: u64, now: Instant) {
        self.full_at = self.full_at.max(now) + self.cost(bytes);
    }

    /// Gives back `bytes` taken but not used. The bucket never holds more
    /// than it can.
    fn refund(&mut self, bytes: u64, now: Instant) {
        self.full_at = self
            .full_at
            .checked_sub(self.cost(bytes))
            .unwrap_or(now)
            .max(now);
    }
}

/// Bytes an `Allowance` has reserved.
#[derive(Debug, Clone, Copy)]
struct Reservation {
    bytes: u64,
    /// When they may move.
    ready: Instant,
    /// The bucket's generation when they were reserved.
    generation: u64,
}

/// One peer's share of a `RateLimiter` in one direction. It holds at most
/// one reservation at a time, so peers take turns: one that wants more must
/// queue up behind the others again.
///
/// A reservation that is not used is given back when the allowance is
/// released or dropped. One made before the limit changed is dropped
/// instead, as the bucket was reset then, and made again when next asked.
#[derive(Debug)]
pub struct Allowance<'a> {
    limiter: &'a RateLimiter,
    reserved: Option<Reservation>,
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
        let mut bucket = self.limiter.lock();
        let reserved = self
            .reserved
            .take()
            .filter(|reserved| reserved.generation == bucket.generation);
        if bucket.rate == 0 {
            return true;
        }
        let reserved = reserved.unwrap_or_else(|| Reservation {
            bytes,
            ready: bucket.reserve(bytes, now),
            generation: bucket.generation,
        });
        if reserved.ready > now {
            self.reserved = Some(reserved);
            return false;
        }
        if bytes > reserved.bytes {
            bucket.charge(bytes - reserved.bytes, now);
        } else if bytes < reserved.bytes {
            bucket.refund(reserved.bytes - bytes, now);
        }
        true
    }

    /// When the reserved bytes may move, if any are reserved. A reservation
    /// made before the limit changed does not count.
    pub fn ready_at(&self) -> Option<Instant> {
        let reserved = self.reserved?;
        (reserved.generation == self.limiter.lock().generation).then_some(reserved.ready)
    }

    /// Whether bytes are reserved that may not move yet.
    pub fn is_waiting(&self, now: Instant) -> bool {
        self.ready_at().is_some_and(|ready| ready > now)
    }

    /// Gives back the reservation, if any, for when there is nothing to send
    /// any more (say, the peer choked us).
    pub fn release(&mut self) {
        let Some(reserved) = self.reserved.take() else {
            return;
        };
        let mut bucket = self.limiter.lock();
        if reserved.generation == bucket.generation {
            bucket.refund(reserved.bytes, Instant::now());
        }
    }
}

impl Drop for Allowance<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod tests;
