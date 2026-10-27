use super::*;
use crate::download::sleep_until_some;
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
    assert_eq!(limits.upload.rate(), 64 * 1024);
    assert_eq!(limits.kib(), (0, 64));
    limits.set(128, 0);
    assert_eq!(limits.download.rate(), 128 * 1024);
    assert!(!limits.upload.is_limited());
    assert_eq!(limits.kib(), (128, 0));
    // Absurd limits saturate instead of overflowing.
    let limits = RateLimits::from_kib(u64::MAX, 1);
    assert_eq!(limits.download.rate(), u64::MAX);
}

#[test]
fn burst_is_a_quarter_second_and_at_least_a_block() {
    let now = Instant::now();
    // 100 KiB/s: a quarter second is 25 KiB, more than a block.
    assert_eq!(Bucket::new(RATE, now).burst, BURST_TIME);
    // 16 KiB/s: a block is a whole second.
    assert_eq!(Bucket::new(BLOCK, now).burst, Duration::from_secs(1));
    // A new rate gets its own burst.
    let limiter = RateLimiter::new(RATE);
    limiter.set_rate(BLOCK);
    assert_eq!(limiter.lock().burst, Duration::from_secs(1));
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

#[tokio::test(start_paused = true)]
async fn a_new_rate_changes_the_next_wait() {
    let limiter = RateLimiter::new(RATE);
    let now = Instant::now();
    // At 100 KiB/s, `a` takes the burst and reserves a block more: the
    // bucket is 320 ms from full.
    let mut a = limiter.allowance();
    assert!(a.try_take(BLOCK, now));
    assert!(!a.try_take(BLOCK, now));
    // At 16 KiB/s the burst is a second, of which 320 ms are used: the
    // 680 ms left are not a block, so the next one waits 320 ms (at the old
    // rate it would have been 230 ms).
    limiter.set_rate(BLOCK);
    let mut b = limiter.allowance();
    assert!(!b.try_take(BLOCK, now));
    assert_eq!(b.ready_at().unwrap() - now, Duration::from_millis(320));
    // At 1000 KiB/s the debt of 1.32 s is cut to the new burst, a quarter
    // second, so it does not stall the new rate: a block waits 16 ms.
    limiter.set_rate(10 * RATE);
    let mut c = limiter.allowance();
    assert!(!c.try_take(BLOCK, now));
    assert_eq!(c.ready_at().unwrap() - now, Duration::from_millis(16));
}

#[tokio::test(start_paused = true)]
async fn a_reservation_from_before_a_change_is_made_again() {
    let limiter = RateLimiter::new(BLOCK);
    let mut allowance = limiter.allowance();
    let start = Instant::now();
    // At 16 KiB/s, the second block waits a second.
    assert!(allowance.try_take(BLOCK, start));
    assert!(!allowance.try_take(BLOCK, start));
    assert_eq!(allowance.ready_at(), Some(start + Duration::from_secs(1)));
    // Ten times as fast, the old reservation no longer counts...
    limiter.set_rate(10 * BLOCK);
    assert_eq!(allowance.ready_at(), None);
    assert!(!allowance.is_waiting(start));
    // ...and the block is reserved again: behind the debt, cut to one
    // burst, it waits 100 ms.
    assert!(!allowance.try_take(BLOCK, start));
    assert_eq!(
        allowance.ready_at(),
        Some(start + Duration::from_millis(100))
    );
    // Slower again, the reservation is made again at the slow rate, with
    // the 350 ms of debt still there.
    limiter.set_rate(BLOCK);
    assert!(!allowance.try_take(BLOCK, start));
    assert_eq!(
        allowance.ready_at(),
        Some(start + Duration::from_millis(350))
    );
}

#[tokio::test(start_paused = true)]
async fn limited_unlimited_and_limited_again() {
    let limiter = RateLimiter::new(RATE);
    let mut allowance = limiter.allowance();
    let now = Instant::now();
    assert!(allowance.try_take(BLOCK, now));
    assert!(!allowance.try_take(BLOCK, now));
    // Without a limit, the waiting block goes at once, and so does
    // everything after it.
    limiter.set_rate(0);
    assert!(!limiter.is_limited());
    assert_eq!(allowance.ready_at(), None);
    for _ in 0..1000 {
        assert!(allowance.try_take(BLOCK, now));
    }
    assert_eq!(allowance.ready_at(), None);
    // Limited again, the bytes taken meanwhile cost nothing: a full burst
    // is there.
    limiter.set_rate(RATE);
    assert!(limiter.is_limited());
    assert!(allowance.try_take(BLOCK, now));
    assert!(!allowance.try_take(BLOCK, now));
    assert_eq!(
        allowance.ready_at().unwrap() - now,
        Duration::from_millis(70)
    );
    // A reservation from before a change is not given back on release, as
    // the change reset the bucket: with the debt cut to the burst, the
    // next block waits 160 ms, not none.
    limiter.set_rate(RATE);
    allowance.release();
    let mut other = limiter.allowance();
    assert!(!other.try_take(BLOCK, now));
    assert_eq!(other.ready_at().unwrap() - now, Duration::from_millis(160));
}

#[tokio::test(start_paused = true)]
async fn a_change_wakes_the_waiters() {
    let limiter = Arc::new(RateLimiter::new(BLOCK));
    // A peer waiting for its turn, a second away at 16 KiB/s.
    let waiter = {
        let limiter = Arc::clone(&limiter);
        tokio::spawn(async move {
            let mut allowance = limiter.allowance();
            let start = Instant::now();
            assert!(allowance.try_take(BLOCK, start));
            loop {
                let changed = limiter.changed();
                if allowance.try_take(BLOCK, Instant::now()) {
                    return start.elapsed();
                }
                tokio::select! {
                    () = changed => {}
                    () = sleep_until_some(allowance.ready_at()) => {}
                }
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiter.is_finished());
    // Raised to 1600 KiB/s, it wakes and reserves again, and its block
    // goes one block's time (10 ms) later rather than at 1 s.
    limiter.set_rate(100 * BLOCK);
    assert_eq!(waiter.await.unwrap(), Duration::from_millis(110));
    // A future made before a change resolves even if not polled before it.
    let changed = limiter.changed();
    limiter.set_rate(BLOCK);
    tokio::time::timeout(Duration::from_secs(1), changed)
        .await
        .expect("not woken");
}
