use super::*;

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn drain(pool: &mut PeerPool, now: Instant) -> Vec<SocketAddr> {
    std::iter::from_fn(|| pool.next(now)).collect()
}

#[test]
fn hands_out_each_new_peer_once_in_order() {
    let now = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1), addr(2), addr(1)]);
    pool.add([addr(2), addr(3)]);
    assert_eq!(pool.ready_len(), 3);
    assert_eq!(drain(&mut pool, now), [addr(1), addr(2), addr(3)]);
    assert_eq!(pool.ready_len(), 0);
    // Re-announced peers that are already connected are not duplicated.
    pool.add([addr(1), addr(3)]);
    assert_eq!(pool.next(now), None);
    assert_eq!(pool.next_retry(), None);
}

#[test]
fn retries_failed_peer_with_growing_backoff_then_gives_up() {
    let start = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1)]);
    let mut now = start;
    for backoff in [RETRY_BACKOFF, RETRY_BACKOFF * 2] {
        assert_eq!(pool.next(now), Some(addr(1)));
        pool.failed(addr(1), false, now);
        assert_eq!(pool.next_retry(), Some(now + backoff));
        assert_eq!(pool.next(now + backoff - Duration::from_millis(1)), None);
        now += backoff;
    }
    assert_eq!(pool.next(now), Some(addr(1)));
    pool.failed(addr(1), false, now);
    assert_eq!(pool.next_retry(), None, "retried past MAX_FAILURES");
    assert_eq!(pool.next(now + RETRY_BACKOFF * 100), None);
}

#[test]
fn useful_peer_failures_start_over() {
    let mut now = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1)]);
    for _ in 0..MAX_FAILURES * 2 {
        assert_eq!(pool.next(now), Some(addr(1)));
        pool.failed(addr(1), true, now);
        assert_eq!(pool.next_retry(), Some(now + RETRY_BACKOFF));
        now += RETRY_BACKOFF;
    }
}

#[test]
fn banned_peer_is_never_returned() {
    let now = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1)]);
    assert_eq!(pool.next(now), Some(addr(1)));
    pool.ban(addr(1));
    pool.failed(addr(1), true, now);
    pool.add([addr(1)]);
    assert_eq!(pool.next(now + RETRY_BACKOFF * 100), None);
    assert_eq!(pool.next_retry(), None);
}

#[test]
fn requeued_peers_are_handed_out_again_unless_banned() {
    let now = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1), addr(2)]);
    assert_eq!(drain(&mut pool, now), [addr(1), addr(2)]);
    pool.requeue(addr(2));
    pool.requeue(addr(2));
    pool.ban(addr(1));
    pool.requeue(addr(1));
    assert_eq!(drain(&mut pool, now), [addr(2)]);
}

#[test]
fn due_retries_queue_behind_fresh_peers_in_due_order() {
    let now = Instant::now();
    let mut pool = PeerPool::default();
    pool.add([addr(1), addr(2)]);
    assert_eq!(pool.next(now), Some(addr(1)));
    assert_eq!(pool.next(now), Some(addr(2)));
    pool.failed(addr(2), false, now);
    pool.failed(addr(1), false, now + Duration::from_secs(1));
    pool.add([addr(3)]);
    let later = now + RETRY_BACKOFF * 2;
    assert_eq!(drain(&mut pool, later), [addr(3), addr(2), addr(1)]);
}
