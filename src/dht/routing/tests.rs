use super::*;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::net::Ipv4Addr;

/// An id sharing exactly `prefix` leading bits with the all-zero id,
/// made distinct by `n`.
fn id_at(prefix: usize, n: u8) -> NodeId {
    let mut id = NodeId([0; 20]);
    id.set_bit(prefix, true);
    id.0[19] |= n;
    id
}

fn node(id: NodeId, port: u16) -> NodeInfo {
    NodeInfo {
        id,
        addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, port),
    }
}

fn table(now: Instant) -> RoutingTable {
    RoutingTable::new(NodeId([0; 20]), now)
}

#[test]
fn distance_and_prefix() {
    let a = NodeId([0; 20]);
    let mut b = a;
    b.0[2] = 0b0001_0000;
    assert_eq!(a.common_prefix(&b), 19);
    assert_eq!(a.common_prefix(&a), ID_BITS);
    assert_eq!(a.distance(&b), b.0);
    assert_eq!(id_at(0, 0).common_prefix(&a), 0);
    assert_eq!(id_at(159, 0).common_prefix(&a), 159);
    assert!(a.distance(&id_at(5, 0)) < a.distance(&id_at(4, 0)));
}

#[test]
fn splits_only_the_bucket_holding_our_id() {
    let now = Instant::now();
    let mut t = table(now);
    // Eight far nodes fill the single bucket.
    for n in 0..K as u8 {
        assert_eq!(t.insert(node(id_at(0, n), n.into()), now), Insert::Added);
    }
    assert_eq!(t.buckets.len(), 1);
    // A ninth node splits it: the far nodes stay in bucket 0, and the
    // new one goes into the new last bucket.
    assert_eq!(t.insert(node(id_at(3, 0), 100), now), Insert::Added);
    assert_eq!(t.buckets.len(), 2);
    assert_eq!(t.buckets[0].entries.len(), K);
    assert_eq!(t.buckets[1].entries.len(), 1);
    // Bucket 0 no longer contains our id, so it is full for good.
    assert_eq!(t.insert(node(id_at(0, 99), 99), now), Insert::Full);
    assert_eq!(t.buckets.len(), 2);
    assert_eq!(t.buckets[0].replacements.len(), 1);
    assert_eq!(t.len(), K + 1);
    // Close nodes keep splitting our bucket.
    for n in 0..K as u8 {
        assert_eq!(
            t.insert(node(id_at(3 + n as usize, 1), 200 + u16::from(n)), now),
            Insert::Added
        );
    }
    assert!(t.buckets.len() > 4);
    for (i, bucket) in t.buckets.iter().enumerate() {
        for entry in &bucket.entries {
            assert_eq!(t.index(&entry.node.id), i);
        }
    }
}

#[test]
fn known_node_is_updated_and_new_address_replaces_old_id() {
    let now = Instant::now();
    let mut t = table(now);
    assert_eq!(t.insert(node(id_at(0, 1), 1), now), Insert::Added);
    assert_eq!(t.insert(node(id_at(0, 1), 1), now), Insert::Updated);
    // Same address, new id: the node restarted.
    assert_eq!(t.insert(node(id_at(1, 1), 1), now), Insert::Added);
    assert_eq!(t.len(), 1);
    assert_eq!(t.closest(&NodeId([0; 20]), 8)[0].id, id_at(1, 1));
    // Our own id never goes in.
    assert_eq!(t.insert(node(NodeId([0; 20]), 9), now), Insert::Full);
    assert_eq!(t.len(), 1);
}

#[test]
fn questionable_nodes_are_pinged_before_eviction() {
    let start = Instant::now();
    let mut t = table(start);
    for n in 0..K as u8 {
        t.insert(node(id_at(0, n), n.into()), start);
    }
    t.split(start);
    // All good: the newcomer only becomes a replacement.
    assert_eq!(t.insert(node(id_at(0, 50), 50), start), Insert::Full);

    let later = start + QUESTIONABLE_AFTER;
    // Node 3 was heard from since; node 0 is the least recently seen.
    assert!(t.heard_from(
        &id_at(0, 3),
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, 3),
        later
    ));
    let Insert::PingFirst(first) = t.insert(node(id_at(0, 51), 51), later) else {
        panic!("no ping asked for");
    };
    // One ping per bucket at a time.
    assert_eq!(t.insert(node(id_at(0, 52), 52), later), Insert::Full);

    // It answers (its response goes through `insert`), so the next
    // questionable node is pinged; that one does not answer.
    assert_eq!(t.insert(first, later), Insert::Updated);
    let second = t.ping_next(&first.id, later).unwrap();
    assert_ne!(second.id, first.id);
    assert_ne!(second.id, id_at(0, 3));
    t.evict(&second.id, later);
    assert_eq!(t.buckets[0].entries.len(), K);
    // The newest replacement took its place.
    assert!(
        t.buckets[0]
            .entries
            .iter()
            .any(|e| e.node.id == id_at(0, 52))
    );
    assert!(!t.buckets[0].entries.iter().any(|e| e.node.id == second.id));
}

#[test]
fn an_update_keeps_the_ping_in_flight() {
    let start = Instant::now();
    let mut t = table(start);
    for n in 0..K as u8 {
        t.insert(node(id_at(0, n), n.into()), start);
    }
    t.split(start);
    t.insert(node(id_at(0, 50), 50), start);

    let later = start + QUESTIONABLE_AFTER;
    let Insert::PingFirst(first) = t.insert(node(id_at(0, 51), 51), later) else {
        panic!("no ping asked for");
    };
    // The node answers some other query before its ping ends. That must not
    // start a second ping in the bucket.
    assert_eq!(t.insert(first, later), Insert::Updated);
    assert_eq!(t.insert(node(id_at(0, 52), 52), later), Insert::Full);
    // Once its ping ends, the next questionable node may be pinged.
    let second = t.ping_next(&first.id, later).unwrap();
    assert_ne!(second.id, first.id);
}

#[test]
fn bad_nodes_are_replaced() {
    let now = Instant::now();
    let mut t = table(now);
    for n in 0..K as u8 {
        t.insert(node(id_at(0, n), n.into()), now);
    }
    t.split(now);
    let dead = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 2);
    t.failed(dead, now);
    assert!(
        t.closest(&id_at(0, 2), 1)[0].addr == dead,
        "one failure is not bad yet"
    );
    t.failed(dead, now);
    assert!(
        !t.closest(&NodeId([0; 20]), 20)
            .iter()
            .any(|n| n.addr == dead)
    );
    assert!(t.wants(&id_at(0, 60)));
    assert_eq!(t.insert(node(id_at(0, 60), 60), now), Insert::Added);
    assert_eq!(t.buckets[0].entries.len(), K);
    assert!(!t.wants(&id_at(0, 61)));

    // With a replacement waiting, a node going bad is swapped out at once.
    assert_eq!(t.insert(node(id_at(0, 61), 61), now), Insert::Full);
    let flaky = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 5);
    t.failed(flaky, now);
    t.failed(flaky, now);
    assert!(
        t.buckets[0]
            .entries
            .iter()
            .any(|e| e.node.id == id_at(0, 61))
    );
    assert!(!t.buckets[0].entries.iter().any(|e| e.node.addr == flaky));
}

#[test]
fn closest_nodes_come_sorted_by_distance() {
    let now = Instant::now();
    let mut t = table(now);
    let mut rng = StdRng::seed_from_u64(1);
    for port in 0..200 {
        t.insert(node(NodeId(rng.random()), port), now);
    }
    let target = NodeId(rng.random());
    let closest = t.closest(&target, K);
    assert_eq!(closest.len(), K);
    assert!(closest.is_sorted_by_key(|n| n.id.distance(&target)));
    // Nobody in the table is closer than the last one returned.
    let worst = closest[K - 1].id.distance(&target);
    let all = t.closest(&target, usize::MAX);
    assert!(all.len() <= 200);
    assert_eq!(
        all.iter()
            .filter(|n| n.id.distance(&target) <= worst)
            .count(),
        K
    );
}

#[test]
fn refresh_targets_fall_in_stale_buckets() {
    let start = Instant::now();
    let mut t = table(start);
    let mut rng = StdRng::seed_from_u64(2);
    for port in 0..100 {
        t.insert(node(NodeId(rng.random()), port), start);
    }
    assert!(t.refresh_targets(start, &mut rng).is_empty());
    let later = start + QUESTIONABLE_AFTER;
    let targets = t.refresh_targets(later, &mut rng);
    assert_eq!(targets.len(), t.buckets.len());
    for (i, target) in targets.iter().enumerate() {
        assert_eq!(t.index(target), i);
    }
    // A bucket that changes is no longer stale.
    let known = t.closest(&id_at(0, 0), 1)[0];
    assert_eq!(t.insert(known, later), Insert::Updated);
    assert_eq!(
        t.refresh_targets(later, &mut rng).len(),
        t.buckets.len() - 1
    );
}
