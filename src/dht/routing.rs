//! The Kademlia routing table of BEP 5: the DHT nodes we know, in k-buckets
//! by XOR distance from our own id.
//!
//! Bucket `i` holds the nodes whose ids share exactly `i` leading bits with
//! ours, and the last bucket everything closer. Only that last bucket, the
//! one containing our own id, is ever split, which is the same as BEP 5's
//! splitting of id ranges. Only nodes that answered one of our queries are
//! added, so a spoofed source address cannot get into the table.

use rand::Rng;
use std::fmt;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::time::Instant;

/// Nodes per bucket.
pub const K: usize = 8;
const ID_BITS: usize = 160;
/// A node not heard from for this long is questionable, and a bucket not
/// changed for this long is refreshed.
pub const QUESTIONABLE_AFTER: Duration = Duration::from_secs(15 * 60);
/// A node that fails this many queries in a row is bad and replaced.
const MAX_FAILURES: u32 = 2;

/// A 160-bit node id. Info hashes live in the same space.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub [u8; 20]);

impl NodeId {
    pub fn random() -> Self {
        Self(rand::random())
    }

    /// XOR distance. Arrays compare like big-endian numbers, so distances
    /// can be compared directly.
    pub fn distance(&self, other: &NodeId) -> [u8; 20] {
        std::array::from_fn(|i| self.0[i] ^ other.0[i])
    }

    /// Number of leading bits the two ids share.
    pub fn common_prefix(&self, other: &NodeId) -> usize {
        self.0
            .iter()
            .zip(&other.0)
            .position(|(a, b)| a != b)
            .map_or(ID_BITS, |i| {
                i * 8 + (self.0[i] ^ other.0[i]).leading_zeros() as usize
            })
    }

    fn bit(&self, bit: usize) -> bool {
        self.0[bit / 8] & (0x80 >> (bit % 8)) != 0
    }

    fn set_bit(&mut self, bit: usize, value: bool) {
        let mask = 0x80 >> (bit % 8);
        if value {
            self.0[bit / 8] |= mask;
        } else {
            self.0[bit / 8] &= !mask;
        }
    }
}

impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|b| write!(f, "{b:02x}"))
    }
}

/// A node and where to reach it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddrV4,
}

/// What `RoutingTable::insert` did with a node.
#[derive(Debug, PartialEq, Eq)]
pub enum Insert {
    Added,
    /// Already known; marked as just heard from.
    Updated,
    /// The bucket is full of good nodes, so the node was kept as a
    /// replacement. This questionable node should be pinged: if it does not
    /// answer, `evict` it; if it does, ask `ping_next` for the next one.
    PingFirst(NodeInfo),
    /// The bucket is full of good nodes; kept as a replacement, if at all.
    Full,
}

#[derive(Debug, Clone)]
struct Entry {
    node: NodeInfo,
    last_seen: Instant,
    /// Queries failed in a row.
    failures: u32,
    /// Being pinged to decide whether it stays.
    pinging: bool,
}

impl Entry {
    fn new(node: NodeInfo, now: Instant) -> Self {
        Self {
            node,
            last_seen: now,
            failures: 0,
            pinging: false,
        }
    }

    fn is_bad(&self) -> bool {
        self.failures >= MAX_FAILURES
    }

    fn is_questionable(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_seen) >= QUESTIONABLE_AFTER
    }
}

#[derive(Debug)]
struct Bucket {
    entries: Vec<Entry>,
    /// Nodes that answered us while the bucket was full, newest last, to
    /// take the place of nodes that go bad.
    replacements: Vec<Entry>,
    last_changed: Instant,
}

impl Bucket {
    fn new(now: Instant) -> Self {
        Self {
            entries: Vec::with_capacity(K),
            replacements: Vec::new(),
            last_changed: now,
        }
    }

    fn cache(&mut self, entry: Entry) {
        self.replacements.retain(|e| e.node.id != entry.node.id);
        self.replacements.push(entry);
        if self.replacements.len() > K {
            self.replacements.remove(0);
        }
    }

    /// The least recently seen questionable node, if one should be pinged:
    /// a replacement is waiting and no other node of the bucket is being
    /// pinged.
    fn questionable_to_ping(&mut self, now: Instant) -> Option<NodeInfo> {
        if self.replacements.is_empty() || self.entries.iter().any(|e| e.pinging) {
            return None;
        }
        let entry = self
            .entries
            .iter_mut()
            .filter(|e| e.is_questionable(now))
            .min_by_key(|e| e.last_seen)?;
        entry.pinging = true;
        Some(entry.node)
    }

    /// Removes the node with `id` and moves the newest replacement into its
    /// place.
    fn evict(&mut self, id: &NodeId, now: Instant) {
        let before = self.entries.len();
        self.entries.retain(|e| e.node.id != *id);
        if self.entries.len() < before {
            self.entries.extend(self.replacements.pop());
            self.last_changed = now;
        }
    }
}

pub struct RoutingTable {
    own: NodeId,
    /// Never empty; the last bucket contains our own id.
    buckets: Vec<Bucket>,
}

impl RoutingTable {
    pub fn new(own: NodeId, now: Instant) -> Self {
        Self {
            own,
            buckets: vec![Bucket::new(now)],
        }
    }

    /// Number of nodes in the table.
    pub fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.entries.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn index(&self, id: &NodeId) -> usize {
        self.own.common_prefix(id).min(self.buckets.len() - 1)
    }

    /// Adds a node that just answered one of our queries, or marks it as
    /// heard from if it is known.
    pub fn insert(&mut self, node: NodeInfo, now: Instant) -> Insert {
        if node.id == self.own {
            return Insert::Full;
        }
        // A node that restarted with a new id drops its old entry.
        for bucket in &mut self.buckets {
            bucket
                .entries
                .retain(|e| e.node.addr != node.addr || e.node.id == node.id);
        }
        loop {
            let index = self.index(&node.id);
            let can_split = index == self.buckets.len() - 1 && self.buckets.len() < ID_BITS;
            let bucket = &mut self.buckets[index];
            if let Some(entry) = bucket.entries.iter_mut().find(|e| e.node.id == node.id) {
                *entry = Entry::new(node, now);
                bucket.last_changed = now;
                return Insert::Updated;
            }
            if bucket.entries.len() < K {
                bucket.replacements.retain(|e| e.node.id != node.id);
                bucket.entries.push(Entry::new(node, now));
                bucket.last_changed = now;
                return Insert::Added;
            }
            if can_split {
                self.split(now);
                continue;
            }
            if let Some(bad) = bucket.entries.iter_mut().find(|e| e.is_bad()) {
                *bad = Entry::new(node, now);
                bucket.last_changed = now;
                return Insert::Added;
            }
            bucket.cache(Entry::new(node, now));
            return match bucket.questionable_to_ping(now) {
                Some(questionable) => Insert::PingFirst(questionable),
                None => Insert::Full,
            };
        }
    }

    /// Splits the last bucket in two: nodes sharing one more bit with our id
    /// move to a new last bucket.
    fn split(&mut self, now: Instant) {
        let own = self.own;
        let depth = self.buckets.len();
        let closer = |e: &Entry| own.common_prefix(&e.node.id) >= depth;
        let mut new = Bucket::new(now);
        if let Some(last) = self.buckets.last_mut() {
            let (moved, kept) = std::mem::take(&mut last.entries)
                .into_iter()
                .partition(closer);
            last.entries = kept;
            new.entries = moved;
            let (moved, kept) = std::mem::take(&mut last.replacements)
                .into_iter()
                .partition(closer);
            last.replacements = kept;
            new.replacements = moved;
        }
        self.buckets.push(new);
    }

    /// After a questionable node answered our ping: the next questionable
    /// node of the bucket `id` is in, if one should be pinged.
    pub fn ping_next(&mut self, id: &NodeId, now: Instant) -> Option<NodeInfo> {
        let index = self.index(id);
        self.buckets[index].questionable_to_ping(now)
    }

    /// Drops a node that did not answer a ping, taking a replacement in.
    pub fn evict(&mut self, id: &NodeId, now: Instant) {
        let index = self.index(id);
        self.buckets[index].evict(id, now);
    }

    /// Records a query to `addr` that went unanswered. A node that goes bad
    /// this way is replaced at once if a replacement is waiting.
    pub fn failed(&mut self, addr: SocketAddrV4, now: Instant) {
        for bucket in &mut self.buckets {
            let Some(entry) = bucket.entries.iter_mut().find(|e| e.node.addr == addr) else {
                continue;
            };
            entry.failures += 1;
            entry.pinging = false;
            if entry.is_bad() && !bucket.replacements.is_empty() {
                let id = entry.node.id;
                bucket.evict(&id, now);
            }
            return;
        }
    }

    /// Notes a query from a known node. Returns false if it is not in the
    /// table (at that address).
    pub fn heard_from(&mut self, id: &NodeId, addr: SocketAddrV4, now: Instant) -> bool {
        let index = self.index(id);
        let entry = self.buckets[index]
            .entries
            .iter_mut()
            .find(|e| e.node.id == *id && e.node.addr == addr);
        match entry {
            Some(entry) => {
                entry.last_seen = now;
                entry.failures = 0;
                true
            }
            None => false,
        }
    }

    /// Whether a node we do not know yet would get a place in the table,
    /// so that it is worth checking that it answers.
    pub fn wants(&self, id: &NodeId) -> bool {
        if *id == self.own {
            return false;
        }
        let index = self.index(id);
        let bucket = &self.buckets[index];
        let can_split = index == self.buckets.len() - 1 && self.buckets.len() < ID_BITS;
        !bucket.entries.iter().any(|e| e.node.id == *id)
            && (bucket.entries.len() < K || can_split || bucket.entries.iter().any(Entry::is_bad))
    }

    /// Up to `count` nodes closest to `target`, closest first. Bad nodes are
    /// left out.
    pub fn closest(&self, target: &NodeId, count: usize) -> Vec<NodeInfo> {
        let mut nodes: Vec<NodeInfo> = self
            .buckets
            .iter()
            .flat_map(|b| &b.entries)
            .filter(|e| !e.is_bad())
            .map(|e| e.node)
            .collect();
        nodes.sort_by_key(|node| node.id.distance(target));
        nodes.truncate(count);
        nodes
    }

    /// A random id in each bucket that has not changed for
    /// `QUESTIONABLE_AFTER`. Looking those up refreshes the buckets.
    pub fn refresh_targets(&self, now: Instant, rng: &mut impl Rng) -> Vec<NodeId> {
        (0..self.buckets.len())
            .filter(|&i| {
                now.saturating_duration_since(self.buckets[i].last_changed) >= QUESTIONABLE_AFTER
            })
            .map(|i| self.random_id_in(i, rng))
            .collect()
    }

    /// A random id that falls in bucket `index`: our first `index` bits,
    /// then (except in the last bucket) the opposite of our next bit.
    fn random_id_in(&self, index: usize, rng: &mut impl Rng) -> NodeId {
        let mut id = NodeId(rng.random());
        for bit in 0..index {
            id.set_bit(bit, self.own.bit(bit));
        }
        if index < self.buckets.len() - 1 {
            id.set_bit(index, !self.own.bit(index));
        }
        id
    }
}

#[cfg(test)]
mod tests {
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
}
