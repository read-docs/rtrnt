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

    /// Gives up pinging `id` without evicting it, for a ping that could not
    /// be sent.
    pub fn stop_pinging(&mut self, id: &NodeId) {
        let index = self.index(id);
        for entry in &mut self.buckets[index].entries {
            if entry.node.id == *id {
                entry.pinging = false;
            }
        }
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
mod tests;
