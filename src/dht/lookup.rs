//! The iterative lookup of BEP 5 as a pure state machine, driven by
//! `Node::lookup` with the answers it gets.
//!
//! A lookup keeps the nodes it has heard of sorted by distance to the
//! target, queries the closest ones it has not asked yet (at most `ALPHA` at
//! a time), and adds the nodes they return. A query not answered within
//! its first timeout stops counting against `ALPHA`, so dead nodes do not
//! hold the lookup up, and nodes that never answer are dropped. The lookup
//! ends once the `K` closest nodes left have all answered.

use super::routing::{K, NodeId, NodeInfo};
use std::collections::HashSet;
use std::net::SocketAddrV4;

/// Queries in flight at once.
pub const ALPHA: usize = 3;
/// Candidates kept, closest first; only the closest `K` are ever queried
/// unless some of them fail.
const MAX_CANDIDATES: usize = 8 * K;
/// Queries one lookup may send, whatever the network answers.
const MAX_QUERIES: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Fresh,
    InFlight,
    /// Sent but not answered in time: it no longer counts against `ALPHA`,
    /// though a late answer still counts.
    Stalled,
    Responded,
}

#[derive(Debug)]
struct Candidate {
    node: NodeInfo,
    distance: [u8; 20],
    state: State,
    /// The `get_peers` token it sent, for `announce_peer`.
    token: Option<Vec<u8>>,
}

pub struct Lookup {
    own: NodeId,
    target: NodeId,
    /// Sorted by distance to the target. Nodes that failed are removed.
    candidates: Vec<Candidate>,
    /// Every address ever added, so that no node is queried twice.
    seen: HashSet<SocketAddrV4>,
    /// Bootstrap routers not queried yet. Their ids are unknown and they
    /// are never candidates themselves: they only hand out nodes.
    routers: Vec<SocketAddrV4>,
    routers_in_flight: HashSet<SocketAddrV4>,
    routers_stalled: HashSet<SocketAddrV4>,
    queries: usize,
}

impl Lookup {
    /// A lookup for `target` (never returning our `own` id) that starts
    /// from `nodes` and the `routers`.
    pub fn new(
        own: NodeId,
        target: NodeId,
        nodes: Vec<NodeInfo>,
        routers: Vec<SocketAddrV4>,
    ) -> Self {
        let mut lookup = Self {
            own,
            target,
            candidates: Vec::new(),
            seen: routers.iter().copied().collect(),
            routers,
            routers_in_flight: HashSet::new(),
            routers_stalled: HashSet::new(),
            queries: 0,
        };
        lookup.add(nodes);
        lookup
    }

    /// Adds nodes we have not seen, keeping the closest `MAX_CANDIDATES`.
    fn add(&mut self, nodes: Vec<NodeInfo>) {
        for node in nodes {
            let reachable = node.addr.port() != 0 && !node.addr.ip().is_unspecified();
            if node.id == self.own
                || !reachable
                || self.candidates.iter().any(|c| c.node.id == node.id)
                || !self.seen.insert(node.addr)
            {
                continue;
            }
            let distance = node.id.distance(&self.target);
            let at = self.candidates.partition_point(|c| c.distance < distance);
            if at >= MAX_CANDIDATES {
                continue;
            }
            self.candidates.insert(
                at,
                Candidate {
                    node,
                    distance,
                    state: State::Fresh,
                    token: None,
                },
            );
            self.candidates.truncate(MAX_CANDIDATES);
        }
    }

    fn in_flight(&self) -> usize {
        let nodes = self
            .candidates
            .iter()
            .filter(|c| c.state == State::InFlight);
        self.routers_in_flight.len() + nodes.count()
    }

    /// The addresses to query now: the routers at once, then the closest
    /// fresh candidates among the `K` closest ones, keeping at most
    /// `ALPHA` queries in flight.
    pub fn next_queries(&mut self) -> Vec<SocketAddrV4> {
        let mut out = Vec::new();
        for router in std::mem::take(&mut self.routers) {
            if self.queries < MAX_QUERIES {
                self.queries += 1;
                self.routers_in_flight.insert(router);
                out.push(router);
            }
        }
        let mut in_flight = self.in_flight();
        for candidate in self.candidates.iter_mut().take(K) {
            if in_flight >= ALPHA || self.queries >= MAX_QUERIES {
                break;
            }
            if candidate.state == State::Fresh {
                candidate.state = State::InFlight;
                in_flight += 1;
                self.queries += 1;
                out.push(candidate.node.addr);
            }
        }
        out
    }

    /// Records the answer from `from`, which says it is node `id`, with the
    /// `nodes` closer to the target it knows and its `get_peers` token.
    pub fn on_response(
        &mut self,
        from: SocketAddrV4,
        id: NodeId,
        nodes: Vec<NodeInfo>,
        token: Option<Vec<u8>>,
    ) {
        if self.routers_in_flight.remove(&from) | self.routers_stalled.remove(&from) {
            self.add(nodes);
            return;
        }
        let Some(pos) = self.candidates.iter().position(|c| c.node.addr == from) else {
            // Dropped meanwhile for closer candidates.
            return;
        };
        if self.candidates[pos].node.id == id {
            self.candidates[pos].state = State::Responded;
            self.candidates[pos].token = token;
        } else {
            // It was listed under another id, so it sits at the wrong
            // distance. Put it where its real id belongs.
            self.candidates.remove(pos);
            let distance = id.distance(&self.target);
            let duplicate = self.candidates.iter().any(|c| c.node.id == id);
            let at = self.candidates.partition_point(|c| c.distance < distance);
            if id != self.own && !duplicate && at < MAX_CANDIDATES {
                let node = NodeInfo { id, addr: from };
                let state = State::Responded;
                self.candidates.insert(
                    at,
                    Candidate {
                        node,
                        distance,
                        state,
                        token,
                    },
                );
                self.candidates.truncate(MAX_CANDIDATES);
            }
        }
        self.add(nodes);
    }

    /// Records that `from` has not answered within the first timeout, so
    /// that another node can be queried meanwhile.
    pub fn on_slow(&mut self, from: SocketAddrV4) {
        if self.routers_in_flight.remove(&from) {
            self.routers_stalled.insert(from);
            return;
        }
        let in_flight = self.candidates.iter_mut().find(|c| c.node.addr == from);
        if let Some(candidate) = in_flight.filter(|c| c.state == State::InFlight) {
            candidate.state = State::Stalled;
        }
    }

    /// Records that `from` did not answer at all.
    pub fn on_failure(&mut self, from: SocketAddrV4) {
        if self.routers_in_flight.remove(&from) | self.routers_stalled.remove(&from) {
            return;
        }
        self.candidates.retain(|c| c.node.addr != from);
    }

    /// Whether the lookup is over: the routers have answered (or failed)
    /// and the `K` closest candidates have all answered too (or we may not
    /// query any more). Until then, `next_queries` returns something or a
    /// query is still outstanding.
    pub fn is_done(&self) -> bool {
        let can_query = self.queries < MAX_QUERIES;
        let busy = self.candidates.iter().take(K).any(|c| match c.state {
            State::InFlight | State::Stalled => true,
            State::Fresh => can_query,
            State::Responded => false,
        });
        let routers_busy = !self.routers_in_flight.is_empty() || !self.routers_stalled.is_empty();
        self.routers.is_empty() && !routers_busy && !busy
    }

    /// The `K` closest nodes that answered, closest first, with the token
    /// each sent.
    pub fn closest_responded(&self) -> Vec<(NodeInfo, Option<Vec<u8>>)> {
        self.candidates
            .iter()
            .filter(|c| c.state == State::Responded)
            .take(K)
            .map(|c| (c.node, c.token.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests;
