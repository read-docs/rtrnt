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
mod tests {
    use super::*;
    use crate::dht::routing::RoutingTable;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use std::collections::HashMap;
    use std::net::Ipv4Addr;
    use tokio::time::Instant;

    fn addr(n: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), n)
    }

    fn node(id: u8, port: u16) -> NodeInfo {
        let mut bytes = [0u8; 20];
        bytes[0] = id;
        NodeInfo {
            id: NodeId(bytes),
            addr: addr(port),
        }
    }

    const OWN: NodeId = NodeId([0xff; 20]);
    const TARGET: NodeId = NodeId([0; 20]);

    #[test]
    fn queries_routers_then_alpha_closest() {
        let mut lookup = Lookup::new(OWN, TARGET, Vec::new(), vec![addr(1), addr(2)]);
        assert!(!lookup.is_done());
        assert_eq!(lookup.next_queries(), [addr(1), addr(2)]);
        assert!(lookup.next_queries().is_empty());
        assert!(!lookup.is_done());

        let nodes = (10..20).map(|i| node(i, i.into())).collect();
        lookup.on_response(addr(1), NodeId([7; 20]), nodes, None);
        // The other router is still in flight, so two slots are left.
        assert_eq!(lookup.next_queries(), [addr(10), addr(11)]);
        lookup.on_failure(addr(2));
        assert_eq!(lookup.next_queries(), [addr(12)]);
        assert!(lookup.next_queries().is_empty());
        // Routers never become candidates.
        assert!(lookup.candidates.iter().all(|c| c.node.addr.port() >= 10));
    }

    #[test]
    fn ends_when_the_closest_k_answered() {
        let seeds: Vec<NodeInfo> = (1..=12).map(|i| node(i, i.into())).collect();
        let mut lookup = Lookup::new(OWN, TARGET, seeds, Vec::new());
        let mut asked = Vec::new();
        while !lookup.is_done() {
            let queries = lookup.next_queries();
            assert!(!queries.is_empty() && queries.len() <= ALPHA);
            for to in queries {
                asked.push(to.port());
                let id = node(to.port() as u8, 0).id;
                let token = Some(vec![to.port() as u8]);
                lookup.on_response(to, id, Vec::new(), token);
            }
        }
        // Only the closest eight were asked; nobody returned closer nodes.
        assert_eq!(asked, (1..=8).collect::<Vec<u16>>());
        let closest = lookup.closest_responded();
        assert_eq!(closest.len(), K);
        assert_eq!(closest[0], (node(1, 1), Some(vec![1])));
    }

    #[test]
    fn failed_nodes_make_room_for_the_next_ones() {
        let seeds: Vec<NodeInfo> = (1..=10).map(|i| node(i, i.into())).collect();
        let mut lookup = Lookup::new(OWN, TARGET, seeds, Vec::new());
        assert_eq!(lookup.next_queries(), [addr(1), addr(2), addr(3)]);
        lookup.on_failure(addr(1));
        lookup.on_failure(addr(2));
        assert_eq!(lookup.next_queries(), [addr(4), addr(5)]);
        for port in [3, 4, 5] {
            lookup.on_response(addr(port), node(port as u8, 0).id, Vec::new(), None);
        }
        let mut rest = Vec::new();
        while !lookup.is_done() {
            for to in lookup.next_queries() {
                rest.push(to.port());
                lookup.on_response(to, node(to.port() as u8, 0).id, Vec::new(), None);
            }
        }
        assert_eq!(rest, [6, 7, 8, 9, 10]);
        assert_eq!(lookup.closest_responded().len(), 8);
    }

    #[test]
    fn slow_queries_free_their_slot_but_may_still_answer() {
        let seeds: Vec<NodeInfo> = (1..=10).map(|i| node(i, i.into())).collect();
        let mut lookup = Lookup::new(OWN, TARGET, seeds, vec![addr(100)]);
        assert_eq!(lookup.next_queries(), [addr(100), addr(1), addr(2)]);
        lookup.on_slow(addr(100));
        lookup.on_slow(addr(1));
        assert_eq!(lookup.next_queries(), [addr(3), addr(4)]);
        // A late answer still counts.
        lookup.on_response(addr(1), node(1, 0).id, Vec::new(), None);
        assert!(lookup.next_queries().is_empty());
        assert_eq!(lookup.closest_responded()[0].0, node(1, 1));
        // Answering twice changes nothing.
        lookup.on_slow(addr(1));
        assert_eq!(lookup.closest_responded()[0].0, node(1, 1));

        for port in 2..=8 {
            lookup.next_queries();
            lookup.on_response(addr(port), node(port as u8, 0).id, Vec::new(), None);
        }
        // The closest eight answered, but the stalled router is awaited.
        assert!(!lookup.is_done());
        lookup.on_failure(addr(100));
        assert!(lookup.is_done());
    }

    #[test]
    fn ignores_ourselves_duplicates_and_unreachable_nodes() {
        let mut lookup = Lookup::new(OWN, TARGET, Vec::new(), Vec::new());
        let unspecified = NodeInfo {
            id: NodeId([3; 20]),
            addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 1),
        };
        lookup.add(vec![
            NodeInfo {
                id: OWN,
                addr: addr(1),
            },
            node(1, 2),
            node(1, 3),
            node(2, 2),
            node(4, 0),
            unspecified,
        ]);
        let listed: Vec<_> = lookup.candidates.iter().map(|c| c.node).collect();
        assert_eq!(listed, [node(1, 2)]);
        assert_eq!(lookup.next_queries(), [addr(2)]);
    }

    #[test]
    fn a_node_answering_with_another_id_moves_to_its_real_distance() {
        let mut lookup = Lookup::new(OWN, TARGET, vec![node(1, 1), node(2, 2)], Vec::new());
        lookup.next_queries();
        // Listed as the closest, but really far away.
        lookup.on_response(addr(1), node(200, 0).id, Vec::new(), None);
        let order: Vec<u16> = lookup
            .candidates
            .iter()
            .map(|c| c.node.addr.port())
            .collect();
        assert_eq!(order, [2, 1]);
        assert_eq!(lookup.candidates[1].node.id, node(200, 0).id);
    }

    #[test]
    fn query_count_is_bounded() {
        let mut lookup = Lookup::new(OWN, TARGET, vec![node(1, 1)], Vec::new());
        let mut port = 2u16;
        let mut sent = 0;
        while !lookup.is_done() {
            for to in lookup.next_queries() {
                sent += 1;
                // Every node fails, but points at more nodes.
                lookup.on_failure(to);
                let more = (0..4).map(|_| {
                    port += 1;
                    let mut id = [0u8; 20];
                    id[..2].copy_from_slice(&port.to_be_bytes());
                    NodeInfo {
                        id: NodeId(id),
                        addr: addr(port),
                    }
                });
                lookup.add(more.collect());
            }
        }
        assert_eq!(sent, MAX_QUERIES);
    }

    /// 300 simulated nodes, each with a real routing table holding everyone
    /// it could fit. A lookup from three random seeds must find the true
    /// closest eight, as Kademlia promises.
    #[test]
    fn finds_the_closest_nodes_in_a_simulated_network() {
        let now = Instant::now();
        let mut rng = StdRng::seed_from_u64(3);
        let nodes: Vec<NodeInfo> = (1..=300)
            .map(|port| NodeInfo {
                id: NodeId(rng.random()),
                addr: addr(port),
            })
            .collect();
        let tables: HashMap<SocketAddrV4, (NodeId, RoutingTable)> = nodes
            .iter()
            .map(|me| {
                let mut table = RoutingTable::new(me.id, now);
                for other in &nodes {
                    table.insert(*other, now);
                }
                (me.addr, (me.id, table))
            })
            .collect();

        for _ in 0..20 {
            let target = NodeId(rng.random());
            let seeds = (0..3)
                .map(|_| nodes[rng.random_range(0..nodes.len())])
                .collect();
            let mut lookup = Lookup::new(OWN, target, seeds, Vec::new());
            let mut sent = 0;
            while !lookup.is_done() {
                for to in lookup.next_queries() {
                    sent += 1;
                    let (id, table) = &tables[&to];
                    lookup.on_response(to, *id, table.closest(&target, K), None);
                }
            }
            let mut expected = nodes.clone();
            expected.sort_by_key(|n| n.id.distance(&target));
            let found: Vec<NodeInfo> = lookup
                .closest_responded()
                .into_iter()
                .map(|(n, _)| n)
                .collect();
            assert_eq!(found, expected[..K]);
            assert!(sent < 60, "{sent} queries");
        }
    }
}
