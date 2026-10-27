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
