use super::*;
use std::net::IpAddr;

/// A socket address from anything that is an IP address.
fn sock(ip: impl Into<IpAddr>, port: u16) -> SocketAddr {
    SocketAddr::new(ip.into(), port)
}

const OWN: NodeId = NodeId([0; 20]);
const INFO_HASH: [u8; 20] = [9; 20];

fn addr(port: u16) -> SocketAddr {
    sock(Ipv4Addr::new(10, 0, 0, 1), port)
}

fn ask(state: &mut State, query: Query, from: SocketAddr, now: Instant) -> (Body, bool) {
    answer(state, OWN, NodeId([1; 20]), query, from, now)
}

fn get_peers(noseed: bool, scrape: bool) -> Query {
    Query::GetPeers {
        want: Want::default(),
        info_hash: INFO_HASH,
        noseed,
        scrape,
    }
}

fn response(body: Body) -> Response {
    match body {
        Body::Response(response) => response,
        other => panic!("not a response: {other:?}"),
    }
}

#[test]
fn answers_ping_and_find_node() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let known = NodeInfo {
        id: NodeId([2; 20]),
        addr: addr(2),
    };
    state.table.insert(known, now);

    let (body, verify) = ask(&mut state, Query::Ping, addr(1), now);
    assert_eq!(
        response(body),
        Response {
            id: OWN,
            ..Response::default()
        }
    );
    assert!(verify, "an unknown node with room for it is worth a ping");

    let target = NodeId([3; 20]);
    let (body, _) = ask(
        &mut state,
        Query::FindNode {
            want: Want::default(),
            target,
        },
        addr(1),
        now,
    );
    assert_eq!(response(body).nodes, [known]);

    // A known node is not pinged again.
    let (_, verify) = answer(&mut state, OWN, known.id, Query::Ping, known.addr, now);
    assert!(!verify);
}

#[test]
fn announce_needs_a_token_from_get_peers() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let (body, _) = ask(&mut state, get_peers(false, false), addr(1), now);
    let reply = response(body);
    assert!(reply.values.is_empty());
    let token = reply.token.unwrap();

    let announce = |port, implied_port, token: &[u8]| Query::AnnouncePeer {
        info_hash: INFO_HASH,
        port,
        implied_port,
        seed: false,
        token: token.to_vec(),
    };
    // From another address, or with a made-up token: refused.
    let other_ip = sock(Ipv4Addr::new(10, 0, 0, 2), 1);
    for (from, token) in [(other_ip, &token[..]), (addr(1), b"made up".as_slice())] {
        let (body, _) = ask(&mut state, announce(6881, false, token), from, now);
        assert!(
            matches!(
                body,
                Body::Error {
                    code: ERROR_PROTOCOL,
                    ..
                }
            ),
            "{body:?}"
        );
    }
    // The token is bound to the IP address, not the port.
    let (body, _) = ask(&mut state, announce(6881, false, &token), addr(5), now);
    response(body);
    let get_peers =
        |state: &mut State| response(ask(state, get_peers(false, false), addr(1), now).0).values;
    assert_eq!(get_peers(&mut state), [addr(6881)]);
    // The same host announcing again, with its source port as `implied_port`
    // says, replaces its entry.
    let (body, _) = ask(&mut state, announce(0, true, &token), addr(7), now);
    response(body);
    assert_eq!(get_peers(&mut state), [addr(7)]);
}

#[test]
fn unknown_methods_get_error_204() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let (body, _) = ask(&mut state, Query::Unknown(b"vote".to_vec()), addr(1), now);
    assert!(matches!(
        body,
        Body::Error {
            code: ERROR_METHOD_UNKNOWN,
            ..
        }
    ));
}

#[tokio::test]
async fn a_full_pending_table_does_not_evict_nodes() {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let dht = Dht::start(socket, Vec::new()).unwrap();
    let node = &dht.0.node;
    let known = NodeInfo {
        id: NodeId([2; 20]),
        addr: addr(2),
    };
    {
        let mut state = node.lock();
        state.table.insert(known, Instant::now());
        for tid in 0..MAX_PENDING as u16 {
            let (reply, _) = oneshot::channel();
            state.pending.insert(
                tid,
                Pending {
                    addr: addr(3),
                    reply,
                },
            );
        }
    }
    // The ping cannot even be sent, which says nothing about the node.
    Arc::clone(node).evict_check(known).await;
    assert_eq!(node.lock().table.len(), 1);
}

fn public(n: u8) -> Ipv4Addr {
    Ipv4Addr::new(1, 2, 3, n)
}

#[test]
fn own_id_follows_the_address_nodes_agree_on() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let known = NodeInfo {
        id: NodeId([2; 20]),
        addr: addr(2),
    };
    state.table.insert(known, now);
    let ours = Ipv4Addr::new(124, 31, 75, 21);
    // A few reports are not enough, however often they come.
    for _ in 0..5 {
        assert_eq!(state.learn_external(public(1), ours, now), None);
    }
    assert_eq!(state.learn_external(public(2), ours, now), None);
    assert_eq!(state.learn_external(public(3), ours, now), None);
    let id = state.learn_external(public(4), ours, now).unwrap();
    assert!(id.is_secure_v4(ours));
    assert_eq!(state.table.own(), id);
    assert_eq!(state.external, Some(ours));
    assert_eq!(state.table.len(), 1, "known nodes are kept");
    // Once the id is valid, more reports change nothing.
    assert_eq!(state.learn_external(public(5), ours, now), None);
    assert_eq!(state.table.own(), id);
}

#[test]
fn a_valid_id_is_kept_and_local_reports_are_ignored() {
    let now = Instant::now();
    let ours = Ipv4Addr::new(124, 31, 75, 21);
    let id = NodeId::secure_v4(ours);
    let mut state = State::new(id, now);
    for n in 1..=8 {
        assert_eq!(state.learn_external(public(n), ours, now), None);
    }
    assert_eq!(state.table.own(), id);

    let mut state = State::new(OWN, now);
    for n in 1..=8 {
        let lan = Ipv4Addr::new(192, 168, 1, 5);
        assert_eq!(state.learn_external(public(n), lan, now), None);
    }
    assert_eq!(state.table.own(), OWN);
    assert_eq!(state.external, None);
}

#[tokio::test]
async fn answers_carry_the_senders_address() {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let dht = Dht::start(socket, Vec::new()).unwrap();
    let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let ping = Message {
        tid: vec![1, 2],
        ip: None,
        body: Body::Query {
            id: NodeId([7; 20]),
            query: Query::Ping,
        },
    };
    client
        .send_to(&ping.encode(), dht.local_addr())
        .await
        .unwrap();
    let mut buf = [0u8; 1500];
    let (len, _) = timeout(Duration::from_secs(5), client.recv_from(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let reply = Message::decode(&buf[..len]).unwrap();
    assert_eq!(reply.ip, Some(client.local_addr().unwrap()));
}

#[test]
fn the_routers_address_counts_at_once_if_it_can_be_public() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    // Not an address the nodes outside could see.
    for ip in [[192, 168, 1, 5], [100, 64, 1, 1], [0, 0, 0, 0]] {
        assert_eq!(state.adopt(Ipv4Addr::from(ip), now), None);
        assert_eq!(state.external, None);
    }
    let ours = Ipv4Addr::new(124, 31, 75, 21);
    let id = state.adopt(ours, now).unwrap();
    assert!(id.is_secure_v4(ours));
    assert_eq!(state.table.own(), id);
    assert_eq!(state.adopt(ours, now), None);
}

#[test]
fn scrape_answers_with_filters_and_noseed_skips_seeds() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let host = |n: u8| sock(Ipv4Addr::new(10, 0, 0, n), 1);
    // Tokens go by address, so each host gets its own.
    for (n, seed) in [(2, true), (3, false), (4, false)] {
        let token = response(ask(&mut state, get_peers(false, false), host(n), now).0)
            .token
            .unwrap();
        let query = Query::AnnouncePeer {
            info_hash: INFO_HASH,
            port: 6881,
            implied_port: false,
            seed,
            token,
        };
        response(ask(&mut state, query, host(n), now).0);
    }

    // No filters unless asked for.
    let plain = response(ask(&mut state, get_peers(false, false), addr(1), now).0);
    assert_eq!((plain.seed_filter, plain.peer_filter), (None, None));
    assert_eq!(plain.values.len(), 3);

    let scraped = response(ask(&mut state, get_peers(false, true), addr(1), now).0);
    let (seeds, others) = (scraped.seed_filter.unwrap(), scraped.peer_filter.unwrap());
    assert_eq!((seeds.count(), others.count()), (1, 2));
    assert!(seeds.contains(host(2).ip()));

    // A seed is sent no seeds.
    let noseed = response(ask(&mut state, get_peers(true, false), addr(1), now).0);
    assert_eq!(noseed.values.len(), 2);
    assert!(!noseed.values.iter().any(|v| v.ip() == host(2).ip()));

    // A torrent nobody announced has nothing to scrape.
    let other = Query::GetPeers {
        want: Want::default(),
        info_hash: [3; 20],
        noseed: false,
        scrape: true,
    };
    let none = response(ask(&mut state, other, addr(1), now).0);
    assert_eq!((none.seed_filter, none.peer_filter), (None, None));
}

#[test]
fn a_lookup_merges_the_filters_into_swarm_counts() {
    let mut seeds = Bloom::default();
    let mut others = Bloom::default();
    for n in 0..30u8 {
        seeds.insert(Ipv4Addr::new(1, 1, 1, n).into());
    }
    for n in 0..70u8 {
        others.insert(Ipv4Addr::new(2, 2, 2, n).into());
    }
    let none = Found {
        peers: HashSet::new(),
        seeds: None,
        others: None,
        closest: Vec::new(),
    };
    assert_eq!(none.swarm(), None);
    // Peers from nodes that sent no filters count too, unless a seed filter
    // has them.
    let found = Found {
        peers: [
            sock(Ipv4Addr::new(1, 1, 1, 5), 1),
            sock(Ipv4Addr::new(3, 3, 3, 3), 1),
        ]
        .into(),
        seeds: Some(seeds),
        others: Some(others),
        closest: Vec::new(),
    };
    let swarm = found.swarm().unwrap();
    // The estimate of a filter is close, not exact; the seed that sits in
    // both lists is not counted twice.
    assert_eq!(swarm.seeds, 30);
    assert!((70..=72).contains(&swarm.leechers), "{swarm:?}");
}

#[test]
fn want_decides_which_nodes_an_answer_names() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let known = NodeInfo {
        id: NodeId([2; 20]),
        addr: addr(2),
    };
    state.table.insert(known, now);
    let target = NodeId([3; 20]);
    let nodes = |state: &mut State, want| {
        let query = Query::FindNode { target, want };
        response(ask(state, query, addr(1), now).0).nodes
    };
    // Not given, or n4: the IPv4 nodes we have, to an IPv4 asker.
    assert_eq!(nodes(&mut state, Want::default()), [known]);
    assert_eq!(
        nodes(
            &mut state,
            Want {
                v4: true,
                v6: false
            }
        ),
        [known]
    );
    assert_eq!(nodes(&mut state, Want::BOTH), [known]);
    // Only n6: we have no IPv6 nodes.
    assert!(
        nodes(
            &mut state,
            Want {
                v4: false,
                v6: true
            }
        )
        .is_empty()
    );
}

#[test]
fn a_peer_of_either_family_may_announce_and_be_found() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let six: SocketAddr = "[2001:db8::7]:5000".parse().unwrap();
    let token = response(ask(&mut state, get_peers(false, false), six, now).0)
        .token
        .unwrap();
    let announce = Query::AnnouncePeer {
        info_hash: INFO_HASH,
        port: 6881,
        implied_port: false,
        seed: true,
        token,
    };
    response(ask(&mut state, announce, six, now).0);
    let found = response(ask(&mut state, get_peers(false, true), addr(1), now).0);
    assert_eq!(
        found.values,
        ["[2001:db8::7]:6881".parse::<SocketAddr>().unwrap()]
    );
    assert_eq!(found.seed_filter.unwrap().count(), 1);
}
