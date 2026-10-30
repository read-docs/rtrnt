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
    assert_eq!(state.external, Some(ours.into()));
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
    let other_family = response(ask(&mut state, get_peers(false, true), addr(1), now).0);
    assert!(
        other_family.values.is_empty(),
        "values stay in the querying family"
    );
    let found = response(ask(&mut state, get_peers(false, true), six, now).0);
    assert_eq!(
        found.values,
        ["[2001:db8::7]:6881".parse::<SocketAddr>().unwrap()]
    );
    assert_eq!(found.seed_filter.unwrap().count(), 1);
}

#[tokio::test]
async fn dual_stack_bootstraps_ipv6_through_ipv4_and_announces() {
    let Ok(router6) = bind_ipv6(Ipv6Addr::LOCALHOST, 0) else {
        return;
    };
    let router_port = router6.local_addr().unwrap().port();
    let router4 = UdpSocket::bind((Ipv4Addr::LOCALHOST, router_port))
        .await
        .unwrap();
    let router = Dht::start_sockets(router4, Some(router6), Vec::new()).unwrap();
    let peer_socket = bind_ipv6(Ipv6Addr::LOCALHOST, 0).unwrap();
    let peer = Dht::start(peer_socket, Vec::new()).unwrap();
    let peer_addr = peer.local_addr();
    router.0.node6.as_ref().unwrap().lock().table.insert(
        NodeInfo {
            id: NodeId(peer.node_id()),
            addr: peer_addr,
        },
        Instant::now(),
    );
    let advertised = sock(Ipv6Addr::LOCALHOST, 54321);
    peer.0
        .node
        .lock()
        .store
        .announce(INFO_HASH, advertised, true, Instant::now());

    let client6 = bind_ipv6(Ipv6Addr::LOCALHOST, 0).unwrap();
    let client_port = client6.local_addr().unwrap().port();
    let client4 = UdpSocket::bind((Ipv4Addr::LOCALHOST, client_port))
        .await
        .unwrap();
    // There is deliberately no IPv6 bootstrap router in this list.
    let client = Dht::start_sockets(
        client4,
        Some(client6),
        vec![router.local_addr().to_string()],
    )
    .unwrap();
    let found = timeout(
        Duration::from_secs(5),
        client.lookup_peers(INFO_HASH, Some(client_port), false),
    )
    .await
    .unwrap();
    assert!(found.peers.contains(&advertised));
    let node6 = client.0.node6.as_ref().unwrap();
    assert!(!node6.lock().table.is_empty());
    assert!(
        client
            .0
            .node
            .lock()
            .table
            .closest(&OWN, K)
            .iter()
            .all(|n| n.addr.is_ipv4())
    );
    assert!(
        node6
            .lock()
            .table
            .closest(&OWN, K)
            .iter()
            .all(|n| n.addr.is_ipv6())
    );
    let stored = peer
        .0
        .node
        .lock()
        .store
        .get(&INFO_HASH, false, Instant::now());
    assert!(
        stored.contains(&node6.addr),
        "IPv6 announce uses the IPv6 socket and shared port"
    );
}

#[tokio::test]
async fn ipv6_port_message_verifies_a_node() {
    let Ok(socket) = bind_ipv6(Ipv6Addr::LOCALHOST, 0) else {
        return;
    };
    let peer = Dht::start(socket, Vec::new()).unwrap();
    let client6 = bind_ipv6(Ipv6Addr::LOCALHOST, 0).unwrap();
    let client4 = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let client = Dht::start_sockets(client4, Some(client6), Vec::new()).unwrap();
    client.add_node(peer.local_addr());
    timeout(Duration::from_secs(3), async {
        while client.node_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(client.0.node.lock().table.len(), 0);
    assert_eq!(client.0.node6.as_ref().unwrap().lock().table.len(), 1);
}

#[tokio::test]
async fn unavailable_ipv6_port_keeps_ipv4_working() {
    let Ok(blocker) = bind_ipv6(Ipv6Addr::UNSPECIFIED, 0) else {
        return;
    };
    let port = blocker.local_addr().unwrap().port();
    let dht = Dht::bind(port).await.unwrap();
    assert_eq!(dht.port(), port);
    assert_eq!(dht.local_addr_v6(), None);
    let peer = Dht::start(
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap(),
        Vec::new(),
    )
    .unwrap();
    assert!(
        dht.0
            .node
            .query(peer.local_addr(), Query::Ping)
            .await
            .is_ok()
    );
}

#[test]
fn ipv6_id_adopts_only_same_family_votes() {
    let mut state = State::new(OWN, Instant::now());
    let ours: IpAddr = "2a01:4f8:1c1e:9a42::1".parse().unwrap();
    for n in 1..=5 {
        assert_eq!(state.learn_external(public(n), ours, Instant::now()), None);
    }
    for n in 1..=4 {
        let reporter: IpAddr = format!("2606:4700::{n}").parse().unwrap();
        state.learn_external(reporter, ours, Instant::now());
    }
    assert_eq!(state.external, Some(ours));
    assert!(state.table.own().is_secure(ours));
}

#[test]
fn large_ipv6_answers_fit_bep32_packet_limit() {
    let reply = Message {
        tid: vec![1, 2],
        ip: None,
        body: Body::Response(Response {
            id: OWN,
            values: (1..=50)
                .map(|port| sock(Ipv6Addr::LOCALHOST, port))
                .collect(),
            nodes: (1..=K)
                .map(|port| NodeInfo {
                    id: NodeId([port as u8; 20]),
                    addr: sock(Ipv6Addr::LOCALHOST, port as u16),
                })
                .collect(),
            seed_filter: Some(Bloom::default()),
            peer_filter: Some(Bloom::default()),
            ..Response::default()
        }),
    };
    let packet = bounded_reply(reply).unwrap();
    assert!(packet.len() <= 1024);
    let decoded = Message::decode(&packet).unwrap();
    let response = response(decoded.body);
    assert!(response.seed_filter.is_some());
    assert!(!response.values.is_empty());
}

#[tokio::test]
async fn an_oversized_remote_token_is_not_sent_back() {
    let dht = Dht::start(
        UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap(),
        Vec::new(),
    )
    .unwrap();
    let remote = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let result = dht
        .0
        .node
        .query(
            remote.local_addr().unwrap(),
            Query::AnnouncePeer {
                info_hash: INFO_HASH,
                port: 6881,
                implied_port: false,
                seed: false,
                token: vec![0; 1024],
            },
        )
        .await;
    assert!(matches!(result, Err(QueryError::Local(_))));
    assert!(dht.0.node.lock().pending.is_empty());
    let mut buf = [0; 2048];
    assert_eq!(
        remote.try_recv_from(&mut buf).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[tokio::test]
async fn binding_policy_selects_the_dht_source_family_and_refuses_proxy_udp() {
    let ipv4 = crate::network::Policy::new(Some(Ipv4Addr::LOCALHOST.into()), None, None).unwrap();
    let node = Dht::bind_with_policy(0, &ipv4).await.unwrap();
    assert_eq!(node.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(node.local_addr_v6(), None);
    let ipv6 = crate::network::Policy::new(Some(Ipv6Addr::LOCALHOST.into()), None, None).unwrap();
    if let Ok(probe) = UdpSocket::bind("[::1]:0").await {
        drop(probe);
        let node = Dht::bind_with_policy(0, &ipv6).await.unwrap();
        assert_eq!(node.local_addr().ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(node.local_addr_v6(), Some(node.local_addr()));
    }
    let proxied =
        crate::network::Policy::new(None, None, Some("socks5h://127.0.0.1:1080")).unwrap();
    assert!(Dht::bind_with_policy(0, &proxied).await.is_err());
}
