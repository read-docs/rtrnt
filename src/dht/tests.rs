use super::*;

const OWN: NodeId = NodeId([0; 20]);
const INFO_HASH: [u8; 20] = [9; 20];

fn addr(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), port)
}

fn ask(state: &mut State, query: Query, from: SocketAddrV4, now: Instant) -> (Body, bool) {
    answer(state, OWN, NodeId([1; 20]), query, from, now)
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
    let (body, _) = ask(&mut state, Query::FindNode { target }, addr(1), now);
    assert_eq!(response(body).nodes, [known]);

    // A known node is not pinged again.
    let (_, verify) = answer(&mut state, OWN, known.id, Query::Ping, known.addr, now);
    assert!(!verify);
}

#[test]
fn announce_needs_a_token_from_get_peers() {
    let now = Instant::now();
    let mut state = State::new(OWN, now);
    let (body, _) = ask(
        &mut state,
        Query::GetPeers {
            info_hash: INFO_HASH,
        },
        addr(1),
        now,
    );
    let reply = response(body);
    assert!(reply.values.is_empty());
    let token = reply.token.unwrap();

    let announce = |port, implied_port, token: &[u8]| Query::AnnouncePeer {
        info_hash: INFO_HASH,
        port,
        implied_port,
        token: token.to_vec(),
    };
    // From another address, or with a made-up token: refused.
    let other_ip = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 2), 1);
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
    let get_peers = |state: &mut State| {
        let query = Query::GetPeers {
            info_hash: INFO_HASH,
        };
        response(ask(state, query, addr(1), now).0).values
    };
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
