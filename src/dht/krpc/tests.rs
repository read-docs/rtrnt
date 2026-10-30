use super::*;
use crate::dht::bloom;
use std::net::Ipv6Addr;

const ID: &[u8; 20] = b"abcdefghij0123456789";
const OTHER: &[u8; 20] = b"mnopqrstuvwxyz123456";

fn node(byte: u8, port: u16) -> NodeInfo {
    NodeInfo {
        id: NodeId([byte; 20]),
        addr: v4(Ipv4Addr::new(10, 0, 0, byte), port),
    }
}

#[test]
fn values_roundtrip() {
    let data = b"d4:listli-42ei0e3:abce4:zero0:1:ad1:xi1eee";
    let value = Value::decode(data).unwrap();
    let mut out = Vec::new();
    value.encode(&mut out);
    // Keys come out sorted.
    assert_eq!(out, b"d1:ad1:xi1ee4:listli-42ei0e3:abce4:zero0:e");
    assert_eq!(Value::decode(&out).unwrap(), value);
}

#[test]
fn rejects_malformed_bencode() {
    let cases: &[&[u8]] = &[
        b"",
        b"i1",
        b"ie",
        b"i-e",
        b"i-0e",
        b"i03e",
        b"i+3e",
        b"i99999999999999999999e",
        b"5:abc",
        b"99999999999:a",
        b"1x",
        b"l",
        b"d1:a",
        b"di1ei2ee",
        b"i1ei2e",
        b"x",
    ];
    for case in cases {
        assert!(Value::decode(case).is_err(), "accepted {case:?}");
    }
    let deep: Vec<u8> = std::iter::repeat_n(b'l', 10_000).collect();
    assert!(Value::decode(&deep).is_err());
}

/// The example messages from BEP 5.
#[test]
fn decodes_bep_5_examples() {
    let query = |data: &[u8]| match Message::decode(data).unwrap() {
        Message {
            tid,
            body: Body::Query { id, query },
            ..
        } => {
            assert_eq!((tid.as_slice(), id), (b"aa".as_slice(), NodeId(*ID)));
            query
        }
        other => panic!("not a query: {other:?}"),
    };
    assert_eq!(
        query(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"),
        Query::Ping
    );
    assert_eq!(
        query(b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe"),
        Query::FindNode { want: Want::default(), target: NodeId(*OTHER) }
    );
    assert_eq!(
        query(b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"),
        Query::GetPeers {
            want: Want::default(),
            info_hash: *OTHER,
            noseed: false,
            scrape: false
        }
    );
    assert_eq!(
        query(b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe"),
        Query::AnnouncePeer {
            info_hash: *OTHER,
            port: 6881,
            implied_port: true,
            seed: false,
            token: b"aoeusnth".to_vec(),
        }
    );

    let response = Message::decode(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re").unwrap();
    assert_eq!(
        response.body,
        Body::Response(Response {
            id: NodeId(*OTHER),
            ..Response::default()
        })
    );
    let peers = Message::decode(b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re").unwrap();
    let Body::Response(peers) = peers.body else {
        panic!("not a response");
    };
    assert_eq!(peers.token.as_deref(), Some(b"aoeusnth".as_slice()));
    assert_eq!(
        peers.values,
        [
            v4(Ipv4Addr::new(97, 120, 106, 101), 0x2e75),
            v4(Ipv4Addr::new(105, 100, 104, 116), 0x6e6d),
        ]
    );

    let error = Message::decode(b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee").unwrap();
    assert_eq!(
        error.body,
        Body::Error {
            code: 201,
            message: "A Generic Error Ocurred".into()
        }
    );
}

#[test]
fn messages_roundtrip() {
    let id = NodeId(*ID);
    let bodies = [
        Body::Query {
            id,
            query: Query::Ping,
        },
        Body::Query {
            id,
            query: Query::FindNode {
                want: Want::default(),
                target: NodeId(*OTHER),
            },
        },
        Body::Query {
            id,
            query: Query::GetPeers {
                want: Want::default(),
                info_hash: *OTHER,
                noseed: false,
                scrape: false,
            },
        },
        Body::Query {
            id,
            query: Query::GetPeers {
                want: Want::default(),
                info_hash: *OTHER,
                noseed: true,
                scrape: true,
            },
        },
        Body::Query {
            id,
            query: Query::AnnouncePeer {
                info_hash: *OTHER,
                port: 51413,
                implied_port: false,
                seed: false,
                token: vec![1, 2, 3],
            },
        },
        Body::Query {
            id,
            query: Query::AnnouncePeer {
                info_hash: *OTHER,
                port: 51413,
                implied_port: true,
                seed: true,
                token: vec![1, 2, 3],
            },
        },
        Body::Query {
            id,
            query: Query::Unknown(b"vote".to_vec()),
        },
        Body::Response(Response {
            id,
            nodes: vec![node(1, 6881), node(2, 1)],
            values: vec![v4(Ipv4Addr::new(1, 2, 3, 4), 80)],
            token: Some(b"tok".to_vec()),
            seed_filter: Some(Bloom::from_bytes(&[0x55; bloom::BYTES]).unwrap()),
            peer_filter: Some(Bloom::default()),
        }),
        Body::Response(Response {
            id,
            ..Response::default()
        }),
        Body::Error {
            code: ERROR_PROTOCOL,
            message: "bad token".into(),
        },
    ];
    for body in bodies {
        let message = Message {
            tid: vec![0, 7],
            ip: None,
            body,
        };
        let encoded = message.encode();
        assert_eq!(Message::decode(&encoded).unwrap(), message, "{encoded:?}");
    }
}

#[test]
fn implied_port_needs_no_port() {
    let data = b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234565:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe";
    let Body::Query { query, .. } = Message::decode(data).unwrap().body else {
        panic!("not a query");
    };
    assert!(matches!(
        query,
        Query::AnnouncePeer {
            port: 0,
            implied_port: true,
            ..
        }
    ));
}

#[test]
fn rejects_malformed_messages() {
    let long_token = format!(
        "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti1e5:token65:{}e1:q13:announce_peer1:t2:aa1:y1:qe",
        "x".repeat(65)
    );
    let cases: &[&[u8]] = &[
        b"le",
        b"d1:t2:aa1:y1:qe",
        b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:y1:qe",
        b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t17:aaaaaaaaaaaaaaaaa1:y1:qe",
        b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:xe",
        b"d1:ad2:id20:abcdefghij0123456789e1:q9:find_node1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe",
        b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti70000e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe",
        long_token.as_bytes(),
        b"d1:rd2:id20:mnopqrstuvwxyz1234565:nodes3:abce1:t2:aa1:y1:re",
        b"d1:rd2:id20:mnopqrstuvwxyz1234566:valuesi1ee1:t2:aa1:y1:re",
        b"d1:t2:aa1:y1:ee",
    ];
    for case in cases {
        assert!(
            Message::decode(case).is_err(),
            "accepted {}",
            String::from_utf8_lossy(case)
        );
    }
}

#[test]
fn compact_formats() {
    let nodes = [node(1, 6881), node(200, 65535)];
    let bytes = encode_nodes(&nodes);
    assert_eq!(bytes.len(), 52);
    assert_eq!(&bytes[20..26], &[10, 0, 0, 1, 0x1a, 0xe1]);
    assert_eq!(decode_nodes(&bytes).unwrap(), nodes);
    assert!(decode_nodes(&bytes[..51]).is_err());
    assert!(decode_nodes(&[]).unwrap().is_empty());

    let peer = v4(Ipv4Addr::new(127, 0, 0, 1), 6881);
    assert_eq!(decode_addr(&encode_addr(peer)), Some(peer));
    // Neither 6 nor 18 bytes.
    assert_eq!(decode_addr(&[0; 10]), None);
    assert_eq!(decode_addr(&[]), None);
}

fn v6(last: u16, port: u16) -> SocketAddr {
    SocketAddr::new(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, last).into(),
        port,
    )
}

fn node6(byte: u8, port: u16) -> NodeInfo {
    NodeInfo {
        id: NodeId([byte; 20]),
        addr: v6(u16::from(byte), port),
    }
}

/// BEP 32: IPv6 nodes in `nodes6` (38 bytes each), next to `nodes`, and
/// peers of both families in `values`.
#[test]
fn ipv6_nodes_and_peers() {
    let nodes = [node6(1, 6881), node(2, 1), node6(3, 65535)];
    let response = Response {
        id: NodeId(*ID),
        nodes: nodes.to_vec(),
        values: vec![v4(Ipv4Addr::new(1, 2, 3, 4), 80), v6(9, 443)],
        ..Response::default()
    };
    let message = Message {
        tid: vec![1],
        ip: None,
        body: Body::Response(response),
    };
    let encoded = message.encode();
    let Value::Dict(dict) = Value::decode(&encoded).unwrap() else {
        panic!("not a dictionary");
    };
    let r = dict_at(&dict, b"r").unwrap();
    assert_eq!(bytes(r, b"nodes").unwrap().len(), 26);
    assert_eq!(bytes(r, b"nodes6").unwrap().len(), 2 * 38);
    // Families come out grouped, IPv4 first.
    let Body::Response(decoded) = Message::decode(&encoded).unwrap().body else {
        panic!("not a response");
    };
    assert_eq!(decoded.nodes, [nodes[1], nodes[0], nodes[2]]);
    assert_eq!(decoded.values.len(), 2);
    assert!(decoded.values.contains(&v6(9, 443)));

    let bytes6 = encode_nodes(&[node6(1, 6881)]);
    assert_eq!(bytes6.len(), 38);
    assert_eq!(&bytes6[20..], encode_addr(v6(1, 6881)));
    assert_eq!(decode_nodes6(&bytes6).unwrap(), [node6(1, 6881)]);
    // The wrong size for the key is an error, not a misread.
    assert!(decode_nodes6(&bytes6[..37]).is_err());
    assert!(decode_nodes(&bytes6).is_err());
}

#[test]
fn a_malformed_nodes6_is_an_error() {
    let data = b"d1:rd2:id20:abcdefghij01234567896:nodes63:abce1:t2:aa1:y1:re";
    assert!(Message::decode(data).is_err());
}

#[test]
fn want_names_the_families() {
    let query = |extra: &[u8]| {
        let mut data = b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456".to_vec();
        data.extend_from_slice(extra);
        data.extend_from_slice(b"e1:q9:find_node1:t2:aa1:y1:qe");
        match Message::decode(&data).unwrap().body {
            Body::Query {
                query: Query::FindNode { want, .. },
                ..
            } => want,
            other => panic!("{other:?}"),
        }
    };
    assert_eq!(query(b""), Want::default());
    assert_eq!(
        query(b"4:wantl2:n4e"),
        Want {
            v4: true,
            v6: false
        }
    );
    assert_eq!(query(b"4:wantl2:n62:n4e"), Want::BOTH);
    assert_eq!(
        query(b"4:wantl2:n6e"),
        Want {
            v4: false,
            v6: true
        }
    );
    // Unknown names and a wrong type are as good as none.
    assert_eq!(query(b"4:wantl2:n8e"), Want::default());
    assert_eq!(query(b"4:want2:n4"), Want::default());

    let args = |want| {
        query_args(
            &NodeId(*ID),
            &Query::FindNode {
                target: NodeId(*OTHER),
                want,
            },
        )
    };
    assert!(!args(Want::default()).contains_key(b"want".as_slice()));
    let both = args(Want::BOTH);
    assert_eq!(
        both.get(b"want".as_slice()),
        Some(&Value::List(vec![
            Value::Bytes(b"n4".to_vec()),
            Value::Bytes(b"n6".to_vec())
        ]))
    );
}

#[test]
fn want_selects_nodes_by_family() {
    let v4 = IpAddr::from([1, 2, 3, 4]);
    let v6 = IpAddr::from(Ipv6Addr::LOCALHOST);
    // Not given: the family the query came over.
    assert!(Want::default().includes(v4, v4));
    assert!(!Want::default().includes(v6, v4));
    assert!(Want::default().includes(v6, v6));
    // Given: exactly what is named, whatever it came over.
    let n6 = Want {
        v4: false,
        v6: true,
    };
    assert!(n6.includes(v6, v4) && !n6.includes(v4, v4));
    assert!(Want::BOTH.includes(v4, v6) && Want::BOTH.includes(v6, v6));
}

#[test]
fn caps_nodes_and_values_per_response() {
    let nodes: Vec<NodeInfo> = (0..100).map(|i| node(i, 1)).collect();
    let response = Response {
        id: NodeId(*ID),
        nodes,
        values: vec![v4(Ipv4Addr::LOCALHOST, 1); 500],
        token: None,
        seed_filter: None,
        peer_filter: None,
    };
    let message = Message {
        tid: vec![1],
        ip: None,
        body: Body::Response(response),
    };
    let Body::Response(decoded) = Message::decode(&message.encode()).unwrap().body else {
        panic!("not a response");
    };
    assert_eq!(
        (decoded.nodes.len(), decoded.values.len()),
        (MAX_NODES, MAX_VALUES)
    );
}

#[test]
fn carries_our_address_as_top_level_ip() {
    let message = |ip| Message {
        tid: vec![1],
        ip,
        body: Body::Response(Response::default()),
    };
    let v4 = Some(SocketAddr::from(([1, 2, 3, 4], 6881)));
    let v6 = Some(SocketAddr::from((Ipv6Addr::LOCALHOST, 6881)));
    for ip in [None, v4, v6] {
        let encoded = message(ip).encode();
        assert_eq!(Message::decode(&encoded).unwrap(), message(ip));
    }
    // The BEP's compact form, in the top-level dictionary.
    let encoded = message(v4).encode();
    let needle = b"2:ip6:\x01\x02\x03\x04\x1a\xe1";
    assert!(encoded.windows(needle.len()).any(|w| w == needle));
    // A malformed one is dropped, not an error.
    let data = b"d2:ip3:abc1:rd2:id20:abcdefghij0123456789e1:t2:aa1:y1:re";
    assert_eq!(Message::decode(data).unwrap().ip, None);
}

/// BEP 33's keys are plain integers and 256-byte strings.
#[test]
fn bep_33_fields_on_the_wire() {
    let query = |extra: &[u8]| {
        let mut data =
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456".to_vec();
        data.extend_from_slice(extra);
        data.extend_from_slice(b"e1:q9:get_peers1:t2:aa1:y1:qe");
        match Message::decode(&data).unwrap().body {
            Body::Query { query, .. } => query,
            other => panic!("not a query: {other:?}"),
        }
    };
    let flags = |query| match query {
        Query::GetPeers { noseed, scrape, .. } => (noseed, scrape),
        other => panic!("{other:?}"),
    };
    assert_eq!(flags(query(b"")), (false, false));
    assert_eq!(flags(query(b"6:noseedi1e6:scrapei1e")), (true, true));
    assert_eq!(flags(query(b"6:noseedi0e6:scrape3:abc")), (false, false));

    let filter = vec![0xaa; bloom::BYTES];
    let mut data = b"d1:rd4:BFpe256:".to_vec();
    data.extend_from_slice(&filter);
    data.extend_from_slice(b"4:BFsd3:abc2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re");
    let Body::Response(response) = Message::decode(&data).unwrap().body else {
        panic!("not a response");
    };
    // The one of the right size is kept, the other dropped.
    assert_eq!(response.peer_filter.unwrap().as_bytes().as_slice(), filter);
    assert_eq!(response.seed_filter, None);

    let announce = |seed| Query::AnnouncePeer {
        info_hash: *OTHER,
        port: 1,
        implied_port: false,
        seed,
        token: vec![1],
    };
    let args = query_args(&NodeId(*ID), &announce(true));
    assert_eq!(args.get(b"seed".as_slice()), Some(&Value::Int(1)));
    assert!(!query_args(&NodeId(*ID), &announce(false)).contains_key(b"seed".as_slice()));
    // Only 1 means a seed.
    let mut data = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti1e4:seedi2e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe".to_vec();
    let seed = |data: &[u8]| match Message::decode(data).unwrap().body {
        Body::Query {
            query: Query::AnnouncePeer { seed, .. },
            ..
        } => seed,
        other => panic!("{other:?}"),
    };
    assert!(!seed(&data));
    let at = data.windows(7).position(|w| w == b"4:seedi").unwrap() + 7;
    data[at] = b'1';
    assert!(seed(&data));
}

fn v4(ip: Ipv4Addr, port: u16) -> SocketAddr {
    SocketAddr::new(ip.into(), port)
}
