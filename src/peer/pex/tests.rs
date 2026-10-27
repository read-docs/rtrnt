use super::*;

fn v4(port: u16) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, 1], port))
}

fn v6(port: u16) -> SocketAddr {
    SocketAddr::new("2001:db8::1".parse().unwrap(), port)
}

#[test]
fn encodes_each_family_in_its_own_keys() {
    let message = PexMessage {
        added: vec![
            (v4(0x1ae1), FLAG_SEED),
            (v6(2), FLAG_CONNECTABLE),
            ("[::ffff:10.0.0.2]:3".parse().unwrap(), 0),
        ],
        dropped: vec![v6(4), v4(5)],
    };
    let payload = message.encode().unwrap();
    let mut expected = b"d5:added12:\x0a\x00\x00\x01\x1a\xe1\x0a\x00\x00\x02\x00\x03".to_vec();
    expected.extend_from_slice(b"7:added.f2:\x02\x00");
    expected.extend_from_slice(b"6:added618:");
    expected.extend_from_slice(&v6_bytes(2));
    expected.extend_from_slice(b"8:added6.f1:\x10");
    expected.extend_from_slice(b"7:dropped6:\x0a\x00\x00\x01\x00\x05");
    expected.extend_from_slice(b"8:dropped618:");
    expected.extend_from_slice(&v6_bytes(4));
    expected.push(b'e');
    assert_eq!(payload, expected);

    let decoded = PexMessage::decode(&payload).unwrap();
    assert_eq!(
        decoded.added,
        [
            (v4(0x1ae1), FLAG_SEED),
            ("10.0.0.2:3".parse().unwrap(), 0),
            (v6(2), FLAG_CONNECTABLE),
        ]
    );
    assert_eq!(decoded.dropped, [v4(5), v6(4)]);
}

fn v6_bytes(port: u16) -> Vec<u8> {
    let SocketAddr::V6(addr) = v6(port) else {
        unreachable!()
    };
    let mut bytes = addr.ip().octets().to_vec();
    bytes.extend_from_slice(&port.to_be_bytes());
    bytes
}

#[test]
fn an_empty_message_has_every_key() {
    let message = PexMessage::default();
    assert!(message.is_empty());
    let payload = message.encode().unwrap();
    assert_eq!(
        payload,
        b"d5:added0:7:added.f0:6:added60:8:added6.f0:7:dropped0:8:dropped60:e"
    );
    assert_eq!(PexMessage::decode(&payload).unwrap(), message);
}

#[test]
fn decoding_is_lenient() {
    // No keys at all, wrong types, unknown keys and trailing bytes.
    assert_eq!(PexMessage::decode(b"de").unwrap(), PexMessage::default());
    let payload = b"d5:addedi5e7:dropped3:abc1:xi1eeextra";
    assert_eq!(PexMessage::decode(payload).unwrap(), PexMessage::default());
    // A partial address at the end, flags missing for the second peer,
    // and flags for a peer that is not there.
    let payload = b"d5:added14:\x0a\x00\x00\x01\x00\x01\x0a\x00\x00\x01\x00\x02\xff\xff\
        7:added.f1:\x028:added6.f3:\x01\x01\x01e";
    let message = PexMessage::decode(payload).unwrap();
    assert_eq!(message.added, [(v4(1), FLAG_SEED), (v4(2), 0)]);
    assert!(message.dropped.is_empty());
}

#[test]
fn rejects_payloads_that_are_not_a_dictionary() {
    let cases: &[&[u8]] = &[b"", b"le", b"i1e", b"5:added", b"d5:added", b"\xff"];
    for case in cases {
        assert!(PexMessage::decode(case).is_err(), "accepted {case:?}");
    }
    let mut deep = b"d1:x".to_vec();
    deep.extend(std::iter::repeat_n(b'l', 100_000));
    assert!(PexMessage::decode(&deep).is_err());
}

#[test]
fn first_message_lists_everyone_and_later_ones_what_changed() {
    let mut sent = BTreeSet::new();
    let mut current: BTreeMap<SocketAddr, u8> =
        [(v4(1), 0), (v6(2), FLAG_SEED)].into_iter().collect();
    let first = next_message(&mut sent, &current);
    assert_eq!(first.added, [(v4(1), 0), (v6(2), FLAG_SEED)]);
    assert!(first.dropped.is_empty());
    assert!(next_message(&mut sent, &current).is_empty());

    current.remove(&v4(1));
    current.insert(v4(3), FLAG_CONNECTABLE);
    let second = next_message(&mut sent, &current);
    assert_eq!(second.added, [(v4(3), FLAG_CONNECTABLE)]);
    assert_eq!(second.dropped, [v4(1)]);
    assert_eq!(sent, [v4(3), v6(2)].into());
    // A flag that changes does not make the peer news.
    current.insert(v4(3), FLAG_CONNECTABLE | FLAG_SEED);
    assert!(next_message(&mut sent, &current).is_empty());
}

#[test]
fn messages_hold_at_most_fifty_added_and_fifty_dropped() {
    let mut sent = BTreeSet::new();
    let mut current: BTreeMap<SocketAddr, u8> = (1..=120).map(|port| (v4(port), 0)).collect();
    let sizes = |message: &PexMessage| (message.added.len(), message.dropped.len());
    assert_eq!(sizes(&next_message(&mut sent, &current)), (50, 0));
    assert_eq!(sizes(&next_message(&mut sent, &current)), (50, 0));
    current.clear();
    // The 100 the peer heard of are dropped, 50 at a time. It never
    // heard of the other 20.
    let message = next_message(&mut sent, &current);
    assert_eq!(sizes(&message), (0, 50));
    assert_eq!(message.dropped[0], v4(1));
    assert_eq!(sizes(&next_message(&mut sent, &current)), (0, 50));
    assert!(next_message(&mut sent, &current).is_empty());
    assert!(sent.is_empty());
}

#[test]
fn takes_at_most_fifty_plausible_peers_other_than_the_sender() {
    let sender = v4(9);
    let mut added = vec![
        (sender, 0),
        (v4(0), 0),
        ("0.0.0.0:1".parse().unwrap(), 0),
        ("224.0.0.1:1".parse().unwrap(), 0),
        ("255.255.255.255:1".parse().unwrap(), 0),
        ("[::]:1".parse().unwrap(), 0),
        ("[::ffff:10.0.0.1]:7".parse().unwrap(), 0),
        (v6(8), FLAG_SEED),
    ];
    added.extend((100..200).map(|port| (v4(port), 0)));
    let message = PexMessage {
        added,
        dropped: vec![v4(10)],
    };
    let peers = worth_trying(&message, false, &[sender]);
    // Only the first 50 entries are looked at.
    assert_eq!(peers.len(), 2 + 42);
    assert_eq!(peers[..2], [v4(7), v6(8)]);
    assert_eq!(peers[2], v4(100));
    // A seed is no use to us once we are one.
    let peers = worth_trying(&message, true, &[sender]);
    assert_eq!(peers[..2], [v4(7), v4(100)]);
}

#[tokio::test]
async fn ignores_messages_that_come_too_often() {
    let (exchange, mut learned) = PeerExchange::new(true);
    let mut peer = exchange.join([1; 20], v4(9), true);
    let message = |port| {
        PexMessage {
            added: vec![(v4(port), 0)],
            dropped: Vec::new(),
        }
        .encode()
        .unwrap()
    };
    let start = Instant::now();
    peer.received(&message(1), start, false).unwrap();
    assert_eq!(learned.try_recv().unwrap(), [v4(1)]);
    let soon = start + MIN_RECEIVE_INTERVAL - Duration::from_secs(1);
    peer.received(&message(2), soon, false).unwrap();
    assert!(learned.try_recv().is_err());
    peer.received(&message(3), start + MIN_RECEIVE_INTERVAL, false)
        .unwrap();
    assert_eq!(learned.try_recv().unwrap(), [v4(3)]);
    // Malformed messages are a protocol violation, even when too soon.
    let err = peer.received(b"le", start, false).unwrap_err();
    assert!(err.is::<ProtocolViolation>(), "{err:#}");
}

#[tokio::test]
async fn advertises_connected_peers_with_known_addresses_only() {
    let (exchange, _learned) = PeerExchange::new(true);
    let outgoing = exchange.join([1; 20], v4(1), true);
    let mut incoming = exchange.join([2; 20], v4(50_000), false);
    let mut target = exchange.join([3; 20], v6(3), false);
    // An incoming peer is only advertised once its listening port is
    // known.
    assert_eq!(
        exchange.others(&[3; 20], &target.own_addrs()),
        [(v4(1), FLAG_CONNECTABLE)].into()
    );
    incoming.advertise(Some(v4(2)), FLAG_SEED);
    target.advertise(Some(v6(4)), 0);
    assert_eq!(
        exchange.others(&[3; 20], &target.own_addrs()),
        [(v4(1), FLAG_CONNECTABLE), (v4(2), FLAG_SEED)].into()
    );
    // Never a peer's own address, even under another connection.
    assert_eq!(
        exchange.others(&[9; 20], &[v4(2), v6(4)]),
        [(v4(1), FLAG_CONNECTABLE)].into()
    );
    drop(outgoing);
    assert_eq!(
        exchange.others(&[3; 20], &target.own_addrs()),
        [(v4(2), FLAG_SEED)].into()
    );
}

#[tokio::test]
async fn private_torrents_never_exchange_peers() {
    let (exchange, mut learned) = PeerExchange::new(false);
    let mut peer = exchange.join([1; 20], v4(9), true);
    let _other = exchange.join([2; 20], v4(1), true);
    assert!(exchange.others(&[9; 20], &[]).is_empty());
    let message = PexMessage {
        added: vec![(v4(1), 0)],
        dropped: Vec::new(),
    };
    peer.received(&message.encode().unwrap(), Instant::now(), false)
        .unwrap();
    // Not even parsed.
    peer.received(b"le", Instant::now(), false).unwrap();
    assert!(learned.try_recv().is_err());
}
