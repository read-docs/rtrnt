use super::*;

fn addr(n: u8) -> SocketAddr {
    SocketAddr::from(([192, 0, 2, n], 6881))
}

#[test]
fn exact_ipv4_and_ipv6_wire_payloads_round_trip() {
    let message = Message {
        endpoint: addr(1),
        kind: Kind::Rendezvous,
    };
    assert_eq!(
        message.encode(),
        [0, 0, 192, 0, 2, 1, 0x1a, 0xe1, 0, 0, 0, 0]
    );
    for endpoint in [addr(1), "[2001:db8::1]:6881".parse().unwrap()] {
        for kind in [
            Kind::Rendezvous,
            Kind::Connect,
            Kind::Error(NO_SUPPORT),
            Kind::Error(999),
        ] {
            let message = Message { endpoint, kind };
            assert_eq!(Message::decode(&message.encode()).unwrap(), message);
        }
    }
}

#[test]
fn malformed_payloads_are_rejected_without_panics() {
    let bytes = Message {
        endpoint: addr(1),
        kind: Kind::Connect,
    }
    .encode();
    for len in 0..bytes.len() {
        assert!(Message::decode(&bytes[..len]).is_err());
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(Message::decode(&extra).is_err());
    for (position, value) in [(0, 3), (1, 2), (11, 1)] {
        let mut bad = bytes.clone();
        bad[position] = value;
        assert!(Message::decode(&bad).is_err());
    }
}

#[test]
fn relay_tells_both_negotiated_peers_to_connect_to_each_other() {
    let (hub, _) = Hub::new(true);
    let mut a = hub.join([1; 20]);
    let mut b = hub.join([2; 20]);
    a.update(Some(addr(1)), true, None);
    b.update(Some(addr(2)), true, None);
    a.received(
        Message {
            endpoint: addr(2),
            kind: Kind::Rendezvous,
        },
        Instant::now(),
    );
    assert_eq!(
        a.try_next(),
        Some(Message {
            endpoint: addr(2),
            kind: Kind::Connect
        })
    );
    assert_eq!(
        b.try_next(),
        Some(Message {
            endpoint: addr(1),
            kind: Kind::Connect
        })
    );
}

#[test]
fn relay_reports_errors_with_the_original_endpoint() {
    let (hub, _) = Hub::new(true);
    let mut a = hub.join([1; 20]);
    let b = hub.join([2; 20]);
    a.update(Some(addr(1)), true, Some(addr(3)));
    b.update(Some(addr(2)), false, None);
    let now = Instant::now();
    for (n, endpoint, code) in [
        (0, addr(2), NO_SUPPORT),
        (1, addr(3), NO_SELF),
        (2, addr(4), NOT_CONNECTED),
        (3, "0.0.0.0:0".parse().unwrap(), NO_SUCH_PEER),
    ] {
        a.received(
            Message {
                endpoint,
                kind: Kind::Rendezvous,
            },
            now + RELAY_INTERVAL * n,
        );
        assert_eq!(
            a.try_next(),
            Some(Message {
                endpoint,
                kind: Kind::Error(code)
            })
        );
    }
}

#[test]
fn failed_pex_peer_requests_its_relay_once_until_timeout() {
    let (hub, _) = Hub::new(true);
    let mut relay = hub.join([1; 20]);
    relay.update(Some(addr(1)), true, None);
    let now = Instant::now();
    relay.learned([addr(2)], now);
    hub.failed(addr(2), now);
    hub.failed(addr(2), now);
    assert_eq!(
        relay.try_next(),
        Some(Message {
            endpoint: addr(2),
            kind: Kind::Rendezvous
        })
    );
    assert_eq!(relay.try_next(), None);
    hub.failed(addr(2), now + ATTEMPT_LIFETIME);
    assert!(relay.try_next().is_some());
    drop(relay);
    assert!(hub.lock().known.is_empty());
}

#[test]
fn connect_requests_are_bounded_deduplicated_and_expire() {
    let (hub, mut connections) = Hub::new(true);
    let relay = hub.join([1; 20]);
    relay.update(Some(addr(1)), true, None);
    let now = Instant::now();
    for n in 2..100 {
        relay.received(
            Message {
                endpoint: addr(n),
                kind: Kind::Connect,
            },
            now,
        );
    }
    assert_eq!(hub.lock().connecting.len(), MAX_PENDING);
    for _ in 0..MAX_PENDING {
        assert!(connections.try_recv().is_ok());
    }
    assert!(connections.try_recv().is_err());
    relay.received(
        Message {
            endpoint: addr(2),
            kind: Kind::Connect,
        },
        now,
    );
    assert!(connections.try_recv().is_err());
    relay.received(
        Message {
            endpoint: addr(2),
            kind: Kind::Connect,
        },
        now + ATTEMPT_LIFETIME,
    );
    assert_eq!(connections.try_recv().unwrap(), addr(2));
}

#[test]
fn disabled_or_unnegotiated_peers_cannot_trigger_udp_or_relay_commands() {
    for enabled in [false, true] {
        let (hub, mut connections) = Hub::new(enabled);
        let mut relay = hub.join([1; 20]);
        relay.update(Some(addr(1)), false, None);
        relay.learned([addr(2)], Instant::now());
        relay.received(
            Message {
                endpoint: addr(2),
                kind: Kind::Connect,
            },
            Instant::now(),
        );
        hub.failed(addr(2), Instant::now());
        assert!(connections.try_recv().is_err());
        assert_eq!(relay.try_next(), None);
    }
}

#[test]
fn stale_provenance_and_disabled_negotiation_never_request_rendezvous() {
    let (hub, _) = Hub::new(true);
    let mut relay = hub.join([1; 20]);
    relay.update(Some(addr(1)), true, None);
    let now = Instant::now();
    relay.learned([addr(2)], now);
    hub.failed(addr(2), now + PROVENANCE_LIFETIME);
    assert_eq!(relay.try_next(), None);
    relay.learned([addr(2)], now + PROVENANCE_LIFETIME);
    relay.update(Some(addr(1)), false, None);
    hub.failed(addr(2), now + PROVENANCE_LIFETIME);
    relay.received(
        Message {
            endpoint: addr(2),
            kind: Kind::Rendezvous,
        },
        now + PROVENANCE_LIFETIME,
    );
    assert_eq!(relay.try_next(), None);
}

#[test]
fn redundant_connects_and_repeated_relay_requests_are_ignored() {
    let (hub, mut connections) = Hub::new(true);
    let mut a = hub.join([1; 20]);
    let mut b = hub.join([2; 20]);
    a.update(Some(addr(1)), true, None);
    b.update(Some(addr(2)), true, None);
    let now = Instant::now();
    a.received(
        Message {
            endpoint: addr(2),
            kind: Kind::Connect,
        },
        now,
    );
    assert!(connections.try_recv().is_err());
    for _ in 0..2 {
        a.received(
            Message {
                endpoint: addr(2),
                kind: Kind::Rendezvous,
            },
            now,
        );
    }
    assert!(a.try_next().is_some());
    assert_eq!(a.try_next(), None);
    assert!(b.try_next().is_some());
    assert_eq!(b.try_next(), None);
}
