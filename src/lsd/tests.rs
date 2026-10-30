use super::*;

const A: [u8; 20] = [0xab; 20];
const B: [u8; 20] = [0x01; 20];

fn ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(192, 168, 1, 7))
}

fn announce(port: u16, info_hashes: Vec<[u8; 20]>) -> Announce {
    Announce {
        port,
        info_hashes,
        cookie: None,
    }
}

#[test]
fn encodes_the_bep_14_message() {
    let text = String::from_utf8(encode(&[A, B], 6881, "cafe")).unwrap();
    assert_eq!(
        text,
        format!(
            "BT-SEARCH * HTTP/1.1\r\nHost: 239.192.152.143:6771\r\nPort: 6881\r\n\
             Infohash: {}\r\nInfohash: {}\r\ncookie: cafe\r\n\r\n\r\n",
            "ab".repeat(20),
            "01".repeat(20),
        )
    );
}

#[test]
fn parses_what_it_encodes() {
    let hashes: Vec<_> = (0..HASHES_PER_MESSAGE as u8).map(|i| [i; 20]).collect();
    let message = encode(&hashes, 51413, "xyz");
    assert!(message.len() <= MAX_MESSAGE);
    assert_eq!(
        parse(&message),
        Some(Announce {
            port: 51413,
            info_hashes: hashes,
            cookie: Some("xyz".into()),
        })
    );
}

#[test]
fn headers_are_case_insensitive_and_unknown_ones_ignored() {
    let text = format!(
        "BT-SEARCH * HTTP/1.1\r\nhost: x\r\nPORT:6881\r\nX-Other: 1\r\nINFOHASH: {}\r\n\r\n\r\n",
        "AB".repeat(20)
    );
    let parsed = parse(text.as_bytes()).unwrap();
    assert_eq!(parsed.port, 6881);
    assert_eq!(parsed.info_hashes, [A]);
    assert_eq!(parsed.cookie, None);
}

#[test]
fn rejects_junk() {
    let hash = "ab".repeat(20);
    let message = |port: &str, infohash: &str| {
        format!("BT-SEARCH * HTTP/1.1\r\nPort: {port}\r\nInfohash: {infohash}\r\n\r\n\r\n")
    };
    assert!(parse(message("6881", &hash).as_bytes()).is_some());
    assert!(parse(b"").is_none());
    assert!(parse(b"\xff\xfe\x00junk").is_none());
    assert!(parse(b"GET / HTTP/1.1\r\n\r\n").is_none());
    assert!(parse(message("0", &hash).as_bytes()).is_none());
    assert!(parse(message("65536", &hash).as_bytes()).is_none());
    assert!(parse(message("x", &hash).as_bytes()).is_none());
    assert!(parse(message("6881", &hash[..39]).as_bytes()).is_none());
    assert!(parse(message("6881", &"zz".repeat(20)).as_bytes()).is_none());
    assert!(parse(message("6881", &"é".repeat(20)).as_bytes()).is_none());
    // No Port, no Infohash, a header without a colon, a repeated Port.
    let no_port = format!("BT-SEARCH * HTTP/1.1\r\nInfohash: {hash}\r\n\r\n\r\n");
    assert!(parse(no_port.as_bytes()).is_none());
    assert!(parse(b"BT-SEARCH * HTTP/1.1\r\nPort: 1\r\n\r\n\r\n").is_none());
    let no_colon = format!("BT-SEARCH * HTTP/1.1\r\nPort 1\r\nInfohash: {hash}\r\n\r\n");
    assert!(parse(no_colon.as_bytes()).is_none());
    let twice = format!("BT-SEARCH * HTTP/1.1\r\nPort: 1\r\nPort: 2\r\nInfohash: {hash}\r\n\r\n");
    assert!(parse(twice.as_bytes()).is_none());
}

#[test]
fn bounds_the_size() {
    let mut long = encode(&[A], 1, "c");
    long.extend(std::iter::repeat_n(b'x', MAX_MESSAGE));
    assert!(parse(&long).is_none());
}

#[test]
fn the_cookie_tells_our_own_announce() {
    let parsed = parse(&encode(&[A], 6881, "mine")).unwrap();
    assert_eq!(parsed.cookie.as_deref(), Some("mine"));
    assert_ne!(parsed.cookie.as_deref(), Some("theirs"));
}

#[test]
fn announces_a_new_torrent_once_and_then_every_five_minutes() {
    let t0 = Instant::now();
    let mut schedule = Schedule::default();
    assert!(schedule.due(t0).is_empty());
    schedule.add(A);
    assert_eq!(schedule.due(t0), [A]);
    assert!(schedule.due(t0 + Duration::from_secs(1)).is_empty());
    assert!(
        schedule
            .due(t0 + ANNOUNCE_INTERVAL - Duration::from_secs(1))
            .is_empty()
    );
    assert_eq!(schedule.due(t0 + ANNOUNCE_INTERVAL), [A]);
    assert!(
        schedule
            .due(t0 + ANNOUNCE_INTERVAL + Duration::from_secs(10))
            .is_empty()
    );
}

#[test]
fn a_torrent_is_not_announced_twice_within_a_minute() {
    let t0 = Instant::now();
    let mut schedule = Schedule::default();
    schedule.add(A);
    assert_eq!(schedule.due(t0), [A]);
    // Asked for again: it waits out the minute.
    schedule.add(A);
    assert!(schedule.due(t0 + Duration::from_secs(59)).is_empty());
    assert_eq!(schedule.due(t0 + MIN_ANNOUNCE_INTERVAL), [A]);
}

#[test]
fn torrents_are_limited_one_by_one() {
    let t0 = Instant::now();
    let mut schedule = Schedule::default();
    schedule.add(A);
    assert_eq!(schedule.due(t0), [A]);
    schedule.add(B);
    assert_eq!(schedule.due(t0 + Duration::from_secs(5)), [B]);
    schedule.remove(&B);
    assert_eq!(schedule.due(t0 + ANNOUNCE_INTERVAL * 2), [A]);
}

#[test]
fn private_torrents_are_neither_announced_nor_given_peers() {
    let mut state = State::default();
    assert!(state.register(A, true).is_none());
    assert!(state.schedule.due(Instant::now()).is_empty());
    assert_eq!(state.deliver(&announce(6881, vec![A]), ip()), 0);
}

#[test]
fn peers_go_to_the_torrents_they_announce() {
    let mut state = State::default();
    let mut rx_a = state.register(A, false).unwrap();
    let mut rx_b = state.register(B, false).unwrap();
    let theirs = announce(6882, vec![A, [7; 20]]);
    assert_eq!(state.deliver(&theirs, ip()), 1);
    assert_eq!(rx_a.try_recv().unwrap(), SocketAddr::new(ip(), 6882));
    assert!(rx_b.try_recv().is_err());

    // Not from a multicast or unspecified address.
    assert_eq!(state.deliver(&theirs, IpAddr::V4(GROUP)), 0);
    assert_eq!(state.deliver(&theirs, IpAddr::V4(Ipv4Addr::UNSPECIFIED)), 0);

    state.unregister(&A);
    assert_eq!(state.deliver(&theirs, ip()), 0);
}

#[test]
fn a_full_queue_drops_peers_instead_of_blocking() {
    let mut state = State::default();
    let mut rx = state.register(A, false).unwrap();
    for _ in 0..QUEUE * 2 {
        state.deliver(&announce(1, vec![A]), ip());
    }
    let mut got = 0;
    while rx.try_recv().is_ok() {
        got += 1;
    }
    assert_eq!(got, QUEUE);
}
