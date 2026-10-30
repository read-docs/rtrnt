use super::*;

/// The table from BEP 42: address, random byte and the example id.
const VECTORS: [([u8; 4], u8, &str); 5] = [
    (
        [124, 31, 75, 21],
        1,
        "5fbfbff10c5d6a4ec8a88e4c6ab4c28b95eee401",
    ),
    (
        [21, 75, 31, 124],
        86,
        "5a3ce9c14e7a08645677bbd1cfe7d8f956d53256",
    ),
    (
        [65, 23, 51, 170],
        22,
        "a5d43220bc8f112a3d426c84764f8c2a1150e616",
    ),
    (
        [84, 124, 73, 14],
        65,
        "1b0321dd1bb1fe518101ceef99462b947a01ff41",
    ),
    (
        [43, 213, 53, 83],
        90,
        "e56f6cbf5b7c4be0237986d5243b87aa6d51305a",
    ),
];

fn parse_id(hex: &str) -> [u8; 20] {
    std::array::from_fn(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
}

#[test]
fn crc32c_check_value() {
    // The standard check value of CRC-32C.
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn bep_42_vectors() {
    for (ip, rand, id) in VECTORS {
        let ip = Ipv4Addr::from(ip);
        let id = parse_id(id);
        // Everything but the 21 bits and the last byte is free, so the
        // example's own bytes are used for it.
        assert_eq!(NodeId::secure_v4_with(ip, rand, id), NodeId(id), "{ip}");
        assert!(NodeId(id).is_secure_v4(ip), "{ip}");
    }
}

#[test]
fn generated_ids_are_valid_for_their_address_only() {
    let ip = Ipv4Addr::new(124, 31, 75, 21);
    let mut other_hits = 0;
    for _ in 0..200 {
        let id = NodeId::secure_v4(ip);
        assert!(id.is_secure_v4(ip));
        // A 21-bit prefix matches another address by chance once in two
        // million.
        if id.is_secure_v4(Ipv4Addr::new(8, 8, 8, 8)) {
            other_hits += 1;
        }
    }
    assert_eq!(other_hits, 0);
    let id = NodeId(parse_id(VECTORS[0].2));
    assert!(!id.is_secure_v4(Ipv4Addr::new(124, 31, 75, 22)));
    // Flipping a bit in the checked prefix breaks it.
    let mut broken = id;
    broken.0[0] ^= 0x80;
    assert!(!broken.is_secure_v4(Ipv4Addr::new(124, 31, 75, 21)));
}

#[test]
fn local_addresses_are_exempt() {
    let id = NodeId([0xab; 20]);
    for ip in [
        [10, 1, 2, 3],
        [172, 16, 0, 1],
        [172, 31, 255, 255],
        [192, 168, 1, 1],
        [169, 254, 1, 1],
        [127, 0, 0, 1],
    ] {
        assert!(id.is_secure_v4(Ipv4Addr::from(ip)), "{ip:?}");
    }
    assert!(!id.is_secure_v4(Ipv4Addr::new(172, 32, 0, 1)));
}

#[test]
fn votes_need_several_distinct_reporters() {
    let ip = Ipv4Addr::new(1, 2, 3, 4);
    let mut votes = IpVotes::default();
    let reporter = |n: u8| Ipv4Addr::new(9, 9, 9, n);
    // One node repeating itself is one vote.
    for _ in 0..10 {
        assert_eq!(votes.vote(reporter(1), ip), None);
    }
    assert_eq!(votes.vote(reporter(2), ip), None);
    assert_eq!(votes.vote(reporter(3), ip), None);
    assert_eq!(votes.vote(reporter(4), ip), Some(ip));
}

#[test]
fn votes_need_a_majority() {
    let mut votes = IpVotes::default();
    let reporter = |n: u8| Ipv4Addr::new(9, 9, 9, n);
    let (a, b) = (Ipv4Addr::new(1, 2, 3, 4), Ipv4Addr::new(5, 6, 7, 8));
    for n in 0..5 {
        votes.vote(reporter(n), b);
    }
    // Four for `a` against five for `b` are not a majority.
    for n in 10..14 {
        assert_eq!(votes.vote(reporter(n), a), None);
    }
    // Five against five is a tie, six against five wins.
    assert_eq!(votes.vote(reporter(14), a), None);
    assert_eq!(votes.vote(reporter(15), a), Some(a));
}

#[test]
fn votes_ignore_local_addresses_and_are_bounded() {
    let mut votes = IpVotes::default();
    for n in 0..100 {
        let reporter = Ipv4Addr::new(9, 9, 9, n);
        assert_eq!(votes.vote(reporter, Ipv4Addr::new(192, 168, 0, 5)), None);
        assert_eq!(votes.vote(reporter, Ipv4Addr::UNSPECIFIED), None);
    }
    assert!(votes.votes.is_empty());
    for n in 0..100 {
        votes.vote(Ipv4Addr::new(9, 9, 9, n), Ipv4Addr::new(1, 2, 3, n));
    }
    assert_eq!(votes.votes.len(), MAX_VOTES);
}

#[test]
fn public_addresses() {
    for ip in [
        [8, 8, 8, 8],
        [100, 63, 0, 1],
        [100, 128, 0, 1],
        [124, 31, 75, 21],
    ] {
        assert!(is_public(Ipv4Addr::from(ip)), "{ip:?}");
    }
    for ip in [
        [10, 0, 0, 1],
        [100, 64, 0, 1],
        [100, 127, 255, 255],
        [0, 0, 0, 0],
        [255, 255, 255, 255],
        [224, 0, 0, 1],
        [127, 0, 0, 1],
    ] {
        assert!(!is_public(Ipv4Addr::from(ip)), "{ip:?}");
    }
}

fn v6(text: &str) -> IpAddr {
    text.parse().unwrap()
}

/// BEP 42 gives no IPv6 vectors, so this follows its formula: the first 64
/// bits masked with 0x0103070f1f3f7fff, `r` in the top 3 bits, CRC-32C of
/// those 8 bytes.
#[test]
fn ipv6_ids_follow_the_formula() {
    let ip = v6("2a01:4f8:1c1e:9a42::1");
    let r = 5u8;
    let high: u64 = 0x2a01_04f8_1c1e_9a42;
    let masked = (high & 0x0103_070f_1f3f_7fff) | (u64::from(r) << 61);
    let hash = crc32c(&masked.to_be_bytes());
    let id = NodeId::secure_with(ip, 0xd0 | r, [0x77; 20]);
    assert_eq!(id.0[0], (hash >> 24) as u8);
    assert_eq!(id.0[1], (hash >> 16) as u8);
    assert_eq!(id.0[2] & 0xf8, (hash >> 8) as u8 & 0xf8);
    assert_eq!(id.0[2] & 0x07, 0x77 & 0x07);
    assert_eq!(id.0[19], 0xd5);
    assert_eq!(&id.0[3..19], &[0x77; 16]);
    assert!(id.is_secure(ip));
}

#[test]
fn ipv6_ids_are_valid_for_their_network_only() {
    let ip = v6("2a01:4f8:1c1e:9a42::1");
    for _ in 0..100 {
        let id = NodeId::secure(ip);
        assert!(id.is_secure(ip));
        // Only the first 64 bits count, and only some of them: the host
        // part is free.
        assert!(id.is_secure(v6("2a01:4f8:1c1e:9a42:dead:beef:1:2")));
        assert!(!id.is_secure(v6("2606:4700::1")));
        // An IPv4 address does not pass as an IPv6 one.
        assert!(!id.is_secure(v6("8.8.8.8")));
    }
}

#[test]
fn ipv6_local_addresses_are_exempt_and_not_public() {
    let id = NodeId([0xab; 20]);
    for ip in ["::1", "fe80::1", "febf::1", "fc00::1", "fd12:3456::1"] {
        assert!(id.is_secure(v6(ip)), "{ip}");
        assert!(!is_public_ip(v6(ip)), "{ip}");
    }
    for ip in ["2001:db8::1", "2a01:4f8::1"] {
        assert!(!id.is_secure(v6(ip)), "{ip}");
        assert!(is_public_ip(v6(ip)), "{ip}");
    }
    for ip in ["::", "ff02::1"] {
        assert!(!is_public_ip(v6(ip)), "{ip}");
    }
    // A mapped IPv4 address is judged as IPv4.
    assert!(!is_public_ip(v6("::ffff:192.168.1.1")));
    assert!(is_public_ip(v6("::ffff:8.8.8.8")));
}
