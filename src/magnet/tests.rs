use super::*;

const HASH: [u8; 20] = [
    0xc1, 0x2f, 0xe1, 0xc0, 0x6b, 0xba, 0x25, 0x4a, 0x9d, 0xc9, 0xf5, 0x19, 0xb3, 0x35, 0xaa, 0x7c,
    0x13, 0x67, 0xa8, 0x8a,
];
const HEX: &str = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
const BASE32: &str = "YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK";

#[test]
fn parses_hex_and_base32_info_hashes_in_any_case() {
    for hash in [
        HEX.to_string(),
        HEX.to_uppercase(),
        BASE32.to_string(),
        BASE32.to_lowercase(),
    ] {
        let link = MagnetLink::parse(&format!("magnet:?xt=urn:btih:{hash}")).unwrap();
        assert_eq!(link.info_hash, HASH, "{hash}");
        assert_eq!(link.name, None);
        assert!(link.trackers.is_empty());
    }
    let link = MagnetLink::parse(&format!("MAGNET:?XT=URN:BTIH:{HEX}"));
    assert!(link.is_err(), "parameter names are case-sensitive");
    let link = MagnetLink::parse(&format!("Magnet:?xt=URN:BTIH:{HEX}")).unwrap();
    assert_eq!(link.info_hash, HASH);
}

#[test]
fn parses_name_trackers_and_peers() {
    let uri = format!(
        "magnet:?xt=urn:btih:{HEX}&dn=Some+File%20%C3%A9.iso\
         &tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce\
         &tr=http://t.example/announce?a=1%26b%3D2\
         &tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce&tr=\
         &x.pe=10.0.0.1:6881&x.pe=%5B2001:db8::1%5D:51413&x.pe=host.example:1\
         &xt=urn:btmh:1220abcd&ws=http://seed.example/&unknown"
    );
    let link = MagnetLink::parse(&uri).unwrap();
    assert_eq!(
        link,
        MagnetLink {
            info_hash: HASH,
            name: Some("Some File é.iso".into()),
            trackers: vec![
                "udp://tracker.example:1337/announce".into(),
                "http://t.example/announce?a=1&b=2".into(),
            ],
            peers: vec![
                "10.0.0.1:6881".parse().unwrap(),
                "[2001:db8::1]:51413".parse().unwrap(),
            ],
        }
    );
    assert_eq!(
        link.tracker_tiers(),
        [
            ["udp://tracker.example:1337/announce"],
            ["http://t.example/announce?a=1&b=2"]
        ]
    );
    assert_eq!(
        link.saved_torrent_path(Path::new("/out")),
        Path::new("/out").join(format!("{HEX}.torrent"))
    );
    // The same hash twice, in different forms, is fine.
    let uri = format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btih:{BASE32}");
    assert_eq!(MagnetLink::parse(&uri).unwrap().info_hash, HASH);
}

#[test]
fn rejects_malformed_links_with_a_reason() {
    let cases = [
        ("magnet.torrent", "not a magnet link"),
        ("magnet:", "no parameters"),
        ("magnet:xt=urn:btih:", "no parameters"),
        ("magnet:?dn=x&tr=udp://t", "no info hash"),
        ("magnet:?xt=urn:sha1:abc", "no info hash"),
        ("magnet:?xt=urn:btmh:1220abcd", "v2"),
        ("magnet:?xt=urn:btih:", "neither 40 hex digits"),
        (
            "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88",
            "neither 40 hex digits",
        ),
        (
            "magnet:?xt=urn:btih:g12fe1c06bba254a9dc9f519b335aa7c1367a88a",
            "neither 40 hex digits",
        ),
        (
            "magnet:?xt=urn:btih:+12fe1c06bba254a9dc9f519b335aa7c1367a88a",
            "neither 40 hex digits",
        ),
        (
            "magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKE1",
            "neither 40 hex digits",
        ),
        (
            "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a\
             &xt=urn:btih:0000000000000000000000000000000000000000",
            "two different info hashes",
        ),
    ];
    for (uri, reason) in cases {
        let err = MagnetLink::parse(uri).unwrap_err().to_string();
        assert!(err.contains(reason), "{uri}: {err}");
    }
}

#[test]
fn decodes_base32() {
    assert_eq!(parse_base32(BASE32), Some(HASH));
    assert_eq!(parse_base32(&"A".repeat(32)), Some([0; 20]));
    assert_eq!(parse_base32(&"7".repeat(32)), Some([0xff; 20]));
    assert_eq!(parse_base32(&"A".repeat(31)), None);
    assert_eq!(parse_base32(&"A".repeat(33)), None);
    assert_eq!(parse_base32(&"A=".repeat(16)), None);
    assert_eq!(parse_hex(HEX), Some(HASH));
    assert_eq!(parse_hex(&"é".repeat(20)), None);
}

#[test]
fn tells_magnet_links_from_paths() {
    assert!(is_magnet("magnet:?xt=urn:btih:x"));
    assert!(is_magnet("MAGNET:"));
    assert!(!is_magnet("magnet"));
    assert!(!is_magnet("./magnet:x"));
    assert!(!is_magnet("file.torrent"));
    assert!(!is_magnet("mägnet:"));
}
