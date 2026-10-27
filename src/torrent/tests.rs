use super::*;

fn single_file(length: u64, piece_length: u64) -> TorrentFile {
    let pieces = length.div_ceil(piece_length) as usize;
    TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: "file".into(),
            piece_length,
            pieces: ByteBuf::from(vec![0u8; pieces * 20]),
            private: None,
            length: Some(length),
            files: None,
        },
    }
}

#[test]
fn piece_lengths() {
    let t = single_file(100, 32);
    assert_eq!(t.num_pieces(), 4);
    assert_eq!(t.piece_len(0).unwrap(), 32);
    assert_eq!(t.piece_len(3).unwrap(), 4);
    assert!(t.piece_len(4).is_err());

    let exact = single_file(64, 32);
    assert_eq!(exact.piece_len(1).unwrap(), 32);
}

#[test]
fn piece_hash_bounds() {
    let t = single_file(100, 32);
    assert!(t.piece_hash(3).is_ok());
    assert!(t.piece_hash(4).is_err());
    assert!(t.piece_hash(usize::MAX).is_err());
}

#[test]
fn tracker_tiers_follow_bep_12() {
    let mut t = single_file(10, 32);
    assert!(t.tracker_tiers().is_empty());

    t.announce = Some("udp://x".into());
    assert_eq!(t.tracker_tiers(), [["udp://x"]]);
    // An announce-list without trackers does not count.
    t.announce_list = Some(vec![vec![]]);
    assert_eq!(t.tracker_tiers(), [["udp://x"]]);

    // Otherwise it replaces `announce`; duplicates and the tiers they
    // leave empty are dropped.
    t.announce_list = Some(vec![
        vec!["udp://a".into(), "http://b".into()],
        vec![],
        vec!["http://c".into(), "http://b".into()],
        vec!["udp://a".into()],
    ]);
    assert_eq!(
        t.tracker_tiers(),
        [vec!["udp://a", "http://b"], vec!["http://c"]]
    );
}

#[test]
fn info_hash_covers_unknown_keys() {
    let pieces = [7u8; 20];
    let mut info = b"d6:lengthi10e4:name4:file12:piece lengthi32e6:pieces20:".to_vec();
    info.extend_from_slice(&pieces);
    info.extend_from_slice(b"7:privatei1ee");
    let mut data = b"d8:announce8:http://x4:info".to_vec();
    data.extend_from_slice(&info);
    data.push(b'e');

    let (torrent, hash) = parse(&data).unwrap();
    assert_eq!(torrent.info.name, "file");
    assert!(torrent.is_private());
    assert_eq!(torrent.tracker_tiers(), [["http://x"]]);
    let expected: [u8; 20] = sha1::Sha1::digest(&info).into();
    assert_eq!(hash, expected);
}

/// An info dict with a key we do not model, so re-encoding the parsed
/// struct would change it.
fn raw_info() -> Vec<u8> {
    let mut info = b"d6:lengthi10e4:name4:file12:piece lengthi32e6:pieces20:".to_vec();
    info.extend_from_slice(&[7u8; 20]);
    info.extend_from_slice(b"6:source3:xyze");
    info
}

#[test]
fn builds_a_torrent_from_fetched_metadata() {
    let info = raw_info();
    let tiers = vec![vec!["udp://a".to_string()], vec!["http://b".to_string()]];
    let torrent = from_info(&info, &tiers).unwrap();
    assert_eq!(torrent.info.name, "file");
    assert_eq!(torrent.length(), 10);
    assert_eq!(torrent.tracker_tiers(), tiers);
    assert!(from_info(&info, &[]).unwrap().tracker_tiers().is_empty());

    let mut trailing = info.clone();
    trailing.push(b'x');
    let cases: &[&[u8]] = &[b"", b"le", b"i1e", b"de", &trailing, &info[1..]];
    for case in cases {
        assert!(from_info(case, &tiers).is_err(), "accepted {case:?}");
    }
}

#[test]
fn saved_torrent_keeps_the_info_dict_byte_for_byte() {
    let info = raw_info();
    let hash: [u8; 20] = sha1::Sha1::digest(&info).into();
    let tiers = vec![
        vec!["udp://a".to_string(), "udp://b".to_string()],
        vec!["http://c".to_string()],
    ];
    for tiers in [tiers, Vec::new()] {
        let data = encode_with_info(&tiers, &info).unwrap();
        let metainfo = parse_metainfo(&data).unwrap();
        assert_eq!(metainfo.info_hash, hash);
        assert_eq!(metainfo.info_bytes, info);
        assert_eq!(metainfo.torrent.tracker_tiers(), tiers);
    }
}

#[test]
fn rejects_inconsistent_piece_count() {
    let mut t = single_file(100, 32);
    t.info.pieces = ByteBuf::from(vec![0u8; 3 * 20]);
    assert!(t.validate().is_err());
    t.info.pieces = ByteBuf::from(vec![0u8; 81]);
    assert!(t.validate().is_err());
}
