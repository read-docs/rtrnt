use super::*;

#[test]
fn peer_flags_pack_and_unpack() {
    for bits in 0..64u8 {
        let set = |i: u8| bits & (1 << i) != 0;
        let flags = PeerFlags::new(set(0), set(1), set(2), set(3), set(4), set(5));
        assert_eq!(
            [
                flags.am_choking(),
                flags.peer_choking(),
                flags.am_interested(),
                flags.peer_interested(),
                flags.fast(),
                flags.pex(),
            ],
            [set(0), set(1), set(2), set(3), set(4), set(5)]
        );
    }
    let peer = PeerStats::new("127.0.0.1:1".parse().unwrap(), false);
    assert_eq!(peer.flags(), PeerFlags::initial());
    let flags = PeerFlags::new(false, true, true, false, true, false);
    peer.set_flags(flags);
    assert_eq!(peer.flags(), flags);
}

#[test]
fn dropped_peers_leave_the_list() {
    let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
    let first = Arc::new(PeerStats::new("127.0.0.1:1".parse().unwrap(), false));
    let second = Arc::new(PeerStats::new("127.0.0.1:2".parse().unwrap(), true));
    torrent.add_peer(&first);
    torrent.add_peer(&second);
    assert_eq!(torrent.peers().len(), 2);
    drop(first);
    let peers = torrent.peers();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].addr.port(), 2);
    assert_eq!(lock(&torrent.peers).len(), 1);
    // Pruned on the way in too, so churn does not grow the list unread.
    drop(peers);
    drop(second);
    for port in 3..10 {
        let peer = Arc::new(PeerStats::new(
            SocketAddr::from(([127, 0, 0, 1], port)),
            false,
        ));
        torrent.add_peer(&peer);
    }
    assert_eq!(lock(&torrent.peers).len(), 1);
}

#[test]
fn trackers_start_uncontacted_and_record_answers() {
    let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
    torrent.set_trackers(&[
        vec!["udp://a".into(), "udp://b".into()],
        vec!["http://c".into()],
    ]);
    let response = AnnounceResponse {
        peers: vec!["127.0.0.1:1".parse().unwrap()],
        interval: None,
        min_interval: None,
        seeders: Some(3),
        leechers: None,
    };
    torrent.tracker_answered("udp://b", &Ok(response));
    torrent.tracker_answered("http://c", &Err(anyhow::anyhow!("timed out")));
    torrent.tracker_answered("udp://unknown", &Err(anyhow::anyhow!("x")));
    let trackers = torrent.trackers();
    assert_eq!(trackers.len(), 3);
    assert_eq!(trackers[0].result, TrackerResult::NotContacted);
    assert_eq!(trackers[1].result, TrackerResult::Ok);
    assert_eq!((trackers[1].peers, trackers[1].seeders), (Some(1), Some(3)));
    assert_eq!(trackers[2].tier, 1);
    assert_eq!(trackers[2].result, TrackerResult::Error("timed out".into()));
}

#[test]
fn a_finished_state_stamps_the_end() {
    let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
    torrent.set_state(TorrentState::Checking { done: 0, total: 10 });
    assert_eq!(torrent.checked(4), 4);
    assert_eq!(
        torrent.state(),
        TorrentState::Checking { done: 4, total: 10 }
    );
    assert!(torrent.finished_at().is_none());
    torrent.set_state(TorrentState::Done);
    assert!(torrent.finished_at().is_some());
}
