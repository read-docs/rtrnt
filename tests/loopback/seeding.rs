use rtrnt::announcer::Announcer;
use rtrnt::download::{DownloadConfig, SeedLimits, download_file, download_from_peers};
use rtrnt::peer::{PeerMessage, generate_peer_id};
use rtrnt::tracker::{AnnounceEvent, AnnounceRequest};
use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::common::*;

/// We have pieces 0 and 2 and download piece 1. A leecher that connects to
/// us gets our bitfield, is unchoked once interested, and gets the blocks it
/// asks for, except one it cancels before it was sent. Once piece 1 is done,
/// it hears Have for it.
#[tokio::test]
async fn serves_blocks_to_a_peer_that_connects_to_us() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let (release, release_rx) = oneshot::channel();
    let seeder_addr = gated_seeder(Arc::clone(&data), info_hash, num_pieces, release_rx).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        let data = &leecher_data;
        let mut socket = connect_to(our_addr, info_hash).await;
        let bitfield = vec![0b1010_0000];
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { bitfield })
        );
        send(&mut socket, PeerMessage::Interested).await;
        assert_eq!(next_message(&mut socket).await, Some(PeerMessage::Unchoke));

        // The cancel arrives with the requests, before the block is sent.
        let cancel = PeerMessage::Cancel {
            index: 0,
            begin: BLOCK,
            length: BLOCK,
        };
        let batch = [
            request(0, 0, BLOCK),
            request(2, 0, BLOCK),
            request(0, BLOCK, BLOCK),
            cancel,
        ];
        send_batch(&mut socket, &batch).await;
        assert_eq!(
            next_message(&mut socket).await,
            Some(piece(data, 0, 0, BLOCK))
        );
        assert_eq!(
            next_message(&mut socket).await,
            Some(piece(data, 2, 0, BLOCK))
        );
        // The short last block; the cancelled one never comes first.
        let last = data.len() as u32 - 2 * PIECE_LENGTH as u32 - BLOCK;
        send(&mut socket, request(2, BLOCK, last)).await;
        assert_eq!(
            next_message(&mut socket).await,
            Some(piece(data, 2, BLOCK, last))
        );

        release.send(()).unwrap();
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Have { piece_index: 1 })
        );
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[seeder_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    leecher.await.unwrap();
    assert_files_match(out.path(), &data);
}

/// A peer that asks for a piece we do not have is cut off without an answer.
#[tokio::test]
async fn drops_a_peer_that_requests_a_piece_we_do_not_have() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let (release, release_rx) = oneshot::channel();
    let seeder_addr = gated_seeder(Arc::clone(&data), info_hash, num_pieces, release_rx).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher = tokio::spawn(async move {
        let mut socket = connect_to(our_addr, info_hash).await;
        assert!(matches!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { .. })
        ));
        send(&mut socket, PeerMessage::Interested).await;
        assert_eq!(next_message(&mut socket).await, Some(PeerMessage::Unchoke));
        send_batch(&mut socket, &[request(1, 0, BLOCK), request(0, 0, BLOCK)]).await;
        assert_eq!(next_message(&mut socket).await, None);
        release.send(()).unwrap();
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[seeder_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    leecher.await.unwrap();
}

/// Six peers that want to download from us compete for four regular unchoke
/// slots and an optimistic one: five are unchoked, and the sixth waits with
/// no churn among the others. When an unchoked peer loses interest, it is
/// choked and the waiting one gets the slot at once, not at the next 10 s
/// round.
#[tokio::test]
async fn unchokes_four_peers_and_an_optimistic_one() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let (release, release_rx) = oneshot::channel();
    let seeder_addr = gated_seeder(Arc::clone(&data), info_hash, num_pieces, release_rx).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leechers = tokio::spawn(async move {
        // Every leecher's choke changes, tagged with its number.
        let (events, mut events_rx) = mpsc::unbounded_channel();
        let mut writers = Vec::new();
        for i in 0..6 {
            let socket = connect_to(our_addr, info_hash).await;
            let (mut reader, writer) = socket.into_split();
            let events = events.clone();
            tokio::spawn(async move {
                while let Some(message) = read_message(&mut reader).await {
                    if matches!(message, PeerMessage::Choke | PeerMessage::Unchoke) {
                        let _ = events.send((i, message));
                    }
                }
            });
            writers.push(writer);
        }
        for writer in &mut writers {
            let interested = PeerMessage::Interested.encode();
            writer.write_all(&interested).await.unwrap();
        }
        let mut next_event = async || {
            tokio::time::timeout(Duration::from_secs(5), events_rx.recv())
                .await
                .expect("no choke change")
                .unwrap()
        };

        let mut unchoked = Vec::new();
        while unchoked.len() < 5 {
            let (i, message) = next_event().await;
            assert_eq!(message, PeerMessage::Unchoke, "leecher {i}");
            unchoked.push(i);
        }
        let quiet = tokio::time::timeout(Duration::from_millis(500), next_event()).await;
        assert!(quiet.is_err(), "unexpected {:?}", quiet.unwrap());
        let waiting = (0..6).find(|i| !unchoked.contains(i)).unwrap();

        let leaving = unchoked[0];
        let not_interested = PeerMessage::NotInterested.encode();
        writers[leaving].write_all(&not_interested).await.unwrap();
        let mut changes = vec![next_event().await, next_event().await];
        changes.sort_by_key(|(i, _)| *i);
        let mut expected = vec![
            (leaving, PeerMessage::Choke),
            (waiting, PeerMessage::Unchoke),
        ];
        expected.sort_by_key(|(i, _)| *i);
        assert_eq!(changes, expected);
        release.send(()).unwrap();
        writers
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[seeder_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    leechers.await.unwrap();
}

/// With `--seed-ratio 1`, a finished download keeps going: the tracker hears
/// `completed` at once, a leecher that joins then downloads the whole
/// torrent from us, and with that the run ends, reporting the upload.
#[tokio::test]
async fn seeds_after_completion_until_the_ratio_is_reached() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;

    // A seed, so we let it go once we are one too.
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let (tracker_url, mut queries) = mock_tracker(vec![seeder_addr]).await;
    let request = AnnounceRequest {
        info_hash,
        peer_id: generate_peer_id(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: total,
        event: AnnounceEvent::Started,
    };
    let (announcer, peers) = Announcer::start(vec![vec![tracker_url]], request)
        .await
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        // Joins once the tracker has heard that we finished.
        let mut seen = Vec::new();
        while let Some(query) = queries.recv().await {
            let completed = query_param(&query, "event") == Some("completed");
            seen.push(query);
            if completed {
                break;
            }
        }
        let mut socket = connect_to(our_addr, info_hash).await;
        leech_everything(&mut socket, &leecher_data).await;
        assert_eq!(next_message(&mut socket).await, None, "still seeding");
        (seen, queries)
    });

    let out = tempfile::tempdir().unwrap();
    let seed = SeedLimits {
        ratio: 1.0,
        time: Duration::ZERO,
    };
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            seeding_session(torrent, info_hash, out.path(), seed),
            &vec![false; num_pieces],
            &peers,
            10,
            vec![listener],
            Some(announcer),
            pending(),
        ),
    )
    .await
    .expect("seeding did not stop")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let (mut seen, mut queries) = leecher.await.unwrap();
    while let Ok(query) = queries.try_recv() {
        seen.push(query);
    }
    let (total, zero) = (total.to_string(), "0".to_string());
    assert_eq!(
        announces(&seen),
        [
            ["started".into(), zero.clone(), zero.clone(), total.clone()],
            [
                "completed".into(),
                total.clone(),
                zero.clone(),
                zero.clone()
            ],
            ["stopped".into(), total.clone(), total, zero],
        ]
    );
}

/// `download_file` on a download that is already complete, with a seed
/// ratio: it announces itself as a seed, waits for peers though the tracker
/// has none, and serves the one that finds it through the port it
/// announced. The tracker never hears `completed`.
#[tokio::test]
async fn seeds_a_download_that_is_already_complete() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let total = data.len() as u64;

    let (tracker_url, mut queries) = mock_tracker(Vec::new()).await;
    torrent.announce = Some(tracker_url);
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);
    let torrent_path = out.path().join("multi.torrent");
    std::fs::write(&torrent_path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        let started = queries.recv().await.unwrap();
        let port: u16 = query_param(&started, "port").unwrap().parse().unwrap();
        let mut socket = connect_to(SocketAddr::from(([127, 0, 0, 1], port)), info_hash).await;
        leech_everything(&mut socket, &leecher_data).await;
        assert_eq!(next_message(&mut socket).await, None, "still seeding");
        (vec![started], queries)
    });

    let config = DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits {
            ratio: 1.0,
            time: Duration::ZERO,
        },
        // Tests stay off the real internet.
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
        recheck: false,
    };
    tokio::time::timeout(
        Duration::from_secs(20),
        download_file(&torrent_path, &client(config), pending()),
    )
    .await
    .expect("seeding did not stop")
    .expect("seeding failed");

    let (mut seen, mut queries) = leecher.await.unwrap();
    while let Ok(query) = queries.try_recv() {
        seen.push(query);
    }
    let (total, zero) = (total.to_string(), "0".to_string());
    assert_eq!(
        announces(&seen),
        [
            ["started".into(), zero.clone(), zero.clone(), zero.clone()],
            ["stopped".into(), zero.clone(), total, zero],
        ]
    );
}

/// With only a time limit and nobody to upload to, seeding lasts until the
/// time is up rather than ending when we run out of peers. A shutdown while
/// seeding ends the run successfully.
#[tokio::test]
async fn seeding_ends_when_the_time_is_up_or_on_shutdown() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let limit = Duration::from_millis(300);
    let seed = SeedLimits {
        ratio: 0.0,
        time: limit,
    };
    let session = seeding_session(torrent, info_hash, out.path(), seed);
    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(5),
        download_from_peers(
            Arc::clone(&session),
            &vec![true; num_pieces],
            &[],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("seeding did not stop")
    .expect("seeding failed");
    assert!(started.elapsed() >= limit);

    let forever = SeedLimits {
        ratio: 1000.0,
        time: Duration::ZERO,
    };
    let (torrent, _) = make_torrent(&data);
    let session = seeding_session(torrent, info_hash, out.path(), forever);
    tokio::time::timeout(
        Duration::from_secs(5),
        download_from_peers(
            session,
            &vec![true; num_pieces],
            &[],
            10,
            Vec::new(),
            None,
            tokio::time::sleep(limit),
        ),
    )
    .await
    .expect("shutdown did not stop seeding")
    .expect("a shutdown while seeding is a success");
}
