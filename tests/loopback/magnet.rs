use rtrnt::download::{DownloadConfig, SeedLimits, check_file, download_from_peers};
use rtrnt::magnet::metadata::{self, Message};
use rtrnt::magnet::{self, MagnetLink, download_magnet};
use rtrnt::manager::StartOptions;
use rtrnt::peer::extension::{ExtendedHandshake, UT_METADATA, UT_METADATA_ID};
use rtrnt::peer::{PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::torrent::{self, TorrentFile};
use sha1::Digest;
use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::common::*;

/// The test torrent's info dict with an extra key we do not model, padded
/// so that it takes two metadata pieces, and its info hash.
fn padded_info(torrent: &TorrentFile) -> (Vec<u8>, [u8; 20]) {
    let mut info = serde_bencode::to_bytes(&torrent.info).unwrap();
    info.pop();
    info.extend_from_slice(b"8:zpadding20000:");
    info.extend(std::iter::repeat_n(b'x', 20_000));
    info.push(b'e');
    assert!(info.len() > METADATA_PIECE_LEN);
    let hash = sha1::Sha1::digest(&info).into();
    (info, hash)
}

const METADATA_PIECE_LEN: usize = metadata::PIECE_LEN;

/// The id a mock peer asks us to send its `ut_metadata` messages with.
const MOCK_UT_METADATA: u8 = 3;

/// A peer with the whole torrent and its metadata, for magnet links: it
/// accepts leechers one after the other, sends `metadata` through
/// `ut_metadata` (each piece with a byte flipped if `corrupt`), and blocks
/// of `data`. With `fast` it supports the fast extension (BEP 6), and says
/// Have All rather than sending a bitfield. Counts the connections it takes
/// and the blocks it is asked for.
fn magnet_seeder(
    listener: TcpListener,
    info_hash: [u8; 20],
    metadata: Vec<u8>,
    data: Arc<Vec<u8>>,
    corrupt: bool,
    fast: bool,
) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let connections = Arc::new(AtomicUsize::new(0));
    let blocks = Arc::new(AtomicUsize::new(0));
    let counters = (Arc::clone(&connections), Arc::clone(&blocks));
    let num_pieces = data.len().div_ceil(PIECE_LENGTH as usize);
    let all: Vec<usize> = (0..num_pieces).collect();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            connections.fetch_add(1, Ordering::SeqCst);
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            let handshake = parse_handshake(&handshake, &info_hash).unwrap();
            assert!(handshake.supports_extensions());
            let reply = if fast {
                handshake_bytes(&info_hash, &generate_peer_id())
            } else {
                mock_handshake(&info_hash)
            };
            socket.write_all(&reply).await.unwrap();
            let theirs = ExtendedHandshake {
                extensions: [(UT_METADATA.to_string(), MOCK_UT_METADATA)].into(),
                metadata_size: Some(metadata.len() as u32),
                ..Default::default()
            };
            let payload = theirs.encode().unwrap();
            send(&mut socket, PeerMessage::Extended { id: 0, payload }).await;
            if fast {
                send(&mut socket, PeerMessage::HaveAll).await;
            } else {
                send_bitfield(&mut socket, num_pieces, &all).await;
            }
            send(&mut socket, PeerMessage::Unchoke).await;
            while let Some(message) = read_message(&mut socket).await {
                match message {
                    PeerMessage::Extended {
                        id: MOCK_UT_METADATA,
                        payload,
                    } => {
                        let Some(Message::Request { piece }) = Message::decode(&payload).unwrap()
                        else {
                            panic!("unexpected ut_metadata message");
                        };
                        let mut reply = metadata::answer(Some(&metadata), piece);
                        if let Message::Data { data, .. } = &mut reply
                            && corrupt
                        {
                            data[0] ^= 0xff;
                        }
                        let payload = reply.encode();
                        let id = UT_METADATA_ID;
                        send(&mut socket, PeerMessage::Extended { id, payload }).await;
                    }
                    PeerMessage::Request {
                        index,
                        begin,
                        length,
                    } => {
                        blocks.fetch_add(1, Ordering::SeqCst);
                        send(&mut socket, piece(&data, index, begin, length)).await;
                    }
                    _ => {}
                }
            }
        }
    });
    counters
}

/// A magnet link names a lying peer (`x.pe`) and a tracker that knows an
/// honest one. With room for one peer, the liar is asked first, sends
/// metadata that fails the hash check and is banned; the honest peer's
/// metadata passes, and the download carries on over the same connection,
/// on which its Have All (fast extension) came before we knew the number of
/// pieces.
/// The metadata is saved, so a second run neither fetches it again nor
/// contacts the tracker, and `--check` can use it.
#[tokio::test]
async fn downloads_a_magnet_link_banning_a_peer_with_bad_metadata() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, _) = make_torrent(&data);
    let (info, info_hash) = padded_info(&torrent);
    let total = data.len() as u64;

    let liar = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let liar_addr = liar.local_addr().unwrap();
    let (liar_connections, liar_blocks) = magnet_seeder(
        liar,
        info_hash,
        info.clone(),
        Arc::clone(&data),
        true,
        false,
    );
    let honest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let honest_addr = honest.local_addr().unwrap();
    let (honest_connections, honest_blocks) = magnet_seeder(
        honest,
        info_hash,
        info.clone(),
        Arc::clone(&data),
        false,
        true,
    );
    let (tracker_url, mut queries) = mock_tracker(vec![honest_addr]).await;

    let hex: String = info_hash.iter().map(|b| format!("{b:02x}")).collect();
    let uri = format!("magnet:?xt=urn:btih:{hex}&dn=multi&tr={tracker_url}&x.pe={liar_addr}");
    let link = MagnetLink::parse(&uri).unwrap();
    let out = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 1,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        // Tests stay off the real internet.
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
    };
    let client = client(config);
    tokio::time::timeout(
        Duration::from_secs(30),
        download_magnet(&link, &StartOptions::default(), &client, pending()),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    assert_eq!(liar_connections.load(Ordering::SeqCst), 1, "liar retried");
    assert_eq!(liar_blocks.load(Ordering::SeqCst), 0);
    assert_eq!(honest_connections.load(Ordering::SeqCst), 1, "reconnected");
    assert!(honest_blocks.load(Ordering::SeqCst) > 0);

    let mut seen = Vec::new();
    while let Ok(query) = queries.try_recv() {
        seen.push(query);
    }
    let (total, zero) = (total.to_string(), "0".to_string());
    assert_eq!(
        announces(&seen),
        [
            ["started".into(), zero.clone(), zero.clone(), "16384".into()],
            [
                "completed".into(),
                total.clone(),
                zero.clone(),
                zero.clone()
            ],
            ["stopped".into(), total, zero.clone(), zero],
        ]
    );

    // Saved with the info dict byte for byte, and the trackers.
    let saved = out.path().join(format!("{hex}.torrent"));
    let metainfo = torrent::load_metainfo(&saved).await.unwrap();
    assert_eq!(metainfo.info_hash, info_hash);
    assert_eq!(metainfo.info_bytes, info);
    assert_eq!(metainfo.torrent.tracker_tiers(), [[tracker_url]]);

    tokio::time::timeout(
        Duration::from_secs(10),
        download_magnet(&link, &StartOptions::default(), &client, pending()),
    )
    .await
    .expect("second run timed out")
    .expect("second run failed");
    assert!(queries.try_recv().is_err(), "second run asked the tracker");
    assert_eq!(honest_connections.load(Ordering::SeqCst), 1);
    assert_eq!(
        magnet::saved_torrent(&link, out.path()).await,
        Some(saved.clone())
    );
    check_file(&saved, out.path()).await.unwrap();
}

/// A peer that only has the magnet link connects to us while we seed and
/// fetches the metadata: our extension handshake gives its size, every
/// piece comes back, and a piece past the end is rejected.
#[tokio::test]
async fn serves_metadata_to_a_magnet_client() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let info = serde_bencode::to_bytes(&torrent.info).unwrap();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let (done, done_rx) = oneshot::channel();
    let client = tokio::spawn(async move {
        let mut socket = connect_to(our_addr, info_hash).await;
        let theirs = ExtendedHandshake {
            extensions: [(UT_METADATA.to_string(), MOCK_UT_METADATA)].into(),
            ..Default::default()
        };
        let payload = theirs.encode().unwrap();
        send(&mut socket, PeerMessage::Extended { id: 0, payload }).await;
        let ours = ExtendedHandshake::decode(&next_extended(&mut socket, 0).await).unwrap();
        let size = ours.metadata_size.expect("no metadata_size") as usize;
        let id = ours.id(UT_METADATA).expect("no ut_metadata");
        let pieces = size.div_ceil(METADATA_PIECE_LEN) as u32;
        let requests: Vec<PeerMessage> = (0..=pieces)
            .map(|piece| PeerMessage::Extended {
                id,
                payload: Message::Request { piece }.encode(),
            })
            .collect();
        send_batch(&mut socket, &requests).await;
        let mut metadata = Vec::new();
        for _ in 0..=pieces {
            let payload = next_extended(&mut socket, MOCK_UT_METADATA).await;
            match Message::decode(&payload).unwrap().unwrap() {
                Message::Data {
                    piece,
                    total_size,
                    data,
                } => {
                    assert_eq!(piece as usize * METADATA_PIECE_LEN, metadata.len());
                    assert_eq!(total_size, Some(size as u64));
                    metadata.extend(data);
                }
                Message::Reject { piece } => assert_eq!(piece, pieces),
                other => panic!("unexpected {other:?}"),
            }
        }
        done.send(()).unwrap();
        metadata
    });

    let forever = SeedLimits {
        ratio: 1000.0,
        time: Duration::ZERO,
    };
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            seeding_session(torrent, info_hash, out.path(), forever),
            &vec![true; num_pieces],
            &[],
            10,
            vec![listener],
            None,
            async {
                done_rx.await.ok();
            },
        ),
    )
    .await
    .expect("seeding did not stop")
    .expect("seeding failed");
    let metadata = client.await.unwrap();
    assert_eq!(metadata, info);
    assert_eq!(<[u8; 20]>::from(sha1::Sha1::digest(&metadata)), info_hash);
}

/// One rtrnt seeds a torrent, another downloads it from a magnet link that
/// names only the first one's address: the metadata and then the data come
/// over the same connection, with the fast extension on both sides.
#[tokio::test]
async fn downloads_a_magnet_link_from_another_rtrnt() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &data);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let (done, done_rx) = oneshot::channel::<()>();
    let forever = SeedLimits {
        ratio: 1000.0,
        time: Duration::ZERO,
    };
    let session = seeding_session(torrent, info_hash, seed_dir.path(), forever);
    let seeder = tokio::spawn(async move {
        let stop = async {
            done_rx.await.ok();
        };
        let have = vec![true; num_pieces];
        download_from_peers(session, &have, &[], 10, vec![listener], None, stop).await
    });

    let hex: String = info_hash.iter().map(|b| format!("{b:02x}")).collect();
    let link = MagnetLink::parse(&format!("magnet:?xt=urn:btih:{hex}&x.pe={seeder_addr}")).unwrap();
    let out = tempfile::tempdir().unwrap();
    let config = DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
    };
    let client = client(config);
    tokio::time::timeout(
        Duration::from_secs(20),
        download_magnet(&link, &StartOptions::default(), &client, pending()),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
    assert!(out.path().join(format!("{hex}.torrent")).exists());
    done.send(()).unwrap();
    seeder.await.unwrap().unwrap();
}
