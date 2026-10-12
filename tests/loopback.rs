//! End-to-end downloads from mock seeders (and a mock tracker) over loopback.

use rtrnt::announcer::Announcer;
use rtrnt::download::{Interrupted, Session, download_from_peers, listen};
use rtrnt::peer::{PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::storage::Storage;
use rtrnt::torrent::{FileInfo, Info, TorrentFile};
use rtrnt::tracker::{AnnounceEvent, AnnounceRequest, ScrapeStats, scrape, scrape_tiers};
use rtrnt::verify;
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, oneshot};

const PIECE_LENGTH: u64 = 32 * 1024;
const FILES: &[(&str, usize)] = &[("a.bin", 40_000), ("empty.bin", 0), ("b.bin", 50_123)];

fn make_torrent(data: &[u8]) -> (TorrentFile, [u8; 20]) {
    let pieces: Vec<u8> = data
        .chunks(PIECE_LENGTH as usize)
        .flat_map(|chunk| sha1::Sha1::digest(chunk).to_vec())
        .collect();
    let info = Info {
        name: "multi".into(),
        piece_length: PIECE_LENGTH,
        pieces: ByteBuf::from(pieces),
        length: None,
        files: Some(
            FILES
                .iter()
                .map(|(name, len)| FileInfo {
                    path: vec!["dir".into(), name.to_string()],
                    length: *len as u64,
                })
                .collect(),
        ),
    };
    let info_hash: [u8; 20] = sha1::Sha1::digest(serde_bencode::to_bytes(&info).unwrap()).into();
    let torrent = TorrentFile {
        announce: None,
        announce_list: None,
        info,
    };
    (torrent, info_hash)
}

/// Accepts one leecher, completes the handshake and advertises every piece.
async fn accept_leecher(
    listener: &TcpListener,
    info_hash: [u8; 20],
    num_pieces: usize,
) -> TcpStream {
    let all: Vec<usize> = (0..num_pieces).collect();
    accept_partial(listener, info_hash, num_pieces, &all).await
}

/// Accepts one leecher, completes the handshake and advertises `has`.
async fn accept_partial(
    listener: &TcpListener,
    info_hash: [u8; 20],
    num_pieces: usize,
    has: &[usize],
) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    parse_handshake(&handshake, &info_hash).unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    send_bitfield(&mut socket, num_pieces, has).await;
    socket
}

/// Connects to the leecher listening at `addr`, completes the handshake and
/// advertises every piece.
async fn connect_to_leecher(addr: SocketAddr, info_hash: [u8; 20], num_pieces: usize) -> TcpStream {
    let all: Vec<usize> = (0..num_pieces).collect();
    connect_partial(addr, info_hash, num_pieces, &all).await
}

/// Connects to the leecher listening at `addr`, completes the handshake and
/// advertises `has`.
async fn connect_partial(
    addr: SocketAddr,
    info_hash: [u8; 20],
    num_pieces: usize,
    has: &[usize],
) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    parse_handshake(&handshake, &info_hash).unwrap();
    send_bitfield(&mut socket, num_pieces, has).await;
    socket
}

async fn send_bitfield(socket: &mut TcpStream, num_pieces: usize, has: &[usize]) {
    let mut bitfield = vec![0u8; num_pieces.div_ceil(8)];
    for &i in has {
        bitfield[i / 8] |= 0x80 >> (i % 8);
    }
    send(socket, PeerMessage::Bitfield { bitfield }).await;
}

/// Reads the next message, or `None` once the leecher disconnects.
async fn read_message(socket: &mut TcpStream) -> Option<PeerMessage> {
    let len = socket.read_u32().await.ok()?;
    let mut payload = vec![0u8; len as usize];
    socket.read_exact(&mut payload).await.ok()?;
    Some(PeerMessage::decode(&payload).unwrap())
}

/// Answers block requests from `data` until the leecher disconnects.
async fn serve(mut socket: TcpStream, data: &[u8]) {
    while let Some(message) = read_message(&mut socket).await {
        if let PeerMessage::Request {
            index,
            begin,
            length,
        } = message
        {
            let start = index as usize * PIECE_LENGTH as usize + begin as usize;
            let block = data[start..start + length as usize].to_vec();
            send(
                &mut socket,
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                },
            )
            .await;
        }
    }
}

/// Serves `data` to one leecher. Chokes on the first request (dropping it)
/// and corrupts the first block it sends for piece 1.
async fn seeder(listener: TcpListener, data: Arc<Vec<u8>>, info_hash: [u8; 20], num_pieces: usize) {
    let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
    send(&mut socket, PeerMessage::Unchoke).await;

    let mut choked_once = false;
    let mut corrupted_once = false;
    while let Some(message) = read_message(&mut socket).await {
        let PeerMessage::Request {
            index,
            begin,
            length,
        } = message
        else {
            continue;
        };
        if !choked_once {
            choked_once = true;
            send(&mut socket, PeerMessage::Choke).await;
            send(&mut socket, PeerMessage::Unchoke).await;
            continue;
        }
        let start = index as usize * PIECE_LENGTH as usize + begin as usize;
        let mut block = data[start..start + length as usize].to_vec();
        if index == 1 && !corrupted_once {
            corrupted_once = true;
            block[0] ^= 0xff;
        }
        send(
            &mut socket,
            PeerMessage::Piece {
                index,
                begin,
                block,
            },
        )
        .await;
    }
}

async fn send(socket: &mut TcpStream, message: PeerMessage) {
    socket.write_all(&message.encode()).await.unwrap();
}

fn init_tracing() {
    // `RUST_LOG=debug cargo test -- --nocapture` shows the choke/retry path.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn test_data() -> Vec<u8> {
    let total: usize = FILES.iter().map(|(_, len)| len).sum();
    (0..total).map(|i| (i * 31 % 251) as u8).collect()
}

fn session(torrent: TorrentFile, info_hash: [u8; 20], out: &Path) -> Arc<Session> {
    let storage = Arc::new(Storage::new(&torrent.info, out).unwrap());
    Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
    })
}

fn assert_files_match(out: &Path, data: &[u8]) {
    let mut offset = 0;
    for (name, len) in FILES {
        let written = std::fs::read(out.join("multi/dir").join(name)).unwrap();
        assert_eq!(written, data[offset..offset + len], "{name} differs");
        offset += len;
    }
}

#[tokio::test]
async fn downloads_multi_file_torrent_from_mock_seeder() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    tokio::spawn(seeder(listener, Arc::clone(&data), info_hash, num_pieces));

    // A peer that refuses connections must not break the download.
    let dead_addr: SocketAddr = {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        dead.local_addr().unwrap()
    };

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[dead_addr, seeder_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    assert_files_match(out.path(), &data);
}

/// With room for a single connection, a dead peer and one that disconnects
/// mid-download must each be replaced by the next peer from the list.
#[tokio::test]
async fn replaces_peers_that_fail() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let dead_addr: SocketAddr = {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        dead.local_addr().unwrap()
    };

    // Serves one block, then hangs up.
    let flaky = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flaky_addr = flaky.local_addr().unwrap();
    let flaky_data = Arc::clone(&data);
    let (flaky_served, flaky_served_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&flaky, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            if let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
            {
                let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                let block = flaky_data[start..start + length as usize].to_vec();
                send(
                    &mut socket,
                    PeerMessage::Piece {
                        index,
                        begin,
                        block,
                    },
                )
                .await;
                let _ = flaky_served.send(());
                return;
            }
        }
    });

    // Only accepts once the flaky peer has had its turn, so the download
    // cannot finish without replacing it.
    let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy_addr = healthy.local_addr().unwrap();
    let healthy_data = Arc::clone(&data);
    tokio::spawn(async move {
        flaky_served_rx.await.unwrap();
        let mut socket = accept_leecher(&healthy, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &healthy_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[dead_addr, flaky_addr, healthy_addr],
            1,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    assert_files_match(out.path(), &data);
}

/// One seeder takes a piece and never sends it. Endgame must fetch that piece
/// from the healthy seeder and cancel the stalled requests, instead of
/// waiting out the 30 s read timeout.
#[tokio::test]
async fn endgame_finishes_despite_a_stalled_peer() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let (stalled_requested, stalled_requested_rx) = oneshot::channel();
    let (cancel_seen, cancel_seen_rx) = oneshot::channel();
    let stalled = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stalled_addr = stalled.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&stalled, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut stalled_requested = Some(stalled_requested);
        let mut cancel_seen = Some(cancel_seen);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request { .. } => {
                    stalled_requested.take().map(|tx| tx.send(()));
                }
                PeerMessage::Cancel { .. } => {
                    cancel_seen.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
    });

    // The healthy seeder only unchokes once the stalled one holds a piece, so
    // the download is guaranteed to need endgame.
    let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy_addr = healthy.local_addr().unwrap();
    let healthy_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&healthy, info_hash, num_pieces).await;
        stalled_requested_rx.await.unwrap();
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &healthy_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[stalled_addr, healthy_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download stalled behind the slow peer")
    .expect("download failed");

    assert_files_match(out.path(), &data);
    tokio::time::timeout(Duration::from_secs(5), cancel_seen_rx)
        .await
        .expect("stalled peer never got a Cancel")
        .unwrap();
}

/// A peer that only has piece 0 and never unchokes us is sent Interested, and
/// NotInterested once another peer delivers piece 0. It is never asked for a
/// block.
#[tokio::test]
async fn loses_interest_when_a_peer_has_nothing_left_we_need() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let (choking_done, choking_done_rx) = oneshot::channel();
    let choking = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let choking_addr = choking.local_addr().unwrap();
    let choking_task = tokio::spawn(async move {
        let mut socket = accept_partial(&choking, info_hash, num_pieces, &[0]).await;
        let mut received = Vec::new();
        let mut choking_done = Some(choking_done);
        while let Some(message) = read_message(&mut socket).await {
            received.push(message);
            if received.last() == Some(&PeerMessage::NotInterested) {
                choking_done.take().map(|tx| tx.send(()));
            }
        }
        received
    });

    // Has pieces 0 and 1, so the choking peer's only piece gets downloaded.
    let partial = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let partial_addr = partial.local_addr().unwrap();
    let partial_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_partial(&partial, info_hash, num_pieces, &[0, 1]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &partial_data).await;
    });

    // Piece 2 only becomes available once the choking peer lost interest, so
    // the download is still running when that has to happen.
    let last = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let last_addr = last.local_addr().unwrap();
    let last_data = Arc::clone(&data);
    tokio::spawn(async move {
        choking_done_rx.await.unwrap();
        let mut socket = accept_partial(&last, info_hash, num_pieces, &[2]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &last_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[choking_addr, partial_addr, last_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("choking peer was never sent NotInterested")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let received = choking_task.await.unwrap();
    assert_eq!(
        received,
        [PeerMessage::Interested, PeerMessage::NotInterested]
    );
}

/// Requests are pipelined across piece boundaries: the seeder holds back its
/// replies until four requests (a new peer's pipeline depth) are queued,
/// which takes two of these two-block pieces. A per-piece pipeline would
/// stall here.
#[tokio::test]
async fn pipelines_requests_across_pieces() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(PIECE_LENGTH, 2 * 16 * 1024);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let first_batch = tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut queued = Vec::new();
        while queued.len() < 4 {
            match read_message(&mut socket).await {
                Some(PeerMessage::Request {
                    index,
                    begin,
                    length,
                }) => queued.push((index, begin, length)),
                Some(_) => {}
                None => return queued,
            }
        }
        for &(index, begin, length) in &queued {
            let start = index as usize * PIECE_LENGTH as usize + begin as usize;
            let block = seeder_data[start..start + length as usize].to_vec();
            send(
                &mut socket,
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                },
            )
            .await;
        }
        serve(socket, &seeder_data).await;
        queued
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[seeder_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("pipeline stalled at a piece boundary")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let queued = first_batch.await.unwrap();
    let mut pieces: Vec<u32> = queued.iter().map(|&(index, ..)| index).collect();
    pieces.dedup();
    assert_eq!(pieces.len(), 2, "first requests: {queued:?}");
}

/// Data left by an interrupted run is hashed at startup: intact pieces are
/// kept, and only the corrupt one is downloaded again.
#[tokio::test]
async fn resumes_from_data_on_disk() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    // Every file is in place, but piece 1 (which spans a.bin and b.bin) has
    // a corrupt byte in b.bin.
    let out = tempfile::tempdir().unwrap();
    let dir = out.path().join("multi/dir");
    std::fs::create_dir_all(&dir).unwrap();
    let mut offset = 0;
    for (name, len) in FILES {
        let mut contents = data[offset..offset + len].to_vec();
        if *name == "b.bin" {
            contents[0] ^= 0xff;
        }
        std::fs::write(dir.join(name), contents).unwrap();
        offset += len;
    }

    let session = session(torrent, info_hash, out.path());
    let candidates = verify::candidates(&session.torrent, &session.storage).unwrap();
    let have = verify::verify(&session.torrent, &session.storage, &candidates, &|_| {}).unwrap();
    assert_eq!(have, [true, false, true]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let (requested_tx, mut requested) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
            else {
                continue;
            };
            requested_tx.send(index).unwrap();
            let start = index as usize * PIECE_LENGTH as usize + begin as usize;
            let block = seeder_data[start..start + length as usize].to_vec();
            send(
                &mut socket,
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                },
            )
            .await;
        }
    });

    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session,
            &have,
            &[seeder_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let mut pieces = Vec::new();
    while let Ok(index) = requested.try_recv() {
        pieces.push(index);
    }
    assert!(!pieces.is_empty());
    assert!(
        pieces.iter().all(|&index| index == 1),
        "requested {pieces:?}"
    );
}

/// The only seeder connects to us. The one peer we know of has nothing; it
/// just keeps the download from running out of peers until the seeder
/// arrives. Our own address is in the peer list too, as a tracker would
/// list it, and connecting to it must do no harm.
#[tokio::test]
async fn downloads_from_a_peer_that_connects_to_us() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let empty = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let empty_addr = empty.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_partial(&empty, info_hash, num_pieces, &[]).await;
        while read_message(&mut socket).await.is_some() {}
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = connect_to_leecher(our_addr, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[empty_addr, our_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
}

/// A minimal HTTP tracker: answers every announce with `peers` and forwards
/// each request's target (path and query).
async fn mock_tracker(peers: Vec<SocketAddr>) -> (String, mpsc::UnboundedReceiver<String>) {
    let mut compact = Vec::new();
    for peer in &peers {
        let IpAddr::V4(ip) = peer.ip() else {
            panic!("IPv4 peers only")
        };
        compact.extend_from_slice(&ip.octets());
        compact.extend_from_slice(&peer.port().to_be_bytes());
    }
    let mut body = format!("d8:intervali1800e5:peers{}:", compact.len()).into_bytes();
    body.extend_from_slice(&compact);
    body.push(b'e');
    let (addr, targets) = mock_http(body).await;
    (format!("http://{addr}/announce"), targets)
}

/// A minimal HTTP server: answers every request with `body` and forwards
/// each request's target (path and query).
async fn mock_http(body: Vec<u8>) -> (SocketAddr, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (targets, targets_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0, "tracker connection closed mid-request");
                request.extend_from_slice(&buf[..n]);
            }
            let request = String::from_utf8(request).unwrap();
            let target = request.split(' ').nth(1).unwrap();
            let _ = targets.send(target.to_string());
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    (addr, targets_rx)
}

/// The value of `key` in the query of a request target.
fn query_param<'a>(target: &'a str, key: &str) -> Option<&'a str> {
    let (_, query) = target.split_once('?')?;
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// The tracker hears `started` with everything left, then `completed` and
/// `stopped` reporting the full download.
#[tokio::test]
async fn reports_events_and_progress_to_the_tracker() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;

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
    assert_eq!(peers, [seeder_addr]);

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &peers,
            10,
            Vec::new(),
            Some(announcer),
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let total = total.to_string();
    let mut announces = Vec::new();
    while let Ok(query) = queries.try_recv() {
        let param = |key| query_param(&query, key).unwrap_or("").to_string();
        announces.push([param("event"), param("downloaded"), param("left")]);
    }
    assert_eq!(
        announces,
        [
            ["started".into(), "0".into(), total.clone()],
            ["completed".into(), total.clone(), "0".into()],
            ["stopped".into(), total, "0".into()],
        ]
    );
}

/// Interrupted after one piece: the download stops with `Interrupted`, the
/// piece stays on disk, and the tracker hears `stopped` (never `completed`)
/// with that piece counted.
#[tokio::test]
async fn interrupted_download_keeps_pieces_and_tells_the_tracker() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;

    // Only has piece 0. Once it is written we lose interest, and that is
    // when the download gets interrupted.
    let (not_interested, not_interested_rx) = oneshot::channel();
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_partial(&seeder, info_hash, num_pieces, &[0]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut not_interested = Some(not_interested);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                    let block = seeder_data[start..start + length as usize].to_vec();
                    send(
                        &mut socket,
                        PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        },
                    )
                    .await;
                }
                PeerMessage::NotInterested => {
                    not_interested.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
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

    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());
    let shutdown = async {
        not_interested_rx.await.unwrap();
    };
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            Arc::clone(&session),
            &vec![false; num_pieces],
            &peers,
            10,
            Vec::new(),
            Some(announcer),
            shutdown,
        ),
    )
    .await
    .expect("interrupted download did not stop")
    .expect_err("an interrupted download must fail");
    let interrupted = err
        .downcast_ref::<Interrupted>()
        .unwrap_or_else(|| panic!("not an interruption: {err:#}"));
    assert_eq!((interrupted.remaining, interrupted.pieces), (2, num_pieces));

    let all: Vec<usize> = (0..num_pieces).collect();
    let have = verify::verify(&session.torrent, &session.storage, &all, &|_| {}).unwrap();
    assert_eq!(have, [true, false, false]);

    let piece = PIECE_LENGTH.to_string();
    let left = (total - PIECE_LENGTH).to_string();
    let mut announces = Vec::new();
    while let Ok(query) = queries.try_recv() {
        let param = |key| query_param(&query, key).unwrap_or("").to_string();
        announces.push([param("event"), param("downloaded"), param("left")]);
    }
    assert_eq!(
        announces,
        [
            ["started".into(), "0".into(), total.to_string()],
            ["stopped".into(), piece, left],
        ]
    );
}

/// A minimal UDP tracker (BEP 15): answers connects, and answers each scrape
/// with `stats` (seeders, completed, leechers) for every torrent asked
/// about, or with an error saying `error` if one is given.
async fn mock_udp_tracker(stats: [u32; 3], error: Option<&'static str>) -> String {
    const CONNECTION_ID: u64 = 0x0123_4567_89ab_cdef;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            let (n, from) = socket.recv_from(&mut buf).await.unwrap();
            let request = &buf[..n];
            let action = u32::from_be_bytes(request[8..12].try_into().unwrap());
            let transaction = &request[12..16];
            let mut response = Vec::new();
            match (action, error) {
                (0, _) => {
                    assert_eq!(request[..8], 0x417_2710_1980u64.to_be_bytes());
                    response.extend_from_slice(&0u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    response.extend_from_slice(&CONNECTION_ID.to_be_bytes());
                }
                (2, Some(error)) => {
                    response.extend_from_slice(&3u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    response.extend_from_slice(error.as_bytes());
                }
                (2, None) => {
                    assert_eq!(request[..8], CONNECTION_ID.to_be_bytes());
                    let hashes = &request[16..];
                    assert_eq!(hashes.len() % 20, 0, "partial info hash");
                    response.extend_from_slice(&2u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    for _ in hashes.chunks(20) {
                        for count in stats {
                            response.extend_from_slice(&count.to_be_bytes());
                        }
                    }
                }
                _ => panic!("unexpected action {action}"),
            }
            socket.send_to(&response, from).await.unwrap();
        }
    });
    url
}

/// An HTTP scrape body listing `info_hash` with 5 seeders, 10 leechers and
/// 50 completed downloads.
fn http_scrape_body(info_hash: [u8; 20]) -> Vec<u8> {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend_from_slice(&info_hash);
    body.extend_from_slice(b"d8:completei5e10:downloadedi50e10:incompletei10eeee");
    body
}

/// A tracker URL on a port nobody listens on.
async fn dead_tracker() -> String {
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    format!("http://{}/announce", dead.local_addr().unwrap())
}

/// Every tracker is asked, and each answer is reported in tier order: HTTP
/// and UDP counts, a UDP error response and an unsupported tracker.
#[tokio::test]
async fn scrapes_http_and_udp_trackers() {
    init_tracing();
    // Not valid UTF-8, as the `files` keys are raw bytes.
    let info_hash = [0xc3; 20];
    let (addr, mut targets) = mock_http(http_scrape_body(info_hash)).await;
    let http = format!("http://{addr}/announce?passkey=x");
    let udp = mock_udp_tracker([7, 70, 3], None).await;
    let refusing = mock_udp_tracker([0; 3], Some("torrent not registered")).await;
    let tiers = vec![
        vec![http.clone()],
        vec![udp.clone(), refusing.clone(), "wss://t/announce".into()],
    ];
    let scrapes = tokio::time::timeout(Duration::from_secs(10), scrape_tiers(&tiers, info_hash))
        .await
        .expect("scrape timed out");

    let summary: Vec<_> = scrapes
        .iter()
        .map(|s| {
            let result = s.result.as_ref().map_err(|e| format!("{e:#}"));
            (s.tier, s.tracker.as_str(), result.cloned())
        })
        .collect();
    let stats = |seeders, leechers, completed| {
        Ok(Some(ScrapeStats {
            seeders,
            leechers,
            completed,
        }))
    };
    assert_eq!(summary[0], (1, http.as_str(), stats(5, 10, 50)));
    assert_eq!(summary[1], (2, udp.as_str(), stats(7, 3, 70)));
    assert_eq!(summary[2].1, refusing);
    let Err(error) = &summary[2].2 else {
        panic!("UDP error response not reported: {summary:?}");
    };
    assert!(error.contains("torrent not registered"), "{error}");
    assert_eq!(summary[3].1, "wss://t/announce");
    assert!(summary[3].2.is_err());
    assert_eq!(summary.len(), 4);

    let target = targets.try_recv().unwrap();
    assert_eq!(
        target,
        format!("/scrape?passkey=x&info_hash={}", "%c3".repeat(20))
    );
}

/// A UDP scrape of more torrents than fit in a packet is split into several
/// requests, and the answers come back in the order asked.
#[tokio::test]
async fn udp_scrape_of_many_torrents() {
    init_tracing();
    let udp = mock_udp_tracker([1, 2, 3], None).await;
    let info_hashes: Vec<[u8; 20]> = (0..100u8).map(|i| [i; 20]).collect();
    let stats = tokio::time::timeout(Duration::from_secs(10), scrape(&udp, &info_hashes))
        .await
        .expect("scrape timed out")
        .unwrap();
    let expected = ScrapeStats {
        seeders: 1,
        leechers: 3,
        completed: 2,
    };
    assert_eq!(stats, vec![Some(expected); 100]);
}

/// `--scrape` prints one line per tracker and fails only when no tracker
/// answers.
#[tokio::test]
async fn scrape_command_reports_each_tracker() {
    init_tracing();
    let (mut torrent, info_hash) = make_torrent(&test_data());
    let (addr, _targets) = mock_http(http_scrape_body(info_hash)).await;
    let http = format!("http://{addr}/announce");
    let dead = dead_tracker().await;
    let dir = tempfile::tempdir().unwrap();

    let run = async |torrent: &TorrentFile| {
        let path = dir.path().join("t.torrent");
        std::fs::write(&path, serde_bencode::to_bytes(torrent).unwrap()).unwrap();
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_rtrnt"))
            .arg("--scrape")
            .arg(&path)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(20), output)
            .await
            .expect("--scrape timed out")
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        (output.status.success(), stdout, stderr)
    };

    torrent.announce_list = Some(vec![vec![http.clone()], vec![dead.clone()]]);
    let (success, stdout, stderr) = run(&torrent).await;
    assert!(success, "{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "{stdout}");
    assert_eq!(
        lines[0],
        format!("tier 1  {http}  5 seeders, 10 leechers, 50 completed")
    );
    assert!(lines[1].starts_with(&format!("tier 2  {dead}  failed: ")));

    torrent.announce_list = Some(vec![vec![dead]]);
    let (success, _, stderr) = run(&torrent).await;
    assert!(!success);
    assert!(stderr.contains("no tracker answered"), "{stderr}");
}

/// `listen` listens over IPv6 too, on the port the IPv4 listener got. Two
/// seeders connect to us on that port, one over `127.0.0.1` with piece 0
/// and one over `[::1]` with the rest, so the download needs both. The one
/// peer we know of, which keeps the download from running out of peers
/// until the seeders arrive, is reached over IPv6 as well. Skipped where
/// IPv6 is unavailable.
#[tokio::test]
async fn downloads_from_peers_that_connect_over_ipv4_and_ipv6() {
    init_tracing();
    if TcpListener::bind("[::1]:0").await.is_err() {
        eprintln!("skipping: IPv6 is not available");
        return;
    }
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let empty = TcpListener::bind("[::1]:0").await.unwrap();
    let empty_addr = empty.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_partial(&empty, info_hash, num_pieces, &[]).await;
        while read_message(&mut socket).await.is_some() {}
    });

    let listeners = listen(0).await;
    let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    assert_eq!(addrs.len(), 2, "not listening over both: {addrs:?}");
    assert!(addrs[0].is_ipv4() && addrs[1].is_ipv6(), "{addrs:?}");
    assert_eq!(addrs[0].port(), addrs[1].port());
    let port = addrs[0].port();

    for (ip, has) in [
        (IpAddr::from(Ipv4Addr::LOCALHOST), vec![0]),
        (IpAddr::from(Ipv6Addr::LOCALHOST), vec![1, 2]),
    ] {
        let seeder_data = Arc::clone(&data);
        tokio::spawn(async move {
            let our_addr = SocketAddr::new(ip, port);
            let mut socket = connect_partial(our_addr, info_hash, num_pieces, &has).await;
            send(&mut socket, PeerMessage::Unchoke).await;
            serve(socket, &seeder_data).await;
        });
    }

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[empty_addr],
            10,
            listeners,
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
}
