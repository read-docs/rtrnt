//! End-to-end downloads from mock seeders (and a mock tracker) over loopback.

use rtrnt::announcer::Announcer;
use rtrnt::dht::Dht;
use rtrnt::download::{
    DownloadConfig, Interrupted, SeedLimits, Session, download_file, download_from_peers, listen,
};
use rtrnt::extension::ExtendedHandshake;
use rtrnt::fast;
use rtrnt::peer::{PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::pex::{self, PexMessage};
use rtrnt::rate_limit::RateLimits;
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
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
        private: None,
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

/// A mock peer's handshake: ours, but without the fast extension (BEP 6),
/// which mock peers only support where a test says so.
fn mock_handshake(info_hash: &[u8; 20]) -> [u8; 68] {
    let mut handshake = handshake_bytes(info_hash, &generate_peer_id());
    handshake[20 + fast::RESERVED_BYTE] &= !fast::RESERVED_BIT;
    handshake
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
    socket.write_all(&mock_handshake(&info_hash)).await.unwrap();
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
    let mut socket = connect_to(addr, info_hash).await;
    send_bitfield(&mut socket, num_pieces, has).await;
    socket
}

/// Connects to the client listening at `addr` and completes the handshake,
/// as a peer with no pieces (which need not send a bitfield).
async fn connect_to(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_all(&mock_handshake(&info_hash)).await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    parse_handshake(&handshake, &info_hash).unwrap();
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
async fn read_message(socket: &mut (impl AsyncRead + Unpin)) -> Option<PeerMessage> {
    let len = socket.read_u32().await.ok()?;
    let mut payload = vec![0u8; len as usize];
    socket.read_exact(&mut payload).await.ok()?;
    Some(PeerMessage::decode(&payload).unwrap())
}

/// Reads the next message other than a keep-alive or the extension
/// handshake, or `None` once the client disconnects. Fails if nothing comes
/// for 5 s.
async fn next_message(socket: &mut TcpStream) -> Option<PeerMessage> {
    let next = async {
        loop {
            match read_message(socket).await {
                Some(PeerMessage::KeepAlive | PeerMessage::Extended { .. }) => {}
                other => return other,
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .expect("client went silent")
}

/// Sends several messages in one write, so they arrive together.
async fn send_batch(socket: &mut TcpStream, messages: &[PeerMessage]) {
    let bytes: Vec<u8> = messages.iter().flat_map(PeerMessage::encode).collect();
    socket.write_all(&bytes).await.unwrap();
}

fn request(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::Request {
        index,
        begin,
        length,
    }
}

/// The Piece message answering a request for `length` bytes of `data` at
/// `begin` in piece `index`.
fn piece(data: &[u8], index: u32, begin: u32, length: u32) -> PeerMessage {
    let start = index as usize * PIECE_LENGTH as usize + begin as usize;
    PeerMessage::Piece {
        index,
        begin,
        block: data[start..start + length as usize].to_vec(),
    }
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
    seeding_session(torrent, info_hash, out, SeedLimits::default())
}

fn seeding_session(
    torrent: TorrentFile,
    info_hash: [u8; 20],
    out: &Path,
    seed: SeedLimits,
) -> Arc<Session> {
    limited_session(torrent, info_hash, out, seed, RateLimits::default())
}

fn limited_session(
    torrent: TorrentFile,
    info_hash: [u8; 20],
    out: &Path,
    seed: SeedLimits,
    rate_limits: RateLimits,
) -> Arc<Session> {
    let storage = Arc::new(Storage::new(&torrent.info, out).unwrap());
    Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
        seed,
        dht: None,
        rate_limits,
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
            // The extension handshake that opens every connection.
            if matches!(message, PeerMessage::Extended { .. }) {
                continue;
            }
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

    // It also hears Have for the pieces it lacks, as they complete.
    let mut received = choking_task.await.unwrap();
    received.retain(|message| !matches!(message, PeerMessage::Have { .. }));
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

/// The seeder supports the extension protocol and says in its extension
/// handshake that it queues only two requests. It answers only once no more
/// requests arrive, so every request we were willing to send is outstanding
/// by then, and there must never be more than two (a new peer's pipeline
/// would otherwise allow four).
#[tokio::test]
async fn never_exceeds_the_request_queue_a_peer_announces() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    const REQQ: usize = 2;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        assert!(
            parse_handshake(&handshake, &info_hash)
                .unwrap()
                .supports_extensions()
        );
        // `mock_handshake` sets the extension protocol bit.
        socket.write_all(&mock_handshake(&info_hash)).await.unwrap();
        let theirs = ExtendedHandshake {
            client: Some("mock seeder".into()),
            request_queue: Some(REQQ as u32),
            ..Default::default()
        };
        let payload = theirs.encode().unwrap();
        send(&mut socket, PeerMessage::Extended { id: 0, payload }).await;
        let all: Vec<usize> = (0..num_pieces).collect();
        send_bitfield(&mut socket, num_pieces, &all).await;
        send(&mut socket, PeerMessage::Unchoke).await;

        // Read on a separate task, so waiting for the next message can time
        // out without losing part of one.
        let (mut reader, mut writer) = socket.into_split();
        let (messages_tx, mut messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(len) = reader.read_u32().await {
                let mut payload = vec![0u8; len as usize];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                let _ = messages_tx.send(PeerMessage::decode(&payload).unwrap());
            }
        });

        // Ours comes first.
        let Some(PeerMessage::Extended { id: 0, payload }) = messages.recv().await else {
            panic!("no extension handshake");
        };
        let ours = ExtendedHandshake::decode(&payload).unwrap();
        assert!(ours.client.unwrap().starts_with("rtrnt "));
        assert!(ours.request_queue.is_some());

        let mut outstanding = Vec::new();
        let mut most_outstanding = 0;
        loop {
            match tokio::time::timeout(Duration::from_millis(100), messages.recv()).await {
                Ok(Some(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })) => {
                    outstanding.push((index, begin, length));
                    most_outstanding = most_outstanding.max(outstanding.len());
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                // No more requests are coming: answer them all.
                Err(_) => {
                    for (index, begin, length) in outstanding.drain(..) {
                        let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                        let block = seeder_data[start..start + length as usize].to_vec();
                        let message = PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        };
                        writer.write_all(&message.encode()).await.unwrap();
                    }
                }
            }
        }
        most_outstanding
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
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
    assert_eq!(seeder.await.unwrap(), REQQ);
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

/// No tracker and no peer list: the only seeder announced itself in the
/// DHT, and the download finds it there. The DHT runs over loopback: a hub
/// to bootstrap from and one node to hold the announce.
#[tokio::test]
async fn downloads_from_a_peer_found_in_the_dht() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_port = seeder.local_addr().unwrap().port();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        // Its first message is our DHT port, which `serve` ignores.
        serve(socket, &seeder_data).await;
    });

    let dht_node = |routers: Vec<String>| async move {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        Dht::start(socket, routers).unwrap()
    };
    let hub = dht_node(Vec::new()).await;
    let routers = vec![hub.local_addr().to_string()];
    let holder = dht_node(routers.clone()).await;
    holder.bootstrap().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while hub.node_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the hub never added the holder");
    let seeder_dht = dht_node(routers.clone()).await;
    seeder_dht.get_peers(info_hash, Some(seeder_port)).await;

    let out = tempfile::tempdir().unwrap();
    let mut session = session(torrent, info_hash, out.path());
    Arc::get_mut(&mut session).unwrap().dht = Some(dht_node(routers).await);
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &[],
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

const BLOCK: u32 = 16 * 1024;

/// Writes every file of the torrent in place, as a finished download would.
fn write_files(out: &Path, data: &[u8]) {
    let dir = out.join("multi/dir");
    std::fs::create_dir_all(&dir).unwrap();
    let mut offset = 0;
    for (name, len) in FILES {
        std::fs::write(dir.join(name), &data[offset..offset + len]).unwrap();
        offset += len;
    }
}

/// A seeder of piece 1 alone that only unchokes us once `release` fires, so
/// the download keeps running until then.
async fn gated_seeder(
    data: Arc<Vec<u8>>,
    info_hash: [u8; 20],
    num_pieces: usize,
    release: oneshot::Receiver<()>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_partial(&listener, info_hash, num_pieces, &[1]).await;
        release.await.unwrap();
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &data).await;
    });
    addr
}

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

/// Asks the client over `socket` for every block of the torrent once it
/// unchokes us, and checks what comes back.
async fn leech_everything(socket: &mut TcpStream, data: &[u8]) {
    send(socket, PeerMessage::Interested).await;
    loop {
        match next_message(socket).await {
            Some(PeerMessage::Unchoke) => break,
            Some(PeerMessage::Bitfield { .. } | PeerMessage::Have { .. }) => {}
            other => panic!("expected an unchoke, got {other:?}"),
        }
    }
    let mut requests = Vec::new();
    for (index, chunk) in data.chunks(PIECE_LENGTH as usize).enumerate() {
        for begin in (0..chunk.len()).step_by(BLOCK as usize) {
            let length = (chunk.len() - begin).min(BLOCK as usize);
            requests.push((index as u32, begin as u32, length as u32));
        }
    }
    let messages: Vec<PeerMessage> = requests
        .iter()
        .map(|&(index, begin, length)| request(index, begin, length))
        .collect();
    send_batch(socket, &messages).await;
    for (index, begin, length) in requests {
        assert_eq!(
            next_message(socket).await,
            Some(piece(data, index, begin, length))
        );
    }
}

/// The `event`, `downloaded`, `uploaded` and `left` of each announce.
fn announces(queries: &[String]) -> Vec<[String; 4]> {
    queries
        .iter()
        .map(|query| {
            let param = |key| query_param(query, key).unwrap_or("").to_string();
            [
                param("event"),
                param("downloaded"),
                param("uploaded"),
                param("left"),
            ]
        })
        .collect()
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
        listen_port: 0,
        seed: SeedLimits {
            ratio: 1.0,
            time: Duration::ZERO,
        },
        // Tests stay off the real internet.
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
    };
    tokio::time::timeout(
        Duration::from_secs(20),
        download_file(&torrent_path, &config, pending()),
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

/// Serves every block asked for to the leecher that connects on `listener`,
/// as a seed, and returns how many block bytes it sent.
async fn counting_seeder(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    info_hash: [u8; 20],
    num_pieces: usize,
) -> u64 {
    let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
    send(&mut socket, PeerMessage::Unchoke).await;
    let mut sent = 0;
    while let Some(message) = read_message(&mut socket).await {
        if let PeerMessage::Request {
            index,
            begin,
            length,
        } = message
        {
            send(&mut socket, piece(&data, index, begin, length)).await;
            sent += u64::from(length);
        }
    }
    sent
}

/// The rate limit used by the tests below, in KiB/s.
const TEST_RATE_KIB: u64 = 32;

/// The shortest time `bytes` can take at `TEST_RATE_KIB`, once the burst
/// (one block, at this rate) has gone out at once, less a fifth so that a
/// slow machine cannot make the test flaky.
fn shortest_limited_time(bytes: u64) -> Duration {
    let rate = (TEST_RATE_KIB * 1024) as f64;
    Duration::from_secs_f64((bytes - u64::from(BLOCK)) as f64 / rate * 0.8)
}

/// With a download limit, two fast seeders share it: the download takes at
/// least as long as the limit allows, and both get to send blocks.
#[tokio::test]
async fn download_rate_limit_is_shared_by_all_peers() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let mut addrs = Vec::new();
    let mut seeders = Vec::new();
    for _ in 0..2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        let data = Arc::clone(&data);
        seeders.push(tokio::spawn(counting_seeder(
            listener, data, info_hash, num_pieces,
        )));
    }

    let out = tempfile::tempdir().unwrap();
    let limits = RateLimits::from_kib(TEST_RATE_KIB, 0);
    let session = limited_session(
        torrent,
        info_hash,
        out.path(),
        SeedLimits::default(),
        limits,
    );
    let start = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &addrs,
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    let elapsed = start.elapsed();
    assert_files_match(out.path(), &data);

    let total = data.len() as u64;
    assert!(
        elapsed >= shortest_limited_time(total),
        "{total} bytes in {elapsed:?} at {TEST_RATE_KIB} KiB/s"
    );
    for seeder in seeders {
        let sent = seeder.await.unwrap();
        assert!(sent >= u64::from(BLOCK), "a seeder only sent {sent} bytes");
    }
}

/// With an upload limit, a leecher that asks for the whole torrent at once
/// gets it no faster than the limit allows.
#[tokio::test]
async fn upload_rate_limit_paces_the_blocks_we_send() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        let mut socket = connect_to(our_addr, info_hash).await;
        leech_everything(&mut socket, &leecher_data).await;
    });

    // Seeding to a ratio of 1 ends the run once the leecher has everything.
    let seed = SeedLimits {
        ratio: 1.0,
        time: Duration::ZERO,
    };
    let limits = RateLimits::from_kib(0, TEST_RATE_KIB);
    let session = limited_session(torrent, info_hash, out.path(), seed, limits);
    let start = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![true; num_pieces],
            &[],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("seeding did not stop")
    .expect("seeding failed");
    let elapsed = start.elapsed();
    leecher.await.unwrap();

    let total = data.len() as u64;
    assert!(
        elapsed >= shortest_limited_time(total),
        "{total} bytes in {elapsed:?} at {TEST_RATE_KIB} KiB/s"
    );
}

/// The torrent of `make_torrent`, marked private (BEP 27).
fn make_private_torrent(data: &[u8]) -> (TorrentFile, [u8; 20]) {
    let (mut torrent, _) = make_torrent(data);
    torrent.info.private = Some(1);
    let info_hash = sha1::Sha1::digest(serde_bencode::to_bytes(&torrent.info).unwrap()).into();
    (torrent, info_hash)
}

/// Sends an extension handshake that offers peer exchange under `pex_id`,
/// with `listen_port` as `p`.
async fn send_pex_handshake(socket: &mut TcpStream, pex_id: u8, listen_port: Option<u16>) {
    let theirs = ExtendedHandshake {
        extensions: [(pex::NAME.to_string(), pex_id)].into(),
        listen_port,
        ..Default::default()
    };
    let payload = theirs.encode().unwrap();
    send(socket, PeerMessage::Extended { id: 0, payload }).await;
}

/// Reads messages until an extended message with extended id `id`, and
/// returns its payload. Fails if the client disconnects first, or if none
/// comes within 5 s.
async fn next_extended(socket: &mut TcpStream, id: u8) -> Vec<u8> {
    let next = async {
        loop {
            match read_message(socket).await {
                Some(PeerMessage::Extended { id: got, payload }) if got == id => return payload,
                Some(_) => {}
                None => panic!("client disconnected"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), next)
        .await
        .expect("no extended message")
}

/// L, our client, knows of one peer: H, our client too, which has nothing
/// yet. H is connected to the seeder S, but S has nothing for H, only for
/// the next peer to connect. H tells L about S through peer exchange (it
/// knows where S listens, having connected to it), and L downloads
/// everything from S.
#[tokio::test]
async fn downloads_from_a_peer_learned_through_pex() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let (h_connected, h_connected_rx) = oneshot::channel();
    let seeder_data = Arc::clone(&data);
    let seeder_task = tokio::spawn(async move {
        let h = accept_partial(&seeder, info_hash, num_pieces, &[]).await;
        h_connected.send(()).unwrap();
        let mut l = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut l, PeerMessage::Unchoke).await;
        serve(l, &seeder_data).await;
        drop(h);
    });

    let h_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let h_addr = h_listener.local_addr().unwrap();
    let h_out = tempfile::tempdir().unwrap();
    let h_session = session(torrent.clone(), info_hash, h_out.path());
    let (stop_h, stop_h_rx) = oneshot::channel::<()>();
    let h = tokio::spawn(async move {
        let have = vec![false; num_pieces];
        let stop = async {
            let _ = stop_h_rx.await;
        };
        download_from_peers(
            h_session,
            &have,
            &[seeder_addr],
            10,
            vec![h_listener],
            None,
            stop,
        )
        .await
    });
    h_connected_rx.await.unwrap();

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[h_addr],
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
    seeder_task.await.unwrap();

    stop_h.send(()).unwrap();
    let err = h.await.unwrap().unwrap_err();
    assert!(err.is::<Interrupted>(), "{err:#}");
}

/// Peers that support peer exchange hear about the other peers we are
/// connected to whose listening address we know, with their flags, but
/// never about themselves. X is a seed we connected to; Z, Y and W connect
/// to us in turn, and only Y gives its listening port.
#[tokio::test]
async fn tells_peers_about_each_other_through_pex() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    // X never unchokes us, so the client stays connected without
    // downloading anything.
    let x = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let x_addr = x.local_addr().unwrap();
    let (x_ready, x_ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&x, info_hash, num_pieces).await;
        // Interest shows that the client has seen X's bitfield.
        while next_message(&mut socket).await != Some(PeerMessage::Interested) {}
        x_ready.send(()).unwrap();
        while read_message(&mut socket).await.is_some() {}
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());
    let (stop, stop_rx) = oneshot::channel::<()>();
    let client = tokio::spawn(async move {
        let have = vec![false; num_pieces];
        let stop = async {
            let _ = stop_rx.await;
        };
        download_from_peers(session, &have, &[x_addr], 10, vec![listener], None, stop).await
    });
    x_ready_rx.await.unwrap();

    let mut z = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut z, 9, None).await;
    let x_flags = pex::FLAG_SEED | pex::FLAG_CONNECTABLE;
    let to_z = PexMessage::decode(&next_extended(&mut z, 9).await).unwrap();
    assert_eq!(to_z.added, [(x_addr, x_flags)]);

    let y_listen = SocketAddr::from((Ipv4Addr::LOCALHOST, 5555));
    let mut y = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut y, 7, Some(y_listen.port())).await;
    let ours = ExtendedHandshake::decode(&next_extended(&mut y, 0).await).unwrap();
    assert_eq!(ours.id(pex::NAME), Some(pex::ID));
    let to_y = PexMessage::decode(&next_extended(&mut y, 7).await).unwrap();
    assert_eq!(to_y.added, [(x_addr, x_flags)]);
    assert!(to_y.dropped.is_empty());

    let mut w = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut w, 8, None).await;
    let mut to_w = PexMessage::decode(&next_extended(&mut w, 8).await).unwrap();
    to_w.added.sort();
    let mut expected = vec![(x_addr, x_flags), (y_listen, 0)];
    expected.sort();
    assert_eq!(to_w.added, expected);

    stop.send(()).unwrap();
    let err = client.await.unwrap().unwrap_err();
    assert!(err.is::<Interrupted>(), "{err:#}");
}

/// Private torrents (BEP 27) never use peer exchange: we do not offer it,
/// and a peer that sends a peer exchange message anyway is not listened to.
#[tokio::test]
async fn private_torrents_do_not_exchange_peers() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_private_torrent(&data);
    assert!(torrent.is_private());
    let num_pieces = torrent.num_pieces();

    // Only the peer exchange message names it.
    let decoy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let decoy_addr = decoy.local_addr().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        let ours = ExtendedHandshake::decode(&next_extended(&mut socket, 0).await).unwrap();
        assert_eq!(ours.id(pex::NAME), None);
        send_pex_handshake(&mut socket, 3, None).await;
        let message = PexMessage {
            added: vec![(decoy_addr, pex::FLAG_CONNECTABLE)],
            dropped: Vec::new(),
        };
        let payload = message.encode().unwrap();
        send(
            &mut socket,
            PeerMessage::Extended {
                id: pex::ID,
                payload,
            },
        )
        .await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
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
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
    seeder.await.unwrap();
    let connected = tokio::time::timeout(Duration::from_millis(200), decoy.accept()).await;
    assert!(
        connected.is_err(),
        "connected to a peer named by peer exchange"
    );
}

/// Accepts the client's connection as a peer that supports the fast
/// extension (BEP 6), checking that the client does too.
async fn accept_fast(listener: &TcpListener, info_hash: [u8; 20]) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert!(
        parse_handshake(&handshake, &info_hash)
            .unwrap()
            .supports_fast()
    );
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    socket
}

/// Connects to the client listening at `addr` as a peer that supports the
/// fast extension (BEP 6), checking that the client does too.
async fn connect_fast(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert!(
        parse_handshake(&handshake, &info_hash)
            .unwrap()
            .supports_fast()
    );
    socket
}

fn reject(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::RejectRequest {
        index,
        begin,
        length,
    }
}

fn cancel(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::Cancel {
        index,
        begin,
        length,
    }
}

/// Several messages encoded back to back, to send in one write.
fn encode_all(messages: &[PeerMessage]) -> Vec<u8> {
    messages.iter().flat_map(PeerMessage::encode).collect()
}

/// A fast peer (BEP 6) with no pieces connects to us while we download
/// piece 1. It says Have None, and gets our Bitfield and then our allowed
/// fast set, which in a torrent of three pieces is all of them. It gets an
/// allowed fast block while still choked, and once unchoked, a Reject for a
/// request it cancels before the block was sent.
#[tokio::test]
async fn serves_a_fast_peer_its_allowed_fast_pieces_while_choked() {
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
        let mut socket = connect_fast(our_addr, info_hash).await;
        send(&mut socket, PeerMessage::HaveNone).await;
        let bitfield = vec![0b1010_0000];
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { bitfield })
        );
        let set = fast::allowed_fast_set(
            IpAddr::from(Ipv4Addr::LOCALHOST),
            &info_hash,
            num_pieces as u32,
            fast::ALLOWED_FAST_COUNT,
        );
        assert_eq!(set.len(), num_pieces);
        for piece_index in set {
            assert_eq!(
                next_message(&mut socket).await,
                Some(PeerMessage::AllowedFast { piece_index })
            );
        }

        // Choked, and not even interested, but piece 0 is allowed fast.
        send(&mut socket, request(0, 0, BLOCK)).await;
        assert_eq!(
            next_message(&mut socket).await,
            Some(piece(data, 0, 0, BLOCK))
        );

        send(&mut socket, PeerMessage::Interested).await;
        assert_eq!(next_message(&mut socket).await, Some(PeerMessage::Unchoke));
        // The cancel arrives with the requests, before its block is sent.
        let batch = [
            request(2, 0, BLOCK),
            request(0, BLOCK, BLOCK),
            cancel(0, BLOCK, BLOCK),
        ];
        send_batch(&mut socket, &batch).await;
        let answers = [
            next_message(&mut socket).await.unwrap(),
            next_message(&mut socket).await.unwrap(),
        ];
        for expected in [reject(0, BLOCK, BLOCK), piece(data, 2, 0, BLOCK)] {
            assert!(answers.contains(&expected), "missing {expected:?}");
        }

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

/// The next message from a split-off reader, or `None` after `wait` of
/// silence or once the client hangs up.
async fn recv_within(
    messages: &mut mpsc::UnboundedReceiver<PeerMessage>,
    wait: Duration,
) -> Option<PeerMessage> {
    tokio::time::timeout(wait, messages.recv())
        .await
        .ok()
        .flatten()
}

/// The whole download comes from a fast peer (BEP 6) that has every piece
/// and puts the extension's rules to work:
///
/// - We have nothing yet, so it hears Have None.
/// - While it chokes us we ask only for the piece it allows us fast. When
///   it rejects one of those blocks anyway, we stop asking for the piece
///   until unchoked, and let go of it once nothing of it is outstanding.
/// - A choke no longer drops our requests: a block sent after the Choke is
///   still taken, and the rest wait for their Rejects.
/// - A block rejected while we are unchoked is asked for again.
#[tokio::test]
async fn downloads_from_a_fast_peer_that_chokes_and_rejects() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let data = &seeder_data;
        let socket = accept_fast(&listener, info_hash).await;
        // Read on a separate task, so waiting for the next message can time
        // out without losing part of one.
        let (mut reader, mut writer) = socket.into_split();
        let (messages_tx, mut messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(message) = read_message(&mut reader).await {
                if !matches!(
                    message,
                    PeerMessage::KeepAlive | PeerMessage::Extended { .. }
                ) {
                    let _ = messages_tx.send(message);
                }
            }
        });
        let long = Duration::from_secs(5);
        let quiet = Duration::from_millis(300);

        let hello = [
            PeerMessage::HaveAll,
            PeerMessage::AllowedFast { piece_index: 1 },
        ];
        writer.write_all(&encode_all(&hello)).await.unwrap();
        // A seed gets no allowed fast set from us.
        for expected in [PeerMessage::HaveNone, PeerMessage::Interested] {
            assert_eq!(recv_within(&mut messages, long).await, Some(expected));
        }
        for expected in [request(1, 0, BLOCK), request(1, BLOCK, BLOCK)] {
            assert_eq!(recv_within(&mut messages, long).await, Some(expected));
        }
        let answer = [reject(1, BLOCK, BLOCK), piece(data, 1, 0, BLOCK)];
        writer.write_all(&encode_all(&answer)).await.unwrap();
        assert_eq!(
            recv_within(&mut messages, quiet).await,
            None,
            "asked for a rejected piece while choked"
        );

        // Unchoked: everything the pipeline allows is asked for.
        let unchoke = PeerMessage::Unchoke.encode();
        writer.write_all(&unchoke).await.unwrap();
        let mut asked = Vec::new();
        while let Some(message) = recv_within(&mut messages, quiet).await {
            match message {
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => asked.push((index, begin, length)),
                other => panic!("expected a request, got {other:?}"),
            }
        }
        assert!(asked.len() >= 2, "asked for {asked:?}");
        let mut answer = vec![PeerMessage::Choke];
        for (i, &(index, begin, length)) in asked.iter().enumerate() {
            answer.push(if i == 0 {
                piece(data, index, begin, length)
            } else {
                reject(index, begin, length)
            });
        }
        writer.write_all(&encode_all(&answer)).await.unwrap();
        assert_eq!(
            recv_within(&mut messages, quiet).await,
            None,
            "asked for a block while choked"
        );

        // Unchoked again: serves everything, but rejects the first request
        // once, so it must be asked for again.
        writer.write_all(&unchoke).await.unwrap();
        let mut rejected = None;
        let mut asked_again = false;
        while let Some(message) = recv_within(&mut messages, long).await {
            let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
            else {
                continue;
            };
            let reply = match rejected {
                None => {
                    rejected = Some((index, begin, length));
                    reject(index, begin, length)
                }
                Some(first) => {
                    asked_again |= first == (index, begin, length);
                    piece(data, index, begin, length)
                }
            };
            writer.write_all(&reply.encode()).await.unwrap();
        }
        asked_again
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
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
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
    assert!(seeder.await.unwrap(), "rejected block never asked again");
}

/// We have pieces 0 and 2, and the only peer is a fast peer (BEP 6) that
/// has every piece and keeps us choked. Its request while we choke it gets
/// a Reject rather than silence, and a Reject for a request we never sent
/// gets it banned.
#[tokio::test]
async fn rejects_a_choked_fast_peer_and_bans_one_that_rejects_unsent_requests() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut socket = accept_fast(&listener, info_hash).await;
        send(&mut socket, PeerMessage::HaveAll).await;
        let bitfield = vec![0b1010_0000];
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { bitfield })
        );
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Interested)
        );
        send(&mut socket, request(0, 0, BLOCK)).await;
        assert_eq!(next_message(&mut socket).await, Some(reject(0, 0, BLOCK)));
        send(&mut socket, reject(1, 0, BLOCK)).await;
        assert_eq!(next_message(&mut socket).await, None, "not banned");
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[peer_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out");
    assert!(result.is_err(), "downloaded without a peer");
    peer.await.unwrap();
}
