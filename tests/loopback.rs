//! End-to-end downloads from mock seeders (and a mock tracker) over loopback.

use rtrnt::announcer::Announcer;
use rtrnt::download::{
    DownloadConfig, Interrupted, SeedLimits, Session, download_file, download_from_peers,
};
use rtrnt::peer::{PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::storage::Storage;
use rtrnt::torrent::{FileInfo, Info, TorrentFile};
use rtrnt::tracker::{AnnounceEvent, AnnounceRequest};
use rtrnt::verify;
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::future::pending;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
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
    let mut socket = connect_to(addr, info_hash).await;
    let all: Vec<usize> = (0..num_pieces).collect();
    send_bitfield(&mut socket, num_pieces, &all).await;
    socket
}

/// Connects to the client listening at `addr` and completes the handshake,
/// as a peer with no pieces (which need not send a bitfield).
async fn connect_to(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
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

/// Reads the next message other than a keep-alive, or `None` once the
/// client disconnects. Fails if nothing comes for 5 s.
async fn next_message(socket: &mut TcpStream) -> Option<PeerMessage> {
    let next = async {
        loop {
            match read_message(socket).await {
                Some(PeerMessage::KeepAlive) => {}
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
    let storage = Arc::new(Storage::new(&torrent.info, out).unwrap());
    Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
        seed,
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
            None,
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
            None,
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
            None,
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
            None,
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
            None,
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
        download_from_peers(session, &have, &[seeder_addr], 10, None, None, pending()),
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
            Some(listener),
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
            Some(listener),
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
            Some(listener),
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
            Some(listener),
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
/// each request's query string.
async fn mock_tracker(peers: Vec<SocketAddr>) -> (String, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let (queries, queries_rx) = mpsc::unbounded_channel();
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
            let (_, query) = target.split_once('?').unwrap();
            let _ = queries.send(query.to_string());
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    (url, queries_rx)
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
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
            None,
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
            Some(listener),
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
            None,
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
            None,
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
            None,
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
