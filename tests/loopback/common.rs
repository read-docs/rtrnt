use indicatif::MultiProgress;
use rtrnt::client::Client;
use rtrnt::download::{DownloadConfig, SeedLimits, Session};
use rtrnt::peer::fast;
use rtrnt::peer::{BlockRef, PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::rate_limit::RateLimits;
use rtrnt::selection::FileSelection;
use rtrnt::stats::TorrentStats;
use rtrnt::storage::Storage;
use rtrnt::torrent::{FileInfo, Info, TorrentFile};
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

pub(crate) const PIECE_LENGTH: u64 = 32 * 1024;

pub(crate) const FILES: &[(&str, usize)] =
    &[("a.bin", 40_000), ("empty.bin", 0), ("b.bin", 50_123)];

pub(crate) fn make_torrent(data: &[u8]) -> (TorrentFile, [u8; 20]) {
    make_named_torrent(data, "multi")
}

/// `make_torrent`, with its files in the directory `name`.
pub(crate) fn make_named_torrent(data: &[u8], name: &str) -> (TorrentFile, [u8; 20]) {
    let pieces: Vec<u8> = data
        .chunks(PIECE_LENGTH as usize)
        .flat_map(|chunk| sha1::Sha1::digest(chunk).to_vec())
        .collect();
    let info = Info {
        name: name.into(),
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
pub(crate) fn mock_handshake(info_hash: &[u8; 20]) -> [u8; 68] {
    let mut handshake = handshake_bytes(info_hash, &generate_peer_id());
    handshake[20 + fast::RESERVED_BYTE] &= !fast::RESERVED_BIT;
    handshake
}

/// Accepts one leecher, completes the handshake and advertises every piece.
pub(crate) async fn accept_leecher(
    listener: &TcpListener,
    info_hash: [u8; 20],
    num_pieces: usize,
) -> TcpStream {
    let all: Vec<usize> = (0..num_pieces).collect();
    accept_partial(listener, info_hash, num_pieces, &all).await
}

/// Accepts one leecher, completes the handshake and advertises `has`.
pub(crate) async fn accept_partial(
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
pub(crate) async fn connect_to_leecher(
    addr: SocketAddr,
    info_hash: [u8; 20],
    num_pieces: usize,
) -> TcpStream {
    let all: Vec<usize> = (0..num_pieces).collect();
    connect_partial(addr, info_hash, num_pieces, &all).await
}

/// Connects to the leecher listening at `addr`, completes the handshake and
/// advertises `has`.
pub(crate) async fn connect_partial(
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
pub(crate) async fn connect_to(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket.write_all(&mock_handshake(&info_hash)).await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    parse_handshake(&handshake, &info_hash).unwrap();
    socket
}

pub(crate) async fn send_bitfield(socket: &mut TcpStream, num_pieces: usize, has: &[usize]) {
    let mut bitfield = vec![0u8; num_pieces.div_ceil(8)];
    for &i in has {
        bitfield[i / 8] |= 0x80 >> (i % 8);
    }
    send(socket, PeerMessage::Bitfield { bitfield }).await;
}

/// Reads the next message, or `None` once the leecher disconnects.
pub(crate) async fn read_message(socket: &mut (impl AsyncRead + Unpin)) -> Option<PeerMessage> {
    let len = socket.read_u32().await.ok()?;
    let mut payload = vec![0u8; len as usize];
    socket.read_exact(&mut payload).await.ok()?;
    Some(PeerMessage::decode(&payload).unwrap())
}

/// Reads the next message other than a keep-alive or the extension
/// handshake, or `None` once the client disconnects. Fails if nothing comes
/// for 5 s.
pub(crate) async fn next_message(socket: &mut TcpStream) -> Option<PeerMessage> {
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
pub(crate) async fn send_batch(socket: &mut TcpStream, messages: &[PeerMessage]) {
    let bytes: Vec<u8> = messages.iter().flat_map(PeerMessage::encode).collect();
    socket.write_all(&bytes).await.unwrap();
}

pub(crate) fn request(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::Request(BlockRef {
        index,
        begin,
        length,
    })
}

/// The Piece message answering a request for `length` bytes of `data` at
/// `begin` in piece `index`.
pub(crate) fn piece(data: &[u8], index: u32, begin: u32, length: u32) -> PeerMessage {
    let start = index as usize * PIECE_LENGTH as usize + begin as usize;
    PeerMessage::Piece {
        index,
        begin,
        block: data[start..start + length as usize].to_vec(),
    }
}

/// Answers block requests from `data` until the leecher disconnects.
pub(crate) async fn serve(mut socket: TcpStream, data: &[u8]) {
    while let Some(message) = read_message(&mut socket).await {
        if let PeerMessage::Request(BlockRef {
            index,
            begin,
            length,
        }) = message
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
pub(crate) async fn seeder(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    info_hash: [u8; 20],
    num_pieces: usize,
) {
    let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
    send(&mut socket, PeerMessage::Unchoke).await;

    let mut choked_once = false;
    let mut corrupted_once = false;
    while let Some(message) = read_message(&mut socket).await {
        let PeerMessage::Request(BlockRef {
            index,
            begin,
            length,
        }) = message
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

pub(crate) async fn send(socket: &mut TcpStream, message: PeerMessage) {
    socket.write_all(&message.encode()).await.unwrap();
}

pub(crate) fn init_tracing() {
    // `RUST_LOG=debug cargo test -- --nocapture` shows the choke/retry path.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

pub(crate) fn test_data() -> Vec<u8> {
    let total: usize = FILES.iter().map(|(_, len)| len).sum();
    (0..total).map(|i| (i * 31 % 251) as u8).collect()
}

pub(crate) fn session(torrent: TorrentFile, info_hash: [u8; 20], out: &Path) -> Arc<Session> {
    seeding_session(torrent, info_hash, out, SeedLimits::default())
}

pub(crate) fn seeding_session(
    torrent: TorrentFile,
    info_hash: [u8; 20],
    out: &Path,
    seed: SeedLimits,
) -> Arc<Session> {
    limited_session(torrent, info_hash, out, seed, RateLimits::default())
}

pub(crate) fn limited_session(
    torrent: TorrentFile,
    info_hash: [u8; 20],
    out: &Path,
    seed: SeedLimits,
    rate_limits: RateLimits,
) -> Arc<Session> {
    let storage = Arc::new(Storage::new(&torrent.info, out, &FileSelection::default()).unwrap());
    // What `make_torrent` hashed.
    let metadata = serde_bencode::to_bytes(&torrent.info).unwrap();
    Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
        seed,
        dht: None,
        rate_limits: Arc::new(rate_limits),
        metadata: Some(metadata),
        bars: MultiProgress::new(),
        resume: None,
        stats: Arc::new(TorrentStats::new(info_hash, "test", Path::new("."))),
    })
}

/// A client with the settings in `config`, for a test.
pub(crate) fn client(config: DownloadConfig) -> Client {
    Client::new(config, MultiProgress::new())
}

pub(crate) fn assert_files_match(out: &Path, data: &[u8]) {
    assert_named_files_match(out, "multi", data);
}

/// `assert_files_match` for a torrent made by `make_named_torrent`.
pub(crate) fn assert_named_files_match(out: &Path, torrent: &str, data: &[u8]) {
    let mut offset = 0;
    for (name, len) in FILES {
        let written = std::fs::read(out.join(torrent).join("dir").join(name)).unwrap();
        assert_eq!(written, data[offset..offset + len], "{name} differs");
        offset += len;
    }
}

/// A config that keeps resume files in `state`.
pub(crate) fn resume_config(out: &Path, state: &Path) -> DownloadConfig {
    DownloadConfig {
        output_dir: out.to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: Some(state.to_path_buf()),
    }
}

pub(crate) const BLOCK: u32 = 16 * 1024;

/// Writes every file of the torrent in place, as a finished download would.
pub(crate) fn write_files(out: &Path, data: &[u8]) {
    write_named_files(out, "multi", data);
}

/// `write_files` for a torrent made by `make_named_torrent`.
pub(crate) fn write_named_files(out: &Path, torrent: &str, data: &[u8]) {
    let dir = out.join(torrent).join("dir");
    std::fs::create_dir_all(&dir).unwrap();
    let mut offset = 0;
    for (name, len) in FILES {
        std::fs::write(dir.join(name), &data[offset..offset + len]).unwrap();
        offset += len;
    }
}

/// A seeder of piece 1 alone that only unchokes us once `release` fires, so
/// the download keeps running until then.
pub(crate) async fn gated_seeder(
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

/// A minimal HTTP tracker: answers every announce with `peers` and forwards
/// each request's target (path and query).
pub(crate) async fn mock_tracker(
    peers: Vec<SocketAddr>,
) -> (String, mpsc::UnboundedReceiver<String>) {
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

/// Waits for the next announce `mock_tracker` hears, failing after 5 s.
pub(crate) async fn next_announce(queries: &mut mpsc::UnboundedReceiver<String>) -> String {
    tokio::time::timeout(Duration::from_secs(5), queries.recv())
        .await
        .expect("no announce")
        .unwrap()
}

/// A minimal HTTP server: answers every request with `body` and forwards
/// each request's target (path and query).
pub(crate) async fn mock_http(body: Vec<u8>) -> (SocketAddr, mpsc::UnboundedReceiver<String>) {
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
pub(crate) fn query_param<'a>(target: &'a str, key: &str) -> Option<&'a str> {
    let (_, query) = target.split_once('?')?;
    query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// Asks the client over `socket` for every block of the torrent once it
/// unchokes us, and checks what comes back.
pub(crate) async fn leech_everything(socket: &mut TcpStream, data: &[u8]) {
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
pub(crate) fn announces(queries: &[String]) -> Vec<[String; 4]> {
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

/// Reads messages until an extended message with extended id `id`, and
/// returns its payload. Fails if the client disconnects first, or if none
/// comes within 5 s.
pub(crate) async fn next_extended(socket: &mut TcpStream, id: u8) -> Vec<u8> {
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

pub(crate) fn cancel(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::Cancel(BlockRef {
        index,
        begin,
        length,
    })
}
