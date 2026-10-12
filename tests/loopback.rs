//! End-to-end download from a mock seeder over loopback, no tracker involved.

use rtrnt::download::{Session, download_from_peers};
use rtrnt::peer::{PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use rtrnt::storage::Storage;
use rtrnt::torrent::{FileInfo, Info, TorrentFile};
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

/// Serves `data` to one leecher. Chokes on the first request (dropping it)
/// and corrupts the first block it sends for piece 1.
async fn seeder(listener: TcpListener, data: Arc<Vec<u8>>, info_hash: [u8; 20], num_pieces: usize) {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    parse_handshake(&handshake, &info_hash).unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();

    let mut bitfield = vec![0u8; num_pieces.div_ceil(8)];
    for i in 0..num_pieces {
        bitfield[i / 8] |= 0x80 >> (i % 8);
    }
    send(&mut socket, PeerMessage::Bitfield { bitfield }).await;
    send(&mut socket, PeerMessage::Unchoke).await;

    let mut choked_once = false;
    let mut corrupted_once = false;
    loop {
        let Ok(len) = socket.read_u32().await else {
            return;
        };
        let mut payload = vec![0u8; len as usize];
        if socket.read_exact(&mut payload).await.is_err() {
            return;
        }
        let PeerMessage::Request {
            index,
            begin,
            length,
        } = PeerMessage::decode(&payload).unwrap()
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

#[tokio::test]
async fn downloads_multi_file_torrent_from_mock_seeder() {
    // `RUST_LOG=debug cargo test -- --nocapture` shows the choke/retry path.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let total: usize = FILES.iter().map(|(_, len)| len).sum();
    let data: Vec<u8> = (0..total).map(|i| (i * 31 % 251) as u8).collect();
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
    let storage = Arc::new(Storage::new(&torrent.info, out.path()).unwrap());
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
    });

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        download_from_peers(session, &[dead_addr, seeder_addr], 10),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    let mut offset = 0;
    for (name, len) in FILES {
        let written = std::fs::read(out.path().join("multi/dir").join(name)).unwrap();
        assert_eq!(written, data[offset..offset + len], "{name} differs");
        offset += len;
    }
}
