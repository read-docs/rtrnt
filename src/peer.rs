use anyhow::{Result, anyhow};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

enum PeerMessage {
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield {
        bitfield: Vec<u8>,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port {
        port: u16,
    },
}

pub struct PeerConnection {
    socket: TcpStream,
    pub peer_ip: String,
    pub peer_port: u16,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: Vec<bool>,
}

impl PeerConnection {
    pub fn new(
        mut socket: TcpStream,
        peer_ip: String,
        peer_port: u16,
        info_hash: [u8; 20],
    ) -> Self {
        Self {
            socket,
            peer_ip,
            peer_port,
            info_hash,
            peer_id: [0u8; 20],
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Vec::new(),
        }
    }
}

pub async fn connect(
    peer_ip: String,
    peer_port: u16,
    info_hash: [u8; 20],
) -> Result<PeerConnection> {
    let addr = format!("{}:{}", peer_ip, peer_port);
    let connection_timeout = Duration::from_secs(5);

    let socket = match timeout(connection_timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(socket)) => socket,
        Ok(Err(e)) => return Err(anyhow!("Failed to connect to {}: {}", addr, e)),
        Err(_) => return Err(anyhow!("Connection timeout to {}", addr)),
    };

    Ok(PeerConnection::new(socket, peer_ip, peer_port, info_hash))
}

pub async fn handshake(mut peer: PeerConnection) -> Result<PeerConnection> {
    let mut handshake = Vec::new();
    handshake.push(19u8); // Length
    handshake.extend_from_slice(b"BitTorrent protocol");
    handshake.extend_from_slice(&[0u8; 8]); // Reserved
    handshake.extend_from_slice(&peer.info_hash); // Info hash
    handshake.extend_from_slice(b"-RT0001-123456789012"); // Your peer ID

    peer.socket.write_all(&handshake).await?;
    let mut response_buf = vec![0u8; 68];
    peer.socket.read_exact(&mut response_buf).await?;

    // Validate protocol name length
    if response_buf[0] != 19 {
        return Err(anyhow!("Invalid protocol length: {}", response_buf[0]));
    }

    // Validate protocol name
    if &response_buf[1..20] != b"BitTorrent protocol" {
        return Err(anyhow!("Invalid protocol name"));
    }

    // Extract and verify info_hash
    let received_info_hash: [u8; 20] = response_buf[28..48].try_into()?;
    if received_info_hash != peer.info_hash {
        return Err(anyhow!("Info hash mismatch"));
    }
    // Extract peer_id
    let peer_id: [u8; 20] = response_buf[48..68].try_into()?;
    peer.peer_id = peer_id;

    Ok(peer)
}
