use anyhow::{Result, anyhow};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

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
    pub fn new(socket: TcpStream, peer_ip: String, peer_port: u16, info_hash: [u8; 20]) -> Self {
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

pub async fn handshake(peer: PeerConnection) -> Result<()> {
    let mut handshake = Vec::new();
    handshake.push(19u8); // Length
    handshake.extend_from_slice(b"BitTorrent protocol");
    handshake.extend_from_slice(&[0u8; 8]); // Reserved
    handshake.extend_from_slice(&peer.info_hash); // Info hash
    handshake.extend_from_slice(b"-RT0001-123456789012"); // Your peer ID
    todo!()
}
