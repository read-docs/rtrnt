use crate::extension;
use crate::fast;
use anyhow::{Context, Result, bail};
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::HANDSHAKE_TIMEOUT;

const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
/// Reserved handshake bit saying we run a DHT node (BEP 5): the last bit of
/// the last reserved byte.
const RESERVED_DHT: u8 = 0x01;

/// Our handshake.
pub fn handshake_bytes(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 68] {
    let mut handshake = [0u8; 68];
    handshake[0] = PROTOCOL.len() as u8;
    handshake[1..20].copy_from_slice(PROTOCOL);
    // Bytes 20..28 are reserved extension bits. We set the extension
    // protocol's (BEP 10), the DHT's (BEP 5) and the fast extension's
    // (BEP 6).
    handshake[20 + extension::RESERVED_BYTE] |= extension::RESERVED_BIT;
    handshake[27] |= RESERVED_DHT;
    handshake[20 + fast::RESERVED_BYTE] |= fast::RESERVED_BIT;
    handshake[28..48].copy_from_slice(info_hash);
    handshake[48..68].copy_from_slice(peer_id);
    handshake
}

/// What a peer told us in its handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub peer_id: [u8; 20],
    /// The extension bits the peer set.
    pub reserved: [u8; 8],
}

impl Handshake {
    /// Whether the peer supports the extension protocol (BEP 10).
    pub fn supports_extensions(&self) -> bool {
        self.reserved[extension::RESERVED_BYTE] & extension::RESERVED_BIT != 0
    }

    /// Whether the peer runs a DHT node (BEP 5), and so wants our `Port`
    /// message.
    pub fn supports_dht(&self) -> bool {
        self.reserved[7] & RESERVED_DHT != 0
    }

    /// Whether the peer supports the fast extension (BEP 6).
    pub fn supports_fast(&self) -> bool {
        self.reserved[fast::RESERVED_BYTE] & fast::RESERVED_BIT != 0
    }
}

/// Validates a handshake and returns what the peer told us in it.
pub fn parse_handshake(handshake: &[u8; 68], info_hash: &[u8; 20]) -> Result<Handshake> {
    if handshake[0] as usize != PROTOCOL.len() || &handshake[1..20] != PROTOCOL {
        bail!("peer does not speak the BitTorrent protocol");
    }
    if &handshake[28..48] != info_hash {
        bail!("info hash mismatch");
    }
    let mut parsed = Handshake {
        peer_id: [0u8; 20],
        reserved: [0u8; 8],
    };
    parsed.reserved.copy_from_slice(&handshake[20..28]);
    parsed.peer_id.copy_from_slice(&handshake[48..68]);
    Ok(parsed)
}

/// A connection a peer opened to us, whose handshake we have read but not
/// answered yet.
#[derive(Debug)]
pub struct Incoming {
    pub socket: TcpStream,
    pub addr: SocketAddr,
    /// The torrent the peer asked for.
    pub info_hash: [u8; 20],
    pub handshake: Handshake,
}

impl Incoming {
    /// Reads the handshake of the peer that connected to us from `addr`,
    /// which says which torrent it wants.
    pub async fn read(mut socket: TcpStream, addr: SocketAddr) -> Result<Self> {
        let mut bytes = [0u8; 68];
        timeout(HANDSHAKE_TIMEOUT, socket.read_exact(&mut bytes))
            .await
            .context("handshake timed out")??;
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&bytes[28..48]);
        let handshake = parse_handshake(&bytes, &info_hash)?;
        Ok(Self {
            socket,
            addr,
            info_hash,
            handshake,
        })
    }
}
