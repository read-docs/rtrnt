use super::extension;
use super::fast;
use super::mse::{self, Encryption};
use super::stream::{PeerStream, Transport};
use anyhow::{Context, Result, bail};
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
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
    /// The connection, encrypted if the peer opened it with MSE.
    pub socket: PeerStream<Transport>,
    pub addr: SocketAddr,
    /// The torrent the peer asked for.
    pub info_hash: [u8; 20],
    pub handshake: Handshake,
}

impl Incoming {
    /// Reads the handshake of the peer that connected to us from `addr`,
    /// which says which torrent it wants. A peer that starts with the plain
    /// handshake's `19 "BitTorrent protocol"` speaks plain BitTorrent,
    /// refused if `encryption` is `Require`; any other peer is taken to
    /// start an MSE handshake, refused if `encryption` is `Off`, for one of
    /// `torrents` (see `mse::respond`).
    pub async fn read(
        socket: Transport,
        addr: SocketAddr,
        encryption: Encryption,
        torrents: impl FnOnce() -> Vec<[u8; 20]>,
    ) -> Result<Self> {
        timeout(
            HANDSHAKE_TIMEOUT,
            Self::read_untimed(socket, addr, encryption, torrents),
        )
        .await
        .context("handshake timed out")?
    }

    async fn read_untimed(
        mut socket: Transport,
        addr: SocketAddr,
        encryption: Encryption,
        torrents: impl FnOnce() -> Vec<[u8; 20]>,
    ) -> Result<Self> {
        let mut first = vec![0u8; 1 + PROTOCOL.len()];
        socket.read_exact(&mut first).await?;
        let plaintext = usize::from(first[0]) == PROTOCOL.len() && first[1..] == PROTOCOL[..];
        let (mut socket, encrypted_for) = match (plaintext, encryption) {
            (true, Encryption::Require) => bail!("refusing a peer that does not encrypt"),
            (true, _) => (PeerStream::with_unread(socket, first), None),
            (false, Encryption::Off) => bail!("peer does not speak the BitTorrent protocol"),
            (false, _) => {
                let (socket, info_hash) = mse::respond(socket, first, encryption, torrents).await?;
                (socket, Some(info_hash))
            }
        };
        let mut bytes = [0u8; 68];
        socket.read_exact(&mut bytes).await?;
        let mut info_hash = [0u8; 20];
        info_hash.copy_from_slice(&bytes[28..48]);
        let handshake = parse_handshake(&bytes, &info_hash)?;
        // The torrent the encryption handshake named must be the one the
        // handshake inside it asks for.
        if encrypted_for.is_some_and(|encrypted_for| encrypted_for != info_hash) {
            bail!("info hash mismatch");
        }
        Ok(Self {
            socket,
            addr,
            info_hash,
            handshake,
        })
    }
}
