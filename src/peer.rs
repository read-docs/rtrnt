use crate::extension::{self, Advertised, ExtendedHandshake};
use crate::fast;
use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, BytesMut};
use rand::Rng;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout};
use tracing::debug;

/// Port we listen on for incoming peers unless told otherwise.
pub const DEFAULT_LISTEN_PORT: u16 = 6881;
/// Largest frame we accept: room for a 16 KiB block or a bitfield of 8M pieces.
pub const MAX_FRAME: usize = 1 << 20;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Peers commonly drop connections that stay silent for two minutes.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(90);
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
const PEER_ID_PREFIX: &[u8; 8] = b"-RT0100-";
/// Reserved handshake bit saying we run a DHT node (BEP 5): the last bit of
/// the last reserved byte.
const RESERVED_DHT: u8 = 0x01;
/// Have messages kept from a peer while the number of pieces is unknown
/// (see `PeerConnection::defer_pieces`); more are dropped.
const MAX_DEFERRED_HAVES: usize = 1 << 16;

/// Azureus-style peer id: client prefix plus 12 random alphanumerics.
pub fn generate_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(PEER_ID_PREFIX);
    let mut rng = rand::rng();
    for byte in &mut id[8..] {
        *byte = rng.sample(rand::distr::Alphanumeric);
    }
    id
}

mod connection;
mod handshake;
mod message;
#[cfg(test)]
mod tests;

pub use connection::{PeerConnection, ProtocolViolation};
pub use handshake::{Handshake, Incoming, handshake_bytes, parse_handshake};
pub use message::{PeerMessage, encode_bitfield};
