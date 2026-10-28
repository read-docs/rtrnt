use rand::Rng;
use tokio::time::Duration;

mod connection;
pub mod extension;
pub mod fast;
mod handshake;
mod message;
pub mod pex;

pub use connection::{PeerConnection, ProtocolViolation};
pub use handshake::{Handshake, Incoming, handshake_bytes, parse_handshake};
pub use message::{BlockRef, PeerMessage, encode_bitfield};

/// Port we listen on for incoming peers unless told otherwise.
pub const DEFAULT_LISTEN_PORT: u16 = 6881;
/// Largest frame we accept: room for a 16 KiB block or a bitfield of 8M pieces.
pub const MAX_FRAME: usize = 1 << 20;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Peers commonly drop connections that stay silent for two minutes.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(90);
const PEER_ID_PREFIX: &[u8; 8] = b"-RT0100-";

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

#[cfg(test)]
mod tests;
