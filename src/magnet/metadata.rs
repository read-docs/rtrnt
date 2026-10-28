//! Metadata exchange (BEP 9): the `ut_metadata` extension, through which
//! peers send each other a torrent's info dictionary in 16 KiB pieces, so
//! that a magnet link, which only gives the info hash, is enough to
//! download.
//!
//! Each message is a bencoded dictionary with a `msg_type` (0 request, 1
//! data, 2 reject) and a `piece` index; a data message also carries the
//! metadata's `total_size`, and the piece itself after the dictionary. A
//! peer gives the metadata's size as `metadata_size` in its extension
//! handshake.
//!
//! `MetadataFetch` fetches the metadata from several peers at once and
//! checks it against the info hash; `serve` answers peers that ask us.

mod assembly;
mod fetch;

pub(crate) use fetch::MetadataFetch;

use crate::bencode;
use crate::peer::extension::UT_METADATA;
use crate::peer::{PeerConnection, ProtocolViolation};
use anyhow::{Context, Result};
use serde_bencode::value::Value;
use tracing::debug;

/// Length of every metadata piece but the last.
pub const PIECE_LEN: usize = 16 * 1024;
/// Largest metadata we fetch. An info dictionary holds 20 bytes per piece,
/// so this is room for over 800,000 pieces, far more than any sane torrent
/// has; a peer claiming more is not asked.
pub const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;

/// A `ut_metadata` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Asks for metadata piece `piece`.
    Request { piece: u32 },
    /// Metadata piece `piece`, of metadata `total_size` bytes long.
    Data {
        piece: u32,
        total_size: Option<u64>,
        data: Vec<u8>,
    },
    /// The sender will not send piece `piece`, usually because it does not
    /// have the metadata.
    Reject { piece: u32 },
}

impl Message {
    /// Serializes the message into an extended message payload.
    pub fn encode(&self) -> Vec<u8> {
        // Keys in sorted order, as bencode requires.
        match self {
            Message::Request { piece } => format!("d8:msg_typei0e5:piecei{piece}ee").into_bytes(),
            Message::Data {
                piece,
                total_size,
                data,
            } => {
                let mut payload = format!("d8:msg_typei1e5:piecei{piece}e").into_bytes();
                if let Some(total_size) = total_size {
                    payload.extend_from_slice(format!("10:total_sizei{total_size}e").as_bytes());
                }
                payload.push(b'e');
                payload.extend_from_slice(data);
                payload
            }
            Message::Reject { piece } => format!("d8:msg_typei2e5:piecei{piece}ee").into_bytes(),
        }
    }

    /// Parses an extended message payload. Parsing is lenient, as for the
    /// extension handshake: a message of an unknown type, or without a valid
    /// `piece`, gives `None` and is to be ignored (BEP 9 asks for unknown
    /// types to be ignored), and so is a data message's ill-typed
    /// `total_size`. Only a payload that does not start with a bencoded
    /// dictionary is an error.
    pub fn decode(payload: &[u8]) -> Result<Option<Self>> {
        let (dict, len) = bencode::decode_dict(payload)
            .context("ut_metadata message is not a bencoded dictionary")?;
        let int = |key: &[u8]| match dict.get(key) {
            Some(Value::Int(n)) => Some(*n),
            _ => None,
        };
        let Some(piece) = int(b"piece").and_then(|piece| u32::try_from(piece).ok()) else {
            return Ok(None);
        };
        Ok(match int(b"msg_type") {
            Some(0) => Some(Message::Request { piece }),
            Some(1) => Some(Message::Data {
                piece,
                total_size: int(b"total_size").and_then(|size| u64::try_from(size).ok()),
                data: payload[len..].to_vec(),
            }),
            Some(2) => Some(Message::Reject { piece }),
            _ => None,
        })
    }
}

/// Our answer to a request for piece `piece` of the info dictionary
/// `metadata`: the piece, or a reject if we do not have the metadata or it
/// has no such piece.
pub fn answer(metadata: Option<&[u8]>, piece: u32) -> Message {
    let start = usize::try_from(piece)
        .ok()
        .and_then(|piece| piece.checked_mul(PIECE_LEN));
    match (metadata, start) {
        (Some(metadata), Some(start)) if start < metadata.len() => Message::Data {
            piece,
            total_size: Some(metadata.len() as u64),
            data: metadata[start..metadata.len().min(start + PIECE_LEN)].to_vec(),
        },
        _ => Message::Reject { piece },
    }
}

/// Handles a `ut_metadata` message from a peer of a download: a request is
/// answered from `metadata`, our info dictionary if we have it, and anything
/// else (such as a late reply to the fetch) is ignored. A payload that is
/// not a bencoded dictionary is a protocol violation.
pub(crate) async fn serve(
    conn: &mut PeerConnection,
    metadata: Option<&[u8]>,
    payload: &[u8],
) -> Result<()> {
    let message =
        Message::decode(payload).context(ProtocolViolation("malformed ut_metadata message"))?;
    let Some(Message::Request { piece }) = message else {
        return Ok(());
    };
    // Without an id for it in the peer's handshake there is no way to reply.
    if !peer_supports_metadata(conn) {
        return Ok(());
    }
    debug!(addr = %conn.addr, piece, "metadata requested");
    conn.send_extended(UT_METADATA, answer(metadata, piece).encode())
        .await
}

/// Whether the peer listed `ut_metadata` in its extension handshake.
fn peer_supports_metadata(conn: &PeerConnection) -> bool {
    conn.peer_extensions()
        .and_then(|handshake| handshake.id(UT_METADATA))
        .is_some()
}

#[cfg(test)]
mod tests;
