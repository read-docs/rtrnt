//! Extension protocol (BEP 10).
//!
//! Peers that both set reserved bit 20 in the handshake may send extended
//! messages (message id 20). The first payload byte is an extended message
//! id. Id 0 is the extension handshake: a bencoded dictionary in which each
//! side lists the extensions it supports (`m`), each with the id it wants to
//! receive that extension's messages under, plus a few facts about itself.
//! Any other id is one the receiver picked in its own handshake.

use super::pex;
use crate::bencode;
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_bencode::value::Value;
use serde_bytes::ByteBuf;
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Index into the handshake's 8 reserved bytes of the byte holding the
/// extension protocol bit.
pub const RESERVED_BYTE: usize = 5;
/// Reserved bit 20, counting from the last bit of the last reserved byte.
pub const RESERVED_BIT: u8 = 0x10;
/// Extended message id of the extension handshake.
pub const HANDSHAKE_ID: u8 = 0;
/// Our client name and version, sent as `v`.
pub const CLIENT: &str = concat!("rtrnt ", env!("CARGO_PKG_VERSION"));
/// How many requests we queue from one peer, sent as `reqq`; more are
/// dropped. libtorrent allows as many by default.
pub const MAX_QUEUED_REQUESTS: u32 = 500;
/// Name of the metadata exchange extension (BEP 9), which lets magnet links
/// fetch the info dictionary from peers.
pub const UT_METADATA: &str = "ut_metadata";
/// Extended message id peers send us `ut_metadata` messages with. The same
/// as libtorrent's.
pub const UT_METADATA_ID: u8 = 2;
/// The extensions we implement, each with the extended message id peers
/// must send its messages to us with. Advertised in our handshake as `m`.
pub const EXTENSIONS: &[(&str, u8)] = &[(pex::NAME, pex::ID), (UT_METADATA, UT_METADATA_ID)];
/// Extensions that private torrents (BEP 27) must not use, which are left
/// out of `m` for them.
pub const PUBLIC_ONLY: &[&str] = &[pex::NAME];

/// What our extension handshake says that depends on the torrent and on
/// us, rather than being fixed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Advertised {
    /// The port we accept incoming peers on, if any (`p`).
    pub listen_port: Option<u16>,
    /// The length of the info dictionary, if we have it to send
    /// (`metadata_size`, BEP 9).
    pub metadata_size: Option<u32>,
    /// Whether the torrent is private (BEP 27), so that the extensions in
    /// `PUBLIC_ONLY` are not offered.
    pub private: bool,
}

/// An extension handshake. Every field is optional on the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExtendedHandshake {
    /// `m`: extension names, each with the extended message id the sender
    /// wants its messages sent with. Id 0 means the extension is disabled.
    pub extensions: BTreeMap<String, u8>,
    /// `v`: client name and version.
    pub client: Option<String>,
    /// `p`: the port the sender listens on for incoming peers.
    pub listen_port: Option<u16>,
    /// `reqq`: how many requests the sender queues before dropping more.
    pub request_queue: Option<u32>,
    /// `yourip`: the receiver's address, as the sender sees it.
    pub your_ip: Option<IpAddr>,
    /// `metadata_size`: the length of the torrent's info dictionary, which
    /// the sender can send through `ut_metadata` (BEP 9). Left out by a
    /// sender that does not have it yet.
    pub metadata_size: Option<u32>,
}

/// Our handshake as it goes on the wire. `serde_bencode` sorts the keys.
#[derive(Serialize)]
struct Wire<'a> {
    m: &'a BTreeMap<String, u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    v: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reqq: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    yourip: Option<ByteBuf>,
}

impl ExtendedHandshake {
    /// Our handshake for the peer at `peer_ip`, saying what `advertised`
    /// says.
    pub fn ours(advertised: Advertised, peer_ip: IpAddr) -> Self {
        let Advertised {
            listen_port,
            metadata_size,
            private,
        } = advertised;
        Self {
            extensions: EXTENSIONS
                .iter()
                .filter(|(name, _)| !(private && PUBLIC_ONLY.contains(name)))
                .map(|&(name, id)| (name.to_string(), id))
                .collect(),
            client: Some(CLIENT.to_string()),
            listen_port,
            request_queue: Some(MAX_QUEUED_REQUESTS),
            your_ip: Some(peer_ip),
            metadata_size,
        }
    }

    /// The extended message id the sender wants extension `name` sent with,
    /// or `None` if it does not support it.
    pub fn id(&self, name: &str) -> Option<u8> {
        self.extensions.get(name).copied().filter(|&id| id != 0)
    }

    /// Serializes the handshake into an extended message payload.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let wire = Wire {
            m: &self.extensions,
            metadata_size: self.metadata_size,
            v: self.client.as_deref(),
            p: self.listen_port,
            reqq: self.request_queue,
            yourip: self.your_ip.map(|ip| match ip.to_canonical() {
                IpAddr::V4(ip) => ByteBuf::from(ip.octets().to_vec()),
                IpAddr::V6(ip) => ByteBuf::from(ip.octets().to_vec()),
            }),
        };
        serde_bencode::to_bytes(&wire).context("cannot encode extension handshake")
    }

    /// Parses an extended message payload. Parsing is lenient: unknown keys
    /// are ignored, and so are known keys whose value has the wrong type or
    /// is out of range. Only a payload that is not a bencoded dictionary is
    /// an error. Bytes after the dictionary are ignored.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        // Scanned first because it limits the nesting depth, which
        // `serde_bencode` would follow until the stack overflows.
        let len = bencode::value_len(payload).context("extension handshake is not bencoded")?;
        let value: Value = serde_bencode::from_bytes(&payload[..len])
            .context("extension handshake is not bencoded")?;
        let Value::Dict(dict) = value else {
            bail!("extension handshake is not a dictionary");
        };
        let int = |key: &[u8]| match dict.get(key) {
            Some(Value::Int(n)) => Some(*n),
            _ => None,
        };
        let bytes = |key: &[u8]| match dict.get(key) {
            Some(Value::Bytes(bytes)) => Some(bytes.as_slice()),
            _ => None,
        };
        let extensions = match dict.get(b"m".as_slice()) {
            Some(Value::Dict(m)) => m
                .iter()
                .filter_map(|(name, id)| {
                    let name = String::from_utf8(name.clone()).ok()?;
                    let Value::Int(id) = id else {
                        return None;
                    };
                    Some((name, u8::try_from(*id).ok()?))
                })
                .collect(),
            _ => BTreeMap::new(),
        };
        Ok(Self {
            extensions,
            client: bytes(b"v").map(|v| String::from_utf8_lossy(v).into_owned()),
            listen_port: int(b"p")
                .and_then(|p| u16::try_from(p).ok())
                .filter(|&p| p != 0),
            request_queue: int(b"reqq")
                .and_then(|n| u32::try_from(n).ok())
                .filter(|&n| n != 0),
            your_ip: bytes(b"yourip").and_then(parse_ip),
            metadata_size: int(b"metadata_size")
                .and_then(|n| u32::try_from(n).ok())
                .filter(|&n| n != 0),
        })
    }

    /// Applies a later handshake from the same peer: the extensions it lists
    /// are added, changed or (with id 0) disabled, and the other fields it
    /// carries replace ours.
    pub fn update(&mut self, newer: ExtendedHandshake) {
        self.extensions.extend(newer.extensions);
        self.client = newer.client.or(self.client.take());
        self.listen_port = newer.listen_port.or(self.listen_port);
        self.request_queue = newer.request_queue.or(self.request_queue);
        self.your_ip = newer.your_ip.or(self.your_ip);
        self.metadata_size = newer.metadata_size.or(self.metadata_size);
    }
}

/// An IPv4 (4 bytes) or IPv6 (16 bytes) address in network order.
fn parse_ip(bytes: &[u8]) -> Option<IpAddr> {
    if let Ok(octets) = <[u8; 4]>::try_from(bytes) {
        Some(Ipv4Addr::from(octets).into())
    } else if let Ok(octets) = <[u8; 16]>::try_from(bytes) {
        Some(Ipv6Addr::from(octets).into())
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
