//! Extension protocol (BEP 10).
//!
//! Peers that both set reserved bit 20 in the handshake may send extended
//! messages (message id 20). The first payload byte is an extended message
//! id. Id 0 is the extension handshake: a bencoded dictionary in which each
//! side lists the extensions it supports (`m`), each with the id it wants to
//! receive that extension's messages under, plus a few facts about itself.
//! Any other id is one the receiver picked in its own handshake.

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
/// The extensions we implement, each with the extended message id peers
/// must send its messages to us with. Advertised in our handshake as `m`.
/// None yet; `ut_metadata` (BEP 9) and `ut_pex` (BEP 11) go here once they
/// are implemented.
pub const EXTENSIONS: &[(&str, u8)] = &[];

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
}

/// Our handshake as it goes on the wire. `serde_bencode` sorts the keys.
#[derive(Serialize)]
struct Wire<'a> {
    m: &'a BTreeMap<String, u8>,
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
    /// Our handshake for the peer at `peer_ip`. `listen_port` is the port we
    /// accept incoming peers on, if any.
    pub fn ours(listen_port: Option<u16>, peer_ip: IpAddr) -> Self {
        Self {
            extensions: EXTENSIONS
                .iter()
                .map(|&(name, id)| (name.to_string(), id))
                .collect(),
            client: Some(CLIENT.to_string()),
            listen_port,
            request_queue: Some(MAX_QUEUED_REQUESTS),
            your_ip: Some(peer_ip),
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
mod tests {
    use super::*;

    #[test]
    fn our_handshake_roundtrips() {
        let ip: IpAddr = "10.1.2.3".parse().unwrap();
        let ours = ExtendedHandshake::ours(Some(6881), ip);
        assert!(ours.extensions.is_empty());
        assert_eq!(ours.client.as_deref(), Some(CLIENT));
        assert!(CLIENT.starts_with("rtrnt "));

        let payload = ours.encode().unwrap();
        assert_eq!(
            payload,
            format!(
                "d1:mde1:pi6881e4:reqqi500e1:v{}:{}6:yourip4:\x0a\x01\x02\x03e",
                CLIENT.len(),
                CLIENT
            )
            .as_bytes()
        );
        assert_eq!(ExtendedHandshake::decode(&payload).unwrap(), ours);
    }

    #[test]
    fn roundtrips_every_field() {
        let handshake = ExtendedHandshake {
            extensions: [("ut_metadata".to_string(), 3), ("ut_pex".to_string(), 0)].into(),
            client: Some("µTorrent 3.5".into()),
            listen_port: None,
            request_queue: Some(1),
            your_ip: Some("2001:db8::1".parse().unwrap()),
        };
        let decoded = ExtendedHandshake::decode(&handshake.encode().unwrap()).unwrap();
        assert_eq!(decoded, handshake);
        assert_eq!(decoded.id("ut_metadata"), Some(3));
        assert_eq!(decoded.id("ut_pex"), None, "id 0 means disabled");
        assert_eq!(decoded.id("lt_donthave"), None);
    }

    #[test]
    fn ipv4_mapped_addresses_are_sent_as_ipv4() {
        let ip: IpAddr = "::ffff:10.1.2.3".parse().unwrap();
        let payload = ExtendedHandshake::ours(None, ip).encode().unwrap();
        let decoded = ExtendedHandshake::decode(&payload).unwrap();
        assert_eq!(decoded.your_ip, Some("10.1.2.3".parse().unwrap()));
        assert_eq!(decoded.listen_port, None);
    }

    #[test]
    fn decodes_a_typical_handshake_ignoring_unknown_keys() {
        let payload = b"d12:complete_agoi1441e1:md11:lt_donthavei7e\
            10:share_modei8e11:ut_metadatai2e6:ut_pexi1ee13:metadata_sizei5205e\
            1:pi51413e4:reqqi512e1:v16:Transmission 4.06:yourip4:\x7f\x00\x00\x01e";
        let handshake = ExtendedHandshake::decode(payload).unwrap();
        assert_eq!(handshake.id("ut_metadata"), Some(2));
        assert_eq!(handshake.id("ut_pex"), Some(1));
        assert_eq!(handshake.extensions.len(), 4);
        assert_eq!(handshake.client.as_deref(), Some("Transmission 4.0"));
        assert_eq!(handshake.listen_port, Some(51413));
        assert_eq!(handshake.request_queue, Some(512));
        assert_eq!(handshake.your_ip, Some("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn missing_and_ill_typed_keys_are_ignored() {
        assert_eq!(
            ExtendedHandshake::decode(b"de").unwrap(),
            ExtendedHandshake::default()
        );
        let payload = b"d1:mli1ee1:p4:68814:reqqi-1e1:vi7e6:yourip3:abce";
        assert_eq!(
            ExtendedHandshake::decode(payload).unwrap(),
            ExtendedHandshake::default()
        );
        // Out of range values, and extension entries that make no sense.
        let payload = b"d1:md1:ai256e1:b1:x1:ci-1e1:di4ee1:pi65536e4:reqqi0ee";
        let handshake = ExtendedHandshake::decode(payload).unwrap();
        assert_eq!(handshake.extensions, [("d".to_string(), 4)].into());
        assert_eq!(handshake.listen_port, None);
        assert_eq!(handshake.request_queue, None);
        // A client name that is not UTF-8 is kept, lossily.
        let handshake = ExtendedHandshake::decode(b"d1:v2:a\xffe").unwrap();
        assert_eq!(handshake.client.as_deref(), Some("a\u{fffd}"));
    }

    #[test]
    fn rejects_payloads_that_are_not_a_dictionary() {
        let cases: &[&[u8]] = &[
            b"",
            b"i5e",
            b"le",
            b"4:spam",
            b"d1:m",
            b"d1:mde",
            b"di1ei2ee",
            b"\xff",
        ];
        for case in cases {
            assert!(
                ExtendedHandshake::decode(case).is_err(),
                "accepted {case:?}"
            );
        }
        let mut deep = b"d1:x".to_vec();
        deep.extend(std::iter::repeat_n(b'l', 100_000));
        assert!(ExtendedHandshake::decode(&deep).is_err());
    }

    #[test]
    fn trailing_bytes_are_ignored() {
        let handshake = ExtendedHandshake::decode(b"d4:reqqi8eeextra").unwrap();
        assert_eq!(handshake.request_queue, Some(8));
    }

    #[test]
    fn later_handshakes_update_earlier_ones() {
        let mut handshake =
            ExtendedHandshake::decode(b"d1:md11:ut_metadatai2e6:ut_pexi1ee4:reqqi64e1:v1:ae")
                .unwrap();
        handshake.update(ExtendedHandshake::decode(b"d1:md6:ut_pexi0ee4:reqqi32ee").unwrap());
        assert_eq!(handshake.id("ut_metadata"), Some(2));
        assert_eq!(handshake.id("ut_pex"), None);
        assert_eq!(handshake.request_queue, Some(32));
        assert_eq!(handshake.client.as_deref(), Some("a"));
    }
}
