//! KRPC (BEP 5): bencoded queries, responses and errors carried in single
//! UDP datagrams, plus the compact node and peer formats they contain.
//!
//! Any host can send us a datagram, so decoding never panics, limits
//! nesting depth and the size of every field, and rejects anything
//! malformed. A datagram is at most 64 KiB, which bounds the rest.

use super::routing::{NodeId, NodeInfo};
use anyhow::{Result, anyhow, bail};
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddrV4};

/// KRPC messages are three levels deep; anything much deeper is garbage.
const MAX_DEPTH: usize = 8;
/// Longest transaction id we accept. Ours are 2 bytes; others use up to 8 or so.
const MAX_TID: usize = 16;
/// Longest `get_peers` token we accept. Ours are 8 bytes.
const MAX_TOKEN: usize = 64;
/// Nodes and peers taken from one response. Real responses carry 8 nodes
/// and well under 100 peers.
const MAX_NODES: usize = 32;
const MAX_VALUES: usize = 200;
/// Longest error message or unknown method name we keep.
const MAX_TEXT: usize = 128;
/// Compact node info: 20-byte id, IPv4 address and port.
const COMPACT_NODE: usize = 26;
/// Compact peer info: IPv4 address and port.
const COMPACT_PEER: usize = 6;
/// Our client version, sent as `v`: "RT" and a version number.
const VERSION: &[u8; 4] = b"RT\x01\x00";

/// Error codes from BEP 5.
pub const ERROR_PROTOCOL: i64 = 203;
pub const ERROR_METHOD_UNKNOWN: i64 = 204;

/// A decoded bencode value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Dict(Dict),
}

type Dict = BTreeMap<Vec<u8>, Value>;

impl Value {
    /// Decodes exactly one value spanning all of `data`.
    pub fn decode(data: &[u8]) -> Result<Value> {
        let mut pos = 0;
        let value = parse_value(data, &mut pos, 0)?;
        if pos != data.len() {
            bail!("trailing data after bencode value");
        }
        Ok(value)
    }

    /// Appends the bencoding of the value to `out`. Dictionary keys come out
    /// sorted, as bencode requires.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Value::Int(i) => out.extend_from_slice(format!("i{i}e").as_bytes()),
            Value::Bytes(bytes) => encode_bytes(bytes, out),
            Value::List(list) => {
                out.push(b'l');
                list.iter().for_each(|value| value.encode(out));
                out.push(b'e');
            }
            Value::Dict(dict) => {
                out.push(b'd');
                for (key, value) in dict {
                    encode_bytes(key, out);
                    value.encode(out);
                }
                out.push(b'e');
            }
        }
    }
}

fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn parse_value(data: &[u8], pos: &mut usize, depth: usize) -> Result<Value> {
    if depth > MAX_DEPTH {
        bail!("bencode nesting too deep");
    }
    match data.get(*pos) {
        Some(b'i') => {
            *pos += 1;
            let len = data[*pos..]
                .iter()
                .position(|&b| b == b'e')
                .ok_or_else(|| anyhow!("unterminated integer"))?;
            let digits = &data[*pos..*pos + len];
            *pos += len + 1;
            parse_int(digits).map(Value::Int)
        }
        Some(b'l') => {
            *pos += 1;
            let mut list = Vec::new();
            while data.get(*pos) != Some(&b'e') {
                list.push(parse_value(data, pos, depth + 1)?);
            }
            *pos += 1;
            Ok(Value::List(list))
        }
        Some(b'd') => {
            *pos += 1;
            let mut dict = Dict::new();
            while data.get(*pos) != Some(&b'e') {
                let key = parse_bytes(data, pos)?.to_vec();
                let value = parse_value(data, pos, depth + 1)?;
                dict.insert(key, value);
            }
            *pos += 1;
            Ok(Value::Dict(dict))
        }
        Some(b'0'..=b'9') => parse_bytes(data, pos).map(|bytes| Value::Bytes(bytes.to_vec())),
        Some(&b) => bail!("unexpected byte {b:#04x} in bencode"),
        None => bail!("unexpected end of bencode"),
    }
}

/// Parses the digits of `i<digits>e`: an optional minus sign and no leading
/// zeros, as bencode requires.
fn parse_int(digits: &[u8]) -> Result<i64> {
    let unsigned = digits.strip_prefix(b"-").unwrap_or(digits);
    let valid = !unsigned.is_empty()
        && unsigned.iter().all(u8::is_ascii_digit)
        && (unsigned == b"0" || unsigned[0] != b'0')
        && digits != b"-0";
    if !valid {
        bail!("invalid bencode integer");
    }
    std::str::from_utf8(digits)?
        .parse()
        .map_err(|_| anyhow!("bencode integer out of range"))
}

/// Parses `<len>:<bytes>` at `pos`.
fn parse_bytes<'a>(data: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let colon = data[*pos..]
        .iter()
        .position(|&b| b == b':')
        .ok_or_else(|| anyhow!("unterminated string length"))?;
    let digits = &data[*pos..*pos + colon];
    // Ten digits are more than any datagram can hold.
    if digits.is_empty() || digits.len() > 10 || !digits.iter().all(u8::is_ascii_digit) {
        bail!("invalid string length");
    }
    let len: usize = std::str::from_utf8(digits)?.parse()?;
    let start = *pos + colon + 1;
    let end = start
        .checked_add(len)
        .filter(|&end| end <= data.len())
        .ok_or_else(|| anyhow!("string runs past the end of the data"))?;
    *pos = end;
    Ok(&data[start..end])
}

/// One KRPC message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Transaction id, echoed in the answer to a query.
    pub tid: Vec<u8>,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Query { id: NodeId, query: Query },
    Response(Response),
    Error { code: i64, message: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Ping,
    FindNode {
        target: NodeId,
    },
    GetPeers {
        info_hash: [u8; 20],
    },
    AnnouncePeer {
        info_hash: [u8; 20],
        /// Zero if not given and `implied_port` is set.
        port: u16,
        /// Use the port the query came from instead of `port`.
        implied_port: bool,
        token: Vec<u8>,
    },
    /// A method we do not know, answered with error 204.
    Unknown(Vec<u8>),
}

impl Query {
    fn method(&self) -> &[u8] {
        match self {
            Query::Ping => b"ping",
            Query::FindNode { .. } => b"find_node",
            Query::GetPeers { .. } => b"get_peers",
            Query::AnnouncePeer { .. } => b"announce_peer",
            Query::Unknown(method) => method,
        }
    }
}

/// The answer to any query. Which fields are set depends on the query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Response {
    pub id: NodeId,
    pub nodes: Vec<NodeInfo>,
    /// Peers for the info hash of a `get_peers` query.
    pub values: Vec<SocketAddrV4>,
    /// Lets us `announce_peer` to the node that sent it.
    pub token: Option<Vec<u8>>,
}

impl Message {
    pub fn decode(data: &[u8]) -> Result<Message> {
        let Value::Dict(dict) = Value::decode(data)? else {
            bail!("message is not a dictionary");
        };
        let tid = bytes(&dict, b"t")?;
        if tid.len() > MAX_TID {
            bail!("transaction id too long");
        }
        let body = match bytes(&dict, b"y")? {
            b"q" => decode_query(&dict)?,
            b"r" => Body::Response(decode_response(dict_at(&dict, b"r")?)?),
            b"e" => decode_error(&dict)?,
            _ => bail!("unknown message type"),
        };
        Ok(Message {
            tid: tid.to_vec(),
            body,
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut dict = Dict::new();
        let mut put = |key: &[u8], value| dict.insert(key.to_vec(), value);
        put(b"t", Value::Bytes(self.tid.clone()));
        put(b"v", Value::Bytes(VERSION.to_vec()));
        match &self.body {
            Body::Query { id, query } => {
                put(b"y", Value::Bytes(b"q".to_vec()));
                put(b"q", Value::Bytes(query.method().to_vec()));
                put(b"a", Value::Dict(query_args(id, query)));
            }
            Body::Response(response) => {
                put(b"y", Value::Bytes(b"r".to_vec()));
                put(b"r", Value::Dict(response_fields(response)));
            }
            Body::Error { code, message } => {
                put(b"y", Value::Bytes(b"e".to_vec()));
                let error = vec![Value::Int(*code), Value::Bytes(message.as_bytes().to_vec())];
                put(b"e", Value::List(error));
            }
        }
        let mut out = Vec::new();
        Value::Dict(dict).encode(&mut out);
        out
    }
}

fn query_args(id: &NodeId, query: &Query) -> Dict {
    let mut args = Dict::new();
    args.insert(b"id".to_vec(), Value::Bytes(id.0.to_vec()));
    match query {
        Query::Ping | Query::Unknown(_) => {}
        Query::FindNode { target } => {
            args.insert(b"target".to_vec(), Value::Bytes(target.0.to_vec()));
        }
        Query::GetPeers { info_hash } => {
            args.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.to_vec()));
        }
        Query::AnnouncePeer {
            info_hash,
            port,
            implied_port,
            token,
        } => {
            args.insert(b"info_hash".to_vec(), Value::Bytes(info_hash.to_vec()));
            args.insert(b"port".to_vec(), Value::Int(i64::from(*port)));
            args.insert(b"token".to_vec(), Value::Bytes(token.clone()));
            if *implied_port {
                args.insert(b"implied_port".to_vec(), Value::Int(1));
            }
        }
    }
    args
}

fn response_fields(response: &Response) -> Dict {
    let mut fields = Dict::new();
    fields.insert(b"id".to_vec(), Value::Bytes(response.id.0.to_vec()));
    if !response.nodes.is_empty() {
        fields.insert(
            b"nodes".to_vec(),
            Value::Bytes(encode_nodes(&response.nodes)),
        );
    }
    if !response.values.is_empty() {
        let values = response.values.iter();
        let values = values.map(|&peer| Value::Bytes(encode_peer(peer).to_vec()));
        fields.insert(b"values".to_vec(), Value::List(values.collect()));
    }
    if let Some(token) = &response.token {
        fields.insert(b"token".to_vec(), Value::Bytes(token.clone()));
    }
    fields
}

fn decode_query(dict: &Dict) -> Result<Body> {
    let method = bytes(dict, b"q")?;
    let args = dict_at(dict, b"a")?;
    let id = NodeId(id20(args, b"id")?);
    let query = match method {
        b"ping" => Query::Ping,
        b"find_node" => Query::FindNode {
            target: NodeId(id20(args, b"target")?),
        },
        b"get_peers" => Query::GetPeers {
            info_hash: id20(args, b"info_hash")?,
        },
        b"announce_peer" => {
            let implied_port =
                matches!(args.get(b"implied_port".as_slice()), Some(Value::Int(i)) if *i != 0);
            let port = int(args, b"port")
                .ok()
                .and_then(|port| u16::try_from(port).ok())
                .filter(|&port| port != 0);
            let port = match (port, implied_port) {
                (Some(port), _) => port,
                (None, true) => 0,
                (None, false) => bail!("announce_peer without a valid port"),
            };
            let token = bytes(args, b"token")?;
            if token.len() > MAX_TOKEN {
                bail!("token too long");
            }
            Query::AnnouncePeer {
                info_hash: id20(args, b"info_hash")?,
                port,
                implied_port,
                token: token.to_vec(),
            }
        }
        other => Query::Unknown(other.iter().copied().take(MAX_TEXT).collect()),
    };
    Ok(Body::Query { id, query })
}

fn decode_response(fields: &Dict) -> Result<Response> {
    let id = NodeId(id20(fields, b"id")?);
    let mut nodes = match fields.get(b"nodes".as_slice()) {
        Some(Value::Bytes(bytes)) => decode_nodes(bytes)?,
        Some(_) => bail!("`nodes` is not a string"),
        None => Vec::new(),
    };
    nodes.truncate(MAX_NODES);
    let values = match fields.get(b"values".as_slice()) {
        // IPv6 peers (18 bytes) are skipped: we only speak IPv4.
        Some(Value::List(list)) => list
            .iter()
            .filter_map(|value| match value {
                Value::Bytes(bytes) => decode_peer(bytes),
                _ => None,
            })
            .take(MAX_VALUES)
            .collect(),
        Some(_) => bail!("`values` is not a list"),
        None => Vec::new(),
    };
    let token = match fields.get(b"token".as_slice()) {
        Some(Value::Bytes(token)) if token.len() <= MAX_TOKEN => Some(token.clone()),
        Some(_) => bail!("invalid token"),
        None => None,
    };
    Ok(Response {
        id,
        nodes,
        values,
        token,
    })
}

/// Decodes `e: [code, message]`, leniently: either part may be missing.
fn decode_error(dict: &Dict) -> Result<Body> {
    let Some(Value::List(error)) = dict.get(b"e".as_slice()) else {
        bail!("error message without a list");
    };
    let code = match error.first() {
        Some(Value::Int(code)) => *code,
        _ => 201,
    };
    let message = match error.get(1) {
        Some(Value::Bytes(text)) => String::from_utf8_lossy(text)
            .chars()
            .take(MAX_TEXT)
            .collect(),
        _ => String::new(),
    };
    Ok(Body::Error { code, message })
}

fn get<'a>(dict: &'a Dict, key: &[u8]) -> Result<&'a Value> {
    dict.get(key)
        .ok_or_else(|| anyhow!("missing `{}`", String::from_utf8_lossy(key)))
}

fn bytes<'a>(dict: &'a Dict, key: &[u8]) -> Result<&'a [u8]> {
    match get(dict, key)? {
        Value::Bytes(bytes) => Ok(bytes),
        _ => bail!("`{}` is not a string", String::from_utf8_lossy(key)),
    }
}

fn int(dict: &Dict, key: &[u8]) -> Result<i64> {
    match get(dict, key)? {
        Value::Int(i) => Ok(*i),
        _ => bail!("`{}` is not an integer", String::from_utf8_lossy(key)),
    }
}

fn dict_at<'a>(dict: &'a Dict, key: &[u8]) -> Result<&'a Dict> {
    match get(dict, key)? {
        Value::Dict(dict) => Ok(dict),
        _ => bail!("`{}` is not a dictionary", String::from_utf8_lossy(key)),
    }
}

/// A 20-byte string: a node id or an info hash.
fn id20(dict: &Dict, key: &[u8]) -> Result<[u8; 20]> {
    bytes(dict, key)?
        .try_into()
        .map_err(|_| anyhow!("`{}` is not 20 bytes", String::from_utf8_lossy(key)))
}

/// Compact node info: 26 bytes per node.
pub fn encode_nodes(nodes: &[NodeInfo]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * COMPACT_NODE);
    for node in nodes {
        out.extend_from_slice(&node.id.0);
        out.extend_from_slice(&encode_peer(node.addr));
    }
    out
}

pub fn decode_nodes(bytes: &[u8]) -> Result<Vec<NodeInfo>> {
    let (chunks, rest) = bytes.as_chunks::<COMPACT_NODE>();
    if !rest.is_empty() {
        bail!("compact node info is not a multiple of {COMPACT_NODE} bytes");
    }
    Ok(chunks
        .iter()
        .map(|chunk| {
            let (id, addr) = chunk.split_at(20);
            NodeInfo {
                id: NodeId(id.try_into().unwrap_or_default()),
                addr: decode_peer(addr).unwrap_or(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            }
        })
        .collect())
}

/// Compact peer info: 4-byte IPv4 address and 2-byte port, big-endian.
pub fn encode_peer(addr: SocketAddrV4) -> [u8; COMPACT_PEER] {
    let [a, b, c, d] = addr.ip().octets();
    let [p1, p2] = addr.port().to_be_bytes();
    [a, b, c, d, p1, p2]
}

pub fn decode_peer(bytes: &[u8]) -> Option<SocketAddrV4> {
    let &[a, b, c, d, p1, p2] = bytes else {
        return None;
    };
    Some(SocketAddrV4::new(
        Ipv4Addr::new(a, b, c, d),
        u16::from_be_bytes([p1, p2]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &[u8; 20] = b"abcdefghij0123456789";
    const OTHER: &[u8; 20] = b"mnopqrstuvwxyz123456";

    fn node(byte: u8, port: u16) -> NodeInfo {
        NodeInfo {
            id: NodeId([byte; 20]),
            addr: SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, byte), port),
        }
    }

    #[test]
    fn values_roundtrip() {
        let data = b"d4:listli-42ei0e3:abce4:zero0:1:ad1:xi1eee";
        let value = Value::decode(data).unwrap();
        let mut out = Vec::new();
        value.encode(&mut out);
        // Keys come out sorted.
        assert_eq!(out, b"d1:ad1:xi1ee4:listli-42ei0e3:abce4:zero0:e");
        assert_eq!(Value::decode(&out).unwrap(), value);
    }

    #[test]
    fn rejects_malformed_bencode() {
        let cases: &[&[u8]] = &[
            b"",
            b"i1",
            b"ie",
            b"i-e",
            b"i-0e",
            b"i03e",
            b"i+3e",
            b"i99999999999999999999e",
            b"5:abc",
            b"99999999999:a",
            b"1x",
            b"l",
            b"d1:a",
            b"di1ei2ee",
            b"i1ei2e",
            b"x",
        ];
        for case in cases {
            assert!(Value::decode(case).is_err(), "accepted {case:?}");
        }
        let deep: Vec<u8> = std::iter::repeat_n(b'l', 10_000).collect();
        assert!(Value::decode(&deep).is_err());
    }

    /// The example messages from BEP 5.
    #[test]
    fn decodes_bep_5_examples() {
        let query = |data: &[u8]| match Message::decode(data).unwrap() {
            Message {
                tid,
                body: Body::Query { id, query },
            } => {
                assert_eq!((tid.as_slice(), id), (b"aa".as_slice(), NodeId(*ID)));
                query
            }
            other => panic!("not a query: {other:?}"),
        };
        assert_eq!(
            query(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"),
            Query::Ping
        );
        assert_eq!(
            query(b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe"),
            Query::FindNode { target: NodeId(*OTHER) }
        );
        assert_eq!(
            query(b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe"),
            Query::GetPeers { info_hash: *OTHER }
        );
        assert_eq!(
            query(b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe"),
            Query::AnnouncePeer {
                info_hash: *OTHER,
                port: 6881,
                implied_port: true,
                token: b"aoeusnth".to_vec(),
            }
        );

        let response = Message::decode(b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re").unwrap();
        assert_eq!(
            response.body,
            Body::Response(Response {
                id: NodeId(*OTHER),
                ..Response::default()
            })
        );
        let peers = Message::decode(b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re").unwrap();
        let Body::Response(peers) = peers.body else {
            panic!("not a response");
        };
        assert_eq!(peers.token.as_deref(), Some(b"aoeusnth".as_slice()));
        assert_eq!(
            peers.values,
            [
                SocketAddrV4::new(Ipv4Addr::new(97, 120, 106, 101), 0x2e75),
                SocketAddrV4::new(Ipv4Addr::new(105, 100, 104, 116), 0x6e6d),
            ]
        );

        let error =
            Message::decode(b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee").unwrap();
        assert_eq!(
            error.body,
            Body::Error {
                code: 201,
                message: "A Generic Error Ocurred".into()
            }
        );
    }

    #[test]
    fn messages_roundtrip() {
        let id = NodeId(*ID);
        let bodies = [
            Body::Query {
                id,
                query: Query::Ping,
            },
            Body::Query {
                id,
                query: Query::FindNode {
                    target: NodeId(*OTHER),
                },
            },
            Body::Query {
                id,
                query: Query::GetPeers { info_hash: *OTHER },
            },
            Body::Query {
                id,
                query: Query::AnnouncePeer {
                    info_hash: *OTHER,
                    port: 51413,
                    implied_port: false,
                    token: vec![1, 2, 3],
                },
            },
            Body::Query {
                id,
                query: Query::Unknown(b"vote".to_vec()),
            },
            Body::Response(Response {
                id,
                nodes: vec![node(1, 6881), node(2, 1)],
                values: vec![SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 80)],
                token: Some(b"tok".to_vec()),
            }),
            Body::Response(Response {
                id,
                ..Response::default()
            }),
            Body::Error {
                code: ERROR_PROTOCOL,
                message: "bad token".into(),
            },
        ];
        for body in bodies {
            let message = Message {
                tid: vec![0, 7],
                body,
            };
            let encoded = message.encode();
            assert_eq!(Message::decode(&encoded).unwrap(), message, "{encoded:?}");
        }
    }

    #[test]
    fn implied_port_needs_no_port() {
        let data = b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234565:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe";
        let Body::Query { query, .. } = Message::decode(data).unwrap().body else {
            panic!("not a query");
        };
        assert!(matches!(
            query,
            Query::AnnouncePeer {
                port: 0,
                implied_port: true,
                ..
            }
        ));
    }

    #[test]
    fn rejects_malformed_messages() {
        let long_token = format!(
            "d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti1e5:token65:{}e1:q13:announce_peer1:t2:aa1:y1:qe",
            "x".repeat(65)
        );
        let cases: &[&[u8]] = &[
            b"le",
            b"d1:t2:aa1:y1:qe",
            b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe",
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:y1:qe",
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t17:aaaaaaaaaaaaaaaaa1:y1:qe",
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:xe",
            b"d1:ad2:id20:abcdefghij0123456789e1:q9:find_node1:t2:aa1:y1:qe",
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti0e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe",
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti70000e5:token1:xe1:q13:announce_peer1:t2:aa1:y1:qe",
            long_token.as_bytes(),
            b"d1:rd2:id20:mnopqrstuvwxyz1234565:nodes3:abce1:t2:aa1:y1:re",
            b"d1:rd2:id20:mnopqrstuvwxyz1234566:valuesi1ee1:t2:aa1:y1:re",
            b"d1:t2:aa1:y1:ee",
        ];
        for case in cases {
            assert!(
                Message::decode(case).is_err(),
                "accepted {}",
                String::from_utf8_lossy(case)
            );
        }
    }

    #[test]
    fn compact_formats() {
        let nodes = [node(1, 6881), node(200, 65535)];
        let bytes = encode_nodes(&nodes);
        assert_eq!(bytes.len(), 52);
        assert_eq!(&bytes[20..26], &[10, 0, 0, 1, 0x1a, 0xe1]);
        assert_eq!(decode_nodes(&bytes).unwrap(), nodes);
        assert!(decode_nodes(&bytes[..51]).is_err());
        assert!(decode_nodes(&[]).unwrap().is_empty());

        let peer = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6881);
        assert_eq!(decode_peer(&encode_peer(peer)), Some(peer));
        assert_eq!(decode_peer(&[0; 18]), None);
    }

    #[test]
    fn caps_nodes_and_values_per_response() {
        let nodes: Vec<NodeInfo> = (0..100).map(|i| node(i, 1)).collect();
        let response = Response {
            id: NodeId(*ID),
            nodes,
            values: vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1); 500],
            token: None,
        };
        let message = Message {
            tid: vec![1],
            body: Body::Response(response),
        };
        let Body::Response(decoded) = Message::decode(&message.encode()).unwrap().body else {
            panic!("not a response");
        };
        assert_eq!(
            (decoded.nodes.len(), decoded.values.len()),
            (MAX_NODES, MAX_VALUES)
        );
    }
}
