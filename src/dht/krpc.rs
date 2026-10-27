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
mod tests;
