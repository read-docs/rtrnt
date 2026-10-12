use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use serde_bencode::value::Value;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::{debug, warn};
use url::Url;

/// BEP 15 magic constant identifying a UDP tracker connect request.
const UDP_PROTOCOL_ID: u64 = 0x417_2710_1980;
const UDP_ACTION_CONNECT: u32 = 0;
const UDP_ACTION_ANNOUNCE: u32 = 1;
const UDP_ACTION_ERROR: u32 = 3;
/// Retries with 2s, 4s, 8s timeouts (BEP 15 suggests 15·2ⁿ, too slow for a CLI).
const UDP_RETRIES: u32 = 3;
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
}

/// Why we are announcing (the `event` parameter of BEP 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    /// A re-announce on the tracker's interval; sends no event.
    Regular,
    Started,
    Completed,
    Stopped,
}

impl AnnounceEvent {
    fn http_name(self) -> Option<&'static str> {
        match self {
            AnnounceEvent::Regular => None,
            AnnounceEvent::Started => Some("started"),
            AnnounceEvent::Completed => Some("completed"),
            AnnounceEvent::Stopped => Some("stopped"),
        }
    }

    fn udp_code(self) -> u32 {
        match self {
            AnnounceEvent::Regular => 0,
            AnnounceEvent::Completed => 1,
            AnnounceEvent::Started => 2,
            AnnounceEvent::Stopped => 3,
        }
    }

    /// Whether we announce to get peers, rather than only to report.
    fn wants_peers(self) -> bool {
        matches!(self, AnnounceEvent::Regular | AnnounceEvent::Started)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub peers: Vec<SocketAddr>,
    /// Seconds the tracker asks us to wait before re-announcing.
    pub interval: Option<u64>,
    /// Seconds the tracker requires between announces (HTTP only).
    pub min_interval: Option<u64>,
}

/// Announces to each tracker in turn. When looking for peers, returns the
/// first response with any, or an empty one if no tracker had peers; when
/// only reporting (`completed`, `stopped`), returns the first success.
pub async fn announce(trackers: &[String], request: &AnnounceRequest) -> Result<AnnounceResponse> {
    if trackers.is_empty() {
        bail!("torrent has no trackers");
    }
    let mut empty = None;
    for tracker in trackers {
        match announce_one(tracker, request).await {
            Ok(response) if !response.peers.is_empty() || !request.event.wants_peers() => {
                debug!(%tracker, event = ?request.event, peers = response.peers.len(), "announce succeeded");
                return Ok(response);
            }
            Ok(response) => {
                warn!(%tracker, "tracker returned no peers");
                empty.get_or_insert(response);
            }
            Err(e) => warn!(%tracker, "announce failed: {e:#}"),
        }
    }
    empty.ok_or_else(|| anyhow!("every tracker failed"))
}

async fn announce_one(tracker: &str, request: &AnnounceRequest) -> Result<AnnounceResponse> {
    let url = Url::parse(tracker).context("invalid tracker URL")?;
    match url.scheme() {
        "udp" => announce_udp(&url, request).await,
        "http" | "https" => announce_http(tracker, request).await,
        scheme => bail!("unsupported tracker scheme `{}`", scheme),
    }
}

async fn announce_http(tracker: &str, request: &AnnounceRequest) -> Result<AnnounceResponse> {
    let client = reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?;
    let body = client
        .get(http_announce_url(tracker, request))
        .send()
        .await?
        .bytes()
        .await?;
    parse_http_response(&body)
}

fn http_announce_url(tracker: &str, request: &AnnounceRequest) -> String {
    let mut query = format!(
        "info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1",
        percent_encode(&request.info_hash),
        percent_encode(&request.peer_id),
        request.port,
        request.uploaded,
        request.downloaded,
        request.left,
    );
    if let Some(event) = request.event.http_name() {
        query.push_str("&event=");
        query.push_str(event);
    }
    let separator = if tracker.contains('?') { '&' } else { '?' };
    format!("{tracker}{separator}{query}")
}

fn percent_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("%{:02x}", b)).collect()
}

#[derive(Deserialize)]
struct HttpTrackerResponse {
    #[serde(rename = "failure reason", default)]
    failure_reason: Option<String>,
    #[serde(default)]
    interval: Option<u64>,
    #[serde(rename = "min interval", default)]
    min_interval: Option<u64>,
    #[serde(default)]
    peers: Option<Value>,
    #[serde(default)]
    peers6: Option<serde_bytes::ByteBuf>,
}

fn parse_http_response(body: &[u8]) -> Result<AnnounceResponse> {
    let response: HttpTrackerResponse =
        serde_bencode::from_bytes(body).context("invalid tracker response")?;
    if let Some(reason) = response.failure_reason {
        bail!("tracker error: {}", reason);
    }
    let mut peers = match response.peers {
        // Compact form (BEP 23): 6 bytes per peer.
        Some(Value::Bytes(bytes)) => parse_compact_v4(&bytes),
        // Original form: a list of {ip, port, peer id} dicts.
        Some(Value::List(list)) => list.iter().filter_map(parse_peer_dict).collect(),
        Some(_) => bail!("unexpected `peers` type in tracker response"),
        None => Vec::new(),
    };
    if let Some(peers6) = response.peers6 {
        peers.extend(parse_compact_v6(&peers6));
    }
    Ok(AnnounceResponse {
        peers,
        interval: response.interval,
        min_interval: response.min_interval,
    })
}

fn parse_peer_dict(value: &Value) -> Option<SocketAddr> {
    let Value::Dict(dict) = value else {
        return None;
    };
    let ip = match dict.get(b"ip".as_slice())? {
        Value::Bytes(ip) => std::str::from_utf8(ip).ok()?.parse::<IpAddr>().ok()?,
        _ => return None,
    };
    let port = match dict.get(b"port".as_slice())? {
        Value::Int(port) => u16::try_from(*port).ok()?,
        _ => return None,
    };
    Some(SocketAddr::new(ip, port))
}

fn parse_compact_v4(bytes: &[u8]) -> Vec<SocketAddr> {
    let (chunks, _) = bytes.as_chunks::<6>();
    chunks
        .iter()
        .map(|&[a, b, c, d, p1, p2]| {
            SocketAddr::new(
                Ipv4Addr::new(a, b, c, d).into(),
                u16::from_be_bytes([p1, p2]),
            )
        })
        .collect()
}

fn parse_compact_v6(bytes: &[u8]) -> Vec<SocketAddr> {
    let (chunks, _) = bytes.as_chunks::<18>();
    chunks
        .iter()
        .map(|chunk| {
            let (ip, port) = chunk.split_at(16);
            let ip: [u8; 16] = ip.try_into().unwrap_or_default();
            SocketAddr::new(
                Ipv6Addr::from(ip).into(),
                u16::from_be_bytes([port[0], port[1]]),
            )
        })
        .collect()
}

async fn announce_udp(url: &Url, request: &AnnounceRequest) -> Result<AnnounceResponse> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("tracker URL has no host"))?;
    let port = url
        .port()
        .ok_or_else(|| anyhow!("tracker URL has no port"))?;
    let addr = tokio::net::lookup_host(format!("{host}:{port}"))
        .await?
        .next()
        .ok_or_else(|| anyhow!("could not resolve {}", host))?;

    let bind_addr = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(addr).await?;

    let mut connect = Vec::with_capacity(16);
    connect.extend_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    connect.extend_from_slice(&UDP_ACTION_CONNECT.to_be_bytes());
    let response = udp_transact(&socket, connect, UDP_ACTION_CONNECT).await?;
    let connection_id = response
        .get(8..16)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| anyhow!("connect response too short"))?;

    let mut announce = Vec::with_capacity(98);
    announce.extend_from_slice(&connection_id.to_be_bytes());
    announce.extend_from_slice(&UDP_ACTION_ANNOUNCE.to_be_bytes());
    announce.extend_from_slice(&request.info_hash);
    announce.extend_from_slice(&request.peer_id);
    announce.extend_from_slice(&request.downloaded.to_be_bytes());
    announce.extend_from_slice(&request.left.to_be_bytes());
    announce.extend_from_slice(&request.uploaded.to_be_bytes());
    announce.extend_from_slice(&request.event.udp_code().to_be_bytes());
    announce.extend_from_slice(&0u32.to_be_bytes()); // IP = default
    announce.extend_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    announce.extend_from_slice(&(-1i32).to_be_bytes()); // num_want = default
    announce.extend_from_slice(&request.port.to_be_bytes());
    let response = udp_transact(&socket, announce, UDP_ACTION_ANNOUNCE).await?;
    parse_udp_announce(&response, addr.is_ipv6())
}

/// Sends a request (with a transaction id inserted after the first 12 bytes,
/// as both request types require) and waits for the matching response,
/// retrying on timeout.
async fn udp_transact(socket: &UdpSocket, mut request: Vec<u8>, action: u32) -> Result<Vec<u8>> {
    let transaction_id = rand::random::<u32>();
    request.splice(12..12, transaction_id.to_be_bytes());

    for attempt in 0..UDP_RETRIES {
        socket.send(&request).await?;
        let wait = Duration::from_secs(2 << attempt);
        let Ok(response) = timeout(wait, udp_recv(socket, transaction_id)).await else {
            debug!(attempt, "UDP tracker timed out");
            continue;
        };
        let response = response?;
        let received_action =
            u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        if received_action == UDP_ACTION_ERROR {
            bail!("tracker error: {}", String::from_utf8_lossy(&response[8..]));
        }
        if received_action != action {
            bail!("unexpected action {} in tracker response", received_action);
        }
        return Ok(response);
    }
    bail!("UDP tracker did not respond")
}

/// Receives datagrams until one carries our transaction id.
async fn udp_recv(socket: &UdpSocket, transaction_id: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; 65536];
    loop {
        let n = socket.recv(&mut buf).await?;
        if n >= 8 && buf[4..8] == transaction_id.to_be_bytes() {
            buf.truncate(n);
            return Ok(buf);
        }
    }
}

fn parse_udp_announce(response: &[u8], ipv6: bool) -> Result<AnnounceResponse> {
    if response.len() < 20 {
        bail!("announce response too short");
    }
    let interval = u32::from_be_bytes([response[8], response[9], response[10], response[11]]);
    let peers = if ipv6 {
        parse_compact_v6(&response[20..])
    } else {
        parse_compact_v4(&response[20..])
    };
    Ok(AnnounceResponse {
        peers,
        interval: Some(interval.into()),
        min_interval: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [0xab; 20],
            peer_id: *b"-RT0100-abcdefghijkl",
            port: 6881,
            uploaded: 1,
            downloaded: 2,
            left: 3,
            event: AnnounceEvent::Started,
        }
    }

    #[test]
    fn compact_peers() {
        let peers = parse_compact_v4(&[127, 0, 0, 1, 0x1a, 0xe1, 10, 0, 0, 2, 0, 80, 9]);
        assert_eq!(
            peers,
            [
                "127.0.0.1:6881".parse().unwrap(),
                "10.0.0.2:80".parse().unwrap()
            ]
        );
        let mut v6 = vec![0u8; 16];
        v6[15] = 1;
        v6.extend_from_slice(&[0, 80]);
        assert_eq!(parse_compact_v6(&v6), ["[::1]:80".parse().unwrap()]);
    }

    #[test]
    fn http_response_compact() {
        let mut body = b"d8:intervali1800e12:min intervali60e5:peers6:".to_vec();
        body.extend_from_slice(&[1, 2, 3, 4, 0, 80]);
        body.push(b'e');
        let response = parse_http_response(&body).unwrap();
        assert_eq!(response.interval, Some(1800));
        assert_eq!(response.min_interval, Some(60));
        assert_eq!(response.peers, ["1.2.3.4:80".parse().unwrap()]);
    }

    #[test]
    fn http_response_dict_list() {
        let body = b"d5:peersld2:ip7:1.2.3.47:peer id20:aaaaaaaaaaaaaaaaaaaa4:porti6881eed2:ip8:hostname4:porti1eeee";
        let response = parse_http_response(body).unwrap();
        assert_eq!(response.peers, ["1.2.3.4:6881".parse().unwrap()]);
    }

    #[test]
    fn http_response_failure() {
        let err = parse_http_response(b"d14:failure reason9:not founde").unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn http_url() {
        let url = http_announce_url("http://t/announce", &request());
        assert!(url.starts_with("http://t/announce?info_hash=%ab%ab"));
        assert!(url.contains("&peer_id=%2d%52%54"));
        assert!(url.contains("&port=6881&uploaded=1&downloaded=2&left=3&compact=1"));
        assert!(url.ends_with("&event=started"));
        let url = http_announce_url("http://t/announce?key=1", &request());
        assert!(url.starts_with("http://t/announce?key=1&info_hash="));
    }

    #[test]
    fn http_url_events() {
        let url = |event| http_announce_url("http://t/a", &AnnounceRequest { event, ..request() });
        assert!(!url(AnnounceEvent::Regular).contains("event="));
        assert!(url(AnnounceEvent::Completed).ends_with("&event=completed"));
        assert!(url(AnnounceEvent::Stopped).ends_with("&event=stopped"));
    }

    #[test]
    fn udp_event_codes() {
        // BEP 15: 0 none, 1 completed, 2 started, 3 stopped.
        let codes = [
            AnnounceEvent::Regular,
            AnnounceEvent::Completed,
            AnnounceEvent::Started,
            AnnounceEvent::Stopped,
        ]
        .map(AnnounceEvent::udp_code);
        assert_eq!(codes, [0, 1, 2, 3]);
    }

    #[test]
    fn udp_announce_response() {
        let mut response = vec![0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0];
        response.extend_from_slice(&[1, 2, 3, 4, 0, 80]);
        let parsed = parse_udp_announce(&response, false).unwrap();
        assert_eq!(parsed.interval, Some(1800));
        assert_eq!(parsed.peers, ["1.2.3.4:80".parse().unwrap()]);
        assert!(parse_udp_announce(&response[..19], false).is_err());
    }
}
