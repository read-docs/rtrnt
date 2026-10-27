use anyhow::{Context, Result, anyhow, bail};
use rand::seq::SliceRandom;
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
const UDP_ACTION_SCRAPE: u32 = 2;
const UDP_ACTION_ERROR: u32 = 3;
/// Retries with 2s, 4s, 8s timeouts (BEP 15 suggests 15·2ⁿ, too slow for a CLI).
const UDP_RETRIES: u32 = 3;
/// Info hashes per UDP scrape request; BEP 15 says about 74 fit in a packet.
const UDP_MAX_SCRAPE: usize = 74;
const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// How long resolving a UDP tracker's host name may take.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

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

/// A torrent's trackers in tiers (BEP 12). Tiers are tried in order, and the
/// trackers within a tier one after another. The tracker that answers moves
/// to the front of its tier, so it is asked first next time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tiers(Vec<Vec<String>>);

impl Tiers {
    /// Shuffles each tier, as BEP 12 asks, so clients spread their load
    /// across a tier's trackers.
    pub fn new(mut tiers: Vec<Vec<String>>) -> Self {
        let mut rng = rand::rng();
        for tier in &mut tiers {
            tier.shuffle(&mut rng);
        }
        Self(tiers)
    }

    /// Announces to each tracker in turn. When looking for peers, returns
    /// the first response with any, or else the first empty one; when only
    /// reporting (`completed`, `stopped`), returns the first success.
    pub async fn announce(&mut self, request: &AnnounceRequest) -> Result<AnnounceResponse> {
        self.announce_with(request, announce_one).await
    }

    async fn announce_with(
        &mut self,
        request: &AnnounceRequest,
        mut announce_one: impl AsyncFnMut(&str, &AnnounceRequest) -> Result<AnnounceResponse>,
    ) -> Result<AnnounceResponse> {
        if self.0.iter().all(Vec::is_empty) {
            bail!("torrent has no trackers");
        }
        let mut empty = None;
        for tier in 0..self.0.len() {
            for index in 0..self.0[tier].len() {
                let tracker = &self.0[tier][index];
                match announce_one(tracker, request).await {
                    Ok(response) if !response.peers.is_empty() || !request.event.wants_peers() => {
                        debug!(%tracker, event = ?request.event, peers = response.peers.len(), "announce succeeded");
                        self.promote(tier, index);
                        return Ok(response);
                    }
                    Ok(response) => {
                        warn!(%tracker, "tracker returned no peers");
                        empty.get_or_insert((tier, index, response));
                    }
                    Err(e) => warn!(%tracker, "announce failed: {e:#}"),
                }
            }
        }
        let (tier, index, response) = empty.ok_or_else(|| anyhow!("every tracker failed"))?;
        self.promote(tier, index);
        Ok(response)
    }

    /// Moves a tracker to the front of its tier, keeping the others' order.
    fn promote(&mut self, tier: usize, index: usize) {
        self.0[tier][..=index].rotate_right(1);
    }
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

/// Peers in compact form: 4-byte IPv4 address and 2-byte port each, in
/// network order. A partial entry at the end is ignored.
pub(crate) fn parse_compact_v4(bytes: &[u8]) -> Vec<SocketAddr> {
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

/// Peers in compact form: 16-byte IPv6 address and 2-byte port each, in
/// network order. A partial entry at the end is ignored.
pub(crate) fn parse_compact_v6(bytes: &[u8]) -> Vec<SocketAddr> {
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
    let (socket, addr) = udp_socket(url).await?;
    let connection_id = udp_connect(&socket).await?;

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

/// Resolves a UDP tracker URL and opens a socket connected to the tracker.
async fn udp_socket(url: &Url) -> Result<(UdpSocket, SocketAddr)> {
    let host = url
        .host_str()
        .ok_or_else(|| anyhow!("tracker URL has no host"))?;
    let port = url
        .port()
        .ok_or_else(|| anyhow!("tracker URL has no port"))?;
    let addr = timeout(
        LOOKUP_TIMEOUT,
        tokio::net::lookup_host(format!("{host}:{port}")),
    )
    .await
    .map_err(|_| anyhow!("timed out resolving {}", host))??
    .next()
    .ok_or_else(|| anyhow!("could not resolve {}", host))?;

    let bind_addr = if addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };
    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(addr).await?;
    Ok((socket, addr))
}

/// Gets a connection id from the tracker, which it accepts for about a
/// minute (BEP 15).
async fn udp_connect(socket: &UdpSocket) -> Result<u64> {
    let mut connect = Vec::with_capacity(16);
    connect.extend_from_slice(&UDP_PROTOCOL_ID.to_be_bytes());
    connect.extend_from_slice(&UDP_ACTION_CONNECT.to_be_bytes());
    let response = udp_transact(socket, connect, UDP_ACTION_CONNECT).await?;
    response
        .get(8..16)
        .and_then(|b| b.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| anyhow!("connect response too short"))
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

/// What a tracker knows about one torrent's swarm, from a scrape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeStats {
    /// Peers with the whole torrent (`complete` over HTTP).
    pub seeders: u64,
    /// Peers still downloading (`incomplete`).
    pub leechers: u64,
    /// Downloads the tracker has seen finish (`downloaded`).
    pub completed: u64,
}

/// Asks `tracker` about the swarm of each torrent in `info_hashes`, without
/// joining it. Answers come in the order asked. `None` is a torrent an HTTP
/// tracker did not list; a UDP tracker reports zeros for those instead.
pub async fn scrape(tracker: &str, info_hashes: &[[u8; 20]]) -> Result<Vec<Option<ScrapeStats>>> {
    let url = Url::parse(tracker).context("invalid tracker URL")?;
    match url.scheme() {
        "udp" => Ok(scrape_udp(&url, info_hashes)
            .await?
            .into_iter()
            .map(Some)
            .collect()),
        "http" | "https" => scrape_http(&url, info_hashes).await,
        scheme => bail!("unsupported tracker scheme `{}`", scheme),
    }
}

/// One tracker's answer to a scrape of one torrent.
#[derive(Debug)]
pub struct TrackerScrape {
    /// The tracker's tier, counting from 1.
    pub tier: usize,
    pub tracker: String,
    pub result: Result<Option<ScrapeStats>>,
}

impl std::fmt::Display for TrackerScrape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tier {}  {}  ", self.tier, self.tracker)?;
        match &self.result {
            Ok(Some(stats)) => write!(
                f,
                "{} seeders, {} leechers, {} completed",
                stats.seeders, stats.leechers, stats.completed
            ),
            Ok(None) => f.write_str("torrent not known to the tracker"),
            Err(e) => write!(f, "failed: {e:#}"),
        }
    }
}

/// Scrapes every tracker in `tiers` for the torrent `info_hash`, all at
/// once, and returns each answer, tier by tier in the order listed.
pub async fn scrape_tiers(tiers: &[Vec<String>], info_hash: [u8; 20]) -> Vec<TrackerScrape> {
    let mut tasks = tokio::task::JoinSet::new();
    let trackers = tiers
        .iter()
        .enumerate()
        .flat_map(|(tier, trackers)| trackers.iter().map(move |tracker| (tier + 1, tracker)));
    for (order, (tier, tracker)) in trackers.enumerate() {
        let tracker = tracker.clone();
        tasks.spawn(async move {
            let result = scrape(&tracker, &[info_hash]).await.and_then(|stats| {
                stats
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("tracker sent no answer"))
            });
            (
                order,
                TrackerScrape {
                    tier,
                    tracker,
                    result,
                },
            )
        });
    }
    let mut scrapes = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(scrape) => scrapes.push(scrape),
            Err(e) => warn!("scrape task failed: {e}"),
        }
    }
    scrapes.sort_by_key(|(order, _)| *order);
    scrapes.into_iter().map(|(_, scrape)| scrape).collect()
}

async fn scrape_udp(url: &Url, info_hashes: &[[u8; 20]]) -> Result<Vec<ScrapeStats>> {
    let (socket, _) = udp_socket(url).await?;
    let mut stats = Vec::with_capacity(info_hashes.len());
    for chunk in info_hashes.chunks(UDP_MAX_SCRAPE) {
        // A fresh connection id per request, as the last one may have
        // expired while earlier requests were retried.
        let connection_id = udp_connect(&socket).await?;
        let request = udp_scrape_request(connection_id, chunk);
        let response = udp_transact(&socket, request, UDP_ACTION_SCRAPE).await?;
        stats.extend(parse_udp_scrape(&response, chunk.len())?);
    }
    Ok(stats)
}

/// A scrape request without its transaction id, which `udp_transact` adds.
fn udp_scrape_request(connection_id: u64, info_hashes: &[[u8; 20]]) -> Vec<u8> {
    let mut request = Vec::with_capacity(12 + 20 * info_hashes.len());
    request.extend_from_slice(&connection_id.to_be_bytes());
    request.extend_from_slice(&UDP_ACTION_SCRAPE.to_be_bytes());
    for info_hash in info_hashes {
        request.extend_from_slice(info_hash);
    }
    request
}

/// Parses a scrape response for `count` torrents: after the 8-byte header,
/// seeders, completed and leechers for each, in the order asked.
fn parse_udp_scrape(response: &[u8], count: usize) -> Result<Vec<ScrapeStats>> {
    let (entries, _) = response.get(8..).unwrap_or_default().as_chunks::<12>();
    if entries.len() < count {
        bail!(
            "scrape response too short: {} of {count} torrents",
            entries.len()
        );
    }
    Ok(entries[..count]
        .iter()
        .map(
            |&[s0, s1, s2, s3, c0, c1, c2, c3, l0, l1, l2, l3]| ScrapeStats {
                seeders: u32::from_be_bytes([s0, s1, s2, s3]).into(),
                completed: u32::from_be_bytes([c0, c1, c2, c3]).into(),
                leechers: u32::from_be_bytes([l0, l1, l2, l3]).into(),
            },
        )
        .collect())
}

async fn scrape_http(url: &Url, info_hashes: &[[u8; 20]]) -> Result<Vec<Option<ScrapeStats>>> {
    let client = reqwest::Client::builder().timeout(HTTP_TIMEOUT).build()?;
    let body = client
        .get(http_scrape_url(url, info_hashes)?)
        .send()
        .await?
        .error_for_status()?
        .bytes()
        .await?;
    parse_http_scrape(&body, info_hashes)
}

/// The scrape URL for an HTTP announce URL, asking about `info_hashes`
/// (BEP 48). The last path segment must start with `announce`, which becomes
/// `scrape` (so `/announce.php` becomes `/scrape.php`); a tracker whose URL
/// does not fit does not support scrape.
fn http_scrape_url(announce: &Url, info_hashes: &[[u8; 20]]) -> Result<String> {
    let path = announce.path();
    let (dir, last) = path.rsplit_once('/').unwrap_or(("", path));
    let Some(rest) = last.strip_prefix("announce") else {
        bail!("tracker does not support scrape");
    };
    let mut url = announce.clone();
    url.set_path(&format!("{dir}/scrape{rest}"));
    url.set_fragment(None);
    let mut separator = if url.query().is_some() { '&' } else { '?' };
    let mut url = String::from(url);
    for info_hash in info_hashes {
        url.push(separator);
        url.push_str("info_hash=");
        url.push_str(&percent_encode(info_hash));
        separator = '&';
    }
    Ok(url)
}

#[derive(Deserialize)]
struct HttpScrapeResponse {
    #[serde(rename = "failure reason", default)]
    failure_reason: Option<String>,
    /// Counts keyed by raw 20-byte info hash.
    #[serde(default)]
    files: Option<Value>,
}

fn parse_http_scrape(body: &[u8], info_hashes: &[[u8; 20]]) -> Result<Vec<Option<ScrapeStats>>> {
    let response: HttpScrapeResponse =
        serde_bencode::from_bytes(body).context("invalid scrape response")?;
    if let Some(reason) = response.failure_reason {
        bail!("tracker error: {}", reason);
    }
    let files = match response.files {
        Some(Value::Dict(files)) => files,
        Some(_) => bail!("unexpected `files` type in scrape response"),
        None => Default::default(),
    };
    info_hashes
        .iter()
        .map(|info_hash| {
            files
                .get(info_hash.as_slice())
                .map(parse_scrape_file)
                .transpose()
        })
        .collect()
}

/// Parses one torrent's entry in an HTTP scrape. A missing count is taken
/// as zero.
fn parse_scrape_file(value: &Value) -> Result<ScrapeStats> {
    let Value::Dict(dict) = value else {
        bail!("unexpected torrent entry type in scrape response");
    };
    let count = |key: &str| match dict.get(key.as_bytes()) {
        Some(Value::Int(n)) => {
            u64::try_from(*n).map_err(|_| anyhow!("negative `{key}` in scrape response"))
        }
        Some(_) => Err(anyhow!("unexpected `{key}` type in scrape response")),
        None => Ok(0),
    };
    Ok(ScrapeStats {
        seeders: count("complete")?,
        leechers: count("incomplete")?,
        completed: count("downloaded")?,
    })
}

#[cfg(test)]
mod tests;
