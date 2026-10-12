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

    fn tiers(tiers: &[&[&str]]) -> Tiers {
        Tiers(
            tiers
                .iter()
                .map(|tier| tier.iter().map(|t| t.to_string()).collect())
                .collect(),
        )
    }

    /// Announces `event` with canned answers: a tracker listed in `answers`
    /// returns that many peers, any other fails. Also returns the trackers
    /// asked, in order.
    async fn announce_fake(
        tiers: &mut Tiers,
        event: AnnounceEvent,
        answers: &[(&str, usize)],
    ) -> (Result<AnnounceResponse>, Vec<String>) {
        let mut asked = Vec::new();
        let request = AnnounceRequest { event, ..request() };
        let result = tiers
            .announce_with(
                &request,
                async |tracker: &str, _: &AnnounceRequest| -> Result<AnnounceResponse> {
                    asked.push(tracker.to_string());
                    let &(_, peers) = answers
                        .iter()
                        .find(|(t, _)| *t == tracker)
                        .ok_or_else(|| anyhow!("tracker down"))?;
                    Ok(AnnounceResponse {
                        peers: vec!["1.2.3.4:80".parse().unwrap(); peers],
                        interval: None,
                        min_interval: None,
                    })
                },
            )
            .await;
        (result, asked)
    }

    #[tokio::test]
    async fn the_tracker_that_answers_moves_to_the_front_of_its_tier() {
        let mut t = tiers(&[&["a", "b", "c"], &["d"]]);
        let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[("c", 1)]).await;
        assert_eq!(result.unwrap().peers.len(), 1);
        assert_eq!(asked, ["a", "b", "c"]);
        assert_eq!(t, tiers(&[&["c", "a", "b"], &["d"]]));

        let (_, asked) = announce_fake(&mut t, AnnounceEvent::Regular, &[("c", 1)]).await;
        assert_eq!(asked, ["c"]);
    }

    #[tokio::test]
    async fn later_tiers_are_tried_when_a_tier_fails() {
        let mut t = tiers(&[&["a", "b"], &["c", "d"]]);
        let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[("d", 2)]).await;
        assert_eq!(result.unwrap().peers.len(), 2);
        assert_eq!(asked, ["a", "b", "c", "d"]);
        assert_eq!(t, tiers(&[&["a", "b"], &["d", "c"]]));
    }

    #[tokio::test]
    async fn trackers_without_peers_are_skipped_only_when_looking_for_peers() {
        let answers = [("b", 0), ("c", 3)];
        let mut t = tiers(&[&["a", "b", "c"]]);
        let (result, asked) = announce_fake(&mut t, AnnounceEvent::Regular, &answers).await;
        assert_eq!(result.unwrap().peers.len(), 3);
        assert_eq!(asked, ["a", "b", "c"]);
        assert_eq!(t, tiers(&[&["c", "a", "b"]]));

        let mut t = tiers(&[&["a", "b", "c"]]);
        let (result, asked) = announce_fake(&mut t, AnnounceEvent::Stopped, &answers).await;
        assert!(result.unwrap().peers.is_empty());
        assert_eq!(asked, ["a", "b"]);
        assert_eq!(t, tiers(&[&["b", "a", "c"]]));

        // No tracker has peers: the first one that answered wins.
        let mut t = tiers(&[&["a"], &["b", "c"]]);
        let (result, _) =
            announce_fake(&mut t, AnnounceEvent::Started, &[("c", 0), ("a", 0)]).await;
        assert!(result.unwrap().peers.is_empty());
        assert_eq!(t, tiers(&[&["a"], &["b", "c"]]));
    }

    #[tokio::test]
    async fn fails_when_no_tracker_answers() {
        let mut t = tiers(&[&["a"], &["b"]]);
        let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[]).await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("every tracker failed")
        );
        assert_eq!(asked, ["a", "b"]);
        assert_eq!(t, tiers(&[&["a"], &["b"]]));

        for mut empty in [tiers(&[]), tiers(&[&[], &[]])] {
            let (result, asked) = announce_fake(&mut empty, AnnounceEvent::Started, &[]).await;
            assert!(result.unwrap_err().to_string().contains("no trackers"));
            assert!(asked.is_empty());
        }
    }

    #[test]
    fn new_shuffles_within_tiers_only() {
        let original = tiers(&[&["a", "b", "c"], &["d", "e"]]);
        let mut orders = std::collections::HashSet::new();
        for _ in 0..100 {
            let Tiers(mut shuffled) = Tiers::new(original.0.clone());
            orders.insert(shuffled[0].clone());
            shuffled.iter_mut().for_each(|tier| tier.sort());
            assert_eq!(Tiers(shuffled), original);
        }
        assert!(orders.len() > 1, "tiers were never shuffled");
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

    #[test]
    fn udp_scrape_request_layout() {
        let request = udp_scrape_request(0x0102_0304_0506_0708, &[[0xaa; 20], [0xbb; 20]]);
        assert_eq!(request.len(), 12 + 40);
        assert_eq!(request[..8], [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(request[8..12], [0, 0, 0, 2]);
        assert_eq!(request[12..32], [0xaa; 20]);
        assert_eq!(request[32..], [0xbb; 20]);
    }

    #[test]
    fn udp_scrape_response() {
        let mut response = vec![0, 0, 0, 2, 9, 9, 9, 9];
        response.extend_from_slice(&[0, 0, 0, 5, 0, 0, 1, 0, 0, 0, 0, 7]);
        response.extend_from_slice(&[0; 12]);
        let stats = parse_udp_scrape(&response, 2).unwrap();
        assert_eq!(
            stats,
            [
                ScrapeStats {
                    seeders: 5,
                    leechers: 7,
                    completed: 256
                },
                ScrapeStats {
                    seeders: 0,
                    leechers: 0,
                    completed: 0
                }
            ]
        );
        // Fewer entries than torrents asked about, or a cut-off entry.
        assert!(parse_udp_scrape(&response, 3).is_err());
        assert!(parse_udp_scrape(&response[..response.len() - 1], 2).is_err());
        assert!(parse_udp_scrape(&response[..4], 1).is_err());
        assert_eq!(parse_udp_scrape(&response[..4], 0).unwrap(), []);
    }

    #[test]
    fn http_scrape_urls() {
        let scrape = |announce: &str| {
            http_scrape_url(&Url::parse(announce).unwrap(), &[[0xab; 20]])
                .map(|url| url.replace(&"%ab".repeat(20), "H"))
                .ok()
        };
        // The examples of BEP 48.
        let cases = [
            (
                "http://example.com/announce",
                Some("http://example.com/scrape?info_hash=H"),
            ),
            (
                "http://example.com/x/announce",
                Some("http://example.com/x/scrape?info_hash=H"),
            ),
            (
                "http://example.com/announce.php",
                Some("http://example.com/scrape.php?info_hash=H"),
            ),
            (
                "http://example.com/announce?x2%0644",
                Some("http://example.com/scrape?x2%0644&info_hash=H"),
            ),
            (
                "https://example.com:8080/announce#frag",
                Some("https://example.com:8080/scrape?info_hash=H"),
            ),
            ("http://example.com/a", None),
            (
                "http://example.com/announce?x=2/4",
                Some("http://example.com/scrape?x=2/4&info_hash=H"),
            ),
            ("http://example.com/x%064announce", None),
            ("http://example.com/announce/x", None),
            ("http://example.com/", None),
        ];
        for (announce, expected) in cases {
            assert_eq!(scrape(announce).as_deref(), expected, "{announce}");
        }
        let url = Url::parse("http://t/announce").unwrap();
        let url = http_scrape_url(&url, &[[1; 20], [2; 20]]).unwrap();
        assert!(url.starts_with("http://t/scrape?info_hash=%01%01"));
        assert!(url.contains("%01&info_hash=%02%02"));
    }

    #[test]
    fn http_scrape_response() {
        let mut body = b"d5:filesd20:".to_vec();
        body.extend_from_slice(&[0xff; 20]);
        body.extend_from_slice(b"d8:completei5e10:downloadedi50e10:incompletei10ee20:");
        body.extend_from_slice(&[0xee; 20]);
        body.extend_from_slice(b"d8:completei1eee5:flagsd20:min_request_intervali900eee");
        let stats = parse_http_scrape(&body, &[[0xff; 20], [0x00; 20], [0xee; 20]]).unwrap();
        assert_eq!(
            stats,
            [
                Some(ScrapeStats {
                    seeders: 5,
                    leechers: 10,
                    completed: 50
                }),
                None,
                Some(ScrapeStats {
                    seeders: 1,
                    leechers: 0,
                    completed: 0
                }),
            ]
        );
        assert_eq!(parse_http_scrape(b"de", &[[0; 20]]).unwrap(), [None]);
    }

    #[test]
    fn http_scrape_errors() {
        let err = parse_http_scrape(b"d14:failure reason9:not founde", &[[0; 20]]).unwrap_err();
        assert!(err.to_string().contains("not found"));
        let hash = [b'h'; 20];
        for body in [
            &b"not bencode"[..],
            b"d5:filesi1ee",
            b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhi1eee",
            b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhd8:completei-1eeee",
            b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhd8:complete1:xeee",
        ] {
            assert!(
                parse_http_scrape(body, &[hash]).is_err(),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
    }

    #[test]
    fn tracker_scrape_display() {
        let line = |result| {
            TrackerScrape {
                tier: 2,
                tracker: "udp://t:1".into(),
                result,
            }
            .to_string()
        };
        let stats = ScrapeStats {
            seeders: 3,
            leechers: 4,
            completed: 5,
        };
        assert_eq!(
            line(Ok(Some(stats))),
            "tier 2  udp://t:1  3 seeders, 4 leechers, 5 completed"
        );
        assert_eq!(
            line(Ok(None)),
            "tier 2  udp://t:1  torrent not known to the tracker"
        );
        assert_eq!(
            line(Err(anyhow!("down"))),
            "tier 2  udp://t:1  failed: down"
        );
    }

    #[tokio::test]
    async fn scrape_rejects_unknown_schemes() {
        for tracker in ["wss://t/announce", "not a url"] {
            assert!(scrape(tracker, &[[0; 20]]).await.is_err(), "{tracker}");
        }
    }
}
