use super::{AnnounceRequest, AnnounceResponse, ScrapeStats, parse_compact_v4, parse_compact_v6};
use anyhow::{Result, anyhow, bail};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::timeout;
use tracing::debug;
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

/// How long resolving a UDP tracker's host name may take.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn announce_udp(url: &Url, request: &AnnounceRequest) -> Result<AnnounceResponse> {
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
    announce.extend_from_slice(&request.key.to_be_bytes());
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
    let word = |at: usize| {
        u32::from_be_bytes([
            response[at],
            response[at + 1],
            response[at + 2],
            response[at + 3],
        ])
    };
    let (interval, leechers, seeders) = (word(8), word(12), word(16));
    let peers = if ipv6 {
        parse_compact_v6(&response[20..])
    } else {
        parse_compact_v4(&response[20..])
    };
    Ok(AnnounceResponse {
        peers,
        interval: Some(interval.into()),
        min_interval: None,
        seeders: Some(seeders),
        leechers: Some(leechers),
    })
}

pub(super) async fn scrape_udp(url: &Url, info_hashes: &[[u8; 20]]) -> Result<Vec<ScrapeStats>> {
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

#[cfg(test)]
mod tests;
