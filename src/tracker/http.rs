use super::{AnnounceRequest, AnnounceResponse, ScrapeStats, parse_compact_v4, parse_compact_v6};
use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_bencode::value::Value;
use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;
use std::time::Duration;
use url::Url;

const HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// Shared by every announce and scrape, so connections to a tracker are reused.
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .timeout(HTTP_TIMEOUT)
        .build()
        .expect("the HTTP client builds with its default settings")
});

pub(super) async fn announce_http(
    tracker: &str,
    request: &AnnounceRequest,
) -> Result<AnnounceResponse> {
    let response = CLIENT
        .get(http_announce_url(tracker, request))
        .send()
        .await?;
    let status = response.status();
    let body = response.bytes().await?;
    parse_http_response(status, &body)
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
    complete: Option<u32>,
    #[serde(default)]
    incomplete: Option<u32>,
    #[serde(default)]
    peers: Option<Value>,
    #[serde(default)]
    peers6: Option<serde_bytes::ByteBuf>,
}

fn parse_http_response(status: StatusCode, body: &[u8]) -> Result<AnnounceResponse> {
    let response: HttpTrackerResponse = serde_bencode::from_bytes(body)
        .with_context(|| format!("invalid tracker response (HTTP {status})"))?;
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
        seeders: response.complete,
        leechers: response.incomplete,
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

pub(super) async fn scrape_http(
    url: &Url,
    info_hashes: &[[u8; 20]],
) -> Result<Vec<Option<ScrapeStats>>> {
    let body = CLIENT
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
