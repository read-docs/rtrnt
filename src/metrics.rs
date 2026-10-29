//! Machine-readable views of a session: a JSON line for `--json`, and the
//! Prometheus text format for the `metrics-listen` endpoint.
//!
//! Both are pure functions of a [`Snapshot`], the same copy of the stats
//! the terminal UI draws from, so rates and the rest agree with what the UI
//! shows. [`Sampler`] takes snapshots with their rates filled in, and
//! [`serve`] answers HTTP requests from the latest one.

use crate::stats::{SessionStats, TorrentState};
use crate::tui::app::Rates;
use crate::tui::log_buffer::LogBuffer;
use crate::tui::snapshot::{Snapshot, TorrentSnapshot};
use serde_json::{Value, json};
use std::fmt::Write as _;
use std::future::Future;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::debug;

/// How often the endpoint's snapshot is refreshed, and so how often its
/// rates change.
pub const SAMPLE_PERIOD: Duration = Duration::from_secs(1);

/// Longest request head (the request line and headers) that is read.
const MAX_REQUEST_HEAD: usize = 8 * 1024;
/// How long a client has to send its request.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// The media type of the Prometheus text format, version 0.0.4.
const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Takes snapshots of a session with the rates and ETAs filled in, which
/// need the difference with the last one: the first snapshot has none.
pub struct Sampler {
    stats: Arc<SessionStats>,
    logs: LogBuffer,
    rates: Rates,
}

impl Sampler {
    pub fn new(stats: Arc<SessionStats>) -> Self {
        Self {
            stats,
            logs: LogBuffer::default(),
            rates: Rates::default(),
        }
    }

    /// The session as it is now.
    pub fn sample(&mut self) -> Snapshot {
        let mut snapshot = Snapshot::take(&self.stats, &self.logs, None, false);
        self.rates.record(&snapshot, Instant::now());
        self.rates.apply(&mut snapshot, None);
        snapshot
    }
}

/// Samples `stats` every `period` in the background, and gives the latest
/// snapshot. The first is taken at once. The task ends when the receiver and
/// every clone of it are dropped.
pub fn sample_every(stats: Arc<SessionStats>, period: Duration) -> watch::Receiver<Snapshot> {
    let mut sampler = Sampler::new(stats);
    let (latest, receiver) = watch::channel(sampler.sample());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick is now, and its sample was taken above.
        tick.tick().await;
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    if latest.send(sampler.sample()).is_err() {
                        return;
                    }
                }
                () = latest.closed() => return,
            }
        }
    });
    receiver
}

/// How often `--json` prints the state of the session.
pub const JSON_PERIOD: Duration = Duration::from_secs(1);

/// Writes a line of JSON (see `json_line`) for the session to `out` now and
/// then every `period`, and once more when `stop` resolves, which is when
/// the torrents have ended and their last state is in. Fails, without a
/// last line, if `out` cannot be written to, such as a closed pipe.
pub async fn report_json(
    mut sampler: Sampler,
    period: Duration,
    mut out: impl Write,
    stop: impl Future<Output = ()>,
) -> io::Result<()> {
    let mut line = |snapshot: &Snapshot| -> io::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        writeln!(out, "{}", json_line(snapshot, now))?;
        out.flush()
    };
    let mut tick = tokio::time::interval(period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(stop);
    loop {
        tokio::select! {
            _ = tick.tick() => line(&sampler.sample())?,
            () = &mut stop => return line(&sampler.sample()),
        }
    }
}

/// What a torrent's state is called in the output: the words the terminal
/// UI uses.
pub fn state_name(state: &TorrentState) -> &'static str {
    match state {
        TorrentState::FetchingMetadata { .. } => "metadata",
        TorrentState::QueuedForChecking | TorrentState::Queued => "queued",
        TorrentState::Checking { .. } => "checking",
        TorrentState::Downloading => "downloading",
        TorrentState::Seeding => "seeding",
        TorrentState::Pausing => "pausing",
        TorrentState::Paused => "paused",
        TorrentState::Removing => "removing",
        TorrentState::Done => "done",
        TorrentState::Interrupted => "stopped",
        TorrentState::Failed(_) => "failed",
    }
}

/// Uploaded over downloaded this session, or `None` while nothing has been
/// downloaded (a torrent that was complete at the start has no ratio).
pub fn ratio(downloaded: u64, uploaded: u64) -> Option<f64> {
    (downloaded > 0).then(|| uploaded as f64 / downloaded as f64)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// Bytes downloaded and uploaded this session, over all torrents.
fn totals(snapshot: &Snapshot) -> (u64, u64) {
    snapshot
        .torrents
        .iter()
        .fold((0, 0), |(down, up), torrent| {
            (down + torrent.downloaded, up + torrent.uploaded)
        })
}

/// `snapshot` as one line of JSON, without the newline, taken at
/// `unix_time` (seconds since the epoch).
///
/// The object has `time`, the session's `downloaded` and `uploaded` bytes
/// this session, its `rate_down` and `rate_up` in bytes per second, its
/// `connections`, `dht_nodes` and `port` (`null` if unknown), and
/// `torrents`, a list with each torrent's `name`, `info_hash` (hex),
/// `state`, `error` (`null` unless it failed), `progress` (0 to 1), `size`
/// and `left` in bytes (`null` until the metadata is known), `verified`,
/// `downloaded` and `uploaded` bytes, `rate_down` and `rate_up` in bytes
/// per second, `peers`, `seeds` (peers with every piece), `ratio` (`null`
/// until something is downloaded) and `eta` in seconds (`null` if unknown).
pub fn json_line(snapshot: &Snapshot, unix_time: u64) -> String {
    let (rate_down, rate_up) = snapshot.rates();
    let (downloaded, uploaded) = totals(snapshot);
    let torrents: Vec<Value> = snapshot.torrents.iter().map(torrent_json).collect();
    json!({
        "time": unix_time,
        "downloaded": downloaded,
        "uploaded": uploaded,
        "rate_down": rate_down,
        "rate_up": rate_up,
        "connections": snapshot.session.connections,
        "dht_nodes": snapshot.session.dht_nodes,
        "port": snapshot.session.port,
        "torrents": torrents,
    })
    .to_string()
}

fn torrent_json(torrent: &TorrentSnapshot) -> Value {
    let error = match &torrent.state {
        TorrentState::Failed(why) => Some(why.as_str()),
        _ => None,
    };
    json!({
        "name": torrent.name,
        "info_hash": hex(&torrent.info_hash),
        "state": state_name(&torrent.state),
        "error": error,
        "progress": torrent.progress(),
        "size": torrent.size(),
        "left": torrent.left(),
        "verified": torrent.verified,
        "downloaded": torrent.downloaded,
        "uploaded": torrent.uploaded,
        "rate_down": torrent.rate_down,
        "rate_up": torrent.rate_up,
        "peers": torrent.peers,
        "seeds": torrent.seeds,
        "ratio": ratio(torrent.downloaded, torrent.uploaded),
        "eta": torrent.eta.map(|eta| eta.as_secs()),
    })
}

/// Escapes `text` as the value of a label: backslash, double quote and
/// newline are written `\\`, `\"` and `\n`.
pub fn escape_label(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            c => escaped.push(c),
        }
    }
    escaped
}

/// Escapes `text` for a `# HELP` line: backslash and newline only.
fn escape_help(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\n', "\\n")
}

/// The kind of a metric.
#[derive(Clone, Copy)]
enum Kind {
    Gauge,
    Counter,
}

/// Builds the text exposition: each family is written once, with its
/// `# HELP` and `# TYPE` lines, followed by all of its samples.
#[derive(Default)]
struct Exposition(String);

impl Exposition {
    /// Adds the family `name` with `samples`, each a label set (empty or
    /// in braces) and a value. A family with no sample is left out.
    fn family(&mut self, name: &str, kind: Kind, help: &str, samples: Vec<(String, f64)>) {
        if samples.is_empty() {
            return;
        }
        let kind = match kind {
            Kind::Gauge => "gauge",
            Kind::Counter => "counter",
        };
        // Writing to a String cannot fail.
        let _ = writeln!(self.0, "# HELP {name} {}", escape_help(help));
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
        for (labels, value) in samples {
            let _ = writeln!(self.0, "{name}{labels} {value}");
        }
    }
}

/// The labels that name a torrent, `{info_hash="...",name="..."}`, and
/// `state` too if given.
fn torrent_labels(torrent: &TorrentSnapshot, state: Option<&str>) -> String {
    let mut labels = format!(
        "{{info_hash=\"{}\",name=\"{}\"",
        hex(&torrent.info_hash),
        escape_label(&torrent.name)
    );
    if let Some(state) = state {
        let _ = write!(labels, ",state=\"{}\"", escape_label(state));
    }
    labels.push('}');
    labels
}

/// `snapshot` in the Prometheus text exposition format (version 0.0.4).
///
/// Per torrent, labelled with `info_hash` and `name`: counters
/// `rtrnt_torrent_downloaded_bytes_total` and
/// `rtrnt_torrent_uploaded_bytes_total` (this session), gauges for the
/// rates in bytes per second, `peers`, `seeds`, `progress` (0 to 1) and the
/// size; and `rtrnt_torrent_state`, 1 for the torrent's current state,
/// given in an extra `state` label. For the session: the same two counters
/// summed, the rates, the torrent count, `connections`, and `dht_nodes`
/// while a DHT node runs.
pub fn prometheus(snapshot: &Snapshot) -> String {
    let mut out = Exposition::default();
    let each = |value: fn(&TorrentSnapshot) -> f64| -> Vec<(String, f64)> {
        snapshot
            .torrents
            .iter()
            .map(|torrent| (torrent_labels(torrent, None), value(torrent)))
            .collect()
    };
    out.family(
        "rtrnt_torrent_downloaded_bytes_total",
        Kind::Counter,
        "Verified piece data received this session.",
        each(|t| t.downloaded as f64),
    );
    out.family(
        "rtrnt_torrent_uploaded_bytes_total",
        Kind::Counter,
        "Piece data sent to peers this session.",
        each(|t| t.uploaded as f64),
    );
    out.family(
        "rtrnt_torrent_download_rate_bytes",
        Kind::Gauge,
        "Download rate in bytes per second.",
        each(|t| t.rate_down),
    );
    out.family(
        "rtrnt_torrent_upload_rate_bytes",
        Kind::Gauge,
        "Upload rate in bytes per second.",
        each(|t| t.rate_up),
    );
    out.family(
        "rtrnt_torrent_peers",
        Kind::Gauge,
        "Peers connected.",
        each(|t| t.peers as f64),
    );
    out.family(
        "rtrnt_torrent_seeds",
        Kind::Gauge,
        "Connected peers that have every piece.",
        each(|t| t.seeds as f64),
    );
    out.family(
        "rtrnt_torrent_progress",
        Kind::Gauge,
        "Fraction done, from 0 to 1.",
        each(TorrentSnapshot::progress),
    );
    out.family(
        "rtrnt_torrent_size_bytes",
        Kind::Gauge,
        "Bytes to download in all, once the metadata is known.",
        snapshot
            .torrents
            .iter()
            .filter_map(|t| Some((torrent_labels(t, None), t.size()? as f64)))
            .collect(),
    );
    out.family(
        "rtrnt_torrent_state",
        Kind::Gauge,
        "1 for the state the torrent is in, named by the state label.",
        snapshot
            .torrents
            .iter()
            .map(|t| (torrent_labels(t, Some(state_name(&t.state))), 1.0))
            .collect(),
    );
    let (rate_down, rate_up) = snapshot.rates();
    let (downloaded, uploaded) = totals(snapshot);
    let one = |value: f64| vec![(String::new(), value)];
    out.family(
        "rtrnt_downloaded_bytes_total",
        Kind::Counter,
        "Verified piece data received this session, over all torrents.",
        one(downloaded as f64),
    );
    out.family(
        "rtrnt_uploaded_bytes_total",
        Kind::Counter,
        "Piece data sent to peers this session, over all torrents.",
        one(uploaded as f64),
    );
    out.family(
        "rtrnt_download_rate_bytes",
        Kind::Gauge,
        "Download rate of the session in bytes per second.",
        one(rate_down),
    );
    out.family(
        "rtrnt_upload_rate_bytes",
        Kind::Gauge,
        "Upload rate of the session in bytes per second.",
        one(rate_up),
    );
    out.family(
        "rtrnt_torrents",
        Kind::Gauge,
        "Torrents in the session.",
        one(snapshot.torrents.len() as f64),
    );
    out.family(
        "rtrnt_connections",
        Kind::Gauge,
        "Peer connections open, across all torrents.",
        one(snapshot.session.connections as f64),
    );
    out.family(
        "rtrnt_dht_nodes",
        Kind::Gauge,
        "Nodes in the DHT routing table.",
        snapshot
            .session
            .dht_nodes
            .map(|nodes| one(nodes as f64))
            .unwrap_or_default(),
    );
    out.0
}

/// What a request asks for.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    Metrics,
    NotFound,
    BadRequest,
}

/// Decides what to answer to the request whose head is `head`: only
/// `GET /metrics` (with or without a query) is served.
fn route(head: &str) -> Route {
    let mut parts = head.lines().next().unwrap_or_default().split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Route::BadRequest;
    };
    if !version.starts_with("HTTP/1.") {
        return Route::BadRequest;
    }
    let path = target.split('?').next().unwrap_or_default();
    if method == "GET" && path == "/metrics" {
        Route::Metrics
    } else {
        Route::NotFound
    }
}

/// An HTTP/1.1 response that closes the connection after `body`.
fn response(status: &str, content_type: &str, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// The response to a request with head `head`, with the metrics of
/// `snapshot` if it asks for them.
fn answer(head: &str, snapshot: &Snapshot) -> Vec<u8> {
    match route(head) {
        Route::Metrics => response("200 OK", CONTENT_TYPE, &prometheus(snapshot)),
        Route::NotFound => response("404 Not Found", "text/plain", "not found\n"),
        Route::BadRequest => response("400 Bad Request", "text/plain", "bad request\n"),
    }
}

/// Serves the latest snapshot from `snapshots` as Prometheus metrics to
/// every client of `listener`, forever. Only `GET /metrics` is answered;
/// anything else is a 404 (or a 400 if it is not HTTP). Each connection
/// serves one request.
pub async fn serve(listener: TcpListener, snapshots: watch::Receiver<Snapshot>) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                debug!("metrics endpoint cannot accept: {e}");
                // Out of file descriptors, say: do not spin.
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let snapshots = snapshots.clone();
        tokio::spawn(async move {
            let served = tokio::time::timeout(REQUEST_TIMEOUT, handle(stream, snapshots)).await;
            if let Ok(Err(e)) = served {
                debug!("metrics request failed: {e}");
            }
        });
    }
}

/// Whether `buf` holds a whole request head: it ends at a blank line.
fn head_complete(buf: &[u8]) -> bool {
    buf.windows(4).any(|window| window == b"\r\n\r\n") || buf.windows(2).any(|w| w == b"\n\n")
}

/// Reads one request from `stream` and answers it.
async fn handle(mut stream: TcpStream, snapshots: watch::Receiver<Snapshot>) -> io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head_complete(&buf) {
        if buf.len() >= MAX_REQUEST_HEAD {
            let reply = response(
                "431 Request Header Fields Too Large",
                "text/plain",
                "too large\n",
            );
            return stream.write_all(&reply).await;
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let head = String::from_utf8_lossy(&buf);
    let reply = answer(&head, &snapshots.borrow());
    stream.write_all(&reply).await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests;
