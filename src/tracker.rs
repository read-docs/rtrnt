use anyhow::{Context, Result, anyhow, bail};
use rand::seq::SliceRandom;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tracing::{debug, warn};
use url::Url;

pub mod announcer;
mod http;
mod udp;

use http::{announce_http, scrape_http};
use udp::{announce_udp, scrape_udp};

#[derive(Debug, Clone, Copy)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    /// Identifies us to UDP trackers across announces (BEP 15). Fixed for
    /// the session: the `Announcer` picks it, whatever the caller sets.
    pub key: u32,
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
    /// Peers with the whole torrent, if the tracker says.
    pub seeders: Option<u32>,
    /// Peers still downloading, if the tracker says.
    pub leechers: Option<u32>,
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
        self.announce_with(request, announce_one, |_, _| {}).await
    }

    /// `announce`, telling `report` what each tracker asked answered.
    pub async fn announce_reporting(
        &mut self,
        request: &AnnounceRequest,
        report: impl Fn(&str, &Result<AnnounceResponse>),
    ) -> Result<AnnounceResponse> {
        self.announce_with(request, announce_one, report).await
    }

    async fn announce_with(
        &mut self,
        request: &AnnounceRequest,
        mut announce_one: impl AsyncFnMut(&str, &AnnounceRequest) -> Result<AnnounceResponse>,
        report: impl Fn(&str, &Result<AnnounceResponse>),
    ) -> Result<AnnounceResponse> {
        if self.0.iter().all(Vec::is_empty) {
            bail!("torrent has no trackers");
        }
        let mut empty = None;
        for tier in 0..self.0.len() {
            for index in 0..self.0[tier].len() {
                let tracker = &self.0[tier][index];
                let result = announce_one(tracker, request).await;
                report(tracker, &result);
                match result {
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

#[cfg(test)]
mod tests;
