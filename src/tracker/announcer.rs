//! Keeps the tracker informed for the whole run: re-announces on the
//! tracker's interval with real stats, sends `completed` when the download
//! finishes, and `stopped` at the end.

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, Tiers};
use crate::stats::TorrentStats;
use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

/// Used when the tracker does not say how often to announce.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Bounds on the time between announces, whatever the tracker asks for.
const MIN_INTERVAL: Duration = Duration::from_secs(60);
const MAX_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
/// How long the final `completed` and `stopped` announces may take in total.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Payload bytes moved this session, as reported to the tracker.
#[derive(Debug, Default)]
pub struct Transferred {
    /// Verified piece data received, not counting data already on disk.
    pub downloaded: AtomicU64,
    /// Block data sent to peers.
    pub uploaded: AtomicU64,
}

impl Transferred {
    fn load(&self) -> (u64, u64) {
        (
            self.downloaded.load(Ordering::Relaxed),
            self.uploaded.load(Ordering::Relaxed),
        )
    }
}

/// Whether the tracker has heard that the download finished.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Completion {
    Downloading,
    /// Finished, and `completed` is still to be sent.
    Due,
    Sent,
}

pub struct Announcer {
    trackers: Tiers,
    /// The `started` request. Later announces send its stats updated.
    started: AnnounceRequest,
    interval: Duration,
    /// Shortest gap between announces the tracker allows.
    min_interval: Duration,
    /// When the last announce was sent.
    last: Instant,
    /// When the next announce is due.
    next: Instant,
    completion: Completion,
    /// A background announce, of the given event. It works on a copy of the
    /// tiers and hands it back, reordered to put the tracker that answered
    /// first.
    in_flight: Option<JoinHandle<(Tiers, AnnounceEvent, Result<AnnounceResponse>)>>,
    /// Told what each tracker answered, and when the next announce is due.
    stats: Option<Arc<TorrentStats>>,
}

impl Announcer {
    /// Sends `request` as the `started` announce to the tracker `tiers`.
    /// Returns the announcer and the peers the tracker sent back, which may
    /// be none.
    pub async fn start(
        tiers: Vec<Vec<String>>,
        request: AnnounceRequest,
    ) -> Result<(Self, Vec<SocketAddr>)> {
        Self::start_reporting(tiers, request, None).await
    }

    /// `start`, recording in `stats` what each tracker answers, now and on
    /// every announce after.
    pub async fn start_reporting(
        tiers: Vec<Vec<String>>,
        request: AnnounceRequest,
        stats: Option<Arc<TorrentStats>>,
    ) -> Result<(Self, Vec<SocketAddr>)> {
        if let Some(stats) = &stats {
            stats.set_trackers(&tiers);
        }
        let mut announcer = Self::new(Tiers::new(tiers), request, Instant::now());
        announcer.stats = stats;
        let started = announcer.started;
        let stats = announcer.stats.clone();
        let response = announcer
            .trackers
            .announce_reporting(&started, |url, result| report(&stats, url, result))
            .await?;
        announcer.schedule(&response);
        Ok((announcer, response.peers))
    }

    fn new(trackers: Tiers, request: AnnounceRequest, now: Instant) -> Self {
        Self {
            trackers,
            started: AnnounceRequest {
                event: AnnounceEvent::Started,
                ..request
            },
            interval: DEFAULT_INTERVAL,
            min_interval: MIN_INTERVAL,
            last: now,
            next: now + DEFAULT_INTERVAL,
            completion: Completion::Downloading,
            in_flight: None,
            stats: None,
        }
    }

    /// Plans the next announce from the tracker's answer to the last one.
    fn schedule(&mut self, response: &AnnounceResponse) {
        let secs = |s: Option<u64>| s.map(Duration::from_secs);
        self.min_interval = secs(response.min_interval)
            .unwrap_or(MIN_INTERVAL)
            .clamp(MIN_INTERVAL, MAX_INTERVAL);
        self.interval = secs(response.interval)
            .unwrap_or(DEFAULT_INTERVAL)
            .clamp(self.min_interval, MAX_INTERVAL);
        self.next = self.last + self.interval;
        self.publish_next();
        debug!(interval = ?self.interval, "next announce scheduled");
    }

    /// Tells the stats when the next announce is due.
    fn publish_next(&self) {
        if let Some(stats) = &self.stats {
            stats.set_next_announce(Some(self.next.into_std()));
        }
    }

    /// The request for `event`, reporting the bytes `downloaded` and
    /// `uploaded` since `started`.
    fn request(&self, event: AnnounceEvent, (downloaded, uploaded): (u64, u64)) -> AnnounceRequest {
        AnnounceRequest {
            event,
            downloaded,
            uploaded,
            left: self.started.left.saturating_sub(downloaded),
            ..self.started
        }
    }

    /// Sets how many bytes were left to download when we started, for a
    /// magnet link whose size was unknown at the `started` announce. Later
    /// announces count down from it.
    pub fn set_left(&mut self, left: u64) {
        self.started.left = left;
    }

    /// Records that the download finished: the next announce, due at once,
    /// is `completed`. If it has not gone out by `stop`, it goes then.
    pub fn complete(&mut self) {
        if self.completion == Completion::Downloading {
            self.completion = Completion::Due;
            self.next = Instant::now();
            self.publish_next();
        }
    }

    /// Brings the next announce forward to the earliest the tracker allows,
    /// for when we have run out of peers.
    pub fn hurry(&mut self) {
        self.next = self.next.min(self.last + self.min_interval);
        self.publish_next();
    }

    /// Waits until the next announce is due, sends it in the background
    /// with the current `transferred` counts, and returns the peers it got
    /// (none if every tracker failed). It is `completed` if the download has
    /// finished since the last one, otherwise a regular announce.
    ///
    /// Cancel-safe: an announce already sent is kept, and the next call
    /// waits for its answer.
    pub async fn poll(&mut self, transferred: &Transferred) -> Vec<SocketAddr> {
        if self.in_flight.is_none() {
            tokio::time::sleep_until(self.next).await;
            self.last = Instant::now();
        }
        let event = match self.completion {
            Completion::Due => AnnounceEvent::Completed,
            _ => AnnounceEvent::Regular,
        };
        let request = self.request(event, transferred.load());
        let handle = self.in_flight.get_or_insert_with(|| {
            let mut trackers = self.trackers.clone();
            let stats = self.stats.clone();
            let announce = async move {
                let result = trackers
                    .announce_reporting(&request, |url, result| report(&stats, url, result))
                    .await;
                (trackers, request.event, result)
            };
            tokio::spawn(announce.in_current_span())
        });
        let (event, result) = match handle.await {
            Ok((trackers, event, result)) => {
                self.trackers = trackers;
                (event, result)
            }
            Err(e) => (AnnounceEvent::Regular, Err(e.into())),
        };
        self.in_flight = None;
        match result {
            Ok(response) => {
                if event == AnnounceEvent::Completed {
                    self.completion = Completion::Sent;
                }
                info!(peers = response.peers.len(), ?event, "re-announced");
                self.schedule(&response);
                response.peers
            }
            Err(e) => {
                warn!("re-announce failed: {e:#}");
                self.next = self.last + self.interval;
                self.publish_next();
                Vec::new()
            }
        }
    }

    /// Tells the tracker we are leaving, with the final `transferred`
    /// counts: `completed` first if the download finished and the tracker
    /// has not heard yet, then `stopped`. Failures are only logged, and the
    /// tracker gets `STOP_TIMEOUT` in total so it cannot hold up the exit.
    pub async fn stop(mut self, transferred: &Transferred) {
        if let Some(stats) = &self.stats {
            stats.set_next_announce(None);
        }
        if let Some(handle) = self.in_flight.take() {
            handle.abort();
        }
        let counts = transferred.load();
        let events: &[AnnounceEvent] = if self.completion == Completion::Due {
            &[AnnounceEvent::Completed, AnnounceEvent::Stopped]
        } else {
            &[AnnounceEvent::Stopped]
        };
        let send = async {
            for &event in events {
                let request = self.request(event, counts);
                let stats = &self.stats;
                let announce = self
                    .trackers
                    .announce_reporting(&request, |url, result| report(stats, url, result));
                match announce.await {
                    Ok(_) => debug!(?event, "announced"),
                    Err(e) => warn!(?event, "announce failed: {e:#}"),
                }
            }
        };
        if tokio::time::timeout(STOP_TIMEOUT, send).await.is_err() {
            warn!("tracker did not answer the final announce in time");
        }
    }
}

/// Records what the tracker at `url` answered in `stats`, if any.
fn report(stats: &Option<Arc<TorrentStats>>, url: &str, result: &Result<AnnounceResponse>) {
    if let Some(stats) = stats {
        stats.tracker_answered(url, result);
    }
}

#[cfg(test)]
mod tests;
