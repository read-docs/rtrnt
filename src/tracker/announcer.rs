//! Keeps the tracker informed for the whole run: re-announces on the
//! tracker's interval with real stats, sends `completed` when the download
//! finishes, and `stopped` at the end.

use super::{AnnounceEvent, AnnounceRequest, AnnounceResponse, Tiers};
use crate::periodic::Periodic;
use crate::stats::TorrentStats;
use anyhow::{Result, bail};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// Used when the tracker does not say how often to announce.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Bounds on the time between announces, whatever the tracker asks for.
const MIN_INTERVAL: Duration = Duration::from_secs(60);
const MAX_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
/// How long the final `completed` and `stopped` announces may take in total.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long to wait before announcing `started` again when no tracker
/// answered it. It doubles with each failure, up to the interval.
const START_RETRY: Duration = Duration::from_secs(30);

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
    /// When announces are sent. Each runs in the background, of the given
    /// event, on a copy of the tiers that it hands back, reordered to put
    /// the tracker that answered first.
    timer: Periodic<(Tiers, AnnounceEvent, Result<AnnounceResponse>)>,
    /// Whether a tracker has answered `started`. Until one has, that is the
    /// announce sent, and retried.
    answered: bool,
    /// Announces of `started` in a row that no tracker answered.
    failures: u32,
    completion: Completion,
    /// Told what each tracker answered, and when the next announce is due.
    stats: Option<Arc<TorrentStats>>,
}

impl Announcer {
    /// Sends `request` as the `started` announce to the tracker `tiers`.
    /// Returns the announcer and the peers the tracker sent back, which may
    /// be none. Fails if no tracker answered.
    pub async fn start(
        tiers: Vec<Vec<String>>,
        request: AnnounceRequest,
    ) -> Result<(Self, Vec<SocketAddr>)> {
        let (announcer, peers) = Self::start_reporting(tiers, request, None).await;
        if !announcer.has_answered() {
            bail!("no tracker answered the started announce");
        }
        Ok((announcer, peers))
    }

    /// `start`, recording in `stats` what each tracker answers, now and on
    /// every announce after. Returns the announcer even when no tracker
    /// answered, with the peers none: `poll` sends `started` again, after a
    /// while.
    pub async fn start_reporting(
        tiers: Vec<Vec<String>>,
        request: AnnounceRequest,
        stats: Option<Arc<TorrentStats>>,
    ) -> (Self, Vec<SocketAddr>) {
        if let Some(stats) = &stats {
            stats.set_trackers(&tiers);
        }
        let mut announcer = Self::new(Tiers::new(tiers), request, Instant::now());
        announcer.stats = stats;
        let started = announcer.started;
        let stats = announcer.stats.clone();
        let result = announcer
            .trackers
            .announce_reporting(&started, |url, result| report(&stats, url, result))
            .await;
        let peers = announcer.announced(AnnounceEvent::Started, result);
        (announcer, peers)
    }

    /// Whether a tracker has answered the `started` announce.
    pub fn has_answered(&self) -> bool {
        self.answered
    }

    fn new(trackers: Tiers, request: AnnounceRequest, now: Instant) -> Self {
        Self {
            trackers,
            started: AnnounceRequest {
                event: AnnounceEvent::Started,
                key: rand::random(),
                ..request
            },
            interval: DEFAULT_INTERVAL,
            min_interval: MIN_INTERVAL,
            timer: Periodic::new(now, now + DEFAULT_INTERVAL),
            answered: false,
            failures: 0,
            completion: Completion::Downloading,
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
        self.reschedule();
        debug!(interval = ?self.interval, "next announce scheduled");
    }

    /// Plans the next announce after the last one was sent: on the interval,
    /// or sooner while `started` keeps failing, or while `completed` is
    /// still to be sent, as soon as the tracker allows.
    fn reschedule(&mut self) {
        let wait = match self.completion {
            _ if self.failures > 0 => {
                let doubled = START_RETRY.saturating_mul(1 << (self.failures - 1).min(16));
                doubled.min(self.interval)
            }
            Completion::Due => self.min_interval,
            _ => self.interval,
        };
        self.timer.next = self.timer.last + wait;
        self.publish_next();
    }

    /// Tells the stats when the next announce is due.
    fn publish_next(&self) {
        if let Some(stats) = &self.stats {
            stats.set_next_announce(Some(self.timer.next.into_std()));
        }
    }

    /// The request for `event`, reporting the bytes `downloaded` and
    /// `uploaded` since `started`.
    fn request(&self, event: AnnounceEvent, counts: (u64, u64)) -> AnnounceRequest {
        Self::request_from(&self.started, event, counts)
    }

    /// `request`, from the `started` request.
    fn request_from(
        started: &AnnounceRequest,
        event: AnnounceEvent,
        (downloaded, uploaded): (u64, u64),
    ) -> AnnounceRequest {
        AnnounceRequest {
            event,
            downloaded,
            uploaded,
            left: started.left.saturating_sub(downloaded),
            ..*started
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
            self.timer.next = Instant::now();
            self.publish_next();
        }
    }

    /// Brings the next announce forward to the earliest the tracker allows,
    /// for when we have run out of peers.
    pub fn hurry(&mut self) {
        self.timer.hurry(self.min_interval);
        self.publish_next();
    }

    /// Waits until the next announce is due, sends it in the background
    /// with the current `transferred` counts, and returns the peers it got
    /// (none if every tracker failed). It is `started` if no tracker has
    /// answered that yet, else `completed` if the download has finished since
    /// the last one, otherwise a regular announce.
    ///
    /// Cancel-safe: an announce already sent is kept, and the next call
    /// waits for its answer.
    pub async fn poll(&mut self, transferred: &Transferred) -> Vec<SocketAddr> {
        let event = match self.completion {
            _ if !self.answered => AnnounceEvent::Started,
            Completion::Due => AnnounceEvent::Completed,
            _ => AnnounceEvent::Regular,
        };
        let (started, tiers, stats) = (&self.started, &self.trackers, &self.stats);
        let send = || {
            let request = Self::request_from(started, event, transferred.load());
            let (mut trackers, stats) = (tiers.clone(), stats.clone());
            async move {
                let result = trackers
                    .announce_reporting(&request, |url, result| report(&stats, url, result))
                    .await;
                (trackers, event, result)
            }
        };
        let (event, result) = match self.timer.poll(None, send).await {
            Ok((trackers, event, result)) => {
                self.trackers = trackers;
                (event, result)
            }
            Err(e) => (AnnounceEvent::Regular, Err(e.into())),
        };
        self.announced(event, result)
    }

    /// Takes the `result` of the announce of `event`, returning its peers.
    fn announced(
        &mut self,
        event: AnnounceEvent,
        result: Result<AnnounceResponse>,
    ) -> Vec<SocketAddr> {
        match result {
            Ok(response) => {
                match event {
                    AnnounceEvent::Started => {
                        self.answered = true;
                        self.failures = 0;
                    }
                    AnnounceEvent::Completed => self.completion = Completion::Sent,
                    _ => {}
                }
                info!(peers = response.peers.len(), ?event, "announced");
                self.schedule(&response);
                response.peers
            }
            Err(e) => {
                if !self.answered {
                    self.failures += 1;
                }
                warn!(?event, "announce failed: {e:#}");
                self.reschedule();
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
        self.timer.cancel();
        let counts = transferred.load();
        let events: &[AnnounceEvent] = if !self.answered {
            // No tracker knows of us.
            &[]
        } else if self.completion == Completion::Due {
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
