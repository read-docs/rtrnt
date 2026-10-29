//! What a torrent overrides of the session's settings: its own download and
//! upload limits, nested inside the session's (see `rate_limit`), and its own
//! seed ratio and seed time.
//!
//! The settings live in a `TorrentLimits` shared by the manager, which
//! changes them on the UI's word, and the torrent's download, which reads
//! them as it runs, so a change applies without restarting it. They belong
//! to the torrent, not to a run of it: they outlast a pause, and are saved
//! in the session file.

use crate::download::SeedLimits;
use crate::rate_limit::RateLimits;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;

/// A torrent's own settings; each is "as the session says" unless set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TorrentSettings {
    /// Most block data to download per second, in KiB, across the torrent's
    /// peers; 0 for no limit of its own (the session's still applies).
    pub download: u64,
    /// The same for upload.
    pub upload: u64,
    /// Seed until we uploaded this many thousandths of the torrent's size
    /// (so 1500 is a ratio of 1.5), in place of the session's `seed-ratio`;
    /// `Some(0)` for no ratio limit. `None` to go by the session.
    pub seed_ratio_milli: Option<u32>,
    /// Seed this many minutes, in place of the session's `seed-time`;
    /// `Some(0)` for no time limit. `None` to go by the session.
    pub seed_time: Option<u64>,
}

impl TorrentSettings {
    /// Whether nothing is set.
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }

    /// The settings in words, for a notice: what is set, or that nothing is.
    pub fn describe(&self) -> String {
        if self.is_default() {
            return "limits and seed settings as the session's".to_string();
        }
        let rate = |kib: u64| match kib {
            0 => "session's".to_string(),
            kib => format!("{kib} KiB/s"),
        };
        let ratio = self
            .seed_ratio_milli
            .map_or("session's".to_string(), format_ratio);
        let time = self
            .seed_time
            .map_or("session's".to_string(), |minutes| format!("{minutes} min"));
        format!(
            "download {}, upload {}, seed ratio {ratio}, seed time {time}",
            rate(self.download),
            rate(self.upload)
        )
    }

    /// `global` with this torrent's seed ratio and seed time put in place
    /// of the session's, where they are set.
    pub fn resolve_seed(&self, global: SeedLimits) -> SeedLimits {
        SeedLimits {
            ratio: self
                .seed_ratio_milli
                .map_or(global.ratio, |milli| f64::from(milli) / 1000.0),
            time: self.seed_time.map_or(global.time, |minutes| {
                Duration::from_secs(minutes.saturating_mul(60))
            }),
        }
    }
}

/// A seed ratio in thousandths as it is shown: "none" for 0, else up to
/// three decimals with the trailing zeros left off.
pub fn format_ratio(milli: u32) -> String {
    if milli == 0 {
        return "none".to_string();
    }
    let text = format!("{}.{:03}", milli / 1000, milli % 1000);
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// A torrent's settings, live: the rate limits its peers hold allowances in,
/// and the seed settings its download checks.
#[derive(Debug, Default)]
pub struct TorrentLimits {
    rates: RateLimits,
    settings: Mutex<TorrentSettings>,
    /// Notified whenever the seed ratio or time changes.
    seed_changed: Notify,
}

impl TorrentLimits {
    /// Limits with `settings`.
    pub fn new(settings: TorrentSettings) -> Self {
        let limits = Self::default();
        limits.set(settings);
        limits
    }

    /// Changes the settings, at once for the torrent's peers.
    pub fn set(&self, settings: TorrentSettings) {
        let old = std::mem::replace(
            &mut *self.settings.lock().unwrap_or_else(PoisonError::into_inner),
            settings,
        );
        if old.download != settings.download || old.upload != settings.upload {
            self.rates.set(settings.download, settings.upload);
        }
        if old.seed_ratio_milli != settings.seed_ratio_milli || old.seed_time != settings.seed_time
        {
            self.seed_changed.notify_waiters();
        }
    }

    /// The settings as they are now.
    pub fn settings(&self) -> TorrentSettings {
        *self.settings.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The torrent's own rate limits, to nest inside the session's.
    pub fn rates(&self) -> &RateLimits {
        &self.rates
    }

    /// The seed limits that apply, given the session's.
    pub fn seed(&self, global: SeedLimits) -> SeedLimits {
        self.settings().resolve_seed(global)
    }

    /// Resolves once the seed ratio or time changes after this call.
    pub fn seed_changed(&self) -> Notified<'_> {
        self.seed_changed.notified()
    }
}

#[cfg(test)]
mod tests;
