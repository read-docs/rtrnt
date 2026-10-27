use crate::tracker::announcer::Transferred;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::atomic::Ordering;

/// Bytes verified and written, shown on the progress bar, and bytes moved
/// either way, reported to the tracker.
pub(super) struct Progress {
    pub(super) bar: ProgressBar,
    pub(super) transferred: Transferred,
}

impl Progress {
    /// A bar in `bars` for `total` bytes of the torrent called `name`, of
    /// which `done` were already on disk.
    pub(super) fn new(bars: &MultiProgress, name: &str, total: u64, done: u64) -> Self {
        let bar = bars.add(ProgressBar::new(total));
        let style = ProgressStyle::with_template(
            "{prefix} {bar:40.cyan/blue} {bytes}/{total_bytes} {bytes_per_sec} eta {eta} {msg}",
        );
        if let Ok(style) = style {
            bar.set_style(style);
        }
        bar.set_prefix(bar_label(name));
        // Moving the bar records `done` as a step, and resetting the ETA
        // drops it again, so data from disk does not count towards the rate.
        bar.set_position(done);
        bar.reset_eta();
        Self {
            bar,
            transferred: Transferred::default(),
        }
    }

    /// Counts a verified piece.
    pub(super) fn add(&self, bytes: u64) {
        self.bar.inc(bytes);
        self.transferred
            .downloaded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Counts a block sent to a peer.
    pub(super) fn uploaded(&self, bytes: u64) {
        self.transferred
            .uploaded
            .fetch_add(bytes, Ordering::Relaxed);
    }
}

/// Width of the torrent name in front of each progress bar.
const BAR_LABEL_WIDTH: usize = 20;

/// The torrent name `name` as shown in front of its progress bars: cut short
/// or padded, so the bars of several torrents line up.
pub(super) fn bar_label(name: &str) -> String {
    if name.chars().count() <= BAR_LABEL_WIDTH {
        return format!("{name:<BAR_LABEL_WIDTH$}");
    }
    let mut label: String = name.chars().take(BAR_LABEL_WIDTH - 1).collect();
    label.push('…');
    label
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bar_labels_line_up() {
        assert_eq!(bar_label("a.iso"), format!("{:<20}", "a.iso"));
        let long = bar_label("ubuntu-24.04-desktop-amd64.iso");
        assert_eq!(long, "ubuntu-24.04-deskto…");
        assert_eq!(bar_label("żółw-ąęść-łódź-źdźbło-x").chars().count(), 20);
    }
}
