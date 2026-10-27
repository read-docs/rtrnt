//! What the UI shows, copied out of the session's stats each tick: plain
//! data, no locks, so views can be drawn (and tested) from it alone.

use super::log_buffer::{LogBuffer, LogLine};
use crate::stats::{
    Layout, PeerFlags, PeerSource, PieceSnapshot, SessionStats, TorrentState, TorrentStats,
    TrackerStatus,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// How long a notice shows (see `SessionStats::notice`).
pub const NOTICE_SHOWN: Duration = Duration::from_secs(5);

/// Everything one frame shows.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    /// The time of day, as shown in the header.
    pub clock: String,
    pub session: SessionSnapshot,
    pub torrents: Vec<TorrentSnapshot>,
    /// Peers, pieces and trackers of the selected torrent.
    pub detail: Option<Detail>,
    pub logs: Vec<LogLine>,
    /// The latest notice, while it is fresh.
    pub notice: Option<String>,
    /// Set once every torrent's task has ended.
    pub finished: bool,
}

#[derive(Debug, Clone, Default)]
pub struct SessionSnapshot {
    pub dht_nodes: Option<usize>,
    pub port: Option<u16>,
    pub connections: usize,
    pub max_connections: usize,
    /// Download and upload limits in KiB/s, 0 for none.
    pub rate_limits: (u64, u64),
}

#[derive(Debug, Clone)]
pub struct TorrentSnapshot {
    pub info_hash: [u8; 20],
    pub name: String,
    pub state: TorrentState,
    pub layout: Option<Layout>,
    pub save_path: PathBuf,
    /// Bytes verified and on disk.
    pub verified: u64,
    /// Block data received this session, verified or not.
    pub received: u64,
    /// Verified piece data received this session.
    pub downloaded: u64,
    pub uploaded: u64,
    pub peers: usize,
    /// Peers known, connected or not.
    pub pool: usize,
    /// How long the torrent has been running, or ran.
    pub active: Duration,
    /// Order in which the torrent was added.
    pub order: usize,
    /// Filled in from the rate history (see `app::Rates`).
    pub rate_down: f64,
    pub rate_up: f64,
    pub eta: Option<Duration>,
}

impl TorrentSnapshot {
    /// Fraction done: of the checking while checking, of the metadata while
    /// fetching it, of the data otherwise.
    pub fn progress(&self) -> f64 {
        let fraction = |done: u64, total: u64| {
            if total == 0 {
                0.0
            } else {
                (done as f64 / total as f64).clamp(0.0, 1.0)
            }
        };
        match &self.state {
            TorrentState::Checking { done, total } => fraction(*done, *total),
            TorrentState::FetchingMetadata { pieces, total, .. } => {
                fraction(*pieces as u64, *total as u64)
            }
            _ => match &self.layout {
                Some(layout) => fraction(self.verified, layout.size),
                None => 0.0,
            },
        }
    }

    /// Bytes still to download, once the size is known.
    pub fn left(&self) -> Option<u64> {
        self.layout
            .as_ref()
            .map(|layout| layout.size.saturating_sub(self.verified))
    }
}

#[derive(Debug, Clone)]
pub struct PeerSnapshot {
    pub addr: SocketAddr,
    pub inbound: bool,
    pub client: Option<String>,
    pub flags: PeerFlags,
    pub downloaded: u64,
    pub uploaded: u64,
    /// Pieces the peer has.
    pub pieces: u32,
    /// Filled in from the rate history.
    pub rate_down: f64,
    pub rate_up: f64,
}

/// The selected torrent's insides.
#[derive(Debug, Clone, Default)]
pub struct Detail {
    pub info_hash: [u8; 20],
    pub peers: Vec<PeerSnapshot>,
    pub pieces: Option<PieceSnapshot>,
    pub trackers: Vec<TrackerStatus>,
    /// Time until the trackers are next announced to.
    pub next_announce: Option<Duration>,
    /// Peers learned of from the trackers, the DHT, peer exchange, and
    /// peers that connected to us.
    pub sources: Sources,
    /// Download and upload rates, a sample per tick, oldest first. Filled
    /// in from the rate history.
    pub down_history: Vec<f64>,
    pub up_history: Vec<f64>,
    /// The same for the peer under the Peers tab's cursor, by its address.
    pub peer_history: Option<(SocketAddr, Vec<f64>, Vec<f64>)>,
}

impl Detail {
    /// The peers as the Peers tab lists them: fastest first.
    pub fn sorted_peers(&self) -> Vec<&PeerSnapshot> {
        let mut peers: Vec<&PeerSnapshot> = self.peers.iter().collect();
        peers.sort_by(|a, b| {
            b.rate_down
                .total_cmp(&a.rate_down)
                .then(b.rate_up.total_cmp(&a.rate_up))
                .then(a.addr.cmp(&b.addr))
        });
        peers
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Sources {
    pub tracker: u64,
    pub dht: u64,
    pub pex: u64,
    pub incoming: u64,
}

impl Snapshot {
    /// Copies out the state of `stats` and the lines in `logs`, with the
    /// insides of the torrent with info hash `selected`, if any.
    pub fn take(
        stats: &SessionStats,
        logs: &LogBuffer,
        selected: Option<[u8; 20]>,
        finished: bool,
    ) -> Self {
        let now = Instant::now();
        let torrents = stats.torrents();
        let detail = selected.and_then(|hash| {
            torrents
                .iter()
                .find(|torrent| torrent.info_hash() == hash)
                .map(|torrent| detail(torrent, now))
        });
        Self {
            clock: chrono::Local::now().format("%H:%M").to_string(),
            session: SessionSnapshot {
                dht_nodes: stats.dht_nodes(),
                port: stats.port(),
                connections: stats.connections(),
                max_connections: stats.max_connections(),
                rate_limits: stats.rate_limits(),
            },
            torrents: torrents
                .iter()
                .enumerate()
                .map(|(order, torrent)| torrent_snapshot(torrent, order, now))
                .collect(),
            detail,
            logs: logs.lines(),
            notice: stats
                .last_notice()
                .filter(|(at, _)| now.saturating_duration_since(*at) < NOTICE_SHOWN)
                .map(|(_, text)| text),
            finished,
        }
    }

    pub fn torrent(&self, info_hash: [u8; 20]) -> Option<&TorrentSnapshot> {
        self.torrents
            .iter()
            .find(|torrent| torrent.info_hash == info_hash)
    }

    /// Download and upload rates of the whole session.
    pub fn rates(&self) -> (f64, f64) {
        self.torrents
            .iter()
            .fold((0.0, 0.0), |(down, up), torrent| {
                (down + torrent.rate_down, up + torrent.rate_up)
            })
    }
}

fn torrent_snapshot(torrent: &TorrentStats, order: usize, now: Instant) -> TorrentSnapshot {
    let end = torrent.finished_at().unwrap_or(now);
    TorrentSnapshot {
        info_hash: torrent.info_hash(),
        name: torrent.name(),
        state: torrent.state(),
        layout: torrent.layout().cloned(),
        save_path: torrent.save_path().to_path_buf(),
        verified: torrent.verified(),
        received: torrent.received(),
        downloaded: torrent.downloaded(),
        uploaded: torrent.uploaded(),
        peers: torrent.peers().len(),
        pool: torrent.pool(),
        active: end.saturating_duration_since(torrent.added()),
        order,
        rate_down: 0.0,
        rate_up: 0.0,
        eta: None,
    }
}

fn detail(torrent: &TorrentStats, now: Instant) -> Detail {
    let peers = torrent
        .peers()
        .iter()
        .map(|peer| PeerSnapshot {
            addr: peer.addr,
            inbound: peer.inbound,
            client: peer.client().map(str::to_string),
            flags: peer.flags(),
            downloaded: peer.downloaded(),
            uploaded: peer.uploaded(),
            pieces: peer.pieces(),
            rate_down: 0.0,
            rate_up: 0.0,
        })
        .collect();
    Detail {
        info_hash: torrent.info_hash(),
        peers,
        pieces: torrent.pieces(),
        trackers: torrent.trackers(),
        next_announce: torrent
            .next_announce()
            .map(|at| at.saturating_duration_since(now)),
        sources: Sources {
            tracker: torrent.source(PeerSource::Tracker),
            dht: torrent.source(PeerSource::Dht),
            pex: torrent.source(PeerSource::Pex),
            incoming: torrent.source(PeerSource::Incoming),
        },
        down_history: Vec::new(),
        up_history: Vec::new(),
        peer_history: None,
    }
}

/// Bytes of each file in `layout` covered by the pieces marked in `have`. A
/// piece that spans two files counts for both, by the bytes it holds of
/// each.
pub fn file_progress(layout: &Layout, have: &[bool]) -> Vec<u64> {
    let piece_length = layout.piece_length.max(1);
    let mut offset = 0;
    layout
        .files
        .iter()
        .map(|(_, length)| {
            let (start, end) = (offset, offset + length);
            offset = end;
            if start == end {
                return 0;
            }
            let first = start / piece_length;
            let last = (end - 1) / piece_length;
            (first..=last)
                .filter(|&piece| have.get(piece as usize).copied().unwrap_or(false))
                .map(|piece| {
                    let piece_start = piece * piece_length;
                    let piece_end = piece_start + piece_length;
                    piece_end.min(end) - piece_start.max(start)
                })
                .sum()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(files: &[u64], piece_length: u64) -> Layout {
        let size = files.iter().sum();
        Layout {
            size,
            piece_length,
            pieces: size.div_ceil(piece_length) as usize,
            private: false,
            files: files
                .iter()
                .enumerate()
                .map(|(i, &length)| (PathBuf::from(format!("f{i}")), length))
                .collect(),
        }
    }

    #[test]
    fn pieces_count_for_each_file_they_touch() {
        // Pieces of 10: [0,10) [10,20) [20,30) [30,35).
        // Files: [0,15) [15,15) [15,35).
        let layout = layout(&[15, 0, 20], 10);
        assert_eq!(
            file_progress(&layout, &[true, false, false, false]),
            [10, 0, 0]
        );
        assert_eq!(
            file_progress(&layout, &[false, true, false, false]),
            [5, 0, 5]
        );
        assert_eq!(
            file_progress(&layout, &[false, false, true, true]),
            [0, 0, 15]
        );
        assert_eq!(file_progress(&layout, &[true; 4]), [15, 0, 20]);
    }

    #[test]
    fn progress_follows_the_state() {
        let mut torrent = TorrentSnapshot {
            info_hash: [0; 20],
            name: "t".into(),
            state: TorrentState::FetchingMetadata {
                pieces: 1,
                total: 4,
                peers: 2,
            },
            layout: None,
            save_path: PathBuf::new(),
            verified: 0,
            received: 0,
            downloaded: 0,
            uploaded: 0,
            peers: 0,
            pool: 0,
            active: Duration::ZERO,
            order: 0,
            rate_down: 0.0,
            rate_up: 0.0,
            eta: None,
        };
        assert_eq!(torrent.progress(), 0.25);
        assert_eq!(torrent.left(), None);
        torrent.layout = Some(layout(&[100], 10));
        torrent.state = TorrentState::Checking {
            done: 50,
            total: 200,
        };
        assert_eq!(torrent.progress(), 0.25);
        torrent.state = TorrentState::Downloading;
        torrent.verified = 30;
        assert_eq!(torrent.progress(), 0.3);
        assert_eq!(torrent.left(), Some(70));
    }
}
