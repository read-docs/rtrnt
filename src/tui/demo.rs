//! A made-up session, the one in the mockups (`docs/tui`), for working on
//! the UI without a swarm (`cargo run --example tui_demo`) and for the view
//! tests.

use super::log_buffer::LogLine;
use super::snapshot::{
    Detail, NOTICE_SHOWN, PeerSnapshot, SessionSnapshot, Snapshot, Sources, TorrentSnapshot,
};
use super::{Command, Feed};
use crate::stats::{Layout, PeerFlags, PieceSnapshot, TorrentState, TrackerResult, TrackerStatus};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tracing::Level;

const KIB: f64 = 1024.0;
const MIB: f64 = 1024.0 * 1024.0;
const PIECE: u64 = 256 * 1024;

/// One made-up torrent: what it starts as, and how fast it moves.
struct Torrent {
    name: &'static str,
    size: u64,
    files: &'static [(&'static str, u64)],
    state: TorrentState,
    /// Fraction on disk at the start.
    done: f64,
    down: f64,
    up: f64,
    peers: usize,
}

fn torrents() -> Vec<Torrent> {
    let gib = |g: f64| (g * 1024.0 * MIB) as u64;
    vec![
        Torrent {
            name: "debian-13.7.0-amd64-netinst.iso",
            size: 754 * MIB as u64,
            files: &[("debian-13.7.0-amd64-netinst.iso", 754 << 20)],
            state: TorrentState::Downloading,
            done: 0.62,
            down: 4.2 * MIB,
            up: 310.0 * KIB,
            peers: 38,
        },
        Torrent {
            name: "archlinux-2026.10.01-x86_64.iso",
            size: gib(1.2),
            files: &[],
            state: TorrentState::Seeding,
            done: 1.0,
            down: 0.0,
            up: 612.0 * KIB,
            peers: 12,
        },
        Torrent {
            name: "ubuntu-26.04-desktop-amd64.iso",
            size: gib(6.1),
            files: &[],
            state: TorrentState::Downloading,
            done: 0.12,
            down: 2.1 * MIB,
            up: 96.0 * KIB,
            peers: 21,
        },
        Torrent {
            name: "Big Buck Bunny",
            size: 276_134_947,
            files: &[
                ("Big Buck Bunny/Big Buck Bunny.en.srt", 140),
                (
                    "Big Buck Bunny/Big Buck Bunny.mp4",
                    276_134_947 - 140 - 310_380,
                ),
                ("Big Buck Bunny/poster.jpg", 310_380),
            ],
            state: TorrentState::FetchingMetadata {
                pieces: 3,
                total: 7,
                peers: 4,
            },
            done: 0.0,
            down: 1.5 * MIB,
            up: 0.0,
            peers: 4,
        },
        Torrent {
            name: "fedora-Workstation-Live-43.iso",
            size: gib(2.4),
            files: &[],
            state: TorrentState::Checking { done: 0, total: 0 },
            done: 0.37,
            down: 0.0,
            up: 0.0,
            peers: 0,
        },
    ]
}

/// Address, client, flags (`D`/`d`, `U`/`u`, `I`, `F`, `X`), rates in
/// KiB/s, and the fraction of pieces had: the mockup's peers.
const PEERS: &[(&str, &str, &str, f64, f64, f64)] = &[
    (
        "185.21.216.143:51413",
        "Transmission 4.0.6",
        "DUFX",
        1126.0,
        48.0,
        1.0,
    ),
    (
        "[2a01:4f8:c17::9]:6881",
        "qBittorrent 5.1.2",
        "DuFX",
        812.0,
        0.0,
        1.0,
    ),
    (
        "91.121.44.7:6881",
        "libtorrent 2.0.11",
        "DUF",
        640.0,
        112.0,
        0.83,
    ),
    (
        "73.92.10.211:42069",
        "Deluge 2.2.0",
        "dUIF",
        0.0,
        96.0,
        0.41,
    ),
    (
        "46.4.88.120:51413",
        "Transmission 4.1.0",
        "DuFX",
        511.0,
        0.0,
        1.0,
    ),
    ("5.39.71.22:6881", "rtrnt 0.1.0", "DUF", 402.0, 54.0, 0.66),
    ("178.62.3.98:55555", "BitComet 2.10", "D", 298.0, 0.0, 1.0),
    (
        "82.64.211.5:6881",
        "qBittorrent 4.6.7",
        "dF",
        0.0,
        0.0,
        0.12,
    ),
    (
        "[2001:db8:5::1]:51413",
        "Transmission 3.00",
        "DUFX",
        201.0,
        0.0,
        1.0,
    ),
];

fn flags(letters: &str) -> PeerFlags {
    let has = |c| letters.contains(c);
    let interested = has('D') || has('d');
    PeerFlags::new(
        !has('U'),
        !has('D'),
        interested,
        has('U') || has('u'),
        has('F'),
        has('X'),
    )
}

const LOGS: &[(u64, Level, Option<&str>, &str)] = &[
    (
        40,
        Level::INFO,
        Some("fedora-Workstation-Live-43.iso"),
        "checking data on disk",
    ),
    (
        51,
        Level::INFO,
        Some("session"),
        "DHT lookup found 23 peers",
    ),
    (
        58,
        Level::INFO,
        Some("Big Buck Bunny"),
        "metadata 3/7 pieces from 4 peers",
    ),
    (
        64,
        Level::WARN,
        Some("debian-13.7.0-amd64-netinst.iso"),
        "banning peer: too many corrupt pieces addr=203.0.113.9:6881",
    ),
    (
        69,
        Level::INFO,
        Some("archlinux-2026.10.01-x86_64.iso"),
        "re-announced peers=50 event=Regular",
    ),
    (
        71,
        Level::INFO,
        Some("debian-13.7.0-amd64-netinst.iso"),
        "tracker announce succeeded peers=50",
    ),
];

/// A wobble in [-1, 1] that drifts with `t`.
fn wobble(t: f64, seed: f64) -> f64 {
    ((t * 0.7 + seed).sin() + (t * 1.9 + seed * 3.0).sin() * 0.5) / 1.5
}

/// One torrent as it moves.
struct Live {
    torrent: Torrent,
    state: TorrentState,
    /// What it was doing before it was paused.
    paused: Option<TorrentState>,
    /// Gone from the session.
    removed: bool,
    verified: f64,
    received: f64,
    uploaded: f64,
    have: Vec<bool>,
    availability: Vec<u32>,
    in_flight: Vec<bool>,
}

impl Live {
    fn new(torrent: Torrent, seed: u64) -> Self {
        let pieces = torrent.size.div_ceil(PIECE) as usize;
        let mut rng = StdRng::seed_from_u64(seed);
        // Have runs of pieces, thicker where the sine is high, as in B.
        let mut have = vec![false; pieces];
        let mut run = false;
        for (i, have) in have.iter_mut().enumerate() {
            if rng.random::<f64>() < 0.05 {
                let p = torrent.done + 0.25 * (i as f64 / 90.0).sin() - 0.05;
                run = rng.random::<f64>() < p;
            }
            *have = torrent.done >= 1.0 || (run && rng.random::<f64>() < 0.97);
        }
        let availability = (0..pieces)
            .map(|i| {
                let wave = 8.0 + 6.0 * (i as f64 / 140.0).sin();
                (wave + rng.random_range(-2.0..2.0)).max(0.0) as u32
            })
            .collect();
        let in_flight = have
            .iter()
            .map(|&have| !have && rng.random::<f64>() < 0.02)
            .collect();
        let state = match torrent.state {
            TorrentState::Checking { .. } => TorrentState::Checking {
                done: (torrent.size as f64 * torrent.done) as u64,
                total: torrent.size,
            },
            ref state => state.clone(),
        };
        let verified = match state {
            TorrentState::Checking { .. } => 0.0,
            _ => torrent.size as f64 * torrent.done,
        };
        Self {
            state,
            paused: None,
            removed: false,
            verified,
            received: 0.0,
            uploaded: 0.0,
            have,
            availability,
            in_flight,
            torrent,
        }
    }

    fn rates(&self, t: f64, seed: f64) -> (f64, f64) {
        let down = match self.state {
            TorrentState::Downloading => self.torrent.down * (1.0 + 0.25 * wobble(t, seed)),
            _ => 0.0,
        };
        let up = match self.state {
            TorrentState::Downloading | TorrentState::Seeding => {
                self.torrent.up * (1.0 + 0.3 * wobble(t, seed + 1.0))
            }
            _ => 0.0,
        };
        (down, up)
    }

    /// Moves the torrent on by `dt` seconds, at time `t`.
    fn advance(&mut self, t: f64, dt: f64, seed: f64, rng: &mut StdRng) {
        let (down, up) = self.rates(t, seed);
        let size = self.torrent.size as f64;
        self.received += down * dt;
        self.uploaded += up * dt;
        match &mut self.state {
            TorrentState::Checking { done, total } => {
                *done = (*done + (300.0 * MIB * dt) as u64).min(*total);
                if done == total {
                    self.verified = size * self.torrent.done;
                    self.state = TorrentState::Downloading;
                    self.torrent.down = 3.0 * MIB;
                    self.torrent.up = 40.0 * KIB;
                }
            }
            TorrentState::FetchingMetadata { pieces, total, .. } => {
                if rng.random::<f64>() < dt / 2.0 {
                    *pieces += 1;
                }
                if pieces >= total {
                    self.state = TorrentState::Downloading;
                }
            }
            TorrentState::Downloading => {
                self.verified = (self.verified + down * dt).min(size);
                let want = (self.verified / size * self.have.len() as f64) as usize;
                let mut had = self.have.iter().filter(|&&have| have).count();
                while had < want {
                    let index = rng.random_range(0..self.have.len());
                    if !self.have[index] {
                        self.have[index] = true;
                        self.in_flight[index] = false;
                        had += 1;
                    }
                }
                if self.verified >= size {
                    self.state = TorrentState::Seeding;
                }
            }
            _ => {}
        }
    }

    fn layout(&self) -> Option<Layout> {
        if matches!(self.state, TorrentState::FetchingMetadata { .. }) {
            return None;
        }
        let files = if self.torrent.files.is_empty() {
            vec![(PathBuf::from(self.torrent.name), self.torrent.size)]
        } else {
            self.torrent
                .files
                .iter()
                .map(|&(path, length)| (PathBuf::from(path), length))
                .collect()
        };
        Some(Layout {
            size: self.torrent.size,
            piece_length: PIECE,
            pieces: self.have.len(),
            private: false,
            files,
        })
    }
}

/// The made-up session, moving on each time it is asked for a snapshot.
pub struct DemoFeed {
    torrents: Vec<Live>,
    start: Instant,
    last: f64,
    rng: StdRng,
    stopped: bool,
    rate_limits: (u64, u64),
    /// The latest notice, and when it came.
    notice: Option<(Instant, String)>,
}

impl Default for DemoFeed {
    fn default() -> Self {
        Self::new()
    }
}

impl DemoFeed {
    pub fn new() -> Self {
        Self {
            torrents: torrents()
                .into_iter()
                .enumerate()
                .map(|(i, torrent)| Live::new(torrent, i as u64))
                .collect(),
            start: Instant::now(),
            last: 0.0,
            rng: StdRng::seed_from_u64(7),
            stopped: false,
            rate_limits: (0, 2048),
            notice: None,
        }
    }

    /// The torrent with info hash `hash`.
    fn live(&mut self, hash: [u8; 20]) -> Option<&mut Live> {
        let index = (0..self.torrents.len()).find(|&i| info_hash(i) == hash)?;
        self.torrents.get_mut(index)
    }

    /// The snapshot at `t` seconds in, with the torrent `selected` open.
    fn snapshot_at(&self, t: f64, selected: Option<[u8; 20]>, finished: bool) -> Snapshot {
        let torrents: Vec<TorrentSnapshot> = self
            .torrents
            .iter()
            .enumerate()
            .filter(|(_, live)| !live.removed)
            .map(|(i, live)| TorrentSnapshot {
                info_hash: info_hash(i),
                name: live.torrent.name.to_string(),
                state: if self.stopped {
                    TorrentState::Interrupted
                } else {
                    live.state.clone()
                },
                layout: live.layout(),
                priorities: Vec::new(),
                wanted: None,
                save_path: PathBuf::from("/home/me/Downloads"),
                verified: live.verified as u64,
                received: live.received as u64,
                downloaded: live.received as u64,
                uploaded: live.uploaded as u64,
                peers: live.torrent.peers,
                pool: live.torrent.peers * 11,
                active: Duration::from_secs_f64(9720.0 + t),
                order: i,
                rate_down: 0.0,
                rate_up: 0.0,
                eta: None,
            })
            .collect();
        let detail = selected.and_then(|hash| {
            let index = (0..self.torrents.len()).find(|&i| info_hash(i) == hash)?;
            (!self.torrents[index].removed).then(|| self.detail(index, t))
        });
        let now = SystemTime::now();
        let logs = LOGS
            .iter()
            .map(|&(secs, level, torrent, message)| LogLine {
                time: now - Duration::from_secs(80 - secs),
                level,
                torrent: torrent.map(str::to_string),
                message: message.to_string(),
            })
            .collect();
        Snapshot {
            clock: chrono::Local::now().format("%H:%M").to_string(),
            session: SessionSnapshot {
                dht_nodes: Some(312),
                port: Some(6881),
                connections: 71,
                max_connections: 200,
                rate_limits: self.rate_limits,
                watch_dir: None,
            },
            torrents,
            detail,
            logs,
            notice: self
                .notice
                .as_ref()
                .filter(|(at, _)| at.elapsed() < NOTICE_SHOWN)
                .map(|(_, text)| text.clone()),
            finished,
        }
    }

    fn detail(&self, index: usize, t: f64) -> Detail {
        let live = &self.torrents[index];
        let pieces = live.have.len() as u32;
        let peers = if index == 0 {
            PEERS
                .iter()
                .enumerate()
                .map(
                    |(i, &(addr, client, letters, down, up, has))| PeerSnapshot {
                        addr: addr.parse::<SocketAddr>().expect("demo address"),
                        inbound: letters.contains('I'),
                        client: Some(client.to_string()),
                        flags: flags(letters),
                        downloaded: (down * KIB * (t + 60.0)) as u64,
                        uploaded: (up * KIB * (t + 60.0)) as u64,
                        pieces: (f64::from(pieces) * has) as u32 + i as u32 % 2,
                        rate_down: down * KIB,
                        rate_up: up * KIB,
                    },
                )
                .map(|mut peer| {
                    peer.pieces = peer.pieces.min(pieces);
                    peer
                })
                .collect()
        } else {
            Vec::new()
        };
        let tracker = |tier, url: &str, result, peers, seeders, leechers| TrackerStatus {
            tier,
            url: url.to_string(),
            result,
            peers,
            seeders,
            leechers,
        };
        Detail {
            info_hash: info_hash(index),
            peers,
            pieces: live.layout().map(|_| PieceSnapshot {
                have: live.have.clone(),
                in_flight: live.in_flight.clone(),
                availability: live.availability.clone(),
            }),
            trackers: vec![
                tracker(
                    0,
                    "udp://tracker.opentrackr.org:1337/announce",
                    TrackerResult::Ok,
                    Some(50),
                    Some(1204),
                    Some(87),
                ),
                tracker(
                    1,
                    "http://bttracker.debian.org:6969/announce",
                    TrackerResult::Ok,
                    Some(50),
                    Some(980),
                    Some(41),
                ),
                tracker(
                    1,
                    "udp://tracker.example.org:6969/announce",
                    TrackerResult::Error("connection timed out".into()),
                    None,
                    None,
                    None,
                ),
                tracker(
                    2,
                    "https://tracker.example.net/announce",
                    TrackerResult::NotContacted,
                    None,
                    None,
                    None,
                ),
            ],
            next_announce: Some(Duration::from_secs_f64((1440.0 - t).max(0.0))),
            sources: Sources {
                tracker: 212,
                dht: 118,
                pex: 70,
                incoming: 12,
            },
            down_history: Vec::new(),
            up_history: Vec::new(),
            peer_history: None,
        }
    }
}

/// The made-up info hash of the `index`th torrent.
fn info_hash(index: usize) -> [u8; 20] {
    [index as u8 + 1; 20]
}

impl Feed for DemoFeed {
    fn snapshot(&mut self, selected: Option<[u8; 20]>, finished: bool) -> Snapshot {
        let t = self.start.elapsed().as_secs_f64();
        let dt = t - self.last;
        self.last = t;
        if !self.stopped {
            for (i, live) in self.torrents.iter_mut().enumerate() {
                live.advance(t, dt, i as f64, &mut self.rng);
            }
        }
        self.snapshot_at(t, selected, finished || self.stopped)
    }

    fn command(&mut self, command: Command) -> bool {
        match command {
            Command::Stop => self.stopped = true,
            Command::SetRateLimits(down, up) => self.rate_limits = (down, up),
            Command::Pause(hash) => {
                if let Some(live) = self.live(hash)
                    && !live.state.is_finished()
                {
                    live.paused = Some(std::mem::replace(&mut live.state, TorrentState::Paused));
                }
            }
            Command::Resume(hash) => {
                if let Some(live) = self.live(hash)
                    && let Some(state) = live.paused.take()
                {
                    live.state = state;
                }
            }
            Command::Recheck(_) => {
                self.notice = Some((Instant::now(), "the demo checks nothing".into()));
            }
            // Nothing is on disk to delete.
            Command::Remove { info_hash, .. } => {
                if let Some(live) = self.live(info_hash) {
                    live.removed = true;
                }
            }
            Command::Add(source) => {
                let text = format!("cannot add {}: the demo adds nothing", source.label());
                self.notice = Some((Instant::now(), text));
            }
        }
        true
    }
}

/// The mockup's session as one fixed snapshot, rates and history included,
/// with the first torrent selected: what the view tests draw.
pub fn fixture() -> Snapshot {
    let feed = DemoFeed::new();
    let mut snapshot = feed.snapshot_at(0.0, Some(info_hash(0)), false);
    snapshot.clock = "14:02".to_string();
    for (live, torrent) in feed.torrents.iter().zip(&mut snapshot.torrents) {
        (torrent.rate_down, torrent.rate_up) = live.rates(0.0, torrent.order as f64);
        torrent.eta = match (&torrent.state, torrent.left()) {
            (TorrentState::Downloading, Some(left)) if torrent.rate_down > 0.0 => {
                Some(Duration::from_secs_f64(left as f64 / torrent.rate_down))
            }
            _ => None,
        };
    }
    if let Some(detail) = &mut snapshot.detail {
        detail.down_history = (0..120)
            .map(|i| 4.2 * MIB * (1.0 + 0.25 * wobble(f64::from(i) / 4.0, 0.0)))
            .collect();
        detail.up_history = (0..120)
            .map(|i| 310.0 * KIB * (1.0 + 0.3 * wobble(f64::from(i) / 4.0, 1.0)))
            .collect();
    }
    snapshot
}
