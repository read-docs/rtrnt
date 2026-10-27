use crate::download::Interrupted;
use crate::magnet::FetchInterrupted;
use crate::torrent::TorrentFile;
use crate::tracker::AnnounceResponse;
use crate::tracker::announcer::Transferred;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Instant;

use super::{PeerStats, lock};

/// Where a torrent is in its life.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TorrentState {
    /// Fetching the metadata of a magnet link: `pieces` of `total` metadata
    /// pieces in (`total` is 0 until a peer says), from `peers` peers.
    FetchingMetadata {
        pieces: usize,
        total: usize,
        peers: usize,
    },
    /// Waiting for another torrent to finish checking its data.
    QueuedForChecking,
    /// Hashing the data on disk: `done` of `total` bytes.
    Checking {
        done: u64,
        total: u64,
    },
    Downloading,
    Seeding,
    Done,
    Interrupted,
    Failed(String),
}

impl TorrentState {
    /// Whether the torrent's task has ended.
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            TorrentState::Done | TorrentState::Interrupted | TorrentState::Failed(_)
        )
    }
}

/// The shape of a torrent, known once its metadata is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    pub size: u64,
    pub piece_length: u64,
    pub pieces: usize,
    pub private: bool,
    /// Each file's path within the torrent, and its length, in order.
    pub files: Vec<(PathBuf, u64)>,
}

impl Layout {
    pub fn of(torrent: &TorrentFile) -> Self {
        let info = &torrent.info;
        let files = match &info.files {
            Some(files) => files
                .iter()
                .map(|file| (file.path.iter().collect(), file.length))
                .collect(),
            None => vec![(PathBuf::from(&info.name), torrent.length())],
        };
        Self {
            size: torrent.length(),
            piece_length: info.piece_length,
            pieces: torrent.num_pieces(),
            private: torrent.is_private(),
            files,
        }
    }
}

/// Which pieces we have, are downloading, and how many connected peers have
/// each.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PieceSnapshot {
    pub have: Vec<bool>,
    pub in_flight: Vec<bool>,
    pub availability: Vec<u32>,
}

/// Something that can tell which pieces a running download has.
pub(crate) trait PieceSource: Send + Sync {
    fn piece_snapshot(&self) -> PieceSnapshot;
}

enum Pieces {
    Unknown,
    Live(Weak<dyn PieceSource>),
    /// What the download had when it ended.
    Final(PieceSnapshot),
}

/// What a tracker last said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerResult {
    NotContacted,
    Ok,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackerStatus {
    /// The tier (BEP 12), counting from 0.
    pub tier: usize,
    pub url: String,
    pub result: TrackerResult,
    /// Peers the last successful announce returned.
    pub peers: Option<usize>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
}

/// Where the peers we heard of came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSource {
    Tracker,
    Dht,
    Pex,
    Incoming,
}

/// One torrent of the session.
pub struct TorrentStats {
    info_hash: [u8; 20],
    save_path: PathBuf,
    added: Instant,
    name: Mutex<String>,
    layout: OnceLock<Layout>,
    state: Mutex<TorrentState>,
    /// When the task ended.
    finished: OnceLock<Instant>,
    transferred: Arc<Transferred>,
    /// Block data received, verified or not: what the download rate is
    /// made of, as verified pieces come in bursts.
    received: AtomicU64,
    /// Bytes verified and on disk, from earlier runs too.
    verified: AtomicU64,
    /// Peers known to the download, connected or not.
    pool: AtomicUsize,
    pieces: Mutex<Pieces>,
    peers: Mutex<Vec<Weak<PeerStats>>>,
    trackers: Mutex<Vec<TrackerStatus>>,
    next_announce: Mutex<Option<Instant>>,
    /// Peers learned of from each `PeerSource`, in its order.
    sources: [AtomicU64; 4],
}

impl TorrentStats {
    pub fn new(info_hash: [u8; 20], name: &str, save_path: &Path) -> Self {
        Self {
            info_hash,
            save_path: save_path.to_path_buf(),
            added: Instant::now(),
            name: Mutex::new(name.to_string()),
            layout: OnceLock::new(),
            state: Mutex::new(TorrentState::QueuedForChecking),
            finished: OnceLock::new(),
            transferred: Arc::default(),
            received: AtomicU64::new(0),
            verified: AtomicU64::new(0),
            pool: AtomicUsize::new(0),
            pieces: Mutex::new(Pieces::Unknown),
            peers: Mutex::new(Vec::new()),
            trackers: Mutex::new(Vec::new()),
            next_announce: Mutex::new(None),
            sources: Default::default(),
        }
    }

    /// Stats for `torrent` that nobody reads, for a download run outside a
    /// `Client`.
    pub fn detached(torrent: &TorrentFile, info_hash: [u8; 20], save_path: &Path) -> Arc<Self> {
        let stats = Self::new(info_hash, &torrent.info.name, save_path);
        stats.set_torrent(torrent);
        Arc::new(stats)
    }

    pub fn info_hash(&self) -> [u8; 20] {
        self.info_hash
    }

    pub fn name(&self) -> String {
        lock(&self.name).clone()
    }

    pub fn save_path(&self) -> &Path {
        &self.save_path
    }

    /// When the torrent was added to the session.
    pub fn added(&self) -> Instant {
        self.added
    }

    /// When its task ended, if it has.
    pub fn finished_at(&self) -> Option<Instant> {
        self.finished.get().copied()
    }

    /// Records the torrent's metadata, and takes its name.
    pub fn set_torrent(&self, torrent: &TorrentFile) {
        *lock(&self.name) = torrent.info.name.clone();
        let _ = self.layout.set(Layout::of(torrent));
    }

    pub fn layout(&self) -> Option<&Layout> {
        self.layout.get()
    }

    pub fn state(&self) -> TorrentState {
        lock(&self.state).clone()
    }

    pub fn set_state(&self, state: TorrentState) {
        if state.is_finished() {
            let _ = self.finished.set(Instant::now());
        }
        *lock(&self.state) = state;
    }

    /// Records how the torrent's task ended.
    pub(crate) fn finish(&self, result: &anyhow::Result<()>) {
        let state = match result {
            Ok(()) => TorrentState::Done,
            Err(e) if e.is::<Interrupted>() || e.is::<FetchInterrupted>() => {
                TorrentState::Interrupted
            }
            Err(e) => TorrentState::Failed(format!("{e:#}")),
        };
        self.set_state(state);
    }

    /// Counts `bytes` more hashed, while checking, and returns the bytes
    /// hashed so far.
    pub(crate) fn checked(&self, bytes: u64) -> u64 {
        match &mut *lock(&self.state) {
            TorrentState::Checking { done, .. } => {
                *done += bytes;
                *done
            }
            _ => 0,
        }
    }

    /// The counts reported to the tracker, shared with the download.
    pub fn transferred(&self) -> &Arc<Transferred> {
        &self.transferred
    }

    /// Verified piece data received this session.
    pub fn downloaded(&self) -> u64 {
        self.transferred.downloaded.load(Ordering::Relaxed)
    }

    /// Block data sent to peers this session.
    pub fn uploaded(&self) -> u64 {
        self.transferred.uploaded.load(Ordering::Relaxed)
    }

    /// Block data received this session, verified or not.
    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }

    pub(crate) fn add_received(&self, bytes: u64) {
        self.received.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Bytes verified and on disk, from earlier runs too.
    pub fn verified(&self) -> u64 {
        self.verified.load(Ordering::Relaxed)
    }

    pub(crate) fn set_verified(&self, bytes: u64) {
        self.verified.store(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_verified(&self, bytes: u64) {
        self.verified.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Peers known to the download, connected or not.
    pub fn pool(&self) -> usize {
        self.pool.load(Ordering::Relaxed)
    }

    pub(crate) fn set_pool(&self, peers: usize) {
        self.pool.store(peers, Ordering::Relaxed);
    }

    /// Lets `source` answer `pieces()` while the download runs.
    pub(crate) fn set_piece_source(&self, source: Weak<dyn PieceSource>) {
        *lock(&self.pieces) = Pieces::Live(source);
    }

    /// Keeps `final_pieces` as the answer to `pieces()` once the download
    /// has ended.
    pub(crate) fn set_final_pieces(&self, final_pieces: PieceSnapshot) {
        *lock(&self.pieces) = Pieces::Final(final_pieces);
    }

    /// The torrent's pieces, once known.
    pub fn pieces(&self) -> Option<PieceSnapshot> {
        // Upgraded outside the lock: the snapshot takes the queue's lock.
        let live = match &*lock(&self.pieces) {
            Pieces::Unknown => return None,
            Pieces::Final(pieces) => return Some(pieces.clone()),
            Pieces::Live(source) => source.upgrade(),
        };
        live.map(|source| source.piece_snapshot())
    }

    /// Counts `peer` as connected for as long as it lives.
    pub(crate) fn add_peer(&self, peer: &Arc<PeerStats>) {
        let mut peers = lock(&self.peers);
        peers.retain(|peer| peer.strong_count() > 0);
        peers.push(Arc::downgrade(peer));
    }

    /// The peers connected now.
    pub fn peers(&self) -> Vec<Arc<PeerStats>> {
        let mut peers = lock(&self.peers);
        peers.retain(|peer| peer.strong_count() > 0);
        peers.iter().filter_map(Weak::upgrade).collect()
    }

    /// Lists the trackers in `tiers`, none contacted yet.
    pub(crate) fn set_trackers(&self, tiers: &[Vec<String>]) {
        *lock(&self.trackers) = tiers
            .iter()
            .enumerate()
            .flat_map(|(tier, urls)| {
                urls.iter().map(move |url| TrackerStatus {
                    tier,
                    url: url.clone(),
                    result: TrackerResult::NotContacted,
                    peers: None,
                    seeders: None,
                    leechers: None,
                })
            })
            .collect();
    }

    /// Records what the tracker at `url` answered.
    pub(crate) fn tracker_answered(&self, url: &str, result: &anyhow::Result<AnnounceResponse>) {
        let mut trackers = lock(&self.trackers);
        let Some(tracker) = trackers.iter_mut().find(|tracker| tracker.url == url) else {
            return;
        };
        match result {
            Ok(response) => {
                tracker.result = TrackerResult::Ok;
                tracker.peers = Some(response.peers.len());
                tracker.seeders = response.seeders;
                tracker.leechers = response.leechers;
            }
            Err(e) => tracker.result = TrackerResult::Error(format!("{e:#}")),
        }
    }

    pub fn trackers(&self) -> Vec<TrackerStatus> {
        lock(&self.trackers).clone()
    }

    pub(crate) fn set_next_announce(&self, at: Option<Instant>) {
        *lock(&self.next_announce) = at;
    }

    /// When the trackers are next announced to.
    pub fn next_announce(&self) -> Option<Instant> {
        *lock(&self.next_announce)
    }

    /// Counts `count` peers learned of from `source`.
    pub(crate) fn add_source(&self, source: PeerSource, count: usize) {
        self.sources[source as usize].fetch_add(count as u64, Ordering::Relaxed);
    }

    /// Peers learned of from `source`.
    pub fn source(&self, source: PeerSource) -> u64 {
        self.sources[source as usize].load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn dropped_peers_leave_the_list() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        let first = Arc::new(PeerStats::new("127.0.0.1:1".parse().unwrap(), false));
        let second = Arc::new(PeerStats::new("127.0.0.1:2".parse().unwrap(), true));
        torrent.add_peer(&first);
        torrent.add_peer(&second);
        assert_eq!(torrent.peers().len(), 2);
        drop(first);
        let peers = torrent.peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].addr.port(), 2);
        assert_eq!(lock(&torrent.peers).len(), 1);
        // Pruned on the way in too, so churn does not grow the list unread.
        drop(peers);
        drop(second);
        for port in 3..10 {
            let peer = Arc::new(PeerStats::new(
                SocketAddr::from(([127, 0, 0, 1], port)),
                false,
            ));
            torrent.add_peer(&peer);
        }
        assert_eq!(lock(&torrent.peers).len(), 1);
    }

    #[test]
    fn trackers_start_uncontacted_and_record_answers() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        torrent.set_trackers(&[
            vec!["udp://a".into(), "udp://b".into()],
            vec!["http://c".into()],
        ]);
        let response = AnnounceResponse {
            peers: vec!["127.0.0.1:1".parse().unwrap()],
            interval: None,
            min_interval: None,
            seeders: Some(3),
            leechers: None,
        };
        torrent.tracker_answered("udp://b", &Ok(response));
        torrent.tracker_answered("http://c", &Err(anyhow::anyhow!("timed out")));
        torrent.tracker_answered("udp://unknown", &Err(anyhow::anyhow!("x")));
        let trackers = torrent.trackers();
        assert_eq!(trackers.len(), 3);
        assert_eq!(trackers[0].result, TrackerResult::NotContacted);
        assert_eq!(trackers[1].result, TrackerResult::Ok);
        assert_eq!((trackers[1].peers, trackers[1].seeders), (Some(1), Some(3)));
        assert_eq!(trackers[2].tier, 1);
        assert_eq!(trackers[2].result, TrackerResult::Error("timed out".into()));
    }

    #[test]
    fn a_finished_state_stamps_the_end() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        torrent.set_state(TorrentState::Checking { done: 0, total: 10 });
        assert_eq!(torrent.checked(4), 4);
        assert_eq!(
            torrent.state(),
            TorrentState::Checking { done: 4, total: 10 }
        );
        assert!(torrent.finished_at().is_none());
        torrent.set_state(TorrentState::Done);
        assert!(torrent.finished_at().is_some());
    }
}
