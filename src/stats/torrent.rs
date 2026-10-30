use crate::dht::SwarmEstimate;
use crate::download::{CheckedOnly, Interrupted};
use crate::magnet::FetchInterrupted;
use crate::selection::Priority;
use crate::torrent::TorrentFile;
use crate::tracker::AnnounceResponse;
use crate::tracker::announcer::Transferred;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use super::{PeerStats, WebSeedStats, lock};

/// Where a torrent is in its life.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// Waiting for a slot, as the limits on active downloads and seeds
    /// allow only so many torrents to run (see `queue`). No task runs.
    Queued,
    /// Hashing the data on disk: `done` of `total` bytes.
    Checking {
        done: u64,
        total: u64,
    },
    Downloading,
    Seeding,
    /// Asked to pause, and stopping.
    Pausing,
    /// Asked to be removed, and stopping first if it runs.
    Removing,
    /// Stopped by a pause, to be started again.
    Paused,
    Done,
    Interrupted,
    Failed(String),
}

impl TorrentState {
    /// Whether the torrent's task has ended, or has not started: it is
    /// paused, waiting in the queue, done, stopped or failed.
    pub fn is_finished(&self) -> bool {
        matches!(
            self,
            TorrentState::Paused
                | TorrentState::Queued
                | TorrentState::Done
                | TorrentState::Interrupted
                | TorrentState::Failed(_)
        )
    }
}

/// What the earlier runs of a torrent this session did, before it was
/// paused (or ended) and started again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Previous {
    pub received: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    /// How long they ran.
    pub active: Duration,
}

/// The shape of a torrent, known once its metadata is.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

/// What the run of a torrent is to download.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Selected {
    /// Each file's priority, in order.
    priorities: Vec<Priority>,
    /// Bytes in the pieces wanted.
    wanted: u64,
}

/// Which pieces we have, are downloading, and how many connected peers have
/// each.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TrackerResult {
    NotContacted,
    Ok,
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    Lsd,
}

/// One torrent of the session.
pub struct TorrentStats {
    info_hash: [u8; 20],
    save_path: PathBuf,
    added: Instant,
    /// When this run started; `added` unless an earlier run was paused.
    started: Instant,
    previous: Previous,
    name: Mutex<String>,
    layout: OnceLock<Layout>,
    selection: Mutex<Option<Selected>>,
    /// Whether the download takes its pieces in order, shared with its
    /// queue so that it can change while the download runs.
    sequential: Arc<AtomicBool>,
    /// Whether `sequential` has been set, by the user or the session, so
    /// that the default no longer applies.
    sequential_chosen: AtomicBool,
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
    /// Web seeds of the running download, up to date as it runs.
    web_seeds: Mutex<Vec<Arc<WebSeedStats>>>,
    trackers: Mutex<Vec<TrackerStatus>>,
    next_announce: Mutex<Option<Instant>>,
    /// Peers learned of from each `PeerSource`, in its order.
    sources: [AtomicU64; 5],
    /// The swarm size the DHT's last scrape found (BEP 33).
    dht_swarm: Mutex<Option<SwarmEstimate>>,
}

impl TorrentStats {
    pub fn new(info_hash: [u8; 20], name: &str, save_path: &Path) -> Self {
        let now = Instant::now();
        Self::again(info_hash, name, save_path, now, Previous::default())
    }

    /// Stats for a torrent run again: added at `added`, with what its
    /// earlier runs did in `previous`.
    pub(crate) fn again(
        info_hash: [u8; 20],
        name: &str,
        save_path: &Path,
        added: Instant,
        previous: Previous,
    ) -> Self {
        Self {
            info_hash,
            save_path: save_path.to_path_buf(),
            added,
            started: Instant::now(),
            previous,
            name: Mutex::new(name.to_string()),
            layout: OnceLock::new(),
            selection: Mutex::new(None),
            sequential: Arc::default(),
            sequential_chosen: AtomicBool::new(false),
            state: Mutex::new(TorrentState::QueuedForChecking),
            finished: OnceLock::new(),
            transferred: Arc::default(),
            received: AtomicU64::new(0),
            verified: AtomicU64::new(0),
            pool: AtomicUsize::new(0),
            pieces: Mutex::new(Pieces::Unknown),
            peers: Mutex::new(Vec::new()),
            web_seeds: Mutex::new(Vec::new()),
            trackers: Mutex::new(Vec::new()),
            next_announce: Mutex::new(None),
            sources: Default::default(),
            dht_swarm: Mutex::new(None),
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

    /// When this run of it started.
    pub fn started(&self) -> Instant {
        self.started
    }

    /// What its earlier runs this session did.
    pub fn previous(&self) -> Previous {
        self.previous
    }

    /// What this run and the earlier ones did, as the next run's
    /// `previous`.
    pub(crate) fn so_far(&self, now: Instant) -> Previous {
        Previous {
            received: self.received(),
            downloaded: self.downloaded(),
            uploaded: self.uploaded(),
            active: self.active(now),
        }
    }

    /// How long the torrent has run this session, up to `now` or its end.
    pub fn active(&self, now: Instant) -> Duration {
        let end = self.finished_at().unwrap_or(now);
        self.previous.active + end.saturating_duration_since(self.started)
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

    /// Records which files this run downloads, and the bytes of the pieces
    /// that takes.
    pub(crate) fn set_selection(&self, priorities: Vec<Priority>, wanted: u64) {
        *lock(&self.selection) = Some(Selected { priorities, wanted });
    }

    /// Each file's priority, in order, once the run has chosen them.
    pub fn priorities(&self) -> Option<Vec<Priority>> {
        lock(&self.selection)
            .as_ref()
            .map(|selected| selected.priorities.clone())
    }

    /// Bytes in the pieces wanted, once the run has chosen them: the size
    /// that `verified` is out of. It is the torrent's size unless files are
    /// skipped.
    pub fn wanted(&self) -> Option<u64> {
        lock(&self.selection)
            .as_ref()
            .map(|selected| selected.wanted)
    }

    /// Whether the download takes its pieces in order, the first and last
    /// of each file first, rather than the rarest first.
    pub fn sequential(&self) -> bool {
        self.sequential.load(Ordering::Relaxed)
    }

    /// Whether the download is sequential, if that was chosen rather than
    /// left to the default.
    pub fn sequential_choice(&self) -> Option<bool> {
        self.sequential_chosen
            .load(Ordering::Relaxed)
            .then(|| self.sequential())
    }

    /// The flag behind `sequential`, which the download's queue reads.
    pub(crate) fn sequential_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.sequential)
    }

    /// Chooses whether the download is sequential, now and until it is
    /// chosen again, even by a later run of the torrent.
    pub fn set_sequential(&self, on: bool) {
        self.sequential.store(on, Ordering::Relaxed);
        self.sequential_chosen.store(true, Ordering::Relaxed);
    }

    /// Turns sequential download on or off, as `set_sequential` does, and
    /// returns what it is now.
    pub fn toggle_sequential(&self) -> bool {
        let on = !self.sequential.fetch_xor(true, Ordering::Relaxed);
        self.sequential_chosen.store(true, Ordering::Relaxed);
        on
    }

    /// Makes the download sequential or not by default, unless that was
    /// chosen already, by the user, the session, or an earlier run.
    pub(crate) fn default_sequential(&self, on: bool) {
        if !self.sequential_chosen.swap(true, Ordering::Relaxed) {
            self.sequential.store(on, Ordering::Relaxed);
        }
    }

    /// Takes the choice of sequential download `earlier` run of the same
    /// torrent had made, if it made one.
    pub(crate) fn inherit_sequential(&self, earlier: &TorrentStats) {
        if earlier.sequential_chosen.load(Ordering::Relaxed) {
            self.set_sequential(earlier.sequential());
        }
    }

    pub fn state(&self) -> TorrentState {
        lock(&self.state).clone()
    }

    /// Sets the state, except that a torrent pausing shows so until its
    /// task has ended, whatever it does meanwhile, and one being removed
    /// shows so until its row goes.
    pub fn set_state(&self, state: TorrentState) {
        let mut current = lock(&self.state);
        let keep = match *current {
            TorrentState::Pausing => !state.is_finished(),
            TorrentState::Removing => true,
            _ => false,
        };
        if state.is_finished() {
            let _ = self.finished.set(Instant::now());
        }
        if !keep || state == TorrentState::Removing {
            *current = state;
        }
    }

    /// Records how the torrent's task ended. One stopped by a pause stays
    /// `Pausing` until the manager says it is `Paused`.
    pub(crate) fn finish(&self, result: &anyhow::Result<()>) {
        let state = match result {
            Ok(()) => TorrentState::Done,
            Err(e) if e.is::<CheckedOnly>() => TorrentState::Paused,
            Err(e) if e.is::<Interrupted>() || e.is::<FetchInterrupted>() => {
                if self.state() == TorrentState::Pausing {
                    let _ = self.finished.set(Instant::now());
                    return;
                }
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

    /// Verified piece data received this session, by earlier runs too.
    pub fn downloaded(&self) -> u64 {
        self.previous.downloaded + self.transferred.downloaded.load(Ordering::Relaxed)
    }

    /// Block data sent to peers this session, by earlier runs too.
    pub fn uploaded(&self) -> u64 {
        self.previous.uploaded + self.transferred.uploaded.load(Ordering::Relaxed)
    }

    /// Block data received this session, verified or not, by earlier runs
    /// too.
    pub fn received(&self) -> u64 {
        self.previous.received + self.received.load(Ordering::Relaxed)
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

    /// Shows `seeds` as the download's web seeds, replacing earlier ones.
    pub(crate) fn set_web_seeds(&self, seeds: Vec<Arc<WebSeedStats>>) {
        *lock(&self.web_seeds) = seeds;
    }

    /// The web seeds the download uses.
    pub fn web_seeds(&self) -> Vec<Arc<WebSeedStats>> {
        lock(&self.web_seeds).clone()
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

    /// Records the swarm size the DHT scraped.
    pub(crate) fn set_dht_swarm(&self, swarm: SwarmEstimate) {
        *lock(&self.dht_swarm) = Some(swarm);
    }

    /// The swarm size the DHT scraped, if it did.
    pub fn dht_swarm(&self) -> Option<SwarmEstimate> {
        *lock(&self.dht_swarm)
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
    fn the_dht_swarm_is_what_the_last_scrape_said() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        assert_eq!(torrent.dht_swarm(), None);
        let swarm = SwarmEstimate {
            seeds: 3,
            leechers: 9,
        };
        torrent.set_dht_swarm(swarm);
        assert_eq!(torrent.dht_swarm(), Some(swarm));
    }

    #[test]
    fn the_default_applies_until_sequential_is_chosen() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        assert_eq!(torrent.sequential_choice(), None);
        torrent.default_sequential(true);
        assert!(torrent.sequential());
        // A default is a choice too: the first one stands.
        torrent.default_sequential(false);
        assert!(torrent.sequential());
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        assert!(torrent.toggle_sequential());
        assert_eq!(torrent.sequential_choice(), Some(true));
        torrent.default_sequential(false);
        assert!(torrent.sequential());
        assert!(!torrent.toggle_sequential());
        assert_eq!(torrent.sequential_choice(), Some(false));
    }

    #[test]
    fn the_flag_the_queue_reads_follows_the_choice() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        let flag = torrent.sequential_flag();
        torrent.set_sequential(true);
        assert!(flag.load(Ordering::Relaxed));
        torrent.toggle_sequential();
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn a_later_run_inherits_a_choice_but_not_a_default() {
        let earlier = TorrentStats::new([1; 20], "t", Path::new("."));
        let later = TorrentStats::new([1; 20], "t", Path::new("."));
        earlier.default_sequential(true);
        later.inherit_sequential(&earlier);
        assert_eq!(later.sequential_choice(), Some(true));
        let earlier = TorrentStats::new([1; 20], "t", Path::new("."));
        let later = TorrentStats::new([1; 20], "t", Path::new("."));
        later.inherit_sequential(&earlier);
        assert_eq!(later.sequential_choice(), None);
    }

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

    #[test]
    fn a_torrent_pausing_shows_so_until_it_is_paused() {
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        torrent.set_state(TorrentState::Pausing);
        torrent.set_state(TorrentState::Downloading);
        assert_eq!(torrent.state(), TorrentState::Pausing);
        torrent.finish(&Err(Interrupted {
            remaining: 1,
            pieces: 2,
        }
        .into()));
        assert_eq!(torrent.state(), TorrentState::Pausing);
        assert!(torrent.finished_at().is_some());
        torrent.set_state(TorrentState::Paused);
        assert!(torrent.state().is_finished());

        // Removing outranks pausing, and shows until the row goes.
        let removed = TorrentStats::new([1; 20], "t", Path::new("."));
        removed.set_state(TorrentState::Pausing);
        removed.set_state(TorrentState::Removing);
        removed.set_state(TorrentState::Seeding);
        removed.finish(&Ok(()));
        assert_eq!(removed.state(), TorrentState::Removing);
        assert!(removed.finished_at().is_some());

        // A pause that comes too late shows how it ended.
        let torrent = TorrentStats::new([1; 20], "t", Path::new("."));
        torrent.set_state(TorrentState::Pausing);
        torrent.finish(&Ok(()));
        assert_eq!(torrent.state(), TorrentState::Done);
    }

    #[test]
    fn a_torrent_run_again_keeps_its_row_and_counts() {
        use crate::client::ConnectionSlots;
        use crate::rate_limit::RateLimits;
        use crate::stats::SessionStats;

        let session = SessionStats::new(
            Arc::new(RateLimits::from_kib(0, 0)),
            ConnectionSlots::new(1),
        );
        let first = session.add_torrent([1; 20], "a", Path::new("."));
        session.add_torrent([2; 20], "b", Path::new("."));
        first.transferred().downloaded.store(100, Ordering::Relaxed);
        first.transferred().uploaded.store(40, Ordering::Relaxed);
        first.add_received(120);
        // Still running: a second run of it gets a row of its own.
        let duplicate = session.add_torrent([1; 20], "a", Path::new("."));
        assert_eq!(session.torrents().len(), 3);
        duplicate.set_state(TorrentState::Failed("x".into()));
        first.set_state(TorrentState::Paused);

        // Ended: the first such row is taken over.
        let again = session.add_torrent([1; 20], "a", Path::new("."));
        let torrents = session.torrents();
        assert_eq!(torrents.len(), 3);
        assert!(Arc::ptr_eq(&torrents[0], &again));
        assert_eq!(again.added(), first.added());
        assert!(again.started() > first.added());
        assert_eq!((again.downloaded(), again.uploaded()), (100, 40));
        assert_eq!(again.received(), 120);
        // The tracker hears of this run's only.
        assert_eq!(again.transferred().uploaded.load(Ordering::Relaxed), 0);
        let previous = again.previous();
        assert_eq!(previous.active, first.active(Instant::now()));
        assert!(again.active(Instant::now()) >= previous.active);
        again.transferred().uploaded.store(5, Ordering::Relaxed);
        assert_eq!(again.uploaded(), 45);

        // Removed, its row goes, and only its.
        session.remove_torrent(&again);
        let torrents = session.torrents();
        assert_eq!(torrents.len(), 2);
        assert!(Arc::ptr_eq(&torrents[1], &duplicate));
    }
}
