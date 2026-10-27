//! Live numbers about a session and its torrents, for the terminal UI.
//!
//! The engine publishes cheap counters here (relaxed atomics, and short
//! locks taken at most once per piece or per announce, never per block),
//! and a reader pulls them on its own schedule. Rates and history are the
//! reader's business: nothing here keeps time beyond a few timestamps.

use crate::client::ConnectionSlots;
use crate::dht::Dht;
use crate::download::Interrupted;
use crate::magnet::FetchInterrupted;
use crate::torrent::TorrentFile;
use crate::tracker::AnnounceResponse;
use crate::tracker::announcer::Transferred;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::Instant;

/// What every torrent of a `Client` shares, and the torrents themselves.
pub struct SessionStats {
    torrents: Mutex<Vec<Arc<TorrentStats>>>,
    /// Set once the DHT node is started.
    dht: OnceLock<Dht>,
    /// Set once the listening port is open.
    port: OnceLock<Option<u16>>,
    /// Download and upload limits in KiB/s, 0 for none.
    rate_limits: (u64, u64),
    slots: ConnectionSlots,
}

impl SessionStats {
    /// Stats for a session with the rate limits `rate_limits` (KiB/s each
    /// way) whose connections hold `slots`.
    pub fn new(rate_limits: (u64, u64), slots: ConnectionSlots) -> Self {
        Self {
            torrents: Mutex::new(Vec::new()),
            dht: OnceLock::new(),
            port: OnceLock::new(),
            rate_limits,
            slots,
        }
    }

    /// The torrents of the session, in the order they were added.
    pub fn torrents(&self) -> Vec<Arc<TorrentStats>> {
        lock(&self.torrents).clone()
    }

    /// Adds a torrent called `name` (until its metadata says otherwise),
    /// saved under `save_path`.
    pub fn add_torrent(
        &self,
        info_hash: [u8; 20],
        name: &str,
        save_path: &Path,
    ) -> Arc<TorrentStats> {
        let torrent = Arc::new(TorrentStats::new(info_hash, name, save_path));
        lock(&self.torrents).push(Arc::clone(&torrent));
        torrent
    }

    pub(crate) fn set_dht(&self, dht: Dht) {
        let _ = self.dht.set(dht);
    }

    pub(crate) fn set_port(&self, port: Option<u16>) {
        let _ = self.port.set(port);
    }

    /// Nodes in the DHT routing table, or `None` while no node runs.
    pub fn dht_nodes(&self) -> Option<usize> {
        self.dht.get().map(Dht::node_count)
    }

    /// Where we accept peers, once known.
    pub fn port(&self) -> Option<u16> {
        self.port.get().copied().flatten()
    }

    /// Download and upload limits in KiB/s, 0 for none.
    pub fn rate_limits(&self) -> (u64, u64) {
        self.rate_limits
    }

    /// Peer connections open now, across every torrent.
    pub fn connections(&self) -> usize {
        self.slots.in_use()
    }

    /// Most peer connections open at once.
    pub fn max_connections(&self) -> usize {
        self.slots.limit()
    }
}

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

/// How the two sides of a connection stand, packed in a byte.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerFlags(u8);

impl PeerFlags {
    const AM_CHOKING: u8 = 1;
    const PEER_CHOKING: u8 = 1 << 1;
    const AM_INTERESTED: u8 = 1 << 2;
    const PEER_INTERESTED: u8 = 1 << 3;
    const FAST: u8 = 1 << 4;
    const PEX: u8 = 1 << 5;

    pub fn new(
        am_choking: bool,
        peer_choking: bool,
        am_interested: bool,
        peer_interested: bool,
        fast: bool,
        pex: bool,
    ) -> Self {
        let bit = |set: bool, bit: u8| if set { bit } else { 0 };
        Self(
            bit(am_choking, Self::AM_CHOKING)
                | bit(peer_choking, Self::PEER_CHOKING)
                | bit(am_interested, Self::AM_INTERESTED)
                | bit(peer_interested, Self::PEER_INTERESTED)
                | bit(fast, Self::FAST)
                | bit(pex, Self::PEX),
        )
    }

    /// Every connection starts choked both ways, with neither side
    /// interested.
    pub fn initial() -> Self {
        Self::new(true, true, false, false, false, false)
    }

    pub fn am_choking(self) -> bool {
        self.0 & Self::AM_CHOKING != 0
    }

    pub fn peer_choking(self) -> bool {
        self.0 & Self::PEER_CHOKING != 0
    }

    pub fn am_interested(self) -> bool {
        self.0 & Self::AM_INTERESTED != 0
    }

    pub fn peer_interested(self) -> bool {
        self.0 & Self::PEER_INTERESTED != 0
    }

    pub fn fast(self) -> bool {
        self.0 & Self::FAST != 0
    }

    pub fn pex(self) -> bool {
        self.0 & Self::PEX != 0
    }
}

/// One connected peer, kept up to date by its worker.
pub struct PeerStats {
    pub addr: SocketAddr,
    /// Whether the peer connected to us.
    pub inbound: bool,
    /// `v` from its extension handshake.
    client: OnceLock<String>,
    downloaded: AtomicU64,
    uploaded: AtomicU64,
    /// Pieces the peer has.
    pieces: AtomicU32,
    flags: AtomicU8,
}

impl PeerStats {
    pub fn new(addr: SocketAddr, inbound: bool) -> Self {
        Self {
            addr,
            inbound,
            client: OnceLock::new(),
            downloaded: AtomicU64::new(0),
            uploaded: AtomicU64::new(0),
            pieces: AtomicU32::new(0),
            flags: AtomicU8::new(PeerFlags::initial().0),
        }
    }

    pub fn client(&self) -> Option<&str> {
        self.client.get().map(String::as_str)
    }

    pub(crate) fn has_client(&self) -> bool {
        self.client.get().is_some()
    }

    pub(crate) fn set_client(&self, client: &str) {
        let _ = self.client.set(client.to_string());
    }

    /// Block data received from the peer.
    pub fn downloaded(&self) -> u64 {
        self.downloaded.load(Ordering::Relaxed)
    }

    /// Block data sent to the peer.
    pub fn uploaded(&self) -> u64 {
        self.uploaded.load(Ordering::Relaxed)
    }

    pub(crate) fn add_downloaded(&self, bytes: u64) {
        self.downloaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn add_uploaded(&self, bytes: u64) {
        self.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Pieces the peer has.
    pub fn pieces(&self) -> u32 {
        self.pieces.load(Ordering::Relaxed)
    }

    pub(crate) fn set_pieces(&self, pieces: usize) {
        self.pieces
            .store(u32::try_from(pieces).unwrap_or(u32::MAX), Ordering::Relaxed);
    }

    pub fn flags(&self) -> PeerFlags {
        PeerFlags(self.flags.load(Ordering::Relaxed))
    }

    pub(crate) fn set_flags(&self, flags: PeerFlags) {
        self.flags.store(flags.0, Ordering::Relaxed);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_flags_pack_and_unpack() {
        for bits in 0..64u8 {
            let set = |i: u8| bits & (1 << i) != 0;
            let flags = PeerFlags::new(set(0), set(1), set(2), set(3), set(4), set(5));
            assert_eq!(
                [
                    flags.am_choking(),
                    flags.peer_choking(),
                    flags.am_interested(),
                    flags.peer_interested(),
                    flags.fast(),
                    flags.pex(),
                ],
                [set(0), set(1), set(2), set(3), set(4), set(5)]
            );
        }
        let peer = PeerStats::new("127.0.0.1:1".parse().unwrap(), false);
        assert_eq!(peer.flags(), PeerFlags::initial());
        let flags = PeerFlags::new(false, true, true, false, true, false);
        peer.set_flags(flags);
        assert_eq!(peer.flags(), flags);
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
}
