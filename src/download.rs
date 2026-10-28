use crate::client::{Claim, Client, ConnectionSlots};
use crate::dht::Dht;
use crate::listener::Listener;
use crate::manager::{Recheck, StartOptions};
use crate::rate_limit::RateLimits;
use crate::resume::{self, Target};
use crate::selection::{self, FileSelection, Priority};
use crate::stats::{PeerSource, TorrentStats};
use crate::storage::Storage;
use crate::torrent::{self, TorrentFile};
use crate::tracker::announcer::{Announcer, Transferred};
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use anyhow::{Result, bail};
use indicatif::MultiProgress;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tracing::{info, warn};

mod check;
mod choker;
pub(crate) mod peer_pool;
mod progress;
mod queue;
mod requests;
mod swarm;
mod upload;
mod worker;

pub use check::check_file;
pub(crate) use check::hash_pieces;
pub(crate) use swarm::{
    ConnectedPeers, Limits, StartPeers, dht_lookup, download_with, reannounce, sleep_until_some,
};

const BLOCK_SIZE: u32 = 16 * 1024;

pub struct DownloadConfig {
    pub output_dir: PathBuf,
    /// Most peers to be connected to at once, for each torrent.
    pub max_peers: usize,
    /// Most peers to be connected to at once, across every torrent of a
    /// `Client`.
    pub max_connections: usize,
    /// Port to listen on for incoming peers; 0 picks a free one.
    pub listen_port: u16,
    pub seed: SeedLimits,
    /// Whether to look for peers in the DHT too (never for private torrents).
    pub dht: bool,
    /// Most block data to download per second, in KiB, across all peers;
    /// 0 for no limit.
    pub max_download_rate: u64,
    /// Most block data to upload per second, in KiB, across all peers; 0
    /// for no limit.
    pub max_upload_rate: u64,
    /// Directory the resume files (see `resume`) are kept in, or `None` to
    /// keep none.
    pub resume_dir: Option<PathBuf>,
}

/// How long to keep seeding once every piece is done. Seeding stops at
/// whichever limit is reached first; with neither, the run ends as soon as
/// the download is complete.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct SeedLimits {
    /// Stop once we have uploaded this many times the torrent's size this
    /// session; 0 for no ratio limit. The torrent's size rather than what we
    /// downloaded, so that a download resumed from disk still gives back
    /// its share.
    pub ratio: f64,
    /// Stop after seeding this long; zero for no time limit.
    pub time: Duration,
}

impl SeedLimits {
    /// Whether to seed at all once the download is complete.
    pub fn enabled(&self) -> bool {
        self.ratio > 0.0 || !self.time.is_zero()
    }

    /// Whether a limit is reached after uploading `uploaded` bytes of a
    /// torrent of `size` bytes, having seeded for `seeded`.
    fn reached(&self, uploaded: u64, size: u64, seeded: Duration) -> bool {
        (self.ratio > 0.0 && uploaded as f64 >= self.ratio * size as f64)
            || (!self.time.is_zero() && seeded >= self.time)
    }
}

/// A torrent to download and seed: what the peer workers need to know about
/// it, and how long to stay once it is complete.
pub struct Session {
    pub torrent: TorrentFile,
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub storage: Arc<Storage>,
    pub seed: SeedLimits,
    /// Our DHT node, if any: searched for peers, and told about peers' DHT
    /// nodes, whose ports they send us.
    pub dht: Option<Dht>,
    /// How fast block data may move each way, shared by every peer (of
    /// every torrent in the session).
    pub rate_limits: Arc<RateLimits>,
    /// The torrent's info dictionary exactly as encoded, if we have it, for
    /// peers that fetch it by magnet link (BEP 9).
    pub metadata: Option<Vec<u8>>,
    /// Where the torrent's progress bars go, next to those of the other
    /// torrents in the session.
    pub bars: MultiProgress,
    /// Where the torrent's resume file is saved while it downloads, if it
    /// has one.
    pub resume: Option<Target>,
    /// Where the download publishes its progress and its peers.
    pub stats: Arc<TorrentStats>,
}

impl Session {
    /// How much each piece is wanted, by the selection the storage was
    /// opened with.
    pub fn piece_priorities(&self) -> Vec<Option<Priority>> {
        selection::piece_priorities(
            &self.torrent,
            self.storage.files(),
            self.storage.selection(),
        )
    }

    /// Bytes of wanted pieces not marked in `have`: what is left to
    /// download.
    pub fn left(&self, have: &[bool]) -> Result<u64> {
        let priorities = self.piece_priorities();
        Ok(selection::wanted_len(&self.torrent, &priorities)?
            - selection::wanted_done(&self.torrent, &priorities, have)?)
    }
}

/// The selection a run of a torrent of `file_count` files starts with:
/// the one in `options`, else the one made of the files picked on the
/// command line, else the one in the resume file, else all `Normal`.
pub(crate) fn initial_selection(
    options: &StartOptions,
    resume: Option<&Target>,
    file_count: usize,
) -> FileSelection {
    options
        .selection
        .clone()
        .or_else(|| {
            options
                .picks
                .as_ref()
                .and_then(|picks| picked(picks, file_count))
        })
        .or_else(|| resume.and_then(|target| resume::load_selection(target, file_count)))
        .unwrap_or_default()
}

/// The selection that downloads the files `picks` names of a torrent of
/// `file_count` files. A number past the last file is warned of; if none
/// is left, so is that every file is downloaded, and there is no selection.
fn picked(picks: &selection::FilePicks, file_count: usize) -> Option<FileSelection> {
    if picks.last() > file_count {
        warn!(
            "--select-file names file {}, but the torrent has {file_count}",
            picks.last()
        );
    }
    let selection = picks.selection(file_count);
    if selection.is_all_skipped() {
        warn!("--select-file names none of the files; downloading all of them");
        return None;
    }
    Some(selection)
}

/// Downloads the torrent at `torrent_path` and seeds it for as long as the
/// client's `seed` setting says, stopping early (see `download_from_peers`)
/// if `shutdown` resolves. It is first polled when the data on disk is
/// checked, or, if there is none to check, once the tracker has answered, so
/// a signal handler behind it is installed no later than that. It also ends
/// the wait for the client's turn to check the data, and the check itself,
/// with an `Interrupted` error.
///
/// The torrent shares the client's listening port, DHT node, rate limits,
/// connection slots and turn to check the data on disk with the client's
/// other torrents, and fails if the client is running it already, or is
/// running a torrent that writes one of the same files, before it creates
/// or reads anything.
///
/// A download that is already complete on disk exits at once, unless it is
/// to be seeded.
pub async fn download_file(
    torrent_path: &Path,
    client: &Client,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let metainfo = torrent::load_metainfo(torrent_path).await?;
    download_metainfo(metainfo, &StartOptions::default(), client, shutdown).await
}

/// `download_file` for a torrent file loaded already, run as `options`
/// say.
pub async fn download_metainfo(
    metainfo: torrent::Metainfo,
    options: &StartOptions,
    client: &Client,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    // Before the row, so a torrent the client runs already does not leave a
    // second row of the same torrent behind.
    let claim = client.claim(metainfo.info_hash)?;
    let stats = client.stats().add_torrent(
        metainfo.info_hash,
        &metainfo.torrent.info.name,
        &client.config().output_dir,
    );
    stats.set_torrent(&metainfo.torrent);
    let result = download_loaded(metainfo, options, client, claim, &stats, shutdown).await;
    stats.finish(&result);
    result
}

/// `download_metainfo`, publishing its progress in `stats`.
async fn download_loaded(
    metainfo: torrent::Metainfo,
    options: &StartOptions,
    client: &Client,
    mut claim: Claim<'_>,
    stats: &Arc<TorrentStats>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let config = client.config();
    let torrent::Metainfo {
        torrent,
        info_hash,
        info_bytes,
    } = metainfo;
    info!(
        name = %torrent.info.name,
        size = torrent.length(),
        pieces = torrent.num_pieces(),
        info_hash = %hex(&info_hash),
        "loaded torrent"
    );
    // Before anything is created, so a torrent that shares a file with a
    // running one leaves its files alone.
    claim.claim_files(
        &torrent.info.name,
        &Storage::paths_async(&torrent.info, &config.output_dir).await?,
    )?;

    // Before announcing, so a bad output directory does not leave the
    // tracker without a `stopped`.
    let resume = Target::new(config, info_hash);
    let selection = initial_selection(
        options,
        resume.as_ref(),
        selection::file_count(&torrent.info),
    );
    let storage =
        Arc::new(Storage::new_async(&torrent.info, &config.output_dir, &selection).await?);
    let dht = start_dht(&torrent, client).await;
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: client.peer_id(),
        storage,
        seed: config.seed,
        dht,
        rate_limits: client.rate_limits(),
        metadata: Some(info_bytes),
        bars: client.bars().clone(),
        resume,
        stats: Arc::clone(stats),
    });

    let mut shutdown = std::pin::pin!(shutdown);
    let recheck = options.recheck != Recheck::No;
    let have =
        resume::initial_pieces(&session, recheck, client.hashing(), shutdown.as_mut()).await?;
    if options.recheck == Recheck::ThenPause {
        bail!(CheckedOnly);
    }
    let left = session.left(&have)?;
    if left == 0 {
        info!(output = %config.output_dir.display(), "already complete");
        if !config.seed.enabled() {
            return Ok(());
        }
    }

    let incoming = client.listener().await.register(info_hash)?;
    let port = incoming.port().unwrap_or(config.listen_port);
    let request = AnnounceRequest {
        info_hash,
        peer_id: session.peer_id,
        port,
        uploaded: 0,
        downloaded: 0,
        left,
        event: AnnounceEvent::Started,
    };
    let tiers = session.torrent.tracker_tiers();
    let started = Announcer::start_reporting(tiers, request, Some(Arc::clone(stats)));
    let (announcer, peers) = match started.await {
        Ok((announcer, peers)) => (Some(announcer), peers),
        // No trackers, or none answered: the DHT may still find peers.
        Err(e) if session.dht.is_some() => {
            info!("{e:#}; looking for peers in the DHT only");
            (None, Vec::new())
        }
        Err(e) => return Err(e),
    };
    // A seed can wait for peers to connect to it, and the DHT may still find
    // some.
    if peers.is_empty() && left > 0 && session.dht.is_none() {
        if let Some(announcer) = announcer {
            announcer.stop(&Transferred::default()).await;
        }
        bail!("no tracker returned any peers");
    }
    if announcer.is_some() {
        info!(peers = peers.len(), "tracker announce succeeded");
    }

    stats.add_source(PeerSource::Tracker, peers.len());
    let mut start = StartPeers::default();
    start.pool.add(peers);
    download_with(
        session,
        &have,
        start,
        Limits {
            max_peers: config.max_peers,
            slots: client.slots(),
        },
        incoming,
        announcer,
        shutdown,
    )
    .await?;
    info!(output = %config.output_dir.display(), "done");
    Ok(())
}

/// The client's DHT node, unless disabled or the torrent is private. It
/// bootstraps once a download looks for peers, or within a minute.
async fn start_dht(torrent: &TorrentFile, client: &Client) -> Option<Dht> {
    if !client.config().dht {
        return None;
    }
    if torrent.is_private() {
        info!("private torrent: not using the DHT");
        return None;
    }
    client.dht().await
}

/// Downloads every piece not marked in `have` from `peers`, connected to at
/// most `max_peers` at once. A peer that drops is replaced by the next one,
/// and failed peers are retried with backoff (see `PeerPool`).
///
/// Peers that connect to us on any of `listeners` are downloaded from too,
/// while there is room under `max_peers`. One that misbehaves is turned away
/// by IP address from then on.
///
/// Every connected peer may download the pieces we have, as far as the
/// choker (see `choker`) unchokes it.
///
/// Once every piece is done, we seed until a limit in `session.seed` is
/// reached (or not at all without one): workers keep serving their peers,
/// new peers are still taken on, and running out of peers no longer ends
/// the run. A peer that is a seed too is let go.
///
/// With an `announcer`, the tracker is re-announced to on its interval and
/// the peers it returns join the pool. If we run out of peers while
/// downloading, the tracker is asked for more once before giving up. The
/// tracker is sent `completed` when the download finishes (not if it was
/// complete from the start) and `stopped` at the end, with the bytes
/// downloaded and uploaded.
///
/// With a DHT node in the session, the DHT is searched for peers at once
/// and then every few minutes (see `DhtSearch`), announcing our listening
/// port, and the peers found join the pool. When we run out of peers, the
/// DHT gets two more lookups (the first may have started earlier) before we
/// give up.
///
/// Peers that support peer exchange (BEP 11) are told which peers we are
/// connected to, and the peers they tell us about join the pool (see
/// `pex`), unless the torrent is private.
///
/// When `shutdown` resolves, no more blocks are requested and workers exit,
/// each finishing any piece write it has started. The tracker is still sent
/// `stopped`, and the result is an `Interrupted` error unless every piece
/// was done.
pub async fn download_from_peers(
    session: Arc<Session>,
    have: &[bool],
    peers: &[SocketAddr],
    max_peers: usize,
    listeners: Vec<TcpListener>,
    announcer: Option<Announcer>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let mut start = StartPeers::default();
    start.pool.add(peers.iter().copied());
    let incoming = Listener::start(listeners).register(session.info_hash)?;
    // Slots of its own, as many as the peers it may have.
    let limits = Limits {
        max_peers,
        slots: ConnectionSlots::new(max_peers),
    };
    download_with(session, have, start, limits, incoming, announcer, shutdown).await
}

/// A peer that broke the protocol or sent bad data, or turned out to be us.
/// It is never retried.
#[derive(Debug)]
pub(crate) struct Misbehaved(pub(crate) &'static str);

impl std::fmt::Display for Misbehaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Misbehaved {}

/// The download was stopped by its shutdown signal before every piece was
/// done. What was verified is on disk for the next run to resume from.
#[derive(Debug)]
pub struct Interrupted {
    /// Pieces still missing.
    pub remaining: usize,
    /// Pieces in the torrent.
    pub pieces: usize,
}

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "download interrupted with {} of {} pieces missing",
            self.remaining, self.pieces
        )
    }
}

impl std::error::Error for Interrupted {}

/// The data was checked and the run ended there, as `Recheck::ThenPause`
/// asks. The resume file is saved.
#[derive(Debug)]
pub struct CheckedOnly;

impl std::fmt::Display for CheckedOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data checked")
    }
}

impl std::error::Error for CheckedOnly {}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_limits_stop_at_whichever_comes_first() {
        let minute = Duration::from_secs(60);
        let none = SeedLimits::default();
        assert!(!none.enabled());
        let ratio = SeedLimits {
            ratio: 1.5,
            time: Duration::ZERO,
        };
        assert!(ratio.enabled());
        assert!(!ratio.reached(149, 100, 1000 * minute));
        assert!(ratio.reached(150, 100, Duration::ZERO));
        let time = SeedLimits {
            ratio: 0.0,
            time: minute,
        };
        assert!(time.enabled());
        assert!(!time.reached(u64::MAX, 100, minute / 2));
        assert!(time.reached(0, 100, minute));
        let both = SeedLimits {
            ratio: 2.0,
            time: minute,
        };
        assert!(both.reached(200, 100, Duration::ZERO));
        assert!(both.reached(0, 100, minute));
        assert!(!both.reached(199, 100, minute / 2));
    }

    #[test]
    fn the_selection_comes_from_the_options_then_the_picks() {
        use crate::selection::Priority::{Normal, Skip};
        let picks = selection::FilePicks::parse("2").unwrap();
        let options = StartOptions {
            picks: Some(picks),
            ..StartOptions::default()
        };
        assert_eq!(
            initial_selection(&options, None, 3).priorities(),
            [Skip, Normal, Skip]
        );
        // Picks past the last file leave nothing to download, so all of it.
        let none = StartOptions {
            picks: Some(selection::FilePicks::parse("7").unwrap()),
            ..StartOptions::default()
        };
        assert!(initial_selection(&none, None, 3).is_empty());
        // An explicit selection wins.
        let both = StartOptions {
            selection: Some(FileSelection::all_normal(3)),
            ..options
        };
        assert!(initial_selection(&both, None, 3).is_all_normal());
    }
}
