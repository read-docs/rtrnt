use super::choker::Choker;
use super::peer_pool::PeerPool;
use crate::client::{ConnectionSlots, SlotPermit};
use crate::dht::DhtSearch;
use crate::listener::Registration;
use crate::peer::pex::PeerExchange;
use crate::peer::{self, Incoming, PeerConnection};
use crate::stats::{
    PeerSource, PieceSnapshot, PieceSource, TorrentState,
};
use crate::tracker::announcer::{Announcer, Transferred};
use anyhow::{Result, bail};
use indicatif::HumanBytes;
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

use super::check::verified_bytes;
use super::progress::Progress;
use super::queue::PieceQueue;
use super::worker::peer_worker;
use super::{Interrupted, Misbehaved, Session};

/// How long workers get to wind down, once the download is complete or
/// interrupted, before being aborted.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// How often a seed limit is checked, and the upload count on the progress
/// bar brought up to date, while seeding.
const SEED_CHECK: Duration = Duration::from_secs(1);
/// DHT lookups awaited once we run out of peers, before giving up. One may
/// have started before we ran out.
const DHT_LOOKUPS_WHEN_OUT: u8 = 2;
/// Peers learned through peer exchange join the pool only while fewer than
/// this many wait there, so peers that flood us with addresses cannot grow
/// it without bound.
const MAX_PEX_BACKLOG: usize = 500;
/// How often the resume file is saved while pieces complete.
const RESUME_SAVE_INTERVAL: Duration = Duration::from_secs(30);

/// The peers a download starts with.
#[derive(Default)]
pub(crate) struct StartPeers {
    /// Peers to connect to, with those already connected or banned marked.
    pub(crate) pool: PeerPool,
    /// Addresses of incoming peers that misbehaved.
    pub(crate) banned_ips: HashSet<IpAddr>,
    /// Connections already open, such as those the metadata of a magnet
    /// link came over, each with whether the peer connected to us and the
    /// connection slot it holds.
    pub(crate) connections: Vec<(PeerConnection, bool, SlotPermit)>,
}

/// How many peers a download may be connected to.
pub(crate) struct Limits {
    /// Most peers of this torrent at once.
    pub(crate) max_peers: usize,
    /// Every worker holds one of these while it runs, and no peer is
    /// connected to or taken on while none is free; a slot freed in any
    /// torrent that shares them gets us trying again.
    pub(crate) slots: ConnectionSlots,
}

/// `download_from_peers`, starting with the peers in `start`, some of them
/// perhaps already connected, and taking the peers that connect to us from
/// `incoming`, within `limits`.
pub(crate) async fn download_with(
    session: Arc<Session>,
    have: &[bool],
    start: StartPeers,
    limits: Limits,
    mut incoming: Registration,
    mut announcer: Option<Announcer>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let torrent = &session.torrent;
    let num_pieces = torrent.num_pieces();
    if have.len() != num_pieces {
        bail!("have {} piece flags for {num_pieces} pieces", have.len());
    }
    let Limits { max_peers, slots } = limits;
    let seed = session.seed;
    // Private torrents (BEP 27) get their peers from trackers only.
    let (pex, mut pex_peers) = PeerExchange::new(!torrent.is_private());
    let stats = &session.stats;
    let swarm = Arc::new(Swarm {
        queue: PieceQueue::new(have).seeding(seed.enabled()),
        progress: Progress::new(
            &session.bars,
            &torrent.info.name,
            torrent.length(),
            verified_bytes(torrent, have)?,
            Arc::clone(stats.transferred()),
        ),
        connected: ConnectedPeers::default(),
        choker: Choker::default(),
        pex,
    });
    let Swarm {
        queue,
        progress,
        choker,
        ..
    } = &*swarm;
    let source: Arc<dyn PieceSource> = swarm.clone();
    stats.set_piece_source(Arc::downgrade(&source));
    stats.set_state(if queue.is_done() {
        TorrentState::Seeding
    } else {
        TorrentState::Downloading
    });
    // Incoming peers have no address we could ban in the pool.
    let StartPeers {
        mut pool,
        mut banned_ips,
        connections,
    } = start;

    // Told to peers in the extension handshake.
    let listen_port = incoming.port();

    let mut workers = JoinSet::new();
    // Starts a worker that connects to `addr`, or, given the socket the peer
    // connected to us on, answers its handshake, or carries on over a
    // connection already open. The worker holds `permit` until it exits.
    let spawn = |workers: &mut JoinSet<WorkerExit>, addr, link: Link, permit: SlotPermit| {
        let session = Arc::clone(&session);
        let swarm = Arc::clone(&swarm);
        let inbound = link.is_inbound();
        let worker = async move {
            let _permit = permit;
            let mut pieces = 0;
            let result = peer_worker(&session, &swarm, addr, link, listen_port, &mut pieces).await;
            WorkerExit {
                addr,
                inbound,
                pieces,
                result,
            }
        };
        workers.spawn(worker.in_current_span());
    };
    for (conn, inbound, permit) in connections {
        spawn(
            &mut workers,
            conn.addr,
            Link::Open {
                conn: Box::new(conn),
                inbound,
            },
            permit,
        );
    }
    let mut dht = session
        .dht
        .clone()
        .map(|dht| DhtSearch::new(dht, session.info_hash, listen_port));
    // Set once we ran out of peers and asked for more, counting the answers
    // still awaited; cleared when a new connection is started.
    let mut asked: Option<Asked> = None;
    // Upload slots go by download rate, and by upload rate once we seed.
    let mut choking = std::pin::pin!(choker.run(|| queue.is_done()));
    let mut shutdown = std::pin::pin!(shutdown);
    let mut interrupted = false;
    // Whether the download finishes this session, rather than having been
    // complete on disk.
    let downloading = !queue.is_done();
    // When we finished downloading and started seeding.
    let mut seeding_since = None;
    // Pieces completed this session as of the last save of the resume file,
    // and when the next one is due.
    let mut saved = 0;
    let mut next_save = Instant::now() + RESUME_SAVE_INTERVAL;
    loop {
        if queue.is_done() {
            let since = match seeding_since {
                Some(since) => since,
                None => {
                    if let Some(announcer) = announcer.as_mut().filter(|_| downloading) {
                        announcer.complete();
                    }
                    if !seed.enabled() {
                        break;
                    }
                    info!(ratio = seed.ratio, time = ?seed.time, "download complete, seeding");
                    stats.set_state(TorrentState::Seeding);
                    *seeding_since.insert(Instant::now())
                }
            };
            let uploaded = progress.transferred.uploaded.load(Ordering::Relaxed);
            progress
                .bar
                .set_message(format!("seeding, {} up", HumanBytes(uploaded)));
            if seed.reached(uploaded, torrent.length(), since.elapsed()) {
                info!(uploaded, "seed limit reached");
                break;
            }
        }
        // Made before a slot is looked for, so that one freed meanwhile is
        // not missed.
        let mut freed = std::pin::pin!(slots.freed());
        while workers.len() < max_peers {
            let Some(addr) = pool.next(Instant::now()) else {
                break;
            };
            let Some(permit) = slots.try_acquire() else {
                pool.requeue(addr);
                break;
            };
            asked = None;
            spawn(&mut workers, addr, Link::Connect, permit);
        }

        let retry_at = pool.next_retry().filter(|_| workers.len() < max_peers);
        // Peers still waiting for a slot are not running out.
        if workers.is_empty()
            && retry_at.is_none()
            && pool.ready_len() == 0
            && seeding_since.is_none()
        {
            match &asked {
                None if announcer.is_some() || dht.is_some() => {
                    info!("out of peers, asking for more");
                    if let Some(announcer) = &mut announcer {
                        announcer.hurry();
                    }
                    if let Some(dht) = &mut dht {
                        dht.hurry();
                    }
                    asked = Some(Asked {
                        tracker: announcer.is_some(),
                        dht_lookups: if dht.is_some() {
                            DHT_LOOKUPS_WHEN_OUT
                        } else {
                            0
                        },
                    });
                }
                Some(asked) if asked.tracker || asked.dht_lookups > 0 => {}
                _ => break,
            }
        }
        tokio::select! {
            Some(joined) = workers.join_next() => {
                let WorkerExit { addr, inbound, pieces, result } = match joined {
                    Ok(exit) => exit,
                    Err(e) => {
                        warn!("peer task failed: {e}");
                        continue;
                    }
                };
                match result {
                    Ok(()) => debug!(%addr, "peer finished"),
                    Err(e) if e.is::<Misbehaved>() || e.is::<peer::ProtocolViolation>() => {
                        warn!(%addr, "banning peer: {e:#}");
                        if inbound {
                            banned_ips.insert(addr.ip());
                        } else {
                            pool.ban(addr);
                        }
                    }
                    Err(e) => {
                        debug!(%addr, pieces, "peer dropped: {e:#}");
                        // We cannot connect back to the port an incoming
                        // peer came from.
                        if !inbound {
                            pool.failed(addr, pieces > 0, Instant::now());
                        }
                    }
                }
            }
            peer = incoming.next() => {
                let addr = peer.addr;
                if workers.len() >= max_peers {
                    debug!(%addr, "turning away incoming peer: too many peers");
                } else if banned_ips.contains(&addr.ip()) {
                    debug!(%addr, "turning away banned peer");
                } else if let Some(permit) = slots.try_acquire() {
                    asked = None;
                    stats.add_source(PeerSource::Incoming, 1);
                    spawn(&mut workers, addr, Link::Accepted(peer), permit);
                } else {
                    debug!(%addr, "turning away incoming peer: no free connection slot");
                }
            }
            () = &mut freed => {}
            () = sleep_until_some(retry_at) => {}
            peers = reannounce(&mut announcer, &progress.transferred) => {
                stats.add_source(PeerSource::Tracker, peers.len());
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.tracker = false;
                }
            }
            peers = dht_lookup(&mut dht) => {
                stats.add_source(PeerSource::Dht, peers.len());
                pool.add(peers);
                if let Some(asked) = &mut asked {
                    asked.dht_lookups = asked.dht_lookups.saturating_sub(1);
                }
            }
            Some(peers) = pex_peers.recv() => {
                if pool.ready_len() < MAX_PEX_BACKLOG {
                    stats.add_source(PeerSource::Pex, peers.len());
                    pool.add(peers);
                }
            }
            () = &mut choking => {}
            () = queue.wait_for(|| queue.is_done()), if seeding_since.is_none() => {}
            () = tokio::time::sleep(SEED_CHECK), if seeding_since.is_some() => {}
            () = tokio::time::sleep_until(next_save), if session.resume.is_some() => {
                save_resume(&session, queue, &mut saved).await;
                next_save = Instant::now() + RESUME_SAVE_INTERVAL;
            }
            () = &mut shutdown => {
                info!("interrupted, stopping");
                interrupted = true;
                break;
            }
        }
    }

    // Workers still downloading notice completion or the closed queue and
    // exit (sending Cancel for duplicate endgame requests); ones still
    // connecting are cut short. A worker writing a piece finishes the write
    // first, and even if aborted, the write runs on to the end on the
    // blocking pool. New peers are refused meanwhile.
    drop(incoming);
    queue.close();
    let drain = async { while workers.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        workers.shutdown().await;
    }

    // Every finished write is in the files now.
    save_resume(&session, queue, &mut saved).await;
    stats.set_final_pieces(queue.snapshot());

    let remaining = queue.remaining();
    if remaining == 0 {
        progress.bar.finish();
    } else {
        // `finish` would fill the bar.
        progress.bar.abandon();
    }
    if let Some(announcer) = announcer {
        announcer.stop(&progress.transferred).await;
    }
    if remaining > 0 {
        if interrupted {
            bail!(Interrupted {
                remaining,
                pieces: num_pieces
            });
        }
        bail!("download incomplete: {remaining} of {num_pieces} pieces missing and no peers left");
    }
    Ok(())
}

/// Saves the torrent's resume file if a piece was completed since the last
/// save, which had `saved` completions.
async fn save_resume(session: &Session, queue: &PieceQueue, saved: &mut usize) {
    let Some(resume) = &session.resume else {
        return;
    };
    let completed = queue.completed_count();
    if completed == *saved {
        return;
    }
    // Before the files are looked at, so that a piece completed meanwhile
    // is at worst missing from the file.
    resume.save(&session.storage, queue.have_bitfield()).await;
    *saved = completed;
}

/// How a peer worker reaches its peer.
pub(super) enum Link {
    /// By connecting to it.
    Connect,
    /// Over a connection it opened to us, with its handshake read but not
    /// answered.
    Accepted(Incoming),
    /// Over a connection already open, such as one the metadata of a magnet
    /// link came over.
    Open {
        conn: Box<PeerConnection>,
        /// Whether the peer connected to us.
        inbound: bool,
    },
}

impl Link {
    /// Whether the peer connected to us, so we cannot connect back.
    pub(super) fn is_inbound(&self) -> bool {
        match self {
            Link::Connect => false,
            Link::Accepted(_) => true,
            Link::Open { inbound, .. } => *inbound,
        }
    }
}

/// Sleeps until `at`, or forever if it is `None`.
pub(crate) async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// What the peer workers of a download share.
pub(super) struct Swarm {
    pub(super) queue: PieceQueue,
    pub(super) progress: Progress,
    pub(super) connected: ConnectedPeers,
    /// Decides which peers may download from us.
    pub(super) choker: Choker,
    /// Tells peers about each other (BEP 11).
    pub(super) pex: PeerExchange,
}

impl PieceSource for Swarm {
    fn piece_snapshot(&self) -> PieceSnapshot {
        self.queue.snapshot()
    }
}

/// How a peer worker ended.
struct WorkerExit {
    addr: SocketAddr,
    /// Whether the peer connected to us.
    inbound: bool,
    /// Pieces the peer delivered first.
    pieces: usize,
    result: Result<()>,
}

/// Ids of the peers we are connected to, so that a peer we connect to and
/// that also connects to us is only downloaded from once.
#[derive(Default)]
pub(crate) struct ConnectedPeers(Mutex<HashSet<[u8; 20]>>);

impl ConnectedPeers {
    fn lock(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a connection to the peer with id `id` until the returned
    /// handle is dropped, or returns `None` if there already is one.
    pub(crate) fn register(&self, id: [u8; 20]) -> Option<Connected<'_>> {
        let new = self.lock().insert(id);
        new.then_some(Connected { peers: self, id })
    }
}

pub(crate) struct Connected<'a> {
    peers: &'a ConnectedPeers,
    id: [u8; 20],
}

impl Drop for Connected<'_> {
    fn drop(&mut self) {
        self.peers.lock().remove(&self.id);
    }
}

/// Runs the next re-announce and returns its peers, or waits forever
/// without a tracker. Cancel-safe.
pub(crate) async fn reannounce(
    announcer: &mut Option<Announcer>,
    transferred: &Transferred,
) -> Vec<SocketAddr> {
    match announcer {
        Some(announcer) => announcer.poll(transferred).await,
        None => std::future::pending().await,
    }
}

/// Runs the next DHT lookup and returns its peers, or waits forever without
/// a DHT. Cancel-safe.
pub(crate) async fn dht_lookup(search: &mut Option<DhtSearch>) -> Vec<SocketAddr> {
    match search {
        Some(search) => search.poll().await,
        None => std::future::pending().await,
    }
}

/// Answers still awaited from the peer sources asked for more peers once we
/// ran out.
struct Asked {
    tracker: bool,
    dht_lookups: u8,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_peer_is_connected_to_once_at_a_time() {
        let peers = ConnectedPeers::default();
        let a = peers.register([1; 20]).unwrap();
        assert!(peers.register([1; 20]).is_none());
        let _b = peers.register([2; 20]).unwrap();
        drop(a);
        assert!(peers.register([1; 20]).is_some());
    }
}
