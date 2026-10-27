use super::assembly::{Added, Assembly};
use super::{Message, peer_supports_metadata};
use crate::client::{ConnectionSlots, SlotPermit};
use crate::dht::DhtSearch;
use crate::download::peer_pool::PeerPool;
use crate::download::{self, ConnectedPeers, Misbehaved};
use crate::listener::Registration;
use crate::magnet::FetchInterrupted;
use crate::peer::extension::{Advertised, UT_METADATA, UT_METADATA_ID};
use crate::peer::{Incoming, PeerConnection, PeerMessage, ProtocolViolation};
use crate::tracker::announcer::{Announcer, Transferred};
use anyhow::{Context, Result, anyhow, bail};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

/// Metadata pieces asked of one peer at a time.
const PIPELINE: usize = 4;
/// How long a peer has to send its extension handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a peer has to answer, while we wait for a metadata piece.
const REPLY_TIMEOUT: Duration = Duration::from_secs(20);
/// How often a waiting worker wakes to send a keep-alive if one is due.
const IDLE_POLL: Duration = Duration::from_secs(10);
/// How long workers get, once the metadata is complete, to hand over their
/// connections to the download before being aborted.
const HANDOVER_GRACE: Duration = Duration::from_secs(2);
/// DHT lookups awaited once we run out of peers, before giving up.
const DHT_LOOKUPS_WHEN_OUT: u8 = 2;

/// What the workers of a metadata fetch share.
struct Shared {
    assembly: Mutex<Assembly>,
    /// Signalled when the metadata is complete, when pieces are thrown away,
    /// and when a peer leaves.
    changed: Notify,
    connected: ConnectedPeers,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Assembly> {
        self.assembly.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Fetches a torrent's metadata from its peers, for a magnet link.
pub(crate) struct MetadataFetch<'a> {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    /// Most peers connected at once.
    pub max_peers: usize,
    /// Every connection holds one of these, which the download takes over
    /// along with the connection.
    pub slots: ConnectionSlots,
    /// The peers that connect to us, which may send the metadata too.
    pub incoming: &'a mut Registration,
    /// Re-announced to on its interval, for more peers.
    pub announcer: &'a mut Option<Announcer>,
    /// Searched for more peers.
    pub dht: Option<DhtSearch>,
}

/// A fetched info dictionary, and the peers to carry on downloading from.
pub(crate) struct Fetched {
    /// The info dictionary, which matches the info hash.
    pub metadata: Vec<u8>,
    /// Open connections, each with whether the peer connected to us and the
    /// connection slot it holds. The number of pieces is not set on them yet
    /// (see `PeerConnection::set_piece_count`).
    pub connections: Vec<(PeerConnection, bool, SlotPermit)>,
    /// Every peer known, with the connected and banned ones marked.
    pub pool: PeerPool,
    /// Addresses of incoming peers that misbehaved.
    pub banned_ips: HashSet<IpAddr>,
}

/// How a metadata worker ended: with the connection, ready for the
/// download, or with an error.
struct WorkerExit {
    addr: SocketAddr,
    inbound: bool,
    /// The connection and the slot it holds, ready for the download.
    result: Result<(PeerConnection, SlotPermit)>,
}

/// Peers we have connected to or given up on during the fetch.
struct Peers {
    pool: PeerPool,
    banned_ips: HashSet<IpAddr>,
    connections: Vec<(PeerConnection, bool, SlotPermit)>,
    /// Peers we are connecting to, which go back in the pool if the fetch
    /// ends before they are done.
    outbound: HashSet<SocketAddr>,
}

impl Peers {
    /// Records how a worker ended: its connection is kept for the download,
    /// and a peer that failed is retried later or, if it misbehaved,
    /// banned.
    fn record(&mut self, exit: WorkerExit) {
        let WorkerExit {
            addr,
            inbound,
            result,
        } = exit;
        if !inbound {
            self.outbound.remove(&addr);
        }
        match result {
            Ok((conn, permit)) => self.connections.push((conn, inbound, permit)),
            Err(e) if e.is::<Misbehaved>() || e.is::<ProtocolViolation>() => {
                warn!(%addr, "banning peer: {e:#}");
                if inbound {
                    self.banned_ips.insert(addr.ip());
                } else {
                    self.pool.ban(addr);
                }
            }
            Err(e) => {
                debug!(%addr, "peer dropped: {e:#}");
                if !inbound {
                    self.pool.failed(addr, false, Instant::now());
                }
            }
        }
    }
}

/// Answers still awaited from the peer sources asked for more peers once we
/// ran out.
struct Asked {
    tracker: bool,
    dht_lookups: u8,
}

impl MetadataFetch<'_> {
    /// Connects to peers from `pool` and the ones that connect to us, at
    /// most `max_peers` at once and while `slots` has one free for each, and
    /// fetches the metadata from those that
    /// have it, until it is complete and verified. Peers that cannot send it
    /// are dropped and retried later, as in a download; peers that send bad
    /// data are banned.
    ///
    /// When we run out of peers, the tracker and the DHT are asked for more
    /// once, as in a download, before giving up. A `FetchInterrupted` error
    /// if `shutdown` resolves first.
    pub(crate) async fn run(
        self,
        pool: PeerPool,
        shutdown: impl Future<Output = ()>,
    ) -> Result<Fetched> {
        let MetadataFetch {
            info_hash,
            peer_id,
            max_peers,
            slots,
            incoming,
            announcer,
            mut dht,
        } = self;
        let shared = Arc::new(Shared {
            assembly: Mutex::new(Assembly::new(info_hash)),
            changed: Notify::new(),
            connected: ConnectedPeers::default(),
        });
        let listen_port = incoming.port();
        let mut peers = Peers {
            pool,
            banned_ips: HashSet::new(),
            connections: Vec::new(),
            outbound: HashSet::new(),
        };
        let mut workers = JoinSet::new();
        let spawn = |workers: &mut JoinSet<WorkerExit>,
                     addr,
                     peer: Option<Incoming>,
                     permit: SlotPermit| {
            let shared = Arc::clone(&shared);
            let inbound = peer.is_some();
            let worker = async move {
                // Kept for the download if the worker succeeds, and given
                // back at once if it fails.
                let result = fetch_from_peer(&shared, info_hash, peer_id, addr, peer, listen_port)
                    .await
                    .map(|conn| (conn, permit));
                WorkerExit {
                    addr,
                    inbound,
                    result,
                }
            };
            workers.spawn(worker.in_current_span());
        };
        // Nothing is downloaded while fetching the metadata.
        let transferred = Transferred::default();
        let mut asked: Option<Asked> = None;
        let mut shutdown = std::pin::pin!(shutdown);
        loop {
            if shared.lock().metadata().is_some() {
                break;
            }
            // Made before a slot is looked for, so that one freed meanwhile
            // is not missed.
            let mut freed = std::pin::pin!(slots.freed());
            while workers.len() < max_peers {
                let Some(addr) = peers.pool.next(Instant::now()) else {
                    break;
                };
                let Some(permit) = slots.try_acquire() else {
                    peers.pool.requeue(addr);
                    break;
                };
                asked = None;
                peers.outbound.insert(addr);
                spawn(&mut workers, addr, None, permit);
            }
            let retry_at = peers
                .pool
                .next_retry()
                .filter(|_| workers.len() < max_peers);
            // Peers still waiting for a slot are not running out.
            if workers.is_empty() && retry_at.is_none() && peers.pool.ready_len() == 0 {
                match &asked {
                    None if announcer.is_some() || dht.is_some() => {
                        info!("out of peers, asking for more");
                        if let Some(announcer) = announcer.as_mut() {
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
                    _ => bail!("no peer sent the torrent's metadata"),
                }
            }
            tokio::select! {
                Some(joined) = workers.join_next() => match joined {
                    Ok(exit) => peers.record(exit),
                    Err(e) => warn!("peer task failed: {e}"),
                },
                peer = incoming.next() => {
                    let addr = peer.addr;
                    if workers.len() >= max_peers {
                        debug!(%addr, "turning away incoming peer: too many peers");
                    } else if peers.banned_ips.contains(&addr.ip()) {
                        debug!(%addr, "turning away banned peer");
                    } else if let Some(permit) = slots.try_acquire() {
                        asked = None;
                        spawn(&mut workers, addr, Some(peer), permit);
                    } else {
                        debug!(%addr, "turning away incoming peer: no free connection slot");
                    }
                }
                () = &mut freed => {}
                () = download::sleep_until_some(retry_at) => {}
                found = download::reannounce(announcer, &transferred) => {
                    peers.pool.add(found);
                    if let Some(asked) = &mut asked {
                        asked.tracker = false;
                    }
                }
                found = download::dht_lookup(&mut dht) => {
                    peers.pool.add(found);
                    if let Some(asked) = &mut asked {
                        asked.dht_lookups = asked.dht_lookups.saturating_sub(1);
                    }
                }
                () = &mut shutdown => {
                    workers.shutdown().await;
                    bail!(FetchInterrupted);
                }
            }
        }

        // Every worker has been woken, and hands over its connection.
        // Those still connecting are cut short, and their peers go back in
        // the pool for the download to connect to.
        let handover = async {
            while let Some(joined) = workers.join_next().await {
                match joined {
                    Ok(exit) => peers.record(exit),
                    Err(e) => warn!("peer task failed: {e}"),
                }
            }
        };
        if tokio::time::timeout(HANDOVER_GRACE, handover)
            .await
            .is_err()
        {
            workers.shutdown().await;
        }
        for addr in peers.outbound.drain() {
            peers.pool.requeue(addr);
        }
        let metadata = shared
            .lock()
            .metadata()
            .map(<[u8]>::to_vec)
            .ok_or_else(|| anyhow!("metadata went missing"))?;
        info!(
            size = metadata.len(),
            peers = peers.connections.len(),
            "metadata complete"
        );
        Ok(Fetched {
            metadata,
            connections: peers.connections,
            pool: peers.pool,
            banned_ips: peers.banned_ips,
        })
    }
}

/// Connects to `addr`, or, given the connection the peer opened to us,
/// answers its handshake, and fetches metadata pieces from it until the
/// metadata is complete. Returns the connection then, for the download to
/// carry on with.
async fn fetch_from_peer(
    shared: &Shared,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    addr: SocketAddr,
    peer: Option<Incoming>,
    listen_port: Option<u16>,
) -> Result<PeerConnection> {
    let inbound = peer.is_some();
    // Whether the torrent is private is not known yet, but magnet links for
    // private torrents are not a thing in practice.
    let advertised = Advertised {
        listen_port,
        ..Advertised::default()
    };
    // With 0 pieces for now; their number comes with the metadata.
    let mut conn = match peer {
        Some(peer) => PeerConnection::answer(peer, peer_id, 0, advertised).await?,
        None => PeerConnection::connect(addr, info_hash, peer_id, 0, advertised).await?,
    };
    conn.defer_pieces();
    if conn.peer_id == peer_id {
        // A tracker listed our own address.
        bail!(Misbehaved("connected to ourselves"));
    }
    let Some(_connected) = shared.connected.register(conn.peer_id) else {
        bail!("already connected to this peer");
    };
    // We have no piece yet, which the fast extension (BEP 6) wants said
    // first thing. Pieces we turn out to have once the metadata is known
    // follow as Have messages.
    if conn.supports_fast() {
        conn.send(&PeerMessage::HaveNone).await?;
    }
    debug!(%addr, inbound, "connected, fetching metadata");
    let result = fetch_pieces(shared, &mut conn).await;
    shared.lock().leave(addr);
    shared.changed.notify_waiters();
    result.map(|()| conn)
}

/// The metadata size the peer gave in its extension handshake, or `None`
/// until the handshake comes. Fails if the peer cannot send the metadata.
fn peer_metadata_size(conn: &PeerConnection) -> Result<Option<u32>> {
    if !conn.supports_extensions() {
        bail!("peer does not support the extension protocol");
    }
    let Some(handshake) = conn.peer_extensions() else {
        return Ok(None);
    };
    if handshake.id(UT_METADATA).is_none() {
        bail!("peer does not support ut_metadata");
    }
    match handshake.metadata_size {
        Some(size) => Ok(Some(size)),
        None => bail!("peer does not have the metadata"),
    }
}

/// Asks the peer for metadata pieces, a few at a time, and hands what it
/// sends to the shared assembly, until the metadata is complete (from
/// whichever peers). Fails if the peer cannot or will not send it, is too
/// slow, or sends bad data.
async fn fetch_pieces(shared: &Shared, conn: &mut PeerConnection) -> Result<()> {
    let addr = conn.addr;
    let handshake_deadline = Instant::now() + HANDSHAKE_TIMEOUT;
    // The assembly generation this peer joined.
    let mut joined = None;
    // Set while we wait for pieces from this peer.
    let mut reply_deadline = None;
    loop {
        // Registered before anything is checked, so that a change from here
        // on still wakes the wait below.
        let mut changed = std::pin::pin!(shared.changed.notified());
        changed.as_mut().enable();

        let size = peer_metadata_size(conn)?;
        let requests = {
            let mut assembly = shared.lock();
            if assembly.metadata().is_some() {
                return Ok(());
            }
            if assembly.is_banned(addr) {
                bail!(Misbehaved("peer sent metadata that failed the hash check"));
            }
            let mut requests = Vec::new();
            if let Some(size) = size {
                if joined != Some(assembly.generation()) {
                    assembly.join(size)?;
                    joined = Some(assembly.generation());
                }
                while assembly.outstanding(addr) < PIPELINE {
                    let Some(piece) = assembly.claim(addr) else {
                        break;
                    };
                    requests.push(piece);
                }
                if assembly.outstanding(addr) == 0 {
                    reply_deadline = None;
                }
            }
            requests
        };
        if let Some(id) = conn
            .peer_extensions()
            .and_then(|handshake| handshake.id(UT_METADATA))
            .filter(|_| !requests.is_empty())
        {
            debug!(%addr, ?requests, "requesting metadata");
            reply_deadline.get_or_insert(Instant::now() + REPLY_TIMEOUT);
            let messages: Vec<PeerMessage> = requests
                .into_iter()
                .map(|piece| PeerMessage::Extended {
                    id,
                    payload: Message::Request { piece }.encode(),
                })
                .collect();
            conn.send_all(&messages).await?;
        }

        conn.keep_alive().await?;
        let deadline = match size {
            None => Some(handshake_deadline),
            Some(_) => reply_deadline,
        };
        // `receive_untimed` is cancel-safe, so losing a race loses no data.
        let message = tokio::select! {
            () = &mut changed => continue,
            message = conn.receive_untimed() => message?,
            () = download::sleep_until_some(deadline) => match size {
                None => bail!("peer sent no extension handshake"),
                Some(_) => bail!("peer did not send the metadata pieces we asked for"),
            },
            () = tokio::time::sleep(IDLE_POLL) => continue,
        };
        let PeerMessage::Extended {
            id: UT_METADATA_ID,
            payload,
        } = message
        else {
            continue;
        };
        let message = Message::decode(&payload)
            .context(ProtocolViolation("malformed ut_metadata message"))?;
        match message {
            Some(Message::Data {
                piece,
                total_size,
                data,
            }) => {
                let added = shared.lock().add(addr, piece, total_size, data)?;
                match added {
                    Added::Stored => reply_deadline = Some(Instant::now() + REPLY_TIMEOUT),
                    Added::Ignored => {}
                    Added::Complete => {
                        shared.changed.notify_waiters();
                        return Ok(());
                    }
                    Added::Failed { banned } => {
                        warn!(%addr, "metadata failed the hash check");
                        shared.changed.notify_waiters();
                        if banned == Some(addr) {
                            bail!(Misbehaved("peer sent metadata that failed the hash check"));
                        }
                    }
                }
            }
            Some(Message::Reject { piece }) => {
                bail!("peer rejected our request for metadata piece {piece}")
            }
            // We have no metadata to send yet.
            Some(Message::Request { piece }) if peer_supports_metadata(conn) => {
                conn.send_extended(UT_METADATA, Message::Reject { piece }.encode())
                    .await?;
            }
            Some(Message::Request { .. }) | None => {}
        }
    }
}
