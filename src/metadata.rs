//! Metadata exchange (BEP 9): the `ut_metadata` extension, through which
//! peers send each other a torrent's info dictionary in 16 KiB pieces, so
//! that a magnet link, which only gives the info hash, is enough to
//! download.
//!
//! Each message is a bencoded dictionary with a `msg_type` (0 request, 1
//! data, 2 reject) and a `piece` index; a data message also carries the
//! metadata's `total_size`, and the piece itself after the dictionary. A
//! peer gives the metadata's size as `metadata_size` in its extension
//! handshake.
//!
//! `MetadataFetch` fetches the metadata from several peers at once and
//! checks it against the info hash; `serve` answers peers that ask us.

use crate::announcer::{Announcer, Transferred};
use crate::bencode;
use crate::client::{ConnectionSlots, SlotPermit};
use crate::dht::DhtSearch;
use crate::download::{self, ConnectedPeers, Misbehaved};
use crate::extension::{Advertised, UT_METADATA, UT_METADATA_ID};
use crate::listener::Registration;
use crate::magnet::FetchInterrupted;
use crate::peer::{Incoming, PeerConnection, PeerMessage, ProtocolViolation};
use crate::peer_pool::PeerPool;
use anyhow::{Context, Result, anyhow, bail};
use serde_bencode::value::Value;
use sha1::Digest;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{Instrument, debug, info, warn};

/// Length of every metadata piece but the last.
pub const PIECE_LEN: usize = 16 * 1024;
/// Largest metadata we fetch. An info dictionary holds 20 bytes per piece,
/// so this is room for over 800,000 pieces, far more than any sane torrent
/// has; a peer claiming more is not asked.
pub const MAX_METADATA_SIZE: usize = 16 * 1024 * 1024;
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

/// A `ut_metadata` message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Asks for metadata piece `piece`.
    Request { piece: u32 },
    /// Metadata piece `piece`, of metadata `total_size` bytes long.
    Data {
        piece: u32,
        total_size: Option<u64>,
        data: Vec<u8>,
    },
    /// The sender will not send piece `piece`, usually because it does not
    /// have the metadata.
    Reject { piece: u32 },
}

impl Message {
    /// Serializes the message into an extended message payload.
    pub fn encode(&self) -> Vec<u8> {
        // Keys in sorted order, as bencode requires.
        match self {
            Message::Request { piece } => format!("d8:msg_typei0e5:piecei{piece}ee").into_bytes(),
            Message::Data {
                piece,
                total_size,
                data,
            } => {
                let mut payload = format!("d8:msg_typei1e5:piecei{piece}e").into_bytes();
                if let Some(total_size) = total_size {
                    payload.extend_from_slice(format!("10:total_sizei{total_size}e").as_bytes());
                }
                payload.push(b'e');
                payload.extend_from_slice(data);
                payload
            }
            Message::Reject { piece } => format!("d8:msg_typei2e5:piecei{piece}ee").into_bytes(),
        }
    }

    /// Parses an extended message payload. Parsing is lenient, as for the
    /// extension handshake: a message of an unknown type, or without a valid
    /// `piece`, gives `None` and is to be ignored (BEP 9 asks for unknown
    /// types to be ignored), and so is a data message's ill-typed
    /// `total_size`. Only a payload that does not start with a bencoded
    /// dictionary is an error.
    pub fn decode(payload: &[u8]) -> Result<Option<Self>> {
        // Scanned first because it limits the nesting depth, which
        // `serde_bencode` would follow until the stack overflows.
        let len = bencode::value_len(payload).context("ut_metadata message is not bencoded")?;
        let value: Value = serde_bencode::from_bytes(&payload[..len])
            .context("ut_metadata message is not bencoded")?;
        let Value::Dict(dict) = value else {
            bail!("ut_metadata message is not a dictionary");
        };
        let int = |key: &[u8]| match dict.get(key) {
            Some(Value::Int(n)) => Some(*n),
            _ => None,
        };
        let Some(piece) = int(b"piece").and_then(|piece| u32::try_from(piece).ok()) else {
            return Ok(None);
        };
        Ok(match int(b"msg_type") {
            Some(0) => Some(Message::Request { piece }),
            Some(1) => Some(Message::Data {
                piece,
                total_size: int(b"total_size").and_then(|size| u64::try_from(size).ok()),
                data: payload[len..].to_vec(),
            }),
            Some(2) => Some(Message::Reject { piece }),
            _ => None,
        })
    }
}

/// Our answer to a request for piece `piece` of the info dictionary
/// `metadata`: the piece, or a reject if we do not have the metadata or it
/// has no such piece.
pub fn answer(metadata: Option<&[u8]>, piece: u32) -> Message {
    let start = usize::try_from(piece)
        .ok()
        .and_then(|piece| piece.checked_mul(PIECE_LEN));
    match (metadata, start) {
        (Some(metadata), Some(start)) if start < metadata.len() => Message::Data {
            piece,
            total_size: Some(metadata.len() as u64),
            data: metadata[start..metadata.len().min(start + PIECE_LEN)].to_vec(),
        },
        _ => Message::Reject { piece },
    }
}

/// Handles a `ut_metadata` message from a peer of a download: a request is
/// answered from `metadata`, our info dictionary if we have it, and anything
/// else (such as a late reply to the fetch) is ignored. A payload that is
/// not a bencoded dictionary is a protocol violation.
pub(crate) async fn serve(
    conn: &mut PeerConnection,
    metadata: Option<&[u8]>,
    payload: &[u8],
) -> Result<()> {
    let message =
        Message::decode(payload).context(ProtocolViolation("malformed ut_metadata message"))?;
    let Some(Message::Request { piece }) = message else {
        return Ok(());
    };
    // Without an id for it in the peer's handshake there is no way to reply.
    if !peer_supports_metadata(conn) {
        return Ok(());
    }
    debug!(addr = %conn.addr, piece, "metadata requested");
    conn.send_extended(UT_METADATA, answer(metadata, piece).encode())
        .await
}

/// Whether the peer listed `ut_metadata` in its extension handshake.
fn peer_supports_metadata(conn: &PeerConnection) -> bool {
    conn.peer_extensions()
        .and_then(|handshake| handshake.id(UT_METADATA))
        .is_some()
}

/// A piece of metadata and the peer that sent it.
struct Received {
    data: Vec<u8>,
    from: SocketAddr,
}

/// What became of a metadata piece handed to `Assembly::add`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Added {
    /// Kept; more pieces are needed.
    Stored,
    /// Not asked for (any more), or a duplicate; dropped.
    Ignored,
    /// That was the last piece, and the metadata matches the info hash.
    Complete,
    /// That was the last piece, but the metadata does not match the info
    /// hash, so every piece was thrown away. `banned` is the peer that sent
    /// them all, if a single one did.
    Failed { banned: Option<SocketAddr> },
}

/// The metadata being put together from pieces sent by several peers, and
/// which peer has been asked for which piece.
///
/// Pieces from different peers are combined, so a copy that fails the hash
/// check cannot always be blamed on one peer. If it came from a single peer,
/// that peer is banned; if not, the pieces are thrown away and the fetch
/// goes on one peer at a time (`exclusive`): one peer is asked for every
/// piece, the others wait until it leaves or fails, and a bad copy is then
/// always one peer's.
pub(crate) struct Assembly {
    info_hash: [u8; 20],
    /// Bumped whenever the pieces are thrown away after a failed hash
    /// check; peers must then `join` again.
    generation: u64,
    /// The metadata's length, as the first peer to `join` says.
    size: Option<usize>,
    pieces: Vec<Option<Received>>,
    /// Pieces each peer has been asked for and not sent yet.
    requested: HashMap<SocketAddr, BTreeSet<u32>>,
    /// Whether every piece must come from the same peer.
    exclusive: bool,
    /// In exclusive mode, the peer being asked.
    owner: Option<SocketAddr>,
    /// Peers that sent a whole copy that failed the hash check.
    banned: HashSet<SocketAddr>,
    /// The metadata, once complete and verified.
    metadata: Option<Vec<u8>>,
}

impl Assembly {
    pub(crate) fn new(info_hash: [u8; 20]) -> Self {
        Self {
            info_hash,
            generation: 0,
            size: None,
            pieces: Vec::new(),
            requested: HashMap::new(),
            exclusive: false,
            owner: None,
            banned: HashSet::new(),
            metadata: None,
        }
    }

    /// Changes whenever the pieces are thrown away and peers must `join`
    /// again.
    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    /// The metadata, once complete and verified.
    pub(crate) fn metadata(&self) -> Option<&[u8]> {
        self.metadata.as_deref()
    }

    /// Whether `peer` sent a copy of the metadata that failed the hash
    /// check.
    pub(crate) fn is_banned(&self, peer: SocketAddr) -> bool {
        self.banned.contains(&peer)
    }

    /// Takes on a peer whose extension handshake says the metadata is
    /// `size` bytes long. The first peer to join sets the size; a peer that
    /// disagrees, or names a size over `MAX_METADATA_SIZE`, is not used.
    pub(crate) fn join(&mut self, size: u32) -> Result<()> {
        let size = size as usize;
        if size == 0 || size > MAX_METADATA_SIZE {
            bail!("peer says the metadata is {size} bytes, over the {MAX_METADATA_SIZE} we accept");
        }
        match self.size {
            Some(known) if known != size => {
                bail!("peer says the metadata is {size} bytes, others say {known}")
            }
            Some(_) => {}
            None => {
                self.size = Some(size);
                self.pieces = std::iter::repeat_with(|| None)
                    .take(size.div_ceil(PIECE_LEN))
                    .collect();
            }
        }
        Ok(())
    }

    /// Picks the next piece to ask `peer` for, if any: one nobody has sent
    /// and the peer was not asked for yet, asked of the fewest other peers.
    /// Once every missing piece is being asked for, they are asked of more
    /// peers too, and the first copy to arrive wins.
    pub(crate) fn claim(&mut self, peer: SocketAddr) -> Option<u32> {
        if self.metadata.is_some() || self.size.is_none() {
            return None;
        }
        if self.exclusive {
            match self.owner {
                Some(owner) if owner != peer => return None,
                Some(_) => {}
                None => self.owner = Some(peer),
            }
        }
        let mine = self.requested.get(&peer);
        let asked = |piece: u32| {
            self.requested
                .values()
                .filter(|requested| requested.contains(&piece))
                .count()
        };
        let piece = (0..self.pieces.len() as u32)
            .filter(|&piece| self.pieces[piece as usize].is_none())
            .filter(|piece| !mine.is_some_and(|mine| mine.contains(piece)))
            .min_by_key(|&piece| asked(piece))?;
        self.requested.entry(peer).or_default().insert(piece);
        Some(piece)
    }

    /// Pieces `peer` has been asked for and not sent yet.
    pub(crate) fn outstanding(&self, peer: SocketAddr) -> usize {
        self.requested.get(&peer).map_or(0, BTreeSet::len)
    }

    /// Takes a piece `peer` sent. Pieces it was not asked for are ignored,
    /// but one of the wrong length or with the wrong `total_size` is a
    /// `Misbehaved` error. With the last piece, the metadata is checked
    /// against the info hash.
    pub(crate) fn add(
        &mut self,
        peer: SocketAddr,
        piece: u32,
        total_size: Option<u64>,
        data: Vec<u8>,
    ) -> Result<Added> {
        let Some(size) = self.size else {
            return Ok(Added::Ignored);
        };
        let asked = self
            .requested
            .get_mut(&peer)
            .is_some_and(|requested| requested.remove(&piece));
        if !asked {
            return Ok(Added::Ignored);
        }
        if total_size.is_some_and(|total_size| total_size != size as u64) {
            bail!(Misbehaved(
                "peer sent a metadata piece with the wrong total size"
            ));
        }
        let start = piece as usize * PIECE_LEN;
        if data.len() != PIECE_LEN.min(size - start) {
            bail!(Misbehaved("peer sent a metadata piece of the wrong length"));
        }
        let slot = &mut self.pieces[piece as usize];
        if slot.is_some() {
            return Ok(Added::Ignored);
        }
        *slot = Some(Received { data, from: peer });
        if self.pieces.iter().any(Option::is_none) {
            return Ok(Added::Stored);
        }

        let received = || self.pieces.iter().flatten();
        let metadata: Vec<u8> = received()
            .flat_map(|piece| piece.data.iter().copied())
            .collect();
        if <[u8; 20]>::from(sha1::Sha1::digest(&metadata)) == self.info_hash {
            self.metadata = Some(metadata);
            self.requested.clear();
            return Ok(Added::Complete);
        }
        let senders: BTreeSet<SocketAddr> = received().map(|piece| piece.from).collect();
        let banned = match senders.first() {
            Some(&sender) if senders.len() == 1 => {
                self.banned.insert(sender);
                Some(sender)
            }
            _ => {
                self.exclusive = true;
                None
            }
        };
        self.generation += 1;
        self.size = None;
        self.pieces.clear();
        self.requested.clear();
        self.owner = None;
        Ok(Added::Failed { banned })
    }

    /// Forgets the pieces `peer` was asked for, once it is gone. In
    /// exclusive mode, if it was the peer being asked, the pieces it sent
    /// are thrown away too, and the next peer starts over.
    pub(crate) fn leave(&mut self, peer: SocketAddr) {
        self.requested.remove(&peer);
        if self.owner == Some(peer) && self.metadata.is_none() {
            self.owner = None;
            self.pieces.iter_mut().for_each(|piece| *piece = None);
        }
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    /// Metadata of `len` bytes, and its hash.
    fn metadata(len: usize) -> (Vec<u8>, [u8; 20]) {
        let data: Vec<u8> = (0..len).map(|i| (i * 7 % 256) as u8).collect();
        let hash = sha1::Sha1::digest(&data).into();
        (data, hash)
    }

    fn piece_of(data: &[u8], piece: u32) -> Vec<u8> {
        let start = piece as usize * PIECE_LEN;
        data[start..data.len().min(start + PIECE_LEN)].to_vec()
    }

    #[test]
    fn messages_roundtrip() {
        let messages = [
            Message::Request { piece: 0 },
            Message::Request { piece: u32::MAX },
            Message::Reject { piece: 3 },
            Message::Data {
                piece: 1,
                total_size: Some(31_337),
                data: b"d1:ae\x00\xff".to_vec(),
            },
            Message::Data {
                piece: 0,
                total_size: None,
                data: Vec::new(),
            },
        ];
        for message in messages {
            let payload = message.encode();
            assert_eq!(Message::decode(&payload).unwrap(), Some(message));
        }
        assert_eq!(
            Message::Request { piece: 2 }.encode(),
            b"d8:msg_typei0e5:piecei2ee"
        );
        assert_eq!(
            Message::Data {
                piece: 0,
                total_size: Some(3),
                data: b"abc".to_vec()
            }
            .encode(),
            b"d8:msg_typei1e5:piecei0e10:total_sizei3eeabc"
        );
    }

    #[test]
    fn decoding_is_lenient_but_wants_a_dictionary() {
        // Unknown types and missing or bad pieces are ignored.
        let ignored: &[&[u8]] = &[
            b"d8:msg_typei3e5:piecei0ee",
            b"d8:msg_typei0ee",
            b"d5:piecei0ee",
            b"d8:msg_typei0e5:piecei-1ee",
            b"d8:msg_typei0e5:piecei4294967296ee",
            b"d8:msg_type1:05:piecei0ee",
        ];
        for payload in ignored {
            assert_eq!(Message::decode(payload).unwrap(), None, "{payload:?}");
        }
        // Extra keys, and a bad `total_size`, are ignored too.
        let message = Message::decode(b"d8:msg_typei1e5:piecei2e10:total_size1:x1:zi0eedata");
        assert_eq!(
            message.unwrap(),
            Some(Message::Data {
                piece: 2,
                total_size: None,
                data: b"data".to_vec()
            })
        );
        let errors: &[&[u8]] = &[b"", b"i1e", b"le", b"d8:msg_type", b"\xff"];
        for payload in errors {
            assert!(Message::decode(payload).is_err(), "accepted {payload:?}");
        }
    }

    #[test]
    fn answers_requests_from_our_metadata() {
        let (data, _) = metadata(PIECE_LEN * 2 + 100);
        assert_eq!(
            answer(Some(&data), 0),
            Message::Data {
                piece: 0,
                total_size: Some(data.len() as u64),
                data: data[..PIECE_LEN].to_vec()
            }
        );
        let Message::Data { data: last, .. } = answer(Some(&data), 2) else {
            panic!("no data for the last piece");
        };
        assert_eq!(last, data[PIECE_LEN * 2..]);
        assert_eq!(answer(Some(&data), 3), Message::Reject { piece: 3 });
        assert_eq!(
            answer(Some(&data), u32::MAX),
            Message::Reject { piece: u32::MAX }
        );
        assert_eq!(answer(None, 0), Message::Reject { piece: 0 });
    }

    #[test]
    fn assembles_pieces_from_several_peers() {
        let (data, hash) = metadata(PIECE_LEN * 2 + 1);
        let size = data.len() as u32;
        let mut assembly = Assembly::new(hash);
        assert_eq!(assembly.claim(peer(1)), None, "size unknown");
        assembly.join(size).unwrap();
        assembly.join(size).unwrap();
        assert!(assembly.join(size + 1).is_err());

        // Each peer gets the pieces fewest others were asked for.
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assert_eq!(assembly.claim(peer(2)), Some(1));
        assert_eq!(assembly.claim(peer(2)), Some(2));
        assert_eq!(assembly.claim(peer(1)), Some(1));
        assert_eq!(assembly.claim(peer(1)), Some(2));
        assert_eq!(assembly.claim(peer(1)), None);
        assert_eq!(assembly.outstanding(peer(1)), 3);

        let add = |assembly: &mut Assembly, from, piece| {
            assembly.add(peer(from), piece, Some(size.into()), piece_of(&data, piece))
        };
        assert_eq!(add(&mut assembly, 1, 0).unwrap(), Added::Stored);
        assert_eq!(
            add(&mut assembly, 3, 1).unwrap(),
            Added::Ignored,
            "not asked"
        );
        assert_eq!(add(&mut assembly, 1, 1).unwrap(), Added::Stored);
        assert_eq!(
            add(&mut assembly, 2, 1).unwrap(),
            Added::Ignored,
            "duplicate"
        );
        assert_eq!(assembly.claim(peer(3)), Some(2));
        assert_eq!(add(&mut assembly, 2, 2).unwrap(), Added::Complete);
        assert_eq!(assembly.metadata(), Some(&data[..]));
        assert_eq!(assembly.claim(peer(3)), None);
    }

    #[test]
    fn rejects_bad_sizes_and_lengths() {
        let (data, hash) = metadata(PIECE_LEN + 10);
        let size = data.len() as u32;
        let mut assembly = Assembly::new(hash);
        assert!(assembly.join(0).is_err());
        assert!(assembly.join(MAX_METADATA_SIZE as u32 + 1).is_err());
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assert_eq!(assembly.claim(peer(1)), Some(1));
        let err = assembly
            .add(peer(1), 0, Some(size as u64 + 1), piece_of(&data, 0))
            .unwrap_err();
        assert!(err.is::<Misbehaved>());
        let err = assembly
            .add(peer(1), 1, Some(size.into()), piece_of(&data, 0))
            .unwrap_err();
        assert!(err.is::<Misbehaved>());
    }

    #[test]
    fn bans_the_peer_behind_a_bad_copy() {
        let (data, hash) = metadata(PIECE_LEN * 2);
        let size = data.len() as u32;
        let mut assembly = Assembly::new(hash);
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assert_eq!(assembly.claim(peer(1)), Some(1));
        let mut bad = piece_of(&data, 1);
        bad[0] ^= 1;
        let generation = assembly.generation();
        assert_eq!(
            assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap(),
            Added::Stored
        );
        assert_eq!(
            assembly.add(peer(1), 1, None, bad).unwrap(),
            Added::Failed {
                banned: Some(peer(1))
            }
        );
        assert!(assembly.is_banned(peer(1)));
        assert_ne!(assembly.generation(), generation);
        // Peers join again, and pieces from before are void.
        assert_eq!(assembly.claim(peer(2)), None);
        assert_eq!(
            assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap(),
            Added::Ignored
        );
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(2)), Some(0));
    }

    #[test]
    fn goes_one_peer_at_a_time_after_a_mixed_copy_fails() {
        let (data, hash) = metadata(PIECE_LEN * 2);
        let size = data.len() as u32;
        let mut bad = piece_of(&data, 1);
        bad[0] ^= 1;
        let mut assembly = Assembly::new(hash);
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assert_eq!(assembly.claim(peer(2)), Some(1));
        assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap();
        // Peer 2 lied, but which of the two did cannot be told.
        assert_eq!(
            assembly.add(peer(2), 1, None, bad.clone()).unwrap(),
            Added::Failed { banned: None }
        );
        assert!(!assembly.is_banned(peer(1)) && !assembly.is_banned(peer(2)));

        // From now on, a whole copy comes from one peer.
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(2)), Some(0));
        assert_eq!(assembly.claim(peer(1)), None);
        assert_eq!(assembly.claim(peer(2)), Some(1));
        assembly.add(peer(2), 0, None, piece_of(&data, 0)).unwrap();
        assert_eq!(
            assembly.add(peer(2), 1, None, bad).unwrap(),
            Added::Failed {
                banned: Some(peer(2))
            }
        );

        // Peer 1 takes over; when it leaves halfway, peer 3 starts over.
        assembly.join(size).unwrap();
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assembly.add(peer(1), 0, None, piece_of(&data, 0)).unwrap();
        assembly.leave(peer(1));
        assert_eq!(assembly.claim(peer(3)), Some(0));
        assert_eq!(assembly.claim(peer(3)), Some(1));
        assembly.add(peer(3), 1, None, piece_of(&data, 1)).unwrap();
        assert_eq!(
            assembly.add(peer(3), 0, None, piece_of(&data, 0)).unwrap(),
            Added::Complete
        );
        assert_eq!(assembly.metadata(), Some(&data[..]));
    }

    #[test]
    fn a_leaving_peer_frees_its_pieces() {
        let (data, hash) = metadata(PIECE_LEN);
        let mut assembly = Assembly::new(hash);
        assembly.join(data.len() as u32).unwrap();
        assert_eq!(assembly.claim(peer(1)), Some(0));
        assembly.leave(peer(1));
        assert_eq!(assembly.outstanding(peer(1)), 0);
        // Not asked for any more, so a late reply is ignored.
        assert_eq!(
            assembly.add(peer(1), 0, None, data.clone()).unwrap(),
            Added::Ignored
        );
        assert_eq!(assembly.claim(peer(2)), Some(0));
        assert_eq!(
            assembly.add(peer(2), 0, None, data).unwrap(),
            Added::Complete
        );
    }
}
