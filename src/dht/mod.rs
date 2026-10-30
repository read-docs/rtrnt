//! Trackerless peer discovery over the BitTorrent DHT (BEP 5).
//!
//! [`Dht`] runs one DHT node on a UDP socket. It answers `ping`,
//! `find_node`, `get_peers` and `announce_peer`, keeps a routing table of
//! the nodes that answer it, and runs iterative lookups: `find_node` for
//! its own id to bootstrap, and `get_peers` for an info hash, after which
//! it announces us to the closest nodes. [`DhtSearch`] repeats the lookup
//! for one torrent while it downloads.
//!
//! IPv4 and IPv6 use separate sockets, routing tables and node ids. IPv6
//! is optional: a failed IPv6 bind leaves the IPv4 node running. Bootstrap
//! answers can introduce nodes in the other family (BEP 32).
//!
//! The node id starts out random. Once enough nodes agree on our external
//! address (the `ip` field of their answers, BEP 42), it is replaced by an
//! id derived from that address, and nodes with such ids are preferred in
//! the routing table. The routing table is not saved between runs.

mod bloom;
mod krpc;
mod lookup;
mod routing;
mod search;
mod secure;
mod store;
mod token;

pub use search::DhtSearch;

use anyhow::{Context, Result, anyhow};
use bloom::Bloom;
use krpc::{Body, ERROR_METHOD_UNKNOWN, ERROR_PROTOCOL, Message, Query, Response, Want};
use lookup::Lookup;
use routing::{Insert, K, NodeId, NodeInfo, RoutingTable};
use secure::IpVotes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
#[cfg(test)]
use std::net::Ipv6Addr;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::time::Duration;
use store::PeerStore;
use token::Tokens;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{Instant, timeout};
use tracing::{debug, info, warn};

/// Well-known nodes to bootstrap from.
pub const DEFAULT_ROUTERS: &[&str] = &[
    "router.bittorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "router.utorrent.com:6881",
];
/// How long to wait for an answer before sending a query again, and how
/// many times it is sent.
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);
const QUERY_ATTEMPTS: u32 = 2;
/// Queries of ours waiting for an answer.
const MAX_PENDING: usize = 256;
/// A lookup ends after this long with what it has found.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);
/// Peers kept from one lookup.
const MAX_LOOKUP_PEERS: usize = 500;
/// Router addresses remembered, so their answers stay out of the table.
const MAX_ROUTERS: usize = 64;
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
/// How often stale buckets are refreshed, or an empty table bootstrapped
/// again.
const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(60);
/// Unknown nodes waiting to be pinged before they may enter the table, and
/// how many of those pings run at once.
const VERIFY_QUEUE: usize = 32;
const MAX_VERIFYING: usize = 8;
/// Room for the largest UDP datagram.
const RECV_BUFFER: usize = 65536;
/// Pause after a failed receive, so that an error that keeps coming back
/// cannot spin the loop.
const RECV_ERROR_PAUSE: Duration = Duration::from_millis(100);

/// A running DHT node. Clones share the node, which stops once the last
/// clone is dropped.
#[derive(Clone)]
pub struct Dht(Arc<Running>);

struct Running {
    node: Arc<Node>,
    node6: Option<Arc<Node>>,
    tasks: Vec<AbortHandle>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.tasks.iter().for_each(AbortHandle::abort);
    }
}

impl Dht {
    /// Starts nodes on UDP `port` on every IPv4 and IPv6 interface, or on a free
    /// port if that one is taken, bootstrapping from `DEFAULT_ROUTERS`.
    pub async fn bind(port: u16) -> Result<Dht> {
        Self::bind_with_policy(port, crate::network::global()).await
    }

    async fn bind_with_policy(port: u16, policy: &crate::network::Policy) -> Result<Dht> {
        let ipv6 = policy.source_address().is_some_and(|ip| ip.is_ipv6());
        let socket = match policy.udp_socket(ipv6, port) {
            Err(e) if port != 0 => {
                warn!(
                    port,
                    "cannot use port for the DHT: {e}; using a free one instead"
                );
                policy.udp_socket(ipv6, 0)
            }
            result => result,
        }
        .context("cannot bind the DHT socket")?;
        let routers = DEFAULT_ROUTERS.iter().map(|r| r.to_string()).collect();
        let port = socket.local_addr()?.port();
        // An explicit source address selects one family. With unrestricted
        // addresses, keep the IPv6 sibling optional as before.
        let socket6 = if policy.source_address().is_none() {
            match policy.udp_socket(true, port) {
                Ok(socket) => Some(socket),
                Err(e) => {
                    debug!("IPv6 DHT unavailable; continuing with IPv4: {e}");
                    None
                }
            }
        } else {
            None
        };
        let dht = Self::start_sockets(socket, socket6, routers)?;
        info!(port = dht.port(), "DHT node started");
        Ok(dht)
    }

    /// Starts a node on `socket` that bootstraps from `routers`
    /// (`host:port`). It bootstraps on its first lookup, or within a minute.
    pub fn start(socket: UdpSocket, routers: Vec<String>) -> Result<Dht> {
        Self::start_sockets(socket, None, routers)
    }

    fn start_sockets(
        socket: UdpSocket,
        socket6: Option<UdpSocket>,
        routers: Vec<String>,
    ) -> Result<Dht> {
        let mut tasks = Vec::new();
        let node = Node::start(socket, routers.clone(), &mut tasks)?;
        let node6 = socket6
            .map(|socket| Node::start(socket, routers, &mut tasks))
            .transpose()?;
        if let Some(other) = &node6 {
            let _ = node.sibling.set(Arc::downgrade(other));
            let _ = other.sibling.set(Arc::downgrade(&node));
        }
        Ok(Dht(Arc::new(Running { node, node6, tasks })))
    }

    /// The IPv6 listening address, when IPv6 is available.
    pub fn local_addr_v6(&self) -> Option<SocketAddr> {
        if self.0.node.addr.is_ipv6() {
            Some(self.0.node.addr)
        } else {
            self.0.node6.as_ref().map(|node| node.addr)
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.0.node.addr
    }

    /// The UDP port the node listens on, as sent to peers in `Port`
    /// messages.
    pub fn port(&self) -> u16 {
        self.0.node.addr.port()
    }

    /// Our node id. It changes once we learn our external address.
    pub fn node_id(&self) -> [u8; 20] {
        self.0.node.id().0
    }

    /// Tells the node our external address, as the router reported it. Unlike
    /// `report_external_ip` it counts at once. Addresses that cannot be
    /// public are ignored.
    pub fn set_external_ip(&self, ip: Ipv4Addr) {
        if self.0.node.addr.is_ipv4() {
            self.0.node.set_external(ip.into(), Instant::now());
        }
    }

    /// Tells the node that `reporter` says our external address is `ip`,
    /// as a peer does with `yourip` in its extension handshake (BEP 10).
    /// It counts like one more node's word, so that a lone peer cannot
    /// change our id.
    pub fn report_external_ip(&self, reporter: IpAddr, ip: IpAddr) {
        let node = if reporter.is_ipv4() == self.0.node.addr.is_ipv4() {
            Some(&self.0.node)
        } else {
            self.0.node6.as_ref()
        };
        if let Some(node) = node.filter(|_| reporter.is_ipv4() == ip.is_ipv4()) {
            node.learn_external(reporter, ip, Instant::now());
        }
    }

    /// Nodes across the IPv4 and IPv6 routing tables.
    pub fn node_count(&self) -> usize {
        self.0.node.lock().table.len()
            + self
                .0
                .node6
                .as_ref()
                .map_or(0, |node| node.lock().table.len())
    }

    /// Fills the routing table with a `find_node` lookup for our own id.
    /// Returns the number of nodes in the table afterwards.
    pub async fn bootstrap(&self) -> usize {
        self.0.node.bootstrap().await;
        if let Some(node) = &self.0.node6 {
            node.bootstrap().await;
        }
        self.node_count()
    }

    /// Looks up peers for `info_hash`, bootstrapping first if the table is
    /// empty. With an `announce_port`, the closest nodes are then told that
    /// we accept peers on that TCP port.
    pub async fn get_peers(
        &self,
        info_hash: [u8; 20],
        announce_port: Option<u16>,
    ) -> Vec<SocketAddr> {
        self.lookup_peers(info_hash, announce_port, false)
            .await
            .peers
    }

    /// Like `get_peers`, and also scrapes the swarm size (BEP 33). A node
    /// that `seeding` the torrent announces itself as a seed and asks for no
    /// seeds in return.
    pub async fn lookup_peers(
        &self,
        info_hash: [u8; 20],
        announce_port: Option<u16>,
        seeding: bool,
    ) -> DhtPeers {
        // Bootstrap IPv4 first so nodes6 can seed an IPv6 lookup even
        // when our bootstrap routers have no AAAA records.
        if self.0.node.lock().table.is_empty() && self.0.node6.is_some() {
            self.0.node.bootstrap().await;
        }
        let v4 = self.0.node.get_peers(info_hash, announce_port, seeding);
        let v6 = async {
            match &self.0.node6 {
                Some(node) => node.get_peers(info_hash, announce_port, seeding).await,
                None => DhtPeers::default(),
            }
        };
        let (mut found, other) = tokio::join!(v4, v6);
        found.peers.extend(other.peers);
        found.peers.sort_unstable();
        found.peers.dedup();
        // The same hosts may participate in both DHTs; adding estimates
        // would count them twice. Keep the larger estimate per category.
        found.swarm = match (found.swarm, other.swarm) {
            (Some(a), Some(b)) => Some(SwarmEstimate {
                seeds: a.seeds.max(b.seeds),
                leechers: a.leechers.max(b.leechers),
            }),
            (a, b) => a.or(b),
        };
        found
    }

    /// Pings a node named in a peer's `Port` message on its address family.
    pub fn add_node(&self, addr: SocketAddr) {
        let node = if addr.is_ipv4() == self.0.node.addr.is_ipv4() {
            Some(&self.0.node)
        } else {
            self.0.node6.as_ref()
        };
        if let Some(node) = node.filter(|_| addr.port() != 0)
            && node.verify.try_send(addr).is_err()
        {
            debug!(%addr, "too many DHT nodes to verify; skipping");
        }
    }
}

/// What a lookup for peers found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DhtPeers {
    pub peers: Vec<SocketAddr>,
    /// The size of the swarm, if any node answered the scrape.
    pub swarm: Option<SwarmEstimate>,
}

/// How many peers a torrent has, estimated from the bloom filters of the
/// nodes closest to it (BEP 33).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwarmEstimate {
    /// Peers with the whole torrent.
    pub seeds: u32,
    /// Peers still downloading.
    pub leechers: u32,
}

struct Node {
    socket: UdpSocket,
    sibling: OnceLock<Weak<Node>>,
    addr: SocketAddr,
    routers: Vec<String>,
    state: Mutex<State>,
    /// Unknown nodes to ping, and add to the table if they answer.
    verify: mpsc::Sender<SocketAddr>,
}

struct State {
    table: RoutingTable,
    tokens: Tokens,
    store: PeerStore,
    /// Our queries waiting for an answer, by transaction id.
    pending: HashMap<u16, Pending>,
    /// Resolved bootstrap routers. They help lookups but stay out of the
    /// table: they hold no peers and do not want the traffic.
    routers: HashSet<SocketAddr>,
    /// What nodes say our external address is.
    votes: IpVotes,
    /// Our external address once nodes agree on it.
    external: Option<IpAddr>,
    /// Cross-family bootstrap contacts, untrusted until they answer.
    bridges: HashSet<SocketAddr>,
}

impl State {
    fn new(id: NodeId, now: Instant) -> Self {
        Self {
            table: RoutingTable::new(id, now),
            tokens: Tokens::new(now),
            store: PeerStore::default(),
            pending: HashMap::new(),
            routers: HashSet::new(),
            votes: IpVotes::default(),
            external: None,
            bridges: HashSet::new(),
        }
    }

    /// Counts `reporter`'s word that our address is `reported`. When enough
    /// nodes agree on an address and our id is not valid for it, a new id
    /// takes its place and the routing table is rebuilt around it. Returns
    /// the new id.
    fn learn_external(
        &mut self,
        reporter: impl Into<IpAddr>,
        reported: impl Into<IpAddr>,
        now: Instant,
    ) -> Option<NodeId> {
        let ip = self.votes.vote(reporter, reported)?;
        self.adopt(ip, now)
    }

    /// Takes `ip` as our external address. If our id is not valid for it, a
    /// new one takes its place and the routing table is rebuilt around it,
    /// which is returned.
    fn adopt(&mut self, ip: impl Into<IpAddr>, now: Instant) -> Option<NodeId> {
        let ip = ip.into();
        if !secure::is_public_ip(ip) {
            return None;
        }
        self.external = Some(ip);
        if self.table.own().is_secure(ip) {
            return None;
        }
        let id = NodeId::secure(ip);
        self.table.set_own(id, now);
        Some(id)
    }
}

struct Pending {
    addr: SocketAddr,
    reply: oneshot::Sender<Result<Response>>,
}

/// Why a query got no response.
enum QueryError {
    /// We could not send it, or had no room to. Says nothing about the node.
    Local(anyhow::Error),
    /// The node never answered, or answered with an error.
    Remote(anyhow::Error),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Local(e) | Self::Remote(e)) = self;
        write!(f, "{e:#}")
    }
}

/// Forgets a query once nobody waits for its answer any more.
struct Unregister<'a> {
    node: &'a Node,
    tid: u16,
}

impl Drop for Unregister<'_> {
    fn drop(&mut self) {
        self.node.lock().pending.remove(&self.tid);
    }
}

/// What a lookup is for.
#[derive(Clone, Copy)]
enum Kind {
    FindNode,
    /// Ask for peers and bloom filters of the swarm; `noseed` if we are one.
    GetPeers {
        noseed: bool,
    },
}

/// What a lookup found.
struct Found {
    peers: HashSet<SocketAddr>,
    /// The merged bloom filters of the nodes that sent them: seeds, and
    /// other peers.
    seeds: Option<Bloom>,
    others: Option<Bloom>,
    /// The closest nodes that answered, with their `get_peers` tokens.
    closest: Vec<(NodeInfo, Option<Vec<u8>>)>,
}

impl Found {
    /// The swarm size from the filters of all answers. Peers that nodes
    /// without the extension sent are added to the filter of non-seeds
    /// unless a seed filter has them, so that they count too.
    fn swarm(&self) -> Option<SwarmEstimate> {
        if self.seeds.is_none() && self.others.is_none() {
            return None;
        }
        let seeds = self.seeds.clone().unwrap_or_default();
        let mut others = self.others.clone().unwrap_or_default();
        for peer in &self.peers {
            let ip = peer.ip();
            if !seeds.contains(ip) {
                others.insert(ip);
            }
        }
        Some(SwarmEstimate {
            seeds: seeds.count(),
            leechers: others.count(),
        })
    }
}

impl Node {
    fn start(
        socket: UdpSocket,
        routers: Vec<String>,
        tasks: &mut Vec<AbortHandle>,
    ) -> Result<Arc<Self>> {
        let addr = socket.local_addr().context("DHT socket has no address")?;
        let (verify, verify_rx) = mpsc::channel(VERIFY_QUEUE);
        let node = Arc::new(Self {
            socket,
            addr,
            sibling: OnceLock::new(),
            routers,
            state: Mutex::new(State::new(NodeId::random(), Instant::now())),
            verify,
        });
        tasks.push(tokio::spawn(Arc::clone(&node).receive()).abort_handle());
        tasks.push(tokio::spawn(Arc::clone(&node).maintain(verify_rx)).abort_handle());
        Ok(node)
    }

    fn sibling(&self) -> Option<Arc<Node>> {
        self.sibling.get().and_then(Weak::upgrade)
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Our current node id.
    fn id(&self) -> NodeId {
        self.lock().table.own()
    }

    /// See `State::learn_external`.
    fn learn_external(self: &Arc<Self>, reporter: IpAddr, reported: IpAddr, now: Instant) {
        let new = self.lock().learn_external(reporter, reported, now);
        self.new_id(new, reported);
    }

    /// See `State::adopt`.
    fn set_external(self: &Arc<Self>, ip: IpAddr, now: Instant) {
        let new = self.lock().adopt(ip, now);
        self.new_id(new, ip);
    }

    /// Logs a new id, and looks it up to fill the buckets around it.
    fn new_id(self: &Arc<Self>, id: Option<NodeId>, ip: IpAddr) {
        if let Some(id) = id {
            info!(%ip, ?id, "DHT node id changed to match our external address");
            let node = Arc::clone(self);
            tokio::spawn(async move { node.lookup(id, Kind::FindNode).await });
        }
    }

    /// Receives and handles datagrams until the node stops. Malformed ones
    /// are dropped.
    async fn receive(self: Arc<Self>) {
        let mut buf = vec![0u8; RECV_BUFFER];
        loop {
            let (len, from) = match self.socket.recv_from(&mut buf).await {
                Ok(received) => received,
                Err(e) => {
                    debug!("DHT receive failed: {e}");
                    tokio::time::sleep(RECV_ERROR_PAUSE).await;
                    continue;
                }
            };
            match Message::decode(&buf[..len]) {
                Ok(message) => self.handle(message, from).await,
                Err(e) => debug!(%from, "dropping malformed DHT packet: {e:#}"),
            }
        }
    }

    async fn handle(self: &Arc<Self>, message: Message, from: SocketAddr) {
        let now = Instant::now();
        match message.body {
            Body::Query { id, query } => {
                let (body, verify) = {
                    let mut state = self.lock();
                    let own = state.table.own();
                    answer(&mut state, own, id, query.clone(), from, now)
                };
                let mut body = body;
                if let (Some(other), Body::Response(response)) = (self.sibling(), &mut body) {
                    let target_want = match query {
                        Query::FindNode { target, want } => Some((target, want)),
                        Query::GetPeers {
                            info_hash, want, ..
                        } => Some((NodeId(info_hash), want)),
                        _ => None,
                    };
                    if let Some((target, want)) = target_want {
                        response.nodes.extend(wanted(
                            &other.lock().table,
                            &target,
                            want,
                            from.ip(),
                        ));
                    }
                }
                // If the queue is full, the node gets another chance with
                // its next query.
                if verify && self.verify.try_send(from).is_err() {
                    debug!(%from, "too many DHT nodes to verify; skipping");
                }
                // Tells the node its address as we see it (BEP 42).
                let ip = matches!(body, Body::Response(_)).then_some(from);
                let reply = Message {
                    tid: message.tid,
                    ip,
                    body,
                };
                let Some(packet) = bounded_reply(reply) else {
                    return;
                };
                if let Err(e) = self.socket.send_to(&packet, from).await {
                    debug!(%from, "cannot answer DHT query: {e}");
                }
            }
            Body::Response(response) => {
                self.resolve(&message.tid, from, Ok(response), now);
                if let Some(ip) = message.ip.filter(|ip| ip.is_ipv4() == from.is_ipv4()) {
                    self.learn_external(from.ip(), ip.ip(), now);
                }
            }
            Body::Error {
                code,
                message: text,
            } => {
                let error = anyhow!("DHT node answered with error {code}: {text}");
                self.resolve(&message.tid, from, Err(error), now);
            }
        }
    }

    /// Hands an answer to the query waiting for it. Only an answer from the
    /// address the query went to counts. A node that answers goes into the
    /// routing table, unless it is a router.
    fn resolve(
        self: &Arc<Self>,
        tid: &[u8],
        from: SocketAddr,
        reply: Result<Response>,
        now: Instant,
    ) {
        let Ok(tid) = <[u8; 2]>::try_from(tid).map(u16::from_be_bytes) else {
            debug!(%from, "DHT answer with a transaction id that is not ours");
            return;
        };
        let mut state = self.lock();
        if state
            .pending
            .get(&tid)
            .is_none_or(|pending| pending.addr != from)
        {
            debug!(%from, "unexpected DHT answer");
            return;
        }
        let Some(pending) = state.pending.remove(&tid) else {
            return;
        };
        if let Ok(response) = &reply
            && !state.routers.contains(&from)
        {
            state.bridges.remove(&from);
            let node = NodeInfo {
                id: response.id,
                addr: from,
            };
            if let Insert::PingFirst(questionable) = state.table.insert(node, now) {
                tokio::spawn(Arc::clone(self).evict_check(questionable));
            }
        }
        drop(state);
        // The query may have stopped waiting meanwhile.
        let _ = pending.reply.send(reply);
    }

    fn register(
        &self,
        addr: SocketAddr,
    ) -> Result<(u16, oneshot::Receiver<Result<Response>>), QueryError> {
        let mut state = self.lock();
        if state.pending.len() >= MAX_PENDING {
            return Err(QueryError::Local(anyhow!("too many DHT queries in flight")));
        }
        let (reply, receiver) = oneshot::channel();
        // At most `MAX_PENDING` of 65536 ids are taken, so a free one turns
        // up. Random ids keep an off-path sender from guessing ours.
        let mut tid: u16 = rand::random();
        while state.pending.contains_key(&tid) {
            tid = rand::random();
        }
        state.pending.insert(tid, Pending { addr, reply });
        Ok((tid, receiver))
    }

    /// Sends `query` to `addr` and waits for the answer, sending it again
    /// if none comes. A node that never answers counts as failed in the
    /// routing table.
    async fn query(
        self: &Arc<Self>,
        addr: SocketAddr,
        query: Query,
    ) -> Result<Response, QueryError> {
        let (tid, mut reply) = self.register(addr)?;
        let _unregister = Unregister { node: self, tid };
        let message = Message {
            tid: tid.to_be_bytes().to_vec(),
            ip: None,
            body: Body::Query {
                id: self.id(),
                query,
            },
        };
        let packet = message.encode();
        if packet.len() > 1024 {
            return Err(QueryError::Local(anyhow!(
                "DHT query exceeds the 1024-byte packet limit"
            )));
        }
        for attempt in 1..=QUERY_ATTEMPTS {
            self.socket
                .send_to(&packet, addr)
                .await
                .context("cannot send DHT query")
                .map_err(QueryError::Local)?;
            match timeout(QUERY_TIMEOUT, &mut reply).await {
                Ok(Ok(reply)) => return reply.map_err(QueryError::Remote),
                Ok(Err(_)) => return Err(QueryError::Local(anyhow!("DHT node stopped"))),
                Err(_) => debug!(%addr, attempt, "DHT query timed out"),
            }
        }
        self.lock().table.failed(addr, Instant::now());
        Err(QueryError::Remote(anyhow!("no answer from {addr}")))
    }

    /// Resolves bootstrap routers and includes cross-family contacts.
    async fn resolve_routers(&self) -> Vec<SocketAddr> {
        let mut found = Vec::new();
        for router in &self.routers {
            match timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(router.as_str())).await {
                Ok(Ok(addrs)) => {
                    found.extend(addrs.filter(|addr| addr.is_ipv4() == self.addr.is_ipv4()))
                }
                Ok(Err(e)) => warn!(router, "cannot resolve DHT router: {e}"),
                Err(_) => warn!(router, "resolving DHT router timed out"),
            }
        }
        found.sort();
        found.dedup();
        found.truncate(MAX_ROUTERS);
        let mut state = self.lock();
        if state.routers.len() + found.len() > MAX_ROUTERS {
            state.routers.clear();
        }
        state.routers.extend(found.iter().copied());
        found.extend(state.bridges.iter().copied());
        found.sort_unstable();
        found.dedup();
        found
    }

    /// Runs an iterative lookup for `target`, with `get_peers` queries if
    /// `get_peers` is set and `find_node` otherwise. It starts from the
    /// closest nodes in the table, plus the routers while the table holds
    /// fewer than `K` nodes.
    async fn lookup(self: &Arc<Self>, target: NodeId, kind: Kind) -> Found {
        let seeds = self.lock().table.closest(&target, K);
        let mut routers = if seeds.len() < K {
            self.resolve_routers().await
        } else {
            Vec::new()
        };
        routers.retain(|addr| !seeds.iter().any(|node| node.addr == *addr));
        let want = if self
            .sibling()
            .is_some_and(|other| other.lock().table.len() < K)
        {
            Want::BOTH
        } else {
            Want::default()
        };
        let mut lookup = Lookup::new(self.id(), target, seeds, routers);
        let mut queries = JoinSet::new();
        // When each query becomes slow, in the order they were sent.
        let mut slow = VecDeque::new();
        let mut peers = HashSet::new();
        let (mut seeds, mut others) = (None::<Bloom>, None::<Bloom>);
        let deadline = Instant::now() + LOOKUP_TIMEOUT;
        loop {
            for addr in lookup.next_queries() {
                let node = Arc::clone(self);
                let query = match kind {
                    Kind::GetPeers { noseed } => Query::GetPeers {
                        info_hash: target.0,
                        want,
                        noseed,
                        scrape: true,
                    },
                    Kind::FindNode => Query::FindNode { target, want },
                };
                slow.push_back((Instant::now() + QUERY_TIMEOUT, addr));
                queries.spawn(async move { (addr, node.query(addr, query).await) });
            }
            if lookup.is_done() {
                break;
            }
            let next_slow = slow.front().map(|&(at, _)| at);
            let joined = tokio::select! {
                joined = queries.join_next() => joined,
                () = sleep_until_some(next_slow) => {
                    if let Some((_, addr)) = slow.pop_front() {
                        lookup.on_slow(addr);
                    }
                    continue;
                }
                () = tokio::time::sleep_until(deadline) => {
                    debug!(?target, "DHT lookup timed out");
                    break;
                }
            };
            let (addr, result) = match joined {
                Some(Ok(answer)) => answer,
                // Query tasks never panic, and are only aborted with the set.
                Some(Err(e)) => {
                    warn!("DHT query task failed: {e}");
                    continue;
                }
                None => break,
            };
            match result {
                Ok(response) => {
                    let reachable =
                        |peer: &SocketAddr| peer.port() != 0 && !peer.ip().is_unspecified();
                    for (filter, sent) in [
                        (&mut seeds, &response.seed_filter),
                        (&mut others, &response.peer_filter),
                    ] {
                        if let Some(sent) = sent {
                            filter.get_or_insert_with(Bloom::default).merge(sent);
                        }
                    }
                    for peer in response.values.into_iter().filter(reachable) {
                        if peers.len() >= MAX_LOOKUP_PEERS {
                            break;
                        }
                        peers.insert(peer);
                    }
                    let mut nodes = response.nodes;
                    if let Some(other) = self.sibling() {
                        let mut state = other.lock();
                        for node in &nodes {
                            if node.addr.is_ipv4() == other.addr.is_ipv4()
                                && node.addr.port() != 0
                                && !node.addr.ip().is_unspecified()
                                && state.bridges.len() < MAX_ROUTERS
                            {
                                state.bridges.insert(node.addr);
                                let _ = other.verify.try_send(node.addr);
                            }
                        }
                    }
                    nodes.retain(|node| node.addr.is_ipv4() == self.addr.is_ipv4());
                    lookup.on_response(addr, response.id, nodes, response.token);
                }
                Err(e) => {
                    debug!(%addr, "DHT query failed: {e:#}");
                    lookup.on_failure(addr);
                }
            }
        }
        Found {
            peers,
            seeds,
            others,
            closest: lookup.closest_responded(),
        }
    }

    async fn bootstrap(self: &Arc<Self>) -> usize {
        self.lookup(self.id(), Kind::FindNode).await;
        let nodes = self.lock().table.len();
        if nodes == 0 {
            warn!("DHT bootstrap found no nodes");
        } else {
            info!(nodes, "DHT bootstrapped");
        }
        nodes
    }

    async fn get_peers(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        announce_port: Option<u16>,
        seeding: bool,
    ) -> DhtPeers {
        if self.lock().table.is_empty() {
            self.bootstrap().await;
        }
        let kind = Kind::GetPeers { noseed: seeding };
        let found = self.lookup(NodeId(info_hash), kind).await;
        let swarm = found.swarm();
        if let Some(port) = announce_port {
            self.announce(info_hash, port, seeding, found.closest).await;
        }
        DhtPeers {
            peers: found.peers.into_iter().collect(),
            swarm,
        }
    }

    /// Tells the `closest` nodes that sent a token that we accept peers for
    /// `info_hash` on TCP `port`. When that is our UDP port too, we send
    /// `implied_port`, so that a node sees the port our NAT (if any) maps.
    async fn announce(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        port: u16,
        seed: bool,
        closest: Vec<(NodeInfo, Option<Vec<u8>>)>,
    ) {
        let implied_port = port == self.addr.port();
        let mut announces = JoinSet::new();
        for (node, token) in closest {
            let Some(token) = token else {
                continue;
            };
            let query = Query::AnnouncePeer {
                info_hash,
                port,
                implied_port,
                seed,
                token,
            };
            let dht = Arc::clone(self);
            announces.spawn(async move { dht.query(node.addr, query).await });
        }
        let mut announced = 0;
        while let Some(result) = announces.join_next().await {
            match result {
                Ok(Ok(_)) => announced += 1,
                Ok(Err(e)) => debug!("DHT announce failed: {e:#}"),
                Err(e) => warn!("DHT announce task failed: {e}"),
            }
        }
        debug!(announced, "announced to DHT nodes");
    }

    /// Pings a questionable node to decide whether it keeps its place, then
    /// the bucket's next questionable node for as long as they answer.
    async fn evict_check(self: Arc<Self>, mut node: NodeInfo) {
        loop {
            let result = self.query(node.addr, Query::Ping).await;
            let now = Instant::now();
            let mut state = self.lock();
            if let Err(QueryError::Local(e)) = &result {
                // The ping never went out, so the node stays.
                debug!(addr = %node.addr, "cannot ping DHT node: {e:#}");
                state.table.stop_pinging(&node.id);
                return;
            }
            if !matches!(result, Ok(r) if r.id == node.id) {
                debug!(addr = %node.addr, "evicting DHT node");
                state.table.evict(&node.id, now);
                return;
            }
            match state.table.ping_next(&node.id, now) {
                Some(next) => node = next,
                None => return,
            }
        }
    }

    /// Pings nodes that queried us or were named in `Port` messages, so
    /// that they enter the table if they answer, and refreshes the table
    /// every `MAINTENANCE_INTERVAL`.
    async fn maintain(self: Arc<Self>, mut verify: mpsc::Receiver<SocketAddr>) {
        let start = Instant::now() + MAINTENANCE_INTERVAL;
        let mut tick = tokio::time::interval_at(start, MAINTENANCE_INTERVAL);
        let mut pings = JoinSet::new();
        let mut verifying = HashSet::new();
        let mut refresh = JoinSet::new();
        loop {
            tokio::select! {
                Some(addr) = verify.recv() => {
                    if pings.len() >= MAX_VERIFYING {
                        debug!(%addr, "too many DHT nodes to verify; skipping");
                    } else if verifying.insert(addr) {
                        let node = Arc::clone(&self);
                        pings.spawn(async move {
                            // Its answer puts it in the table.
                            let _ = node.query(addr, Query::Ping).await;
                            node.lock().bridges.remove(&addr);
                            addr
                        });
                    }
                }
                Some(done) = pings.join_next() => {
                    if let Ok(addr) = done {
                        verifying.remove(&addr);
                    }
                }
                _ = tick.tick() => {
                    if refresh.is_empty() {
                        refresh.spawn(Arc::clone(&self).refresh());
                    }
                }
                Some(_) = refresh.join_next() => {}
            }
        }
    }

    /// Looks up a random id in every bucket that has not changed for 15
    /// minutes, or bootstraps again if the table is empty.
    async fn refresh(self: Arc<Self>) {
        let targets = {
            let state = self.lock();
            (!state.table.is_empty()).then(|| {
                state
                    .table
                    .refresh_targets(Instant::now(), &mut rand::rng())
            })
        };
        match targets {
            None => {
                self.bootstrap().await;
            }
            Some(targets) => {
                for target in targets {
                    self.lookup(target, Kind::FindNode).await;
                }
            }
        }
    }
}

/// BEP 32 limits UDP payloads to 1024 bytes, even with scrape filters
/// and both node families. Trim contacts; never send a truncated bencode.
fn bounded_reply(mut reply: Message) -> Option<Vec<u8>> {
    loop {
        let packet = reply.encode();
        if packet.len() <= 1024 {
            return Some(packet);
        }
        let Body::Response(response) = &mut reply.body else {
            return None;
        };
        // Preserve some peers even when scrape filters and both routing
        // tables are present. Keep up to K contacts before trimming values.
        if (response.nodes.len() > K || response.values.len() <= 1)
            && response.nodes.pop().is_some()
        {
            continue;
        }
        response.values.pop()?;
    }
}

/// Binds an IPv6-only UDP socket to the same port as the IPv4 node.
#[cfg(test)]
fn bind_ipv6(ip: Ipv6Addr, port: u16) -> std::io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV6,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    socket.set_only_v6(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::new(ip.into(), port).into())?;
    UdpSocket::from_std(socket.into())
}

/// Sleeps until `at`, or forever if it is `None`.
async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// The nodes closest to `target` of the families a query `want`s, which
/// came from `ip`. Each node's table holds only its own address family.
fn wanted(table: &RoutingTable, target: &NodeId, want: Want, ip: IpAddr) -> Vec<NodeInfo> {
    let mut nodes = table.closest(target, K);
    nodes.retain(|node| want.includes(node.addr.ip(), ip));
    nodes
}

/// Answers a query from node `id` at `from`. Also says whether that node is
/// new to us and would get a place in the table, so is worth pinging.
fn answer(
    state: &mut State,
    own: NodeId,
    id: NodeId,
    query: Query,
    from: SocketAddr,
    now: Instant,
) -> (Body, bool) {
    let node = NodeInfo { id, addr: from };
    let verify = !state.table.heard_from(&id, from, now) && state.table.wants(&node);
    let ip = from.ip();
    let response = |response: Response| {
        Body::Response(Response {
            id: own,
            ..response
        })
    };
    let error = |code, message: &str| Body::Error {
        code,
        message: message.into(),
    };
    let body = match query {
        Query::Ping => response(Response::default()),
        Query::FindNode { target, want } => response(Response {
            nodes: wanted(&state.table, &target, want, ip),
            ..Response::default()
        }),
        // Nodes come along even when there are peers, as most clients do:
        // they help the lookup go on.
        Query::GetPeers {
            info_hash,
            want,
            noseed,
            scrape,
        } => {
            let (seed_filter, peer_filter) = match scrape
                .then(|| state.store.scrape(&info_hash, now))
                .flatten()
            {
                Some((seeds, others)) => (Some(seeds), Some(others)),
                None => (None, None),
            };
            response(Response {
                nodes: wanted(&state.table, &NodeId(info_hash), want, ip),
                values: state
                    .store
                    .get(&info_hash, noseed, now)
                    .into_iter()
                    .filter(|peer| peer.is_ipv4() == from.is_ipv4())
                    .collect(),
                token: Some(state.tokens.issue(ip, now)),
                seed_filter,
                peer_filter,
                ..Response::default()
            })
        }
        Query::AnnouncePeer {
            info_hash,
            port,
            implied_port,
            seed,
            token,
        } => {
            if state.tokens.check(&token, ip, now) {
                let port = if implied_port { from.port() } else { port };
                state
                    .store
                    .announce(info_hash, SocketAddr::new(ip, port), seed, now);
                response(Response::default())
            } else {
                error(ERROR_PROTOCOL, "bad token")
            }
        }
        Query::Unknown(_) => error(ERROR_METHOD_UNKNOWN, "method unknown"),
    };
    (body, verify)
}

#[cfg(test)]
mod tests;
