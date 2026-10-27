//! Trackerless peer discovery over the BitTorrent DHT (BEP 5).
//!
//! [`Dht`] runs one DHT node on a UDP socket. It answers `ping`,
//! `find_node`, `get_peers` and `announce_peer`, keeps a routing table of
//! the nodes that answer it, and runs iterative lookups: `find_node` for
//! its own id to bootstrap, and `get_peers` for an info hash, after which
//! it announces us to the closest nodes. [`DhtSearch`] repeats the lookup
//! for one torrent while it downloads.
//!
//! IPv4 only: IPv6 nodes and peers (`nodes6`, 18-byte values) are ignored.
//! The node id is random rather than derived from our address (BEP 42),
//! and the routing table is not saved between runs.

mod krpc;
mod lookup;
mod routing;
mod search;
mod store;
mod token;

pub use search::DhtSearch;

use anyhow::{Context, Result, anyhow, bail};
use krpc::{Body, ERROR_METHOD_UNKNOWN, ERROR_PROTOCOL, Message, Query, Response};
use lookup::Lookup;
use routing::{Insert, K, NodeId, NodeInfo, RoutingTable};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
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
    tasks: Vec<AbortHandle>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.tasks.iter().for_each(AbortHandle::abort);
    }
}

impl Dht {
    /// Starts a node on UDP `port` on every IPv4 interface, or on a free
    /// port if that one is taken, bootstrapping from `DEFAULT_ROUTERS`.
    pub async fn bind(port: u16) -> Result<Dht> {
        let bind = |port| UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port));
        let socket = match bind(port).await {
            Err(e) if port != 0 => {
                warn!(
                    port,
                    "cannot use port for the DHT: {e}; using a free one instead"
                );
                bind(0).await
            }
            result => result,
        }
        .context("cannot bind the DHT socket")?;
        let routers = DEFAULT_ROUTERS.iter().map(|r| r.to_string()).collect();
        let dht = Self::start(socket, routers)?;
        info!(port = dht.port(), "DHT node started");
        Ok(dht)
    }

    /// Starts a node on `socket` that bootstraps from `routers`
    /// (`host:port`). It bootstraps on its first lookup, or within a minute.
    pub fn start(socket: UdpSocket, routers: Vec<String>) -> Result<Dht> {
        let addr = socket.local_addr().context("DHT socket has no address")?;
        let id = NodeId::random();
        let (verify, verify_rx) = mpsc::channel(VERIFY_QUEUE);
        let node = Arc::new(Node {
            socket,
            addr,
            id,
            routers,
            state: Mutex::new(State::new(id, Instant::now())),
            verify,
        });
        let tasks = vec![
            tokio::spawn(Arc::clone(&node).receive()).abort_handle(),
            tokio::spawn(Arc::clone(&node).maintain(verify_rx)).abort_handle(),
        ];
        Ok(Dht(Arc::new(Running { node, tasks })))
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.0.node.addr
    }

    /// The UDP port the node listens on, as sent to peers in `Port`
    /// messages.
    pub fn port(&self) -> u16 {
        self.0.node.addr.port()
    }

    /// Nodes in the routing table.
    pub fn node_count(&self) -> usize {
        self.0.node.lock().table.len()
    }

    /// Fills the routing table with a `find_node` lookup for our own id.
    /// Returns the number of nodes in the table afterwards.
    pub async fn bootstrap(&self) -> usize {
        self.0.node.bootstrap().await
    }

    /// Looks up peers for `info_hash`, bootstrapping first if the table is
    /// empty. With an `announce_port`, the closest nodes are then told that
    /// we accept peers on that TCP port.
    pub async fn get_peers(
        &self,
        info_hash: [u8; 20],
        announce_port: Option<u16>,
    ) -> Vec<SocketAddr> {
        self.0.node.get_peers(info_hash, announce_port).await
    }

    /// Pings `addr`, which a peer named in a `Port` message, and adds it to
    /// the routing table if it answers. IPv6 addresses are ignored.
    pub fn add_node(&self, addr: SocketAddr) {
        if let SocketAddr::V4(addr) = addr
            && addr.port() != 0
            && self.0.node.verify.try_send(addr).is_err()
        {
            debug!(%addr, "too many DHT nodes to verify; skipping");
        }
    }
}

struct Node {
    socket: UdpSocket,
    addr: SocketAddr,
    id: NodeId,
    routers: Vec<String>,
    state: Mutex<State>,
    /// Unknown nodes to ping, and add to the table if they answer.
    verify: mpsc::Sender<SocketAddrV4>,
}

struct State {
    table: RoutingTable,
    tokens: Tokens,
    store: PeerStore,
    /// Our queries waiting for an answer, by transaction id.
    pending: HashMap<u16, Pending>,
    next_tid: u16,
    /// Resolved bootstrap routers. They help lookups but stay out of the
    /// table: they hold no peers and do not want the traffic.
    routers: HashSet<SocketAddrV4>,
}

impl State {
    fn new(id: NodeId, now: Instant) -> Self {
        Self {
            table: RoutingTable::new(id, now),
            tokens: Tokens::new(now),
            store: PeerStore::default(),
            pending: HashMap::new(),
            next_tid: rand::random(),
            routers: HashSet::new(),
        }
    }
}

struct Pending {
    addr: SocketAddrV4,
    reply: oneshot::Sender<Result<Response>>,
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

/// What a lookup found.
struct Found {
    peers: HashSet<SocketAddrV4>,
    /// The closest nodes that answered, with their `get_peers` tokens.
    closest: Vec<(NodeInfo, Option<Vec<u8>>)>,
}

impl Node {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
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
            let SocketAddr::V4(from) = from else {
                continue;
            };
            match Message::decode(&buf[..len]) {
                Ok(message) => self.handle(message, from).await,
                Err(e) => debug!(%from, "dropping malformed DHT packet: {e:#}"),
            }
        }
    }

    async fn handle(self: &Arc<Self>, message: Message, from: SocketAddrV4) {
        let now = Instant::now();
        match message.body {
            Body::Query { id, query } => {
                let (body, verify) = answer(&mut self.lock(), self.id, id, query, from, now);
                // If the queue is full, the node gets another chance with
                // its next query.
                if verify && self.verify.try_send(from).is_err() {
                    debug!(%from, "too many DHT nodes to verify; skipping");
                }
                let reply = Message {
                    tid: message.tid,
                    body,
                };
                if let Err(e) = self.socket.send_to(&reply.encode(), from).await {
                    debug!(%from, "cannot answer DHT query: {e}");
                }
            }
            Body::Response(response) => self.resolve(&message.tid, from, Ok(response), now),
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
        from: SocketAddrV4,
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

    fn register(&self, addr: SocketAddrV4) -> Result<(u16, oneshot::Receiver<Result<Response>>)> {
        let mut state = self.lock();
        if state.pending.len() >= MAX_PENDING {
            bail!("too many DHT queries in flight");
        }
        let (reply, receiver) = oneshot::channel();
        // Fewer than 65536 are pending, so a free id turns up.
        let mut tid = state.next_tid;
        while state.pending.contains_key(&tid) {
            tid = tid.wrapping_add(1);
        }
        state.next_tid = tid.wrapping_add(1);
        state.pending.insert(tid, Pending { addr, reply });
        Ok((tid, receiver))
    }

    /// Sends `query` to `addr` and waits for the answer, sending it again
    /// if none comes. A node that never answers counts as failed in the
    /// routing table.
    async fn query(self: &Arc<Self>, addr: SocketAddrV4, query: Query) -> Result<Response> {
        let (tid, mut reply) = self.register(addr)?;
        let _unregister = Unregister { node: self, tid };
        let message = Message {
            tid: tid.to_be_bytes().to_vec(),
            body: Body::Query { id: self.id, query },
        };
        let packet = message.encode();
        for attempt in 1..=QUERY_ATTEMPTS {
            self.socket
                .send_to(&packet, addr)
                .await
                .context("cannot send DHT query")?;
            match timeout(QUERY_TIMEOUT, &mut reply).await {
                Ok(Ok(reply)) => return reply,
                Ok(Err(_)) => bail!("DHT node stopped"),
                Err(_) => debug!(%addr, attempt, "DHT query timed out"),
            }
        }
        self.lock().table.failed(addr, Instant::now());
        bail!("no answer from {addr}")
    }

    /// Resolves the bootstrap routers to IPv4 addresses.
    async fn resolve_routers(&self) -> Vec<SocketAddrV4> {
        let mut found = Vec::new();
        for router in &self.routers {
            match timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(router.as_str())).await {
                Ok(Ok(addrs)) => found.extend(addrs.filter_map(|addr| match addr {
                    SocketAddr::V4(addr) => Some(addr),
                    SocketAddr::V6(_) => None,
                })),
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
        found
    }

    /// Runs an iterative lookup for `target`, with `get_peers` queries if
    /// `get_peers` is set and `find_node` otherwise. It starts from the
    /// closest nodes in the table, plus the routers while the table holds
    /// fewer than `K` nodes.
    async fn lookup(self: &Arc<Self>, target: NodeId, get_peers: bool) -> Found {
        let seeds = self.lock().table.closest(&target, K);
        let routers = if seeds.len() < K {
            self.resolve_routers().await
        } else {
            Vec::new()
        };
        let mut lookup = Lookup::new(self.id, target, seeds, routers);
        let mut queries = JoinSet::new();
        // When each query becomes slow, in the order they were sent.
        let mut slow = VecDeque::new();
        let mut peers = HashSet::new();
        let deadline = Instant::now() + LOOKUP_TIMEOUT;
        loop {
            for addr in lookup.next_queries() {
                let node = Arc::clone(self);
                let query = if get_peers {
                    Query::GetPeers {
                        info_hash: target.0,
                    }
                } else {
                    Query::FindNode { target }
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
                        |peer: &SocketAddrV4| peer.port() != 0 && !peer.ip().is_unspecified();
                    for peer in response.values.into_iter().filter(reachable) {
                        if peers.len() >= MAX_LOOKUP_PEERS {
                            break;
                        }
                        peers.insert(peer);
                    }
                    lookup.on_response(addr, response.id, response.nodes, response.token);
                }
                Err(e) => {
                    debug!(%addr, "DHT query failed: {e:#}");
                    lookup.on_failure(addr);
                }
            }
        }
        Found {
            peers,
            closest: lookup.closest_responded(),
        }
    }

    async fn bootstrap(self: &Arc<Self>) -> usize {
        self.lookup(self.id, false).await;
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
    ) -> Vec<SocketAddr> {
        if self.lock().table.is_empty() {
            self.bootstrap().await;
        }
        let found = self.lookup(NodeId(info_hash), true).await;
        if let Some(port) = announce_port {
            self.announce(info_hash, port, found.closest).await;
        }
        found.peers.into_iter().map(SocketAddr::V4).collect()
    }

    /// Tells the `closest` nodes that sent a token that we accept peers for
    /// `info_hash` on TCP `port`. When that is our UDP port too, we send
    /// `implied_port`, so that a node sees the port our NAT (if any) maps.
    async fn announce(
        self: &Arc<Self>,
        info_hash: [u8; 20],
        port: u16,
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
            let answered =
                matches!(self.query(node.addr, Query::Ping).await, Ok(r) if r.id == node.id);
            let now = Instant::now();
            let mut state = self.lock();
            if !answered {
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
    async fn maintain(self: Arc<Self>, mut verify: mpsc::Receiver<SocketAddrV4>) {
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
                    self.lookup(target, false).await;
                }
            }
        }
    }
}

/// Sleeps until `at`, or forever if it is `None`.
async fn sleep_until_some(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Answers a query from node `id` at `from`. Also says whether that node is
/// new to us and would get a place in the table, so is worth pinging.
fn answer(
    state: &mut State,
    own: NodeId,
    id: NodeId,
    query: Query,
    from: SocketAddrV4,
    now: Instant,
) -> (Body, bool) {
    let verify = !state.table.heard_from(&id, from, now) && state.table.wants(&id);
    let ip = *from.ip();
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
        Query::FindNode { target } => response(Response {
            nodes: state.table.closest(&target, K),
            ..Response::default()
        }),
        // Nodes come along even when there are peers, as most clients do:
        // they help the lookup go on.
        Query::GetPeers { info_hash } => response(Response {
            nodes: state.table.closest(&NodeId(info_hash), K),
            values: state.store.get(&info_hash, now),
            token: Some(state.tokens.issue(ip, now)),
            ..Response::default()
        }),
        Query::AnnouncePeer {
            info_hash,
            port,
            implied_port,
            token,
        } => {
            if state.tokens.check(&token, ip, now) {
                let port = if implied_port { from.port() } else { port };
                state
                    .store
                    .announce(info_hash, SocketAddrV4::new(ip, port), now);
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
