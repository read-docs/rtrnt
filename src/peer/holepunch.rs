//! BEP 55 rendezvous messages and bounded per-torrent relay coordination.
//! Connections use the existing uTP endpoint; this module never opens sockets.

use anyhow::{Result, bail, ensure};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;

/// Name negotiated through BEP 10.
pub const NAME: &str = "ut_holepunch";
/// Our local extended-message id (the peer chooses its own id).
pub const ID: u8 = 4;
/// Target endpoint is not a valid peer address.
pub const NO_SUCH_PEER: u32 = 1;
/// The relay has no connection to the requested endpoint.
pub const NOT_CONNECTED: u32 = 2;
/// The requested peer did not negotiate this extension.
pub const NO_SUPPORT: u32 = 3;
/// The requested endpoint belongs to the relay.
pub const NO_SELF: u32 = 4;

const MAX_PEERS: usize = 256;
const MAX_KNOWN: usize = 256;
const MAX_PENDING: usize = 32;
const COMMAND_QUEUE: usize = 8;
const PROVENANCE_LIFETIME: Duration = Duration::from_secs(300);
const ATTEMPT_LIFETIME: Duration = Duration::from_secs(30);
const RELAY_INTERVAL: Duration = Duration::from_secs(1);

type PeerId = [u8; 20];

/// An operation carried in a `ut_holepunch` extended message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Ask a connected peer to introduce this endpoint.
    Rendezvous,
    /// Connect to this endpoint over uTP from our shared UDP port.
    Connect,
    /// Rendezvous failed, with a BEP 55 error code (unknown codes preserved).
    Error(u32),
}

/// A complete BEP 55 payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Message {
    /// Requested, introduced or rejected peer endpoint.
    pub endpoint: SocketAddr,
    /// Which operation this payload carries.
    pub kind: Kind,
}

impl Message {
    /// Serializes the exact 12-byte IPv4 or 24-byte IPv6 payload.
    pub fn encode(self) -> Vec<u8> {
        let (kind, error) = match self.kind {
            Kind::Rendezvous => (0, 0),
            Kind::Connect => (1, 0),
            Kind::Error(code) => (2, code),
        };
        let mut bytes = vec![kind];
        match self.endpoint.ip() {
            IpAddr::V4(ip) => {
                bytes.push(0);
                bytes.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                bytes.push(1);
                bytes.extend_from_slice(&ip.octets());
            }
        }
        bytes.extend_from_slice(&self.endpoint.port().to_be_bytes());
        bytes.extend_from_slice(&error.to_be_bytes());
        bytes
    }

    /// Parses a payload, rejecting truncation, trailing bytes and invalid fields.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() >= 2, "short holepunch payload");
        let (ip, pos) = match bytes[1] {
            0 => {
                ensure!(bytes.len() == 12, "invalid IPv4 holepunch payload length");
                (
                    IpAddr::V4(Ipv4Addr::new(bytes[2], bytes[3], bytes[4], bytes[5])),
                    6,
                )
            }
            1 => {
                ensure!(bytes.len() == 24, "invalid IPv6 holepunch payload length");
                let mut ip = [0; 16];
                ip.copy_from_slice(&bytes[2..18]);
                (IpAddr::V6(Ipv6Addr::from(ip)), 18)
            }
            _ => bail!("unknown holepunch address family"),
        };
        let port = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        let error = u32::from_be_bytes([
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
            bytes[pos + 5],
        ]);
        let kind = match bytes[0] {
            0 if error == 0 => Kind::Rendezvous,
            1 if error == 0 => Kind::Connect,
            2 if error != 0 => Kind::Error(error),
            _ => bail!("invalid holepunch operation or error code"),
        };
        Ok(Self {
            endpoint: SocketAddr::new(ip, port),
            kind,
        })
    }
}

fn plausible(addr: SocketAddr) -> bool {
    let ip = addr.ip().to_canonical();
    addr.port() != 0
        && !ip.is_unspecified()
        && !ip.is_multicast()
        && !matches!(ip, IpAddr::V4(ip) if ip.is_broadcast())
}

struct Peer {
    endpoint: Option<SocketAddr>,
    supported: bool,
    commands: mpsc::Sender<Message>,
    relay_at: Instant,
}

#[derive(Default)]
struct State {
    peers: HashMap<PeerId, Peer>,
    /// Endpoints learned through PEX and the connected peer that knows them.
    known: HashMap<SocketAddr, (PeerId, Instant)>,
    requested: HashMap<SocketAddr, (PeerId, Instant)>,
    connecting: HashMap<SocketAddr, Instant>,
    local: HashSet<SocketAddr>,
}

impl State {
    fn expire(&mut self, now: Instant) {
        self.known.retain(|_, (_, until)| *until > now);
        self.requested.retain(|_, (_, until)| *until > now);
        self.connecting.retain(|_, until| *until > now);
    }

    fn connected(&self, addr: SocketAddr) -> bool {
        self.peers.values().any(|peer| peer.endpoint == Some(addr))
    }
}

/// Relay coordination shared by workers of one public torrent. All queues
/// and endpoint maps are bounded; requests expire without background tasks.
pub(crate) struct Hub {
    enabled: bool,
    state: Mutex<State>,
    connect: mpsc::Sender<SocketAddr>,
}

impl Hub {
    pub(crate) fn new(enabled: bool) -> (Arc<Self>, mpsc::Receiver<SocketAddr>) {
        let (connect, receiver) = mpsc::channel(MAX_PENDING);
        (
            Arc::new(Self {
                enabled,
                state: Mutex::default(),
                connect,
            }),
            receiver,
        )
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Registers a worker until its handle drops. Negotiation is updated
    /// separately, since the peer may change its BEP 10 handshake later.
    pub(crate) fn join(self: &Arc<Self>, id: PeerId) -> PeerHandle {
        let (commands, receiver) = mpsc::channel(COMMAND_QUEUE);
        let mut state = self.lock();
        if self.enabled && state.peers.len() < MAX_PEERS {
            state.peers.insert(
                id,
                Peer {
                    endpoint: None,
                    supported: false,
                    commands,
                    relay_at: Instant::now(),
                },
            );
        }
        PeerHandle {
            hub: Arc::clone(self),
            id,
            receiver,
        }
    }

    /// Requests rendezvous through the peer that advertised an endpoint,
    /// after its direct connection failed. A target is requested at most
    /// once per timeout window, with at most MAX_PENDING requests in flight.
    pub(crate) fn failed(&self, endpoint: SocketAddr, now: Instant) {
        if !self.enabled || !plausible(endpoint) {
            return;
        }
        let mut state = self.lock();
        state.expire(now);
        if state.connected(endpoint)
            || state.requested.contains_key(&endpoint)
            || state.requested.len() >= MAX_PENDING
        {
            return;
        }
        let Some(&(relay, _)) = state.known.get(&endpoint) else {
            return;
        };
        let Some(peer) = state.peers.get(&relay).filter(|peer| peer.supported) else {
            return;
        };
        if peer
            .commands
            .try_send(Message {
                endpoint,
                kind: Kind::Rendezvous,
            })
            .is_ok()
        {
            state
                .requested
                .insert(endpoint, (relay, now + ATTEMPT_LIFETIME));
        }
    }
}

pub(crate) struct PeerHandle {
    hub: Arc<Hub>,
    id: PeerId,
    receiver: mpsc::Receiver<Message>,
}

impl PeerHandle {
    /// Updates observed/reachable address and negotiated support. A private
    /// or disabled hub remains inert even if the remote advertises support.
    pub(crate) fn update(
        &self,
        endpoint: Option<SocketAddr>,
        supported: bool,
        local: Option<SocketAddr>,
    ) {
        let mut state = self.hub.lock();
        if let Some(peer) = state.peers.get_mut(&self.id) {
            peer.endpoint = endpoint.filter(|addr| plausible(*addr));
            peer.supported = supported;
            if let Some(local) = local.filter(|addr| plausible(*addr))
                && state.local.len() < MAX_PEERS
            {
                state.local.insert(local);
            }
        }
    }

    /// Remembers PEX provenance, retaining a bounded recent subset.
    pub(crate) fn learned(&self, endpoints: impl IntoIterator<Item = SocketAddr>, now: Instant) {
        let mut state = self.hub.lock();
        state.expire(now);
        if !state.peers.get(&self.id).is_some_and(|peer| peer.supported) {
            return;
        }
        for endpoint in endpoints
            .into_iter()
            .take(50)
            .filter(|addr| plausible(*addr))
        {
            if state.known.len() >= MAX_KNOWN && !state.known.contains_key(&endpoint) {
                break;
            }
            state
                .known
                .insert(endpoint, (self.id, now + PROVENANCE_LIFETIME));
        }
    }

    /// Handles a negotiated message. CONNECT only queues a request; the
    /// swarm still checks blocklists, bans, connection slots and UDP policy.
    pub(crate) fn received(&self, message: Message, now: Instant) {
        let mut state = self.hub.lock();
        state.expire(now);
        let Some(peer) = state.peers.get_mut(&self.id).filter(|peer| peer.supported) else {
            return;
        };
        match message.kind {
            Kind::Error(_) => {
                if state
                    .requested
                    .get(&message.endpoint)
                    .is_some_and(|(id, _)| *id == self.id)
                {
                    state.requested.remove(&message.endpoint);
                }
            }
            Kind::Connect => {
                if !plausible(message.endpoint)
                    || state.local.contains(&message.endpoint)
                    || state.connected(message.endpoint)
                    || state.connecting.contains_key(&message.endpoint)
                    || state.connecting.len() >= MAX_PENDING
                {
                    return;
                }
                if self.hub.connect.try_send(message.endpoint).is_ok() {
                    state
                        .connecting
                        .insert(message.endpoint, now + ATTEMPT_LIFETIME);
                }
            }
            Kind::Rendezvous => {
                if now < peer.relay_at {
                    return;
                }
                peer.relay_at = now + RELAY_INTERVAL;
                let source = peer.endpoint;
                let reply = peer.commands.clone();
                let error = if !plausible(message.endpoint) {
                    Some(NO_SUCH_PEER)
                } else if state.local.contains(&message.endpoint) {
                    Some(NO_SELF)
                } else {
                    match state
                        .peers
                        .values()
                        .find(|peer| peer.endpoint == Some(message.endpoint))
                    {
                        None => Some(NOT_CONNECTED),
                        Some(target) if !target.supported => Some(NO_SUPPORT),
                        Some(target) => {
                            let Some(source) = source.filter(|source| *source != message.endpoint)
                            else {
                                return;
                            };
                            // Reserve both slots before telling either participant.
                            if let (Ok(a), Ok(b)) =
                                (reply.try_reserve(), target.commands.try_reserve())
                            {
                                a.send(Message {
                                    endpoint: message.endpoint,
                                    kind: Kind::Connect,
                                });
                                b.send(Message {
                                    endpoint: source,
                                    kind: Kind::Connect,
                                });
                            }
                            None
                        }
                    }
                };
                if let Some(code) = error {
                    let _ = reply.try_send(Message {
                        endpoint: message.endpoint,
                        kind: Kind::Error(code),
                    });
                }
            }
        }
    }

    pub(crate) fn try_next(&mut self) -> Option<Message> {
        self.receiver.try_recv().ok()
    }

    /// Waits for a relay command without spinning when this hub is disabled.
    pub(crate) async fn next(&mut self) -> Message {
        match self.receiver.recv().await {
            Some(message) => message,
            None => std::future::pending().await,
        }
    }
}

impl Drop for PeerHandle {
    fn drop(&mut self) {
        let mut state = self.hub.lock();
        state.peers.remove(&self.id);
        state.known.retain(|_, (id, _)| *id != self.id);
        state.requested.retain(|_, (id, _)| *id != self.id);
    }
}

#[cfg(test)]
mod tests;
