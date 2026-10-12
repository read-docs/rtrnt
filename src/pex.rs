//! Peer exchange (BEP 11).
//!
//! Peers that both list `ut_pex` in their extension handshake (BEP 10) tell
//! each other, about once a minute, which peers they have connected to and
//! disconnected from since their last message. The first message lists every
//! current peer. Each peer goes in compact form (4-byte IPv4 or 16-byte IPv6
//! address and 2-byte port, in network order), with a byte of flags per
//! added peer. Private torrents (BEP 27) never use it.
//!
//! [`PeerExchange`] is the download's side: the connected peers whose
//! listening address we know, and a channel the peers learned go out on.
//! Each connection has a [`PexPeer`], which advertises its own peer there,
//! sends its peer what changed, and passes on what that peer tells us.

use crate::bencode;
use crate::peer::{PeerConnection, ProtocolViolation};
use crate::tracker::{parse_compact_v4, parse_compact_v6};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_bencode::value::Value;
use serde_bytes::ByteBuf;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::debug;

/// The extension's name in the extension handshake's `m`.
pub const NAME: &str = "ut_pex";
/// The extended message id we want peer exchange messages sent with. The
/// one most clients use.
pub const ID: u8 = 1;
/// Most peers added in one message, sent or taken, as BEP 11 suggests.
pub const MAX_ADDED: usize = 50;
/// Most peers dropped in one message we send.
pub const MAX_DROPPED: usize = 50;
/// How often a peer is told what changed.
pub const SEND_INTERVAL: Duration = Duration::from_secs(60);
/// Until the first message to a peer has gone out, how often we look for
/// something to tell it, so it hears of the swarm soon after connecting.
const FIRST_RETRY: Duration = Duration::from_secs(1);
/// Messages that arrive sooner than this after the last one taken from the
/// same peer are ignored, so a peer cannot flood us with addresses.
pub const MIN_RECEIVE_INTERVAL: Duration = Duration::from_secs(30);
/// Flag: the peer prefers encrypted connections. We never set it, since we
/// do not know.
pub const FLAG_ENCRYPTION: u8 = 0x01;
/// Flag: the peer is a seed (or only uploads).
pub const FLAG_SEED: u8 = 0x02;
/// Flag: the peer is reachable; we connected to it.
pub const FLAG_CONNECTABLE: u8 = 0x10;

/// A peer exchange message.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PexMessage {
    /// Peers the sender connected to since its last message, with their
    /// flags (`FLAG_*`).
    pub added: Vec<(SocketAddr, u8)>,
    /// Peers the sender disconnected from since its last message.
    pub dropped: Vec<SocketAddr>,
}

/// A message as it goes on the wire. `serde_bencode` sorts the keys.
#[derive(Serialize)]
struct Wire {
    added: ByteBuf,
    #[serde(rename = "added.f")]
    added_flags: ByteBuf,
    added6: ByteBuf,
    #[serde(rename = "added6.f")]
    added6_flags: ByteBuf,
    dropped: ByteBuf,
    dropped6: ByteBuf,
}

impl PexMessage {
    /// Whether the message says nothing.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.dropped.is_empty()
    }

    /// Serializes the message into an extended message payload. IPv4 and
    /// IPv6 peers go in separate keys, and IPv4-mapped IPv6 addresses count
    /// as IPv4. Every key is present, even if empty.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut wire = Wire {
            added: ByteBuf::new(),
            added_flags: ByteBuf::new(),
            added6: ByteBuf::new(),
            added6_flags: ByteBuf::new(),
            dropped: ByteBuf::new(),
            dropped6: ByteBuf::new(),
        };
        for &(addr, flags) in &self.added {
            if push_compact(&mut wire.added, &mut wire.added6, addr) {
                wire.added_flags.push(flags);
            } else {
                wire.added6_flags.push(flags);
            }
        }
        for &addr in &self.dropped {
            push_compact(&mut wire.dropped, &mut wire.dropped6, addr);
        }
        serde_bencode::to_bytes(&wire).context("cannot encode peer exchange message")
    }

    /// Parses an extended message payload. Parsing is lenient, as for the
    /// extension handshake: unknown keys are ignored, and so are known keys
    /// with the wrong type, a partial address at the end of a list, and
    /// flags missing (taken as 0) or left over. Only a payload that is not a
    /// bencoded dictionary is an error. Bytes after the dictionary are
    /// ignored.
    pub fn decode(payload: &[u8]) -> Result<Self> {
        // Scanned first because it limits the nesting depth, which
        // `serde_bencode` would follow until the stack overflows.
        let len = bencode::value_len(payload).context("peer exchange message is not bencoded")?;
        let value: Value = serde_bencode::from_bytes(&payload[..len])
            .context("peer exchange message is not bencoded")?;
        let Value::Dict(dict) = value else {
            bail!("peer exchange message is not a dictionary");
        };
        let bytes = |key: &[u8]| match dict.get(key) {
            Some(Value::Bytes(bytes)) => bytes.as_slice(),
            _ => &[],
        };
        let with_flags = |peers: Vec<SocketAddr>, flags: &[u8]| {
            let flags = flags.iter().copied().chain(std::iter::repeat(0));
            peers.into_iter().zip(flags).collect::<Vec<_>>()
        };
        let mut added = with_flags(parse_compact_v4(bytes(b"added")), bytes(b"added.f"));
        added.extend(with_flags(
            parse_compact_v6(bytes(b"added6")),
            bytes(b"added6.f"),
        ));
        let mut dropped = parse_compact_v4(bytes(b"dropped"));
        dropped.extend(parse_compact_v6(bytes(b"dropped6")));
        Ok(Self { added, dropped })
    }
}

/// Appends `addr` in compact form to `v4` or `v6`, and returns whether it
/// went to `v4`.
fn push_compact(v4: &mut ByteBuf, v6: &mut ByteBuf, addr: SocketAddr) -> bool {
    let port = addr.port().to_be_bytes();
    match addr.ip().to_canonical() {
        IpAddr::V4(ip) => {
            v4.extend_from_slice(&ip.octets());
            v4.extend_from_slice(&port);
            true
        }
        IpAddr::V6(ip) => {
            v6.extend_from_slice(&ip.octets());
            v6.extend_from_slice(&port);
            false
        }
    }
}

/// `addr` with an IPv4-mapped IPv6 address turned into IPv4, so that one
/// peer is always written the same way.
fn canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

/// Whether `addr` could be a peer at all: not port 0, nor an unspecified,
/// multicast or broadcast address.
fn is_plausible(addr: &SocketAddr) -> bool {
    let ip = addr.ip().to_canonical();
    let broadcast = matches!(ip, IpAddr::V4(ip) if ip.is_broadcast());
    addr.port() != 0 && !ip.is_unspecified() && !ip.is_multicast() && !broadcast
}

/// The peers of `message` worth connecting to: among its first `MAX_ADDED`
/// added peers, the plausible ones other than `sender` (the peer that sent
/// it, under any of its addresses), leaving out seeds when `seeding`.
fn worth_trying(message: &PexMessage, seeding: bool, sender: &[SocketAddr]) -> Vec<SocketAddr> {
    message
        .added
        .iter()
        .take(MAX_ADDED)
        .filter(|&&(_, flags)| !(seeding && flags & FLAG_SEED != 0))
        .map(|&(addr, _)| canonical(addr))
        .filter(|addr| is_plausible(addr) && !sender.contains(addr))
        .collect()
}

/// The next message to a peer that has been told about the peers in
/// `sent`, now that we are connected to `current`: the peers it has not
/// heard of as added, and those gone as dropped, at most `MAX_ADDED` and
/// `MAX_DROPPED`, lowest address first. `sent` is updated to what the peer
/// knows once the message is sent; whatever did not fit goes in the next
/// message.
fn next_message(sent: &mut BTreeSet<SocketAddr>, current: &BTreeMap<SocketAddr, u8>) -> PexMessage {
    let added: Vec<(SocketAddr, u8)> = current
        .iter()
        .filter(|(addr, _)| !sent.contains(addr))
        .take(MAX_ADDED)
        .map(|(&addr, &flags)| (addr, flags))
        .collect();
    let dropped: Vec<SocketAddr> = sent
        .iter()
        .filter(|addr| !current.contains_key(addr))
        .take(MAX_DROPPED)
        .copied()
        .collect();
    sent.extend(added.iter().map(|&(addr, _)| addr));
    for addr in &dropped {
        sent.remove(addr);
    }
    PexMessage { added, dropped }
}

/// What a connection tells other peers about its own peer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Advertised {
    /// Where the peer accepts connections, if we know.
    addr: Option<SocketAddr>,
    /// `FLAG_*`.
    flags: u8,
}

/// The peer exchange of one download: the peers we are connected to that
/// may be advertised, and where the peers learned from others go.
pub(crate) struct PeerExchange {
    /// Off for private torrents: nothing is advertised, sent or taken.
    enabled: bool,
    /// The connected peers, by peer id.
    peers: Mutex<HashMap<[u8; 20], Advertised>>,
    learned: mpsc::UnboundedSender<Vec<SocketAddr>>,
}

impl PeerExchange {
    /// A peer exchange, `enabled` unless the torrent is private, and the
    /// receiving end for the peers learned through it.
    pub(crate) fn new(enabled: bool) -> (Self, mpsc::UnboundedReceiver<Vec<SocketAddr>>) {
        let (learned, receiver) = mpsc::unbounded_channel();
        let exchange = Self {
            enabled,
            peers: Mutex::default(),
            learned,
        };
        (exchange, receiver)
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<[u8; 20], Advertised>> {
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts the peer exchange with the peer with id `id`, which we are
    /// connected to at `addr`, over a connection we opened if `outbound`.
    /// The peer is advertised to others while the returned handle lives,
    /// once its listening address is known.
    pub(crate) fn join(&self, id: [u8; 20], addr: SocketAddr, outbound: bool) -> PexPeer<'_> {
        let now = Instant::now();
        let mut peer = PexPeer {
            exchange: self,
            id,
            addr: canonical(addr),
            outbound,
            seed: false,
            advertised: Advertised::default(),
            sent: BTreeSet::new(),
            sent_any: false,
            send_at: now,
            last_received: None,
        };
        if self.enabled {
            self.lock().insert(id, Advertised::default());
            if outbound {
                peer.advertise(Some(addr), FLAG_CONNECTABLE);
            }
        }
        peer
    }

    /// The connected peers with a known listening address, other than the
    /// one with id `me`, and other than any at an address in `exclude`.
    fn others(&self, me: &[u8; 20], exclude: &[SocketAddr]) -> BTreeMap<SocketAddr, u8> {
        self.lock()
            .iter()
            .filter(|&(id, _)| id != me)
            .filter_map(|(_, advertised)| Some((advertised.addr?, advertised.flags)))
            .filter(|(addr, _)| !exclude.contains(addr))
            .collect()
    }
}

/// The peer exchange over one connection. Dropping it stops advertising the
/// peer.
pub(crate) struct PexPeer<'a> {
    exchange: &'a PeerExchange,
    /// The peer's id, its key in the exchange.
    id: [u8; 20],
    /// The address we are connected to.
    addr: SocketAddr,
    /// Whether we opened the connection, so `addr` is where the peer
    /// listens.
    outbound: bool,
    /// Whether the peer has every piece.
    seed: bool,
    /// What the exchange holds about the peer.
    advertised: Advertised,
    /// The peers the peer has been told about, and not told are dropped.
    sent: BTreeSet<SocketAddr>,
    /// Whether a message has gone out to the peer.
    sent_any: bool,
    /// When to look for news to send the peer next.
    send_at: Instant,
    /// When we last took a message from the peer.
    last_received: Option<Instant>,
}

impl PexPeer<'_> {
    /// Records whether the peer has every piece, which other peers are told.
    pub(crate) fn set_seed(&mut self, seed: bool) {
        self.seed = seed;
    }

    /// Brings what the exchange advertises about the peer up to date with
    /// `conn`, and sends the peer what changed in the swarm, if it supports
    /// peer exchange and a message is due. The first message goes out as
    /// soon as there is a peer to tell about, and later ones every
    /// `SEND_INTERVAL`. Cheap when nothing is due, so it can be called on
    /// every turn of a worker's loop.
    pub(crate) async fn tick(&mut self, conn: &mut PeerConnection, now: Instant) -> Result<()> {
        if !self.exchange.enabled {
            return Ok(());
        }
        // An incoming peer's listening port is in its extension handshake.
        let listen_port = conn
            .peer_extensions()
            .and_then(|handshake| handshake.listen_port);
        let addr = match (self.outbound, listen_port) {
            (true, _) => Some(self.addr),
            (false, Some(port)) => Some(SocketAddr::new(self.addr.ip(), port)),
            (false, None) => None,
        };
        let mut flags = 0;
        if self.outbound {
            flags |= FLAG_CONNECTABLE;
        }
        if self.seed {
            flags |= FLAG_SEED;
        }
        self.advertise(addr, flags);

        if now < self.send_at
            || conn
                .peer_extensions()
                .and_then(|handshake| handshake.id(NAME))
                .is_none()
        {
            return Ok(());
        }
        let current = self.exchange.others(&self.id, &self.own_addrs());
        let message = next_message(&mut self.sent, &current);
        if message.is_empty() {
            let wait = if self.sent_any {
                SEND_INTERVAL
            } else {
                FIRST_RETRY
            };
            self.send_at = now + wait;
            return Ok(());
        }
        self.sent_any = true;
        self.send_at = now + SEND_INTERVAL;
        debug!(
            addr = %self.addr,
            added = message.added.len(),
            dropped = message.dropped.len(),
            "sending peer exchange"
        );
        conn.send_extended(NAME, message.encode()?).await
    }

    /// Handles a peer exchange message from the peer: up to `MAX_ADDED` of
    /// the peers it added go out to the download's pool, leaving out seeds
    /// when `seeding`. A message that comes within `MIN_RECEIVE_INTERVAL` of
    /// the last one taken is ignored, and so is every message for a private
    /// torrent. One that is not a bencoded dictionary is a protocol
    /// violation.
    pub(crate) fn received(&mut self, payload: &[u8], now: Instant, seeding: bool) -> Result<()> {
        if !self.exchange.enabled {
            return Ok(());
        }
        let message = PexMessage::decode(payload)
            .context(ProtocolViolation("malformed peer exchange message"))?;
        if let Some(last) = self.last_received
            && now.saturating_duration_since(last) < MIN_RECEIVE_INTERVAL
        {
            debug!(addr = %self.addr, "ignoring peer exchange message: too soon after the last");
            return Ok(());
        }
        self.last_received = Some(now);
        let peers = worth_trying(&message, seeding, &self.own_addrs());
        debug!(
            addr = %self.addr,
            added = message.added.len(),
            taken = peers.len(),
            "received peer exchange"
        );
        if !peers.is_empty() {
            // The download only stops listening once every worker is gone.
            let _ = self.exchange.learned.send(peers);
        }
        Ok(())
    }

    /// The peer's addresses: where we are connected to it, and where it
    /// listens if we know.
    fn own_addrs(&self) -> Vec<SocketAddr> {
        std::iter::once(self.addr)
            .chain(self.advertised.addr)
            .collect()
    }

    /// Advertises the peer as listening at `addr` (or not at all without
    /// one) with `flags`.
    fn advertise(&mut self, addr: Option<SocketAddr>, flags: u8) {
        let advertised = Advertised {
            addr: addr.map(canonical),
            flags,
        };
        if advertised != self.advertised && self.exchange.enabled {
            self.exchange.lock().insert(self.id, advertised);
            self.advertised = advertised;
        }
    }
}

impl Drop for PexPeer<'_> {
    fn drop(&mut self) {
        if self.exchange.enabled {
            self.exchange.lock().remove(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v4(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn v6(port: u16) -> SocketAddr {
        SocketAddr::new("2001:db8::1".parse().unwrap(), port)
    }

    #[test]
    fn encodes_each_family_in_its_own_keys() {
        let message = PexMessage {
            added: vec![
                (v4(0x1ae1), FLAG_SEED),
                (v6(2), FLAG_CONNECTABLE),
                ("[::ffff:10.0.0.2]:3".parse().unwrap(), 0),
            ],
            dropped: vec![v6(4), v4(5)],
        };
        let payload = message.encode().unwrap();
        let mut expected = b"d5:added12:\x0a\x00\x00\x01\x1a\xe1\x0a\x00\x00\x02\x00\x03".to_vec();
        expected.extend_from_slice(b"7:added.f2:\x02\x00");
        expected.extend_from_slice(b"6:added618:");
        expected.extend_from_slice(&v6_bytes(2));
        expected.extend_from_slice(b"8:added6.f1:\x10");
        expected.extend_from_slice(b"7:dropped6:\x0a\x00\x00\x01\x00\x05");
        expected.extend_from_slice(b"8:dropped618:");
        expected.extend_from_slice(&v6_bytes(4));
        expected.push(b'e');
        assert_eq!(payload, expected);

        let decoded = PexMessage::decode(&payload).unwrap();
        assert_eq!(
            decoded.added,
            [
                (v4(0x1ae1), FLAG_SEED),
                ("10.0.0.2:3".parse().unwrap(), 0),
                (v6(2), FLAG_CONNECTABLE),
            ]
        );
        assert_eq!(decoded.dropped, [v4(5), v6(4)]);
    }

    fn v6_bytes(port: u16) -> Vec<u8> {
        let SocketAddr::V6(addr) = v6(port) else {
            unreachable!()
        };
        let mut bytes = addr.ip().octets().to_vec();
        bytes.extend_from_slice(&port.to_be_bytes());
        bytes
    }

    #[test]
    fn an_empty_message_has_every_key() {
        let message = PexMessage::default();
        assert!(message.is_empty());
        let payload = message.encode().unwrap();
        assert_eq!(
            payload,
            b"d5:added0:7:added.f0:6:added60:8:added6.f0:7:dropped0:8:dropped60:e"
        );
        assert_eq!(PexMessage::decode(&payload).unwrap(), message);
    }

    #[test]
    fn decoding_is_lenient() {
        // No keys at all, wrong types, unknown keys and trailing bytes.
        assert_eq!(PexMessage::decode(b"de").unwrap(), PexMessage::default());
        let payload = b"d5:addedi5e7:dropped3:abc1:xi1eeextra";
        assert_eq!(PexMessage::decode(payload).unwrap(), PexMessage::default());
        // A partial address at the end, flags missing for the second peer,
        // and flags for a peer that is not there.
        let payload = b"d5:added14:\x0a\x00\x00\x01\x00\x01\x0a\x00\x00\x01\x00\x02\xff\xff\
            7:added.f1:\x028:added6.f3:\x01\x01\x01e";
        let message = PexMessage::decode(payload).unwrap();
        assert_eq!(message.added, [(v4(1), FLAG_SEED), (v4(2), 0)]);
        assert!(message.dropped.is_empty());
    }

    #[test]
    fn rejects_payloads_that_are_not_a_dictionary() {
        let cases: &[&[u8]] = &[b"", b"le", b"i1e", b"5:added", b"d5:added", b"\xff"];
        for case in cases {
            assert!(PexMessage::decode(case).is_err(), "accepted {case:?}");
        }
        let mut deep = b"d1:x".to_vec();
        deep.extend(std::iter::repeat_n(b'l', 100_000));
        assert!(PexMessage::decode(&deep).is_err());
    }

    #[test]
    fn first_message_lists_everyone_and_later_ones_what_changed() {
        let mut sent = BTreeSet::new();
        let mut current: BTreeMap<SocketAddr, u8> =
            [(v4(1), 0), (v6(2), FLAG_SEED)].into_iter().collect();
        let first = next_message(&mut sent, &current);
        assert_eq!(first.added, [(v4(1), 0), (v6(2), FLAG_SEED)]);
        assert!(first.dropped.is_empty());
        assert!(next_message(&mut sent, &current).is_empty());

        current.remove(&v4(1));
        current.insert(v4(3), FLAG_CONNECTABLE);
        let second = next_message(&mut sent, &current);
        assert_eq!(second.added, [(v4(3), FLAG_CONNECTABLE)]);
        assert_eq!(second.dropped, [v4(1)]);
        assert_eq!(sent, [v4(3), v6(2)].into());
        // A flag that changes does not make the peer news.
        current.insert(v4(3), FLAG_CONNECTABLE | FLAG_SEED);
        assert!(next_message(&mut sent, &current).is_empty());
    }

    #[test]
    fn messages_hold_at_most_fifty_added_and_fifty_dropped() {
        let mut sent = BTreeSet::new();
        let mut current: BTreeMap<SocketAddr, u8> = (1..=120).map(|port| (v4(port), 0)).collect();
        let sizes = |message: &PexMessage| (message.added.len(), message.dropped.len());
        assert_eq!(sizes(&next_message(&mut sent, &current)), (50, 0));
        assert_eq!(sizes(&next_message(&mut sent, &current)), (50, 0));
        current.clear();
        // The 100 the peer heard of are dropped, 50 at a time. It never
        // heard of the other 20.
        let message = next_message(&mut sent, &current);
        assert_eq!(sizes(&message), (0, 50));
        assert_eq!(message.dropped[0], v4(1));
        assert_eq!(sizes(&next_message(&mut sent, &current)), (0, 50));
        assert!(next_message(&mut sent, &current).is_empty());
        assert!(sent.is_empty());
    }

    #[test]
    fn takes_at_most_fifty_plausible_peers_other_than_the_sender() {
        let sender = v4(9);
        let mut added = vec![
            (sender, 0),
            (v4(0), 0),
            ("0.0.0.0:1".parse().unwrap(), 0),
            ("224.0.0.1:1".parse().unwrap(), 0),
            ("255.255.255.255:1".parse().unwrap(), 0),
            ("[::]:1".parse().unwrap(), 0),
            ("[::ffff:10.0.0.1]:7".parse().unwrap(), 0),
            (v6(8), FLAG_SEED),
        ];
        added.extend((100..200).map(|port| (v4(port), 0)));
        let message = PexMessage {
            added,
            dropped: vec![v4(10)],
        };
        let peers = worth_trying(&message, false, &[sender]);
        // Only the first 50 entries are looked at.
        assert_eq!(peers.len(), 2 + 42);
        assert_eq!(peers[..2], [v4(7), v6(8)]);
        assert_eq!(peers[2], v4(100));
        // A seed is no use to us once we are one.
        let peers = worth_trying(&message, true, &[sender]);
        assert_eq!(peers[..2], [v4(7), v4(100)]);
    }

    #[tokio::test]
    async fn ignores_messages_that_come_too_often() {
        let (exchange, mut learned) = PeerExchange::new(true);
        let mut peer = exchange.join([1; 20], v4(9), true);
        let message = |port| {
            PexMessage {
                added: vec![(v4(port), 0)],
                dropped: Vec::new(),
            }
            .encode()
            .unwrap()
        };
        let start = Instant::now();
        peer.received(&message(1), start, false).unwrap();
        assert_eq!(learned.try_recv().unwrap(), [v4(1)]);
        let soon = start + MIN_RECEIVE_INTERVAL - Duration::from_secs(1);
        peer.received(&message(2), soon, false).unwrap();
        assert!(learned.try_recv().is_err());
        peer.received(&message(3), start + MIN_RECEIVE_INTERVAL, false)
            .unwrap();
        assert_eq!(learned.try_recv().unwrap(), [v4(3)]);
        // Malformed messages are a protocol violation, even when too soon.
        let err = peer.received(b"le", start, false).unwrap_err();
        assert!(err.is::<ProtocolViolation>(), "{err:#}");
    }

    #[tokio::test]
    async fn advertises_connected_peers_with_known_addresses_only() {
        let (exchange, _learned) = PeerExchange::new(true);
        let outgoing = exchange.join([1; 20], v4(1), true);
        let mut incoming = exchange.join([2; 20], v4(50_000), false);
        let mut target = exchange.join([3; 20], v6(3), false);
        // An incoming peer is only advertised once its listening port is
        // known.
        assert_eq!(
            exchange.others(&[3; 20], &target.own_addrs()),
            [(v4(1), FLAG_CONNECTABLE)].into()
        );
        incoming.advertise(Some(v4(2)), FLAG_SEED);
        target.advertise(Some(v6(4)), 0);
        assert_eq!(
            exchange.others(&[3; 20], &target.own_addrs()),
            [(v4(1), FLAG_CONNECTABLE), (v4(2), FLAG_SEED)].into()
        );
        // Never a peer's own address, even under another connection.
        assert_eq!(
            exchange.others(&[9; 20], &[v4(2), v6(4)]),
            [(v4(1), FLAG_CONNECTABLE)].into()
        );
        drop(outgoing);
        assert_eq!(
            exchange.others(&[3; 20], &target.own_addrs()),
            [(v4(2), FLAG_SEED)].into()
        );
    }

    #[tokio::test]
    async fn private_torrents_never_exchange_peers() {
        let (exchange, mut learned) = PeerExchange::new(false);
        let mut peer = exchange.join([1; 20], v4(9), true);
        let _other = exchange.join([2; 20], v4(1), true);
        assert!(exchange.others(&[9; 20], &[]).is_empty());
        let message = PexMessage {
            added: vec![(v4(1), 0)],
            dropped: Vec::new(),
        };
        peer.received(&message.encode().unwrap(), Instant::now(), false)
            .unwrap();
        // Not even parsed.
        peer.received(b"le", Instant::now(), false).unwrap();
        assert!(learned.try_recv().is_err());
    }
}
