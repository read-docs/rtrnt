//! Local Service Discovery (BEP 14): finding peers on the local network.
//!
//! Each torrent that is not private is announced on the multicast group
//! 239.192.152.143, port 6771, as a `BT-SEARCH` message (HTTP over UDP) that
//! names our listening port and the torrent's info hash. Other clients on
//! the network do the same, and the senders of the announces for our
//! torrents join the torrent's peer pool, like peers from the DHT.
//!
//! The parts that decide things (the message format, what may be announced
//! when, which torrent gets a peer) are plain code over plain data, so they
//! are tested without a socket. `Lsd` holds the socket and the task that
//! drives them.

use anyhow::{Context, Result};
use rand::Rng;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{Notify, mpsc};
use tokio::task::AbortHandle;
use tokio::time::Instant;
use tracing::{debug, info};

/// The multicast group announces are sent to.
pub const GROUP: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
/// The UDP port of the group.
pub const PORT: u16 = 6771;
/// How often a torrent is announced again.
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// The shortest time between two announces of one torrent, however often
/// one is asked for.
const MIN_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(60);
/// How often the task looks for torrents that are due.
const TICK: Duration = Duration::from_secs(10);
/// Info hashes in one announce we send, which keeps it well under an
/// Ethernet frame.
const HASHES_PER_MESSAGE: usize = 10;
/// The largest message we read; longer ones are dropped.
const MAX_MESSAGE: usize = 1400;
/// Peers found by LSD that a torrent has not taken yet; more are dropped.
const QUEUE: usize = 64;
/// The first line of every message.
const REQUEST_LINE: &str = "BT-SEARCH * HTTP/1.1";

/// What an announce says: the port its sender listens on, and the torrents
/// it has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Announce {
    pub port: u16,
    pub info_hashes: Vec<[u8; 20]>,
    /// The sender's cookie, if it sent one.
    pub cookie: Option<String>,
}

/// The message that announces `info_hashes` (not more than
/// `HASHES_PER_MESSAGE` fit a packet) on `port`, marked with `cookie` so
/// that we can tell our own from others'.
pub fn encode(info_hashes: &[[u8; 20]], port: u16, cookie: &str) -> Vec<u8> {
    let mut message = format!("{REQUEST_LINE}\r\nHost: {GROUP}:{PORT}\r\nPort: {port}\r\n");
    for hash in info_hashes {
        message.push_str("Infohash: ");
        for byte in hash {
            // Writing to a `String` cannot fail.
            let _ = write!(message, "{byte:02x}");
        }
        message.push_str("\r\n");
    }
    let _ = write!(message, "cookie: {cookie}\r\n\r\n\r\n");
    message.into_bytes()
}

/// The announce `message` holds, or `None` if it is not a well-formed one:
/// too long, not `BT-SEARCH`, without a valid `Port` or any `Infohash`,
/// or an `Infohash` that is not 40 hex digits. Header names are case-insensitive, and unknown headers are
/// ignored.
pub fn parse(message: &[u8]) -> Option<Announce> {
    if message.len() > MAX_MESSAGE {
        return None;
    }
    let text = std::str::from_utf8(message).ok()?;
    let mut lines = text.split("\r\n");
    if lines.next()? != REQUEST_LINE {
        return None;
    }
    let mut port = None;
    let mut info_hashes = Vec::new();
    let mut cookie = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':')?;
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "port" => {
                let parsed: u16 = value.parse().ok().filter(|&p| p != 0)?;
                if port.replace(parsed).is_some() {
                    return None;
                }
            }
            "infohash" => {
                info_hashes.push(parse_hash(value)?);
            }
            "cookie" => cookie = Some(value.to_owned()),
            _ => {}
        }
    }
    if info_hashes.is_empty() {
        return None;
    }
    Some(Announce {
        port: port?,
        info_hashes,
        cookie,
    })
}

/// The 20 bytes 40 hex digits spell.
fn parse_hash(text: &str) -> Option<[u8; 20]> {
    if text.len() != 40 || !text.is_ascii() {
        return None;
    }
    let mut hash = [0; 20];
    for (byte, digits) in hash.iter_mut().zip(text.as_bytes().chunks(2)) {
        *byte = u8::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()?;
    }
    Some(hash)
}

/// When each torrent may be announced.
#[derive(Default)]
struct Schedule {
    torrents: HashMap<[u8; 20], Slot>,
}

struct Slot {
    /// When the torrent was last announced.
    last: Option<Instant>,
    /// Whether an announce was asked for, as at the start of the torrent,
    /// rather than waiting for the interval.
    asked: bool,
}

impl Schedule {
    /// Wants `info_hash` announced as soon as the rate limit allows.
    fn add(&mut self, info_hash: [u8; 20]) {
        let last = self.torrents.get(&info_hash).and_then(|slot| slot.last);
        self.torrents.insert(info_hash, Slot { last, asked: true });
    }

    fn remove(&mut self, info_hash: &[u8; 20]) {
        self.torrents.remove(info_hash);
    }

    /// The torrents to announce at `now`: those asked for or whose
    /// `ANNOUNCE_INTERVAL` is over, but never one announced less than
    /// `MIN_ANNOUNCE_INTERVAL` ago. They count as announced from now on.
    fn due(&mut self, now: Instant) -> Vec<[u8; 20]> {
        let mut due = Vec::new();
        for (hash, slot) in &mut self.torrents {
            let since = slot.last.map(|at| now.saturating_duration_since(at));
            let allowed = since.is_none_or(|s| s >= MIN_ANNOUNCE_INTERVAL);
            let wanted = slot.asked || since.is_none_or(|s| s >= ANNOUNCE_INTERVAL);
            if allowed && wanted {
                slot.last = Some(now);
                slot.asked = false;
                due.push(*hash);
            }
        }
        due.sort_unstable();
        due
    }
}

/// The torrents of the session, who announces them and where peers for
/// them go.
#[derive(Default)]
struct State {
    schedule: Schedule,
    routes: HashMap<[u8; 20], mpsc::Sender<SocketAddr>>,
}

impl State {
    /// Takes `info_hash` on, unless the torrent is `private` (BEP 27: its
    /// peers come from its trackers alone, so it is neither announced nor
    /// given peers). Returns where its peers will arrive.
    fn register(
        &mut self,
        info_hash: [u8; 20],
        private: bool,
    ) -> Option<mpsc::Receiver<SocketAddr>> {
        if private {
            return None;
        }
        let (tx, rx) = mpsc::channel(QUEUE);
        self.routes.insert(info_hash, tx);
        self.schedule.add(info_hash);
        Some(rx)
    }

    /// Forgets `info_hash`. Its last announce is remembered by nobody, so
    /// a torrent stopped and started again within a minute is announced
    /// at once; that is the one way the rate limit is not kept, and it
    /// takes a user action each time.
    fn unregister(&mut self, info_hash: &[u8; 20]) {
        self.routes.remove(info_hash);
        self.schedule.remove(info_hash);
    }

    /// Hands the sender of `announce`, at `ip`, to each torrent of ours it
    /// names. Returns how many torrents it was for.
    fn deliver(&self, announce: &Announce, ip: IpAddr) -> usize {
        if ip.is_unspecified() || ip.is_multicast() {
            return 0;
        }
        let peer = SocketAddr::new(ip, announce.port);
        let mut delivered = 0;
        for hash in &announce.info_hashes {
            if let Some(tx) = self.routes.get(hash) {
                // A full queue means the torrent is not keeping up: drop.
                let _ = tx.try_send(peer);
                delivered += 1;
            }
        }
        delivered
    }
}

struct Shared {
    state: Mutex<State>,
    /// Woken when a torrent is added, so that it is announced without
    /// waiting for the next tick.
    wake: Notify,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Stops the task when the last `Lsd` is dropped.
struct TaskGuard(AbortHandle);

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Local Service Discovery for a session: one socket on the multicast
/// group, shared by all its torrents. Cheap to clone.
#[derive(Clone)]
pub struct Lsd {
    shared: Arc<Shared>,
    _task: Arc<TaskGuard>,
}

impl Lsd {
    /// Joins the multicast group on UDP port 6771, announcing `port` as the
    /// one we listen on. Other clients on the machine use the port too, so
    /// it is bound shared. Fails where the port or the group cannot be had.
    pub fn bind(port: u16) -> Result<Self> {
        let socket = bind_socket().context("cannot join the LSD multicast group")?;
        let shared = Arc::new(Shared {
            state: Mutex::default(),
            wake: Notify::new(),
        });
        let cookie = format!("rtrnt-{:08x}", rand::rng().random::<u32>());
        let task = tokio::spawn(run(socket, Arc::clone(&shared), port, cookie));
        info!(port = PORT, "local service discovery started");
        Ok(Self {
            shared,
            _task: Arc::new(TaskGuard(task.abort_handle())),
        })
    }

    /// Announces the torrent `info_hash` from now on and returns the peers
    /// that announce it, until the result is dropped. `None` for a private
    /// torrent, which LSD stays out of.
    pub fn register(&self, info_hash: [u8; 20], private: bool) -> Option<LsdPeers> {
        let rx = self.shared.lock().register(info_hash, private)?;
        self.shared.wake.notify_one();
        Some(LsdPeers {
            info_hash,
            rx,
            shared: Arc::clone(&self.shared),
        })
    }
}

/// The peers on the local network that have a torrent. The torrent is
/// announced while this exists.
pub struct LsdPeers {
    info_hash: [u8; 20],
    rx: mpsc::Receiver<SocketAddr>,
    shared: Arc<Shared>,
}

impl LsdPeers {
    /// Waits for peers and returns those that have arrived. Cancel-safe.
    pub async fn next(&mut self) -> Vec<SocketAddr> {
        let Some(first) = self.rx.recv().await else {
            return std::future::pending().await;
        };
        let mut peers = vec![first];
        while let Ok(peer) = self.rx.try_recv() {
            peers.push(peer);
        }
        peers
    }
}

impl Drop for LsdPeers {
    fn drop(&mut self) {
        self.shared.lock().unregister(&self.info_hash);
    }
}

/// Waits for LSD peers, or forever without LSD. Cancel-safe.
pub async fn lsd_peers(peers: &mut Option<LsdPeers>) -> Vec<SocketAddr> {
    match peers {
        Some(peers) => peers.next().await,
        None => std::future::pending().await,
    }
}

/// A UDP socket on port 6771 that is in the group, shared with other
/// listeners on the machine.
fn bind_socket() -> Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT).into())?;
    socket.join_multicast_v4(&GROUP, &Ipv4Addr::UNSPECIFIED)?;
    // Not past the local network. Loopback stays on so that clients on this
    // machine find each other; our own announces are told apart by cookie.
    socket.set_multicast_ttl_v4(1)?;
    socket.set_multicast_loop_v4(true)?;
    socket.set_nonblocking(true)?;
    Ok(UdpSocket::from_std(socket.into())?)
}

/// Reads announces and sends ours, until aborted.
async fn run(socket: UdpSocket, shared: Arc<Shared>, port: u16, cookie: String) {
    let group = SocketAddr::from((GROUP, PORT));
    let mut tick = tokio::time::interval(TICK);
    // One byte more than the longest message we accept, so that a longer
    // one is seen as such.
    let mut buf = [0u8; MAX_MESSAGE + 1];
    loop {
        tokio::select! {
            received = socket.recv_from(&mut buf) => match received {
                Ok((len, from)) => {
                    let Some(announce) = parse(&buf[..len]) else {
                        continue;
                    };
                    if announce.cookie.as_deref() == Some(cookie.as_str()) {
                        continue;
                    }
                    let count = shared.lock().deliver(&announce, from.ip());
                    if count > 0 {
                        debug!(peer = %from.ip(), port = announce.port, count, "LSD announce");
                    }
                }
                // E.g. an ICMP error from an earlier send; keep going, but
                // not in a busy loop.
                Err(e) => {
                    debug!("LSD receive failed: {e}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            },
            _ = tick.tick() => {}
            () = shared.wake.notified() => {}
        }
        let due = shared.lock().schedule.due(Instant::now());
        for hashes in due.chunks(HASHES_PER_MESSAGE) {
            let message = encode(hashes, port, &cookie);
            if let Err(e) = socket.send_to(&message, group).await {
                debug!("LSD announce failed: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests;
