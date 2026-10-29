//! Asking the home router to forward our listening port, so that peers
//! outside can connect to us.
//!
//! A background task asks for the TCP port (and the DHT's UDP port) with
//! PCP, NAT-PMP and UPnP in turn, whichever the router answers first, then
//! renews each mapping before its lease ends and removes it on
//! [`Portmapper::shutdown`]. None of it can fail the session: a router
//! that does not answer, or says no, is logged and shown in the header,
//! and asked again later.

mod gateway;
mod pcp;
mod pmp;
mod upnp;

use anyhow::{Context, Result, bail};
use futures_util::future::join_all;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until, timeout};
use tracing::{debug, info, warn};

pub use pcp::Pcp;
pub use pmp::Pmp;
pub use upnp::Upnp;

/// The lease asked for. Routers may give less.
const LEASE: Duration = Duration::from_secs(3600);
/// Renewals are never closer together than this.
const MIN_RENEWAL: Duration = Duration::from_secs(30);
/// When the lease is permanent, the mapping is still asked for again this
/// often, in case the router forgot it (a reboot).
const PERMANENT_RENEWAL: Duration = Duration::from_secs(3600);
/// How long after finding no router to look again.
const RETRY_AFTER_NO_ROUTER: Duration = Duration::from_secs(300);
/// How long after losing a router that worked to look again.
const RETRY_AFTER_LOSS: Duration = Duration::from_secs(30);
/// Retransmission waits for a UDP request, doubling (RFC 6886 starts at
/// 250 ms and doubles up to nine times; a few are enough on a LAN).
const RETRANSMIT: [Duration; 3] = [
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1000),
];
/// How long removing the mappings on exit may take.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// A transport protocol of a port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    /// As UPnP spells it.
    fn name(self) -> &'static str {
        match self {
            Protocol::Tcp => "TCP",
            Protocol::Udp => "UDP",
        }
    }
}

/// What a router granted for a mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Granted {
    /// The port outside.
    pub external_port: u16,
    /// How long the mapping lasts; zero for no end.
    pub lifetime: Duration,
    /// The router's outside address, if the answer said.
    pub external_ip: Option<Ipv4Addr>,
}

/// How port mapping is going, as the header shows it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Status {
    /// Turned off, or nothing asked for yet.
    #[default]
    Off,
    /// Looking for a router.
    Searching,
    /// A port is mapped.
    Mapped {
        /// The protocol that worked.
        via: &'static str,
        /// The router's outside address, if it said.
        external_ip: Option<Ipv4Addr>,
        /// The port outside that reaches our TCP port.
        external_port: u16,
    },
    /// No router mapped the port.
    Failed(String),
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Off => write!(f, "port mapping off"),
            Status::Searching => write!(f, "mapping port..."),
            Status::Mapped {
                via,
                external_ip: Some(ip),
                external_port,
            } => write!(f, "mapped {ip}:{external_port} ({via})"),
            Status::Mapped {
                via,
                external_ip: None,
                external_port,
            } => write!(f, "mapped port {external_port} ({via})"),
            Status::Failed(_) => write!(f, "port not mapped"),
        }
    }
}

/// A router found by one of the three protocols.
enum Backend {
    Pcp(Pcp),
    Pmp(Pmp),
    Upnp(Upnp),
}

impl Backend {
    fn name(&self) -> &'static str {
        match self {
            Backend::Pcp(_) => "PCP",
            Backend::Pmp(_) => "NAT-PMP",
            Backend::Upnp(_) => "UPnP",
        }
    }

    async fn map(&self, protocol: Protocol, port: u16, lifetime: Duration) -> Result<Granted> {
        match self {
            Backend::Pcp(pcp) => pcp.map(protocol, port, lifetime).await,
            Backend::Pmp(pmp) => pmp.map(protocol, port, lifetime).await,
            Backend::Upnp(upnp) => upnp.map(protocol, port, lifetime).await,
        }
    }

    /// Removes the mapping of internal `port`, which was granted
    /// `external_port`.
    async fn unmap(&self, protocol: Protocol, port: u16, external_port: u16) -> Result<()> {
        match self {
            Backend::Pcp(pcp) => pcp.unmap(protocol, port).await,
            Backend::Pmp(pmp) => pmp.unmap(protocol, port).await,
            Backend::Upnp(upnp) => upnp.unmap(protocol, external_port).await,
        }
    }

    /// The router's outside address, which PCP's answers to a mapping
    /// already hold.
    async fn external_ip(&self) -> Option<Ipv4Addr> {
        let found = match self {
            Backend::Pcp(_) => return None,
            Backend::Pmp(pmp) => pmp.external_address().await,
            Backend::Upnp(upnp) => upnp.external_ip().await,
        };
        found
            .inspect_err(|e| debug!("cannot get the external address: {e:#}"))
            .ok()
    }
}

/// Finds a router that maps `port` of `protocol` for `LEASE`, trying PCP,
/// then NAT-PMP at the gateway, then UPnP; and what it granted.
async fn discover(protocol: Protocol, port: u16) -> Result<(Backend, Granted)> {
    let gateway = gateway::default_gateway().await;
    let mut failures = Vec::new();
    if let Some(gateway) = gateway {
        let pcp = Pcp::new(gateway);
        match pcp.map(protocol, port, LEASE).await {
            Ok(granted) => return Ok((Backend::Pcp(pcp), granted)),
            Err(e) => failures.push(format!("PCP: {e:#}")),
        }
        let pmp = Pmp::new(gateway);
        match pmp.map(protocol, port, LEASE).await {
            Ok(granted) => return Ok((Backend::Pmp(pmp), granted)),
            Err(e) => failures.push(format!("NAT-PMP: {e:#}")),
        }
    } else {
        failures.push("no default gateway found".to_string());
    }
    let upnp = match Upnp::discover(gateway).await {
        Ok(upnp) => upnp,
        Err(e) => {
            failures.push(format!("UPnP: {e:#}"));
            bail!("{}", failures.join("; "));
        }
    };
    match upnp.map(protocol, port, LEASE).await {
        Ok(granted) => Ok((Backend::Upnp(upnp), granted)),
        Err(e) => {
            failures.push(format!("UPnP: {e:#}"));
            bail!("{}", failures.join("; "))
        }
    }
}

/// A UDP socket connected to `peer`, so that only it answers.
async fn connect(peer: SocketAddr) -> Result<UdpSocket> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    socket.connect(peer).await?;
    Ok(socket)
}

/// Our address on the network that reaches `peer`.
async fn local_ip_towards(peer: SocketAddr) -> Result<Ipv4Addr> {
    match connect(peer).await?.local_addr()?.ip() {
        std::net::IpAddr::V4(address) => Ok(address),
        std::net::IpAddr::V6(_) => bail!("no IPv4 route to {peer}"),
    }
}

/// Sends `request` on `socket` (connected to the router) and waits for a
/// packet that `accept` takes as the answer, sending it again after each
/// wait in `RETRANSMIT`. `accept` gives `None` for a packet that is not the
/// answer, which is ignored.
async fn transact<R>(
    socket: &UdpSocket,
    request: &[u8],
    mut accept: impl FnMut(&[u8]) -> Option<Result<R>>,
) -> Result<R> {
    let mut buf = [0u8; 1100];
    for wait in RETRANSMIT {
        socket.send(request).await.context("cannot send")?;
        let deadline = Instant::now() + wait;
        loop {
            let received = tokio::time::timeout_at(deadline, socket.recv(&mut buf)).await;
            match received {
                Err(_) => break,
                // An ICMP error from an earlier send, say.
                Ok(Err(e)) => return Err(e).context("cannot receive"),
                Ok(Ok(len)) => {
                    if let Some(answer) = accept(&buf[..len]) {
                        return answer;
                    }
                }
            }
        }
    }
    bail!("no answer")
}

/// What the owner of the task asks of it.
enum Command {
    Map(Protocol, u16),
    Shutdown(oneshot::Sender<()>),
}

struct Shared {
    status: Mutex<Status>,
}

impl Shared {
    fn set(&self, status: Status) {
        *self.status.lock().unwrap_or_else(PoisonError::into_inner) = status;
    }

    fn get(&self) -> Status {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The ports to keep mapped, the task that does it, and how it is going.
/// Cheap to clone.
#[derive(Clone)]
pub struct Portmapper {
    enabled: bool,
    shared: Arc<Shared>,
    queue: Arc<Mutex<Queue>>,
}

/// The task, started with the first port.
enum Queue {
    NotStarted,
    Running(mpsc::UnboundedSender<Command>),
}

impl Portmapper {
    /// A port mapper, which does nothing unless `enabled`. The task starts
    /// with the first call to [`Portmapper::map`].
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            shared: Arc::new(Shared {
                status: Mutex::new(Status::Off),
            }),
            queue: Arc::new(Mutex::new(Queue::NotStarted)),
        }
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Asks for `port` of `protocol` to be mapped and kept mapped, and
    /// returns at once. Must be called within the tokio runtime.
    pub fn map(&self, protocol: Protocol, port: u16) {
        if !self.enabled || port == 0 {
            return;
        }
        let mut queue = self.queue();
        let commands = match &*queue {
            Queue::Running(commands) => commands.clone(),
            Queue::NotStarted => {
                let (commands, receiver) = mpsc::unbounded_channel();
                self.shared.set(Status::Searching);
                tokio::spawn(Task::new(Arc::clone(&self.shared)).run(receiver));
                *queue = Queue::Running(commands.clone());
                commands
            }
        };
        let _ = commands.send(Command::Map(protocol, port));
    }

    /// How it is going.
    pub fn status(&self) -> Status {
        self.shared.get()
    }

    /// Removes the mappings, waiting at most `SHUTDOWN_TIMEOUT`. A router
    /// that does not answer is left with the mappings, which end with their
    /// leases.
    pub async fn shutdown(&self) {
        let commands = match &*self.queue() {
            Queue::Running(commands) => commands.clone(),
            Queue::NotStarted => return,
        };
        let (done, wait) = oneshot::channel();
        if commands.send(Command::Shutdown(done)).is_err() {
            return;
        }
        if timeout(SHUTDOWN_TIMEOUT, wait).await.is_err() {
            warn!("the router did not confirm removing the port mappings in time");
        }
    }
}

/// One port wanted mapped.
struct Entry {
    protocol: Protocol,
    port: u16,
    /// Set while the router has the mapping.
    active: Option<Active>,
}

struct Active {
    external_port: u16,
    renew_at: Instant,
}

struct Task {
    shared: Arc<Shared>,
    entries: Vec<Entry>,
    backend: Option<Backend>,
    external_ip: Option<Ipv4Addr>,
    /// When to try the entries that have no mapping again.
    retry_at: Instant,
    /// Why the last attempt failed, until one works.
    failure: Option<String>,
}

impl Task {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            entries: Vec::new(),
            backend: None,
            external_ip: None,
            retry_at: Instant::now(),
            failure: None,
        }
    }

    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        loop {
            let wake = self.next_wake();
            tokio::select! {
                command = commands.recv() => match command {
                    Some(Command::Map(protocol, port)) => {
                        if !self.entries.iter().any(|e| e.protocol == protocol && e.port == port) {
                            self.entries.push(Entry { protocol, port, active: None });
                        }
                        self.sync().await;
                    }
                    Some(Command::Shutdown(done)) => {
                        self.release().await;
                        let _ = done.send(());
                        return;
                    }
                    None => {
                        self.release().await;
                        return;
                    }
                },
                () = async {
                    match wake {
                        Some(at) => sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => self.sync().await,
            }
        }
    }

    /// When the next mapping needs asking for again.
    fn next_wake(&self) -> Option<Instant> {
        self.entries
            .iter()
            .map(|entry| match &entry.active {
                Some(active) => active.renew_at,
                None => self.retry_at,
            })
            .min()
    }

    /// Asks for each mapping that is missing or due for renewal.
    async fn sync(&mut self) {
        let now = Instant::now();
        for i in 0..self.entries.len() {
            let due = match &self.entries[i].active {
                Some(active) => active.renew_at <= now,
                None => self.retry_at <= now,
            };
            if !due {
                continue;
            }
            let (protocol, port) = (self.entries[i].protocol, self.entries[i].port);
            let granted = match &self.backend {
                Some(backend) => backend
                    .map(protocol, port, LEASE)
                    .await
                    .map_err(|e| (e, RETRY_AFTER_LOSS)),
                None => match discover(protocol, port).await {
                    Ok((backend, granted)) => {
                        info!(via = backend.name(), "router found");
                        self.backend = Some(backend);
                        Ok(granted)
                    }
                    Err(e) => Err((e, RETRY_AFTER_NO_ROUTER)),
                },
            };
            match granted {
                Ok(granted) => self.mapped(i, granted).await,
                Err((error, retry)) => {
                    self.failed(&error, retry);
                    break;
                }
            }
        }
        self.publish();
    }

    async fn mapped(&mut self, i: usize, granted: Granted) {
        let entry = &mut self.entries[i];
        let renew_in = if granted.lifetime.is_zero() {
            PERMANENT_RENEWAL
        } else {
            (granted.lifetime / 2).max(MIN_RENEWAL)
        };
        let first = entry.active.is_none();
        entry.active = Some(Active {
            external_port: granted.external_port,
            renew_at: Instant::now() + renew_in,
        });
        if first {
            info!(
                protocol = entry.protocol.name(),
                port = entry.port,
                external_port = granted.external_port,
                "port mapped"
            );
        }
        self.failure = None;
        if granted.external_ip.is_some() {
            self.external_ip = granted.external_ip;
        } else if let Some(backend) = &self.backend
            && let Some(ip) = backend.external_ip().await
        {
            self.external_ip = Some(ip);
        }
    }

    /// Gives up on the router for now. The mappings it had are as good as
    /// lost, since it may have forgotten them.
    fn failed(&mut self, error: &anyhow::Error, retry: Duration) {
        let had_router = self.backend.take().is_some();
        let text = format!("{error:#}");
        if self.failure.as_deref() != Some(&text) {
            if had_router {
                warn!("lost the router's port mapping: {text}");
            } else {
                warn!("cannot map the port on the router: {text}");
            }
        }
        for entry in &mut self.entries {
            entry.active = None;
        }
        self.failure = Some(text);
        self.retry_at = Instant::now() + retry;
    }

    /// Tells the header how it is going: the TCP mapping is the one shown.
    fn publish(&self) {
        let mapped = self
            .entries
            .iter()
            .filter(|entry| entry.active.is_some())
            .min_by_key(|entry| entry.protocol != Protocol::Tcp);
        let status = match (mapped, &self.backend, &self.failure) {
            (
                Some(Entry {
                    active: Some(active),
                    ..
                }),
                Some(backend),
                _,
            ) => Status::Mapped {
                via: backend.name(),
                external_ip: self.external_ip,
                external_port: active.external_port,
            },
            (_, _, Some(failure)) => Status::Failed(failure.clone()),
            _ => Status::Searching,
        };
        self.shared.set(status);
    }

    /// Removes every mapping the router has, all at once.
    async fn release(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        let removals = self.entries.iter().filter_map(|entry| {
            let active = entry.active.as_ref()?;
            Some((entry.protocol, entry.port, active.external_port))
        });
        let results = join_all(removals.map(|(protocol, port, external)| {
            let backend = &backend;
            async move {
                let result = backend.unmap(protocol, port, external).await;
                (protocol, port, result)
            }
        }))
        .await;
        for (protocol, port, result) in results {
            match result {
                Ok(()) => debug!(protocol = protocol.name(), port, "port mapping removed"),
                Err(e) => debug!(protocol = protocol.name(), port, "cannot remove it: {e:#}"),
            }
        }
        for entry in &mut self.entries {
            entry.active = None;
        }
    }
}

#[cfg(test)]
mod tests;
