//! Incoming peers, shared by every torrent of a session. A peer's handshake
//! names the torrent it wants, and the connection is handed to that
//! torrent, which answers it, or dropped if we do not have the torrent.

use crate::peer::Incoming;
use anyhow::{Result, bail};
use rand::Rng;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{debug, info, warn};

/// Incoming connections whose handshake we are still reading; more are
/// dropped at once, so idle connections cannot pile up.
const MAX_HANDSHAKING: usize = 64;
/// Handed-over connections a torrent has not taken yet; more are dropped.
const QUEUE: usize = 8;
/// How long to wait before accepting again after an error, such as running
/// out of file descriptors.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// Where to hand each torrent's incoming peers, by info hash.
#[derive(Default)]
struct Routes(Mutex<HashMap<[u8; 20], mpsc::Sender<Incoming>>>);

impl Routes {
    fn lock(&self) -> MutexGuard<'_, HashMap<[u8; 20], mpsc::Sender<Incoming>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Accepts peers on a set of sockets and hands each to the torrent it asks
/// for. Stops listening once dropped, along with every `Registration`.
pub struct Listener {
    port: Option<u16>,
    routes: Arc<Routes>,
    task: Option<AbortHandle>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

impl Listener {
    /// Listens on `port`, or a free port if it is taken, over IPv4 and IPv6
    /// (see `listen`).
    pub async fn bind(port: u16) -> Arc<Self> {
        let listener = Self::start(listen(port).await);
        if let Some(port) = listener.port {
            info!(port, "listening for peers");
        }
        listener
    }

    /// Accepts peers on `listeners`, the first of which gives the port told
    /// to trackers and peers. Without any, no peer can connect to us.
    pub fn start(listeners: Vec<TcpListener>) -> Arc<Self> {
        let port = listeners
            .first()
            .and_then(|listener| listener.local_addr().ok())
            .map(|addr| addr.port());
        let routes = Arc::new(Routes::default());
        let task = (!listeners.is_empty())
            .then(|| tokio::spawn(route(listeners, Arc::clone(&routes))).abort_handle());
        Arc::new(Self { port, routes, task })
    }

    /// The port peers can reach us on, if we listen at all.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Starts handing the peers that ask for `info_hash` to the returned
    /// registration, until it is dropped. Fails if another registration
    /// has the torrent.
    pub fn register(self: &Arc<Self>, info_hash: [u8; 20]) -> Result<Registration> {
        let (tx, rx) = mpsc::channel(QUEUE);
        match self.routes.lock().entry(info_hash) {
            Entry::Occupied(_) => bail!("already accepting peers for this torrent"),
            Entry::Vacant(entry) => entry.insert(tx),
        };
        Ok(Registration {
            listener: Arc::clone(self),
            info_hash,
            rx,
        })
    }
}

/// One torrent's incoming peers.
pub struct Registration {
    listener: Arc<Listener>,
    info_hash: [u8; 20],
    rx: mpsc::Receiver<Incoming>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.listener.routes.lock().remove(&self.info_hash);
    }
}

impl Registration {
    /// The port peers can reach us on, if we listen at all.
    pub fn port(&self) -> Option<u16> {
        self.listener.port
    }

    /// The next peer that asked for the torrent, with its handshake read
    /// but not answered. Waits forever if we do not listen. Cancel-safe.
    pub async fn next(&mut self) -> Incoming {
        match self.rx.recv().await {
            Some(incoming) => incoming,
            None => std::future::pending().await,
        }
    }
}

/// Accepts peers on `listeners` and reads their handshakes, each on a task
/// of its own so a slow peer holds up nobody, and hands each to the torrent
/// it asks for in `routes`.
async fn route(listeners: Vec<TcpListener>, routes: Arc<Routes>) {
    let mut handshakes = JoinSet::new();
    loop {
        tokio::select! {
            (socket, addr) = accept(&listeners) => {
                if handshakes.len() >= MAX_HANDSHAKING {
                    debug!(%addr, "turning away incoming peer: too many handshakes");
                    continue;
                }
                let routes = Arc::clone(&routes);
                handshakes.spawn(async move {
                    let incoming = match Incoming::read(socket, addr).await {
                        Ok(incoming) => incoming,
                        Err(e) => {
                            debug!(%addr, "incoming peer dropped: {e:#}");
                            return;
                        }
                    };
                    let route = routes.lock().get(&incoming.info_hash).cloned();
                    match route {
                        Some(route) => {
                            if route.try_send(incoming).is_err() {
                                debug!(%addr, "turning away incoming peer: torrent busy");
                            }
                        }
                        None => debug!(%addr, "turning away incoming peer: unknown torrent"),
                    }
                });
            }
            Some(_) = handshakes.join_next() => {}
        }
    }
}

/// Listens for incoming peers on `port`, or on a free port if that one is
/// taken (say, by another download): over IPv4, and over IPv6 on the port
/// the IPv4 listener got, so trackers can be told a single port. The IPv4
/// listener comes first. Without IPv6 we listen over IPv4 only, and without
/// any listener we still download, over the connections we open ourselves.
pub async fn listen(port: u16) -> Vec<TcpListener> {
    let bind = |port| TcpListener::bind((Ipv4Addr::UNSPECIFIED, port));
    let result = match bind(port).await {
        Err(e) if port != 0 => {
            warn!(port, "cannot listen on port: {e}; using a free one instead");
            bind(0).await
        }
        result => result,
    };
    let v4 = result
        .inspect_err(|e| warn!("cannot listen for peers: {e}"))
        .ok();
    let port = match v4.as_ref().map(TcpListener::local_addr) {
        Some(Ok(addr)) => addr.port(),
        _ => port,
    };
    let v6 = listen_v6(port)
        .inspect_err(|e| info!(port, "cannot listen for IPv6 peers: {e}"))
        .ok();
    v4.into_iter().chain(v6).collect()
}

/// Listens on `[::]:port` for IPv6 only, so that the socket does not also
/// claim the port for IPv4.
fn listen_v6(port: u16) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(true)?;
    // As `TcpListener::bind` does on Unix, so a restart can reuse the port.
    #[cfg(unix)]
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
}

/// Accepts the next incoming connection on any of `listeners`, or waits
/// forever without one. Cancel-safe.
async fn accept(listeners: &[TcpListener]) -> (TcpStream, SocketAddr) {
    let next = || {
        // Start at a random listener, so a busy one cannot starve the rest.
        let start = rand::rng().random_range(0..listeners.len().max(1));
        std::future::poll_fn(move |cx| {
            listeners
                .iter()
                .cycle()
                .skip(start)
                .take(listeners.len())
                .find_map(|listener| match listener.poll_accept(cx) {
                    Poll::Ready(result) => Some(result),
                    Poll::Pending => None,
                })
                .map_or(Poll::Pending, Poll::Ready)
        })
    };
    loop {
        match next().await {
            Ok(incoming) => return incoming,
            Err(e) => {
                warn!("cannot accept a peer: {e}");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
