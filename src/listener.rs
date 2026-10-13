//! Incoming peers, shared by every torrent of a session. A peer's handshake
//! names the torrent it wants, and the connection is handed to that
//! torrent, which answers it, or dropped if we do not have the torrent.

use crate::download;
use crate::peer::Incoming;
use anyhow::{Result, bail};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{debug, info};

/// Incoming connections whose handshake we are still reading; more are
/// dropped at once, so idle connections cannot pile up.
const MAX_HANDSHAKING: usize = 64;
/// Handed-over connections a torrent has not taken yet; more are dropped.
const QUEUE: usize = 8;

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
    /// (see `download::listen`).
    pub async fn bind(port: u16) -> Arc<Self> {
        let listener = Self::start(download::listen(port).await);
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
            (socket, addr) = download::accept(&listeners) => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::{generate_peer_id, handshake_bytes};
    use std::net::SocketAddr;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    async fn listener() -> (Arc<Listener>, SocketAddr) {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        (Listener::start(vec![socket]), addr)
    }

    /// Connects to `addr` and sends a handshake for `info_hash`.
    async fn connect(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
        let mut socket = TcpStream::connect(addr).await.unwrap();
        socket
            .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
            .await
            .unwrap();
        socket
    }

    async fn next(registration: &mut Registration) -> Incoming {
        tokio::time::timeout(Duration::from_secs(5), registration.next())
            .await
            .expect("no incoming peer")
    }

    #[tokio::test]
    async fn hands_each_peer_to_the_torrent_it_asks_for() {
        let (listener, addr) = listener().await;
        assert_eq!(listener.port(), Some(addr.port()));
        let mut a = listener.register([1; 20]).unwrap();
        let mut b = listener.register([2; 20]).unwrap();
        assert_eq!(b.port(), Some(addr.port()));

        let _to_b = connect(addr, [2; 20]).await;
        let _to_a = connect(addr, [1; 20]).await;
        assert_eq!(next(&mut b).await.info_hash, [2; 20]);
        assert_eq!(next(&mut a).await.info_hash, [1; 20]);
    }

    #[tokio::test]
    async fn drops_peers_for_torrents_nobody_registered() {
        let (listener, addr) = listener().await;
        let mut a = listener.register([1; 20]).unwrap();

        let mut stranger = connect(addr, [9; 20]).await;
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut stranger, &mut buf),
        )
        .await
        .expect("connection left open");
        assert_eq!(read.unwrap(), 0, "answered a peer for an unknown torrent");

        // Once dropped, the torrent may be registered again.
        drop(a);
        assert!(listener.routes.lock().is_empty());
        a = listener.register([1; 20]).unwrap();
        let _again = connect(addr, [1; 20]).await;
        assert_eq!(next(&mut a).await.info_hash, [1; 20]);
    }

    #[tokio::test]
    async fn a_torrent_is_registered_once() {
        let (listener, _) = listener().await;
        let _a = listener.register([1; 20]).unwrap();
        assert!(listener.register([1; 20]).is_err());
    }

    #[tokio::test]
    async fn without_sockets_nobody_connects() {
        let listener = Listener::start(Vec::new());
        assert_eq!(listener.port(), None);
        let mut a = listener.register([1; 20]).unwrap();
        let next = tokio::time::timeout(Duration::from_millis(50), a.next()).await;
        assert!(next.is_err());
    }
}
