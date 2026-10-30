//! uTP peer transport (BEP 29), sharing UDP sockets with the DHT.
//!
//! The pinned transport uses LEDBAT. uTP remains opt-in while independent
//! client interoperability is being validated.

use crate::blocklist::Blocklist;
use crate::dht::{DEFAULT_ROUTERS, Dht};
use crate::peer::Transport;
use crate::udp::{self, Datagram};
use anyhow::{Context, Result, anyhow, ensure};
use librqbit_utp::{CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocketCustom};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::net::UdpSocket;
use tokio::task::AbortHandle;
use tokio_util::sync::CancellationToken;

/// Whether outgoing connections use uTP, TCP, or uTP with TCP fallback.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    /// TCP only; do not create uTP sockets.
    #[default]
    Off,
    /// Try uTP first, then TCP when the peer cannot be reached over uTP.
    Prefer,
    /// Only uTP, with no TCP fallback.
    Require,
}

impl std::fmt::Display for Policy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Off => "off",
            Self::Prefer => "prefer",
            Self::Require => "require",
        })
    }
}

type Socket = Arc<UtpSocketCustom<Datagram>>;

/// The session's uTP sockets and their DHT receive channels. All torrents
/// reuse these endpoints; no outgoing connection binds another UDP port.
pub struct Endpoint {
    sockets: Vec<Socket>,
    dht: Mutex<Option<Vec<Datagram>>>,
    tasks: Vec<AbortHandle>,
    cancel: CancellationToken,
    blocklist: Arc<Blocklist>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.tasks.iter().for_each(AbortHandle::abort);
    }
}

impl Endpoint {
    /// Binds uTP to the advertised peer port using the global source,
    /// interface and proxy policy. Failure leaves fallback to the caller.
    pub async fn bind(
        port: u16,
        max_connections: usize,
        blocklist: Arc<Blocklist>,
    ) -> Result<Arc<Self>> {
        let policy = crate::network::global();
        let ipv6 = policy.source_address().is_some_and(|ip| ip.is_ipv6());
        let primary = policy
            .udp_socket(ipv6, port)
            .context("cannot bind the shared uTP/DHT port")?;
        let port = primary.local_addr()?.port();
        let mut sockets = vec![primary];
        if policy.source_address().is_none() {
            match policy.udp_socket(true, port) {
                Ok(socket) => sockets.push(socket),
                Err(e) => tracing::debug!("IPv6 uTP unavailable; continuing with IPv4: {e:#}"),
            }
        }
        Self::start(sockets, max_connections, blocklist)
    }

    fn start(
        sockets: Vec<UdpSocket>,
        max_connections: usize,
        blocklist: Arc<Blocklist>,
    ) -> Result<Arc<Self>> {
        ensure!(!sockets.is_empty(), "uTP needs at least one UDP socket");
        let mut handles = Vec::new();
        let mut receivers = Vec::new();
        let mut tasks = Vec::new();
        let cancel = CancellationToken::new();
        for socket in sockets {
            let (dht, utp, task) = udp::split(socket, Arc::clone(&blocklist))?;
            tasks.push(task);
            let opts = SocketOpts {
                congestion: CongestionConfig {
                    kind: CongestionControllerKind::Ledbat,
                    tracing: false,
                },
                max_live_vsocks: Some(max_connections.max(1).try_into()?),
                vsock_rx_bufsize_bytes: Some((256 * 1024).try_into()?),
                vsock_tx_bufsize_bytes_initial: Some((64 * 1024).try_into()?),
                vsock_tx_bufsize_bytes_max: Some((256 * 1024).try_into()?),
                disable_nagle: true,
                cancellation_token: cancel.clone(),
                ..SocketOpts::default()
            };
            let handle =
                UtpSocketCustom::new_custom(utp, opts).context("cannot start uTP dispatcher")?;
            handles.push(handle);
            receivers.push(dht);
        }
        Ok(Arc::new(Self {
            sockets: handles,
            dht: Mutex::new(Some(receivers)),
            tasks,
            cancel,
            blocklist,
        }))
    }

    /// The port shared by uTP and DHT, and normally TCP peers too.
    pub fn port(&self) -> u16 {
        self.sockets[0].bind_addr().port()
    }

    /// Starts the DHT on its reserved protocol channels exactly once.
    pub fn dht(&self) -> Result<Dht> {
        let mut datagrams = self
            .dht
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| anyhow!("DHT already started on this uTP endpoint"))?
            .into_iter();
        let first = datagrams
            .next()
            .context("uTP endpoint has no datagram socket")?;
        let second = datagrams.next();
        let routers = DEFAULT_ROUTERS
            .iter()
            .map(|router| router.to_string())
            .collect();
        Dht::start_datagrams(first, second, routers)
    }

    /// Connects using the existing socket matching the peer's family.
    /// The caller bounds connection and BitTorrent-handshake timeouts.
    pub async fn connect(&self, addr: SocketAddr) -> Result<Transport> {
        ensure!(!self.blocklist.refuses(addr.ip()), "uTP peer is blocked");
        let socket = self
            .sockets
            .iter()
            .find(|socket| socket.bind_addr().is_ipv4() == addr.is_ipv4())
            .context("no uTP socket for the peer's address family")?;
        Ok(Transport::from(socket.connect(addr).await?))
    }

    /// Accepts a uTP stream from either address family. Cancel-safe; the
    /// transport discards canceled accept requests before routing a SYN.
    pub async fn accept(&self) -> Result<(Transport, SocketAddr)> {
        let first = &self.sockets[0];
        let stream = if let Some(second) = self.sockets.get(1) {
            tokio::select! { result = first.accept() => result?, result = second.accept() => result? }
        } else {
            first.accept().await?
        };
        let addr = stream.remote_addr();
        Ok((Transport::from(stream), addr))
    }
}

#[cfg(test)]
mod tests;
