//! Bounded datagram queues for sharing a UDP socket between DHT and uTP.

use crate::blocklist::Blocklist;
use librqbit_dualstack_sockets::PollSendToVectored;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc};
use tokio::task::AbortHandle;

const QUEUE: usize = 64;
const MAX_UTP_PACKET: usize = 16 * 1024;
const MAX_DHT_PACKET: usize = 2048;
type Packet = (Vec<u8>, SocketAddr);

/// One protocol's access to a UDP socket. Shared receivers never race for
/// a datagram: exactly one task owns the real socket's receive side.
pub(crate) struct Datagram {
    socket: Arc<UdpSocket>,
    addr: SocketAddr,
    incoming: Option<Mutex<mpsc::Receiver<Packet>>>,
}

impl Datagram {
    pub(crate) fn direct(socket: UdpSocket) -> io::Result<Self> {
        let addr = socket.local_addr()?;
        Ok(Self {
            socket: Arc::new(socket),
            addr,
            incoming: None,
        })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let Some(incoming) = &self.incoming else {
            return self.socket.recv_from(buf).await;
        };
        let (packet, addr) =
            incoming.lock().await.recv().await.ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "UDP multiplexer stopped")
            })?;
        let len = packet.len().min(buf.len());
        buf[..len].copy_from_slice(&packet[..len]);
        Ok((len, addr))
    }

    pub(crate) async fn send_to(&self, buf: &[u8], addr: SocketAddr) -> io::Result<usize> {
        self.socket.send_to(buf, addr).await
    }
}

/// Splits one socket into DHT and uTP receivers. A full protocol queue
/// drops only its own packets; it cannot hold up the other protocol.
pub(crate) fn split(
    socket: UdpSocket,
    blocklist: Arc<Blocklist>,
) -> io::Result<(Datagram, Datagram, AbortHandle)> {
    let socket = Arc::new(socket);
    let addr = socket.local_addr()?;
    let (dht_tx, dht_rx) = mpsc::channel(QUEUE);
    let (utp_tx, utp_rx) = mpsc::channel(QUEUE);
    let receiving = Arc::clone(&socket);
    let task = tokio::spawn(async move {
        let mut buf = vec![0; u16::MAX as usize + 1];
        loop {
            let (len, from) = match receiving.recv_from(&mut buf).await {
                Ok(received) => received,
                Err(e) => {
                    tracing::debug!("shared UDP receive failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            if blocklist.refuses(from.ip()) {
                continue;
            }
            let queue = if len > 0 && buf[0] == b'd' && len <= MAX_DHT_PACKET {
                &dht_tx
            } else if (20..=MAX_UTP_PACKET).contains(&len) && buf[0] & 0x0f == 1 && buf[0] >> 4 <= 4
            {
                &utp_tx
            } else {
                continue;
            };
            // Reserve first: floods against a full queue allocate nothing.
            if let Ok(permit) = queue.try_reserve() {
                permit.send((buf[..len].to_vec(), from));
            }
        }
    })
    .abort_handle();
    Ok((
        Datagram {
            socket: Arc::clone(&socket),
            addr,
            incoming: Some(Mutex::new(dht_rx)),
        },
        Datagram {
            socket,
            addr,
            incoming: Some(Mutex::new(utp_rx)),
        },
        task,
    ))
}

impl PollSendToVectored for Datagram {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_to_vectored(cx, bufs, target)
    }
}

impl librqbit_utp::Transport for Datagram {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }
    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> io::Result<usize> {
        self.send_to(buf, target).await
    }
    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_to(cx, buf, target)
    }
    fn bind_addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn protocol_flood_does_not_block_other_queue() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let (dht, utp, task) = split(socket, Arc::default()).unwrap();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for _ in 0..QUEUE + 20 {
            client.send_to(b"de", addr).await.unwrap();
            tokio::task::yield_now().await;
        }
        let mut packet = [0; 20];
        packet[0] = 0x41;
        client.send_to(&packet, addr).await.unwrap();
        let mut buf = [0; 32];
        let (len, from) = timeout(Duration::from_secs(2), utp.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buf[..len], packet);
        assert_eq!(from, client.local_addr().unwrap());
        let (len, _) = dht.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], b"de");
        task.abort();
    }
}
