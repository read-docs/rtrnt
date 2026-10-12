use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, BytesMut};
use rand::Rng;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout};

/// Port we listen on for incoming peers unless told otherwise.
pub const DEFAULT_LISTEN_PORT: u16 = 6881;
/// Largest frame we accept: room for a 16 KiB block or a bitfield of 8M pieces.
pub const MAX_FRAME: usize = 1 << 20;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Peers commonly drop connections that stay silent for two minutes.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(90);
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
const PEER_ID_PREFIX: &[u8; 8] = b"-RT0100-";
/// Reserved handshake bit saying we run a DHT node (BEP 5): the last bit of
/// the last reserved byte.
const RESERVED_DHT: u8 = 0x01;

/// Azureus-style peer id: client prefix plus 12 random alphanumerics.
pub fn generate_peer_id() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(PEER_ID_PREFIX);
    let mut rng = rand::rng();
    for byte in &mut id[8..] {
        *byte = rng.sample(rand::distr::Alphanumeric);
    }
    id
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have {
        piece_index: u32,
    },
    Bitfield {
        bitfield: Vec<u8>,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port {
        port: u16,
    },
    /// A message type we don't implement (e.g. extensions); it is skipped.
    Unknown {
        id: u8,
    },
}

impl PeerMessage {
    /// Serializes the message, including its 4-byte length prefix.
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        match self {
            PeerMessage::KeepAlive => {}
            PeerMessage::Choke => payload.push(0),
            PeerMessage::Unchoke => payload.push(1),
            PeerMessage::Interested => payload.push(2),
            PeerMessage::NotInterested => payload.push(3),
            PeerMessage::Have { piece_index } => {
                payload.push(4);
                payload.extend_from_slice(&piece_index.to_be_bytes());
            }
            PeerMessage::Bitfield { bitfield } => {
                payload.push(5);
                payload.extend_from_slice(bitfield);
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                payload.push(6);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                payload.push(7);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(block);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                payload.push(8);
                payload.extend_from_slice(&index.to_be_bytes());
                payload.extend_from_slice(&begin.to_be_bytes());
                payload.extend_from_slice(&length.to_be_bytes());
            }
            PeerMessage::Port { port } => {
                payload.push(9);
                payload.extend_from_slice(&port.to_be_bytes());
            }
            PeerMessage::Unknown { id } => payload.push(*id),
        }
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    /// Parses a frame payload (without the length prefix).
    pub fn decode(payload: &[u8]) -> Result<Self> {
        let Some((&id, body)) = payload.split_first() else {
            return Ok(PeerMessage::KeepAlive);
        };
        let expect_len = |len: usize| {
            if body.len() == len {
                Ok(())
            } else {
                Err(anyhow!(
                    "message {} has {} body bytes, expected {}",
                    id,
                    body.len(),
                    len
                ))
            }
        };
        let message = match id {
            0 => expect_len(0).map(|_| PeerMessage::Choke)?,
            1 => expect_len(0).map(|_| PeerMessage::Unchoke)?,
            2 => expect_len(0).map(|_| PeerMessage::Interested)?,
            3 => expect_len(0).map(|_| PeerMessage::NotInterested)?,
            4 => {
                expect_len(4)?;
                PeerMessage::Have {
                    piece_index: be_u32(body, 0)?,
                }
            }
            5 => PeerMessage::Bitfield {
                bitfield: body.to_vec(),
            },
            6 => {
                expect_len(12)?;
                PeerMessage::Request {
                    index: be_u32(body, 0)?,
                    begin: be_u32(body, 4)?,
                    length: be_u32(body, 8)?,
                }
            }
            7 => {
                if body.len() < 8 {
                    bail!("piece message too short");
                }
                PeerMessage::Piece {
                    index: be_u32(body, 0)?,
                    begin: be_u32(body, 4)?,
                    block: body[8..].to_vec(),
                }
            }
            8 => {
                expect_len(12)?;
                PeerMessage::Cancel {
                    index: be_u32(body, 0)?,
                    begin: be_u32(body, 4)?,
                    length: be_u32(body, 8)?,
                }
            }
            9 => {
                expect_len(2)?;
                PeerMessage::Port {
                    port: u16::from_be_bytes([body[0], body[1]]),
                }
            }
            id => PeerMessage::Unknown { id },
        };
        Ok(message)
    }
}

fn be_u32(bytes: &[u8], at: usize) -> Result<u32> {
    bytes
        .get(at..at + 4)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or_else(|| anyhow!("message truncated"))
}

pub fn handshake_bytes(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 68] {
    let mut handshake = [0u8; 68];
    handshake[0] = PROTOCOL.len() as u8;
    handshake[1..20].copy_from_slice(PROTOCOL);
    // bytes 20..28 are reserved extension bits
    handshake[27] |= RESERVED_DHT;
    handshake[28..48].copy_from_slice(info_hash);
    handshake[48..68].copy_from_slice(peer_id);
    handshake
}

/// Validates a handshake and returns the remote peer id.
pub fn parse_handshake(handshake: &[u8; 68], info_hash: &[u8; 20]) -> Result<[u8; 20]> {
    if handshake[0] as usize != PROTOCOL.len() || &handshake[1..20] != PROTOCOL {
        bail!("peer does not speak the BitTorrent protocol");
    }
    if &handshake[28..48] != info_hash {
        bail!("info hash mismatch");
    }
    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&handshake[48..68]);
    Ok(peer_id)
}

pub struct PeerConnection {
    socket: TcpStream,
    read_buf: BytesMut,
    pub addr: SocketAddr,
    pub peer_id: [u8; 20],
    /// The reserved bytes of the peer's handshake.
    reserved: [u8; 8],
    peer_choking: bool,
    peer_has: Vec<bool>,
    am_interested: bool,
    last_sent: Instant,
}

impl PeerConnection {
    /// Connects to `addr` and performs the handshake.
    pub async fn connect(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
    ) -> Result<Self> {
        let mut socket = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .context("connection timed out")?
            .context("connection failed")?;

        let mut response = [0u8; 68];
        timeout(HANDSHAKE_TIMEOUT, async {
            socket
                .write_all(&handshake_bytes(&info_hash, &peer_id))
                .await?;
            socket.read_exact(&mut response).await
        })
        .await
        .context("handshake timed out")??;
        let remote_id = parse_handshake(&response, &info_hash)?;
        let mut conn = Self::new(socket, addr, remote_id, num_pieces);
        conn.reserved.copy_from_slice(&response[20..28]);
        Ok(conn)
    }

    /// Performs the handshake on a connection that the peer at `addr` opened
    /// to us. The peer speaks first, and we answer only if it asks for our
    /// torrent.
    pub async fn accept(
        mut socket: TcpStream,
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
    ) -> Result<Self> {
        let mut handshake = [0u8; 68];
        let remote_id = timeout(HANDSHAKE_TIMEOUT, async {
            socket.read_exact(&mut handshake).await?;
            let remote_id = parse_handshake(&handshake, &info_hash)?;
            socket
                .write_all(&handshake_bytes(&info_hash, &peer_id))
                .await?;
            anyhow::Ok(remote_id)
        })
        .await
        .context("handshake timed out")??;
        // Our own outgoing connection, to an address a tracker listed. Having
        // answered, the other end sees our id too and gives up on it.
        if remote_id == peer_id {
            bail!("connected to ourselves");
        }
        let mut conn = Self::new(socket, addr, remote_id, num_pieces);
        conn.reserved.copy_from_slice(&handshake[20..28]);
        Ok(conn)
    }

    fn new(socket: TcpStream, addr: SocketAddr, peer_id: [u8; 20], num_pieces: usize) -> Self {
        Self {
            socket,
            read_buf: BytesMut::with_capacity(32 * 1024),
            addr,
            peer_id,
            reserved: [0; 8],
            peer_choking: true,
            peer_has: vec![false; num_pieces],
            am_interested: false,
            last_sent: Instant::now(),
        }
    }

    /// Whether the peer runs a DHT node, and so wants our `Port` message.
    pub fn supports_dht(&self) -> bool {
        self.reserved[7] & RESERVED_DHT != 0
    }

    pub fn is_choked(&self) -> bool {
        self.peer_choking
    }

    /// Which pieces the peer has advertised (via Bitfield and Have).
    pub fn peer_has(&self) -> &[bool] {
        &self.peer_has
    }

    pub async fn send(&mut self, message: &PeerMessage) -> Result<()> {
        self.send_all(std::slice::from_ref(message)).await
    }

    /// Sends several messages in one write.
    pub async fn send_all(&mut self, messages: &[PeerMessage]) -> Result<()> {
        let bytes: Vec<u8> = messages.iter().flat_map(PeerMessage::encode).collect();
        timeout(WRITE_TIMEOUT, self.socket.write_all(&bytes))
            .await
            .context("write timed out")??;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// Sends a keep-alive if nothing has been sent for `KEEPALIVE_INTERVAL`,
    /// so the peer does not time out an idle connection.
    pub async fn keep_alive(&mut self) -> Result<()> {
        if self.last_sent.elapsed() >= KEEPALIVE_INTERVAL {
            self.send(&PeerMessage::KeepAlive).await?;
        }
        Ok(())
    }

    /// Sends Interested or NotInterested if our interest changed.
    pub async fn set_interested(&mut self, interested: bool) -> Result<()> {
        if interested != self.am_interested {
            let message = if interested {
                PeerMessage::Interested
            } else {
                PeerMessage::NotInterested
            };
            self.send(&message).await?;
            self.am_interested = interested;
        }
        Ok(())
    }

    /// Receives the next message, failing if none arrives within 30s.
    pub async fn receive(&mut self) -> Result<PeerMessage> {
        timeout(READ_TIMEOUT, self.receive_untimed())
            .await
            .context("peer read timed out")?
    }

    /// Receives the next message and updates choke and piece state from it.
    ///
    /// Cancel-safe: bytes are buffered in `read_buf`, so dropping this future
    /// (e.g. in `select!`) never loses part of a frame.
    pub async fn receive_untimed(&mut self) -> Result<PeerMessage> {
        let payload = self.read_frame().await?;
        let message = PeerMessage::decode(&payload)?;
        self.apply(&message)?;
        Ok(message)
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>> {
        loop {
            if self.read_buf.len() >= 4 {
                let len = u32::from_be_bytes([
                    self.read_buf[0],
                    self.read_buf[1],
                    self.read_buf[2],
                    self.read_buf[3],
                ]) as usize;
                if len > MAX_FRAME {
                    bail!("peer sent oversized frame ({} bytes)", len);
                }
                if self.read_buf.len() >= 4 + len {
                    self.read_buf.advance(4);
                    return Ok(self.read_buf.split_to(len).to_vec());
                }
                self.read_buf.reserve(4 + len - self.read_buf.len());
            }
            if self.socket.read_buf(&mut self.read_buf).await? == 0 {
                bail!("peer closed the connection");
            }
        }
    }

    fn apply(&mut self, message: &PeerMessage) -> Result<()> {
        match message {
            PeerMessage::Choke => self.peer_choking = true,
            PeerMessage::Unchoke => self.peer_choking = false,
            PeerMessage::Have { piece_index } => {
                if let Some(has) = self.peer_has.get_mut(*piece_index as usize) {
                    *has = true;
                }
            }
            PeerMessage::Bitfield { bitfield } => {
                let expected = self.peer_has.len().div_ceil(8);
                if bitfield.len() != expected {
                    bail!(
                        "bitfield has {} bytes, expected {}",
                        bitfield.len(),
                        expected
                    );
                }
                for (i, has) in self.peer_has.iter_mut().enumerate() {
                    *has = bitfield[i / 8] & (0x80 >> (i % 8)) != 0;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_messages() {
        let messages = [
            PeerMessage::KeepAlive,
            PeerMessage::Choke,
            PeerMessage::Unchoke,
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::Have { piece_index: 42 },
            PeerMessage::Bitfield {
                bitfield: vec![0b1010_0000, 0xff],
            },
            PeerMessage::Request {
                index: 1,
                begin: 16384,
                length: 16384,
            },
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                block: vec![1, 2, 3],
            },
            PeerMessage::Piece {
                index: 1,
                begin: 0,
                block: vec![],
            },
            PeerMessage::Cancel {
                index: 1,
                begin: 2,
                length: 3,
            },
            PeerMessage::Port { port: 6881 },
            PeerMessage::Unknown { id: 20 },
        ];
        for message in messages {
            let frame = message.encode();
            let len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
            assert_eq!(len, frame.len() - 4);
            assert_eq!(PeerMessage::decode(&frame[4..]).unwrap(), message);
        }
    }

    #[test]
    fn rejects_malformed_payloads() {
        let cases: &[&[u8]] = &[
            &[0, 1],
            &[4, 0, 0, 0],
            &[6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            &[7, 0, 0, 0, 0, 0, 0, 0],
            &[8],
            &[9, 1],
        ];
        for case in cases {
            assert!(PeerMessage::decode(case).is_err(), "accepted {:?}", case);
        }
    }

    #[test]
    fn handshake_roundtrip() {
        let info_hash = [3u8; 20];
        let peer_id = generate_peer_id();
        assert!(peer_id.starts_with(PEER_ID_PREFIX));
        assert!(peer_id[8..].iter().all(u8::is_ascii_alphanumeric));

        let handshake = handshake_bytes(&info_hash, &peer_id);
        assert_eq!(handshake[27] & RESERVED_DHT, RESERVED_DHT);
        assert_eq!(parse_handshake(&handshake, &info_hash).unwrap(), peer_id);
        assert!(parse_handshake(&handshake, &[4u8; 20]).is_err());

        let mut bad = handshake;
        bad[5] = b'x';
        assert!(parse_handshake(&bad, &info_hash).is_err());
    }

    #[tokio::test]
    async fn sends_interest_changes_and_keep_alives_only_when_needed() {
        let info_hash = [5u8; 20];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let remote = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            let reply = handshake_bytes(&info_hash, &[1u8; 20]);
            socket.write_all(&reply).await.unwrap();
            let mut received = Vec::new();
            while let Ok(len) = socket.read_u32().await {
                let mut payload = vec![0u8; len as usize];
                socket.read_exact(&mut payload).await.unwrap();
                received.push(PeerMessage::decode(&payload).unwrap());
            }
            received
        });

        let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1)
            .await
            .unwrap();
        assert!(conn.supports_dht());
        conn.keep_alive().await.unwrap();
        conn.set_interested(false).await.unwrap();
        conn.set_interested(true).await.unwrap();
        conn.set_interested(true).await.unwrap();
        conn.set_interested(false).await.unwrap();
        conn.keep_alive().await.unwrap();
        conn.last_sent = Instant::now().checked_sub(KEEPALIVE_INTERVAL).unwrap();
        conn.keep_alive().await.unwrap();
        conn.keep_alive().await.unwrap();
        drop(conn);

        assert_eq!(
            remote.await.unwrap(),
            [
                PeerMessage::Interested,
                PeerMessage::NotInterested,
                PeerMessage::KeepAlive,
            ]
        );
    }

    #[tokio::test]
    async fn accept_answers_only_handshakes_for_our_torrent_from_others() {
        let info_hash = [5u8; 20];
        let ours = [2u8; 20];
        let theirs = [1u8; 20];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        // (their info hash, their peer id, accepted, answered)
        let cases = [
            (info_hash, theirs, true, true),
            ([6u8; 20], theirs, false, false),
            (info_hash, ours, false, true),
        ];
        for (their_hash, their_id, accepted, answered) in cases {
            let remote = tokio::spawn(async move {
                let mut socket = TcpStream::connect(addr).await.unwrap();
                let handshake = handshake_bytes(&their_hash, &their_id);
                socket.write_all(&handshake).await.unwrap();
                let mut reply = [0u8; 68];
                socket.read_exact(&mut reply).await.ok().map(|_| reply)
            });
            let (socket, from) = listener.accept().await.unwrap();
            let result = PeerConnection::accept(socket, from, info_hash, ours, 1).await;
            assert_eq!(result.is_ok(), accepted, "{:?}", result.as_ref().err());
            if let Ok(conn) = &result {
                assert_eq!(conn.peer_id, theirs);
                assert_eq!(conn.addr, from);
                assert!(conn.supports_dht());
            }
            drop(result);
            let reply = remote.await.unwrap();
            assert_eq!(reply.is_some(), answered);
            if let Some(reply) = reply {
                assert_eq!(reply, handshake_bytes(&info_hash, &ours));
            }
        }
    }
}
