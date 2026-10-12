use crate::extension::{self, ExtendedHandshake};
use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, BytesMut};
use rand::Rng;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout};
use tracing::debug;

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
    /// A message of the extension protocol (BEP 10). `id` 0 is the extension
    /// handshake; any other id is one the receiver chose for an extension.
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
    /// A message type we don't implement (e.g. the fast extension); it is
    /// skipped.
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
            PeerMessage::Extended { id, payload: body } => {
                payload.push(20);
                payload.push(*id);
                payload.extend_from_slice(body);
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
            20 => {
                let Some((&id, payload)) = body.split_first() else {
                    bail!("extended message has no extended id");
                };
                PeerMessage::Extended {
                    id,
                    payload: payload.to_vec(),
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

/// Packs piece flags into a Bitfield payload: piece 0 is the high bit of the
/// first byte, and the spare bits at the end are zero.
pub fn encode_bitfield(has: &[bool]) -> Vec<u8> {
    let mut bitfield = vec![0u8; has.len().div_ceil(8)];
    for (i, _) in has.iter().enumerate().filter(|(_, has)| **has) {
        bitfield[i / 8] |= 0x80 >> (i % 8);
    }
    bitfield
}

/// Our handshake.
pub fn handshake_bytes(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> [u8; 68] {
    let mut handshake = [0u8; 68];
    handshake[0] = PROTOCOL.len() as u8;
    handshake[1..20].copy_from_slice(PROTOCOL);
    // Bytes 20..28 are reserved extension bits. We set the extension
    // protocol's (BEP 10) and the DHT's (BEP 5).
    handshake[20 + extension::RESERVED_BYTE] |= extension::RESERVED_BIT;
    handshake[27] |= RESERVED_DHT;
    handshake[28..48].copy_from_slice(info_hash);
    handshake[48..68].copy_from_slice(peer_id);
    handshake
}

/// What a peer told us in its handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub peer_id: [u8; 20],
    /// The extension bits the peer set.
    pub reserved: [u8; 8],
}

impl Handshake {
    /// Whether the peer supports the extension protocol (BEP 10).
    pub fn supports_extensions(&self) -> bool {
        self.reserved[extension::RESERVED_BYTE] & extension::RESERVED_BIT != 0
    }

    /// Whether the peer runs a DHT node (BEP 5), and so wants our `Port`
    /// message.
    pub fn supports_dht(&self) -> bool {
        self.reserved[7] & RESERVED_DHT != 0
    }
}

/// Validates a handshake and returns what the peer told us in it.
pub fn parse_handshake(handshake: &[u8; 68], info_hash: &[u8; 20]) -> Result<Handshake> {
    if handshake[0] as usize != PROTOCOL.len() || &handshake[1..20] != PROTOCOL {
        bail!("peer does not speak the BitTorrent protocol");
    }
    if &handshake[28..48] != info_hash {
        bail!("info hash mismatch");
    }
    let mut parsed = Handshake {
        peer_id: [0u8; 20],
        reserved: [0u8; 8],
    };
    parsed.reserved.copy_from_slice(&handshake[20..28]);
    parsed.peer_id.copy_from_slice(&handshake[48..68]);
    Ok(parsed)
}

/// A peer sent something no honest peer would, such as a malformed
/// extension handshake. Unlike a failed connection, this is no reason to try
/// the peer again.
#[derive(Debug)]
pub struct ProtocolViolation(pub &'static str);

impl std::fmt::Display for ProtocolViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ProtocolViolation {}

pub struct PeerConnection {
    socket: TcpStream,
    read_buf: BytesMut,
    pub addr: SocketAddr,
    pub peer_id: [u8; 20],
    /// Whether the peer runs a DHT node.
    dht: bool,
    peer_choking: bool,
    peer_has: Vec<bool>,
    am_interested: bool,
    /// Whether we refuse to upload to the peer. Every connection starts
    /// choked on both sides.
    am_choking: bool,
    peer_interested: bool,
    last_sent: Instant,
    /// Whether both sides support the extension protocol (BEP 10).
    extensions: bool,
    /// The peer's extension handshake, once it has sent one.
    peer_extensions: Option<ExtendedHandshake>,
}

impl PeerConnection {
    /// Connects to `addr` and performs the handshake, followed by the
    /// extension handshake if the peer supports it. `listen_port` is the
    /// port we accept incoming peers on, if any. A `private` torrent's
    /// extension handshake leaves out the extensions such torrents must not
    /// use (see `extension::PUBLIC_ONLY`).
    pub async fn connect(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
        listen_port: Option<u16>,
        private: bool,
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
        let handshake = parse_handshake(&response, &info_hash)?;
        let mut conn = Self::new(socket, addr, handshake, num_pieces);
        conn.send_extension_handshake(listen_port, private).await?;
        Ok(conn)
    }

    /// Performs the handshake on a connection that the peer at `addr` opened
    /// to us. The peer speaks first, and we answer only if it asks for our
    /// torrent. The extension handshake follows as in `connect`.
    pub async fn accept(
        mut socket: TcpStream,
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
        listen_port: Option<u16>,
        private: bool,
    ) -> Result<Self> {
        let handshake = timeout(HANDSHAKE_TIMEOUT, async {
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await?;
            let handshake = parse_handshake(&handshake, &info_hash)?;
            socket
                .write_all(&handshake_bytes(&info_hash, &peer_id))
                .await?;
            anyhow::Ok(handshake)
        })
        .await
        .context("handshake timed out")??;
        // Our own outgoing connection, to an address a tracker listed. Having
        // answered, the other end sees our id too and gives up on it.
        if handshake.peer_id == peer_id {
            bail!("connected to ourselves");
        }
        let mut conn = Self::new(socket, addr, handshake, num_pieces);
        conn.send_extension_handshake(listen_port, private).await?;
        Ok(conn)
    }

    fn new(socket: TcpStream, addr: SocketAddr, handshake: Handshake, num_pieces: usize) -> Self {
        Self {
            socket,
            read_buf: BytesMut::with_capacity(32 * 1024),
            addr,
            peer_id: handshake.peer_id,
            dht: handshake.supports_dht(),
            peer_choking: true,
            peer_has: vec![false; num_pieces],
            am_interested: false,
            am_choking: true,
            peer_interested: false,
            last_sent: Instant::now(),
            // We always set the bit ourselves.
            extensions: handshake.supports_extensions(),
            peer_extensions: None,
        }
    }

    /// Sends our extension handshake if the peer supports the extension
    /// protocol. It goes out right after the handshake, before anything else.
    async fn send_extension_handshake(
        &mut self,
        listen_port: Option<u16>,
        private: bool,
    ) -> Result<()> {
        if !self.extensions {
            return Ok(());
        }
        let payload = ExtendedHandshake::ours(listen_port, self.addr.ip(), private).encode()?;
        self.send(&PeerMessage::Extended {
            id: extension::HANDSHAKE_ID,
            payload,
        })
        .await
    }

    /// Whether both sides support the extension protocol (BEP 10), so that
    /// extended messages may be sent.
    pub fn supports_extensions(&self) -> bool {
        self.extensions
    }

    /// The peer's extension handshake, once it has sent one. Its `id` method
    /// gives the id to send an extension's messages with.
    pub fn peer_extensions(&self) -> Option<&ExtendedHandshake> {
        self.peer_extensions.as_ref()
    }

    /// How many requests the peer queues before dropping more (its `reqq`),
    /// if it told us.
    pub fn request_limit(&self) -> Option<usize> {
        let reqq = self.peer_extensions.as_ref()?.request_queue?;
        usize::try_from(reqq).ok()
    }

    /// Sends `payload` as a message of the extension `name`, with the id the
    /// peer asked for in its extension handshake. Fails if the peer has not
    /// said it supports the extension.
    pub async fn send_extended(&mut self, name: &str, payload: Vec<u8>) -> Result<()> {
        let id = self
            .peer_extensions
            .as_ref()
            .and_then(|handshake| handshake.id(name))
            .ok_or_else(|| anyhow!("peer does not support the {name} extension"))?;
        self.send(&PeerMessage::Extended { id, payload }).await
    }

    /// Whether the peer runs a DHT node, and so wants our `Port` message.
    pub fn supports_dht(&self) -> bool {
        self.dht
    }

    pub fn is_choked(&self) -> bool {
        self.peer_choking
    }

    /// Whether we are choking the peer, so it may not download from us.
    pub fn am_choking(&self) -> bool {
        self.am_choking
    }

    /// Whether the peer wants to download from us.
    pub fn peer_interested(&self) -> bool {
        self.peer_interested
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

    /// Sends Choke or Unchoke if our choke state changed. A choked peer's
    /// pending requests are dropped, and it knows to ask again once
    /// unchoked.
    pub async fn set_choking(&mut self, choking: bool) -> Result<()> {
        if choking != self.am_choking {
            let message = if choking {
                PeerMessage::Choke
            } else {
                PeerMessage::Unchoke
            };
            self.send(&message).await?;
            self.am_choking = choking;
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
            PeerMessage::Interested => self.peer_interested = true,
            PeerMessage::NotInterested => self.peer_interested = false,
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
            PeerMessage::Extended {
                id: extension::HANDSHAKE_ID,
                payload,
            } => {
                let handshake = ExtendedHandshake::decode(payload)
                    .context(ProtocolViolation("malformed extension handshake"))?;
                debug!(
                    addr = %self.addr,
                    client = handshake.client.as_deref().unwrap_or("unknown"),
                    reqq = ?handshake.request_queue,
                    extensions = ?handshake.extensions,
                    "extension handshake"
                );
                // A peer may send it again to change what it supports.
                match &mut self.peer_extensions {
                    Some(known) => known.update(handshake),
                    None => self.peer_extensions = Some(handshake),
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
            PeerMessage::Extended {
                id: 0,
                payload: b"de".to_vec(),
            },
            PeerMessage::Extended {
                id: 3,
                payload: vec![],
            },
            PeerMessage::Unknown { id: 21 },
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
            &[20],
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
        assert_eq!(handshake[20..28], [0, 0, 0, 0, 0, 0x10, 0, 0x01]);
        let parsed = parse_handshake(&handshake, &info_hash).unwrap();
        assert_eq!(parsed.peer_id, peer_id);
        assert!(parsed.supports_extensions());
        assert!(parsed.supports_dht());
        assert!(parse_handshake(&handshake, &[4u8; 20]).is_err());

        let mut bad = handshake;
        bad[5] = b'x';
        assert!(parse_handshake(&bad, &info_hash).is_err());

        // Other clients' bits are recorded, and the extension bit read alone.
        let mut other = handshake;
        other[20..28].copy_from_slice(&[0x80, 0, 0, 0, 0, 0xef, 0, 0x05]);
        let parsed = parse_handshake(&other, &info_hash).unwrap();
        assert_eq!(parsed.reserved, [0x80, 0, 0, 0, 0, 0xef, 0, 0x05]);
        assert!(!parsed.supports_extensions());
    }

    /// A mock peer's handshake, with the extension protocol bit cleared
    /// unless `extensions`.
    fn remote_handshake(info_hash: &[u8; 20], extensions: bool) -> [u8; 68] {
        let mut handshake = handshake_bytes(info_hash, &[1u8; 20]);
        if !extensions {
            handshake[20 + extension::RESERVED_BYTE] = 0;
        }
        handshake
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
            // Without the extension protocol, so no extension handshake.
            let reply = remote_handshake(&info_hash, false);
            socket.write_all(&reply).await.unwrap();
            let mut received = Vec::new();
            while let Ok(len) = socket.read_u32().await {
                let mut payload = vec![0u8; len as usize];
                socket.read_exact(&mut payload).await.unwrap();
                received.push(PeerMessage::decode(&payload).unwrap());
            }
            received
        });

        let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, Some(6881), false)
            .await
            .unwrap();
        assert!(!conn.supports_extensions());
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

    #[test]
    fn bitfield_encoding_matches_decoding() {
        let has = [
            true, false, false, true, false, false, false, false, true, true,
        ];
        let bitfield = encode_bitfield(&has);
        assert_eq!(bitfield, [0b1001_0000, 0b1100_0000]);
        assert!(encode_bitfield(&[]).is_empty());
        assert_eq!(encode_bitfield(&[false; 9]), [0, 0]);
    }

    /// Connects to a mock peer that answers the handshake, sends
    /// `messages`, and returns everything it receives until we hang up.
    async fn mock_peer(
        num_pieces: usize,
        messages: Vec<PeerMessage>,
    ) -> (PeerConnection, tokio::task::JoinHandle<Vec<PeerMessage>>) {
        let info_hash = [5u8; 20];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let remote = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            // Without the extension protocol, so no extension handshake.
            let reply = remote_handshake(&info_hash, false);
            socket.write_all(&reply).await.unwrap();
            for message in messages {
                socket.write_all(&message.encode()).await.unwrap();
            }
            let mut received = Vec::new();
            while let Ok(len) = socket.read_u32().await {
                let mut payload = vec![0u8; len as usize];
                socket.read_exact(&mut payload).await.unwrap();
                received.push(PeerMessage::decode(&payload).unwrap());
            }
            received
        });
        let conn = PeerConnection::connect(addr, info_hash, [2u8; 20], num_pieces, None, false)
            .await
            .unwrap();
        (conn, remote)
    }

    #[tokio::test]
    async fn tracks_peer_interest_and_sends_choke_changes() {
        let has = [true, false, true];
        let messages = vec![
            PeerMessage::Bitfield {
                bitfield: encode_bitfield(&has),
            },
            PeerMessage::Interested,
            PeerMessage::NotInterested,
        ];
        let (mut conn, remote) = mock_peer(3, messages).await;
        assert!(conn.am_choking());
        assert!(!conn.peer_interested());
        conn.receive().await.unwrap();
        assert_eq!(conn.peer_has(), has);
        conn.receive().await.unwrap();
        assert!(conn.peer_interested());
        conn.receive().await.unwrap();
        assert!(!conn.peer_interested());

        conn.set_choking(true).await.unwrap();
        conn.set_choking(false).await.unwrap();
        conn.set_choking(false).await.unwrap();
        assert!(!conn.am_choking());
        conn.set_choking(true).await.unwrap();
        drop(conn);
        assert_eq!(
            remote.await.unwrap(),
            [PeerMessage::Unchoke, PeerMessage::Choke]
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
            let result =
                PeerConnection::accept(socket, from, info_hash, ours, 1, None, false).await;
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

    async fn read_message(socket: &mut TcpStream) -> PeerMessage {
        let len = socket.read_u32().await.unwrap();
        let mut payload = vec![0u8; len as usize];
        socket.read_exact(&mut payload).await.unwrap();
        PeerMessage::decode(&payload).unwrap()
    }

    #[tokio::test]
    async fn exchanges_extension_handshakes() {
        let info_hash = [5u8; 20];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let metadata_request = b"d8:msg_typei0e5:piecei0ee".to_vec();
        let remote = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            assert!(
                parse_handshake(&handshake, &info_hash)
                    .unwrap()
                    .supports_extensions()
            );
            let reply = remote_handshake(&info_hash, true);
            socket.write_all(&reply).await.unwrap();
            // Ours comes first.
            let PeerMessage::Extended { id: 0, payload } = read_message(&mut socket).await else {
                panic!("no extension handshake");
            };
            let ours = ExtendedHandshake::decode(&payload).unwrap();
            let theirs = ExtendedHandshake {
                extensions: [("ut_metadata".to_string(), 3)].into(),
                client: Some("mock 1.0".into()),
                request_queue: Some(7),
                ..Default::default()
            };
            let message = PeerMessage::Extended {
                id: 0,
                payload: theirs.encode().unwrap(),
            };
            socket.write_all(&message.encode()).await.unwrap();
            (ours, read_message(&mut socket).await)
        });

        let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, Some(6881), false)
            .await
            .unwrap();
        assert!(conn.supports_extensions());
        assert!(conn.peer_extensions().is_none());
        assert_eq!(conn.request_limit(), None);
        assert!(conn.send_extended("ut_metadata", vec![]).await.is_err());

        let message = conn.receive().await.unwrap();
        assert!(matches!(message, PeerMessage::Extended { id: 0, .. }));
        assert_eq!(conn.request_limit(), Some(7));
        let theirs = conn.peer_extensions().unwrap();
        assert_eq!(theirs.client.as_deref(), Some("mock 1.0"));
        assert_eq!(theirs.id("ut_metadata"), Some(3));
        assert!(conn.send_extended("ut_pex", vec![]).await.is_err());
        conn.send_extended("ut_metadata", metadata_request.clone())
            .await
            .unwrap();

        let (ours, next) = remote.await.unwrap();
        let localhost = "127.0.0.1".parse().unwrap();
        assert_eq!(ours, ExtendedHandshake::ours(Some(6881), localhost, false));
        assert_eq!(
            next,
            PeerMessage::Extended {
                id: 3,
                payload: metadata_request
            }
        );
    }

    #[tokio::test]
    async fn malformed_extension_handshake_is_a_protocol_violation() {
        let info_hash = [5u8; 20];
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0u8; 68];
            socket.read_exact(&mut handshake).await.unwrap();
            let reply = remote_handshake(&info_hash, true);
            socket.write_all(&reply).await.unwrap();
            let message = PeerMessage::Extended {
                id: 0,
                payload: b"li7ee".to_vec(),
            };
            socket.write_all(&message.encode()).await.unwrap();
            // Until the other end hangs up.
            socket.read_to_end(&mut Vec::new()).await.unwrap();
        });

        let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, None, false)
            .await
            .unwrap();
        let err = conn.receive().await.unwrap_err();
        assert!(err.is::<ProtocolViolation>(), "{err:#}");
        assert!(conn.peer_extensions().is_none());
    }
}
