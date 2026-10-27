use crate::extension::{self, Advertised, ExtendedHandshake};
use anyhow::{Context, Result, anyhow, bail};
use bytes::{Buf, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, Instant, timeout};
use tracing::debug;

use super::{
    HANDSHAKE_TIMEOUT, Handshake, Incoming, KEEPALIVE_INTERVAL, MAX_FRAME, PeerMessage,
    handshake_bytes, parse_handshake,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Have messages kept from a peer while the number of pieces is unknown
/// (see `PeerConnection::defer_pieces`); more are dropped.
const MAX_DEFERRED_HAVES: usize = 1 << 16;

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
    pub(super) last_sent: Instant,
    /// Whether both sides support the extension protocol (BEP 10).
    extensions: bool,
    /// The peer's extension handshake, once it has sent one.
    peer_extensions: Option<ExtendedHandshake>,
    /// Whether both sides support the fast extension (BEP 6).
    fast: bool,
    /// Pieces the peer said it serves us even while it chokes us (Allowed
    /// Fast).
    allowed_fast: Vec<bool>,
    /// Set while the number of pieces is unknown, because the torrent's
    /// metadata is still being fetched (BEP 9).
    deferred: Option<DeferredPieces>,
}

/// What a peer said about its pieces before we knew how many there are.
#[derive(Debug, Default)]
struct DeferredPieces {
    /// The last Bitfield, Have All or Have None.
    pieces: Option<PeerMessage>,
    haves: Vec<u32>,
    allowed_fast: Vec<u32>,
}

impl PeerConnection {
    /// Connects to `addr` and performs the handshake, followed by the
    /// extension handshake if the peer supports it, which says what
    /// `advertised` says: the port we accept incoming peers on, the size of
    /// the metadata we can send (BEP 9), and whether the torrent is private,
    /// which leaves out the extensions such torrents must not use (see
    /// `extension::PUBLIC_ONLY`).
    pub async fn connect(
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
        advertised: Advertised,
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
        conn.send_extension_handshake(advertised).await?;
        Ok(conn)
    }

    /// Performs the handshake on a connection that the peer at `addr` opened
    /// to us. The peer speaks first, and we answer only if it asks for our
    /// torrent. The extension handshake follows as in `connect`.
    pub async fn accept(
        socket: TcpStream,
        addr: SocketAddr,
        info_hash: [u8; 20],
        peer_id: [u8; 20],
        num_pieces: usize,
        advertised: Advertised,
    ) -> Result<Self> {
        let incoming = Incoming::read(socket, addr).await?;
        if incoming.info_hash != info_hash {
            bail!("info hash mismatch");
        }
        Self::answer(incoming, peer_id, num_pieces, advertised).await
    }

    /// Answers the handshake of a peer that connected to us, which we have
    /// read, with ours for the torrent it asked for. The extension handshake
    /// follows as in `connect`.
    pub async fn answer(
        incoming: Incoming,
        peer_id: [u8; 20],
        num_pieces: usize,
        advertised: Advertised,
    ) -> Result<Self> {
        let Incoming {
            mut socket,
            addr,
            info_hash,
            handshake,
        } = incoming;
        timeout(
            HANDSHAKE_TIMEOUT,
            socket.write_all(&handshake_bytes(&info_hash, &peer_id)),
        )
        .await
        .context("handshake timed out")??;
        // Our own outgoing connection, to an address a tracker listed. Having
        // answered, the other end sees our id too and gives up on it.
        if handshake.peer_id == peer_id {
            bail!("connected to ourselves");
        }
        let mut conn = Self::new(socket, addr, handshake, num_pieces);
        conn.send_extension_handshake(advertised).await?;
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
            // We always set this bit too.
            fast: handshake.supports_fast(),
            allowed_fast: vec![false; num_pieces],
            deferred: None,
        }
    }

    /// Sends our extension handshake if the peer supports the extension
    /// protocol. It goes out right after the handshake, before anything else,
    /// and may be sent again later to change what it says, such as once we
    /// have the metadata to send (`metadata_size`).
    pub async fn send_extension_handshake(&mut self, advertised: Advertised) -> Result<()> {
        if !self.extensions {
            return Ok(());
        }
        let payload = ExtendedHandshake::ours(advertised, self.addr.ip()).encode()?;
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

    /// For a torrent whose metadata is still being fetched (BEP 9), so that
    /// the number of pieces is unknown: the peer's Bitfield, Have, and fast
    /// extension Have All, Have None and Allowed Fast messages are kept
    /// aside until `set_piece_count`. Call before receiving anything.
    pub fn defer_pieces(&mut self) {
        self.deferred = Some(DeferredPieces::default());
    }

    /// Sets the number of pieces, once the metadata is known, and applies
    /// the messages kept aside since `defer_pieces`. Fails if the bitfield
    /// does not fit that many pieces.
    pub fn set_piece_count(&mut self, num_pieces: usize) -> Result<()> {
        let deferred = self.deferred.take().unwrap_or_default();
        self.peer_has = vec![false; num_pieces];
        self.allowed_fast = vec![false; num_pieces];
        if let Some(pieces) = deferred.pieces {
            self.apply(&pieces)?;
        }
        for piece_index in deferred.haves {
            self.apply(&PeerMessage::Have { piece_index })?;
        }
        for piece_index in deferred.allowed_fast {
            self.apply(&PeerMessage::AllowedFast { piece_index })?;
        }
        Ok(())
    }

    /// Whether the peer runs a DHT node, and so wants our `Port` message.
    pub fn supports_dht(&self) -> bool {
        self.dht
    }

    /// Whether both sides support the fast extension (BEP 6), so that its
    /// messages may be sent and every request gets a Piece or a Reject.
    pub fn supports_fast(&self) -> bool {
        self.fast
    }

    /// Pieces the peer said it serves us even while it chokes us. It may
    /// not have them yet.
    pub fn allowed_fast(&self) -> &[bool] {
        &self.allowed_fast
    }

    /// Stops counting piece `index` as allowed fast, say because the peer
    /// rejected a request for it while choking us after all. A later
    /// Allowed Fast for it counts again.
    pub fn disallow_fast(&mut self, index: u32) {
        if let Some(allowed) = self.allowed_fast.get_mut(index as usize) {
            *allowed = false;
        }
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
    /// unchoked; with the fast extension, the caller rejects them after
    /// this.
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
        if message.is_fast() && !self.fast {
            bail!(ProtocolViolation(
                "fast extension message without the fast extension"
            ));
        }
        if let Some(deferred) = &mut self.deferred {
            match message {
                PeerMessage::Bitfield { .. } | PeerMessage::HaveAll | PeerMessage::HaveNone => {
                    deferred.pieces = Some(message.clone());
                    return Ok(());
                }
                PeerMessage::Have { piece_index } => {
                    if deferred.haves.len() < MAX_DEFERRED_HAVES {
                        deferred.haves.push(*piece_index);
                    }
                    return Ok(());
                }
                PeerMessage::AllowedFast { piece_index } => {
                    if deferred.allowed_fast.len() < MAX_DEFERRED_HAVES {
                        deferred.allowed_fast.push(*piece_index);
                    }
                    return Ok(());
                }
                _ => {}
            }
        }
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
            PeerMessage::HaveAll => self.peer_has.fill(true),
            PeerMessage::HaveNone => self.peer_has.fill(false),
            PeerMessage::AllowedFast { piece_index } => {
                // Like Have, an index past the end is ignored.
                if let Some(allowed) = self.allowed_fast.get_mut(*piece_index as usize) {
                    *allowed = true;
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
