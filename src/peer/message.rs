use anyhow::{Result, anyhow, bail};

/// A block of a piece, as named by the messages that request, cancel and
/// reject it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRef {
    pub index: u32,
    pub begin: u32,
    pub length: u32,
}

impl BlockRef {
    /// The Request for this block.
    pub fn request(self) -> PeerMessage {
        PeerMessage::Request(self)
    }

    /// The Cancel that withdraws a request for this block.
    pub fn cancel(self) -> PeerMessage {
        PeerMessage::Cancel(self)
    }

    /// The Reject (fast extension) that turns a request for this block down.
    pub fn reject(self) -> PeerMessage {
        PeerMessage::RejectRequest(self)
    }

    fn encode(&self, payload: &mut Vec<u8>) {
        payload.extend_from_slice(&self.index.to_be_bytes());
        payload.extend_from_slice(&self.begin.to_be_bytes());
        payload.extend_from_slice(&self.length.to_be_bytes());
    }

    fn decode(body: &[u8]) -> Result<Self> {
        Ok(Self {
            index: be_u32(body, 0)?,
            begin: be_u32(body, 4)?,
            length: be_u32(body, 8)?,
        })
    }
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
    Request(BlockRef),
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel(BlockRef),
    Port {
        port: u16,
    },
    /// A message of the extension protocol (BEP 10). `id` 0 is the extension
    /// handshake; any other id is one the receiver chose for an extension.
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
    /// Fast extension (BEP 6): the receiver might like to download this
    /// piece. Only a hint.
    SuggestPiece {
        piece_index: u32,
    },
    /// Fast extension: the sender has every piece. Stands in for a Bitfield.
    HaveAll,
    /// Fast extension: the sender has no piece. Stands in for a Bitfield.
    HaveNone,
    /// Fast extension: the sender will not serve this request.
    RejectRequest(BlockRef),
    /// Fast extension: the sender serves requests for this piece even while
    /// it chokes the receiver (once it has the piece).
    AllowedFast {
        piece_index: u32,
    },
    /// A message type we don't implement; it is skipped.
    Unknown {
        id: u8,
    },
}

impl PeerMessage {
    /// Whether this is one of the fast extension's messages, which only
    /// peers that both support it may send.
    pub fn is_fast(&self) -> bool {
        matches!(
            self,
            PeerMessage::SuggestPiece { .. }
                | PeerMessage::HaveAll
                | PeerMessage::HaveNone
                | PeerMessage::RejectRequest(_)
                | PeerMessage::AllowedFast { .. }
        )
    }

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
            PeerMessage::Request(block) => {
                payload.push(6);
                block.encode(&mut payload);
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
            PeerMessage::Cancel(block) => {
                payload.push(8);
                block.encode(&mut payload);
            }
            PeerMessage::Port { port } => {
                payload.push(9);
                payload.extend_from_slice(&port.to_be_bytes());
            }
            PeerMessage::SuggestPiece { piece_index } => {
                payload.push(0x0d);
                payload.extend_from_slice(&piece_index.to_be_bytes());
            }
            PeerMessage::HaveAll => payload.push(0x0e),
            PeerMessage::HaveNone => payload.push(0x0f),
            PeerMessage::RejectRequest(block) => {
                payload.push(0x10);
                block.encode(&mut payload);
            }
            PeerMessage::AllowedFast { piece_index } => {
                payload.push(0x11);
                payload.extend_from_slice(&piece_index.to_be_bytes());
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
                PeerMessage::Request(BlockRef::decode(body)?)
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
                PeerMessage::Cancel(BlockRef::decode(body)?)
            }
            9 => {
                expect_len(2)?;
                PeerMessage::Port {
                    port: u16::from_be_bytes([body[0], body[1]]),
                }
            }
            0x0d => {
                expect_len(4)?;
                PeerMessage::SuggestPiece {
                    piece_index: be_u32(body, 0)?,
                }
            }
            0x0e => expect_len(0).map(|_| PeerMessage::HaveAll)?,
            0x0f => expect_len(0).map(|_| PeerMessage::HaveNone)?,
            0x10 => {
                expect_len(12)?;
                PeerMessage::RejectRequest(BlockRef::decode(body)?)
            }
            0x11 => {
                expect_len(4)?;
                PeerMessage::AllowedFast {
                    piece_index: be_u32(body, 0)?,
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
