use std::io::Read;

use anyhow::{Ok, Result, anyhow};
use serde::de::value::UsizeDeserializer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

enum PeerMessage {
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
    KeepAlive,
}

impl PeerMessage {
    fn id(&self) -> u8 {
        let id: u8 = match self {
            PeerMessage::Choke => 0,
            PeerMessage::Unchoke => 1,
            PeerMessage::Interested => 2,
            PeerMessage::NotInterested => 3,
            PeerMessage::Have { piece_index } => 4,
            PeerMessage::Bitfield { bitfield } => 5,
            PeerMessage::Request {
                index,
                begin,
                length,
            } => 6,
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => 7,
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => 8,
            PeerMessage::KeepAlive => 9,
        };
        id
    }
}

pub struct PeerConnection {
    socket: TcpStream,
    pub peer_ip: String,
    pub peer_port: u16,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    bitfield: Vec<bool>,
}

impl PeerConnection {
    pub fn new(
        mut socket: TcpStream,
        peer_ip: String,
        peer_port: u16,
        info_hash: [u8; 20],
    ) -> Self {
        Self {
            socket,
            peer_ip,
            peer_port,
            info_hash,
            peer_id: [0u8; 20],
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Vec::new(),
        }
    }

    pub async fn handshake(&mut self) -> Result<()> {
        let mut handshake = Vec::new();
        handshake.push(19u8); // Length
        handshake.extend_from_slice(b"BitTorrent protocol");
        handshake.extend_from_slice(&[0u8; 8]); // Reserved
        handshake.extend_from_slice(&self.info_hash); // Info hash
        handshake.extend_from_slice(b"-RT0001-123456789012"); // Your peer ID

        self.socket.write_all(&handshake).await?;
        let mut response_buf = vec![0u8; 68];
        self.socket.read_exact(&mut response_buf).await?;

        if response_buf[0] != 19 {
            return Err(anyhow!("Invalid protocol length: {}", response_buf[0]));
        }

        if &response_buf[1..20] != b"BitTorrent protocol" {
            return Err(anyhow!("Invalid protocol name"));
        }

        let received_info_hash: [u8; 20] = response_buf[28..48].try_into()?;
        if received_info_hash != self.info_hash {
            return Err(anyhow!("Info hash mismatch"));
        }

        let peer_id: [u8; 20] = response_buf[48..68].try_into()?;
        self.peer_id = peer_id;
        Ok(())
    }

    pub async fn send_message(&mut self, peer_message: PeerMessage) -> Result<()> {
        Ok(())
    }
}

pub async fn connect(
    peer_ip: String,
    peer_port: u16,
    info_hash: [u8; 20],
) -> Result<PeerConnection> {
    let addr = format!("{}:{}", peer_ip, peer_port);
    let connection_timeout = Duration::from_secs(5);

    let socket = match timeout(connection_timeout, TcpStream::connect(&addr)).await {
        Ok(Ok(socket)) => socket,
        Ok(Err(e)) => return Err(anyhow!("Failed to connect to {}: {}", addr, e)),
        Err(_) => return Err(anyhow!("Connection timeout to {}", addr)),
    };

    Ok(PeerConnection::new(socket, peer_ip, peer_port, info_hash))
}

fn get_message_bytes(peer_message: PeerMessage) -> Result<()> {
    let mut message_bytes = vec![];
    match peer_message {
        PeerMessage::Choke => {
            message_bytes.extend_from_slice(&[0, 0, 0, 1]);
            message_bytes.push(0);
        }
        PeerMessage::Unchoke => {
            message_bytes.extend_from_slice(&[0, 0, 0, 1]);
            message_bytes.push(1);
        }
        PeerMessage::Interested => {
            message_bytes.extend_from_slice(&[0, 0, 0, 1]);
            message_bytes.push(2);
        }
        PeerMessage::NotInterested => {
            message_bytes.extend_from_slice(&[0, 0, 0, 1]);
            message_bytes.push(3);
        }
        PeerMessage::Have { piece_index } => {
            let mut buffe: Vec<u8> = vec![0; 4];
            let lenght: usize = piece_index.to_be_bytes().len() + 1;
            buffe = lenght.to_be_bytes().into();
            buffe.extend_from_slice(&piece_index.to_be_bytes());
            message_bytes.push(4);
            message_bytes.extend_from_slice(&buffe);
        }
        PeerMessage::Bitfield { bitfield } => {
            let mut buffe: Vec<u8> = vec![0; 4];
            let lenght: usize = bitfield.len() + 1;
            buffe = lenght.to_be_bytes().into();
            buffe.extend_from_slice(&bitfield);
            message_bytes.push(5);
            message_bytes.extend_from_slice(&buffe);
        }
        PeerMessage::Request {
            index,
            begin,
            length,
        } => {
            let mut buffe: Vec<u8> = vec![0; 4];
            let mut length_all: u32 = index + begin + length + 1;
            buffe = length_all.to_be_bytes().into();
            buffe.extend_from_slice(&length_all.to_be_bytes());
            message_bytes.push(6 as u8);
            message_bytes.extend_from_slice(&buffe);
        }
        PeerMessage::Piece => 7,
        PeerMessage::Cancel => 9,
        PeerMessage::KeepAlive => {
            message_bytes.extend_from_slice(&[0, 0, 0, 0]);
        }
    };
    Ok(())
}
