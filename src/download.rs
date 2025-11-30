use crate::peer::{PeerConnection, PeerMessage};
use anyhow::{Result, anyhow};
use sha1::Digest;

const BLOCK_SIZE: u32 = 16384;

pub async fn download_piece(
    connection: &mut PeerConnection,
    piece_index: u32,
    piece_length: u64,
    expected_hash: &[u8],
) -> Result<Vec<u8>> {
    connection.send_message(PeerMessage::Interested).await?;

    loop {
        let msg = connection.receive_message().await?;
        if matches!(msg, PeerMessage::Unchoke) {
            break;
        }
    }

    let num_blocks = (piece_length + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64;
    let mut piece_data = vec![0u8; piece_length as usize];

    for i in 0..num_blocks {
        let begin = i * BLOCK_SIZE as u64;
        let length = if i == num_blocks - 1 {
            piece_length - begin
        } else {
            BLOCK_SIZE as u64
        };

        connection
            .send_message(PeerMessage::Request {
                index: piece_index,
                begin: begin as u32,
                length: length as u32,
            })
            .await?;
    }

    let mut blocks_received = 0;
    while blocks_received < num_blocks {
        let msg = connection.receive_message().await?;

        if let PeerMessage::Piece {
            index,
            begin,
            block,
        } = msg
        {
            if index == piece_index {
                let start = begin as usize;
                let end = start + block.len();
                piece_data[start..end].copy_from_slice(&block);
                blocks_received += 1;
            }
        }
    }

    let mut hasher = sha1::Sha1::new();
    hasher.update(&piece_data);
    let actual_hash = hasher.finalize();

    if actual_hash.as_slice() != expected_hash {
        return Err(anyhow!("Piece hash mismatch!"));
    }

    Ok(piece_data)
}
