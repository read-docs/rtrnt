use crate::{
    peer::{PeerConnection, PeerMessage},
    torrent, tracker,
    tracker::Peer,
};
use anyhow::{Result, anyhow};
use sha1::Digest;
use std::sync::{Arc, Mutex};

const BLOCK_SIZE: u32 = 16384;

pub async fn download_piece(
    connection: &mut PeerConnection,
    piece_index: u32,
    piece_length: u64,
    expected_hash: &[u8],
) -> Result<Vec<u8>> {
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

pub async fn download_file(torrent_path: &str) -> Result<()> {
    let (torrent, info_hash) = torrent::load_file(torrent_path).await?;
    println!("Torrent: {:?}", torrent);
    println!("Info hash: {:?}", info_hash);

    let peers = tracker::announce(&torrent.announce, info_hash, 0, 0, torrent.length()).await?;
    println!("\nFound {} peers:", peers.len());

    let num_pieces = torrent.info.pieces.len() / 20;
    println!("Total pieces to download: {}", num_pieces);

    let pieces_queue = Arc::new(Mutex::new((0..num_pieces).collect::<Vec<usize>>()));
    let downloaded = Arc::new(Mutex::new(vec![Vec::new(); num_pieces]));

    let mut tasks = vec![];

    for peer in peers.iter().take(20) {
        println!("Spawning task for peer {}:{}", peer.ip, peer.port);

        let queue = pieces_queue.clone();
        let results = downloaded.clone();
        let peer = peer.clone();
        let info_hash_copy = info_hash;
        let pieces_bytes = torrent.info.pieces.clone();
        let piece_length = torrent.info.piece_length;
        let total_length = torrent.length();

        let task = tokio::spawn(async move {
            println!("[{}:{}] Attempting connection...", peer.ip, peer.port);
            let result: Result<()> = async {
                let mut conn =
                    crate::peer::connect(peer.ip.clone(), peer.port, info_hash_copy).await?;
                conn.handshake().await?;
                conn.negotiate().await?;

                println!("[{}:{}] Connected and ready", peer.ip, peer.port);

                loop {
                    let piece_index = {
                        let mut queue = queue.lock().unwrap();
                        queue.pop()
                    };

                    let piece_index = match piece_index {
                        Some(idx) => idx,
                        None => break,
                    };

                    println!(
                        "[{}:{}] Downloading piece {}",
                        peer.ip, peer.port, piece_index
                    );

                    let hash_start = piece_index * 20;
                    let hash_end = hash_start + 20;
                    let hash = &pieces_bytes[hash_start..hash_end];

                    let piece_len = if piece_index == num_pieces - 1 {
                        let remainder = total_length % piece_length;
                        if remainder == 0 {
                            piece_length
                        } else {
                            remainder
                        }
                    } else {
                        piece_length
                    };

                    let piece_data =
                        download_piece(&mut conn, piece_index as u32, piece_len, hash).await?;

                    {
                        let mut results = results.lock().unwrap();
                        results[piece_index] = piece_data;
                    }

                    println!(
                        "[{}:{}] Completed piece {}",
                        peer.ip, peer.port, piece_index
                    );
                }

                Ok(())
            }
            .await;

            if let Err(e) = result {
                println!("[{}:{}] Error: {}", peer.ip, peer.port, e);
            }
        });

        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    let downloaded_pieces = Arc::try_unwrap(downloaded).unwrap().into_inner().unwrap();

    // Check if all pieces were downloaded
    let mut missing_pieces = Vec::new();
    for (i, piece) in downloaded_pieces.iter().enumerate() {
        if piece.is_empty() {
            missing_pieces.push(i);
        }
    }

    if !missing_pieces.is_empty() {
        return Err(anyhow!(
            "Failed to download {} pieces: {:?}",
            missing_pieces.len(),
            missing_pieces
        ));
    }

    let mut full_file = Vec::new();
    for piece in downloaded_pieces {
        full_file.extend(piece);
    }

    tokio::fs::write(&torrent.info.name, &full_file).await?;
    println!("\n✓ Download complete! Saved to: {}", torrent.info.name);

    Ok(())
}
