mod download;
mod peer;
mod torrent;
mod tracker;
#[tokio::main]
async fn main() {
    let (torrent, info_hash) = torrent::load_file("torrents/cwel.torrent").await.unwrap();
    println!("Torrent: {:?}", torrent);
    println!("Info hash: {:?}", info_hash);

    let peers = tracker::announce(&torrent.announce, info_hash, 0, 0, torrent.length())
        .await
        .unwrap();

    println!("\nFound {} peers:", peers.len());
    
    let num_pieces = torrent.info.pieces.len() / 20;
    println!("Total pieces to download: {}", num_pieces);
    
    let mut downloaded_pieces = vec![Vec::new(); num_pieces];
    
    for piece_index in 0..num_pieces {
        println!("\n=== Downloading piece {} of {} ===", piece_index + 1, num_pieces);
        
        for peer in &peers {
            println!("Trying peer {}:{}...", peer.ip, peer.port);
            
            let result = async {
                let mut conn = peer::connect(peer.ip.clone(), peer.port, info_hash).await?;
                conn.handshake().await?;

                let _bitfield = conn.receive_message().await?;

                let hash_start = piece_index * 20;
                let hash_end = hash_start + 20;
                let hash = &torrent.info.pieces[hash_start..hash_end];
                
                let piece_length = if piece_index == num_pieces - 1 {
                    let remainder = torrent.length() % torrent.info.piece_length;
                    if remainder == 0 { torrent.info.piece_length } else { remainder }
                } else {
                    torrent.info.piece_length
                };

                let piece_data = download::download_piece(&mut conn, piece_index as u32, piece_length, hash).await?;
                
                anyhow::Ok(piece_data)
            }.await;

            match result {
                Ok(piece_data) => {
                    println!("Downloaded piece {} successfully!", piece_index);
                    downloaded_pieces[piece_index] = piece_data;
                    break;
                }
                Err(e) => {
                    println!("Failed: {}", e);
                    continue;
                }
            }
        }
    }
    
    let mut full_file = Vec::new();
    for piece in downloaded_pieces {
        full_file.extend(piece);
    }
    
    tokio::fs::write(&torrent.info.name, &full_file).await.unwrap();
    println!("\n✓ Download complete! Saved to: {}", torrent.info.name);
}
