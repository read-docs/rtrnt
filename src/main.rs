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
    for peer in peers {
        println!("  {}:{}", peer.ip, peer.port);
    }
}
