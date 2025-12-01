mod download;
mod peer;
mod torrent;
mod tracker;
#[tokio::main]
async fn main() {
    download::download_file("torrents/debian-13.2.0-amd64-netinst.iso.torrent")
        .await
        .unwrap();
}
