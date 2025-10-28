use std::env::current_dir;

mod torrent;
#[tokio::main]
async fn main() {
    let naplet = torrent::load_file("torrents/naplet.torrent").await.unwrap();
    println!("{:?}", naplet);
}
