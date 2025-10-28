use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::Digest;

#[derive(Deserialize, Serialize, Debug)]
pub struct TorrentFile {
    announce: String,
    info: Info,
}

#[derive(Deserialize, Serialize, Debug)]
struct Info {
    name: String,
    #[serde(rename = "piece length")]
    piece_length: u64,
    pieces: ByteBuf,
    #[serde(default)]
    length: Option<u64>,
    #[serde(default)]
    files: Option<Vec<FileInfo>>,
}

#[derive(Deserialize, Serialize, Debug)]
struct FileInfo {
    path: Vec<String>,
    length: u64,
}

pub async fn load_file(filepath: &str) -> anyhow::Result<(TorrentFile, [u8; 20])> {
    let file = tokio::fs::read(filepath).await?;
    let torrent: TorrentFile = serde_bencode::from_bytes(&file)?;

    // Re-encode just the info dict
    let info_bytes = serde_bencode::to_bytes(&torrent.info)?;

    // Hash it
    let mut hasher = sha1::Sha1::new();
    hasher.update(&info_bytes);
    let result = hasher.finalize();

    let mut hash = [0u8; 20];
    hash.copy_from_slice(&result);

    Ok((torrent, hash))
}
