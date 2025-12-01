use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::Digest;

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TorrentFile {
    pub announce: String,
    pub info: Info,
}

impl TorrentFile {
    pub fn length(&self) -> u64 {
        self.info.length.unwrap_or_else(|| {
            self.info
                .files
                .as_ref()
                .map(|files| files.iter().map(|f| f.length).sum())
                .unwrap_or(0)
        })
    }
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct Info {
    pub name: String,
    #[serde(rename = "piece length")]
    pub piece_length: u64,
    pub pieces: ByteBuf,
    #[serde(default)]
    pub length: Option<u64>,
    #[serde(default)]
    pub files: Option<Vec<FileInfo>>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
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
