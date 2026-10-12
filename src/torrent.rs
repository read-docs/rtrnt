use crate::bencode;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::path::Path;

/// Pieces are held in memory while downloading, so refuse absurd sizes.
const MAX_PIECE_LENGTH: u64 = 64 * 1024 * 1024;

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct TorrentFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub announce: Option<String>,
    #[serde(
        rename = "announce-list",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub announce_list: Option<Vec<Vec<String>>>,
    pub info: Info,
}

#[derive(Deserialize, Clone, Serialize, Debug)]
pub struct Info {
    pub name: String,
    #[serde(rename = "piece length")]
    pub piece_length: u64,
    pub pieces: ByteBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<FileInfo>>,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct FileInfo {
    pub path: Vec<String>,
    pub length: u64,
}

impl TorrentFile {
    /// Total size of all files in bytes.
    pub fn length(&self) -> u64 {
        self.info.length.unwrap_or_else(|| {
            self.info
                .files
                .as_ref()
                .map(|files| files.iter().map(|f| f.length).sum())
                .unwrap_or(0)
        })
    }

    pub fn num_pieces(&self) -> usize {
        self.info.pieces.len() / 20
    }

    /// Length of piece `index`; only the last piece may be shorter.
    pub fn piece_len(&self, index: usize) -> Result<u64> {
        if index >= self.num_pieces() {
            bail!("piece index {} out of range", index);
        }
        let start = index as u64 * self.info.piece_length;
        Ok(self.info.piece_length.min(self.length() - start))
    }

    pub fn piece_hash(&self, index: usize) -> Result<[u8; 20]> {
        let start = index
            .checked_mul(20)
            .ok_or_else(|| anyhow!("piece index {} out of range", index))?;
        self.info
            .pieces
            .get(start..start + 20)
            .and_then(|hash| hash.try_into().ok())
            .ok_or_else(|| anyhow!("piece index {} out of range", index))
    }

    /// All tracker URLs, `announce` first, then `announce-list` tiers in
    /// order, without duplicates.
    pub fn trackers(&self) -> Vec<String> {
        let tiers = self.announce_list.iter().flatten().flatten();
        let mut trackers: Vec<String> = Vec::new();
        for url in self.announce.iter().chain(tiers) {
            if !trackers.contains(url) {
                trackers.push(url.clone());
            }
        }
        trackers
    }

    fn validate(&self) -> Result<()> {
        let info = &self.info;
        if info.piece_length == 0 || info.piece_length > MAX_PIECE_LENGTH {
            bail!("unsupported piece length {}", info.piece_length);
        }
        if !info.pieces.len().is_multiple_of(20) {
            bail!("pieces field is not a multiple of 20 bytes");
        }
        match (&info.length, &info.files) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => bail!("torrent must have exactly one of `length` or `files`"),
        }
        let length = self.length();
        if length == 0 {
            bail!("torrent has no data");
        }
        let expected = length.div_ceil(info.piece_length);
        if self.num_pieces() as u64 != expected {
            bail!(
                "torrent has {} piece hashes but {} pieces of data",
                self.num_pieces(),
                expected
            );
        }
        Ok(())
    }
}

/// Parses a `.torrent` file and computes its info hash.
pub fn parse(data: &[u8]) -> Result<(TorrentFile, [u8; 20])> {
    let torrent: TorrentFile = serde_bencode::from_bytes(data).context("invalid torrent file")?;
    torrent.validate()?;

    // Hash the raw bytes of the info dict: re-encoding the parsed struct
    // would drop keys we don't model (e.g. `private`) and change the hash.
    let info_bytes = &data[bencode::info_span(data)?];
    let hash: [u8; 20] = sha1::Sha1::digest(info_bytes).into();

    Ok((torrent, hash))
}

pub async fn load_file(path: &Path) -> Result<(TorrentFile, [u8; 20])> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse(&data)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn single_file(length: u64, piece_length: u64) -> TorrentFile {
        let pieces = length.div_ceil(piece_length) as usize;
        TorrentFile {
            announce: None,
            announce_list: None,
            info: Info {
                name: "file".into(),
                piece_length,
                pieces: ByteBuf::from(vec![0u8; pieces * 20]),
                length: Some(length),
                files: None,
            },
        }
    }

    #[test]
    fn piece_lengths() {
        let t = single_file(100, 32);
        assert_eq!(t.num_pieces(), 4);
        assert_eq!(t.piece_len(0).unwrap(), 32);
        assert_eq!(t.piece_len(3).unwrap(), 4);
        assert!(t.piece_len(4).is_err());

        let exact = single_file(64, 32);
        assert_eq!(exact.piece_len(1).unwrap(), 32);
    }

    #[test]
    fn piece_hash_bounds() {
        let t = single_file(100, 32);
        assert!(t.piece_hash(3).is_ok());
        assert!(t.piece_hash(4).is_err());
        assert!(t.piece_hash(usize::MAX).is_err());
    }

    #[test]
    fn trackers_are_ordered_and_deduplicated() {
        let mut t = single_file(10, 32);
        t.announce = Some("udp://a".into());
        t.announce_list = Some(vec![
            vec!["udp://a".into(), "http://b".into()],
            vec!["http://c".into(), "http://b".into()],
        ]);
        assert_eq!(t.trackers(), ["udp://a", "http://b", "http://c"]);
    }

    #[test]
    fn info_hash_covers_unknown_keys() {
        let pieces = [7u8; 20];
        let mut info = b"d6:lengthi10e4:name4:file12:piece lengthi32e6:pieces20:".to_vec();
        info.extend_from_slice(&pieces);
        info.extend_from_slice(b"7:privatei1ee");
        let mut data = b"d8:announce8:http://x4:info".to_vec();
        data.extend_from_slice(&info);
        data.push(b'e');

        let (torrent, hash) = parse(&data).unwrap();
        assert_eq!(torrent.info.name, "file");
        assert_eq!(torrent.trackers(), ["http://x"]);
        let expected: [u8; 20] = sha1::Sha1::digest(&info).into();
        assert_eq!(hash, expected);
    }

    #[test]
    fn rejects_inconsistent_piece_count() {
        let mut t = single_file(100, 32);
        t.info.pieces = ByteBuf::from(vec![0u8; 3 * 20]);
        assert!(t.validate().is_err());
        t.info.pieces = ByteBuf::from(vec![0u8; 81]);
        assert!(t.validate().is_err());
    }
}
