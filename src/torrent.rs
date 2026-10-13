use crate::bencode;
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::collections::HashSet;
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
    /// `1` for a private torrent (BEP 27): its peers come from its trackers
    /// only, never from the DHT or peer exchange.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub private: Option<i64>,
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

    /// Whether the torrent is private (BEP 27).
    pub fn is_private(&self) -> bool {
        self.info.private == Some(1)
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

    /// Tracker URLs in tiers (BEP 12): the `announce-list` tiers if it names
    /// any tracker, in which case `announce` is ignored, otherwise
    /// `announce` alone. A URL listed twice is kept where it first appears,
    /// and empty tiers are dropped.
    pub fn tracker_tiers(&self) -> Vec<Vec<String>> {
        let listed = self
            .announce_list
            .as_ref()
            .filter(|tiers| tiers.iter().any(|tier| !tier.is_empty()));
        let tiers = match listed {
            Some(tiers) => tiers.clone(),
            None => self.announce.iter().map(|url| vec![url.clone()]).collect(),
        };
        let mut seen = HashSet::new();
        tiers
            .into_iter()
            .map(|tier| -> Vec<String> {
                tier.into_iter()
                    .filter(|url| seen.insert(url.clone()))
                    .collect()
            })
            .filter(|tier| !tier.is_empty())
            .collect()
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

/// A parsed torrent with its info hash and the exact bytes of its info
/// dictionary, which peers fetching the torrent by magnet link are sent
/// (BEP 9).
pub struct Metainfo {
    pub torrent: TorrentFile,
    pub info_hash: [u8; 20],
    pub info_bytes: Vec<u8>,
}

/// Parses a `.torrent` file and computes its info hash.
pub fn parse(data: &[u8]) -> Result<(TorrentFile, [u8; 20])> {
    let metainfo = parse_metainfo(data)?;
    Ok((metainfo.torrent, metainfo.info_hash))
}

/// Parses a `.torrent` file, keeping the bytes of its info dictionary.
pub fn parse_metainfo(data: &[u8]) -> Result<Metainfo> {
    let torrent: TorrentFile = serde_bencode::from_bytes(data).context("invalid torrent file")?;
    torrent.validate()?;

    // Hash the raw bytes of the info dict: re-encoding the parsed struct
    // would drop keys we don't model (e.g. `private`) and change the hash.
    let info_bytes = data[bencode::info_span(data)?].to_vec();
    let info_hash: [u8; 20] = sha1::Sha1::digest(&info_bytes).into();

    Ok(Metainfo {
        torrent,
        info_hash,
        info_bytes,
    })
}

pub async fn load_file(path: &Path) -> Result<(TorrentFile, [u8; 20])> {
    let metainfo = load_metainfo(path).await?;
    Ok((metainfo.torrent, metainfo.info_hash))
}

/// Reads and parses a `.torrent` file, keeping the bytes of its info
/// dictionary.
pub async fn load_metainfo(path: &Path) -> Result<Metainfo> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("failed to read {}", path.display()))?;
    parse_metainfo(&data)
}

/// Builds a torrent from an info dictionary fetched from peers (BEP 9),
/// announcing to the tracker `tiers`. The caller checks the info hash.
pub fn from_info(info_bytes: &[u8], tiers: &[Vec<String>]) -> Result<TorrentFile> {
    // Scanned first because it limits the nesting depth, which
    // `serde_bencode` would follow until the stack overflows.
    let len = bencode::value_len(info_bytes).context("metadata is not bencoded")?;
    if info_bytes.first() != Some(&b'd') || len != info_bytes.len() {
        bail!("metadata is not a bencoded dictionary");
    }
    let info: Info = serde_bencode::from_bytes(info_bytes).context("invalid metadata")?;
    let torrent = TorrentFile {
        announce: tiers.iter().flatten().next().cloned(),
        announce_list: (!tiers.is_empty()).then(|| tiers.to_vec()),
        info,
    };
    torrent.validate()?;
    Ok(torrent)
}

/// Encodes a `.torrent` file announcing to the tracker `tiers` around
/// `info_bytes`, which go in unchanged so the info hash stays the same.
pub fn encode_with_info(tiers: &[Vec<String>], info_bytes: &[u8]) -> Result<Vec<u8>> {
    // Keys in sorted order, as bencode requires.
    let mut data = b"d".to_vec();
    if let Some(first) = tiers.iter().flatten().next() {
        data.extend_from_slice(b"8:announce");
        data.extend(bencoded(first)?);
        data.extend_from_slice(b"13:announce-list");
        data.extend(bencoded(&tiers)?);
    }
    data.extend_from_slice(b"10:created by");
    data.extend(bencoded(&crate::extension::CLIENT)?);
    data.extend_from_slice(b"4:info");
    data.extend_from_slice(info_bytes);
    data.push(b'e');
    Ok(data)
}

fn bencoded<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_bencode::to_bytes(value).context("cannot encode torrent")
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
                private: None,
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
    fn tracker_tiers_follow_bep_12() {
        let mut t = single_file(10, 32);
        assert!(t.tracker_tiers().is_empty());

        t.announce = Some("udp://x".into());
        assert_eq!(t.tracker_tiers(), [["udp://x"]]);
        // An announce-list without trackers does not count.
        t.announce_list = Some(vec![vec![]]);
        assert_eq!(t.tracker_tiers(), [["udp://x"]]);

        // Otherwise it replaces `announce`; duplicates and the tiers they
        // leave empty are dropped.
        t.announce_list = Some(vec![
            vec!["udp://a".into(), "http://b".into()],
            vec![],
            vec!["http://c".into(), "http://b".into()],
            vec!["udp://a".into()],
        ]);
        assert_eq!(
            t.tracker_tiers(),
            [vec!["udp://a", "http://b"], vec!["http://c"]]
        );
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
        assert!(torrent.is_private());
        assert_eq!(torrent.tracker_tiers(), [["http://x"]]);
        let expected: [u8; 20] = sha1::Sha1::digest(&info).into();
        assert_eq!(hash, expected);
    }

    /// An info dict with a key we do not model, so re-encoding the parsed
    /// struct would change it.
    fn raw_info() -> Vec<u8> {
        let mut info = b"d6:lengthi10e4:name4:file12:piece lengthi32e6:pieces20:".to_vec();
        info.extend_from_slice(&[7u8; 20]);
        info.extend_from_slice(b"6:source3:xyze");
        info
    }

    #[test]
    fn builds_a_torrent_from_fetched_metadata() {
        let info = raw_info();
        let tiers = vec![vec!["udp://a".to_string()], vec!["http://b".to_string()]];
        let torrent = from_info(&info, &tiers).unwrap();
        assert_eq!(torrent.info.name, "file");
        assert_eq!(torrent.length(), 10);
        assert_eq!(torrent.tracker_tiers(), tiers);
        assert!(from_info(&info, &[]).unwrap().tracker_tiers().is_empty());

        let mut trailing = info.clone();
        trailing.push(b'x');
        let cases: &[&[u8]] = &[b"", b"le", b"i1e", b"de", &trailing, &info[1..]];
        for case in cases {
            assert!(from_info(case, &tiers).is_err(), "accepted {case:?}");
        }
    }

    #[test]
    fn saved_torrent_keeps_the_info_dict_byte_for_byte() {
        let info = raw_info();
        let hash: [u8; 20] = sha1::Sha1::digest(&info).into();
        let tiers = vec![
            vec!["udp://a".to_string(), "udp://b".to_string()],
            vec!["http://c".to_string()],
        ];
        for tiers in [tiers, Vec::new()] {
            let data = encode_with_info(&tiers, &info).unwrap();
            let metainfo = parse_metainfo(&data).unwrap();
            assert_eq!(metainfo.info_hash, hash);
            assert_eq!(metainfo.info_bytes, info);
            assert_eq!(metainfo.torrent.tracker_tiers(), tiers);
        }
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
