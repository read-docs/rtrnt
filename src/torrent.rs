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
    /// Web seeds (BEP 19), a single URL or a list of them.
    #[serde(
        rename = "url-list",
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub url_list: Vec<String>,
    /// HTTP seeds (BEP 17).
    #[serde(
        default,
        deserialize_with = "string_or_list",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub httpseeds: Vec<String>,
    pub info: Info,
}

/// Reads a list of strings, or a lone string as a list of one. Entries that
/// are empty or not UTF-8 are dropped, as many torrents carry an empty
/// `url-list`.
fn string_or_list<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    use serde::de::{SeqAccess, Visitor};
    struct Strings;
    fn keep(bytes: &[u8]) -> Vec<String> {
        match std::str::from_utf8(bytes) {
            Ok(url) if !url.is_empty() => vec![url.to_string()],
            _ => Vec::new(),
        }
    }
    impl<'de> Visitor<'de> for Strings {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a string or a list of strings")
        }
        fn visit_str<E>(self, v: &str) -> std::result::Result<Vec<String>, E> {
            Ok(keep(v.as_bytes()))
        }
        fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Vec<String>, E> {
            Ok(keep(v))
        }
        fn visit_seq<A: SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Vec<String>, A::Error> {
            let mut urls = Vec::new();
            while let Some(item) = seq.next_element::<ByteBuf>()? {
                urls.extend(keep(&item));
            }
            Ok(urls)
        }
    }
    deserializer.deserialize_any(Strings)
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
    /// Total size of all files in bytes. A torrent whose size overflows is
    /// refused by `validate`, so this is exact for every parsed torrent.
    pub fn length(&self) -> u64 {
        self.checked_length().unwrap_or(0)
    }

    /// Total size of all files, or `None` if it does not fit in a `u64`.
    fn checked_length(&self) -> Option<u64> {
        match (self.info.length, &self.info.files) {
            (Some(length), _) => Some(length),
            (None, Some(files)) => files
                .iter()
                .try_fold(0u64, |total, f| total.checked_add(f.length)),
            (None, None) => Some(0),
        }
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

    /// The `http` and `https` URLs of the torrent's web seeds (BEP 19), each
    /// once, in the order listed.
    pub fn web_seeds(&self) -> Vec<String> {
        http_urls(&self.url_list)
    }

    /// The `http` and `https` URLs of the torrent's HTTP seeds (BEP 17),
    /// each once, in the order listed.
    pub fn http_seeds(&self) -> Vec<String> {
        http_urls(&self.httpseeds)
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
        let Some(length) = self.checked_length() else {
            bail!("total size of the files overflows");
        };
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

fn http_urls(urls: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    urls.iter()
        .filter(|url| {
            let scheme = url.split_once(':').map(|(scheme, _)| scheme);
            scheme
                .is_some_and(|s| s.eq_ignore_ascii_case("http") || s.eq_ignore_ascii_case("https"))
        })
        .filter(|url| seen.insert(url.as_str()))
        .cloned()
        .collect()
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
    // Scanned first because it limits the nesting depth, which
    // `serde_bencode` would follow until the stack overflows.
    bencode::value_len(data).context("invalid torrent file")?;
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
        url_list: Vec::new(),
        httpseeds: Vec::new(),
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
    data.extend(bencoded(&crate::peer::extension::CLIENT)?);
    data.extend_from_slice(b"4:info");
    data.extend_from_slice(info_bytes);
    data.push(b'e');
    Ok(data)
}

fn bencoded<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_bencode::to_bytes(value).context("cannot encode torrent")
}

#[cfg(test)]
mod tests;
