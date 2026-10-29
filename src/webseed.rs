//! Web seeds: HTTP servers that hold the torrent's files, as pieces sources
//! next to peers.
//!
//! BEP 19 (`url-list`) maps the torrent onto URLs, and a piece is fetched
//! with one `Range` request for each file it touches. BEP 17 (`httpseeds`)
//! asks a script for a whole piece by number. This module only maps and
//! fetches; which piece to ask for, and when to give up on a server, is up
//! to the download (`download::web`).

use crate::torrent::TorrentFile;
use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use reqwest::header::RANGE;
use std::sync::LazyLock;
use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A response that sends nothing for this long is given up on.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Shared by every web seed, so connections to a server are reused.
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .read_timeout(READ_TIMEOUT)
        .build()
        .expect("the HTTP client builds with its default settings")
});

/// Which protocol a web seed speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSeedKind {
    /// BEP 19: the torrent's files are served at URLs, and ranges of them
    /// requested.
    Url,
    /// BEP 17: a script serves whole pieces.
    Http,
}

/// One web seed of a torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebSeed {
    pub kind: WebSeedKind,
    pub url: String,
}

impl WebSeed {
    /// The web seeds of `torrent`: its BEP 19 URLs, then its BEP 17 ones.
    pub fn of(torrent: &TorrentFile) -> Vec<WebSeed> {
        let url = |kind| move |url| WebSeed { kind, url };
        torrent
            .web_seeds()
            .into_iter()
            .map(url(WebSeedKind::Url))
            .chain(torrent.http_seeds().into_iter().map(url(WebSeedKind::Http)))
            .collect()
    }
}

/// A byte range of one file of a BEP 19 web seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileRange {
    pub url: String,
    /// Offset of the first byte in the file.
    pub start: u64,
    pub len: u64,
    /// Size of the whole file.
    pub file_len: u64,
}

/// Percent-encodes `bytes`, leaving only the unreserved characters of
/// RFC 3986.
pub fn percent_encode(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len());
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

/// The ranges that make up piece `index`, one for each file it touches, in
/// order (BEP 19). `base` is the web seed's URL: for a single-file torrent
/// it names the file, unless it ends in a slash, when the torrent's name is
/// appended; for a multi-file torrent it names the directory in which the
/// torrent's name is looked for.
pub fn file_ranges(base: &str, torrent: &TorrentFile, index: usize) -> Result<Vec<FileRange>> {
    let piece_len = torrent.piece_len(index)?;
    let begin = index as u64 * torrent.info.piece_length;
    let end = begin + piece_len;
    let name = percent_encode(torrent.info.name.as_bytes());
    let Some(files) = &torrent.info.files else {
        let url = if base.ends_with('/') {
            format!("{base}{name}")
        } else {
            base.to_string()
        };
        return Ok(vec![FileRange {
            url,
            start: begin,
            len: piece_len,
            file_len: torrent.length(),
        }]);
    };
    let root = if base.ends_with('/') {
        format!("{base}{name}")
    } else {
        format!("{base}/{name}")
    };
    let mut ranges = Vec::new();
    let mut offset = 0u64;
    for file in files {
        let file_end = offset + file.length;
        let (from, to) = (begin.max(offset), end.min(file_end));
        if from < to {
            let path: Vec<String> = file
                .path
                .iter()
                .map(|part| percent_encode(part.as_bytes()))
                .collect();
            ranges.push(FileRange {
                url: format!("{root}/{}", path.join("/")),
                start: from - offset,
                len: to - from,
                file_len: file.length,
            });
        }
        offset = file_end;
        if offset >= end {
            break;
        }
    }
    Ok(ranges)
}

/// The URL that asks a BEP 17 seed at `base` for piece `index`.
pub fn http_seed_url(base: &str, info_hash: &[u8; 20], index: usize) -> String {
    let separator = if base.contains('?') { '&' } else { '?' };
    format!(
        "{base}{separator}info_hash={}&piece={index}",
        percent_encode(info_hash)
    )
}

/// Downloads piece `index` of `torrent` from `seed`, calling `received`
/// with the size of each chunk as it arrives. The data is not checked
/// against the piece's hash.
pub async fn fetch_piece(
    seed: &WebSeed,
    torrent: &TorrentFile,
    info_hash: &[u8; 20],
    index: usize,
    mut received: impl FnMut(u64),
) -> Result<Vec<u8>> {
    let piece_len = torrent.piece_len(index)?;
    let mut data = Vec::with_capacity(usize::try_from(piece_len).unwrap_or(0));
    match seed.kind {
        WebSeedKind::Url => {
            for range in file_ranges(&seed.url, torrent, index)? {
                let request = CLIENT.get(&range.url).header(
                    RANGE,
                    format!("bytes={}-{}", range.start, range.start + range.len - 1),
                );
                // A server that ignores the range sends the whole file,
                // which is what we asked for if that is all we wanted.
                let whole = range.start == 0 && range.len == range.file_len;
                read_body(request, whole, range.len, &mut data, &mut received)
                    .await
                    .with_context(|| range.url.clone())?;
            }
        }
        WebSeedKind::Http => {
            let url = http_seed_url(&seed.url, info_hash, index);
            read_body(CLIENT.get(&url), true, piece_len, &mut data, &mut received)
                .await
                .with_context(|| seed.url.clone())?;
        }
    }
    Ok(data)
}

/// Appends the `len` bytes of the response to `request` to `data`. A 200 is
/// accepted only if `whole`, a 206 always.
async fn read_body(
    request: reqwest::RequestBuilder,
    whole: bool,
    len: u64,
    data: &mut Vec<u8>,
    received: &mut impl FnMut(u64),
) -> Result<()> {
    let mut response = request.send().await?;
    let status = response.status();
    if status != StatusCode::PARTIAL_CONTENT && !(whole && status == StatusCode::OK) {
        bail!("HTTP {status}");
    }
    let mut got = 0u64;
    while let Some(chunk) = response.chunk().await? {
        got += chunk.len() as u64;
        if got > len {
            bail!("sent more than the {len} bytes asked for");
        }
        data.extend_from_slice(&chunk);
        received(chunk.len() as u64);
    }
    if got != len {
        bail!("sent {got} of the {len} bytes asked for");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
