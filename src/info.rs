//! Describing a torrent (`rtrnt info`): its metadata as text, and its
//! magnet link.

use crate::magnet::{MagnetLink, hex};
use crate::torrent::{self, Metainfo};
use crate::tui::view::human_bytes;
use anyhow::Result;
use serde_bencode::value::Value;
use std::collections::HashMap;
use std::fmt::Write;

/// What a `.torrent` file says besides what `TorrentFile` keeps. Each is
/// left out if the file has it in an unexpected form, as it is not needed
/// to download.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Extras {
    /// `url-list` (BEP 19), a single URL or a list of them.
    pub web_seeds: Vec<String>,
    /// `creation date`, in seconds since the epoch.
    pub creation_date: Option<i64>,
    pub created_by: Option<String>,
    pub comment: Option<String>,
    /// `info.source`, a tag some trackers ask for.
    pub source: Option<String>,
}

impl Extras {
    /// Reads the extras of the `.torrent` file `data`, which must already
    /// have parsed.
    pub fn parse(data: &[u8]) -> Self {
        let Ok((top, _)) = crate::bencode::decode_dict(data) else {
            return Self::default();
        };
        let info = match top.get(&b"info"[..]) {
            Some(Value::Dict(info)) => Some(info),
            _ => None,
        };
        let web_seeds = match top.get(&b"url-list"[..]) {
            Some(Value::Bytes(url)) => vec![lossy(url)],
            Some(Value::List(urls)) => urls
                .iter()
                .filter_map(|url| match url {
                    Value::Bytes(url) => Some(lossy(url)),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        };
        let web_seeds = web_seeds
            .into_iter()
            .filter(|url| !url.is_empty())
            .collect();
        Self {
            web_seeds,
            creation_date: match top.get(&b"creation date"[..]) {
                Some(Value::Int(date)) => Some(*date),
                _ => None,
            },
            created_by: text(&top, "created by"),
            comment: text(&top, "comment"),
            source: info.and_then(|info| text(info, "source")),
        }
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn text(dict: &HashMap<Vec<u8>, Value>, key: &str) -> Option<String> {
    match dict.get(key.as_bytes()) {
        Some(Value::Bytes(text)) if !text.is_empty() => Some(lossy(text)),
        _ => None,
    }
}

/// `link` as a URI, with a `ws` parameter (BEP 19) for each of `web_seeds`.
pub fn magnet_uri(link: &MagnetLink, web_seeds: &[String]) -> String {
    // The colons of `xt` are left as they are, as other clients write it.
    let mut uri = link
        .to_uri()
        .replacen("xt=urn%3Abtih%3A", "xt=urn:btih:", 1);
    for url in web_seeds {
        let encoded: String = url::form_urlencoded::byte_serialize(url.as_bytes()).collect();
        uri.push_str("&ws=");
        uri.push_str(&encoded);
    }
    uri
}

/// The metadata of the `.torrent` file `data` as text for `rtrnt info`:
/// one line for each of name, info hash, size, pieces, whether it is
/// private, the dates and comment if there are any, then the trackers, web
/// seeds, files and magnet link.
pub fn describe(data: &[u8]) -> Result<String> {
    let metainfo = torrent::parse_metainfo(data)?;
    Ok(render(&metainfo, &Extras::parse(data)))
}

fn render(metainfo: &Metainfo, extras: &Extras) -> String {
    let Metainfo {
        torrent, info_hash, ..
    } = metainfo;
    let tiers = torrent.tracker_tiers();
    let mut out = String::new();
    // Writing to a `String` cannot fail.
    let mut line = |label: &str, value: &str| {
        let _ = writeln!(out, "{label:<14}{value}");
    };
    line("Name:", &torrent.info.name);
    line("Info hash:", &hex(info_hash));
    let size = torrent.length();
    line("Size:", &format!("{} ({size} bytes)", human_bytes(size)));
    line(
        "Piece length:",
        &format!(
            "{} ({} bytes)",
            human_bytes(torrent.info.piece_length),
            torrent.info.piece_length
        ),
    );
    line("Pieces:", &torrent.num_pieces().to_string());
    line("Private:", if torrent.is_private() { "yes" } else { "no" });
    if let Some(source) = &extras.source {
        line("Source:", source);
    }
    if let Some(date) = extras.creation_date {
        line("Created:", &format_date(date));
    }
    if let Some(by) = &extras.created_by {
        line("Created by:", by);
    }
    if let Some(comment) = &extras.comment {
        line("Comment:", comment);
    }
    if !tiers.is_empty() {
        let _ = writeln!(out, "Trackers:");
        for (i, tier) in tiers.iter().enumerate() {
            let _ = writeln!(out, "  tier {}: {}", i + 1, tier.join(", "));
        }
    }
    if !extras.web_seeds.is_empty() {
        let _ = writeln!(out, "Web seeds:");
        for url in &extras.web_seeds {
            let _ = writeln!(out, "  {url}");
        }
    }
    let files = file_list(torrent);
    let _ = writeln!(out, "Files ({}):", files.len());
    let width = files
        .iter()
        .map(|(_, length)| human_bytes(*length).len())
        .max()
        .unwrap_or(0);
    for (path, length) in &files {
        let _ = writeln!(out, "  {:>width$}  {path}", human_bytes(*length));
    }
    let link = MagnetLink {
        info_hash: *info_hash,
        name: Some(torrent.info.name.clone()),
        trackers: tiers.concat(),
        peers: Vec::new(),
    };
    let _ = writeln!(
        out,
        "{:<14}{}",
        "Magnet:",
        magnet_uri(&link, &extras.web_seeds)
    );
    out
}

/// A file's path, as the torrent lists it (under the torrent's name for a
/// multi-file torrent), and its length.
fn file_list(torrent: &torrent::TorrentFile) -> Vec<(String, u64)> {
    match &torrent.info.files {
        Some(files) => files
            .iter()
            .map(|file| (file.path.join("/"), file.length))
            .collect(),
        None => vec![(torrent.info.name.clone(), torrent.length())],
    }
}

/// `seconds` since the epoch as a UTC date and time.
fn format_date(seconds: i64) -> String {
    match chrono::DateTime::from_timestamp(seconds, 0) {
        Some(date) => date.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => format!("{seconds} (out of range)"),
    }
}

#[cfg(test)]
mod tests;
