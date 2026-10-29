//! Making a torrent (`rtrnt create`): lists the files of a path, hashes
//! their data in pieces on several threads, and bencodes the result.
//!
//! Files are listed in sorted order, so the same data always gives the same
//! info hash. Symbolic links are not followed or included; `Created::skipped`
//! lists them so the caller can say so.

use crate::info;
use crate::magnet::MagnetLink;
use crate::peer::extension::CLIENT;
use crate::storage::verify::MAX_THREADS;
use anyhow::{Context, Result, anyhow, bail};
use sha1::{Digest, Sha1};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The smallest piece length: the size of a block requests are made in.
pub const MIN_PIECE_LENGTH: u64 = 16 * 1024;
/// The largest piece length chosen when none is given.
pub const MAX_AUTO_PIECE_LENGTH: u64 = 16 * 1024 * 1024;
/// The largest piece length accepted, which is what the parser reads.
pub const MAX_PIECE_LENGTH: u64 = 64 * 1024 * 1024;
/// The most pieces a torrent should have when its piece length is chosen.
const TARGET_MAX_PIECES: u64 = 2000;

/// What to make a torrent of, and what to put in it.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The file, or the directory of files, to share.
    pub path: PathBuf,
    /// Tracker URLs, each in a tier of its own, in order.
    pub trackers: Vec<String>,
    /// Web seed URLs (BEP 19).
    pub web_seeds: Vec<String>,
    /// Bytes per piece; chosen by `choose_piece_length` if `None`.
    pub piece_length: Option<u64>,
    /// Mark the torrent private (BEP 27).
    pub private: bool,
    pub comment: Option<String>,
    /// A tag inside the info dictionary, which makes the torrent's info
    /// hash differ from one made of the same data for another source.
    pub source: Option<String>,
    /// Seconds since the epoch, for the `creation date` key.
    pub created_at: Option<i64>,
}

/// A torrent made by `build`.
#[derive(Debug)]
pub struct Created {
    /// The `.torrent` file's contents.
    pub data: Vec<u8>,
    pub info_hash: [u8; 20],
    pub name: String,
    pub size: u64,
    pub piece_length: u64,
    pub pieces: usize,
    /// Symbolic links (and other things that are not files) left out.
    pub skipped: Vec<PathBuf>,
    pub magnet: String,
}

/// A file to put in the torrent, with where its data starts in the
/// concatenation of all files.
struct Entry {
    path: PathBuf,
    /// Path components below the torrent's name.
    parts: Vec<String>,
    length: u64,
    offset: u64,
}

/// The piece length for `total` bytes of data: the smallest power of two
/// that gives at most 2000 pieces (so over 1000 unless a limit is hit),
/// within 16 KiB to 16 MiB.
pub fn choose_piece_length(total: u64) -> u64 {
    let mut length = MIN_PIECE_LENGTH;
    while length < MAX_AUTO_PIECE_LENGTH && total.div_ceil(length) > TARGET_MAX_PIECES {
        length *= 2;
    }
    length
}

/// Parses a piece length given in bytes, or with a `k` or `m` suffix (KiB
/// or MiB). It must be a power of two from 16 KiB to 64 MiB.
pub fn parse_piece_length(arg: &str) -> Result<u64, String> {
    let lower = arg.trim().to_ascii_lowercase();
    let (digits, unit) = if let Some(digits) = lower.strip_suffix('k') {
        (digits, 1024)
    } else if let Some(digits) = lower.strip_suffix('m') {
        (digits, 1024 * 1024)
    } else {
        (lower.as_str(), 1)
    };
    let length = digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(unit))
        .ok_or_else(|| format!("{arg:?} is not a size in bytes (or KiB with k, MiB with m)"))?;
    if !is_valid_piece_length(length) {
        return Err(format!(
            "{arg:?} is not a power of two from 16k to 64m (16384 to 67108864 bytes)"
        ));
    }
    Ok(length)
}

fn is_valid_piece_length(length: u64) -> bool {
    length.is_power_of_two() && (MIN_PIECE_LENGTH..=MAX_PIECE_LENGTH).contains(&length)
}

/// Makes the torrent of `options.path`. Blocks while hashing: `on_total`
/// gets the number of bytes that will be hashed before that starts, and
/// `on_hashed` the length of each piece once it has been hashed, from any
/// of the hashing threads.
pub fn build(
    options: &Options,
    on_total: &dyn Fn(u64),
    on_hashed: &(dyn Fn(u64) + Sync),
) -> Result<Created> {
    for url in &options.trackers {
        check_url(url, "tracker", &["http", "https", "udp"])?;
    }
    for url in &options.web_seeds {
        check_url(url, "web seed", &["http", "https"])?;
    }
    let (name, entries, skipped, single) = list_files(&options.path)?;
    let size: u64 = entries.iter().map(|entry| entry.length).sum();
    if size == 0 {
        bail!("{} has no data to share", options.path.display());
    }
    let piece_length = options
        .piece_length
        .unwrap_or_else(|| choose_piece_length(size));
    if !is_valid_piece_length(piece_length) {
        bail!("piece length {piece_length} is not a power of two from 16 KiB to 64 MiB");
    }
    on_total(size);
    let pieces = hash_pieces(&entries, size, piece_length, on_hashed)?;

    let mut info_dict = Dict::default();
    if single {
        info_dict.int("length", size as i64);
    } else {
        info_dict.raw("files", files_list(&entries));
    }
    info_dict.string("name", &name);
    info_dict.int("piece length", piece_length as i64);
    info_dict.bytes("pieces", &pieces);
    if options.private {
        info_dict.int("private", 1);
    }
    if let Some(source) = &options.source {
        info_dict.string("source", source);
    }
    let info_bytes = info_dict.finish();
    let info_hash: [u8; 20] = Sha1::digest(&info_bytes).into();

    let mut top = Dict::default();
    if let Some(first) = options.trackers.first() {
        top.string("announce", first);
        let tiers: Vec<Vec<u8>> = options
            .trackers
            .iter()
            .map(|url| list(&[string(url)]))
            .collect();
        top.raw("announce-list", list(&tiers));
    }
    if let Some(comment) = &options.comment {
        top.string("comment", comment);
    }
    top.string("created by", CLIENT);
    if let Some(at) = options.created_at {
        top.int("creation date", at);
    }
    top.raw("info", info_bytes);
    if !options.web_seeds.is_empty() {
        let urls: Vec<Vec<u8>> = options.web_seeds.iter().map(|url| string(url)).collect();
        top.raw("url-list", list(&urls));
    }

    let link = MagnetLink {
        info_hash,
        name: Some(name.clone()),
        trackers: options.trackers.clone(),
        peers: Vec::new(),
    };
    Ok(Created {
        data: top.finish(),
        info_hash,
        size,
        piece_length,
        pieces: pieces.len() / 20,
        skipped,
        magnet: info::magnet_uri(&link, &options.web_seeds),
        name,
    })
}

/// Fails unless `url` parses and has one of `schemes`.
fn check_url(url: &str, what: &str, schemes: &[&str]) -> Result<()> {
    let parsed = url::Url::parse(url).with_context(|| format!("invalid {what} URL {url:?}"))?;
    if !schemes.contains(&parsed.scheme()) {
        bail!("{what} URL {url:?} must start with {}", schemes.join(", "));
    }
    Ok(())
}

/// The torrent's name, its files in order, the symlinks left out, and
/// whether it is a single file.
type Listing = (String, Vec<Entry>, Vec<PathBuf>, bool);

fn list_files(path: &Path) -> Result<Listing> {
    let meta =
        std::fs::metadata(path).with_context(|| format!("cannot read {}", path.display()))?;
    let canonical = path
        .canonicalize()
        .with_context(|| format!("cannot resolve {}", path.display()))?;
    let name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("{} has no usable name", path.display()))?
        .to_string();
    if meta.is_file() {
        let entry = Entry {
            path: canonical,
            parts: Vec::new(),
            length: meta.len(),
            offset: 0,
        };
        return Ok((name, vec![entry], Vec::new(), true));
    }
    if !meta.is_dir() {
        bail!("{} is neither a file nor a directory", path.display());
    }
    let (mut entries, mut skipped) = (Vec::new(), Vec::new());
    walk(&canonical, &mut Vec::new(), &mut entries, &mut skipped)?;
    // By path components, so a directory's files stay together.
    entries.sort_by(|a, b| a.parts.cmp(&b.parts));
    let mut offset = 0;
    for entry in &mut entries {
        entry.offset = offset;
        offset += entry.length;
    }
    skipped.sort();
    Ok((name, entries, skipped, false))
}

fn walk(
    dir: &Path,
    parts: &mut Vec<String>,
    entries: &mut Vec<Entry>,
    skipped: &mut Vec<PathBuf>,
) -> Result<()> {
    let read = std::fs::read_dir(dir).with_context(|| format!("cannot read {}", dir.display()))?;
    for item in read {
        let item = item.with_context(|| format!("cannot read {}", dir.display()))?;
        let path = item.path();
        let kind = item
            .file_type()
            .with_context(|| format!("cannot read {}", path.display()))?;
        let file_name = item.file_name();
        let part = file_name
            .to_str()
            .ok_or_else(|| anyhow!("{} has a name that is not UTF-8", path.display()))?
            .to_string();
        if kind.is_dir() {
            parts.push(part);
            walk(&path, parts, entries, skipped)?;
            parts.pop();
        } else if kind.is_file() {
            let length = item
                .metadata()
                .with_context(|| format!("cannot read {}", path.display()))?
                .len();
            let mut file_parts = parts.clone();
            file_parts.push(part);
            entries.push(Entry {
                path,
                parts: file_parts,
                length,
                offset: 0,
            });
        } else {
            // A symbolic link, or a socket or the like.
            skipped.push(path);
        }
    }
    Ok(())
}

/// Hashes the concatenation of the files in pieces of `piece_length` and
/// returns the 20-byte hashes joined, in order.
///
/// The threads take pieces in turn, as `verify::verify` does, each reading
/// the piece's bytes from the files it overlaps.
fn hash_pieces(
    entries: &[Entry],
    size: u64,
    piece_length: u64,
    on_hashed: &(dyn Fn(u64) + Sync),
) -> Result<Vec<u8>> {
    let count = size.div_ceil(piece_length) as usize;
    let next = AtomicUsize::new(0);
    let worker = || -> Result<Vec<(usize, [u8; 20])>> {
        let mut done = Vec::new();
        let mut buf = Vec::new();
        loop {
            let index = next.fetch_add(1, Ordering::Relaxed);
            if index >= count {
                return Ok(done);
            }
            let start = index as u64 * piece_length;
            let len = piece_length.min(size - start) as usize;
            if let Err(e) = read_range(entries, start, len, &mut buf) {
                // Stop the other threads at their next piece.
                next.store(count, Ordering::Relaxed);
                return Err(e);
            }
            done.push((index, Sha1::digest(&buf).into()));
            on_hashed(len as u64);
        }
    };
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(MAX_THREADS)
        .min(count);
    let results: Vec<_> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads).map(|_| scope.spawn(worker)).collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|_| Err(anyhow!("hashing thread panicked")))
            })
            .collect()
    });
    let mut hashes = vec![[0u8; 20]; count];
    for result in results {
        for (index, hash) in result? {
            hashes[index] = hash;
        }
    }
    Ok(hashes.concat())
}

/// Reads `len` bytes at `start` of the concatenated files into `buf`.
fn read_range(entries: &[Entry], start: u64, len: usize, buf: &mut Vec<u8>) -> Result<()> {
    buf.clear();
    buf.resize(len, 0);
    let end = start + len as u64;
    // The first file that ends after `start`; empty files are passed over.
    let first = entries.partition_point(|entry| entry.offset + entry.length <= start);
    for entry in &entries[first..] {
        if entry.offset >= end {
            break;
        }
        let from = start.max(entry.offset);
        let to = end.min(entry.offset + entry.length);
        let mut file = File::open(&entry.path)
            .with_context(|| format!("cannot open {}", entry.path.display()))?;
        file.seek(SeekFrom::Start(from - entry.offset))?;
        let slice = &mut buf[(from - start) as usize..(to - start) as usize];
        file.read_exact(slice).with_context(|| {
            format!(
                "cannot read {}; did it change while hashing?",
                entry.path.display()
            )
        })?;
    }
    Ok(())
}

fn files_list(entries: &[Entry]) -> Vec<u8> {
    let files: Vec<Vec<u8>> = entries
        .iter()
        .map(|entry| {
            let mut file = Dict::default();
            file.int("length", entry.length as i64);
            let parts: Vec<Vec<u8>> = entry.parts.iter().map(|part| string(part)).collect();
            file.raw("path", list(&parts));
            file.finish()
        })
        .collect();
    list(&files)
}

/// A bencoded dictionary being built; `finish` writes its keys in sorted
/// order, as bencode requires.
#[derive(Default)]
struct Dict(BTreeMap<Vec<u8>, Vec<u8>>);

impl Dict {
    fn raw(&mut self, key: &str, value: Vec<u8>) {
        self.0.insert(key.as_bytes().to_vec(), value);
    }

    fn int(&mut self, key: &str, value: i64) {
        self.raw(key, format!("i{value}e").into_bytes());
    }

    fn bytes(&mut self, key: &str, value: &[u8]) {
        self.raw(key, byte_string(value));
    }

    fn string(&mut self, key: &str, value: &str) {
        self.bytes(key, value.as_bytes());
    }

    fn finish(self) -> Vec<u8> {
        let mut out = vec![b'd'];
        for (key, value) in self.0 {
            out.extend(byte_string(&key));
            out.extend(value);
        }
        out.push(b'e');
        out
    }
}

fn byte_string(bytes: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", bytes.len()).into_bytes();
    out.extend_from_slice(bytes);
    out
}

fn string(text: &str) -> Vec<u8> {
    byte_string(text.as_bytes())
}

/// A bencoded list of already encoded `items`.
fn list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = vec![b'l'];
    for item in items {
        out.extend_from_slice(item);
    }
    out.push(b'e');
    out
}

#[cfg(test)]
mod tests;
