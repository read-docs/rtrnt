//! An IP blocklist: ranges of addresses we refuse to talk to.
//!
//! A list file holds one entry per line, in any mix of these formats:
//!
//! - PeerGuardian text (P2P): `description:1.2.3.4-1.2.3.255`;
//! - eMule `ipfilter.dat`: `1.2.3.4 - 1.2.3.255 , 000 , description`, where
//!   an access level of 128 or more means the range is allowed, so it is
//!   left out;
//! - a CIDR block (`1.2.3.0/24`, `2001:db8::/32`), a single address, or a
//!   bare range (`1.2.3.4-1.2.3.255`).
//!
//! Both IPv4 and IPv6 are understood. Blank lines and lines starting with
//! `#`, `;` or `//` are skipped; a line that fits no format is skipped and
//! counted, and does not fail the load. Gzipped lists are not read.
//!
//! The ranges are kept sorted and merged, so a lookup is a binary search.

use anyhow::{Context, Result, bail};
use std::net::IpAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};

/// Inclusive ranges of addresses, sorted, with none touching another.
type Ranges<T> = Vec<(T, T)>;

/// Ranges of addresses to refuse, and how many peers were refused.
#[derive(Debug, Default)]
pub struct Blocklist {
    v4: Ranges<u32>,
    v6: Ranges<u128>,
    blocked: AtomicU64,
}

impl Blocklist {
    /// Reads the list file at `path`, logging how many ranges it holds and
    /// how many lines were skipped as malformed.
    pub fn load(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)
            .with_context(|| format!("cannot read blocklist {}", path.display()))?;
        if bytes.starts_with(&[0x1f, 0x8b]) {
            bail!(
                "blocklist {} is gzip-compressed; decompress it first",
                path.display()
            );
        }
        // Descriptions are often not UTF-8, and are not used anyway.
        let (list, malformed) = Self::parse(&String::from_utf8_lossy(&bytes));
        info!(path = %path.display(), ranges = list.len(), "loaded blocklist");
        if malformed > 0 {
            warn!(
                path = %path.display(),
                malformed,
                "skipped malformed lines in blocklist"
            );
        }
        Ok(list)
    }

    /// The list `text` holds, and how many of its lines were malformed.
    pub fn parse(text: &str) -> (Self, usize) {
        let (mut v4, mut v6) = (Vec::new(), Vec::new());
        let mut malformed = 0;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || ["#", ";", "//"].iter().any(|c| line.starts_with(c)) {
                continue;
            }
            match parse_line(line) {
                Some(Entry::Block(Range::V4(start, end))) => v4.push((start, end)),
                Some(Entry::Block(Range::V6(start, end))) => v6.push((start, end)),
                Some(Entry::Allow) => {}
                None => malformed += 1,
            }
        }
        let list = Self {
            v4: merge(v4),
            v6: merge(v6),
            blocked: AtomicU64::new(0),
        };
        (list, malformed)
    }

    /// How many ranges the list holds, after overlapping ones are merged.
    pub fn len(&self) -> usize {
        self.v4.len() + self.v6.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `ip` is in a blocked range. An IPv4 address in IPv6
    /// clothing (`::ffff:1.2.3.4`) counts as the IPv4 address.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match ip.to_canonical() {
            IpAddr::V4(ip) => lookup(&self.v4, ip.into()),
            IpAddr::V6(ip) => lookup(&self.v6, ip.into()),
        }
    }

    /// Whether to refuse the peer at `ip`: `contains`, which also counts
    /// the peer if so.
    pub fn refuses(&self, ip: IpAddr) -> bool {
        let refused = self.contains(ip);
        if refused {
            self.blocked.fetch_add(1, Ordering::Relaxed);
            debug!(%ip, "refusing blocklisted peer");
        }
        refused
    }

    /// How many peers `refuses` has turned away.
    pub fn blocked(&self) -> u64 {
        self.blocked.load(Ordering::Relaxed)
    }
}

fn lookup<T: Ord + Copy>(ranges: &[(T, T)], ip: T) -> bool {
    let after = ranges.partition_point(|&(start, _)| start <= ip);
    after > 0 && ranges[after - 1].1 >= ip
}

/// Sorts `ranges` and joins those that overlap or touch.
fn merge<T: Ord + Copy + Successor>(mut ranges: Ranges<T>) -> Ranges<T> {
    ranges.sort_unstable();
    let mut merged: Ranges<T> = Vec::with_capacity(ranges.len());
    for (start, end) in ranges {
        match merged.last_mut() {
            Some(last) if last.1.next().is_none_or(|next| start <= next) => {
                last.1 = last.1.max(end);
            }
            _ => merged.push((start, end)),
        }
    }
    merged
}

/// An integer that has a next value, unless it is the largest.
trait Successor: Sized {
    fn next(self) -> Option<Self>;
}

impl Successor for u32 {
    fn next(self) -> Option<Self> {
        self.checked_add(1)
    }
}

impl Successor for u128 {
    fn next(self) -> Option<Self> {
        self.checked_add(1)
    }
}

#[derive(Debug, PartialEq)]
enum Range {
    V4(u32, u32),
    V6(u128, u128),
}

#[derive(Debug, PartialEq)]
enum Entry {
    Block(Range),
    /// A range an eMule list marks as allowed.
    Allow,
}

/// An eMule access level at or below this blocks the range.
const MAX_BLOCKING_LEVEL: u32 = 127;

fn parse_line(line: &str) -> Option<Entry> {
    // eMule: the range comes first, then the level.
    if let Some((range, rest)) = line.split_once(',')
        && let Some(range) = parse_range(range)
    {
        let level = rest
            .split(',')
            .next()
            .and_then(|level| level.trim().parse::<u32>().ok());
        return Some(match level {
            Some(level) if level > MAX_BLOCKING_LEVEL => Entry::Allow,
            _ => Entry::Block(range),
        });
    }
    if let Some(range) = parse_cidr(line).or_else(|| parse_range(line)) {
        return Some(Entry::Block(range));
    }
    // PeerGuardian: the description may hold colons, and so may the range
    // (IPv6), so take the longest tail that is a range.
    line.match_indices(':')
        .find_map(|(i, _)| parse_range(&line[i + 1..]))
        .map(Entry::Block)
}

/// `start-end`, with both ends of one family and `start` no greater.
fn parse_range(text: &str) -> Option<Range> {
    let (start, end) = text.split_once('-')?;
    match (start.trim().parse().ok()?, end.trim().parse().ok()?) {
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            let (a, b) = (u32::from(a), u32::from(b));
            (a <= b).then_some(Range::V4(a, b))
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            let (a, b) = (u128::from(a), u128::from(b));
            (a <= b).then_some(Range::V6(a, b))
        }
        _ => None,
    }
}

/// `ip/length`, or a lone `ip`.
fn parse_cidr(text: &str) -> Option<Range> {
    let (ip, len) = match text.split_once('/') {
        Some((ip, len)) => (
            ip.trim().parse().ok()?,
            Some(len.trim().parse::<u32>().ok()?),
        ),
        None => (text.trim().parse().ok()?, None),
    };
    match ip {
        IpAddr::V4(ip) => {
            let len = len.unwrap_or(32);
            let mask = u32::MAX.checked_shl(32u32.checked_sub(len)?).unwrap_or(0);
            let start = u32::from(ip) & mask;
            Some(Range::V4(start, start | !mask))
        }
        IpAddr::V6(ip) => {
            let len = len.unwrap_or(128);
            let mask = u128::MAX.checked_shl(128u32.checked_sub(len)?).unwrap_or(0);
            let start = u128::from(ip) & mask;
            Some(Range::V6(start, start | !mask))
        }
    }
}

#[cfg(test)]
mod tests;
