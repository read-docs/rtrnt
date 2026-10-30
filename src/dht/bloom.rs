//! The bloom filters of the DHT scrape (BEP 33): sets of IP addresses in
//! 256 bytes, which can be merged by OR and counted approximately.
//!
//! Nodes that store peers for a torrent send one filter of its seeds
//! (`BFsd`) and one of the other peers (`BFpe`) in answer to a `get_peers`
//! query with `scrape` set. The filters of the closest nodes, merged, give
//! the size of the swarm without fetching a single peer.

use sha1::{Digest, Sha1};
use std::fmt;
use std::net::IpAddr;

/// Size of a filter on the wire.
pub const BYTES: usize = 256;
/// Bits in a filter. BEP 33 fixes this and two hash functions (`K`).
const M: usize = BYTES * 8;
const K: f64 = 2.0;

/// A set of IP addresses. Only addresses go in, never ports or names. Boxed
/// so that a response holding two stays small.
#[derive(Clone, PartialEq, Eq)]
pub struct Bloom(Box<[u8; BYTES]>);

impl Default for Bloom {
    fn default() -> Self {
        Self(Box::new([0; BYTES]))
    }
}

impl fmt::Debug for Bloom {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bloom(~{})", self.estimate())
    }
}

impl Bloom {
    /// The filter a node sent, if it has the right size.
    pub fn from_bytes(bytes: &[u8]) -> Option<Bloom> {
        let bytes: [u8; BYTES] = bytes.try_into().ok()?;
        Some(Bloom(Box::new(bytes)))
    }

    pub fn as_bytes(&self) -> &[u8; BYTES] {
        &self.0
    }

    /// The two bit positions of `ip`.
    fn bits(ip: IpAddr) -> [usize; 2] {
        let hash = match ip.to_canonical() {
            IpAddr::V4(ip) => Sha1::digest(ip.octets()),
            IpAddr::V6(ip) => Sha1::digest(ip.octets()),
        };
        let index = |a: u8, b: u8| (usize::from(a) | usize::from(b) << 8) % M;
        [index(hash[0], hash[1]), index(hash[2], hash[3])]
    }

    pub fn insert(&mut self, ip: IpAddr) {
        for bit in Self::bits(ip) {
            self.0[bit / 8] |= 1 << (bit % 8);
        }
    }

    /// Whether `ip` may be in the set: always if it is, rarely if not.
    pub fn contains(&self, ip: IpAddr) -> bool {
        Self::bits(ip)
            .iter()
            .all(|&bit| self.0[bit / 8] & (1 << (bit % 8)) != 0)
    }

    /// Adds everything in `other`, so that the estimate counts the union.
    pub fn merge(&mut self, other: &Bloom) {
        self.0.iter_mut().zip(&other.0).for_each(|(a, b)| *a |= b);
    }

    /// Approximately how many addresses were inserted (BEP 33). It breaks
    /// down around 8000.
    pub fn estimate(&self) -> f64 {
        let zeros: usize = self.0.iter().map(|b| b.count_zeros() as usize).sum();
        if zeros == M {
            return 0.0;
        }
        // BEP 33 clamps at the top only, which makes a full filter infinite.
        let zeros = zeros.clamp(1, M - 1) as f64;
        let m = M as f64;
        (zeros / m).ln() / (K * (1.0 - 1.0 / m).ln())
    }

    /// The estimate as a whole number.
    pub fn count(&self) -> u32 {
        self.estimate().round() as u32
    }
}

#[cfg(test)]
mod tests;
