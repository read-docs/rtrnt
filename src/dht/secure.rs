//! Secure node ids (BEP 42): a node id tied to the IP address of its owner,
//! so that an attacker cannot pick the ids it sits at in the DHT.
//!
//! An IPv4 id starts with the first 21 bits of `crc32c((ip & 0x030f3fff) |
//! (r << 29))`, where `r` is the low 3 bits of the id's last byte. We learn
//! our own address from the `ip` field that nodes put in their answers, and
//! only believe it once several nodes agree: see [`IpVotes`].

use super::routing::NodeId;
use std::net::Ipv4Addr;

/// Distinct nodes that must report the same address before we trust it.
const MIN_VOTES: usize = 4;
/// Reports remembered; the oldest gives way to a new reporter.
const MAX_VOTES: usize = 32;
/// Bits of `ip` that go into the hash, for IPv4.
const V4_MASK: u32 = 0x030f_3fff;

/// CRC-32C (Castagnoli), reflected, as BEP 42 uses.
fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut bit = 0;
            while bit < 8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0x82F6_3B78
                } else {
                    crc >> 1
                };
                bit += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };
    !data.iter().fold(!0u32, |crc, &byte| {
        TABLE[usize::from((crc as u8) ^ byte)] ^ (crc >> 8)
    })
}

/// Addresses on the local network, whose nodes are exempt from the check
/// since they cannot know their public address (BEP 42).
fn is_local(ip: Ipv4Addr) -> bool {
    ip.is_private() || ip.is_loopback() || ip.is_link_local()
}

/// Whether `ip` can be a public address of ours: not local, shared (carrier
/// grade NAT, 100.64.0.0/10), unspecified, broadcast or multicast.
pub fn is_public(ip: Ipv4Addr) -> bool {
    let shared = ip.octets()[0] == 100 && ip.octets()[1] & 0xc0 == 64;
    !(is_local(ip) || shared || ip.is_unspecified() || ip.is_broadcast() || ip.is_multicast())
}

/// The hash that fixes the first 21 bits of an id for `ip` and `r` (0..8).
fn hash_v4(ip: Ipv4Addr, r: u8) -> u32 {
    let masked = (u32::from(ip) & V4_MASK) | (u32::from(r & 7) << 29);
    crc32c(&masked.to_be_bytes())
}

impl NodeId {
    /// An id for a node at `ip`: the 21 bits and last byte that BEP 42
    /// fixes (`rand` is the last byte, of which the low 3 bits go into the
    /// hash), and the rest from `filler`, which should be random.
    pub fn secure_v4_with(ip: Ipv4Addr, rand: u8, filler: [u8; 20]) -> NodeId {
        let hash = hash_v4(ip, rand);
        let mut id = filler;
        id[0] = (hash >> 24) as u8;
        id[1] = (hash >> 16) as u8;
        id[2] = ((hash >> 8) as u8 & 0xf8) | (id[2] & 0x07);
        id[19] = rand;
        NodeId(id)
    }

    /// A random id that is valid for a node at `ip`.
    pub fn secure_v4(ip: Ipv4Addr) -> NodeId {
        Self::secure_v4_with(ip, rand::random(), rand::random())
    }

    /// Whether the id is valid for a node at `ip`. Nodes on the local
    /// network always are.
    pub fn is_secure_v4(&self, ip: Ipv4Addr) -> bool {
        if is_local(ip) {
            return true;
        }
        let hash = hash_v4(ip, self.0[19]);
        self.0[0] == (hash >> 24) as u8
            && self.0[1] == (hash >> 16) as u8
            && self.0[2] & 0xf8 == (hash >> 8) as u8 & 0xf8
    }
}

/// What nodes say our external address is. One report counts per reporting
/// node, so that a single liar cannot decide it.
#[derive(Default)]
pub struct IpVotes {
    /// Reporter and the address it reported, oldest first.
    votes: Vec<(Ipv4Addr, Ipv4Addr)>,
}

impl IpVotes {
    /// Records that `reporter` says our address is `reported`. Returns the
    /// address once at least `MIN_VOTES` nodes agree on it and they are
    /// the majority of all reports. Addresses that cannot be public are
    /// never accepted, as they say nothing about our public one.
    pub fn vote(&mut self, reporter: Ipv4Addr, reported: Ipv4Addr) -> Option<Ipv4Addr> {
        if !is_public(reported) {
            return None;
        }
        self.votes.retain(|&(who, _)| who != reporter);
        if self.votes.len() >= MAX_VOTES {
            self.votes.remove(0);
        }
        self.votes.push((reporter, reported));
        let agreeing = self.votes.iter().filter(|&&(_, ip)| ip == reported).count();
        (agreeing >= MIN_VOTES && agreeing * 2 > self.votes.len()).then_some(reported)
    }
}

#[cfg(test)]
mod tests;
