//! Fast extension (BEP 6).
//!
//! Peers that both set reserved bit 0x04 in the last handshake byte may send
//! five more messages: Have All and Have None, which stand in for a
//! Bitfield of every or no piece; Reject Request, which answers a request
//! that will not be served; Allowed Fast, naming a piece the sender serves
//! even while it chokes the receiver; and Suggest Piece, a hint. With the
//! extension, every request gets exactly one answer, a Piece or a Reject,
//! and a choke no longer drops the requests outstanding.

use crate::peer::{self, PeerMessage};
use sha1::{Digest, Sha1};
use std::net::IpAddr;

/// Index into the handshake's 8 reserved bytes of the byte holding the fast
/// extension bit.
pub const RESERVED_BYTE: usize = 7;
/// The fast extension bit: the third least significant of the last byte.
pub const RESERVED_BIT: u8 = 0x04;
/// Pieces in the allowed fast set we give a peer (`k` in BEP 6).
pub const ALLOWED_FAST_COUNT: usize = 10;

/// The allowed fast set for the peer at `ip`: up to `k` distinct piece
/// indices out of `num_pieces` (all of them if there are no more than `k`),
/// in the order BEP 6 generates them.
///
/// The set depends only on the peer's network, not its exact address, so a
/// peer gains nothing by reconnecting from a neighbouring address: for IPv4
/// the /24 is hashed, as BEP 6 says. BEP 6 says nothing about IPv6; there
/// we hash the 16 address bytes with all but the /48 zeroed, since a /48 is
/// what one site is usually given. An IPv4-mapped IPv6 address counts as
/// IPv4.
pub fn allowed_fast_set(ip: IpAddr, info_hash: &[u8; 20], num_pieces: u32, k: usize) -> Vec<u32> {
    let k = k.min(usize::try_from(num_pieces).unwrap_or(usize::MAX));
    let mut x = match ip.to_canonical() {
        IpAddr::V4(v4) => {
            let mut octets = v4.octets();
            octets[3] = 0;
            octets.to_vec()
        }
        IpAddr::V6(v6) => {
            let mut octets = v6.octets();
            octets[6..].fill(0);
            octets.to_vec()
        }
    };
    x.extend_from_slice(info_hash);
    let mut set = Vec::with_capacity(k);
    while set.len() < k {
        let digest: [u8; 20] = Sha1::digest(&x).into();
        for &word in digest.as_chunks::<4>().0 {
            if set.len() >= k {
                break;
            }
            let y = u32::from_be_bytes(word);
            let index = y % num_pieces;
            if !set.contains(&index) {
                set.push(index);
            }
        }
        x = digest.to_vec();
    }
    set
}

/// Whether a peer that has `pieces` of the torrent's `num_pieces` pieces is
/// just starting out, and so gets our allowed fast set: it has fewer than
/// `ALLOWED_FAST_COUNT`, and not all of them (a seed of a small torrent
/// needs nothing).
pub fn is_starting(pieces: usize, num_pieces: usize) -> bool {
    pieces < ALLOWED_FAST_COUNT && pieces < num_pieces
}

/// The message that tells a peer which pieces we have, sent right after the
/// handshake: with the fast extension, Have All or Have None when we have
/// every piece or none, and a Bitfield otherwise. Without it a Bitfield, or
/// nothing while we have no piece, which the spec lets us leave unsaid.
pub fn have_message(have: &[bool], fast: bool) -> Option<PeerMessage> {
    if !have.contains(&true) {
        fast.then_some(PeerMessage::HaveNone)
    } else if fast && !have.contains(&false) {
        Some(PeerMessage::HaveAll)
    } else {
        Some(PeerMessage::Bitfield {
            bitfield: peer::encode_bitfield(have),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    /// The test vectors of BEP 6.
    #[test]
    fn allowed_fast_set_matches_the_bep_test_vectors() {
        let ip = IpAddr::from([80, 4, 4, 200]);
        let info_hash = [0xaa; 20];
        assert_eq!(
            allowed_fast_set(ip, &info_hash, 1313, 7),
            [1059, 431, 808, 1217, 287, 376, 1188]
        );
        assert_eq!(
            allowed_fast_set(ip, &info_hash, 1313, 9),
            [1059, 431, 808, 1217, 287, 376, 1188, 353, 508]
        );
    }

    #[test]
    fn allowed_fast_set_depends_on_the_network_only() {
        let info_hash = [0xaa; 20];
        let set = |ip: IpAddr| allowed_fast_set(ip, &info_hash, 1313, ALLOWED_FAST_COUNT);
        let ours = set(IpAddr::from([80, 4, 4, 200]));
        assert_eq!(ours.len(), ALLOWED_FAST_COUNT);
        assert_eq!(set(IpAddr::from([80, 4, 4, 1])), ours);
        assert_ne!(set(IpAddr::from([80, 4, 5, 200])), ours);
        let mapped = IpAddr::V6(Ipv4Addr::new(80, 4, 4, 200).to_ipv6_mapped());
        assert_eq!(set(mapped), ours);

        let v6 = |s: &str| set(IpAddr::V6(s.parse::<Ipv6Addr>().unwrap()));
        let site = v6("2001:db8:1::1");
        assert_eq!(site.len(), ALLOWED_FAST_COUNT);
        assert_eq!(v6("2001:db8:1:ffff::abcd"), site);
        assert_ne!(v6("2001:db8:2::1"), site);
    }

    #[test]
    fn small_torrents_allow_every_piece() {
        let ip = IpAddr::from([127, 0, 0, 1]);
        let mut set = allowed_fast_set(ip, &[1; 20], 3, ALLOWED_FAST_COUNT);
        set.sort();
        assert_eq!(set, [0, 1, 2]);
        assert_eq!(allowed_fast_set(ip, &[1; 20], 1, 10), [0]);
        assert!(allowed_fast_set(ip, &[1; 20], 0, 10).is_empty());
    }

    #[test]
    fn starting_peers_have_few_pieces() {
        assert!(is_starting(0, 1000));
        assert!(is_starting(ALLOWED_FAST_COUNT - 1, 1000));
        assert!(!is_starting(ALLOWED_FAST_COUNT, 1000));
        assert!(is_starting(2, 3));
        assert!(!is_starting(3, 3));
    }

    #[test]
    fn have_all_and_have_none_replace_the_bitfield_with_the_fast_extension() {
        let some = [true, false, true];
        let bitfield = Some(PeerMessage::Bitfield {
            bitfield: vec![0b1010_0000],
        });
        assert_eq!(have_message(&some, true), bitfield);
        assert_eq!(have_message(&some, false), bitfield);
        assert_eq!(have_message(&[true; 3], true), Some(PeerMessage::HaveAll));
        assert_eq!(
            have_message(&[true; 3], false),
            Some(PeerMessage::Bitfield {
                bitfield: vec![0b1110_0000]
            })
        );
        assert_eq!(have_message(&[false; 3], true), Some(PeerMessage::HaveNone));
        assert_eq!(have_message(&[false; 3], false), None);
    }
}
