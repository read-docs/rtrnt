//! Peers announced to us with `announce_peer`, handed out to `get_peers`
//! queries for the same info hash, or summed up as bloom filters for a
//! scrape (BEP 33). Bounded in torrents, peers per torrent and time.

use super::bloom::Bloom;
use rand::seq::IndexedRandom;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddrV4};
use std::time::Duration;
use tokio::time::Instant;

/// How long an announce lasts. Clients re-announce every 15 to 30 minutes.
pub const PEER_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_TORRENTS: usize = 1000;
const MAX_PEERS_PER_TORRENT: usize = 200;
/// Peers in one `get_peers` answer, so that it fits a typical MTU.
pub const MAX_VALUES: usize = 50;

/// A peer that announced.
#[derive(Clone, Copy)]
struct Entry {
    addr: SocketAddrV4,
    /// When it last announced.
    at: Instant,
    /// Whether it said it has the whole torrent.
    seed: bool,
}

#[derive(Default)]
pub struct PeerStore {
    torrents: HashMap<[u8; 20], Vec<Entry>>,
}

impl PeerStore {
    /// Records that `peer` has the torrent, all of it if `seed`. A host keeps one entry per
    /// torrent, so that it cannot fill the list with ports; a new port
    /// replaces its old one. When full, the peer that
    /// announced longest ago gives way; when tracking too many torrents, a
    /// new one is ignored.
    pub fn announce(&mut self, info_hash: [u8; 20], peer: SocketAddrV4, seed: bool, now: Instant) {
        if !self.torrents.contains_key(&info_hash) && self.torrents.len() >= MAX_TORRENTS {
            self.expire(now);
            if self.torrents.len() >= MAX_TORRENTS {
                return;
            }
        }
        let peers = self.torrents.entry(info_hash).or_default();
        let entry = Entry {
            addr: peer,
            at: now,
            seed,
        };
        if let Some(known) = peers.iter_mut().find(|e| e.addr.ip() == peer.ip()) {
            *known = entry;
            return;
        }
        if peers.len() >= MAX_PEERS_PER_TORRENT
            && let Some(oldest) = (0..peers.len()).min_by_key(|&i| peers[i].at)
        {
            peers.swap_remove(oldest);
        }
        peers.push(entry);
    }

    /// The peers of the torrent that have not expired, or `None` if it has
    /// none.
    fn live(&mut self, info_hash: &[u8; 20], now: Instant) -> Option<&[Entry]> {
        let peers = self.torrents.get_mut(info_hash)?;
        peers.retain(|e| now.saturating_duration_since(e.at) < PEER_TTL);
        if peers.is_empty() {
            self.torrents.remove(info_hash);
            return None;
        }
        self.torrents.get(info_hash).map(Vec::as_slice)
    }

    /// Up to `MAX_VALUES` peers of the torrent, picked at random. With
    /// `noseed`, only those that are not seeds.
    pub fn get(&mut self, info_hash: &[u8; 20], noseed: bool, now: Instant) -> Vec<SocketAddrV4> {
        let Some(peers) = self.live(info_hash, now) else {
            return Vec::new();
        };
        let wanted: Vec<&Entry> = peers.iter().filter(|e| !(noseed && e.seed)).collect();
        let picked = wanted.choose_multiple(&mut rand::rng(), MAX_VALUES);
        picked.map(|e| e.addr).collect()
    }

    /// Filters of the IP addresses of the torrent's seeds and of its other
    /// peers, if it has any peers.
    pub fn scrape(&mut self, info_hash: &[u8; 20], now: Instant) -> Option<(Bloom, Bloom)> {
        let peers = self.live(info_hash, now)?;
        let (mut seeds, mut others) = (Bloom::default(), Bloom::default());
        for entry in peers {
            let filter = if entry.seed { &mut seeds } else { &mut others };
            filter.insert(IpAddr::V4(*entry.addr.ip()));
        }
        Some((seeds, others))
    }

    fn expire(&mut self, now: Instant) {
        self.torrents.retain(|_, peers| {
            peers.retain(|e| now.saturating_duration_since(e.at) < PEER_TTL);
            !peers.is_empty()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn peer(n: u32) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::from(n), 6881)
    }

    #[test]
    fn hands_out_announced_peers_until_they_expire() {
        let now = Instant::now();
        let mut store = PeerStore::default();
        store.announce([1; 20], peer(1), false, now);
        store.announce([1; 20], peer(2), false, now);
        store.announce([1; 20], peer(1), false, now);
        let mut peers = store.get(&[1; 20], false, now);
        peers.sort();
        assert_eq!(peers, [peer(1), peer(2)]);
        assert!(store.get(&[2; 20], false, now).is_empty());

        // Peer 2 announces again later, so it outlives peer 1.
        let later = now + PEER_TTL / 2;
        store.announce([1; 20], peer(2), false, later);
        assert_eq!(store.get(&[1; 20], false, now + PEER_TTL), [peer(2)]);
        assert!(store.get(&[1; 20], false, later + PEER_TTL).is_empty());
        assert!(store.torrents.is_empty());
    }

    #[test]
    fn one_host_has_one_entry_per_torrent() {
        let now = Instant::now();
        let mut store = PeerStore::default();
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        for port in 1..=MAX_PEERS_PER_TORRENT as u16 {
            store.announce([1; 20], SocketAddrV4::new(ip, port), false, now);
        }
        // Another host is unaffected.
        store.announce([1; 20], peer(7), false, now);
        let mut peers = store.get(&[1; 20], false, now);
        peers.sort();
        // The newest port stands.
        assert_eq!(
            peers,
            [peer(7), SocketAddrV4::new(ip, MAX_PEERS_PER_TORRENT as u16)]
        );
    }

    #[test]
    fn is_bounded() {
        let start = Instant::now();
        let mut store = PeerStore::default();
        for n in 0..MAX_PEERS_PER_TORRENT as u32 + 10 {
            store.announce(
                [1; 20],
                peer(n),
                false,
                start + Duration::from_secs(n.into()),
            );
        }
        let peers = &store.torrents[&[1; 20]];
        assert_eq!(peers.len(), MAX_PEERS_PER_TORRENT);
        // The oldest announces gave way.
        assert!(!peers.iter().any(|e| e.addr == peer(9)));
        assert!(peers.iter().any(|e| e.addr == peer(10)));
        assert_eq!(store.get(&[1; 20], false, start).len(), MAX_VALUES);

        for n in 0..MAX_TORRENTS as u32 + 5 {
            let mut info_hash = [0; 20];
            info_hash[..4].copy_from_slice(&n.to_be_bytes());
            store.announce(info_hash, peer(1), false, start);
        }
        assert_eq!(store.torrents.len(), MAX_TORRENTS);
        // Once the old announces expire, there is room again.
        let later = start + PEER_TTL + Duration::from_secs(1000);
        store.announce([9; 20], peer(1), false, later);
        assert_eq!(store.torrents.len(), 1);
    }

    #[test]
    fn seeds_are_told_apart() {
        let now = Instant::now();
        let mut store = PeerStore::default();
        store.announce([1; 20], peer(1), true, now);
        store.announce([1; 20], peer(2), false, now);
        store.announce([1; 20], peer(3), false, now);
        let mut all = store.get(&[1; 20], false, now);
        all.sort();
        assert_eq!(all, [peer(1), peer(2), peer(3)]);
        let mut leechers = store.get(&[1; 20], true, now);
        leechers.sort();
        assert_eq!(leechers, [peer(2), peer(3)]);

        let (seeds, others) = store.scrape(&[1; 20], now).unwrap();
        assert_eq!((seeds.count(), others.count()), (1, 2));
        assert!(seeds.contains(IpAddr::V4(*peer(1).ip())));
        assert!(!others.contains(IpAddr::V4(*peer(1).ip())));
        assert!(store.scrape(&[2; 20], now).is_none());

        // A peer that finishes announces again as a seed, and moves over.
        store.announce([1; 20], peer(2), true, now);
        let (seeds, others) = store.scrape(&[1; 20], now).unwrap();
        assert_eq!((seeds.count(), others.count()), (2, 1));
        // Nothing is left to scrape once the announces expire.
        assert!(store.scrape(&[1; 20], now + PEER_TTL).is_none());
    }
}
