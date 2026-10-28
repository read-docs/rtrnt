//! Peers announced to us with `announce_peer`, handed out to `get_peers`
//! queries for the same info hash. Bounded in torrents, peers per torrent
//! and time.

use rand::seq::IndexedRandom;
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::time::Duration;
use tokio::time::Instant;

/// How long an announce lasts. Clients re-announce every 15 to 30 minutes.
pub const PEER_TTL: Duration = Duration::from_secs(30 * 60);
const MAX_TORRENTS: usize = 1000;
const MAX_PEERS_PER_TORRENT: usize = 200;
/// Peers in one `get_peers` answer, so that it fits a typical MTU.
pub const MAX_VALUES: usize = 50;

#[derive(Default)]
pub struct PeerStore {
    /// Peers of each info hash, with when they last announced.
    torrents: HashMap<[u8; 20], Vec<(SocketAddrV4, Instant)>>,
}

impl PeerStore {
    /// Records that `peer` has the torrent. A host keeps one entry per
    /// torrent, so that it cannot fill the list with ports; a new port
    /// replaces its old one. When full, the peer that
    /// announced longest ago gives way; when tracking too many torrents, a
    /// new one is ignored.
    pub fn announce(&mut self, info_hash: [u8; 20], peer: SocketAddrV4, now: Instant) {
        if !self.torrents.contains_key(&info_hash) && self.torrents.len() >= MAX_TORRENTS {
            self.expire(now);
            if self.torrents.len() >= MAX_TORRENTS {
                return;
            }
        }
        let peers = self.torrents.entry(info_hash).or_default();
        if let Some(known) = peers.iter_mut().find(|(addr, _)| addr.ip() == peer.ip()) {
            *known = (peer, now);
            return;
        }
        if peers.len() >= MAX_PEERS_PER_TORRENT
            && let Some(oldest) = (0..peers.len()).min_by_key(|&i| peers[i].1)
        {
            peers.swap_remove(oldest);
        }
        peers.push((peer, now));
    }

    /// Up to `MAX_VALUES` peers of the torrent, picked at random.
    pub fn get(&mut self, info_hash: &[u8; 20], now: Instant) -> Vec<SocketAddrV4> {
        let Some(peers) = self.torrents.get_mut(info_hash) else {
            return Vec::new();
        };
        peers.retain(|(_, at)| now.saturating_duration_since(*at) < PEER_TTL);
        if peers.is_empty() {
            self.torrents.remove(info_hash);
            return Vec::new();
        }
        let picked = peers.choose_multiple(&mut rand::rng(), MAX_VALUES);
        picked.map(|(addr, _)| *addr).collect()
    }

    fn expire(&mut self, now: Instant) {
        self.torrents.retain(|_, peers| {
            peers.retain(|(_, at)| now.saturating_duration_since(*at) < PEER_TTL);
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
        store.announce([1; 20], peer(1), now);
        store.announce([1; 20], peer(2), now);
        store.announce([1; 20], peer(1), now);
        let mut peers = store.get(&[1; 20], now);
        peers.sort();
        assert_eq!(peers, [peer(1), peer(2)]);
        assert!(store.get(&[2; 20], now).is_empty());

        // Peer 2 announces again later, so it outlives peer 1.
        let later = now + PEER_TTL / 2;
        store.announce([1; 20], peer(2), later);
        assert_eq!(store.get(&[1; 20], now + PEER_TTL), [peer(2)]);
        assert!(store.get(&[1; 20], later + PEER_TTL).is_empty());
        assert!(store.torrents.is_empty());
    }

    #[test]
    fn one_host_has_one_entry_per_torrent() {
        let now = Instant::now();
        let mut store = PeerStore::default();
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        for port in 1..=MAX_PEERS_PER_TORRENT as u16 {
            store.announce([1; 20], SocketAddrV4::new(ip, port), now);
        }
        // Another host is unaffected.
        store.announce([1; 20], peer(7), now);
        let mut peers = store.get(&[1; 20], now);
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
            store.announce([1; 20], peer(n), start + Duration::from_secs(n.into()));
        }
        let peers = &store.torrents[&[1; 20]];
        assert_eq!(peers.len(), MAX_PEERS_PER_TORRENT);
        // The oldest announces gave way.
        assert!(!peers.iter().any(|(addr, _)| *addr == peer(9)));
        assert!(peers.iter().any(|(addr, _)| *addr == peer(10)));
        assert_eq!(store.get(&[1; 20], start).len(), MAX_VALUES);

        for n in 0..MAX_TORRENTS as u32 + 5 {
            let mut info_hash = [0; 20];
            info_hash[..4].copy_from_slice(&n.to_be_bytes());
            store.announce(info_hash, peer(1), start);
        }
        assert_eq!(store.torrents.len(), MAX_TORRENTS);
        // Once the old announces expire, there is room again.
        let later = start + PEER_TTL + Duration::from_secs(1000);
        store.announce([9; 20], peer(1), later);
        assert_eq!(store.torrents.len(), 1);
    }
}
