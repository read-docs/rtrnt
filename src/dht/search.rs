//! Repeats the DHT lookup for one torrent while it downloads, the way the
//! announcer repeats tracker announces.

use super::{Dht, DhtPeers, SwarmEstimate};
use crate::periodic::Periodic;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{info, warn};

/// Time between lookups. Each one announces us again too, well within the
/// 30 minutes nodes keep an announce.
const LOOKUP_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// Shortest time between lookups when we have run out of peers.
const MIN_LOOKUP_INTERVAL: Duration = Duration::from_secs(30);

pub struct DhtSearch {
    dht: Dht,
    info_hash: [u8; 20],
    /// TCP port to announce, if we accept incoming peers.
    announce_port: Option<u16>,
    /// Whether we have the whole torrent: announced as a seed, and no
    /// seeds asked for.
    seeding: bool,
    /// The swarm size the last lookup that scraped it found.
    swarm: Option<SwarmEstimate>,
    /// When lookups start.
    timer: Periodic<DhtPeers>,
}

impl DhtSearch {
    /// Searches the DHT for peers of `info_hash`, announcing `announce_port`
    /// if given. The first lookup starts at the first `poll`.
    pub fn new(dht: Dht, info_hash: [u8; 20], announce_port: Option<u16>) -> Self {
        let now = Instant::now();
        Self {
            dht,
            info_hash,
            announce_port,
            seeding: false,
            swarm: None,
            timer: Periodic::new(now, now),
        }
    }

    /// Tells the search whether we have the whole torrent, which the next
    /// lookup announces (BEP 33).
    pub fn set_seeding(&mut self, seeding: bool) {
        self.seeding = seeding;
    }

    /// The size of the swarm as the nodes closest to the torrent last
    /// reported it, if any did.
    pub fn swarm(&self) -> Option<SwarmEstimate> {
        self.swarm
    }

    /// Brings the next lookup forward, to `MIN_LOOKUP_INTERVAL` after the
    /// last one started, for when we have run out of peers.
    pub fn hurry(&mut self) {
        self.timer.hurry(MIN_LOOKUP_INTERVAL);
    }

    /// Waits until the next lookup is due, runs it in the background and
    /// returns the peers it found.
    ///
    /// Cancel-safe: a lookup already started is kept, and the next call
    /// waits for it.
    pub async fn poll(&mut self) -> Vec<SocketAddr> {
        let (dht, info_hash, port) = (&self.dht, self.info_hash, self.announce_port);
        let seeding = self.seeding;
        let start = || {
            let dht = dht.clone();
            async move { dht.lookup_peers(info_hash, port, seeding).await }
        };
        let found = self
            .timer
            .poll(Some(LOOKUP_INTERVAL), start)
            .await
            .unwrap_or_else(|e| {
                warn!("DHT lookup failed: {e}");
                DhtPeers::default()
            });
        info!(peers = found.peers.len(), swarm = ?found.swarm, "DHT lookup finished");
        // A lookup that reached no node that scrapes keeps the older figure.
        self.swarm = found.swarm.or(self.swarm);
        found.peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn keeps_the_last_swarm_estimate() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dht = Dht::start(socket, Vec::new()).unwrap();
        let mut search = DhtSearch::new(dht, [1; 20], None);
        assert_eq!(search.swarm(), None);
        search.set_seeding(true);
        assert!(search.seeding);
        // No node to ask, so nothing is scraped and nothing is lost.
        let swarm = SwarmEstimate {
            seeds: 2,
            leechers: 5,
        };
        search.swarm = Some(swarm);
        search.poll().await;
        assert_eq!(search.swarm(), Some(swarm));
    }

    #[tokio::test]
    async fn hurry_respects_the_minimum_interval() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dht = Dht::start(socket, Vec::new()).unwrap();
        let mut search = DhtSearch::new(dht, [1; 20], None);
        assert_eq!(search.timer.next, search.timer.last);
        let start = search.timer.last;
        search.timer.next = start + LOOKUP_INTERVAL;
        search.hurry();
        assert_eq!(search.timer.next, start + MIN_LOOKUP_INTERVAL);
        // Never pushes a lookup back.
        search.timer.next = start + Duration::from_secs(1);
        search.hurry();
        assert_eq!(search.timer.next, start + Duration::from_secs(1));
    }
}
