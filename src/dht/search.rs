//! Repeats the DHT lookup for one torrent while it downloads, the way the
//! announcer repeats tracker announces.

use super::Dht;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{Instrument, info, warn};

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
    /// When the last lookup started.
    last: Instant,
    /// When the next lookup is due.
    next: Instant,
    in_flight: Option<JoinHandle<Vec<SocketAddr>>>,
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
            last: now,
            next: now,
            in_flight: None,
        }
    }

    /// Brings the next lookup forward, to `MIN_LOOKUP_INTERVAL` after the
    /// last one started, for when we have run out of peers.
    pub fn hurry(&mut self) {
        self.next = self.next.min(self.last + MIN_LOOKUP_INTERVAL);
    }

    /// Waits until the next lookup is due, runs it in the background and
    /// returns the peers it found.
    ///
    /// Cancel-safe: a lookup already started is kept, and the next call
    /// waits for it.
    pub async fn poll(&mut self) -> Vec<SocketAddr> {
        if self.in_flight.is_none() {
            tokio::time::sleep_until(self.next).await;
            self.last = Instant::now();
            self.next = self.last + LOOKUP_INTERVAL;
        }
        let handle = self.in_flight.get_or_insert_with(|| {
            let dht = self.dht.clone();
            let (info_hash, port) = (self.info_hash, self.announce_port);
            let lookup = async move { dht.get_peers(info_hash, port).await };
            tokio::spawn(lookup.in_current_span())
        });
        let peers = handle.await.unwrap_or_else(|e| {
            warn!("DHT lookup failed: {e}");
            Vec::new()
        });
        self.in_flight = None;
        info!(peers = peers.len(), "DHT lookup finished");
        peers
    }
}

impl Drop for DhtSearch {
    fn drop(&mut self) {
        if let Some(handle) = &self.in_flight {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn hurry_respects_the_minimum_interval() {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let dht = Dht::start(socket, Vec::new()).unwrap();
        let mut search = DhtSearch::new(dht, [1; 20], None);
        assert_eq!(search.next, search.last);
        let start = search.last;
        search.next = start + LOOKUP_INTERVAL;
        search.hurry();
        assert_eq!(search.next, start + MIN_LOOKUP_INTERVAL);
        // Never pushes a lookup back.
        search.next = start + Duration::from_secs(1);
        search.hurry();
        assert_eq!(search.next, start + Duration::from_secs(1));
    }
}
