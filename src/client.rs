//! What the torrents of one session share: the settings, our peer id, the
//! listening port, the DHT node, the rate limits and the progress bars.

use crate::dht::Dht;
use crate::download::DownloadConfig;
use crate::listener::Listener;
use crate::peer;
use crate::rate_limit::RateLimits;
use anyhow::{Result, bail};
use indicatif::MultiProgress;
use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::OnceCell;
use tracing::{Instrument, Span, info, info_span, warn};

/// Runs any number of torrents at once, each through `download_file` or
/// `download_magnet`, sharing one listening port, one DHT node and one pair
/// of rate limits. The port and the DHT node are only opened once a torrent
/// needs them.
pub struct Client {
    config: DownloadConfig,
    peer_id: [u8; 20],
    rate_limits: Arc<RateLimits>,
    bars: MultiProgress,
    listener: OnceCell<Arc<Listener>>,
    /// `None` if the DHT is off or could not be started.
    dht: OnceCell<Option<Dht>>,
    /// Info hashes of the torrents running, so none runs twice.
    torrents: Mutex<HashSet<[u8; 20]>>,
}

impl Client {
    /// A client with the settings in `config`, drawing its progress bars in
    /// `bars`.
    pub fn new(config: DownloadConfig, bars: MultiProgress) -> Self {
        if config.max_download_rate > 0 || config.max_upload_rate > 0 {
            info!(
                download = config.max_download_rate,
                upload = config.max_upload_rate,
                "rate limits in KiB/s (0: unlimited)"
            );
        }
        let rate_limits = RateLimits::from_kib(config.max_download_rate, config.max_upload_rate);
        Self {
            config,
            peer_id: peer::generate_peer_id(),
            rate_limits: Arc::new(rate_limits),
            bars,
            listener: OnceCell::new(),
            dht: OnceCell::new(),
            torrents: Mutex::new(HashSet::new()),
        }
    }

    pub fn config(&self) -> &DownloadConfig {
        &self.config
    }

    /// Our peer id, the same for every torrent.
    pub fn peer_id(&self) -> [u8; 20] {
        self.peer_id
    }

    /// How fast block data may move each way, across every peer of every
    /// torrent.
    pub fn rate_limits(&self) -> Arc<RateLimits> {
        Arc::clone(&self.rate_limits)
    }

    pub fn bars(&self) -> &MultiProgress {
        &self.bars
    }

    /// Where peers connect to us, listening on the configured port (see
    /// `Listener::bind`) from the first call on.
    pub async fn listener(&self) -> &Arc<Listener> {
        let bind = Listener::bind(self.config.listen_port);
        self.listener
            .get_or_init(|| bind.instrument(session_span()))
            .await
    }

    /// Our DHT node, started on the first call (trying the listening port
    /// for UDP too), unless the DHT is off. A failure to start it is logged
    /// once, and the session goes on without it.
    pub async fn dht(&self) -> Option<Dht> {
        if !self.config.dht {
            return None;
        }
        let port = self.config.listen_port;
        let start = async move {
            Dht::bind(port)
                .await
                .inspect_err(|e| warn!("cannot start the DHT: {e:#}"))
                .ok()
        };
        self.dht
            .get_or_init(|| start.instrument(session_span()))
            .await
            .clone()
    }

    /// Marks the torrent with `info_hash` as running until the returned
    /// claim is dropped. Fails if it is running already.
    pub fn claim(&self, info_hash: [u8; 20]) -> Result<Claim<'_>> {
        if !self.lock_torrents().insert(info_hash) {
            bail!("this torrent is already being downloaded in this session");
        }
        Ok(Claim {
            client: self,
            info_hash,
        })
    }

    fn lock_torrents(&self) -> MutexGuard<'_, HashSet<[u8; 20]>> {
        self.torrents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A span for work done for every torrent, such as opening the listening
/// port, so its logs do not go under the torrent that happened to need it
/// first. Outside any torrent's span it is no span at all.
fn session_span() -> Span {
    if Span::current().is_none() {
        Span::none()
    } else {
        info_span!(parent: None, "session")
    }
}

/// A torrent running in a `Client`.
pub struct Claim<'a> {
    client: &'a Client,
    info_hash: [u8; 20],
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        self.client.lock_torrents().remove(&self.info_hash);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::download::SeedLimits;

    fn client(dht: bool) -> Client {
        let config = DownloadConfig {
            output_dir: ".".into(),
            max_peers: 10,
            listen_port: 0,
            seed: SeedLimits::default(),
            dht,
            max_download_rate: 64,
            max_upload_rate: 0,
        };
        Client::new(config, MultiProgress::new())
    }

    #[test]
    fn a_torrent_runs_once_at_a_time() {
        let client = client(false);
        let claim = client.claim([1; 20]).unwrap();
        let _other = client.claim([2; 20]).unwrap();
        assert!(client.claim([1; 20]).is_err());
        drop(claim);
        assert!(client.claim([1; 20]).is_ok());
    }

    #[tokio::test]
    async fn torrents_share_one_listener_and_one_rate_limit() {
        let client = client(false);
        let first = client.listener().await;
        assert!(Arc::ptr_eq(first, client.listener().await));
        assert!(first.port().is_some());
        assert!(Arc::ptr_eq(&client.rate_limits(), &client.rate_limits()));
        assert!(client.rate_limits().download.is_limited());
        assert!(!client.rate_limits().upload.is_limited());
    }

    #[tokio::test]
    async fn no_dht_when_it_is_off() {
        assert!(client(false).dht().await.is_none());
    }
}
