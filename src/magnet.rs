//! Magnet links (BEP 9): downloading a torrent known only by its info hash.
//!
//! A magnet link names the torrent by its info hash, and may add a display
//! name (`dn`), trackers (`tr`) and peers (`x.pe`). Peers are found through
//! those trackers and the DHT, the info dictionary is fetched from them over
//! `ut_metadata` (see `metadata`), and the download then carries on as for a
//! `.torrent` file, over the connections already open.
//!
//! The fetched info dictionary is saved as `<info hash in hex>.torrent` in
//! the output directory, so a later run with the same link, `--check` among
//! them, uses that instead of asking peers again.

use crate::client::Client;
use crate::dht::DhtSearch;
use crate::download::peer_pool::PeerPool;
use crate::download::{self, CheckedOnly, Limits, Session, StartPeers};
use crate::manager::{Recheck, StartOptions};
use crate::peer::extension::Advertised;
use crate::resume::{self, Target};
use crate::selection;
use crate::stats::{PeerSource, TorrentState, TorrentStats};
use crate::storage::Storage;
use crate::torrent;
use crate::tracker::announcer::{Announcer, Transferred};
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use anyhow::{Context, Result, anyhow, bail};
use metadata::MetadataFetch;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

pub mod metadata;

/// `left` told to trackers while the torrent's size is unknown; libtorrent
/// sends the same. Anything but 0, which would make us look like a seed.
const UNKNOWN_LEFT: u64 = 16 * 1024;

/// What a magnet link says about a torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    /// `xt=urn:btih:`, given as 40 hex digits or 32 base32 characters.
    pub info_hash: [u8; 20],
    /// `dn`: a name to show until the metadata gives the real one.
    pub name: Option<String>,
    /// `tr`: tracker URLs, in order, each listed once.
    pub trackers: Vec<String>,
    /// `x.pe`: addresses of peers to fetch the metadata from directly. Only
    /// IP literals; host names are skipped.
    pub peers: Vec<SocketAddr>,
}

/// Whether `arg` looks like a magnet link rather than a file path.
pub fn is_magnet(arg: &str) -> bool {
    arg.get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("magnet:"))
}

impl MagnetLink {
    /// Parses a magnet link. Parameter values are percent-decoded. It must
    /// have exactly one BitTorrent v1 info hash (`xt=urn:btih:`), though it
    /// may repeat it; other `xt` values and unknown parameters are ignored.
    pub fn parse(uri: &str) -> Result<Self> {
        if !is_magnet(uri) {
            bail!("not a magnet link: {uri:?}");
        }
        let query = uri[7..]
            .strip_prefix('?')
            .ok_or_else(|| anyhow!("magnet link has no parameters"))?;
        let mut info_hash = None;
        let mut v2_only = false;
        let mut link = MagnetLink {
            info_hash: [0; 20],
            name: None,
            trackers: Vec::new(),
            peers: Vec::new(),
        };
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match &*key {
                "xt" => {
                    if let Some(hash) = strip_prefix_ignore_case(&value, "urn:btih:") {
                        let hash = parse_info_hash(hash)?;
                        if info_hash.is_some_and(|known| known != hash) {
                            bail!("magnet link has two different info hashes");
                        }
                        info_hash = Some(hash);
                    } else if strip_prefix_ignore_case(&value, "urn:btmh:").is_some() {
                        v2_only = true;
                    }
                }
                "dn" if !value.is_empty() => link.name = Some(value.into_owned()),
                "tr" if !value.is_empty() => {
                    if !link.trackers.iter().any(|known| *known == value) {
                        link.trackers.push(value.into_owned());
                    }
                }
                "x.pe" => {
                    if let Ok(addr) = value.parse::<SocketAddr>()
                        && !link.peers.contains(&addr)
                    {
                        link.peers.push(addr);
                    }
                }
                _ => {}
            }
        }
        link.info_hash = match info_hash {
            Some(hash) => hash,
            None if v2_only => {
                bail!(
                    "magnet link has only a BitTorrent v2 info hash (urn:btmh), which is not supported"
                )
            }
            None => bail!("magnet link has no info hash (xt=urn:btih:...)"),
        };
        Ok(link)
    }

    /// The link as a URI that `parse` reads back to this link.
    pub fn to_uri(&self) -> String {
        let mut query = url::form_urlencoded::Serializer::new(String::new());
        query.append_pair("xt", &format!("urn:btih:{}", hex(&self.info_hash)));
        if let Some(name) = &self.name {
            query.append_pair("dn", name);
        }
        for tracker in &self.trackers {
            query.append_pair("tr", tracker);
        }
        for peer in &self.peers {
            query.append_pair("x.pe", &peer.to_string());
        }
        format!("magnet:?{}", query.finish())
    }

    /// The trackers in tiers (BEP 12): each in a tier of its own, in the
    /// order listed, as libtorrent does.
    pub fn tracker_tiers(&self) -> Vec<Vec<String>> {
        self.trackers.iter().map(|url| vec![url.clone()]).collect()
    }

    /// Where the metadata fetched for this link is saved, as a `.torrent`
    /// file in `output_dir`.
    pub fn saved_torrent_path(&self, output_dir: &Path) -> PathBuf {
        output_dir.join(format!("{}.torrent", hex(&self.info_hash)))
    }
}

/// `value` without `prefix`, compared ignoring ASCII case.
fn strip_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &value[prefix.len()..])
}

/// Parses an info hash written as 40 hex digits or 32 base32 characters
/// (RFC 4648, as older magnet links use), in either case.
fn parse_info_hash(text: &str) -> Result<[u8; 20]> {
    let parsed = match text.len() {
        40 => parse_hex(text),
        32 => parse_base32(text),
        _ => None,
    };
    parsed.ok_or_else(|| {
        anyhow!("info hash {text:?} is neither 40 hex digits nor 32 base32 characters")
    })
}

fn parse_hex(text: &str) -> Option<[u8; 20]> {
    let mut hash = [0u8; 20];
    let digits = text.as_bytes();
    if digits.len() != 40 {
        return None;
    }
    for (byte, pair) in hash.iter_mut().zip(digits.chunks(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        if !pair.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(hash)
}

fn parse_base32(text: &str) -> Option<[u8; 20]> {
    let mut hash = [0u8; 20];
    let mut filled = 0;
    // Bits not yet written out, in the low `bits` bits of `acc`.
    let (mut acc, mut bits) = (0u32, 0);
    for c in text.bytes() {
        let value = match c.to_ascii_uppercase() {
            c @ b'A'..=b'Z' => c - b'A',
            c @ b'2'..=b'7' => c - b'2' + 26,
            _ => return None,
        };
        acc = (acc << 5) | u32::from(value);
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            *hash.get_mut(filled)? = (acc >> bits) as u8;
            filled += 1;
            acc &= (1 << bits) - 1;
        }
    }
    (filled == 20 && bits == 0).then_some(hash)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The metadata fetch was stopped by the shutdown signal. Nothing was
/// downloaded yet.
#[derive(Debug)]
pub struct FetchInterrupted;

impl std::fmt::Display for FetchInterrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("interrupted while fetching the torrent's metadata")
    }
}

impl std::error::Error for FetchInterrupted {}

/// The `.torrent` file saved for `link` in `output_dir` by an earlier run,
/// if there is one and it is the right torrent.
pub async fn saved_torrent(link: &MagnetLink, output_dir: &Path) -> Option<PathBuf> {
    let path = link.saved_torrent_path(output_dir);
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return None;
    }
    match torrent::load_file(&path).await {
        Ok((_, info_hash)) if info_hash == link.info_hash => Some(path),
        Ok(_) => {
            warn!(path = %path.display(), "saved torrent is for another info hash, ignoring it");
            None
        }
        Err(e) => {
            warn!(path = %path.display(), "cannot read saved torrent, ignoring it: {e:#}");
            None
        }
    }
}

/// Downloads the torrent of a magnet link, like `download::download_file`
/// for a `.torrent` file.
///
/// If an earlier run saved the metadata in the output directory, that is
/// used. Otherwise peers are looked for through the link's trackers and
/// peers and the DHT (unless disabled), the metadata is fetched from them
/// (see `MetadataFetch`) and saved, and the download carries on over the
/// connections already open. Trackers hear a `left` of 16 KiB until the
/// size is known.
///
/// As in `download_file`, the torrent shares the client's listening port,
/// DHT node, rate limits, connection slots and turn to check the data on disk
/// with the client's other torrents, and it fails, once the metadata has
/// arrived, if one of them writes a file it would. `shutdown` is first polled while the
/// metadata is fetched, or, with metadata saved by an earlier run, as in
/// `download_file`.
pub async fn download_magnet(
    link: &MagnetLink,
    options: &StartOptions,
    client: &Client,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let config = client.config();
    if let Some(path) = saved_torrent(link, &config.output_dir).await {
        info!(path = %path.display(), "using metadata saved by an earlier run");
        let metainfo = torrent::load_metainfo(&path).await?;
        return download::download_metainfo(metainfo, options, client, shutdown).await;
    }
    let name = link.name.clone().unwrap_or_else(|| hex(&link.info_hash));
    let stats = client
        .stats()
        .add_torrent(link.info_hash, &name, &config.output_dir);
    stats.set_state(TorrentState::FetchingMetadata {
        pieces: 0,
        total: 0,
        peers: 0,
    });
    let result = fetch_and_download(link, options, client, &stats, shutdown).await;
    stats.finish(&result);
    result
}

/// `download_magnet` without metadata saved by an earlier run, publishing
/// its progress in `stats`.
async fn fetch_and_download(
    link: &MagnetLink,
    options: &StartOptions,
    client: &Client,
    stats: &Arc<TorrentStats>,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let config = client.config();
    let info_hash = link.info_hash;
    info!(
        name = link.name.as_deref().unwrap_or("unknown"),
        info_hash = %hex(&info_hash),
        trackers = link.trackers.len(),
        "fetching metadata for magnet link"
    );
    let mut claim = client.claim(info_hash)?;
    let peer_id = client.peer_id();
    let dht = client.dht().await;
    let mut incoming = client.listener().await.register(info_hash)?;
    let listen_port = incoming.port();
    let tiers = link.tracker_tiers();
    let mut pool = PeerPool::default();
    pool.add(link.peers.iter().copied());
    let mut announcer = None;
    if !tiers.is_empty() {
        let request = AnnounceRequest {
            info_hash,
            peer_id,
            port: listen_port.unwrap_or(config.listen_port),
            uploaded: 0,
            downloaded: 0,
            left: UNKNOWN_LEFT,
            event: AnnounceEvent::Started,
        };
        let started = Announcer::start_reporting(tiers.clone(), request, Some(Arc::clone(stats)));
        let (started, peers) = started.await;
        stats.add_source(PeerSource::Tracker, peers.len());
        pool.add(peers);
        announcer = Some(started);
    }
    // A tracker that did not answer is tried again, but not worth waiting
    // for with nowhere else to look.
    if !announcer.as_ref().is_some_and(Announcer::has_answered)
        && dht.is_none()
        && link.peers.is_empty()
    {
        bail!("no tracker answered, the magnet link names no peers, and the DHT is off");
    }

    let mut shutdown = std::pin::pin!(shutdown);
    let fetch = MetadataFetch {
        info_hash,
        peer_id,
        max_peers: config.max_peers,
        slots: client.slots(),
        incoming: &mut incoming,
        announcer: &mut announcer,
        dht: dht
            .clone()
            .map(|dht| DhtSearch::new(dht, info_hash, listen_port)),
        stats: Arc::clone(stats),
    };
    let fetched = match fetch.run(pool, &mut shutdown).await {
        Ok(fetched) => fetched,
        Err(e) => {
            if let Some(announcer) = announcer {
                announcer.stop(&Transferred::default()).await;
            }
            return Err(e);
        }
    };

    let start = async {
        let torrent = torrent::from_info(&fetched.metadata, &tiers)?;
        info!(
            name = %torrent.info.name,
            size = torrent.length(),
            pieces = torrent.num_pieces(),
            "fetched metadata"
        );
        stats.set_torrent(&torrent);
        claim.claim_files(
            &torrent.info.name,
            &Storage::paths(&torrent.info, &config.output_dir)?,
        )?;
        save_torrent(link, &config.output_dir, &tiers, &fetched.metadata).await;
        let resume = Target::new(config, info_hash);
        let selection = download::initial_selection(
            options,
            resume.as_ref(),
            selection::file_count(&torrent.info),
        );
        let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir, &selection)?);
        // BEP 27: a private torrent's peers come from its trackers alone.
        // Magnet links for private torrents are not a thing in practice,
        // but should one come, we stop using the DHT once we know.
        let dht = match dht {
            Some(_) if torrent.is_private() => {
                info!("private torrent: no longer using the DHT");
                None
            }
            dht => dht,
        };
        let session = Arc::new(Session {
            torrent,
            info_hash,
            peer_id,
            storage,
            seed: config.seed,
            dht,
            rate_limits: client.rate_limits(),
            metadata: Some(fetched.metadata),
            bars: client.bars().clone(),
            resume,
            stats: Arc::clone(stats),
        });
        let recheck = options.recheck != Recheck::No;
        let have =
            resume::initial_pieces(&session, recheck, client.hashing(), shutdown.as_mut()).await?;
        if options.recheck == Recheck::ThenPause {
            bail!(CheckedOnly);
        }
        let left = session.left(&have)?;
        anyhow::Ok((session, have, left))
    };
    let (session, have, left) = match start.await {
        Ok(started) => started,
        Err(e) => {
            if let Some(announcer) = announcer {
                announcer.stop(&Transferred::default()).await;
            }
            return Err(e);
        }
    };
    if let Some(announcer) = &mut announcer {
        announcer.set_left(left);
    }
    if left == 0 && !config.seed.enabled() {
        info!(output = %config.output_dir.display(), "already complete");
        if let Some(announcer) = announcer {
            announcer.stop(&Transferred::default()).await;
        }
        return Ok(());
    }

    // The peers we fetched the metadata from carry on into the download.
    let num_pieces = session.torrent.num_pieces();
    let advertised = Advertised {
        listen_port,
        metadata_size: session
            .metadata
            .as_ref()
            .and_then(|metadata| u32::try_from(metadata.len()).ok()),
        private: session.torrent.is_private(),
    };
    let mut connections = Vec::new();
    for (mut conn, inbound, permit) in fetched.connections {
        let addr = conn.addr;
        if let Err(e) = conn.set_piece_count(num_pieces) {
            debug!(%addr, "dropping peer: {e:#}");
            continue;
        }
        // Now that we have the metadata, peers may fetch it from us too.
        if let Err(e) = conn.send_extension_handshake(advertised).await {
            debug!(%addr, "dropping peer: {e:#}");
            continue;
        }
        connections.push((conn, inbound, permit));
    }
    let start = StartPeers {
        pool: fetched.pool,
        banned_ips: fetched.banned_ips,
        connections,
    };
    download::download_with(
        session,
        &have,
        start,
        Limits {
            max_peers: config.max_peers,
            slots: client.slots(),
        },
        incoming,
        announcer,
        shutdown,
    )
    .await?;
    info!(output = %config.output_dir.display(), "done");
    Ok(())
}

/// Saves fetched metadata as a `.torrent` file, so the next run with the
/// same magnet link need not fetch it again. A failure is only logged.
async fn save_torrent(link: &MagnetLink, output_dir: &Path, tiers: &[Vec<String>], info: &[u8]) {
    let path = link.saved_torrent_path(output_dir);
    let save = async {
        let data = torrent::encode_with_info(tiers, info)?;
        tokio::fs::create_dir_all(output_dir).await?;
        // Written whole and then renamed, so a crash leaves no torn file.
        let partial = path.with_extension("torrent.part");
        tokio::fs::write(&partial, data).await?;
        tokio::fs::rename(&partial, &path).await?;
        anyhow::Ok(())
    };
    match save
        .await
        .with_context(|| format!("cannot save {}", path.display()))
    {
        Ok(()) => info!(path = %path.display(), "saved metadata"),
        Err(e) => warn!("{e:#}"),
    }
}

#[cfg(test)]
mod tests;
