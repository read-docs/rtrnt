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

use crate::announcer::{Announcer, Transferred};
use crate::client::Client;
use crate::dht::DhtSearch;
use crate::download::{self, Limits, Session, StartPeers};
use crate::extension::Advertised;
use crate::metadata::MetadataFetch;
use crate::peer_pool::PeerPool;
use crate::resume::{self, Target};
use crate::storage::Storage;
use crate::torrent;
use crate::tracker::{AnnounceEvent, AnnounceRequest};
use anyhow::{Context, Result, anyhow, bail};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, warn};

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
/// with the client's other torrents. `shutdown` is first polled while the
/// metadata is fetched, or, with metadata saved by an earlier run, as in
/// `download_file`.
pub async fn download_magnet(
    link: &MagnetLink,
    client: &Client,
    shutdown: impl Future<Output = ()>,
) -> Result<()> {
    let config = client.config();
    if let Some(path) = saved_torrent(link, &config.output_dir).await {
        info!(path = %path.display(), "using metadata saved by an earlier run");
        return download::download_file(&path, client, shutdown).await;
    }
    let info_hash = link.info_hash;
    info!(
        name = link.name.as_deref().unwrap_or("unknown"),
        info_hash = %hex(&info_hash),
        trackers = link.trackers.len(),
        "fetching metadata for magnet link"
    );
    let _claim = client.claim(info_hash)?;
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
        match Announcer::start(tiers.clone(), request).await {
            Ok((started, peers)) => {
                info!(peers = peers.len(), "tracker announce succeeded");
                pool.add(peers);
                announcer = Some(started);
            }
            Err(e) => info!("{e:#}"),
        }
    }
    if announcer.is_none() && dht.is_none() && link.peers.is_empty() {
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
        save_torrent(link, &config.output_dir, &tiers, &fetched.metadata).await;
        let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir)?);
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
            resume: Target::new(config, info_hash),
        });
        let have =
            resume::initial_pieces(&session, config, client.hashing(), shutdown.as_mut()).await?;
        let left = session.torrent.length() - download::verified_bytes(&session.torrent, &have)?;
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
mod tests {
    use super::*;

    const HASH: [u8; 20] = [
        0xc1, 0x2f, 0xe1, 0xc0, 0x6b, 0xba, 0x25, 0x4a, 0x9d, 0xc9, 0xf5, 0x19, 0xb3, 0x35, 0xaa,
        0x7c, 0x13, 0x67, 0xa8, 0x8a,
    ];
    const HEX: &str = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
    const BASE32: &str = "YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK";

    #[test]
    fn parses_hex_and_base32_info_hashes_in_any_case() {
        for hash in [
            HEX.to_string(),
            HEX.to_uppercase(),
            BASE32.to_string(),
            BASE32.to_lowercase(),
        ] {
            let link = MagnetLink::parse(&format!("magnet:?xt=urn:btih:{hash}")).unwrap();
            assert_eq!(link.info_hash, HASH, "{hash}");
            assert_eq!(link.name, None);
            assert!(link.trackers.is_empty());
        }
        let link = MagnetLink::parse(&format!("MAGNET:?XT=URN:BTIH:{HEX}"));
        assert!(link.is_err(), "parameter names are case-sensitive");
        let link = MagnetLink::parse(&format!("Magnet:?xt=URN:BTIH:{HEX}")).unwrap();
        assert_eq!(link.info_hash, HASH);
    }

    #[test]
    fn parses_name_trackers_and_peers() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX}&dn=Some+File%20%C3%A9.iso\
             &tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce\
             &tr=http://t.example/announce?a=1%26b%3D2\
             &tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce&tr=\
             &x.pe=10.0.0.1:6881&x.pe=%5B2001:db8::1%5D:51413&x.pe=host.example:1\
             &xt=urn:btmh:1220abcd&ws=http://seed.example/&unknown"
        );
        let link = MagnetLink::parse(&uri).unwrap();
        assert_eq!(
            link,
            MagnetLink {
                info_hash: HASH,
                name: Some("Some File é.iso".into()),
                trackers: vec![
                    "udp://tracker.example:1337/announce".into(),
                    "http://t.example/announce?a=1&b=2".into(),
                ],
                peers: vec![
                    "10.0.0.1:6881".parse().unwrap(),
                    "[2001:db8::1]:51413".parse().unwrap(),
                ],
            }
        );
        assert_eq!(
            link.tracker_tiers(),
            [
                ["udp://tracker.example:1337/announce"],
                ["http://t.example/announce?a=1&b=2"]
            ]
        );
        assert_eq!(
            link.saved_torrent_path(Path::new("/out")),
            Path::new("/out").join(format!("{HEX}.torrent"))
        );
        // The same hash twice, in different forms, is fine.
        let uri = format!("magnet:?xt=urn:btih:{HEX}&xt=urn:btih:{BASE32}");
        assert_eq!(MagnetLink::parse(&uri).unwrap().info_hash, HASH);
    }

    #[test]
    fn rejects_malformed_links_with_a_reason() {
        let cases = [
            ("magnet.torrent", "not a magnet link"),
            ("magnet:", "no parameters"),
            ("magnet:xt=urn:btih:", "no parameters"),
            ("magnet:?dn=x&tr=udp://t", "no info hash"),
            ("magnet:?xt=urn:sha1:abc", "no info hash"),
            ("magnet:?xt=urn:btmh:1220abcd", "v2"),
            ("magnet:?xt=urn:btih:", "neither 40 hex digits"),
            (
                "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88",
                "neither 40 hex digits",
            ),
            (
                "magnet:?xt=urn:btih:g12fe1c06bba254a9dc9f519b335aa7c1367a88a",
                "neither 40 hex digits",
            ),
            (
                "magnet:?xt=urn:btih:+12fe1c06bba254a9dc9f519b335aa7c1367a88a",
                "neither 40 hex digits",
            ),
            (
                "magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKE1",
                "neither 40 hex digits",
            ),
            (
                "magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a\
                 &xt=urn:btih:0000000000000000000000000000000000000000",
                "two different info hashes",
            ),
        ];
        for (uri, reason) in cases {
            let err = MagnetLink::parse(uri).unwrap_err().to_string();
            assert!(err.contains(reason), "{uri}: {err}");
        }
    }

    #[test]
    fn decodes_base32() {
        assert_eq!(parse_base32(BASE32), Some(HASH));
        assert_eq!(parse_base32(&"A".repeat(32)), Some([0; 20]));
        assert_eq!(parse_base32(&"7".repeat(32)), Some([0xff; 20]));
        assert_eq!(parse_base32(&"A".repeat(31)), None);
        assert_eq!(parse_base32(&"A".repeat(33)), None);
        assert_eq!(parse_base32(&"A=".repeat(16)), None);
        assert_eq!(parse_hex(HEX), Some(HASH));
        assert_eq!(parse_hex(&"é".repeat(20)), None);
    }

    #[test]
    fn tells_magnet_links_from_paths() {
        assert!(is_magnet("magnet:?xt=urn:btih:x"));
        assert!(is_magnet("MAGNET:"));
        assert!(!is_magnet("magnet"));
        assert!(!is_magnet("./magnet:x"));
        assert!(!is_magnet("file.torrent"));
        assert!(!is_magnet("mägnet:"));
    }
}
