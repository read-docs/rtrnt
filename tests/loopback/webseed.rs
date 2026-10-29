use rtrnt::download::download_from_peers;
use rtrnt::torrent_limits::{TorrentLimits, TorrentSettings};
use std::collections::HashMap;
use std::future::pending;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::common::*;

/// What the mock web server was asked for: path and range of each request.
type Requests = Arc<Mutex<Vec<(String, String)>>>;

const NOT_FOUND: &[u8] =
    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
const BAD_RANGE: &[u8] =
    b"HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// An HTTP server that serves the files of `make_torrent(data)` under
/// `/files/multi/dir/`, honoring `Range`, and answers 404 to the rest.
async fn web_server(data: Arc<Vec<u8>>) -> (SocketAddr, Requests) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Requests::default();
    let log = Arc::clone(&requests);
    let mut files = HashMap::new();
    let mut offset = 0;
    for (name, len) in FILES {
        files.insert(
            format!("/files/multi/dir/{name}"),
            data[offset..offset + len].to_vec(),
        );
        offset += len;
    }
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (files, log) = (files.clone(), Arc::clone(&log));
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.ends_with(b"\r\n\r\n") {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..n]);
                }
                let request = String::from_utf8(request).unwrap();
                let path = request.split(' ').nth(1).unwrap().to_string();
                let range = request
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("range")
                            .then(|| value.trim().to_string())
                    })
                    .unwrap_or_default();
                log.lock().unwrap().push((path.clone(), range.clone()));
                let reply = match (files.get(&path), parse_range(&range)) {
                    (Some(file), Some((first, last))) if first <= last && last < file.len() => {
                        let body = &file[first..=last];
                        let mut reply = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {first}-{last}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            file.len(),
                            body.len()
                        )
                        .into_bytes();
                        reply.extend_from_slice(body);
                        reply
                    }
                    (Some(_), _) => BAD_RANGE.to_vec(),
                    (None, _) => NOT_FOUND.to_vec(),
                };
                let _ = socket.write_all(&reply).await;
            });
        }
    });
    (addr, requests)
}

fn parse_range(range: &str) -> Option<(usize, usize)> {
    let (first, last) = range.strip_prefix("bytes=")?.split_once('-')?;
    Some((first.parse().ok()?, last.parse().ok()?))
}

/// A multi-file torrent with no peers and no trackers completes from its web
/// seed alone, with one range request for each file a piece touches.
#[tokio::test]
async fn downloads_a_multi_file_torrent_from_a_web_seed_alone() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let (addr, requests) = web_server(Arc::clone(&data)).await;
    torrent.url_list = vec![format!("http://{addr}/files")];

    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());
    let stats = Arc::clone(&session.stats);
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &[],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    assert_files_match(out.path(), &data);
    assert_eq!(stats.verified(), data.len() as u64);
    assert_eq!(stats.received(), data.len() as u64);
    // Piece 0 is inside a.bin, piece 1 spans a.bin and b.bin (the empty
    // file between them gets no request), piece 2 is inside b.bin.
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 4, "{requests:?}");
    assert!(
        requests
            .iter()
            .all(|(_, range)| range.starts_with("bytes="))
    );
}

/// A web seed that cannot be reached does not hold up one that works.
#[tokio::test]
async fn a_dead_web_seed_does_not_hold_up_a_working_one() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let (good, _) = web_server(Arc::clone(&data)).await;
    let dead: SocketAddr = {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    torrent.url_list = vec![
        format!("http://{dead}/files"),
        format!("http://{good}/files/"),
    ];

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    assert_files_match(out.path(), &data);
}

/// A torrent's own download limit holds back its web seeds as it does its
/// peers: the download takes at least as long as the limit allows.
#[tokio::test]
async fn a_torrents_own_download_limit_slows_its_web_seeds() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let (addr, _) = web_server(Arc::clone(&data)).await;
    torrent.url_list = vec![format!("http://{addr}/files")];

    let out = tempfile::tempdir().unwrap();
    let mut session = Arc::into_inner(session(torrent, info_hash, out.path())).unwrap();
    const RATE_KIB: u64 = 32;
    session.torrent_limits = Arc::new(TorrentLimits::new(TorrentSettings {
        download: RATE_KIB,
        ..TorrentSettings::default()
    }));
    let start = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            Arc::new(session),
            &vec![false; num_pieces],
            &[],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    let elapsed = start.elapsed();
    assert_files_match(out.path(), &data);

    // Two lanes may each have a first piece through at once; the rest waits
    // for the limit, less a fifth so a slow machine cannot make it flaky.
    let total = data.len() as u64;
    let waited = total.saturating_sub(2 * PIECE_LENGTH);
    let shortest = Duration::from_secs_f64(waited as f64 / (RATE_KIB * 1024) as f64 * 0.8);
    assert!(
        elapsed >= shortest,
        "{total} bytes in {elapsed:?} at {RATE_KIB} KiB/s"
    );
}
