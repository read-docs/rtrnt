use rtrnt::download::download_from_peers;
use rtrnt::peer::{PeerMessage, generate_peer_id};
use rtrnt::torrent::TorrentFile;
use rtrnt::tracker::announcer::Announcer;
use rtrnt::tracker::{AnnounceEvent, AnnounceRequest, ScrapeStats, scrape, scrape_tiers};
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};

use crate::common::*;

/// The tracker hears `started` with everything left, then `completed` and
/// `stopped` reporting the full download.
#[tokio::test]
async fn reports_events_and_progress_to_the_tracker() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let (tracker_url, mut queries) = mock_tracker(vec![seeder_addr]).await;
    let request = AnnounceRequest {
        info_hash,
        peer_id: generate_peer_id(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: total,
        event: AnnounceEvent::Started,
    };
    let (announcer, peers) = Announcer::start(vec![vec![tracker_url]], request)
        .await
        .unwrap();
    assert_eq!(peers, [seeder_addr]);

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &peers,
            10,
            Vec::new(),
            Some(announcer),
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let total = total.to_string();
    let mut announces = Vec::new();
    while let Ok(query) = queries.try_recv() {
        let param = |key| query_param(&query, key).unwrap_or("").to_string();
        announces.push([param("event"), param("downloaded"), param("left")]);
    }
    assert_eq!(
        announces,
        [
            ["started".into(), "0".into(), total.clone()],
            ["completed".into(), total.clone(), "0".into()],
            ["stopped".into(), total, "0".into()],
        ]
    );
}

/// A minimal UDP tracker (BEP 15): answers connects, and answers each scrape
/// with `stats` (seeders, completed, leechers) for every torrent asked
/// about, or with an error saying `error` if one is given.
async fn mock_udp_tracker(stats: [u32; 3], error: Option<&'static str>) -> String {
    const CONNECTION_ID: u64 = 0x0123_4567_89ab_cdef;
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let url = format!("udp://{}/announce", socket.local_addr().unwrap());
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            let (n, from) = socket.recv_from(&mut buf).await.unwrap();
            let request = &buf[..n];
            let action = u32::from_be_bytes(request[8..12].try_into().unwrap());
            let transaction = &request[12..16];
            let mut response = Vec::new();
            match (action, error) {
                (0, _) => {
                    assert_eq!(request[..8], 0x417_2710_1980u64.to_be_bytes());
                    response.extend_from_slice(&0u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    response.extend_from_slice(&CONNECTION_ID.to_be_bytes());
                }
                (2, Some(error)) => {
                    response.extend_from_slice(&3u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    response.extend_from_slice(error.as_bytes());
                }
                (2, None) => {
                    assert_eq!(request[..8], CONNECTION_ID.to_be_bytes());
                    let hashes = &request[16..];
                    assert_eq!(hashes.len() % 20, 0, "partial info hash");
                    response.extend_from_slice(&2u32.to_be_bytes());
                    response.extend_from_slice(transaction);
                    for _ in hashes.chunks(20) {
                        for count in stats {
                            response.extend_from_slice(&count.to_be_bytes());
                        }
                    }
                }
                _ => panic!("unexpected action {action}"),
            }
            socket.send_to(&response, from).await.unwrap();
        }
    });
    url
}

/// An HTTP scrape body listing `info_hash` with 5 seeders, 10 leechers and
/// 50 completed downloads.
fn http_scrape_body(info_hash: [u8; 20]) -> Vec<u8> {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend_from_slice(&info_hash);
    body.extend_from_slice(b"d8:completei5e10:downloadedi50e10:incompletei10eeee");
    body
}

/// A tracker URL on a port nobody listens on.
async fn dead_tracker() -> String {
    let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
    format!("http://{}/announce", dead.local_addr().unwrap())
}

/// Every tracker is asked, and each answer is reported in tier order: HTTP
/// and UDP counts, a UDP error response and an unsupported tracker.
#[tokio::test]
async fn scrapes_http_and_udp_trackers() {
    init_tracing();
    // Not valid UTF-8, as the `files` keys are raw bytes.
    let info_hash = [0xc3; 20];
    let (addr, mut targets) = mock_http(http_scrape_body(info_hash)).await;
    let http = format!("http://{addr}/announce?passkey=x");
    let udp = mock_udp_tracker([7, 70, 3], None).await;
    let refusing = mock_udp_tracker([0; 3], Some("torrent not registered")).await;
    let tiers = vec![
        vec![http.clone()],
        vec![udp.clone(), refusing.clone(), "wss://t/announce".into()],
    ];
    let scrapes = tokio::time::timeout(Duration::from_secs(10), scrape_tiers(&tiers, info_hash))
        .await
        .expect("scrape timed out");

    let summary: Vec<_> = scrapes
        .iter()
        .map(|s| {
            let result = s.result.as_ref().map_err(|e| format!("{e:#}"));
            (s.tier, s.tracker.as_str(), result.cloned())
        })
        .collect();
    let stats = |seeders, leechers, completed| {
        Ok(Some(ScrapeStats {
            seeders,
            leechers,
            completed,
        }))
    };
    assert_eq!(summary[0], (1, http.as_str(), stats(5, 10, 50)));
    assert_eq!(summary[1], (2, udp.as_str(), stats(7, 3, 70)));
    assert_eq!(summary[2].1, refusing);
    let Err(error) = &summary[2].2 else {
        panic!("UDP error response not reported: {summary:?}");
    };
    assert!(error.contains("torrent not registered"), "{error}");
    assert_eq!(summary[3].1, "wss://t/announce");
    assert!(summary[3].2.is_err());
    assert_eq!(summary.len(), 4);

    let target = targets.try_recv().unwrap();
    assert_eq!(
        target,
        format!("/scrape?passkey=x&info_hash={}", "%c3".repeat(20))
    );
}

/// A UDP scrape of more torrents than fit in a packet is split into several
/// requests, and the answers come back in the order asked.
#[tokio::test]
async fn udp_scrape_of_many_torrents() {
    init_tracing();
    let udp = mock_udp_tracker([1, 2, 3], None).await;
    let info_hashes: Vec<[u8; 20]> = (0..100u8).map(|i| [i; 20]).collect();
    let stats = tokio::time::timeout(Duration::from_secs(10), scrape(&udp, &info_hashes))
        .await
        .expect("scrape timed out")
        .unwrap();
    let expected = ScrapeStats {
        seeders: 1,
        leechers: 3,
        completed: 2,
    };
    assert_eq!(stats, vec![Some(expected); 100]);
}

/// `--scrape` prints one line per tracker and fails only when no tracker
/// answers.
#[tokio::test]
async fn scrape_command_reports_each_tracker() {
    init_tracing();
    let (mut torrent, info_hash) = make_torrent(&test_data());
    let (addr, _targets) = mock_http(http_scrape_body(info_hash)).await;
    let http = format!("http://{addr}/announce");
    let dead = dead_tracker().await;
    let dir = tempfile::tempdir().unwrap();

    let run = async |torrent: &TorrentFile| {
        let path = dir.path().join("t.torrent");
        std::fs::write(&path, serde_bencode::to_bytes(torrent).unwrap()).unwrap();
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_rtrnt"))
            .arg("--scrape")
            .arg(&path)
            .output();
        let output = tokio::time::timeout(Duration::from_secs(20), output)
            .await
            .expect("--scrape timed out")
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        (output.status.success(), stdout, stderr)
    };

    torrent.announce_list = Some(vec![vec![http.clone()], vec![dead.clone()]]);
    let (success, stdout, stderr) = run(&torrent).await;
    assert!(success, "{stderr}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "{stdout}");
    assert_eq!(
        lines[0],
        format!("tier 1  {http}  5 seeders, 10 leechers, 50 completed")
    );
    assert!(lines[1].starts_with(&format!("tier 2  {dead}  failed: ")));

    torrent.announce_list = Some(vec![vec![dead]]);
    let (success, _, stderr) = run(&torrent).await;
    assert!(!success);
    assert!(stderr.contains("no tracker answered"), "{stderr}");
}
