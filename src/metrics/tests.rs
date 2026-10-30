use super::*;
use crate::stats::Layout;
use crate::torrent_limits::TorrentSettings;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

fn torrent(hash: u8, name: &str, state: TorrentState) -> TorrentSnapshot {
    TorrentSnapshot {
        info_hash: [hash; 20],
        name: name.into(),
        state,
        layout: Some(Layout {
            size: 1000,
            piece_length: 100,
            pieces: 10,
            private: false,
            files: vec![(PathBuf::from(name), 1000)],
        }),
        priorities: Vec::new(),
        sequential: false,
        wanted: None,
        save_path: PathBuf::new(),
        verified: 250,
        received: 300,
        downloaded: 250,
        uploaded: 500,
        peers: 7,
        seeds: 2,
        pool: 20,
        active: Duration::ZERO,
        order: 0,
        rate_down: 1500.5,
        rate_up: 20.0,
        eta: Some(Duration::from_secs(90)),
        limits: TorrentSettings::default(),
        effective_limits: TorrentSettings::default(),
        tags: Vec::new(),
    }
}

fn snapshot() -> Snapshot {
    let mut snapshot = Snapshot::default();
    snapshot.session.connections = 9;
    snapshot.session.dht_nodes = Some(312);
    snapshot.session.port = Some(6881);
    snapshot
        .torrents
        .push(torrent(0xab, "ubuntu.iso", TorrentState::Downloading));
    let mut seeding = torrent(0x01, "book", TorrentState::Seeding);
    seeding.verified = 1000;
    seeding.downloaded = 0;
    seeding.uploaded = 40;
    seeding.eta = None;
    seeding.rate_down = 0.0;
    snapshot.torrents.push(seeding);
    snapshot
}

fn parse(line: &str) -> Value {
    serde_json::from_str(line).unwrap()
}

#[test]
fn json_is_one_line_with_the_session_and_each_torrent() {
    let line = json_line(&snapshot(), 1_700_000_000);
    assert!(!line.contains('\n'));
    let value = parse(&line);
    assert_eq!(value["time"], 1_700_000_000u64);
    assert_eq!(value["downloaded"], 250);
    assert_eq!(value["uploaded"], 540);
    assert_eq!(value["connections"], 9);
    assert_eq!(value["dht_nodes"], 312);
    assert_eq!(value["port"], 6881);
    assert_eq!(value["rate_down"], 1500.5);
    let torrents = value["torrents"].as_array().unwrap();
    assert_eq!(torrents.len(), 2);
    let first = &torrents[0];
    assert_eq!(first["name"], "ubuntu.iso");
    assert_eq!(first["info_hash"], "ab".repeat(20));
    assert_eq!(first["state"], "downloading");
    assert_eq!(first["error"], Value::Null);
    assert_eq!(first["progress"], 0.25);
    assert_eq!(first["size"], 1000);
    assert_eq!(first["left"], 750);
    assert_eq!(first["downloaded"], 250);
    assert_eq!(first["uploaded"], 500);
    assert_eq!(first["rate_down"], 1500.5);
    assert_eq!(first["peers"], 7);
    assert_eq!(first["seeds"], 2);
    assert_eq!(first["ratio"], 2.0);
    assert_eq!(first["eta"], 90);
}

#[test]
fn json_has_nulls_for_what_is_not_known() {
    let value = parse(&json_line(&snapshot(), 0));
    let seeding = &value["torrents"][1];
    assert_eq!(seeding["state"], "seeding");
    assert_eq!(seeding["progress"], 1.0);
    assert_eq!(seeding["ratio"], Value::Null);
    assert_eq!(seeding["eta"], Value::Null);

    let mut bare = Snapshot::default();
    let mut magnet = torrent(2, "m", TorrentState::Failed("no peers".into()));
    magnet.layout = None;
    bare.torrents.push(magnet);
    let value = parse(&json_line(&bare, 0));
    assert_eq!(value["dht_nodes"], Value::Null);
    assert_eq!(value["port"], Value::Null);
    let torrent = &value["torrents"][0];
    assert_eq!(torrent["state"], "failed");
    assert_eq!(torrent["error"], "no peers");
    assert_eq!(torrent["size"], Value::Null);
    assert_eq!(torrent["left"], Value::Null);
}

#[test]
fn json_escapes_what_a_name_can_hold() {
    let mut snapshot = Snapshot::default();
    snapshot
        .torrents
        .push(torrent(1, "a \"b\"\n\\c", TorrentState::Paused));
    let line = json_line(&snapshot, 0);
    assert!(!line.contains('\n'));
    assert_eq!(parse(&line)["torrents"][0]["name"], "a \"b\"\n\\c");
}

#[test]
fn label_values_escape_backslash_quote_and_newline() {
    assert_eq!(escape_label("plain é"), "plain é");
    assert_eq!(escape_label(r#"a"b"#), r#"a\"b"#);
    assert_eq!(escape_label("a\\b"), "a\\\\b");
    assert_eq!(escape_label("a\nb"), "a\\nb");
    assert_eq!(escape_label("\\\"\n"), "\\\\\\\"\\n");
}

#[test]
fn prometheus_writes_each_family_once_with_its_type() {
    let text = prometheus(&snapshot());
    let hash = "ab".repeat(20);
    assert!(text.contains(
        "# HELP rtrnt_torrent_downloaded_bytes_total Verified piece data received this session.\n\
         # TYPE rtrnt_torrent_downloaded_bytes_total counter\n"
    ));
    assert!(text.contains(&format!(
        "rtrnt_torrent_downloaded_bytes_total{{info_hash=\"{hash}\",name=\"ubuntu.iso\"}} 250\n"
    )));
    assert!(text.contains(&format!(
        "rtrnt_torrent_uploaded_bytes_total{{info_hash=\"{hash}\",name=\"ubuntu.iso\"}} 500\n"
    )));
    assert!(text.contains(&format!(
        "rtrnt_torrent_download_rate_bytes{{info_hash=\"{hash}\",name=\"ubuntu.iso\"}} 1500.5\n"
    )));
    assert!(text.contains(&format!(
        "rtrnt_torrent_progress{{info_hash=\"{hash}\",name=\"ubuntu.iso\"}} 0.25\n"
    )));
    assert!(text.contains(&format!(
        "rtrnt_torrent_state{{info_hash=\"{hash}\",name=\"ubuntu.iso\",state=\"downloading\"}} 1\n"
    )));
    assert!(text.contains("rtrnt_torrent_peers{info_hash="));
    assert!(text.contains("rtrnt_torrent_seeds{info_hash="));
    assert!(text.contains("# TYPE rtrnt_torrent_state gauge\n"));
    assert!(text.contains("rtrnt_downloaded_bytes_total 250\n"));
    assert!(text.contains("rtrnt_uploaded_bytes_total 540\n"));
    assert!(text.contains("# TYPE rtrnt_uploaded_bytes_total counter\n"));
    assert!(text.contains("rtrnt_torrents 2\n"));
    assert!(text.contains("rtrnt_connections 9\n"));
    assert!(text.contains("rtrnt_dht_nodes 312\n"));
    assert!(text.ends_with('\n'));
    // Every family has exactly one HELP and one TYPE, ahead of its samples.
    let families: Vec<&str> = text
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .map(|line| line.split(' ').next().unwrap())
        .collect();
    let mut unique = families.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(families.len(), unique.len());
    assert_eq!(text.matches("# HELP ").count(), families.len());
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        let name = line.split(['{', ' ']).next().unwrap();
        assert!(families.contains(&name), "{line}");
        assert!(
            line.rsplit(' ').next().unwrap().parse::<f64>().is_ok(),
            "{line}"
        );
    }
}

#[test]
fn prometheus_escapes_label_values() {
    let mut snapshot = Snapshot::default();
    snapshot
        .torrents
        .push(torrent(1, "we\"ird\\name\nx", TorrentState::Paused));
    let text = prometheus(&snapshot);
    assert!(text.contains(r#"name="we\"ird\\name\nx""#), "{text}");
    // The newline in the name did not break a sample over two lines.
    assert!(
        text.lines()
            .all(|line| line.starts_with('#') || line.starts_with("rtrnt_"))
    );
}

#[test]
fn prometheus_leaves_out_what_is_not_known() {
    let mut snapshot = Snapshot::default();
    let mut magnet = torrent(
        1,
        "m",
        TorrentState::FetchingMetadata {
            pieces: 0,
            total: 0,
            peers: 0,
        },
    );
    magnet.layout = None;
    snapshot.torrents.push(magnet);
    let text = prometheus(&snapshot);
    assert!(!text.contains("rtrnt_dht_nodes"));
    assert!(!text.contains("rtrnt_torrent_size_bytes"));
    assert!(text.contains("state=\"metadata\"} 1\n"));
    assert!(prometheus(&Snapshot::default()).contains("rtrnt_torrents 0\n"));
}

#[test]
fn only_get_metrics_is_served() {
    assert_eq!(
        route("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n"),
        Route::Metrics
    );
    assert_eq!(route("GET /metrics?x=1 HTTP/1.0\r\n\r\n"), Route::Metrics);
    assert_eq!(route("GET / HTTP/1.1\r\n\r\n"), Route::NotFound);
    assert_eq!(route("GET /metrics/ HTTP/1.1\r\n\r\n"), Route::NotFound);
    assert_eq!(route("POST /metrics HTTP/1.1\r\n\r\n"), Route::NotFound);
    assert_eq!(route("HEAD /metrics HTTP/1.1\r\n\r\n"), Route::NotFound);
    assert_eq!(route("\r\n\r\n"), Route::BadRequest);
    assert_eq!(route("GET /metrics\r\n\r\n"), Route::BadRequest);
    assert_eq!(route("GET /metrics SPDY/3\r\n\r\n"), Route::BadRequest);
    assert_eq!(route("GET /metrics HTTP/1.1 x\r\n\r\n"), Route::BadRequest);
}

/// Sends `request` to a server on an ephemeral port and returns everything
/// it answers until it closes the connection.
async fn ask(request: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_latest, snapshots) = watch::channel(snapshot());
    let server = tokio::spawn(serve(listener, snapshots));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await.unwrap();
    server.abort();
    reply
}

#[tokio::test]
async fn the_server_answers_get_metrics_on_an_ephemeral_port() {
    let reply = ask("GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n").await;
    let (head, body) = reply.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(head.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n"));
    assert!(head.contains(&format!("Content-Length: {}\r\n", body.len())));
    assert!(head.ends_with("Connection: close"));
    assert_eq!(body, prometheus(&snapshot()));
}

#[tokio::test]
async fn the_server_answers_404_elsewhere_and_400_to_nonsense() {
    let reply = ask("GET /other HTTP/1.1\r\n\r\n").await;
    assert!(reply.starts_with("HTTP/1.1 404 Not Found\r\n"), "{reply}");
    let reply = ask("POST /metrics HTTP/1.1\r\n\r\n").await;
    assert!(reply.starts_with("HTTP/1.1 404 Not Found\r\n"), "{reply}");
    let reply = ask("hello\r\n\r\n").await;
    assert!(reply.starts_with("HTTP/1.1 400 Bad Request\r\n"), "{reply}");
}

#[tokio::test]
async fn the_server_serves_the_latest_snapshot_to_many_clients() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (latest, snapshots) = watch::channel(Snapshot::default());
    let server = tokio::spawn(serve(listener, snapshots));
    for expected in [0, 2] {
        if expected == 2 {
            latest.send_replace(snapshot());
        }
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\n\r\n")
            .await
            .unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).await.unwrap();
        assert!(
            reply.contains(&format!("rtrnt_torrents {expected}\n")),
            "{reply}"
        );
    }
    server.abort();
}

#[tokio::test]
async fn a_head_that_never_ends_is_cut_off() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (_latest, snapshots) = watch::channel(snapshot());
    let server = tokio::spawn(serve(listener, snapshots));
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let junk = "X-Pad: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n".repeat(400);
    // The server may hang up while this is still being written.
    let _ = stream
        .write_all(format!("GET /metrics HTTP/1.1\r\n{junk}").as_bytes())
        .await;
    let mut reply = String::new();
    let _ = stream.read_to_string(&mut reply).await;
    assert!(
        reply.is_empty() || reply.starts_with("HTTP/1.1 431"),
        "{reply}"
    );
    server.abort();
}

#[test]
fn the_ratio_needs_something_downloaded() {
    assert_eq!(ratio(0, 0), None);
    assert_eq!(ratio(0, 10), None);
    assert_eq!(ratio(100, 150), Some(1.5));
}

#[tokio::test]
async fn the_sampler_follows_a_session() {
    let stats = empty_session();
    stats.add_torrent([7; 20], "t", std::path::Path::new("/tmp"));
    let mut sampler = Sampler::new(Arc::clone(&stats));
    let first = sampler.sample();
    assert_eq!(first.torrents.len(), 1);
    assert_eq!(first.torrents[0].name, "t");
    let latest = sample_every(Arc::clone(&stats), Duration::from_millis(10));
    assert_eq!(latest.borrow().torrents.len(), 1);
    stats.add_torrent([8; 20], "u", std::path::Path::new("/tmp"));
    let mut latest = latest;
    tokio::time::timeout(
        Duration::from_secs(5),
        latest.wait_for(|snapshot| snapshot.torrents.len() == 2),
    )
    .await
    .unwrap()
    .unwrap();
}

fn empty_session() -> Arc<SessionStats> {
    use crate::client::ConnectionSlots;
    use crate::rate_limit::RateLimits;
    Arc::new(SessionStats::new(
        Arc::new(RateLimits::default()),
        ConnectionSlots::new(5),
    ))
}

#[tokio::test(start_paused = true)]
async fn json_lines_come_every_period_and_once_more_at_the_end() {
    let stats = empty_session();
    stats.add_torrent([7; 20], "t", std::path::Path::new("/tmp"));
    let mut out = Vec::new();
    let stop = tokio::time::sleep(Duration::from_millis(2500));
    report_json(Sampler::new(stats), Duration::from_secs(1), &mut out, stop)
        .await
        .unwrap();
    let text = String::from_utf8(out).unwrap();
    // At 0, 1 and 2 seconds, and at the end.
    assert_eq!(text.lines().count(), 4, "{text}");
    for line in text.lines() {
        assert_eq!(parse(line)["torrents"][0]["name"], "t");
    }
}

#[tokio::test]
async fn json_reporting_stops_when_the_output_closes() {
    struct Closed;
    impl Write for Closed {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let err = report_json(
        Sampler::new(empty_session()),
        Duration::from_secs(60),
        Closed,
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
}
