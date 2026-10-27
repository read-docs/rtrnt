use indicatif::MultiProgress;
use rtrnt::download::{DownloadConfig, Interrupted, SeedLimits, Session, download_file};
use rtrnt::peer::{PeerMessage, generate_peer_id};
use rtrnt::resume;
use rtrnt::stats::TorrentStats;
use rtrnt::storage::Storage;
use std::future::pending;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, oneshot};

use crate::common::*;

/// Two torrents in one client share one listening port: both trackers hear
/// the same port, and the seeder of each, connecting to it, is handed to
/// the torrent it asks for. The trackers list only a dead peer, so every
/// byte comes over the shared port.
#[tokio::test]
async fn two_torrents_share_one_listening_port() {
    init_tracing();
    let out = tempfile::tempdir().unwrap();
    let dead_addr: SocketAddr = {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        dead.local_addr().unwrap()
    };
    let mut paths = Vec::new();
    let mut seeders = Vec::new();
    let mut datas = Vec::new();
    for (name, salt) in [("first", 0u8), ("second", 7)] {
        let data: Arc<Vec<u8>> = Arc::new(test_data().iter().map(|b| b ^ salt).collect());
        let (mut torrent, info_hash) = make_named_torrent(&data, name);
        let num_pieces = torrent.num_pieces();
        let (tracker_url, mut queries) = mock_tracker(vec![dead_addr]).await;
        torrent.announce = Some(tracker_url);
        let path = out.path().join(format!("{name}.torrent"));
        std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
        paths.push(path);

        let seeder_data = Arc::clone(&data);
        seeders.push(tokio::spawn(async move {
            let started = queries.recv().await.unwrap();
            let port: u16 = query_param(&started, "port").unwrap().parse().unwrap();
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            let mut socket = connect_to_leecher(addr, info_hash, num_pieces).await;
            send(&mut socket, PeerMessage::Unchoke).await;
            serve(socket, &seeder_data).await;
            port
        }));
        datas.push((name, data));
    }

    let client = client(DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
        recheck: false,
    });
    let (first, second) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            download_file(&paths[0], &client, pending()),
            download_file(&paths[1], &client, pending()),
        )
    })
    .await
    .expect("downloads timed out");
    first.expect("first download failed");
    second.expect("second download failed");
    for (name, data) in &datas {
        assert_named_files_match(out.path(), name, data);
    }

    let mut ports = Vec::new();
    for seeder in seeders {
        ports.push(seeder.await.unwrap());
    }
    assert_eq!(ports[0], ports[1], "the torrents announced different ports");
    assert_eq!(Some(ports[0]), client.listener().await.port());
}

/// The same torrent twice in one client: the second is refused while the
/// first runs.
#[tokio::test]
async fn a_client_runs_a_torrent_once() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let (tracker_url, _queries) = mock_tracker(Vec::new()).await;
    torrent.announce = Some(tracker_url);
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);
    let path = out.path().join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let client = client(DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        // Seeds until stopped, so it is still running when the second
        // starts.
        seed: SeedLimits {
            ratio: 0.0,
            time: Duration::from_secs(3600),
        },
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
        recheck: false,
    });
    let (stop, stop_rx) = oneshot::channel::<()>();
    let first = download_file(&path, &client, async {
        stop_rx.await.ok();
    });
    let second = async {
        // Wait until the first has claimed the torrent.
        while client.claim(info_hash).is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let err = download_file(&path, &client, pending()).await.unwrap_err();
        stop.send(()).unwrap();
        err
    };
    let (first, err) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(first, second)
    })
    .await
    .expect("timed out");
    first.expect("first run failed");
    assert!(
        err.to_string().contains("already being downloaded"),
        "{err:#}"
    );
    // Once done, the torrent may run again.
    assert!(client.claim(info_hash).is_ok());
}

/// Two torrents that write the same files: the one that starts first runs,
/// the other fails, naming the file and the first torrent, and leaves the
/// files, and the first's resume file, as they were.
#[tokio::test]
async fn a_torrent_sharing_a_file_with_a_running_one_is_refused() {
    init_tracing();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let data = Arc::new(test_data());
    let other: Vec<u8> = data.iter().map(|b| b ^ 7).collect();
    let (mut first, first_hash) = make_named_torrent(&data, "shared");
    let (second, second_hash) = make_named_torrent(&other, "shared");
    assert_ne!(first_hash, second_hash);
    let (tracker_url, _queries) = mock_tracker(Vec::new()).await;
    first.announce = Some(tracker_url);
    let first_path = out.path().join("first.torrent");
    let second_path = out.path().join("second.torrent");
    std::fs::write(&first_path, serde_bencode::to_bytes(&first).unwrap()).unwrap();
    std::fs::write(&second_path, serde_bencode::to_bytes(&second).unwrap()).unwrap();
    write_named_files(out.path(), "shared", &data);

    let mut config = resume_config(out.path(), state.path());
    // Seeds until stopped, so it is still running when the second starts.
    config.seed = SeedLimits {
        ratio: 0.0,
        time: Duration::from_secs(3600),
    };
    let client = client(config);
    let (stop, stop_rx) = oneshot::channel::<()>();
    let running = download_file(&first_path, &client, async {
        stop_rx.await.ok();
    });
    let refused = async {
        // Wait until the first has claimed its torrent, and so its files.
        while client.claim(first_hash).is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let err = download_file(&second_path, &client, pending())
            .await
            .unwrap_err();
        stop.send(()).unwrap();
        err
    };
    let (running, err) = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(running, refused)
    })
    .await
    .expect("timed out");
    running.expect("first run failed");
    let message = format!("{err:#}");
    assert!(
        message.contains("a.bin") && message.contains("shared"),
        "{message}"
    );
    assert_named_files_match(out.path(), "shared", &data);
    let resumes: Vec<_> = std::fs::read_dir(state.path()).unwrap().collect();
    assert_eq!(resumes.len(), 1, "{resumes:?}");
    // Once the first is done, the second may take the files over.
    let claim = client.claim(second_hash).unwrap();
    drop(claim);
}

/// With one connection slot for the session, the second torrent does not
/// connect to its seeder while the first holds the slot, and a peer that
/// connects to it is turned away; once the first is done, the second
/// connects and finishes.
#[tokio::test]
async fn the_connection_limit_spans_torrents() {
    init_tracing();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let mut paths = Vec::new();
    let mut datas = Vec::new();
    let mut hashes = Vec::new();
    let mut seeder_listeners = Vec::new();
    let mut trackers = Vec::new();
    for (name, salt) in [("first", 0u8), ("second", 7)] {
        let data: Vec<u8> = test_data().iter().map(|b| b ^ salt).collect();
        let (mut torrent, info_hash) = make_named_torrent(&data, name);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (tracker_url, queries) = mock_tracker(vec![listener.local_addr().unwrap()]).await;
        torrent.announce = Some(tracker_url);
        let path = out.path().join(format!("{name}.torrent"));
        std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
        paths.push(path);
        datas.push((name, Arc::new(data)));
        hashes.push((info_hash, torrent.num_pieces()));
        seeder_listeners.push(listener);
        trackers.push(queries);
    }
    let client = client(DownloadConfig {
        max_connections: 1,
        ..resume_config(out.path(), state.path())
    });

    let (start_second, start_second_rx) = oneshot::channel::<()>();
    let run_second = async {
        start_second_rx.await.unwrap();
        download_file(&paths[1], &client, pending()).await
    };
    let orchestrate = async {
        let (first_hash, first_pieces) = hashes[0];
        let (second_hash, second_pieces) = hashes[1];
        // The first torrent takes the one slot, and the seeder keeps it
        // there by not unchoking us until the second has been tried.
        let mut first_seeder = accept_leecher(&seeder_listeners[0], first_hash, first_pieces).await;
        start_second.send(()).unwrap();

        // The second has announced, so its port is open and its torrent
        // registered. A peer that connects there is turned away: the
        // connection is closed without a handshake.
        let started = trackers[1].recv().await.unwrap();
        let port: u16 = query_param(&started, "port").unwrap().parse().unwrap();
        let mut peer = TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .unwrap();
        peer.write_all(&mock_handshake(&second_hash)).await.unwrap();
        let mut reply = Vec::new();
        let _ = peer.read_to_end(&mut reply).await;
        assert!(reply.is_empty(), "the peer was answered");

        // The second torrent tried its own seeder before it looked at the
        // incoming peer, and has not got through.
        let waiting = tokio::time::timeout(Duration::ZERO, seeder_listeners[1].accept()).await;
        assert!(waiting.is_err(), "the second torrent connected early");

        send(&mut first_seeder, PeerMessage::Unchoke).await;
        serve(first_seeder, &datas[0].1).await;

        let mut second_seeder =
            accept_leecher(&seeder_listeners[1], second_hash, second_pieces).await;
        send(&mut second_seeder, PeerMessage::Unchoke).await;
        serve(second_seeder, &datas[1].1).await;
    };
    let (first, second, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(
            download_file(&paths[0], &client, pending()),
            run_second,
            orchestrate
        )
    })
    .await
    .expect("downloads timed out");
    first.expect("first download failed");
    second.expect("second download failed");
    for (name, data) in &datas {
        assert_named_files_match(out.path(), name, data);
    }
}

/// A terminal that keeps what is drawn to it.
#[derive(Debug, Clone, Default)]
struct RecordedTerm(Arc<std::sync::Mutex<String>>);

impl RecordedTerm {
    fn drawn(&self) -> String {
        self.0.lock().unwrap().clone()
    }
}

impl indicatif::TermLike for RecordedTerm {
    fn width(&self) -> u16 {
        100
    }
    fn move_cursor_up(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn move_cursor_down(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn move_cursor_right(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn move_cursor_left(&self, _: usize) -> std::io::Result<()> {
        Ok(())
    }
    fn write_line(&self, s: &str) -> std::io::Result<()> {
        self.write_str(s)
    }
    fn write_str(&self, s: &str) -> std::io::Result<()> {
        let mut drawn = self.0.lock().unwrap();
        drawn.push_str(s);
        drawn.push('\n');
        Ok(())
    }
    fn clear_line(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A torrent that finds the hashing turn taken waits, its bar says so, and
/// Ctrl-C ends the wait without hashing anything; with the turn free it
/// hashes. The turn is taken by hand, standing in for another torrent that
/// is hashing.
#[tokio::test]
async fn a_torrent_waits_its_turn_to_check_its_data() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);
    let config = resume_config(out.path(), state.path());
    let term = RecordedTerm::default();
    let storage = Arc::new(Storage::new(&torrent.info, out.path()).unwrap());
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
        seed: SeedLimits::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: MultiProgress::with_draw_target(indicatif::ProgressDrawTarget::term_like(Box::new(
            term.clone(),
        ))),
        resume: resume::Target::new(&config, info_hash),
        stats: Arc::new(TorrentStats::new(info_hash, "test", Path::new("."))),
    });
    let hashing = Semaphore::new(1);
    let turn = hashing.acquire().await.unwrap();

    // Stopped while it waits: nothing is checked, and no resume file is
    // written for data that was never looked at.
    let stopped = resume::initial_pieces(&session, &config, &hashing, async {})
        .await
        .unwrap_err();
    assert!(stopped.is::<Interrupted>(), "{stopped:#}");
    assert!(term.drawn().contains("queued for checking"));
    assert!(!term.drawn().contains("checking multi"));
    assert!(std::fs::read_dir(state.path()).unwrap().next().is_none());

    // Waiting without a shutdown: nothing is checked until the turn is
    // given up, and then the data is.
    let waiting = resume::initial_pieces(&session, &config, &hashing, pending());
    let release = async {
        assert!(!term.drawn().contains("checking multi"));
        drop(turn);
    };
    let (have, ()) = tokio::join!(waiting, release);
    assert_eq!(have.unwrap(), vec![true; num_pieces]);
    // The turn is back.
    assert_eq!(hashing.available_permits(), 1);
}

/// Polls `done` until it holds, failing after a few seconds.
async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A torrent's stats follow its download: queued for checking while
/// another torrent hashes, downloading with its peer listed while that peer
/// holds back, and done with every byte counted and the peer gone.
#[tokio::test]
async fn stats_follow_a_download() {
    use rtrnt::stats::{PeerSource, TorrentState};
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, _queries) = mock_tracker(vec![listener.local_addr().unwrap()]).await;
    torrent.announce = Some(tracker_url.clone());
    let out = tempfile::tempdir().unwrap();
    // Zeros where the data goes: every piece is checked, and none passes.
    write_files(out.path(), &vec![0; data.len()]);
    let path = out.path().join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let (go, gate) = oneshot::channel::<()>();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        gate.await.unwrap();
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let client = Arc::new(client(DownloadConfig {
        output_dir: out.path().to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
        recheck: false,
    }));
    // Another torrent's turn at checking.
    let turn = client.hashing().acquire().await.unwrap();
    let download = tokio::spawn({
        let client = Arc::clone(&client);
        async move { download_file(&path, &client, pending()).await }
    });
    let stats = client.stats();
    wait_until("torrent listed", || stats.torrents().len() == 1).await;
    let torrent_stats = stats.torrents()[0].clone();
    assert_eq!(torrent_stats.info_hash(), info_hash);
    assert_eq!(torrent_stats.name(), "multi");
    assert_eq!(torrent_stats.layout().unwrap().size, data.len() as u64);
    wait_until("queued for checking", || {
        torrent_stats.state() == TorrentState::QueuedForChecking
    })
    .await;
    drop(turn);

    wait_until("peer connected", || {
        torrent_stats.state() == TorrentState::Downloading
            && torrent_stats
                .peers()
                .first()
                .is_some_and(|peer| peer.flags().am_interested() && peer.flags().peer_choking())
    })
    .await;
    let peer = torrent_stats.peers()[0].clone();
    assert!(!peer.inbound);
    assert_eq!(peer.pieces() as usize, num_pieces);
    let pieces = torrent_stats.pieces().unwrap();
    assert_eq!(pieces.have, vec![false; num_pieces]);
    assert_eq!(pieces.availability, vec![1; num_pieces]);
    assert_eq!(torrent_stats.source(PeerSource::Tracker), 1);
    let trackers = torrent_stats.trackers();
    assert_eq!(trackers.len(), 1);
    assert_eq!(trackers[0].url, tracker_url);
    assert_eq!(trackers[0].peers, Some(1));
    assert!(torrent_stats.next_announce().is_some());
    assert_eq!(stats.connections(), 1);
    drop(peer);

    go.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(20), download)
        .await
        .expect("download timed out")
        .unwrap()
        .expect("download failed");
    seeder.await.unwrap();
    assert_files_match(out.path(), &data);
    assert_eq!(torrent_stats.state(), TorrentState::Done);
    assert!(torrent_stats.finished_at().is_some());
    assert_eq!(torrent_stats.downloaded(), data.len() as u64);
    assert_eq!(torrent_stats.received(), data.len() as u64);
    assert_eq!(torrent_stats.verified(), data.len() as u64);
    assert!(torrent_stats.pool() >= 1);
    assert!(torrent_stats.peers().is_empty());
    assert_eq!(torrent_stats.pieces().unwrap().have, vec![true; num_pieces]);
    assert_eq!(torrent_stats.next_announce(), None);
}
