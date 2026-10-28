use super::*;
use crate::client::Client;
use crate::download::{DownloadConfig, SeedLimits};
use crate::manager::{Manager, StartOptions};
use crate::torrent::{Info, TorrentFile};
use futures_util::future::BoxFuture;
use indicatif::{MultiProgress, ProgressDrawTarget};
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;

const LINK: &str = "magnet:?xt=urn:btih:0101010101010101010101010101010101010101&dn=Linked";

fn client() -> Arc<Client> {
    let config = DownloadConfig {
        output_dir: ".".into(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: None,
    };
    Arc::new(Client::new(
        config,
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden()),
    ))
}

/// A watcher of `dir`, with the receiving end of its commands.
fn watcher(
    dir: &Path,
    delete: bool,
    store: Option<SessionStore>,
) -> (Watcher, UnboundedReceiver<Command>, Arc<Client>) {
    let (commands, rx) = mpsc::unbounded_channel();
    let client = client();
    let watcher = Watcher::new(
        dir.to_path_buf(),
        delete,
        store,
        commands,
        Arc::clone(client.stats()),
    );
    (watcher, rx, client)
}

/// The bytes of a valid one-piece `.torrent` called `name`, and its info
/// hash.
fn torrent_bytes(name: &str) -> (Vec<u8>, [u8; 20]) {
    let torrent = TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: name.to_string(),
            piece_length: 16 * 1024,
            pieces: serde_bytes::ByteBuf::from(vec![0; 20]),
            private: None,
            length: Some(16 * 1024),
            files: None,
        },
    };
    let data = serde_bencode::to_bytes(&torrent).unwrap();
    let info_hash = torrent::parse_metainfo(&data).unwrap().info_hash;
    (data, info_hash)
}

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

fn notice(client: &Client) -> String {
    client
        .stats()
        .last_notice()
        .map(|(_, text)| text)
        .unwrap_or_default()
}

#[tokio::test]
async fn a_file_is_taken_once_it_looks_the_same_on_two_polls() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let watched = dir.path().join("watched");
    std::fs::create_dir(&watched).unwrap();
    let (data, hash) = torrent_bytes("t");
    std::fs::write(watched.join("t.torrent"), &data).unwrap();
    let (mut watcher, mut rx, _client) = watcher(&watched, false, Some(store.clone()));

    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_err(), "seen once is not enough");
    assert_eq!(names(&watched), ["t.torrent"]);

    assert!(watcher.poll().await);
    // The torrent is added from its copy, which the rename leaves be.
    let copy = store.torrent_path(hash);
    assert_eq!(
        rx.try_recv().unwrap(),
        Command::Add(Source::File(copy.clone()))
    );
    assert_eq!(std::fs::read(copy).unwrap(), data);
    assert_eq!(names(&watched), ["t.torrent.added"]);

    // Nothing more comes of it.
    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn a_file_still_being_written_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.torrent");
    let (data, _) = torrent_bytes("t");
    let store = SessionStore::new(dir.path().join("state"));
    let (mut watcher, mut rx, _client) = watcher(dir.path(), false, Some(store));

    std::fs::write(&path, &data[..10]).unwrap();
    assert!(watcher.poll().await);
    // It grows between the polls, so the second poll is a first look again.
    std::fs::write(&path, &data[..20]).unwrap();
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_err());
    assert!(path.exists());

    std::fs::write(&path, &data).unwrap();
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_err());
    assert!(watcher.poll().await);
    assert!(matches!(rx.try_recv(), Ok(Command::Add(Source::File(_)))));
    assert!(!path.exists());
}

#[tokio::test]
async fn a_magnet_file_is_read_from_its_first_line() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.magnet"), format!("{LINK}\r\nignored\n")).unwrap();
    std::fs::write(dir.path().join("b.MAGNET"), format!("  {LINK}\n")).unwrap();
    let (mut watcher, mut rx, _client) = watcher(dir.path(), false, None);

    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    let link = Source::parse(LINK).unwrap();
    assert_eq!(rx.try_recv().unwrap(), Command::Add(link.clone()));
    assert_eq!(rx.try_recv().unwrap(), Command::Add(link));
    assert_eq!(names(dir.path()), ["a.magnet.added", "b.MAGNET.added"]);
}

#[tokio::test]
async fn a_file_that_does_not_parse_is_set_aside_with_a_notice() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("bad.torrent"), "not bencode").unwrap();
    std::fs::write(dir.path().join("bad.magnet"), "just text\n").unwrap();
    std::fs::write(dir.path().join("empty.magnet"), "").unwrap();
    let (mut watcher, mut rx, client) = watcher(dir.path(), false, None);

    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_err());
    assert_eq!(
        names(dir.path()),
        [
            "bad.magnet.invalid",
            "bad.torrent.invalid",
            "empty.magnet.invalid"
        ]
    );
    assert!(
        notice(&client).ends_with(", set aside"),
        "{}",
        notice(&client)
    );
}

#[tokio::test]
async fn other_files_and_directories_are_ignored() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("notes.txt"), "x").unwrap();
    std::fs::write(dir.path().join("t.torrent.added"), "x").unwrap();
    std::fs::write(dir.path().join("t.torrent.invalid"), "x").unwrap();
    std::fs::create_dir(dir.path().join("dir.torrent")).unwrap();
    let (mut watcher, mut rx, _client) = watcher(dir.path(), false, None);

    for _ in 0..3 {
        assert!(watcher.poll().await);
    }
    assert!(rx.try_recv().is_err());
    assert_eq!(
        names(dir.path()),
        [
            "dir.torrent",
            "notes.txt",
            "t.torrent.added",
            "t.torrent.invalid"
        ]
    );
}

#[tokio::test]
async fn with_delete_the_file_is_deleted_instead_of_renamed() {
    let dir = tempfile::tempdir().unwrap();
    let watched = dir.path().join("watched");
    std::fs::create_dir(&watched).unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let (data, hash) = torrent_bytes("t");
    std::fs::write(watched.join("t.torrent"), data).unwrap();
    std::fs::write(watched.join("l.magnet"), LINK).unwrap();
    let (mut watcher, mut rx, _client) = watcher(&watched, true, Some(store.clone()));

    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_ok());
    assert!(rx.try_recv().is_ok());
    assert!(names(&watched).is_empty());
    // The copy is what is left of the torrent.
    assert!(store.torrent_path(hash).exists());
}

#[tokio::test]
async fn without_a_store_the_file_stays_and_is_added_once() {
    let dir = tempfile::tempdir().unwrap();
    let (data, _) = torrent_bytes("t");
    let path = dir.path().join("t.torrent");
    std::fs::write(&path, data).unwrap();
    let (mut watcher, mut rx, _client) = watcher(dir.path(), true, None);

    for _ in 0..5 {
        assert!(watcher.poll().await);
    }
    assert_eq!(
        rx.try_recv().unwrap(),
        Command::Add(Source::File(path.clone()))
    );
    assert!(rx.try_recv().is_err());
    assert!(path.exists());

    // Changed, it is a new file.
    std::fs::write(&path, "longer than before, and not a torrent").unwrap();
    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    assert!(!path.exists());
    assert!(dir.path().join("t.torrent.invalid").exists());
}

#[tokio::test]
async fn a_missing_directory_is_not_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let watched = dir.path().join("later");
    let (mut watcher, mut rx, _client) = watcher(&watched, false, None);
    assert!(watcher.poll().await);
    assert!(watcher.poll().await);

    std::fs::create_dir(&watched).unwrap();
    std::fs::write(watched.join("l.magnet"), LINK).unwrap();
    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    assert!(rx.try_recv().is_ok());
}

#[tokio::test]
async fn a_closed_command_channel_ends_the_watch() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("l.magnet"), LINK).unwrap();
    let (mut watcher, rx, _client) = watcher(dir.path(), false, None);
    drop(rx);
    assert!(watcher.poll().await);
    assert!(!watcher.poll().await);
    assert!(dir.path().join("l.magnet").exists());
}

#[tokio::test(start_paused = true)]
async fn the_watcher_polls_until_shutdown_and_then_drops_its_sender() {
    let dir = tempfile::tempdir().unwrap();
    let (watcher, mut rx, _client) = watcher(dir.path(), false, None);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let run = tokio::spawn(watcher.run(async move {
        let _ = stopped.await;
    }));

    std::fs::write(dir.path().join("l.magnet"), LINK).unwrap();
    // Two polls, with the clock moving by itself.
    tokio::time::sleep(POLL_INTERVAL * 3).await;
    assert!(matches!(rx.try_recv(), Ok(Command::Add(_))));

    stop.send(()).unwrap();
    run.await.unwrap();
    assert!(rx.recv().await.is_none(), "the channel is closed");
}

#[tokio::test]
async fn a_dropped_file_becomes_an_entry_of_the_manager() {
    let dir = tempfile::tempdir().unwrap();
    let watched = dir.path().join("watched");
    std::fs::create_dir(&watched).unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let (data, hash) = torrent_bytes("t");
    std::fs::write(watched.join("t.torrent"), data).unwrap();
    std::fs::write(watched.join("l.magnet"), LINK).unwrap();

    let started = Arc::new(Mutex::new(Vec::new()));
    let fake = {
        let started = Arc::clone(&started);
        move |_: Arc<Client>, torrent: crate::manager::Torrent, _: StartOptions, _: _| {
            started.lock().unwrap().push(torrent.info_hash());
            Box::pin(async { Ok(()) }) as BoxFuture<'static, Result<()>>
        }
    };
    let (watcher, rx, _client) = watcher(&watched, false, Some(store.clone()));
    let mut manager = Manager::with_runner(client(), std::future::pending::<()>, fake);
    manager.expect_more();
    manager.start(Vec::new(), StartOptions::default()).await;
    let run = tokio::spawn(manager.run(rx));

    let mut watcher = watcher;
    assert!(watcher.poll().await);
    assert!(watcher.poll().await);
    // Dropping the watcher closes the channel and lets the run end.
    drop(watcher);
    run.await.unwrap().unwrap();

    let mut started = started.lock().unwrap().clone();
    started.sort();
    let mut expected = vec![[1; 20], hash];
    expected.sort();
    assert_eq!(started, expected);
}
