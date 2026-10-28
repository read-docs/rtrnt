use rtrnt::manager::{Command, Manager, StartOptions, State};
use rtrnt::session::{SavedSession, SessionStore};
use rtrnt::source::Source;
use rtrnt::stats::TorrentState;
use std::future::pending;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::common::*;

/// Waits until `check` holds, or fails the test.
async fn wait_for(what: &str, mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// A torrent added from a file and paused, with limits set, is there again
/// in a new manager started from the session file: paused, with a row, from
/// the copy of the `.torrent` kept in the store even though the original is
/// gone, and with the same limits saved.
#[tokio::test]
async fn a_paused_torrent_and_the_limits_survive_a_new_manager() {
    init_tracing();
    let data = test_data();
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    // A seed that connects and says nothing more, so the torrent waits.
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, _queries) = mock_tracker(vec![seeder.local_addr().unwrap()]).await;
    tokio::spawn(async move {
        loop {
            let socket = accept_leecher(&seeder, info_hash, num_pieces).await;
            std::mem::forget(socket);
        }
    });
    torrent.announce = Some(tracker_url);
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let store = SessionStore::new(state.path().join("session-dir"));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    // First run.
    let client_one = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager =
        Manager::new(Arc::clone(&client_one), pending::<()>).with_session(store.clone());
    manager.expect_more();
    manager.start(Vec::new(), StartOptions::default()).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    commands
        .send(Command::Add(Source::File(path.clone())))
        .unwrap();
    commands.send(Command::SetRateLimits(300, 50)).unwrap();
    wait_for("the torrent to run", || {
        client_one
            .stats()
            .torrents()
            .first()
            .is_some_and(|stats| !stats.state().is_finished())
    })
    .await;
    commands.send(Command::Pause(info_hash)).unwrap();
    wait_for("the pause", || {
        client_one
            .stats()
            .torrents()
            .first()
            .is_some_and(|stats| stats.state() == TorrentState::Paused)
    })
    .await;
    drop(commands);
    assert_eq!(
        run.await.unwrap().unwrap(),
        ExitCode::from(rtrnt::manager::EXIT_INTERRUPTED)
    );
    drop(client_one);
    std::fs::remove_file(&path).unwrap();

    // Second run: what the session file says.
    let saved = store.load().unwrap();
    assert_eq!(saved.torrents.len(), 1);
    assert!(saved.torrents[0].paused);
    let limits = saved.rate_limits.unwrap();
    assert_eq!((limits.download, limits.upload), (300, 50));

    let client_two = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager =
        Manager::new(Arc::clone(&client_two), pending::<()>).with_session(store.clone());
    manager.expect_more();
    manager
        .restore(SavedSession {
            torrents: saved.torrents,
            rate_limits: saved.rate_limits,
        })
        .await;
    assert_eq!(manager.entries().len(), 1);
    assert_eq!(manager.entries()[0].info_hash, info_hash);
    assert!(matches!(manager.entries()[0].state, State::Paused));
    let rows = client_two.stats().torrents();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state(), TorrentState::Paused);
    assert_eq!(rows[0].name(), "multi");
    assert_eq!(store.load().unwrap().rate_limits, Some(limits));

    // And it resumes from the copy.
    let (commands, commands_rx) = mpsc::unbounded_channel();
    commands.send(Command::Resume(info_hash)).unwrap();
    let run = tokio::spawn(manager.run(commands_rx));
    wait_for("the resume", || {
        client_two
            .stats()
            .torrents()
            .first()
            .is_some_and(|stats| !stats.state().is_finished())
    })
    .await;
    commands.send(Command::Stop).unwrap();
    commands.send(Command::Pause(info_hash)).unwrap();
    drop(commands);
    run.await.unwrap().unwrap();
}
