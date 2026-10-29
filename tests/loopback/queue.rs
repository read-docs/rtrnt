use rtrnt::manager::{Command, Manager, StartOptions};
use rtrnt::queue::QueueLimits;
use rtrnt::source::Source;
use rtrnt::stats::{TorrentState, TorrentStats};
use std::future::pending;
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

/// Writes a `.torrent` called `name`, whose only peer connects and says
/// nothing more, so that the torrent runs until it is stopped. Returns its
/// path and info hash.
async fn stalled_torrent(dir: &std::path::Path, name: &str) -> (std::path::PathBuf, [u8; 20]) {
    let data = test_data();
    let (mut torrent, info_hash) = make_named_torrent(&data, name);
    let num_pieces = torrent.num_pieces();
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, _queries) = mock_tracker(vec![seeder.local_addr().unwrap()]).await;
    tokio::spawn(async move {
        loop {
            let socket = accept_leecher(&seeder, info_hash, num_pieces).await;
            std::mem::forget(socket);
        }
    });
    torrent.announce = Some(tracker_url);
    let path = dir.join(format!("{name}.torrent"));
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
    (path, info_hash)
}

fn row(client: &rtrnt::client::Client, info_hash: [u8; 20]) -> Option<Arc<TorrentStats>> {
    client
        .stats()
        .torrents()
        .into_iter()
        .find(|stats| stats.info_hash() == info_hash)
}

/// With one download allowed at a time, the second of two torrents waits,
/// shown as queued, and runs once the first is paused; moving it up in the
/// queue changes the order of the table.
#[tokio::test]
async fn a_torrent_over_the_limit_waits_for_the_one_ahead() {
    init_tracing();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (first, first_hash) = stalled_torrent(dir.path(), "first").await;
    let (second, second_hash) = stalled_torrent(dir.path(), "second").await;

    let client = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager =
        Manager::new(Arc::clone(&client), pending::<()>).with_queue_limits(QueueLimits {
            downloads: 1,
            seeds: 0,
        });
    manager.expect_more();
    manager
        .start(
            vec![Source::File(first), Source::File(second)],
            StartOptions::default(),
        )
        .await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));

    wait_for("the first to run", || {
        row(&client, first_hash).is_some_and(|stats| !stats.state().is_finished())
    })
    .await;
    assert_eq!(
        row(&client, second_hash).unwrap().state(),
        TorrentState::Queued
    );

    // The table follows the queue.
    commands.send(Command::MoveUp(second_hash)).unwrap();
    wait_for("the move", || {
        client.stats().torrents().first().map(|s| s.info_hash()) == Some(second_hash)
    })
    .await;

    commands.send(Command::Pause(first_hash)).unwrap();
    wait_for("the second to run", || {
        row(&client, second_hash).is_some_and(|stats| !stats.state().is_finished())
    })
    .await;
    wait_for("the first to pause", || {
        row(&client, first_hash).is_some_and(|stats| stats.state() == TorrentState::Paused)
    })
    .await;

    commands.send(Command::Stop).unwrap();
    commands.send(Command::Pause(second_hash)).unwrap();
    drop(commands);
    run.await.unwrap().unwrap();
}
