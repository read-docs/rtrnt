use rtrnt::download::SeedLimits;
use rtrnt::manager::{Command, Manager};
use rtrnt::source::Source;
use std::future::pending;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::common::*;

/// A complete torrent seeding in `out`, with its resume file in `state`,
/// removed once it has announced, `delete` saying whether its files go
/// too. The tracker must hear `stopped` and the row must go; returns once
/// the session has ended, as it should, with success.
async fn seed_and_remove(out: &Path, state: &Path, delete: bool) {
    init_tracing();
    let data = test_data();
    let (mut torrent, info_hash) = make_torrent(&data);
    write_files(out, &data);
    let (tracker_url, mut queries) = mock_tracker(Vec::new()).await;
    torrent.announce = Some(tracker_url);
    let path = out.join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let mut config = resume_config(out, state);
    config.seed = SeedLimits {
        ratio: 1.0,
        time: Duration::ZERO,
    };
    let client = Arc::new(client(config));
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    let mut idle = manager.idle();
    manager.start(vec![Source::File(path)]).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));

    let started = next_announce(&mut queries).await;
    assert_eq!(query_param(&started, "event"), Some("started"));
    assert_eq!(client.stats().torrents().len(), 1);
    commands
        .send(Command::Remove { info_hash, delete })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| idle))
        .await
        .expect("the task did not end")
        .unwrap();
    let stopped = next_announce(&mut queries).await;
    assert_eq!(query_param(&stopped, "event"), Some("stopped"));
    assert!(client.stats().torrents().is_empty());

    drop(commands);
    let exit = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("the session did not end")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);
}

/// Removed without `delete`, a seed stops and its row goes, but its files
/// and its resume file stay, so adding it again would start at once.
#[tokio::test]
async fn a_removed_seed_keeps_its_files() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seed_and_remove(out.path(), state.path(), false).await;
    assert_files_match(out.path(), &test_data());
    assert_eq!(std::fs::read_dir(state.path()).unwrap().count(), 1);
}

/// Removed with `delete`, its files go with the directories they leave
/// empty, and so does its resume file; the `.torrent` it came from and the
/// output dir stay.
#[tokio::test]
async fn a_removed_seed_deletes_its_files_if_asked() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    seed_and_remove(out.path(), state.path(), true).await;
    let left: Vec<_> = std::fs::read_dir(out.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(left, ["multi.torrent"]);
    assert_eq!(std::fs::read_dir(state.path()).unwrap().count(), 0);
}
