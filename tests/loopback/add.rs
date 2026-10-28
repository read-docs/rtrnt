use rtrnt::manager::{Command, Manager};
use rtrnt::peer::PeerMessage;
use rtrnt::source::Source;
use rtrnt::stats::TorrentState;
use std::future::pending;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::common::*;

/// A session started with no torrent, as the UI starts one, gets a torrent
/// added and downloads it. Added again while it runs, it is refused with a
/// notice and no second row, and the first goes on to finish.
#[tokio::test]
async fn an_added_torrent_downloads_and_is_not_added_twice() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();

    // The seed holds back until the second add has been refused.
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, mut queries) = mock_tracker(vec![seeder.local_addr().unwrap()]).await;
    let (release, released) = oneshot::channel();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        released.await.unwrap();
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });
    torrent.announce = Some(tracker_url);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let client = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    manager.expect_more();
    let mut idle = manager.idle();
    manager.start(Vec::new()).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    let stats = client.stats();

    commands
        .send(Command::Add(Source::File(path.clone())))
        .unwrap();
    let started = next_announce(&mut queries).await;
    assert_eq!(query_param(&started, "event"), Some("started"));
    commands.send(Command::Add(Source::File(path))).unwrap();
    let notice = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some((_, notice)) = stats.last_notice() {
                break notice;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no notice");
    assert_eq!(notice, "multi: already running");
    let torrents = stats.torrents();
    assert_eq!(torrents.len(), 1);
    assert!(!torrents[0].state().is_finished());

    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| idle))
        .await
        .expect("the download did not finish")
        .unwrap();
    assert_eq!(stats.torrents()[0].state(), TorrentState::Done);
    assert_files_match(out.path(), &data);

    drop(commands);
    let exit = tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("the session did not end")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);
}
