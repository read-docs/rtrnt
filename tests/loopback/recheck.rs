use rtrnt::manager::{Command, Manager, StartOptions};
use rtrnt::peer::PeerMessage;
use rtrnt::source::Source;
use rtrnt::stats::TorrentState;
use std::future::pending;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

use crate::common::*;

/// A torrent whose files are all in place is Done at once. A byte of a piece
/// is then corrupted on disk, which only a recheck notices. `Recheck` runs
/// the ended torrent again with
/// hashing: the seed is asked for that piece alone, and the run ends Done
/// with the files right.
#[tokio::test]
async fn a_rechecked_torrent_fetches_the_piece_found_corrupt() {
    init_tracing();
    let data = Arc::new(test_data());
    let (mut torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, _queries) = mock_tracker(vec![seeder.local_addr().unwrap()]).await;
    torrent.announce = Some(tracker_url);
    let path = out.path().join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();

    let (requested_tx, mut requested) = mpsc::unbounded_channel();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            if let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
            {
                requested_tx.send(index).unwrap();
                send(&mut socket, piece(&seeder_data, index, begin, length)).await;
            }
        }
    });

    let client = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    let mut idle = manager.idle();
    manager
        .start(vec![Source::File(path)], StartOptions::default())
        .await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| idle))
        .await
        .expect("the first run did not end")
        .unwrap();
    assert!(requested.try_recv().is_err(), "nothing was to be fetched");

    // Flip the first byte of the last file (inside piece 1), keeping its size.
    let victim = out.path().join("multi/dir").join(FILES[2].0);
    let mut contents = std::fs::read(&victim).unwrap();
    contents[0] ^= 0xff;
    std::fs::write(&victim, contents).unwrap();

    commands.send(Command::Recheck(info_hash)).unwrap();
    drop(commands);
    let exit = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the recheck timed out")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);
    assert_files_match(out.path(), &data);
    assert_eq!(client.stats().torrents()[0].state(), TorrentState::Done);

    let mut pieces = Vec::new();
    while let Ok(index) = requested.try_recv() {
        pieces.push(index);
    }
    assert!(!pieces.is_empty(), "the corrupt piece was not fetched");
    assert!(
        pieces.iter().all(|&index| index == 1),
        "requested {pieces:?}"
    );
}
