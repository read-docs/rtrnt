use rtrnt::download::SeedLimits;
use rtrnt::manager::{Command, Manager, StartOptions};
use rtrnt::peer::{BlockRef, PeerMessage};
use rtrnt::source::Source;
use rtrnt::stats::TorrentState;
use std::future::pending;
use std::net::SocketAddr;
use std::ops::Range;
use std::path::Path;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::common::*;

/// Writes `torrent` next to the download, announcing to `tracker_url`, and
/// returns its path.
fn write_torrent(
    out: &Path,
    mut torrent: rtrnt::torrent::TorrentFile,
    tracker_url: String,
) -> std::path::PathBuf {
    torrent.announce = Some(tracker_url);
    let path = out.join("multi.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&torrent).unwrap()).unwrap();
    path
}

/// A download paused partway: the tracker hears `stopped`, the seed's
/// connection closes and the task ends, with the resume file saved. Resumed,
/// it trusts the resume file (the turn to hash is held throughout, so any
/// hashing would hang), fetches only what is missing, and finishes; the
/// row's totals count the bytes from before the pause, and the tracker hears
/// this run's only.
#[tokio::test]
async fn a_paused_download_resumes_where_it_stopped() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();

    // First the seed has piece 0 alone, then, connected to again, all of
    // them; it says which pieces the second run asked for.
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (tracker_url, mut queries) = mock_tracker(vec![seeder.local_addr().unwrap()]).await;
    let path = write_torrent(out.path(), torrent, tracker_url);
    let (first_done, first_done_rx) = oneshot::channel();
    let (closed, closed_rx) = oneshot::channel();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let mut socket = accept_partial(&seeder, info_hash, num_pieces, &[0]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut first_done = Some(first_done);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request(BlockRef {
                    index,
                    begin,
                    length,
                }) => send(&mut socket, piece(&seeder_data, index, begin, length)).await,
                PeerMessage::NotInterested => {
                    first_done.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
        closed.send(()).unwrap();

        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut requested = Vec::new();
        while let Some(message) = read_message(&mut socket).await {
            if let PeerMessage::Request(BlockRef {
                index,
                begin,
                length,
            }) = message
            {
                requested.push(index);
                send(&mut socket, piece(&seeder_data, index, begin, length)).await;
            }
        }
        requested
    });

    let client = Arc::new(client(resume_config(out.path(), state.path())));
    // Nothing is hashed: there is no data at first, and then a resume file.
    let _turn = client.hashing().acquire().await.unwrap();
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    let mut idle = manager.idle();
    manager
        .start(vec![Source::File(path)], StartOptions::default())
        .await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    let stats = client.stats();

    tokio::time::timeout(Duration::from_secs(10), first_done_rx)
        .await
        .expect("piece 0 not downloaded")
        .unwrap();
    commands.send(Command::Pause(info_hash)).unwrap();
    tokio::time::timeout(Duration::from_secs(10), closed_rx)
        .await
        .expect("the seed's connection stayed open")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| idle))
        .await
        .expect("the task did not end")
        .unwrap();
    let torrents = stats.torrents();
    assert_eq!(torrents.len(), 1);
    assert_eq!(torrents[0].state(), TorrentState::Paused);
    assert_eq!(torrents[0].downloaded(), PIECE_LENGTH);
    let (total_s, piece_s, zero) = (total.to_string(), PIECE_LENGTH.to_string(), "0".to_string());
    let rest = total - PIECE_LENGTH;
    let rest_s = rest.to_string();
    let first_run = [
        next_announce(&mut queries).await,
        next_announce(&mut queries).await,
    ];
    assert_eq!(
        announces(&first_run),
        [
            ["started".into(), zero.clone(), zero.clone(), total_s],
            ["stopped".into(), piece_s, zero.clone(), rest_s.clone()],
        ]
    );
    assert_eq!(std::fs::read_dir(state.path()).unwrap().count(), 1);

    commands.send(Command::Resume(info_hash)).unwrap();
    drop(commands);
    let exit = tokio::time::timeout(Duration::from_secs(20), run)
        .await
        .expect("the resumed download timed out")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);
    assert_files_match(out.path(), &data);
    let requested = seeder.await.unwrap();
    assert!(!requested.is_empty());
    assert!(
        requested.iter().all(|&index| index != 0),
        "requested {requested:?}"
    );

    // The same row, counting both runs.
    let torrents = stats.torrents();
    assert_eq!(torrents.len(), 1);
    let torrent = &torrents[0];
    assert_eq!(torrent.state(), TorrentState::Done);
    assert_eq!(torrent.downloaded(), total);
    assert_eq!(torrent.received(), total);
    assert_eq!(torrent.verified(), total);
    assert_eq!(torrent.previous().downloaded, PIECE_LENGTH);
    assert!(torrent.added() < torrent.started());

    let mut second_run = Vec::new();
    while let Ok(query) = queries.try_recv() {
        second_run.push(query);
    }
    let second_run = announces(&second_run);
    assert_eq!(
        second_run.first().unwrap(),
        &[
            "started".to_string(),
            zero.clone(),
            zero.clone(),
            rest_s.clone()
        ]
    );
    assert_eq!(
        second_run.last().unwrap(),
        &["stopped".to_string(), rest_s, zero.clone(), zero]
    );
}

/// Asks the client over `socket` for every block of `pieces` once it
/// unchokes us, and checks what comes back.
async fn leech_pieces(socket: &mut TcpStream, data: &[u8], pieces: Range<usize>) {
    send(socket, PeerMessage::Interested).await;
    loop {
        match next_message(socket).await {
            Some(PeerMessage::Unchoke) => break,
            Some(PeerMessage::Bitfield { .. } | PeerMessage::Have { .. }) => {}
            other => panic!("expected an unchoke, got {other:?}"),
        }
    }
    let mut requests = Vec::new();
    for (index, chunk) in data.chunks(PIECE_LENGTH as usize).enumerate() {
        if !pieces.contains(&index) {
            continue;
        }
        for begin in (0..chunk.len()).step_by(BLOCK as usize) {
            let length = (chunk.len() - begin).min(BLOCK as usize);
            requests.push((index as u32, begin as u32, length as u32));
        }
    }
    let messages: Vec<PeerMessage> = requests
        .iter()
        .map(|&(index, begin, length)| request(index, begin, length))
        .collect();
    send_batch(socket, &messages).await;
    for (index, begin, length) in requests {
        assert_eq!(
            next_message(socket).await,
            Some(piece(data, index, begin, length))
        );
    }
}

/// A seed with a ratio of 1 uploads piece 0 and is paused. Resumed, it
/// stops once a leecher has taken pieces 1 and 2, which is the rest of a
/// full share counting what went before the pause: the pause does not
/// start the ratio over.
#[tokio::test]
async fn a_paused_seed_keeps_its_ratio() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let total = data.len() as u64;
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);
    let (tracker_url, mut queries) = mock_tracker(Vec::new()).await;
    let path = write_torrent(out.path(), torrent, tracker_url);

    let mut config = resume_config(out.path(), state.path());
    config.seed = SeedLimits {
        ratio: 1.0,
        time: Duration::ZERO,
    };
    let client = Arc::new(client(config));
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    let mut idle = manager.idle();
    manager
        .start(vec![Source::File(path)], StartOptions::default())
        .await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    // The leecher finds us through the port we announce.
    let our_addr = |started: &str| {
        let port: u16 = query_param(started, "port").unwrap().parse().unwrap();
        SocketAddr::from(([127, 0, 0, 1], port))
    };

    let started = next_announce(&mut queries).await;
    let mut socket = connect_to(our_addr(&started), info_hash).await;
    leech_pieces(&mut socket, &data, 0..1).await;
    commands.send(Command::Pause(info_hash)).unwrap();
    assert_eq!(next_message(&mut socket).await, None, "still seeding");
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| idle))
        .await
        .expect("the task did not end")
        .unwrap();
    let stopped = next_announce(&mut queries).await;
    assert_eq!(query_param(&stopped, "event"), Some("stopped"));
    assert_eq!(client.stats().torrents()[0].uploaded(), PIECE_LENGTH);

    commands.send(Command::Resume(info_hash)).unwrap();
    drop(commands);
    let started = next_announce(&mut queries).await;
    assert_eq!(query_param(&started, "event"), Some("started"));
    let mut socket = connect_to(our_addr(&started), info_hash).await;
    leech_pieces(&mut socket, &data, 1..3).await;
    // Short of a share this run, but a full one in all.
    assert_eq!(next_message(&mut socket).await, None, "still seeding");
    let exit = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("seeding did not stop")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);

    let stopped = next_announce(&mut queries).await;
    let rest = (total - PIECE_LENGTH).to_string();
    assert_eq!(
        announces(&[stopped]),
        [["stopped".into(), "0".into(), rest, "0".into()]]
    );
    let torrent = &client.stats().torrents()[0];
    assert_eq!(torrent.state(), TorrentState::Done);
    assert_eq!(torrent.uploaded(), total);
}
