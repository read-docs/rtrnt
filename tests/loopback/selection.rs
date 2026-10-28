use rtrnt::download::{download_from_peers, download_metainfo};
use rtrnt::manager::{Command, Manager, StartOptions};
use rtrnt::peer::{BlockRef, PeerMessage};
use rtrnt::selection::{FilePicks, FileSelection, Priority};
use rtrnt::source::Source;
use rtrnt::stats::TorrentState;
use rtrnt::storage::Storage;
use rtrnt::torrent::{FileInfo, Info, Metainfo, TorrentFile};
use serde_bytes::ByteBuf;
use sha1::Digest;
use std::future::pending;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

use crate::common::*;

/// Three files of one piece each, so that the middle one can be skipped
/// without any piece being shared.
fn three_files() -> (Vec<u8>, Metainfo) {
    let len = PIECE_LENGTH as usize;
    let data: Vec<u8> = (0..3 * len).map(|i| (i * 7 % 251) as u8).collect();
    let pieces: Vec<u8> = data
        .chunks(len)
        .flat_map(|chunk| sha1::Sha1::digest(chunk).to_vec())
        .collect();
    let info = Info {
        name: "sel".into(),
        piece_length: PIECE_LENGTH,
        pieces: ByteBuf::from(pieces),
        private: None,
        length: None,
        files: Some(
            ["x", "y", "z"]
                .iter()
                .map(|name| FileInfo {
                    path: vec![name.to_string()],
                    length: PIECE_LENGTH,
                })
                .collect(),
        ),
    };
    let info_bytes = serde_bencode::to_bytes(&info).unwrap();
    let info_hash = sha1::Sha1::digest(&info_bytes).into();
    let torrent = TorrentFile {
        announce: None,
        announce_list: None,
        info,
    };
    (
        data,
        Metainfo {
            torrent,
            info_hash,
            info_bytes,
        },
    )
}

fn skip_middle() -> FileSelection {
    FileSelection::new(vec![Priority::Normal, Priority::Skip, Priority::Normal])
}

/// A skipped file that no wanted piece touches is never created, the
/// download is complete without its piece, and the tracker is told only
/// about what is wanted. A second run with no selection of its own takes
/// the one saved in the resume file.
#[tokio::test]
async fn a_skipped_file_is_not_downloaded_or_created() {
    init_tracing();
    let (data, mut metainfo) = three_files();
    let info_hash = metainfo.info_hash;
    let num_pieces = metainfo.torrent.num_pieces();
    let data = Arc::new(data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    tokio::spawn(seeder(listener, Arc::clone(&data), info_hash, num_pieces));
    let (tracker_url, mut queries) = mock_tracker(vec![seeder_addr]).await;
    metainfo.torrent.announce = Some(tracker_url);

    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = resume_config(out.path(), state.path());
    let options = StartOptions {
        selection: Some(skip_middle()),
        ..StartOptions::default()
    };
    tokio::time::timeout(
        Duration::from_secs(30),
        download_metainfo(metainfo, &options, &client(config), pending()),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    let root = out.path().join("sel");
    let len = PIECE_LENGTH as usize;
    assert_eq!(std::fs::read(root.join("x")).unwrap(), data[..len]);
    assert_eq!(std::fs::read(root.join("z")).unwrap(), data[2 * len..]);
    assert!(!root.join("y").exists(), "the skipped file was created");
    let started = next_announce(&mut queries).await;
    assert_eq!(query_param(&started, "left"), Some("65536"));

    // Again, with nothing said: the resume file remembers.
    let (_, metainfo) = three_files();
    let config = resume_config(out.path(), state.path());
    tokio::time::timeout(
        Duration::from_secs(30),
        download_metainfo(
            metainfo,
            &StartOptions::default(),
            &client(config),
            pending(),
        ),
    )
    .await
    .expect("second run timed out")
    .expect("second run failed");
    assert!(!root.join("y").exists(), "the selection was forgotten");
}

/// A skipped file that shares a piece with a wanted one is created in full,
/// and its pieces that no wanted file shares are left as zeros.
#[tokio::test]
async fn a_skipped_file_sharing_a_piece_is_created_but_not_filled() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    tokio::spawn(seeder(listener, Arc::clone(&data), info_hash, num_pieces));

    // a.bin is bytes 0..40000: piece 0 is only a.bin's, piece 1 has the end
    // of a.bin and the start of b.bin.
    let out = tempfile::tempdir().unwrap();
    let selection = FileSelection::new(vec![Priority::Skip, Priority::Normal, Priority::Normal]);
    let storage = Arc::new(Storage::new(&torrent.info, out.path(), &selection).unwrap());
    let session = Arc::new(rtrnt::download::Session {
        torrent,
        info_hash,
        peer_id: rtrnt::peer::generate_peer_id(),
        storage,
        seed: Default::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: indicatif::MultiProgress::new(),
        resume: None,
        stats: Arc::new(rtrnt::stats::TorrentStats::new(
            info_hash,
            "test",
            std::path::Path::new("."),
        )),
    });
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &[seeder_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    let dir = out.path().join("multi").join("dir");
    let a = std::fs::read(dir.join("a.bin")).unwrap();
    assert_eq!(a.len(), 40_000);
    let piece = PIECE_LENGTH as usize;
    assert!(
        a[..piece].iter().all(|&byte| byte == 0),
        "piece 0 was fetched"
    );
    assert_eq!(a[piece..], data[piece..40_000]);
    let b = std::fs::read(dir.join("b.bin")).unwrap();
    assert_eq!(b, data[40_000..]);
}

/// Waits for the run that a command just started to end: the manager is
/// not idle while it goes.
async fn next_run_ends(idle: &mut watch::Receiver<bool>) {
    tokio::time::timeout(Duration::from_secs(10), idle.wait_for(|&idle| !idle))
        .await
        .expect("the run did not start")
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), idle.wait_for(|&idle| idle))
        .await
        .expect("the run did not end")
        .unwrap();
}

/// `--select-file 1,3` leaves the middle file out and the run Done; a
/// resume keeps the choice, with nothing fetched; choosing the file later
/// (on the ended torrent) and running it again fetches just that piece.
#[tokio::test]
async fn the_choice_of_files_is_kept_across_runs_and_can_change() {
    init_tracing();
    let (data, mut metainfo) = three_files();
    let info_hash = metainfo.info_hash;
    let num_pieces = metainfo.torrent.num_pieces();
    let data = Arc::new(data);

    // Serves every connection, and notes which pieces were asked for.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let requested = Arc::new(Mutex::new(Vec::new()));
    {
        let (data, requested) = (Arc::clone(&data), Arc::clone(&requested));
        tokio::spawn(async move {
            loop {
                let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
                let (data, requested) = (Arc::clone(&data), Arc::clone(&requested));
                tokio::spawn(async move {
                    send(&mut socket, PeerMessage::Unchoke).await;
                    while let Some(message) = read_message(&mut socket).await {
                        if let PeerMessage::Request(BlockRef {
                            index,
                            begin,
                            length,
                        }) = message
                        {
                            requested.lock().unwrap().push(index);
                            send(&mut socket, piece(&data, index, begin, length)).await;
                        }
                    }
                });
            }
        });
    }
    let (tracker_url, _queries) = mock_tracker(vec![seeder_addr]).await;
    metainfo.torrent.announce = Some(tracker_url);

    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let path = out.path().join("sel.torrent");
    std::fs::write(&path, serde_bencode::to_bytes(&metainfo.torrent).unwrap()).unwrap();

    let client = Arc::new(client(resume_config(out.path(), state.path())));
    let mut manager = Manager::new(Arc::clone(&client), pending::<()>);
    let mut idle = manager.idle();
    let options = StartOptions {
        picks: Some(FilePicks::parse("1,3").unwrap()),
        ..StartOptions::default()
    };
    manager.start(vec![Source::File(path)], options).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    let run = tokio::spawn(manager.run(commands_rx));
    tokio::time::timeout(Duration::from_secs(30), idle.wait_for(|&idle| idle))
        .await
        .expect("the first run did not end")
        .unwrap();

    let root = out.path().join("sel");
    let len = PIECE_LENGTH as usize;
    let state_of = || client.stats().torrents()[0].state();
    assert_eq!(state_of(), TorrentState::Done);
    assert_eq!(std::fs::read(root.join("x")).unwrap(), data[..len]);
    assert_eq!(std::fs::read(root.join("z")).unwrap(), data[2 * len..]);
    assert!(!root.join("y").exists(), "the skipped file was created");
    let wanted = client.stats().torrents()[0].wanted();
    assert_eq!(wanted, Some(2 * PIECE_LENGTH));

    // A resume goes by the resume file's choice: still no middle file, and
    // nothing asked of the seed.
    let asked = requested.lock().unwrap().len();
    commands.send(Command::Resume(info_hash)).unwrap();
    next_run_ends(&mut idle).await;
    assert_eq!(state_of(), TorrentState::Done);
    assert!(!root.join("y").exists(), "the choice was forgotten");
    assert_eq!(requested.lock().unwrap().len(), asked);

    // Choosing every file, then running it again, fetches the middle one.
    commands
        .send(Command::SetPriorities {
            info_hash,
            priorities: vec![Priority::Normal; 3],
        })
        .unwrap();
    commands.send(Command::Resume(info_hash)).unwrap();
    next_run_ends(&mut idle).await;
    assert_eq!(state_of(), TorrentState::Done);
    assert_eq!(std::fs::read(root.join("y")).unwrap(), data[len..2 * len]);
    // The first run asked for pieces 0 and 2 only, the last for piece 1.
    let pieces = requested.lock().unwrap().clone();
    let (first, last) = pieces.split_at(asked);
    let distinct = |pieces: &[u32]| {
        let mut pieces = pieces.to_vec();
        pieces.sort_unstable();
        pieces.dedup();
        pieces
    };
    assert_eq!(distinct(first), [0, 2]);
    assert_eq!(distinct(last), [1]);

    drop(commands);
    let exit = tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the session did not end")
        .unwrap()
        .unwrap();
    assert_eq!(exit, ExitCode::SUCCESS);
}
