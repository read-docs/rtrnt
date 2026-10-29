use indicatif::MultiProgress;
use rtrnt::download::{DownloadConfig, Interrupted, SeedLimits, Session, download_from_peers};
use rtrnt::peer::{BlockRef, PeerMessage, generate_peer_id};
use rtrnt::resume;
use rtrnt::selection::FileSelection;
use rtrnt::stats::TorrentStats;
use rtrnt::storage::Storage;
use rtrnt::storage::verify;
use rtrnt::torrent::TorrentFile;
use rtrnt::tracker::announcer::Announcer;
use rtrnt::tracker::{AnnounceEvent, AnnounceRequest};
use std::future::pending;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::common::*;

/// Data left by an interrupted run is hashed at startup: intact pieces are
/// kept, and only the corrupt one is downloaded again.
#[tokio::test]
async fn resumes_from_data_on_disk() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    // Every file is in place, but piece 1 (which spans a.bin and b.bin) has
    // a corrupt byte in b.bin.
    let out = tempfile::tempdir().unwrap();
    let dir = out.path().join("multi/dir");
    std::fs::create_dir_all(&dir).unwrap();
    let mut offset = 0;
    for (name, len) in FILES {
        let mut contents = data[offset..offset + len].to_vec();
        if *name == "b.bin" {
            contents[0] ^= 0xff;
        }
        std::fs::write(dir.join(name), contents).unwrap();
        offset += len;
    }

    let session = session(torrent, info_hash, out.path());
    let candidates = verify::candidates(&session.torrent, &session.storage).unwrap();
    let have = verify::verify(
        &session.torrent,
        &session.storage,
        &candidates,
        verify::OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    assert_eq!(have, [true, false, true]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let (requested_tx, mut requested) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            let PeerMessage::Request(BlockRef {
                index,
                begin,
                length,
            }) = message
            else {
                continue;
            };
            requested_tx.send(index).unwrap();
            let start = index as usize * PIECE_LENGTH as usize + begin as usize;
            let block = seeder_data[start..start + length as usize].to_vec();
            send(
                &mut socket,
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                },
            )
            .await;
        }
    });

    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session,
            &have,
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
    assert_files_match(out.path(), &data);

    let mut pieces = Vec::new();
    while let Ok(index) = requested.try_recv() {
        pieces.push(index);
    }
    assert!(!pieces.is_empty());
    assert!(
        pieces.iter().all(|&index| index == 1),
        "requested {pieces:?}"
    );
}

/// A session that saves its resume file as `config` says.
fn resumable_session(
    torrent: TorrentFile,
    info_hash: [u8; 20],
    config: &DownloadConfig,
) -> Arc<Session> {
    let storage = Arc::new(
        Storage::new(&torrent.info, &config.output_dir, &FileSelection::default()).unwrap(),
    );
    Arc::new(Session {
        torrent,
        info_hash,
        peer_id: generate_peer_id(),
        storage,
        seed: SeedLimits::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: MultiProgress::new(),
        resume: resume::Target::new(config, info_hash),
        stats: Arc::new(TorrentStats::new(info_hash, "test", Path::new("."))),
    })
}

/// Sets the modification time of the file `name` of the torrent "multi".
fn set_mtime(out: &Path, name: &str, mtime: std::time::SystemTime) {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(out.join("multi/dir").join(name))
        .unwrap();
    file.set_modified(mtime).unwrap();
}

/// Flips the byte of the file `name` of the torrent "multi" at `at`, and
/// gives the file the modification time `mtime`.
fn corrupt_file(out: &Path, name: &str, at: usize, mtime: std::time::SystemTime) {
    let path = out.join("multi/dir").join(name);
    let mut contents = std::fs::read(&path).unwrap();
    contents[at] ^= 0xff;
    std::fs::write(&path, contents).unwrap();
    set_mtime(out, name, mtime);
}

fn mtime_of(out: &Path, name: &str) -> std::time::SystemTime {
    std::fs::metadata(out.join("multi/dir").join(name))
        .unwrap()
        .modified()
        .unwrap()
}

/// Downloads piece 0 of the torrent into `out` from a seeder that only has
/// that piece, and is interrupted once it is written. The resume file is
/// saved on the way out.
async fn interrupted_after_one_piece(
    data: &Arc<Vec<u8>>,
    torrent: TorrentFile,
    info_hash: [u8; 20],
    config: &DownloadConfig,
) {
    let num_pieces = torrent.num_pieces();
    let (not_interested, not_interested_rx) = oneshot::channel();
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let seeder_data = Arc::clone(data);
    tokio::spawn(async move {
        let mut socket = accept_partial(&seeder, info_hash, num_pieces, &[0]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut not_interested = Some(not_interested);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request(BlockRef {
                    index,
                    begin,
                    length,
                }) => send(&mut socket, piece(&seeder_data, index, begin, length)).await,
                PeerMessage::NotInterested => {
                    not_interested.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
    });

    let session = resumable_session(torrent, info_hash, config);
    let have = resume::initial_pieces(&session, false, &Semaphore::new(1), pending())
        .await
        .unwrap();
    assert_eq!(have, vec![false; num_pieces]);
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session,
            &have,
            &[seeder_addr],
            10,
            Vec::new(),
            None,
            async { not_interested_rx.await.unwrap() },
        ),
    )
    .await
    .expect("interrupted download did not stop")
    .expect_err("an interrupted download must fail");
    assert!(err.is::<Interrupted>(), "{err:#}");
}

/// A restart after an interrupted run goes by the resume file: the piece
/// that was done is believed without being hashed (it is corrupt here, with
/// the file's modification time put back, so only a hash would tell), and
/// only the other pieces are requested.
#[tokio::test]
async fn resumes_from_the_resume_file_without_hashing() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = resume_config(out.path(), state.path());

    interrupted_after_one_piece(&data, torrent.clone(), info_hash, &config).await;
    let before = mtime_of(out.path(), "a.bin");
    corrupt_file(out.path(), "a.bin", 0, before);

    let session = resumable_session(torrent, info_hash, &config);
    let have = resume::initial_pieces(&session, false, &Semaphore::new(1), pending())
        .await
        .unwrap();
    assert_eq!(have, [true, false, false]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let (requested_tx, mut requested) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            if let PeerMessage::Request(BlockRef {
                index,
                begin,
                length,
            }) = message
            {
                requested_tx.send(index).unwrap();
                send(&mut socket, piece(&seeder_data, index, begin, length)).await;
            }
        }
    });
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session,
            &have,
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

    let mut pieces = Vec::new();
    while let Ok(index) = requested.try_recv() {
        pieces.push(index);
    }
    assert!(!pieces.is_empty());
    assert!(
        pieces.iter().all(|&index| index != 0),
        "requested {pieces:?}"
    );
}

/// After a complete download, a file touched since is the only one hashed:
/// the pieces of the other files are believed (piece 2, in b.bin alone, is
/// corrupt here with its time put back), and `--recheck` hashes everything.
#[tokio::test]
async fn rehashes_only_the_files_changed_since_the_resume_file() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = resume_config(out.path(), state.path());
    write_files(out.path(), &data);

    // No resume file yet: everything on disk is hashed, then saved.
    let session = resumable_session(torrent.clone(), info_hash, &config);
    let have = resume::initial_pieces(&session, false, &Semaphore::new(1), pending())
        .await
        .unwrap();
    assert_eq!(have, [true; 3]);

    // a.bin (pieces 0 and 1) is rewritten, with one byte of piece 0 wrong,
    // and b.bin (pieces 1 and 2) has a wrong byte in piece 2 but looks the
    // same as before.
    let a_time = mtime_of(out.path(), "a.bin") + Duration::from_secs(5);
    let b_time = mtime_of(out.path(), "b.bin");
    corrupt_file(out.path(), "a.bin", 0, a_time);
    corrupt_file(out.path(), "b.bin", 50_000, b_time);
    let session = resumable_session(torrent.clone(), info_hash, &config);
    let have = resume::initial_pieces(&session, false, &Semaphore::new(1), pending())
        .await
        .unwrap();
    assert_eq!(have, [false, true, true]);

    let session = resumable_session(torrent, info_hash, &config);
    let have = resume::initial_pieces(&session, true, &Semaphore::new(1), pending())
        .await
        .unwrap();
    assert_eq!(have, [false, true, false]);
}

/// Interrupted after one piece: the download stops with `Interrupted`, the
/// piece stays on disk, and the tracker hears `stopped` (never `completed`)
/// with that piece counted.
#[tokio::test]
async fn interrupted_download_keeps_pieces_and_tells_the_tracker() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let total = data.len() as u64;

    // Only has piece 0. Once it is written we lose interest, and that is
    // when the download gets interrupted.
    let (not_interested, not_interested_rx) = oneshot::channel();
    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_partial(&seeder, info_hash, num_pieces, &[0]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut not_interested = Some(not_interested);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request(BlockRef {
                    index,
                    begin,
                    length,
                }) => {
                    let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                    let block = seeder_data[start..start + length as usize].to_vec();
                    send(
                        &mut socket,
                        PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        },
                    )
                    .await;
                }
                PeerMessage::NotInterested => {
                    not_interested.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
    });

    let (tracker_url, mut queries) = mock_tracker(vec![seeder_addr]).await;
    let request = AnnounceRequest {
        info_hash,
        peer_id: generate_peer_id(),
        port: 6881,
        key: 0,
        uploaded: 0,
        downloaded: 0,
        left: total,
        event: AnnounceEvent::Started,
    };
    let (announcer, peers) = Announcer::start(vec![vec![tracker_url]], request)
        .await
        .unwrap();

    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());
    let shutdown = async {
        not_interested_rx.await.unwrap();
    };
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            Arc::clone(&session),
            &vec![false; num_pieces],
            &peers,
            10,
            Vec::new(),
            Some(announcer),
            shutdown,
        ),
    )
    .await
    .expect("interrupted download did not stop")
    .expect_err("an interrupted download must fail");
    let interrupted = err
        .downcast_ref::<Interrupted>()
        .unwrap_or_else(|| panic!("not an interruption: {err:#}"));
    assert_eq!((interrupted.remaining, interrupted.pieces), (2, num_pieces));

    let all: Vec<usize> = (0..num_pieces).collect();
    let have = verify::verify(
        &session.torrent,
        &session.storage,
        &all,
        verify::OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    assert_eq!(have, [true, false, false]);

    let piece = PIECE_LENGTH.to_string();
    let left = (total - PIECE_LENGTH).to_string();
    let mut announces = Vec::new();
    while let Ok(query) = queries.try_recv() {
        let param = |key| query_param(&query, key).unwrap_or("").to_string();
        announces.push([param("event"), param("downloaded"), param("left")]);
    }
    assert_eq!(
        announces,
        [
            ["started".into(), "0".into(), total.to_string()],
            ["stopped".into(), piece, left],
        ]
    );
}
