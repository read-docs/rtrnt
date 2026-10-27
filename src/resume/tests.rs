use super::*;
use crate::torrent::{FileInfo, Info};
use indicatif::MultiProgress;
use sha1::Digest;

const PIECE_LENGTH: usize = 4;
const DATA: &[u8] = b"abcdefghijk";

/// A torrent of two files, "a" (6 bytes) and "b" (5 bytes), over `DATA`.
fn torrent() -> TorrentFile {
    let pieces: Vec<u8> = DATA
        .chunks(PIECE_LENGTH)
        .flat_map(|chunk| sha1::Sha1::digest(chunk).to_vec())
        .collect();
    TorrentFile {
        announce: None,
        announce_list: None,
        info: Info {
            name: "root".into(),
            piece_length: PIECE_LENGTH as u64,
            pieces: ByteBuf::from(pieces),
            private: None,
            length: None,
            files: Some(vec![
                FileInfo {
                    path: vec!["a".into()],
                    length: 6,
                },
                FileInfo {
                    path: vec!["b".into()],
                    length: 5,
                },
            ]),
        },
    }
}

fn config(output: &Path, resume: &Path) -> DownloadConfig {
    DownloadConfig {
        output_dir: output.to_path_buf(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: Default::default(),
        dht: false,
        max_download_rate: 0,
        max_upload_rate: 0,
        resume_dir: Some(resume.to_path_buf()),
        recheck: false,
    }
}

/// A session over the files in `config.output_dir`.
fn session(config: &DownloadConfig) -> Arc<Session> {
    let torrent = torrent();
    let storage = Arc::new(Storage::new(&torrent.info, &config.output_dir).unwrap());
    Arc::new(Session {
        torrent,
        info_hash: [7; 20],
        peer_id: [0; 20],
        storage,
        seed: Default::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: MultiProgress::new(),
        resume: Target::new(config, [7; 20]),
        stats: Arc::new(crate::stats::TorrentStats::new(
            [7; 20],
            "t",
            Path::new("."),
        )),
    })
}

fn write_data(output: &Path) {
    let root = output.join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), &DATA[..6]).unwrap();
    std::fs::write(root.join("b"), &DATA[6..]).unwrap();
}

/// Changes the byte of `file` at `at` in the torrent's directory, giving
/// the file the modification time `mtime`.
fn corrupt(output: &Path, file: &str, at: usize, mtime: std::time::SystemTime) {
    let path = output.join("root").join(file);
    let mut contents = std::fs::read(&path).unwrap();
    contents[at] ^= 0xff;
    std::fs::write(&path, contents).unwrap();
    let handle = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    handle.set_modified(mtime).unwrap();
}

fn modified(output: &Path, file: &str) -> std::time::SystemTime {
    std::fs::metadata(output.join("root").join(file))
        .unwrap()
        .modified()
        .unwrap()
}

fn target() -> Target {
    Target {
        path: "x.resume".into(),
        info_hash: [7; 20],
        output_dir: "/out".into(),
    }
}

fn saved() -> Saved {
    Saved {
        version: VERSION,
        info_hash: ByteBuf::from(vec![7; 20]),
        output_dir: ByteBuf::from(b"/out".to_vec()),
        pieces: ByteBuf::from(pack(&[true, false, true])),
        files: vec![(6, 1_000), (5, 2_000)],
    }
}

#[test]
fn packs_the_first_piece_into_the_high_bit() {
    let have: Vec<bool> = (0..10).map(|i| i == 0 || i == 9).collect();
    assert_eq!(pack(&have), [0b1000_0000, 0b0100_0000]);
    assert!(is_set(&pack(&have), 9));
    assert!(!is_set(&pack(&have), 8));
    assert!(!is_set(&pack(&have), 100));
}

#[test]
fn round_trips_through_bencode() {
    let encoded = serde_bencode::to_bytes(&saved()).unwrap();
    // Keys in sorted order, as bencode wants.
    assert!(encoded.starts_with(b"d5:filesl"), "{:?}", encoded);
    let parsed = parse(&encoded, &target(), 2, 3).unwrap();
    assert_eq!(parsed, saved());
}

#[test]
fn ignores_a_file_that_does_not_match() {
    let encoded = serde_bencode::to_bytes(&saved()).unwrap();
    let mut other_hash = target();
    other_hash.info_hash = [8; 20];
    let mut other_dir = target();
    other_dir.output_dir = "/elsewhere".into();
    assert!(parse(&encoded, &other_hash, 2, 3).is_err());
    assert!(parse(&encoded, &other_dir, 2, 3).is_err());
    assert!(parse(&encoded, &target(), 3, 3).is_err());
    assert!(parse(&encoded, &target(), 2, 9).is_err());
    let mut future = saved();
    future.version = VERSION + 1;
    let encoded = serde_bencode::to_bytes(&future).unwrap();
    assert!(parse(&encoded, &target(), 2, 3).is_err());
}

#[test]
fn ignores_a_truncated_or_garbage_file() {
    let encoded = serde_bencode::to_bytes(&saved()).unwrap();
    for len in [0, 1, encoded.len() / 2, encoded.len() - 1] {
        assert!(parse(&encoded[..len], &target(), 2, 3).is_err());
    }
    for garbage in [&b"not bencode"[..], b"i42e", b"le", b"d3:foo3:bare"] {
        assert!(parse(garbage, &target(), 2, 3).is_err());
    }
}

#[test]
fn only_unchanged_files_are_clean() {
    let dir = tempfile::tempdir().unwrap();
    let torrent = torrent();
    let storage = Storage::new(&torrent.info, dir.path()).unwrap();
    let files = storage.files();
    let saved = [(6, 1_000), (5, 2_000)];
    assert_eq!(
        clean_files(files, &saved, &[(6, 1_000), (5, 2_001)]),
        [true, false]
    );
    // The size must be the torrent's, and the time must be readable.
    assert_eq!(
        clean_files(files, &saved, &[(7, 1_000), (5, 2_000)]),
        [false, true]
    );
    let unreadable = [(0, -1), (5, 2_000)];
    assert_eq!(clean_files(files, &unreadable, &unreadable), [false, true]);
}

#[test]
fn hashes_only_candidates_that_touch_a_dirty_file() {
    let dir = tempfile::tempdir().unwrap();
    let torrent = torrent();
    let storage = Storage::new(&torrent.info, dir.path()).unwrap();
    let files = storage.files();
    // Pieces 0 and 1 are in "a" (1 also in "b"), and 2 is in "b".
    let all_done = pack(&[true, true, true]);
    let candidates = [0, 1, 2];

    let (have, to_hash) = split(&torrent, files, &[true, true], &all_done, &candidates).unwrap();
    assert_eq!((have, to_hash), (vec![true; 3], vec![]));

    // "a" changed: its pieces are hashed, including the one it shares.
    let (have, to_hash) = split(&torrent, files, &[false, true], &all_done, &candidates).unwrap();
    assert_eq!((have, to_hash), (vec![false, false, true], vec![0, 1]));

    // "b" changed: piece 0 is still trusted.
    let (have, to_hash) = split(&torrent, files, &[true, false], &all_done, &candidates).unwrap();
    assert_eq!((have, to_hash), (vec![true, false, false], vec![1, 2]));

    // Clean files only give the pieces the resume file lists, and a
    // dirty file's pieces that held no earlier data are not hashed.
    let some = pack(&[true, false, false]);
    let (have, to_hash) = split(&torrent, files, &[true, true], &some, &candidates).unwrap();
    assert_eq!((have, to_hash), (vec![true, false, false], vec![]));
    let (have, to_hash) = split(&torrent, files, &[true, false], &some, &[0, 1]).unwrap();
    assert_eq!((have, to_hash), (vec![true, false, false], vec![1]));
}

#[tokio::test]
async fn trusts_a_resume_file_until_a_file_changes() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = config(out.path(), state.path());
    write_data(out.path());

    // No resume file yet: everything on disk is hashed, then saved.
    let have = initial_pieces(
        &session(&config),
        &config,
        &Semaphore::new(1),
        std::future::pending(),
    )
    .await
    .unwrap();
    assert_eq!(have, [true; 3]);
    let resume_file = state.path().join(format!("{}.resume", hex(&[7; 20])));
    assert!(resume_file.is_file());

    // Piece 2 is wrong now, but "b" looks unchanged, so it is believed.
    let b_time = modified(out.path(), "b");
    corrupt(out.path(), "b", 4, b_time);
    let have = initial_pieces(
        &session(&config),
        &config,
        &Semaphore::new(1),
        std::future::pending(),
    )
    .await
    .unwrap();
    assert_eq!(have, [true; 3]);

    // "a" is touched (and piece 0 is wrong): its pieces are hashed, but
    // piece 2 is still believed.
    corrupt(
        out.path(),
        "a",
        0,
        modified(out.path(), "a") + std::time::Duration::from_secs(5),
    );
    let have = initial_pieces(
        &session(&config),
        &config,
        &Semaphore::new(1),
        std::future::pending(),
    )
    .await
    .unwrap();
    assert_eq!(have, [false, true, true]);

    // `--recheck` ignores the resume file and finds piece 2 wrong too.
    let recheck = DownloadConfig {
        recheck: true,
        ..config
    };
    let have = initial_pieces(
        &session(&recheck),
        &recheck,
        &Semaphore::new(1),
        std::future::pending(),
    )
    .await
    .unwrap();
    assert_eq!(have, [false, true, false]);
}

#[tokio::test]
async fn falls_back_to_hashing_without_a_usable_resume_file() {
    let out = tempfile::tempdir().unwrap();
    let state = tempfile::tempdir().unwrap();
    let config = config(out.path(), state.path());
    write_data(out.path());
    let resume_file = state.path().join(format!("{}.resume", hex(&[7; 20])));

    for contents in [&b"garbage"[..], b""] {
        std::fs::write(&resume_file, contents).unwrap();
        let have = initial_pieces(
            &session(&config),
            &config,
            &Semaphore::new(1),
            std::future::pending(),
        )
        .await
        .unwrap();
        assert_eq!(have, [true; 3]);
        // The unusable file was replaced by a good one.
        let data = std::fs::read(&resume_file).unwrap();
        assert!(parse(&data, session(&config).resume.as_ref().unwrap(), 2, 3).is_ok());
    }
}

#[test]
fn state_dir_prefers_xdg_state_home() {
    let home = Path::new("/home/me");
    assert_eq!(
        default_dir(Some("/state".into()), Some(home)),
        Some(PathBuf::from("/state/rtrnt/resume"))
    );
    for unusable in [None, Some("".into()), Some("relative".into())] {
        assert_eq!(
            default_dir(unusable, Some(home)),
            Some(PathBuf::from("/home/me/.local/state/rtrnt/resume"))
        );
    }
    assert_eq!(default_dir(None, None), None);
}

#[test]
fn candidates_are_not_needed_to_trust_a_resume_file() {
    // Fresh files hold no earlier data, so nothing is a candidate, yet a
    // resume file can still vouch for a piece in files it saw complete.
    let dir = tempfile::tempdir().unwrap();
    let torrent = torrent();
    let storage = Storage::new(&torrent.info, dir.path()).unwrap();
    assert!(verify::candidates(&torrent, &storage).unwrap().is_empty());
    let (have, to_hash) = split(
        &torrent,
        storage.files(),
        &[true, true],
        &pack(&[false, true, false]),
        &[],
    )
    .unwrap();
    assert_eq!((have, to_hash), (vec![false, true, false], vec![]));
}
