use super::*;
use crate::storage::Storage;
use crate::storage::verify::{self, OnReadError};
use crate::torrent;
use std::sync::atomic::AtomicBool;

const KIB: u64 = 1024;

/// `data/` with files that start and end mid-piece, a nested one, an empty
/// one and (on Unix) a symlink, for 16 KiB pieces.
fn make_tree(root: &Path) -> PathBuf {
    let dir = root.join("data");
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    let bytes = |n: usize, seed: u8| -> Vec<u8> {
        (0..n)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    };
    std::fs::write(dir.join("b.bin"), bytes(20_000, 2)).unwrap();
    std::fs::write(dir.join("a.bin"), bytes(5_000, 1)).unwrap();
    std::fs::write(dir.join("empty"), b"").unwrap();
    std::fs::write(dir.join("sub").join("c.bin"), bytes(30_000, 3)).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(dir.join("a.bin"), dir.join("link")).unwrap();
    dir
}

fn options(path: &Path) -> Options {
    Options {
        path: path.to_path_buf(),
        piece_length: Some(16 * KIB),
        created_at: Some(1_700_000_000),
        ..Options::default()
    }
}

fn make(options: &Options) -> Created {
    build(options, &|_| {}, &|_| {}).unwrap()
}

/// Whether every piece of `t`, as the files under `root` hold it, is intact.
fn all_pieces_verify(t: &torrent::TorrentFile, root: &Path) -> bool {
    let storage = Storage::open_existing(&t.info, root).unwrap();
    let pieces = verify::present(t, &storage).unwrap();
    let have = verify::verify(
        t,
        &storage,
        &pieces,
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    have.len() == t.num_pieces() && have.iter().all(|&ok| ok)
}

#[test]
fn piece_length_aims_for_1000_to_2000_pieces() {
    assert_eq!(choose_piece_length(0), 16 * KIB);
    assert_eq!(choose_piece_length(1), 16 * KIB);
    // Small data gets the smallest, however few pieces that makes.
    assert_eq!(choose_piece_length(10 * 1024 * KIB), 16 * KIB);
    for exp in 0..40 {
        for total in [(1u64 << exp) + 1, 3 << exp, 5 << exp] {
            let length = choose_piece_length(total);
            assert!(length.is_power_of_two());
            assert!((16 * KIB..=16 * 1024 * KIB).contains(&length), "{total}");
            let pieces = total.div_ceil(length);
            if length < 16 * 1024 * KIB {
                assert!(pieces <= 2000, "{total}: {pieces}");
            }
            if length > 16 * KIB {
                assert!(pieces > 1000, "{total}: {pieces}");
            }
        }
    }
    // 1 GiB: 1024 pieces of 1 MiB. Huge data stops at 16 MiB.
    assert_eq!(choose_piece_length(1024 * 1024 * KIB), 1024 * KIB);
    assert_eq!(choose_piece_length(u64::MAX), 16 * 1024 * KIB);
}

#[test]
fn parses_piece_lengths() {
    assert_eq!(parse_piece_length("16384"), Ok(16 * KIB));
    assert_eq!(parse_piece_length("256k"), Ok(256 * KIB));
    assert_eq!(parse_piece_length("4M"), Ok(4 * 1024 * KIB));
    for bad in [
        "",
        "x",
        "8k",
        "20k",
        "128m",
        "0",
        "-16k",
        "18446744073709551615m",
    ] {
        assert!(parse_piece_length(bad).is_err(), "accepted {bad:?}");
    }
}

#[test]
fn a_directory_makes_a_torrent_that_parses_and_verifies() {
    let root = tempfile::tempdir().unwrap();
    let dir = make_tree(root.path());
    let mut options = options(&dir);
    options.trackers = vec![
        "udp://tracker.example:6969".into(),
        "http://b.example/a".into(),
    ];
    options.web_seeds = vec!["https://seed.example/files/".into()];
    options.comment = Some("hello".into());
    options.private = true;
    options.source = Some("TEST".into());
    let created = make(&options);

    let metainfo = torrent::parse_metainfo(&created.data).unwrap();
    assert_eq!(metainfo.info_hash, created.info_hash);
    let t = &metainfo.torrent;
    assert_eq!(t.info.name, "data");
    assert_eq!(t.info.piece_length, 16 * KIB);
    assert_eq!(t.length(), 55_000);
    assert_eq!(created.size, 55_000);
    assert_eq!(t.num_pieces(), 4);
    assert_eq!(created.pieces, 4);
    assert!(t.is_private());
    assert_eq!(
        t.tracker_tiers(),
        [
            vec!["udp://tracker.example:6969"],
            vec!["http://b.example/a"]
        ]
    );
    // Sorted by path components; the empty file is listed.
    let files: Vec<_> = t
        .info
        .files
        .as_ref()
        .unwrap()
        .iter()
        .map(|f| (f.path.join("/"), f.length))
        .collect();
    assert_eq!(
        files,
        [
            ("a.bin".to_string(), 5_000),
            ("b.bin".to_string(), 20_000),
            ("empty".to_string(), 0),
            ("sub/c.bin".to_string(), 30_000),
        ]
    );
    #[cfg(unix)]
    assert_eq!(created.skipped, [dir.canonicalize().unwrap().join("link")]);

    let extras = crate::info::Extras::parse(&created.data);
    assert_eq!(extras.web_seeds, ["https://seed.example/files/"]);
    assert_eq!(extras.comment.as_deref(), Some("hello"));
    assert_eq!(extras.source.as_deref(), Some("TEST"));
    assert_eq!(extras.creation_date, Some(1_700_000_000));

    // The pieces are those of the data as the storage reads it.
    assert!(all_pieces_verify(t, root.path()));
}

#[test]
fn a_single_file_makes_a_torrent_that_verifies() {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("one.bin");
    std::fs::write(&file, vec![7u8; 40_000]).unwrap();
    let created = make(&options(&file));
    let metainfo = torrent::parse_metainfo(&created.data).unwrap();
    let t = &metainfo.torrent;
    assert_eq!(t.info.name, "one.bin");
    assert_eq!(t.info.length, Some(40_000));
    assert!(t.info.files.is_none() && !t.is_private());
    assert!(created.skipped.is_empty());
    assert_eq!(t.num_pieces(), 3);
    assert!(all_pieces_verify(t, root.path()));
}

#[test]
fn a_changed_file_fails_verification() {
    let root = tempfile::tempdir().unwrap();
    let dir = make_tree(root.path());
    let created = make(&options(&dir));
    let t = torrent::parse_metainfo(&created.data).unwrap().torrent;
    std::fs::write(dir.join("b.bin"), vec![0u8; 20_000]).unwrap();
    assert!(!all_pieces_verify(&t, root.path()));
}

#[test]
fn the_info_hash_is_stable() {
    let root = tempfile::tempdir().unwrap();
    let dir = make_tree(root.path());
    let first = make(&options(&dir));
    let second = make(&options(&dir));
    assert_eq!(first.info_hash, second.info_hash);
    assert_eq!(first.data, second.data);
    // Neither the trackers, comment nor date are in the info dictionary,
    // but the private flag, source and piece length are.
    let mut other = options(&dir);
    other.trackers = vec!["udp://t:1".into()];
    other.comment = Some("x".into());
    other.created_at = None;
    assert_eq!(make(&other).info_hash, first.info_hash);
    for change in [
        |o: &mut Options| o.private = true,
        |o: &mut Options| o.source = Some("S".into()),
        |o: &mut Options| o.piece_length = Some(32 * KIB),
    ] {
        let mut changed = options(&dir);
        change(&mut changed);
        assert_ne!(make(&changed).info_hash, first.info_hash);
    }
}

#[test]
fn progress_counts_every_byte_once() {
    let root = tempfile::tempdir().unwrap();
    let dir = make_tree(root.path());
    let (total, hashed) = (AtomicUsize::new(0), AtomicUsize::new(0));
    build(
        &options(&dir),
        &|n| total.store(n as usize, Ordering::Relaxed),
        &|n| {
            hashed.fetch_add(n as usize, Ordering::Relaxed);
        },
    )
    .unwrap();
    assert_eq!(total.load(Ordering::Relaxed), 55_000);
    assert_eq!(hashed.load(Ordering::Relaxed), 55_000);
}

#[test]
fn refuses_what_cannot_make_a_torrent() {
    let root = tempfile::tempdir().unwrap();
    let empty = root.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    let fails = |options: &Options| build(options, &|_| {}, &|_| {}).is_err();
    assert!(fails(&options(&empty)));
    assert!(fails(&options(&root.path().join("missing"))));

    let file = root.path().join("f");
    std::fs::write(&file, b"data").unwrap();
    let mut bad = options(&file);
    bad.piece_length = Some(20_000);
    assert!(fails(&bad));
    let mut bad = options(&file);
    bad.trackers = vec!["not a url".into()];
    assert!(fails(&bad));
    let mut bad = options(&file);
    bad.web_seeds = vec!["udp://seed".into()];
    assert!(fails(&bad));
}
