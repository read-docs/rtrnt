use super::*;
use crate::selection::FileSelection;
use crate::torrent::{FileInfo, Info};
use serde_bytes::ByteBuf;
use std::sync::atomic::AtomicU64;

const PIECE_LENGTH: usize = 4;

/// A torrent of two files, "a" (6 bytes) and "b" (5 bytes), over `data`.
fn torrent(data: &[u8]) -> TorrentFile {
    let pieces: Vec<u8> = data
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

#[test]
fn finds_intact_pieces_and_skips_fresh_files() {
    let data = b"abcdefghijk";
    let torrent = torrent(data);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    // "a" is complete from an earlier run except for a corrupt byte in
    // piece 1, which spans both files; "b" does not exist yet.
    std::fs::write(root.join("a"), b"abcdeX").unwrap();

    let storage = Storage::new(&torrent.info, dir.path(), &FileSelection::default()).unwrap();
    // Piece 2 lies entirely in the new, empty "b".
    let candidates = candidates(&torrent, &storage).unwrap();
    assert_eq!(candidates, [0, 1]);

    let checked = AtomicU64::new(0);
    let have = verify(
        &torrent,
        &storage,
        &candidates,
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|len| {
            checked.fetch_add(len, Ordering::Relaxed);
        },
    )
    .unwrap();
    assert_eq!(have, [true, false, false]);
    assert_eq!(checked.load(Ordering::Relaxed), 8);
}

#[test]
fn complete_data_verifies_every_piece() {
    let data = b"abcdefghijk";
    let torrent = torrent(data);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), &data[..6]).unwrap();
    std::fs::write(root.join("b"), &data[6..]).unwrap();

    let storage = Storage::new(&torrent.info, dir.path(), &FileSelection::default()).unwrap();
    let candidates = candidates(&torrent, &storage).unwrap();
    assert_eq!(candidates, [0, 1, 2]);
    let have = verify(
        &torrent,
        &storage,
        &candidates,
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    assert_eq!(have, [true, true, true]);
}

#[test]
fn nothing_to_check_in_a_new_download() {
    let torrent = torrent(b"abcdefghijk");
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::new(&torrent.info, dir.path(), &FileSelection::default()).unwrap();
    assert!(candidates(&torrent, &storage).unwrap().is_empty());
    let have = verify(
        &torrent,
        &storage,
        &[],
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    assert_eq!(have, [false, false, false]);
}

#[test]
fn a_cancelled_check_hashes_nothing_more_and_fails() {
    let data = b"abcdefghijk";
    let torrent = torrent(data);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), &data[..6]).unwrap();
    std::fs::write(root.join("b"), &data[6..]).unwrap();
    let storage = Storage::new(&torrent.info, dir.path(), &FileSelection::default()).unwrap();

    // Cancelled before it starts: no piece is read.
    let checked = AtomicU64::new(0);
    let on_checked = |len| {
        checked.fetch_add(len, Ordering::Relaxed);
    };
    let result = verify(
        &torrent,
        &storage,
        &[0, 1, 2],
        OnReadError::Fail,
        &AtomicBool::new(true),
        &on_checked,
    );
    assert!(result.is_err());
    assert_eq!(checked.load(Ordering::Relaxed), 0);

    // Cancelled while hashing: the pieces already hashed are not reported.
    let cancel = AtomicBool::new(false);
    let result = verify(
        &torrent,
        &storage,
        &[0, 1, 2],
        OnReadError::Fail,
        &cancel,
        &|_| {
            cancel.store(true, Ordering::Relaxed);
        },
    );
    assert!(result.is_err());
}

/// Writes the files, checks them read-only and returns the report.
fn check(a: Option<&[u8]>, b: Option<&[u8]>) -> (Report, PathBuf) {
    let torrent = torrent(b"abcdefghijk");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    for (name, contents) in [("a", a), ("b", b)] {
        if let Some(contents) = contents {
            std::fs::write(root.join(name), contents).unwrap();
        }
    }
    let storage = Storage::open_existing(&torrent.info, dir.path()).unwrap();
    let pieces = present(&torrent, &storage).unwrap();
    let have = verify(
        &torrent,
        &storage,
        &pieces,
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    )
    .unwrap();
    (Report::new(&torrent, &storage, &have), root)
}

#[test]
fn report_on_a_complete_download() {
    let (report, _) = check(Some(b"abcdef"), Some(b"ghijk"));
    assert!(report.is_complete());
    assert_eq!(report.to_string(), "3 of 3 pieces OK\n");
}

#[test]
fn report_on_a_missing_file() {
    let (report, root) = check(Some(b"abcdef"), None);
    assert!(!report.is_complete());
    // Piece 1 spans both files, so "a" cannot be fully verified either.
    assert_eq!(
        report.problems,
        [
            (root.join("a"), FileProblem::BadPieces(1)),
            (root.join("b"), FileProblem::Missing),
        ]
    );
    assert_eq!(report.good_pieces, 1);
}

#[test]
fn report_on_corrupt_and_resized_files() {
    let (report, root) = check(Some(b"abcdeX"), Some(b"ghijk!"));
    assert_eq!(
        report.problems,
        [
            (root.join("a"), FileProblem::BadPieces(1)),
            (
                root.join("b"),
                FileProblem::WrongSize {
                    expected: 5,
                    found: 6
                }
            ),
        ]
    );
    assert_eq!(report.good_pieces, 2);
    let text = report.to_string();
    assert!(text.starts_with("2 of 3 pieces OK\n"), "{text}");
    assert!(text.contains("corrupt     "), "{text}");
    assert!(text.contains(": 6 bytes, expected 5\n"), "{text}");
}

#[test]
fn pieces_in_files_that_were_never_created_are_not_candidates() {
    use crate::selection::Priority::Skip;
    let data = b"abcdefghijk";
    let torrent = torrent(data);
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), b"abcdef").unwrap();

    let all = Storage::new(&torrent.info, dir.path(), &FileSelection::default()).unwrap();
    assert_eq!(candidates(&torrent, &all).unwrap(), [0, 1]);
    // With both files skipped, "b" is not made, so piece 1 (which has data
    // of "a" and of "b") could not be read.
    std::fs::remove_file(root.join("b")).unwrap();
    let skipped = Storage::new(
        &torrent.info,
        dir.path(),
        &FileSelection::new(vec![Skip, Skip]),
    )
    .unwrap();
    assert!(!skipped.files()[1].exists());
    assert_eq!(candidates(&torrent, &skipped).unwrap(), [0]);
}

/// Two intact files of which "a" is then cut short, so reading piece 1 hits
/// the end of the file.
fn storage_with_unreadable_piece(dir: &std::path::Path) -> (TorrentFile, Storage) {
    let data = b"abcdefghijk";
    let torrent = torrent(data);
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), &data[..6]).unwrap();
    std::fs::write(root.join("b"), &data[6..]).unwrap();
    let storage = Storage::new(&torrent.info, dir, &FileSelection::default()).unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(root.join("a"))
        .unwrap();
    file.set_len(5).unwrap();
    (torrent, storage)
}

#[test]
fn an_unreadable_piece_can_count_as_missing() {
    let dir = tempfile::tempdir().unwrap();
    let (torrent, storage) = storage_with_unreadable_piece(dir.path());
    let checked = AtomicU64::new(0);
    let have = verify(
        &torrent,
        &storage,
        &[0, 1, 2],
        OnReadError::Missing,
        &AtomicBool::new(false),
        &|len| {
            checked.fetch_add(len, Ordering::Relaxed);
        },
    )
    .unwrap();
    assert_eq!(have, [true, false, true]);
    assert_eq!(checked.load(Ordering::Relaxed), 11);
}

#[test]
fn an_unreadable_piece_can_fail_the_check() {
    let dir = tempfile::tempdir().unwrap();
    let (torrent, storage) = storage_with_unreadable_piece(dir.path());
    let result = verify(
        &torrent,
        &storage,
        &[0, 1, 2],
        OnReadError::Fail,
        &AtomicBool::new(false),
        &|_| {},
    );
    assert!(result.is_err());
}
