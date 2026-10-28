use super::*;
use crate::selection::FileSelection;
use crate::torrent::FileInfo;
use serde_bytes::ByteBuf;
use sha1::Digest;

fn multi_file_info(files: &[(&[&str], u64)]) -> Info {
    Info {
        name: "root".into(),
        piece_length: 4,
        pieces: ByteBuf::new(),
        private: None,
        length: None,
        files: Some(
            files
                .iter()
                .map(|(path, length)| FileInfo {
                    path: path.iter().map(|s| s.to_string()).collect(),
                    length: *length,
                })
                .collect(),
        ),
    }
}

#[test]
fn piece_spanning_files() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 3), (&["sub", "empty"], 0), (&["sub", "b"], 6)]);
    let storage = Storage::new(&info, dir.path(), &FileSelection::default()).unwrap();

    storage.write_at(0, b"abcd").unwrap();
    storage.write_at(4, b"efgh").unwrap();
    storage.write_at(8, b"i").unwrap();

    let root = dir.path().join("root");
    assert_eq!(std::fs::read(root.join("a")).unwrap(), b"abc");
    assert_eq!(std::fs::read(root.join("sub/empty")).unwrap(), b"");
    assert_eq!(std::fs::read(root.join("sub/b")).unwrap(), b"defghi");
    assert_eq!(storage.read_at(2, 5).unwrap(), b"cdefg");
    assert!(storage.write_at(8, b"jk").is_err());
    assert!(storage.read_at(u64::MAX, 1).is_err());
}

#[tokio::test]
async fn reads_blocks_by_piece_and_offset() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 3), (&["b"], 6)]);
    let storage = Arc::new(Storage::new(&info, dir.path(), &FileSelection::default()).unwrap());
    storage.write_at(0, b"abcdefghi").unwrap();
    // Piece 1 is "efgh", and its middle spans no file boundary.
    assert_eq!(storage.read_block(1, 1, 2).await.unwrap(), b"fg");
    // Piece 0 spans both files; piece 2 is the short last one.
    assert_eq!(storage.read_block(0, 2, 2).await.unwrap(), b"cd");
    assert_eq!(storage.read_block(2, 0, 1).await.unwrap(), b"i");
    assert!(storage.read_block(2, 0, 2).await.is_err());
}

#[tokio::test]
async fn writes_a_piece_only_if_it_matches_its_hash() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 3), (&["b"], 6)]);
    let storage = Arc::new(Storage::new(&info, dir.path(), &FileSelection::default()).unwrap());
    let hash: [u8; 20] = sha1::Sha1::digest(b"abcd").into();

    assert!(
        !storage
            .verify_and_write(0, b"abcx".to_vec(), hash)
            .await
            .unwrap()
    );
    assert_eq!(storage.read_at(0, 4).unwrap(), [0; 4]);

    assert!(
        storage
            .verify_and_write(0, b"abcd".to_vec(), hash)
            .await
            .unwrap()
    );
    assert_eq!(storage.read_at(0, 4).unwrap(), b"abcd");
}

#[tokio::test]
async fn a_failed_write_or_read_is_a_storage_error() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 4)]);
    let storage = Arc::new(Storage::new(&info, dir.path(), &FileSelection::default()).unwrap());
    // A directory in place of the file cannot be opened for writing.
    let path = dir.path().join("root/a");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();

    let hash: [u8; 20] = sha1::Sha1::digest(b"abcd").into();
    let err = storage
        .verify_and_write(0, b"abcd".to_vec(), hash)
        .await
        .unwrap_err();
    assert!(err.is::<StorageError>(), "{err:#}");
    assert!(format!("{err:#}").contains("failed to open"), "{err:#}");
}

#[test]
fn caches_handles_up_to_limit() {
    let dir = tempfile::tempdir().unwrap();
    let count = MAX_OPEN_FILES + 2;
    let names: Vec<String> = (0..count).map(|i| format!("f{i}")).collect();
    let paths: Vec<[&str; 1]> = names.iter().map(|n| [n.as_str()]).collect();
    let files: Vec<(&[&str], u64)> = paths.iter().map(|p| (&p[..], 1)).collect();
    let storage = Storage::new(
        &multi_file_info(&files),
        dir.path(),
        &FileSelection::default(),
    )
    .unwrap();
    let cached = |storage: &Storage| -> Vec<usize> {
        let cache = storage.handles.lock().unwrap();
        cache.open.iter().map(|(i, _)| *i).collect()
    };

    // Fill the cache, then touch file 0 again so file 1 is the oldest.
    storage.write_at(0, &[0; MAX_OPEN_FILES]).unwrap();
    let first = Arc::clone(&storage.handles.lock().unwrap().open[0].1);
    storage.write_at(0, &[0]).unwrap();
    let reused = Arc::clone(&storage.handles.lock().unwrap().open.last().unwrap().1);
    assert!(Arc::ptr_eq(&first, &reused));

    storage.write_at(MAX_OPEN_FILES as u64, &[1]).unwrap();
    let open = cached(&storage);
    assert_eq!(open.len(), MAX_OPEN_FILES);
    assert!(!open.contains(&1));
    assert!(open.contains(&0));

    // Evicted files are reopened on demand.
    let data: Vec<u8> = (0..count as u8).collect();
    storage.write_at(0, &data).unwrap();
    assert_eq!(storage.read_at(0, count).unwrap(), data);
    assert_eq!(cached(&storage).len(), MAX_OPEN_FILES);
    let root = dir.path().join("root");
    assert_eq!(std::fs::read(root.join("f1")).unwrap(), [1]);
}

#[test]
fn open_existing_reads_without_touching_the_disk() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 3), (&["b"], 6), (&["c"], 2)]);
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("a"), b"abc").unwrap();
    // "b" is cut short and "c" is missing.
    std::fs::write(root.join("b"), b"def").unwrap();

    let storage = Storage::open_existing(&info, dir.path()).unwrap();
    let found: Vec<_> = storage.files().iter().map(|f| f.found).collect();
    assert_eq!(found, [Some(3), Some(3), None]);
    assert!(storage.is_on_disk(0, 6));
    assert!(!storage.is_on_disk(0, 7));
    assert!(storage.may_hold_prior_data(5, 2));
    assert!(!storage.may_hold_prior_data(6, 5));
    assert_eq!(storage.read_at(1, 4).unwrap(), b"bcde");
    assert!(storage.read_at(9, 2).is_err());
    assert!(storage.write_at(0, b"x").is_err());

    assert_eq!(std::fs::read(root.join("a")).unwrap(), b"abc");
    assert_eq!(std::fs::read(root.join("b")).unwrap(), b"def");
    assert!(!root.join("c").exists());
}

#[test]
fn rejects_path_traversal() {
    let dir = tempfile::tempdir().unwrap();
    for bad in [&["..", "x"][..], &["/etc"], &["a/b"], &[""], &["."], &[]] {
        let info = multi_file_info(&[(bad, 1)]);
        assert!(
            Storage::new(&info, dir.path(), &FileSelection::default()).is_err(),
            "accepted {:?}",
            bad
        );
    }
    let mut info = multi_file_info(&[(&["ok"], 1)]);
    info.name = "..".into();
    assert!(Storage::new(&info, dir.path(), &FileSelection::default()).is_err());
}

#[test]
fn rejects_the_same_path_twice() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 1), (&["sub", "b"], 1), (&["a"], 2)]);
    let err = Storage::new(&info, dir.path(), &FileSelection::default()).unwrap_err();
    assert!(err.to_string().contains("twice"), "{err:#}");
    assert!(Storage::paths(&info, dir.path()).is_err());
    assert!(Storage::open_existing(&info, dir.path()).is_err());
    assert!(!dir.path().join("root").exists());

    let info = multi_file_info(&[(&["a"], 1), (&["A"], 1)]);
    // Names that differ in case are two files only where case counts.
    assert_eq!(
        Storage::new(&info, dir.path(), &FileSelection::default()).is_err(),
        cfg!(any(target_os = "macos", windows))
    );
}

#[test]
fn composed_and_decomposed_names_are_one_file() {
    // HFS+ and APFS-style file systems treat these as the same name.
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["caf\u{e9}"], 1), (&["cafe\u{301}"], 1)]);
    assert_eq!(
        Storage::new(&info, dir.path(), &FileSelection::default()).is_err(),
        cfg!(any(target_os = "macos", windows))
    );
    assert_eq!(
        fold_case(Path::new("caf\u{e9}")) == fold_case(Path::new("cafe\u{301}")),
        cfg!(any(target_os = "macos", windows))
    );
}

#[test]
fn paths_sit_under_the_absolute_output_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("new").join("out");
    let info = multi_file_info(&[(&["a"], 1), (&["sub", "b"], 1)]);
    let paths = Storage::paths(&info, &out).unwrap();
    assert!(out.is_dir());
    assert!(!out.join("root").exists());
    let root = out.canonicalize().unwrap().join("root");
    let expected = [root.join("a"), root.join("sub").join("b")];
    assert_eq!(paths, expected.map(|path| fold_case(&path)));
}

#[cfg(unix)]
#[test]
fn deleting_removes_the_files_and_the_directories_they_leave_empty() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let elsewhere = dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("kept"), b"x").unwrap();
    let info = multi_file_info(&[
        (&["a"], 1),
        (&["sub", "deep", "b"], 1),
        (&["shared", "c"], 1),
        (&["link"], 1),
        (&["linked", "d"], 1),
        (&["missing"], 1),
    ]);
    drop(Storage::new(&info, &out, &FileSelection::default()).unwrap());
    let root = out.join("root");
    std::fs::remove_file(root.join("missing")).unwrap();
    // Not the torrent's: it keeps its directory.
    std::fs::write(root.join("shared").join("other"), b"x").unwrap();
    // Links where a file and a directory of the torrent's should be.
    std::fs::remove_file(root.join("link")).unwrap();
    std::os::unix::fs::symlink(elsewhere.join("kept"), root.join("link")).unwrap();
    std::fs::remove_dir_all(root.join("linked")).unwrap();
    std::fs::write(elsewhere.join("d"), b"x").unwrap();
    std::os::unix::fs::symlink(&elsewhere, root.join("linked")).unwrap();
    // Beside the torrent, in the output dir.
    std::fs::write(out.join("beside"), b"x").unwrap();

    let failures = delete_files(&info, &out);
    let failures: Vec<String> = failures.iter().map(|e| format!("{e:#}")).collect();
    assert_eq!(failures.len(), 2, "{failures:?}");
    assert!(
        failures.iter().all(|e| e.contains("left alone")),
        "{failures:?}"
    );
    assert!(!root.join("a").exists());
    assert!(!root.join("sub").exists());
    assert!(!root.join("shared").join("c").exists());
    assert!(root.join("shared").join("other").exists());
    // The links stay, and what they point to.
    assert!(root.join("link").is_symlink());
    assert!(root.join("linked").is_symlink());
    assert_eq!(std::fs::read(elsewhere.join("kept")).unwrap(), b"x");
    assert_eq!(std::fs::read(elsewhere.join("d")).unwrap(), b"x");
    assert!(out.join("beside").exists());

    // Once nothing else is left, the torrent's directory goes too, but
    // never the output dir.
    for left in ["shared/other", "link", "linked"] {
        std::fs::remove_file(root.join(left)).unwrap();
    }
    std::fs::remove_file(out.join("beside")).unwrap();
    std::fs::write(root.join("a"), b"x").unwrap();
    assert!(delete_files(&info, &out).is_empty());
    assert!(!root.exists());
    assert!(out.is_dir());
}

/// Files of the given lengths, `piece_length` 4, with room for every piece's
/// hash.
fn selectable_info(lengths: &[u64]) -> Info {
    let mut info = multi_file_info(&lengths.iter().map(|&l| (&["x"][..], l)).collect::<Vec<_>>());
    for (i, file) in info.files.as_mut().unwrap().iter_mut().enumerate() {
        file.path = vec![format!("f{i}")];
    }
    info.pieces = ByteBuf::from(vec![
        0;
        lengths.iter().sum::<u64>().div_ceil(4) as usize * 20
    ]);
    info
}

use crate::selection::Priority::{High, Normal, Skip};

#[test]
fn a_skipped_file_no_wanted_piece_touches_is_not_created() {
    let dir = tempfile::tempdir().unwrap();
    let info = selectable_info(&[4, 4, 4]);
    let selection = FileSelection::new(vec![Normal, Skip, High]);
    let storage = Storage::new(&info, dir.path(), &selection).unwrap();
    let root = dir.path().join("root");
    assert!(root.join("f0").is_file());
    assert!(!root.join("f1").exists());
    assert_eq!(std::fs::metadata(root.join("f2")).unwrap().len(), 4);
    let exists: Vec<bool> = storage.files().iter().map(FileSpan::exists).collect();
    assert_eq!(exists, [true, false, true]);
    assert_eq!(storage.selection(), &selection);
    // Its pieces are not on disk, but the others are.
    assert!(!storage.files_exist(4, 4));
    assert!(storage.files_exist(0, 4));
    assert!(storage.files_exist(8, 4));
    // Nothing in a wanted piece reaches it.
    storage.write_at(0, b"abcd").unwrap();
    storage.write_at(8, b"efgh").unwrap();
    assert!(!root.join("f1").exists());
}

#[test]
fn a_skipped_file_sharing_a_wanted_piece_is_created_full_length() {
    let dir = tempfile::tempdir().unwrap();
    // Pieces: [a a a b] [b b b b] [b c c c]: both neighbours need "b".
    let info = selectable_info(&[3, 6, 3]);
    let storage = Storage::new(
        &info,
        dir.path(),
        &FileSelection::new(vec![Normal, Skip, Normal]),
    )
    .unwrap();
    let root = dir.path().join("root");
    assert_eq!(std::fs::metadata(root.join("f1")).unwrap().len(), 6);
    assert!(storage.files().iter().all(FileSpan::exists));
}

#[test]
fn skipped_files_that_exist_are_left_as_they_are() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("f1"), b"xy").unwrap();
    let info = selectable_info(&[4, 4]);
    let storage = Storage::new(&info, dir.path(), &FileSelection::new(vec![Normal, Skip])).unwrap();
    assert_eq!(std::fs::read(root.join("f1")).unwrap(), b"xy");
    assert_eq!(storage.files()[1].found, Some(2));
}

#[test]
fn rejects_a_selection_for_another_number_of_files() {
    let dir = tempfile::tempdir().unwrap();
    let info = selectable_info(&[4, 4]);
    assert!(Storage::new(&info, dir.path(), &FileSelection::new(vec![Skip])).is_err());
}

#[test]
fn delete_files_ignores_files_that_were_never_created() {
    let dir = tempfile::tempdir().unwrap();
    let info = selectable_info(&[4, 4, 4]);
    Storage::new(
        &info,
        dir.path(),
        &FileSelection::new(vec![Normal, Skip, Normal]),
    )
    .unwrap();
    assert!(delete_files(&info, dir.path()).is_empty());
    assert!(!dir.path().join("root").exists());
}

#[test]
fn files_in_finds_the_files_overlapping_a_range() {
    let dir = tempfile::tempdir().unwrap();
    // a: 0..3, empty: 3..3, b: 3..9, empty2: 9..9, c: 9..10
    let info = multi_file_info(&[
        (&["a"], 3),
        (&["empty"], 0),
        (&["b"], 6),
        (&["empty2"], 0),
        (&["c"], 1),
    ]);
    let storage = Storage::new(&info, dir.path(), &FileSelection::default()).unwrap();
    let files_in = |start, end| storage.files_in(start..end).collect::<Vec<_>>();

    assert_eq!(files_in(0, 10), [0, 2, 4]);
    assert_eq!(files_in(0, 1), [0]);
    // A range that ends or starts exactly on a boundary leaves the next or
    // previous file out, and empty files are never listed.
    assert_eq!(files_in(0, 3), [0]);
    assert_eq!(files_in(3, 9), [2]);
    assert_eq!(files_in(2, 4), [0, 2]);
    assert_eq!(files_in(8, 10), [2, 4]);
    assert_eq!(files_in(9, 10), [4]);
    // Nothing is held by an empty range on a boundary, or past the end.
    assert_eq!(files_in(3, 3), Vec::<usize>::new());
    assert_eq!(files_in(10, 12), Vec::<usize>::new());
}
