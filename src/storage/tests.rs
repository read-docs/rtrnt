use super::*;
use crate::torrent::FileInfo;
use serde_bytes::ByteBuf;

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
    let storage = Storage::new(&info, dir.path()).unwrap();

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
    let storage = Arc::new(Storage::new(&info, dir.path()).unwrap());
    storage.write_at(0, b"abcdefghi").unwrap();
    // Piece 1 is "efgh", and its middle spans no file boundary.
    assert_eq!(storage.read_block(1, 1, 2).await.unwrap(), b"fg");
    // Piece 0 spans both files; piece 2 is the short last one.
    assert_eq!(storage.read_block(0, 2, 2).await.unwrap(), b"cd");
    assert_eq!(storage.read_block(2, 0, 1).await.unwrap(), b"i");
    assert!(storage.read_block(2, 0, 2).await.is_err());
}

#[test]
fn caches_handles_up_to_limit() {
    let dir = tempfile::tempdir().unwrap();
    let count = MAX_OPEN_FILES + 2;
    let names: Vec<String> = (0..count).map(|i| format!("f{i}")).collect();
    let paths: Vec<[&str; 1]> = names.iter().map(|n| [n.as_str()]).collect();
    let files: Vec<(&[&str], u64)> = paths.iter().map(|p| (&p[..], 1)).collect();
    let storage = Storage::new(&multi_file_info(&files), dir.path()).unwrap();
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
            Storage::new(&info, dir.path()).is_err(),
            "accepted {:?}",
            bad
        );
    }
    let mut info = multi_file_info(&[(&["ok"], 1)]);
    info.name = "..".into();
    assert!(Storage::new(&info, dir.path()).is_err());
}

#[test]
fn rejects_the_same_path_twice() {
    let dir = tempfile::tempdir().unwrap();
    let info = multi_file_info(&[(&["a"], 1), (&["sub", "b"], 1), (&["a"], 2)]);
    let err = Storage::new(&info, dir.path()).unwrap_err();
    assert!(err.to_string().contains("twice"), "{err:#}");
    assert!(Storage::paths(&info, dir.path()).is_err());
    assert!(Storage::open_existing(&info, dir.path()).is_err());
    assert!(!dir.path().join("root").exists());

    let info = multi_file_info(&[(&["a"], 1), (&["A"], 1)]);
    // Names that differ in case are two files only where case counts.
    assert_eq!(
        Storage::new(&info, dir.path()).is_err(),
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
    drop(Storage::new(&info, &out).unwrap());
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
