use super::*;
use crate::selection::FileSelection;
use crate::selection::Priority::{High, Normal, Skip};
use crate::storage::{FileSpan, Storage};
use crate::torrent::{FileInfo, Info};
use serde_bytes::ByteBuf;
use std::fs::OpenOptions;

const BIG: u64 = 256 * 1024;

/// A torrent of files `f0`, `f1`, ... of the given lengths, in 4-byte
/// pieces.
fn info_of(lengths: &[u64]) -> Info {
    Info {
        name: "root".into(),
        piece_length: 4,
        pieces: ByteBuf::from(vec![
            0;
            lengths.iter().sum::<u64>().div_ceil(4) as usize * 20
        ]),
        private: None,
        length: None,
        files: Some(
            lengths
                .iter()
                .enumerate()
                .map(|(i, &length)| FileInfo {
                    path: vec![format!("f{i}")],
                    length,
                })
                .collect(),
        ),
    }
}

/// Bytes the file system gave the file, as opposed to its length.
#[cfg(unix)]
fn allocated(path: &std::path::Path) -> u64 {
    std::os::unix::fs::MetadataExt::blocks(&std::fs::metadata(path).unwrap()) * 512
}

#[cfg(unix)]
#[test]
fn full_preallocation_reserves_the_wanted_files_and_skips_the_rest() {
    let dir = tempfile::tempdir().unwrap();
    let info = info_of(&[BIG, BIG, BIG]);
    let selection = FileSelection::new(vec![Normal, Skip, High]);
    let storage =
        Storage::with_preallocate(&info, dir.path(), &selection, Preallocate::Full).unwrap();
    let root = dir.path().join("root");
    for name in ["f0", "f2"] {
        assert_eq!(std::fs::metadata(root.join(name)).unwrap().len(), BIG);
        assert!(allocated(&root.join(name)) >= BIG, "{name} is not reserved");
    }
    assert!(!root.join("f1").exists());
    assert_eq!(storage.files()[1].found, None);
    // What was reserved reads as zeros.
    assert_eq!(storage.read_at(0, 8).unwrap(), [0; 8]);
}

#[cfg(unix)]
#[test]
fn sparse_preallocation_sizes_the_files_without_reserving_them() {
    let dir = tempfile::tempdir().unwrap();
    let info = info_of(&[BIG, BIG]);
    Storage::with_preallocate(
        &info,
        dir.path(),
        &FileSelection::default(),
        Preallocate::Sparse,
    )
    .unwrap();
    // Also what `new` does.
    let other = tempfile::tempdir().unwrap();
    Storage::new(&info, other.path(), &FileSelection::default()).unwrap();
    for root in [dir.path().join("root"), other.path().join("root")] {
        for name in ["f0", "f1"] {
            assert_eq!(std::fs::metadata(root.join(name)).unwrap().len(), BIG);
            assert!(allocated(&root.join(name)) < BIG, "{name} is not sparse");
        }
    }
}

#[test]
fn full_preallocation_keeps_what_an_earlier_run_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("root");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("f0"), b"hello").unwrap();
    // Longer than it should be: it is cut back.
    std::fs::write(root.join("f1"), vec![7u8; 2 * BIG as usize]).unwrap();
    let info = info_of(&[BIG, BIG]);
    let storage = Storage::with_preallocate(
        &info,
        dir.path(),
        &FileSelection::default(),
        Preallocate::Full,
    )
    .unwrap();
    assert_eq!(std::fs::metadata(root.join("f0")).unwrap().len(), BIG);
    assert_eq!(&std::fs::read(root.join("f0")).unwrap()[..6], b"hello\0");
    assert_eq!(std::fs::metadata(root.join("f1")).unwrap().len(), BIG);
    assert_eq!(storage.files()[0].found, Some(5));
    assert!(storage.may_hold_prior_data(0, 4));
    assert!(storage.files().iter().all(FileSpan::exists));
}

#[test]
fn the_zero_fallback_writes_only_the_new_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f");
    std::fs::write(&path, b"keep").unwrap();
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    let length = 3 * 1024 * 1024 + 7;
    file.set_len(length).unwrap();
    zero_fill(&file, 4, length).unwrap();
    let data = std::fs::read(&path).unwrap();
    assert_eq!(data.len() as u64, length);
    assert_eq!(&data[..4], b"keep");
    assert!(data[4..].iter().all(|&b| b == 0));
}

#[test]
fn preallocate_is_named_as_in_the_config_file() {
    assert_eq!(Preallocate::default(), Preallocate::Sparse);
    assert_eq!(Preallocate::Sparse.as_str(), "sparse");
    assert_eq!(Preallocate::Full.as_str(), "full");
}
