use super::*;
use crate::selection::FileSelection;
use crate::storage::{Storage, StorageOptions};
use crate::torrent::{FileInfo, Info};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};

fn offsets(runs: &[Vec<Cached>]) -> Vec<Vec<u64>> {
    runs.iter()
        .map(|run| run.iter().map(|(offset, _)| *offset).collect())
        .collect()
}

#[test]
fn bytes_count_what_is_held() {
    let cache = WriteCache::new(100);
    assert!(cache.enabled());
    assert!(!WriteCache::new(0).enabled());
    assert!(!cache.insert(0, vec![1; 30]));
    assert!(!cache.insert(30, vec![2; 20]));
    assert_eq!(cache.bytes(), 50);
    // Data at the same offset replaces what was there.
    assert!(!cache.insert(30, vec![3; 25]));
    assert_eq!(cache.bytes(), 55);
    // The limit is reached, not passed.
    assert!(cache.insert(55, vec![4; 45]));
    assert_eq!(cache.bytes(), 100);
}

#[test]
fn retiring_an_entry_frees_its_bytes_unless_it_was_replaced() {
    let cache = WriteCache::new(100);
    cache.insert(0, vec![1; 10]);
    cache.insert(10, vec![2; 10]);
    let runs = cache.runs();
    // New data arrives at 10 while the old is being written.
    cache.insert(10, vec![9; 6]);
    for entry in runs.iter().flatten() {
        cache.retire(entry);
    }
    assert_eq!(cache.bytes(), 6);
    assert_eq!(cache.overlapping(0..100).len(), 1);
    assert_eq!(*cache.overlapping(0..100)[0].1, vec![9; 6]);
}

#[test]
fn runs_are_in_offset_order_and_join_adjacent_entries() {
    let cache = WriteCache::new(1 << 20);
    for offset in [8, 100, 0, 4, 20] {
        cache.insert(offset, vec![0; 4]);
    }
    assert_eq!(offsets(&cache.runs()), [vec![0, 4, 8], vec![20], vec![100]]);
    assert!(WriteCache::new(1).runs().is_empty());
}

#[test]
fn a_run_is_cut_at_the_most_one_write_joins() {
    let cache = WriteCache::new(usize::MAX);
    let piece = MAX_RUN / 2;
    for i in 0..3u64 {
        cache.insert(i * piece as u64, vec![0; piece]);
    }
    assert_eq!(
        offsets(&cache.runs()),
        [vec![0, piece as u64], vec![2 * piece as u64]]
    );
}

#[test]
fn overlapping_finds_entries_that_reach_into_the_range() {
    let cache = WriteCache::new(1 << 20);
    cache.insert(0, vec![0; 10]);
    cache.insert(10, vec![0; 10]);
    cache.insert(30, vec![0; 10]);
    let found = |range: Range<u64>| -> Vec<u64> {
        cache.overlapping(range).iter().map(|(o, _)| *o).collect()
    };
    assert_eq!(found(5..12), [0, 10]);
    assert_eq!(found(10..20), [10]);
    assert_eq!(found(9..10), [0]);
    assert_eq!(found(20..30), Vec::<u64>::new());
    assert_eq!(found(25..31), [30]);
    assert_eq!(found(0..100), [0, 10, 30]);
}

/// Two files, `a` of 6 bytes and `b` of 10, in pieces of 4 bytes.
fn info() -> Info {
    Info {
        name: "root".into(),
        piece_length: 4,
        pieces: ByteBuf::from(vec![0; 4 * 20]),
        private: None,
        length: None,
        files: Some(vec![
            FileInfo {
                path: vec!["a".into()],
                length: 6,
            },
            FileInfo {
                path: vec!["b".into()],
                length: 10,
            },
        ]),
    }
}

fn storage(dir: &std::path::Path, cache_bytes: u64) -> std::sync::Arc<Storage> {
    let options = StorageOptions {
        cache_bytes,
        ..StorageOptions::default()
    };
    let storage = Storage::with_options(&info(), dir, &FileSelection::default(), options);
    std::sync::Arc::new(storage.unwrap())
}

fn piece(byte: u8) -> (Vec<u8>, [u8; 20]) {
    let data = vec![byte; 4];
    let hash = Sha1::digest(&data).into();
    (data, hash)
}

async fn write(storage: &std::sync::Arc<Storage>, index: usize, byte: u8) {
    let (data, hash) = piece(byte);
    storage
        .write_verified(index, data, hash)
        .await
        .unwrap()
        .unwrap();
}

fn on_disk(dir: &std::path::Path) -> Vec<u8> {
    let mut all = std::fs::read(dir.join("root/a")).unwrap();
    all.extend(std::fs::read(dir.join("root/b")).unwrap());
    all
}

#[tokio::test]
async fn pieces_wait_in_the_cache_and_reads_see_them() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 1024);
    write(&storage, 2, 3).await;
    write(&storage, 0, 1).await;
    assert_eq!(storage.cached_bytes(), 8);
    // Nothing is on disk yet.
    assert_eq!(on_disk(dir.path()), [0; 16]);
    // A read, an upload's block and a recheck's read all see the pieces.
    assert_eq!(storage.read_at(0, 4).unwrap(), [1; 4]);
    assert_eq!(storage.read_block(2, 1, 2).await.unwrap(), [3; 2]);
    let all = storage.read_at(0, 16).unwrap();
    assert_eq!(all[..4], [1; 4]);
    assert_eq!(all[4..8], [0; 4]);
    assert_eq!(all[8..12], [3; 4]);
    // A read that starts inside a piece, and ends past it.
    assert_eq!(storage.read_at(2, 4).unwrap(), [1, 1, 0, 0]);
}

#[tokio::test]
async fn a_flush_writes_in_offset_order_across_files_and_empties_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 1024);
    for (index, byte) in [(3, 4), (1, 2), (2, 3), (0, 1)] {
        write(&storage, index, byte).await;
    }
    storage.flush_async().await.unwrap();
    assert_eq!(storage.cached_bytes(), 0);
    let expected: Vec<u8> = (1..=4).flat_map(|byte| [byte; 4]).collect();
    assert_eq!(on_disk(dir.path()), expected);
    assert_eq!(storage.read_at(0, 16).unwrap(), expected);
    // Nothing left to write.
    storage.flush().unwrap();
}

#[tokio::test]
async fn a_full_cache_flushes_on_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 12);
    write(&storage, 0, 1).await;
    write(&storage, 1, 2).await;
    assert_eq!(storage.cached_bytes(), 8);
    write(&storage, 3, 4).await;
    assert_eq!(storage.cached_bytes(), 0);
    assert_eq!(on_disk(dir.path())[..8], [1, 1, 1, 1, 2, 2, 2, 2]);
    assert_eq!(on_disk(dir.path())[12..], [4; 4]);
}

#[tokio::test]
async fn without_a_cache_pieces_are_written_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 0);
    assert!(!storage.cache_enabled());
    write(&storage, 1, 2).await;
    assert_eq!(storage.cached_bytes(), 0);
    assert_eq!(on_disk(dir.path())[4..8], [2; 4]);
}

#[tokio::test]
async fn a_piece_that_fails_its_hash_is_not_cached() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 1024);
    let (data, _) = piece(1);
    let bad = storage.write_verified(0, data.clone(), [0; 20]).await;
    assert_eq!(bad.unwrap().unwrap_err(), data);
    assert_eq!(storage.cached_bytes(), 0);
}

#[tokio::test]
async fn the_cache_is_flushed_when_the_storage_goes_away() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 1024);
    write(&storage, 1, 2).await;
    write(&storage, 3, 4).await;
    assert_eq!(on_disk(dir.path()), [0; 16]);
    drop(storage);
    let data = on_disk(dir.path());
    assert_eq!(data[4..8], [2; 4]);
    assert_eq!(data[12..], [4; 4]);
}

#[tokio::test]
async fn a_direct_write_goes_after_what_is_cached() {
    let dir = tempfile::tempdir().unwrap();
    let storage = storage(dir.path(), 1024);
    write(&storage, 0, 1).await;
    storage.write_at(0, &[9; 4]).unwrap();
    assert_eq!(storage.read_at(0, 4).unwrap(), [9; 4]);
    drop(storage);
    assert_eq!(on_disk(dir.path())[..4], [9; 4]);
}

#[test]
fn a_read_only_storage_takes_no_pieces() {
    let dir = tempfile::tempdir().unwrap();
    drop(storage(dir.path(), 0));
    let existing = Storage::open_existing(&info(), dir.path()).unwrap();
    assert!(!existing.cache_enabled());
    assert!(existing.flush().is_ok());
}
