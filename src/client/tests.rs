use super::*;
use crate::download::SeedLimits;

fn client(dht: bool) -> Client {
    let config = DownloadConfig {
        output_dir: ".".into(),
        max_peers: 10,
        max_connections: 100,
        listen_port: 0,
        seed: SeedLimits::default(),
        dht,
        max_download_rate: 64,
        max_upload_rate: 0,
        resume_dir: None,
    };
    Client::new(config, MultiProgress::new())
}

#[test]
fn a_torrent_runs_once_at_a_time() {
    let client = client(false);
    let claim = client.claim([1; 20]).unwrap();
    let _other = client.claim([2; 20]).unwrap();
    assert!(client.claim([1; 20]).is_err());
    drop(claim);
    assert!(client.claim([1; 20]).is_ok());
}

#[test]
fn torrents_cannot_claim_each_others_files() {
    let client = client(false);
    let paths = |names: &[&str]| -> Vec<PathBuf> {
        names
            .iter()
            .map(|name| PathBuf::from("/out").join(name))
            .collect()
    };
    let first = client
        .claim_files([1; 20], "first", &paths(&["a", "b"]))
        .unwrap();
    let err = client
        .claim_files([2; 20], "second", &paths(&["c", "b"]))
        .err()
        .expect("shared file accepted");
    let message = err.to_string();
    assert!(
        message.contains("/out/b") && message.contains("first"),
        "{message}"
    );
    // The failed claim took nothing, not even the file that was free.
    let _disjoint = client
        .claim_files([3; 20], "third", &paths(&["c", "d"]))
        .unwrap();
    drop(first);
    assert!(
        client
            .claim_files([2; 20], "second", &paths(&["b"]))
            .is_ok()
    );
}

#[test]
fn a_claim_frees_its_files_when_it_ends() {
    let client = client(false);
    let paths = [PathBuf::from("/out/a")];
    let mut claim = client.claim([1; 20]).unwrap();
    claim.claim_files("first", &paths).unwrap();
    assert!(client.claim_files([2; 20], "second", &paths).is_err());
    assert_eq!(client.file_owner(&paths).as_deref(), Some("first"));
    drop(claim);
    assert_eq!(client.file_owner(&paths), None);
    assert!(client.claim_files([2; 20], "second", &paths).is_ok());
}

#[cfg(any(target_os = "macos", windows))]
#[test]
fn names_that_differ_in_case_collide() {
    use crate::storage::Storage;
    let dir = tempfile::tempdir().unwrap();
    let client = client(false);
    let info = |name: &str| crate::torrent::Info {
        name: name.into(),
        piece_length: 4,
        pieces: serde_bytes::ByteBuf::new(),
        private: None,
        length: Some(1),
        files: None,
    };
    let lower = Storage::paths(&info("movie.mkv"), dir.path()).unwrap();
    let upper = Storage::paths(&info("MOVIE.mkv"), dir.path()).unwrap();
    let _first = client.claim_files([1; 20], "lower", &lower).unwrap();
    assert!(client.claim_files([2; 20], "upper", &upper).is_err());
}

#[test]
fn connection_slots_run_out_and_come_back() {
    let slots = ConnectionSlots::new(2);
    let first = slots.try_acquire().unwrap();
    let _second = slots.try_acquire().unwrap();
    assert!(slots.try_acquire().is_none());
    assert_eq!((slots.in_use(), slots.limit()), (2, 2));
    drop(first);
    assert_eq!(slots.in_use(), 1);
    assert!(slots.try_acquire().is_some());
}

#[test]
fn the_client_hands_out_one_set_of_slots() {
    let client = client(false);
    let held: Vec<_> = (0..100)
        .map(|_| client.slots().try_acquire().unwrap())
        .collect();
    assert!(client.slots().try_acquire().is_none());
    drop(held);
    assert!(client.slots().try_acquire().is_some());
}

#[tokio::test]
async fn freed_wakes_when_a_slot_comes_back() {
    let slots = ConnectionSlots::new(1);
    let permit = slots.try_acquire().unwrap();
    let mut freed = std::pin::pin!(slots.freed());
    // Nothing came back yet.
    assert!(futures_now_or_never(freed.as_mut()).is_none());
    drop(permit);
    freed.await;
    assert!(slots.try_acquire().is_some());
}

/// Polls `future` once.
fn futures_now_or_never<F: Future>(future: F) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match future.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(output) => Some(output),
        std::task::Poll::Pending => None,
    }
}

#[tokio::test]
async fn torrents_share_one_listener_and_one_rate_limit() {
    let client = client(false);
    let first = client.listener().await;
    assert!(Arc::ptr_eq(first, client.listener().await));
    assert!(first.port().is_some());
    assert!(Arc::ptr_eq(&client.rate_limits(), &client.rate_limits()));
    assert!(client.rate_limits().download.is_limited());
    assert!(!client.rate_limits().upload.is_limited());
}

#[tokio::test]
async fn the_dht_uses_the_port_the_listener_got() {
    let client = client(true);
    let dht = client.dht().await.unwrap();
    assert_eq!(client.listener().await.port(), Some(dht.port()));
}

#[tokio::test]
async fn no_dht_when_it_is_off() {
    assert!(client(false).dht().await.is_none());
}
