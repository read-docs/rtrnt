use crate::common::*;
use rtrnt::download::{SeedLimits, download_from_peers};
use rtrnt::peer::Encryption;
use rtrnt::rate_limit::RateLimits;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// One rtrnt seeds to another over MSE: the seeder requires encryption, so
/// the download only completes if the whole stack runs encrypted.
#[tokio::test]
async fn downloads_from_a_seeder_that_requires_encryption() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &data);
    let forever = SeedLimits {
        ratio: 1000.0,
        time: Duration::ZERO,
    };
    let seeder = encrypting_session(
        torrent,
        info_hash,
        seed_dir.path(),
        forever,
        RateLimits::default(),
        Encryption::Require,
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel::<()>();
    let seeding = tokio::spawn(async move {
        download_from_peers(
            seeder,
            &vec![true; num_pieces],
            &[],
            10,
            vec![listener],
            None,
            async {
                let _ = stopped.await;
            },
        )
        .await
    });

    let (torrent, _) = make_torrent(&data);
    let out = tempfile::tempdir().unwrap();
    let leecher = encrypting_session(
        torrent,
        info_hash,
        out.path(),
        SeedLimits::default(),
        RateLimits::default(),
        Encryption::Prefer,
    );
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            leecher,
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
    assert_files_match(out.path(), &data);

    stop.send(()).unwrap();
    seeding.await.unwrap().unwrap();
}
