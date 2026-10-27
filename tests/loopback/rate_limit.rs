use rtrnt::download::{SeedLimits, download_from_peers};
use rtrnt::peer::PeerMessage;
use rtrnt::rate_limit::RateLimits;
use std::future::pending;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

use crate::common::*;

/// Serves every block asked for to the leecher that connects on `listener`,
/// as a seed, and returns how many block bytes it sent.
async fn counting_seeder(
    listener: TcpListener,
    data: Arc<Vec<u8>>,
    info_hash: [u8; 20],
    num_pieces: usize,
) -> u64 {
    let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
    send(&mut socket, PeerMessage::Unchoke).await;
    let mut sent = 0;
    while let Some(message) = read_message(&mut socket).await {
        if let PeerMessage::Request {
            index,
            begin,
            length,
        } = message
        {
            send(&mut socket, piece(&data, index, begin, length)).await;
            sent += u64::from(length);
        }
    }
    sent
}

/// The rate limit used by the tests below, in KiB/s.
const TEST_RATE_KIB: u64 = 32;

/// The shortest time `bytes` can take at `TEST_RATE_KIB`, once the burst
/// (one block, at this rate) has gone out at once, less a fifth so that a
/// slow machine cannot make the test flaky.
fn shortest_limited_time(bytes: u64) -> Duration {
    let rate = (TEST_RATE_KIB * 1024) as f64;
    Duration::from_secs_f64((bytes - u64::from(BLOCK)) as f64 / rate * 0.8)
}

/// With a download limit, two fast seeders share it: the download takes at
/// least as long as the limit allows, and both get to send blocks.
#[tokio::test]
async fn download_rate_limit_is_shared_by_all_peers() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let mut addrs = Vec::new();
    let mut seeders = Vec::new();
    for _ in 0..2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(listener.local_addr().unwrap());
        let data = Arc::clone(&data);
        seeders.push(tokio::spawn(counting_seeder(
            listener, data, info_hash, num_pieces,
        )));
    }

    let out = tempfile::tempdir().unwrap();
    let limits = RateLimits::from_kib(TEST_RATE_KIB, 0);
    let session = limited_session(
        torrent,
        info_hash,
        out.path(),
        SeedLimits::default(),
        limits,
    );
    let start = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &addrs,
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    let elapsed = start.elapsed();
    assert_files_match(out.path(), &data);

    let total = data.len() as u64;
    assert!(
        elapsed >= shortest_limited_time(total),
        "{total} bytes in {elapsed:?} at {TEST_RATE_KIB} KiB/s"
    );
    for seeder in seeders {
        let sent = seeder.await.unwrap();
        assert!(sent >= u64::from(BLOCK), "a seeder only sent {sent} bytes");
    }
}

/// With an upload limit, a leecher that asks for the whole torrent at once
/// gets it no faster than the limit allows.
#[tokio::test]
async fn upload_rate_limit_paces_the_blocks_we_send() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        let mut socket = connect_to(our_addr, info_hash).await;
        leech_everything(&mut socket, &leecher_data).await;
    });

    // Seeding to a ratio of 1 ends the run once the leecher has everything.
    let seed = SeedLimits {
        ratio: 1.0,
        time: Duration::ZERO,
    };
    let limits = RateLimits::from_kib(0, TEST_RATE_KIB);
    let session = limited_session(torrent, info_hash, out.path(), seed, limits);
    let start = std::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![true; num_pieces],
            &[],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("seeding did not stop")
    .expect("seeding failed");
    let elapsed = start.elapsed();
    leecher.await.unwrap();

    let total = data.len() as u64;
    assert!(
        elapsed >= shortest_limited_time(total),
        "{total} bytes in {elapsed:?} at {TEST_RATE_KIB} KiB/s"
    );
}
