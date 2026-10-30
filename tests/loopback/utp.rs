use crate::common::*;
use rtrnt::download::{SeedLimits, download_from_peers};
use rtrnt::peer::Encryption;
use rtrnt::rate_limit::RateLimits;
use rtrnt::utp::{Endpoint, Policy};
use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

async fn encrypted_transfer(magnet: bool) {
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let count = torrent.num_pieces();
    let seed_dir = tempfile::tempdir().unwrap();
    write_files(seed_dir.path(), &data);
    let endpoint = Endpoint::bind(0, 10, Arc::default()).await.unwrap();
    let addr = SocketAddr::from(([127, 0, 0, 1], endpoint.port()));
    let mut seed = encrypting_session(
        torrent,
        info_hash,
        seed_dir.path(),
        SeedLimits {
            ratio: 1000.0,
            time: Duration::ZERO,
        },
        RateLimits::default(),
        Encryption::Require,
    );
    let session = Arc::get_mut(&mut seed).unwrap();
    session.utp = Some(endpoint);
    session.utp_policy = Policy::Require;
    let (stop, stopped) = oneshot::channel::<()>();
    let seeding = tokio::spawn(async move {
        download_from_peers(seed, &vec![true; count], &[], 10, Vec::new(), None, async {
            let _ = stopped.await;
        })
        .await
    });
    let (torrent, _) = make_torrent(&data);
    let out = tempfile::tempdir().unwrap();
    if magnet {
        let state = tempfile::tempdir().unwrap();
        let mut config = resume_config(out.path(), state.path());
        config.encryption = Encryption::Require;
        config.utp_policy = Policy::Require;
        let client = client(config);
        let link = rtrnt::magnet::MagnetLink {
            info_hash,
            name: Some("uTP magnet".into()),
            trackers: Vec::new(),
            peers: vec![addr],
            select_only: None,
        };
        tokio::time::timeout(
            Duration::from_secs(20),
            rtrnt::magnet::download_magnet(&link, &Default::default(), &client, pending()),
        )
        .await
        .expect("uTP magnet timed out")
        .expect("uTP magnet failed");
    } else {
        let mut leech = encrypting_session(
            torrent,
            info_hash,
            out.path(),
            SeedLimits::default(),
            RateLimits::default(),
            Encryption::Require,
        );
        let session = Arc::get_mut(&mut leech).unwrap();
        session.utp = Some(Endpoint::bind(0, 10, Arc::default()).await.unwrap());
        session.utp_policy = Policy::Require;
        tokio::time::timeout(
            Duration::from_secs(20),
            download_from_peers(
                leech,
                &vec![false; count],
                &[addr],
                10,
                Vec::new(),
                None,
                pending(),
            ),
        )
        .await
        .expect("uTP download timed out")
        .expect("uTP download failed");
    }
    assert_files_match(out.path(), &data);
    stop.send(()).unwrap();
    seeding.await.unwrap().unwrap();
}

#[tokio::test]
async fn downloads_pieces_from_a_utp_only_encrypted_seeder() {
    encrypted_transfer(false).await;
}

#[tokio::test]
async fn fetches_metadata_and_downloads_a_magnet_over_encrypted_utp() {
    encrypted_transfer(true).await;
}
