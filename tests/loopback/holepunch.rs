use crate::common::*;
use rtrnt::download::{SeedLimits, download_from_peers};
use rtrnt::peer::extension::Advertised;
use rtrnt::peer::holepunch::{self, Kind, Message};
use rtrnt::peer::pex::{self, PexMessage};
use rtrnt::peer::{Encryption, PeerConnection, PeerMessage, Wire};
use rtrnt::rate_limit::RateLimits;
use rtrnt::utp::{Endpoint, Policy};
use std::future::pending;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::oneshot;

async fn negotiated(conn: &mut PeerConnection) {
    loop {
        conn.receive().await.unwrap();
        if let Some(handshake) = conn.peer_extensions() {
            assert_eq!(handshake.id(holepunch::NAME), Some(holepunch::ID));
            return;
        }
    }
}

/// Both participants receive CONNECT and dial simultaneously. The initially
/// silent target emulates a filtering NAT until the relay coordinates them;
/// only the introduced target has any data, so the relay cannot finish it.
#[tokio::test]
async fn a_relay_introduces_two_peers_that_transfer_over_utp() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let count = torrent.num_pieces();
    let relay = Endpoint::bind(0, 16, Arc::default()).await.unwrap();
    let relay_addr = SocketAddr::from(([127, 0, 0, 1], relay.port()));
    let blocked = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target_addr = blocked.local_addr().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    write_files(target_dir.path(), &data);
    let out = tempfile::tempdir().unwrap();
    let mut target = encrypting_session(
        torrent,
        info_hash,
        target_dir.path(),
        SeedLimits {
            ratio: 1000.0,
            time: Duration::ZERO,
        },
        RateLimits::default(),
        Encryption::Off,
    );
    Arc::get_mut(&mut target).unwrap().peer_id = [2; 20];
    let (target_stop, target_stopped) = oneshot::channel::<()>();
    let (relay_stop, relay_stopped) = oneshot::channel::<()>();
    let (introduced, introduction) = oneshot::channel();
    let relay_task = tokio::spawn(async move {
        let (socket, source_addr) = relay.accept().await.unwrap();
        let advertised = Advertised {
            listen_port: Some(relay.port()),
            holepunch: true,
            ..Default::default()
        };
        let mut source = PeerConnection::accept(
            socket,
            source_addr,
            info_hash,
            [9; 20],
            count,
            advertised,
            Encryption::Off,
        )
        .await
        .unwrap();
        negotiated(&mut source).await;
        source.send(&PeerMessage::HaveNone).await.unwrap();
        source
            .send_extended(
                pex::NAME,
                PexMessage {
                    added: vec![(target_addr, pex::FLAG_HOLEPUNCH)],
                    dropped: Vec::new(),
                }
                .encode()
                .unwrap(),
            )
            .await
            .unwrap();
        loop {
            if let PeerMessage::Extended {
                id: holepunch::ID,
                payload,
            } = source.receive().await.unwrap()
            {
                let message = Message::decode(&payload).unwrap();
                assert_eq!(
                    message,
                    Message {
                        endpoint: target_addr,
                        kind: Kind::Rendezvous
                    }
                );
                break;
            }
        }
        drop(blocked);
        let endpoint = Endpoint::bind(target_addr.port(), 16, Arc::default())
            .await
            .unwrap();
        let target_session = Arc::get_mut(&mut target).unwrap();
        target_session.utp = Some(endpoint);
        target_session.utp_policy = Policy::Require;
        let seeding = tokio::spawn(async move {
            download_from_peers(
                target,
                &vec![true; count],
                &[],
                10,
                Vec::new(),
                None,
                async {
                    let _ = target_stopped.await;
                },
            )
            .await
        });
        let mut destination = PeerConnection::connect_via(
            target_addr,
            info_hash,
            [9; 20],
            count,
            advertised,
            &[Wire::Plaintext],
            Some(&relay),
            Policy::Require,
        )
        .await
        .unwrap();
        negotiated(&mut destination).await;
        destination.send(&PeerMessage::HaveNone).await.unwrap();
        tokio::try_join!(
            source.send_extended(
                holepunch::NAME,
                Message {
                    endpoint: target_addr,
                    kind: Kind::Connect
                }
                .encode()
            ),
            destination.send_extended(
                holepunch::NAME,
                Message {
                    endpoint: source_addr,
                    kind: Kind::Connect
                }
                .encode()
            ),
        )
        .unwrap();
        introduced.send(()).unwrap();
        let _ = relay_stopped.await;
        drop(source);
        drop(destination);
        seeding.await.unwrap().unwrap();
    });
    let (torrent, _) = make_torrent(&data);
    let mut leecher = encrypting_session(
        torrent,
        info_hash,
        out.path(),
        SeedLimits::default(),
        RateLimits::default(),
        Encryption::Off,
    );
    let session = Arc::get_mut(&mut leecher).unwrap();
    session.peer_id = [1; 20];
    session.utp = Some(Endpoint::bind(0, 16, Arc::default()).await.unwrap());
    session.utp_policy = Policy::Require;
    tokio::time::timeout(
        Duration::from_secs(25),
        download_from_peers(
            leecher,
            &vec![false; count],
            &[relay_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("introduced transfer timed out")
    .expect("introduced transfer failed");
    introduction.await.unwrap();
    assert_files_match(out.path(), &data);
    target_stop.send(()).unwrap();
    relay_stop.send(()).unwrap();
    relay_task.await.unwrap();
}
