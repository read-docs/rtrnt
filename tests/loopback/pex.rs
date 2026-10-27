use rtrnt::download::{Interrupted, download_from_peers};
use rtrnt::extension::ExtendedHandshake;
use rtrnt::peer::PeerMessage;
use rtrnt::pex::{self, PexMessage};
use rtrnt::torrent::TorrentFile;
use sha1::Digest;
use std::future::pending;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::common::*;

/// The torrent of `make_torrent`, marked private (BEP 27).
fn make_private_torrent(data: &[u8]) -> (TorrentFile, [u8; 20]) {
    let (mut torrent, _) = make_torrent(data);
    torrent.info.private = Some(1);
    let info_hash = sha1::Sha1::digest(serde_bencode::to_bytes(&torrent.info).unwrap()).into();
    (torrent, info_hash)
}

/// Sends an extension handshake that offers peer exchange under `pex_id`,
/// with `listen_port` as `p`.
async fn send_pex_handshake(socket: &mut TcpStream, pex_id: u8, listen_port: Option<u16>) {
    let theirs = ExtendedHandshake {
        extensions: [(pex::NAME.to_string(), pex_id)].into(),
        listen_port,
        ..Default::default()
    };
    let payload = theirs.encode().unwrap();
    send(socket, PeerMessage::Extended { id: 0, payload }).await;
}

/// L, our client, knows of one peer: H, our client too, which has nothing
/// yet. H is connected to the seeder S, but S has nothing for H, only for
/// the next peer to connect. H tells L about S through peer exchange (it
/// knows where S listens, having connected to it), and L downloads
/// everything from S.
#[tokio::test]
async fn downloads_from_a_peer_learned_through_pex() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = seeder.local_addr().unwrap();
    let (h_connected, h_connected_rx) = oneshot::channel();
    let seeder_data = Arc::clone(&data);
    let seeder_task = tokio::spawn(async move {
        let h = accept_partial(&seeder, info_hash, num_pieces, &[]).await;
        h_connected.send(()).unwrap();
        let mut l = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut l, PeerMessage::Unchoke).await;
        serve(l, &seeder_data).await;
        drop(h);
    });

    let h_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let h_addr = h_listener.local_addr().unwrap();
    let h_out = tempfile::tempdir().unwrap();
    let h_session = session(torrent.clone(), info_hash, h_out.path());
    let (stop_h, stop_h_rx) = oneshot::channel::<()>();
    let h = tokio::spawn(async move {
        let have = vec![false; num_pieces];
        let stop = async {
            let _ = stop_h_rx.await;
        };
        download_from_peers(
            h_session,
            &have,
            &[seeder_addr],
            10,
            vec![h_listener],
            None,
            stop,
        )
        .await
    });
    h_connected_rx.await.unwrap();

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[h_addr],
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
    seeder_task.await.unwrap();

    stop_h.send(()).unwrap();
    let err = h.await.unwrap().unwrap_err();
    assert!(err.is::<Interrupted>(), "{err:#}");
}

/// Peers that support peer exchange hear about the other peers we are
/// connected to whose listening address we know, with their flags, but
/// never about themselves. X is a seed we connected to; Z, Y and W connect
/// to us in turn, and only Y gives its listening port.
#[tokio::test]
async fn tells_peers_about_each_other_through_pex() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    // X never unchokes us, so the client stays connected without
    // downloading anything.
    let x = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let x_addr = x.local_addr().unwrap();
    let (x_ready, x_ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&x, info_hash, num_pieces).await;
        // Interest shows that the client has seen X's bitfield.
        while next_message(&mut socket).await != Some(PeerMessage::Interested) {}
        x_ready.send(()).unwrap();
        while read_message(&mut socket).await.is_some() {}
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());
    let (stop, stop_rx) = oneshot::channel::<()>();
    let client = tokio::spawn(async move {
        let have = vec![false; num_pieces];
        let stop = async {
            let _ = stop_rx.await;
        };
        download_from_peers(session, &have, &[x_addr], 10, vec![listener], None, stop).await
    });
    x_ready_rx.await.unwrap();

    let mut z = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut z, 9, None).await;
    let x_flags = pex::FLAG_SEED | pex::FLAG_CONNECTABLE;
    let to_z = PexMessage::decode(&next_extended(&mut z, 9).await).unwrap();
    assert_eq!(to_z.added, [(x_addr, x_flags)]);

    let y_listen = SocketAddr::from((Ipv4Addr::LOCALHOST, 5555));
    let mut y = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut y, 7, Some(y_listen.port())).await;
    let ours = ExtendedHandshake::decode(&next_extended(&mut y, 0).await).unwrap();
    assert_eq!(ours.id(pex::NAME), Some(pex::ID));
    let to_y = PexMessage::decode(&next_extended(&mut y, 7).await).unwrap();
    assert_eq!(to_y.added, [(x_addr, x_flags)]);
    assert!(to_y.dropped.is_empty());

    let mut w = connect_to(our_addr, info_hash).await;
    send_pex_handshake(&mut w, 8, None).await;
    let mut to_w = PexMessage::decode(&next_extended(&mut w, 8).await).unwrap();
    to_w.added.sort();
    let mut expected = vec![(x_addr, x_flags), (y_listen, 0)];
    expected.sort();
    assert_eq!(to_w.added, expected);

    stop.send(()).unwrap();
    let err = client.await.unwrap().unwrap_err();
    assert!(err.is::<Interrupted>(), "{err:#}");
}

/// Private torrents (BEP 27) never use peer exchange: we do not offer it,
/// and a peer that sends a peer exchange message anyway is not listened to.
#[tokio::test]
async fn private_torrents_do_not_exchange_peers() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_private_torrent(&data);
    assert!(torrent.is_private());
    let num_pieces = torrent.num_pieces();

    // Only the peer exchange message names it.
    let decoy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let decoy_addr = decoy.local_addr().unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        let ours = ExtendedHandshake::decode(&next_extended(&mut socket, 0).await).unwrap();
        assert_eq!(ours.id(pex::NAME), None);
        send_pex_handshake(&mut socket, 3, None).await;
        let message = PexMessage {
            added: vec![(decoy_addr, pex::FLAG_CONNECTABLE)],
            dropped: Vec::new(),
        };
        let payload = message.encode().unwrap();
        send(
            &mut socket,
            PeerMessage::Extended {
                id: pex::ID,
                payload,
            },
        )
        .await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
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
    seeder.await.unwrap();
    let connected = tokio::time::timeout(Duration::from_millis(200), decoy.accept()).await;
    assert!(
        connected.is_err(),
        "connected to a peer named by peer exchange"
    );
}
