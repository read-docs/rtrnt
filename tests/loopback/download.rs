use rtrnt::dht::Dht;
use rtrnt::download::{download_from_peers, listen};
use rtrnt::extension::ExtendedHandshake;
use rtrnt::peer::{PeerMessage, parse_handshake};
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::{mpsc, oneshot};

use crate::common::*;

#[tokio::test]
async fn downloads_multi_file_torrent_from_mock_seeder() {
    init_tracing();
    let data = test_data();
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let data = Arc::new(data);
    tokio::spawn(seeder(listener, Arc::clone(&data), info_hash, num_pieces));

    // A peer that refuses connections must not break the download.
    let dead_addr: SocketAddr = {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        dead.local_addr().unwrap()
    };

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(30),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[dead_addr, seeder_addr],
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
}

/// With room for a single connection, a dead peer and one that disconnects
/// mid-download must each be replaced by the next peer from the list.
#[tokio::test]
async fn replaces_peers_that_fail() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let dead_addr: SocketAddr = {
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap();
        dead.local_addr().unwrap()
    };

    // Serves one block, then hangs up.
    let flaky = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let flaky_addr = flaky.local_addr().unwrap();
    let flaky_data = Arc::clone(&data);
    let (flaky_served, flaky_served_rx) = oneshot::channel();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&flaky, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        while let Some(message) = read_message(&mut socket).await {
            if let PeerMessage::Request {
                index,
                begin,
                length,
            } = message
            {
                let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                let block = flaky_data[start..start + length as usize].to_vec();
                send(
                    &mut socket,
                    PeerMessage::Piece {
                        index,
                        begin,
                        block,
                    },
                )
                .await;
                let _ = flaky_served.send(());
                return;
            }
        }
    });

    // Only accepts once the flaky peer has had its turn, so the download
    // cannot finish without replacing it.
    let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy_addr = healthy.local_addr().unwrap();
    let healthy_data = Arc::clone(&data);
    tokio::spawn(async move {
        flaky_served_rx.await.unwrap();
        let mut socket = accept_leecher(&healthy, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &healthy_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[dead_addr, flaky_addr, healthy_addr],
            1,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");

    assert_files_match(out.path(), &data);
}

/// One seeder takes a piece and never sends it. Endgame must fetch that piece
/// from the healthy seeder and cancel the stalled requests, instead of
/// waiting out the 30 s read timeout.
#[tokio::test]
async fn endgame_finishes_despite_a_stalled_peer() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let (stalled_requested, stalled_requested_rx) = oneshot::channel();
    let (cancel_seen, cancel_seen_rx) = oneshot::channel();
    let stalled = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stalled_addr = stalled.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_leecher(&stalled, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut stalled_requested = Some(stalled_requested);
        let mut cancel_seen = Some(cancel_seen);
        while let Some(message) = read_message(&mut socket).await {
            match message {
                PeerMessage::Request { .. } => {
                    stalled_requested.take().map(|tx| tx.send(()));
                }
                PeerMessage::Cancel { .. } => {
                    cancel_seen.take().map(|tx| tx.send(()));
                }
                _ => {}
            }
        }
    });

    // The healthy seeder only unchokes once the stalled one holds a piece, so
    // the download is guaranteed to need endgame.
    let healthy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let healthy_addr = healthy.local_addr().unwrap();
    let healthy_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&healthy, info_hash, num_pieces).await;
        stalled_requested_rx.await.unwrap();
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &healthy_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[stalled_addr, healthy_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download stalled behind the slow peer")
    .expect("download failed");

    assert_files_match(out.path(), &data);
    tokio::time::timeout(Duration::from_secs(5), cancel_seen_rx)
        .await
        .expect("stalled peer never got a Cancel")
        .unwrap();
}

/// A peer that only has piece 0 and never unchokes us is sent Interested, and
/// NotInterested once another peer delivers piece 0. It is never asked for a
/// block.
#[tokio::test]
async fn loses_interest_when_a_peer_has_nothing_left_we_need() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let (choking_done, choking_done_rx) = oneshot::channel();
    let choking = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let choking_addr = choking.local_addr().unwrap();
    let choking_task = tokio::spawn(async move {
        let mut socket = accept_partial(&choking, info_hash, num_pieces, &[0]).await;
        let mut received = Vec::new();
        let mut choking_done = Some(choking_done);
        while let Some(message) = read_message(&mut socket).await {
            // The extension handshake that opens every connection.
            if matches!(message, PeerMessage::Extended { .. }) {
                continue;
            }
            received.push(message);
            if received.last() == Some(&PeerMessage::NotInterested) {
                choking_done.take().map(|tx| tx.send(()));
            }
        }
        received
    });

    // Has pieces 0 and 1, so the choking peer's only piece gets downloaded.
    let partial = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let partial_addr = partial.local_addr().unwrap();
    let partial_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_partial(&partial, info_hash, num_pieces, &[0, 1]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &partial_data).await;
    });

    // Piece 2 only becomes available once the choking peer lost interest, so
    // the download is still running when that has to happen.
    let last = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let last_addr = last.local_addr().unwrap();
    let last_data = Arc::clone(&data);
    tokio::spawn(async move {
        choking_done_rx.await.unwrap();
        let mut socket = accept_partial(&last, info_hash, num_pieces, &[2]).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &last_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[choking_addr, partial_addr, last_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("choking peer was never sent NotInterested")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    // It also hears Have for the pieces it lacks, as they complete.
    let mut received = choking_task.await.unwrap();
    received.retain(|message| !matches!(message, PeerMessage::Have { .. }));
    assert_eq!(
        received,
        [PeerMessage::Interested, PeerMessage::NotInterested]
    );
}

/// Requests are pipelined across piece boundaries: the seeder holds back its
/// replies until four requests (a new peer's pipeline depth) are queued,
/// which takes two of these two-block pieces. A per-piece pipeline would
/// stall here.
#[tokio::test]
async fn pipelines_requests_across_pieces() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(PIECE_LENGTH, 2 * 16 * 1024);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let first_batch = tokio::spawn(async move {
        let mut socket = accept_leecher(&listener, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        let mut queued = Vec::new();
        while queued.len() < 4 {
            match read_message(&mut socket).await {
                Some(PeerMessage::Request {
                    index,
                    begin,
                    length,
                }) => queued.push((index, begin, length)),
                Some(_) => {}
                None => return queued,
            }
        }
        for &(index, begin, length) in &queued {
            let start = index as usize * PIECE_LENGTH as usize + begin as usize;
            let block = seeder_data[start..start + length as usize].to_vec();
            send(
                &mut socket,
                PeerMessage::Piece {
                    index,
                    begin,
                    block,
                },
            )
            .await;
        }
        serve(socket, &seeder_data).await;
        queued
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
    .expect("pipeline stalled at a piece boundary")
    .expect("download failed");
    assert_files_match(out.path(), &data);

    let queued = first_batch.await.unwrap();
    let mut pieces: Vec<u32> = queued.iter().map(|&(index, ..)| index).collect();
    pieces.dedup();
    assert_eq!(pieces.len(), 2, "first requests: {queued:?}");
}

/// The seeder supports the extension protocol and says in its extension
/// handshake that it queues only two requests. It answers only once no more
/// requests arrive, so every request we were willing to send is outstanding
/// by then, and there must never be more than two (a new peer's pipeline
/// would otherwise allow four).
#[tokio::test]
async fn never_exceeds_the_request_queue_a_peer_announces() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    const REQQ: usize = 2;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        assert!(
            parse_handshake(&handshake, &info_hash)
                .unwrap()
                .supports_extensions()
        );
        // `mock_handshake` sets the extension protocol bit.
        socket.write_all(&mock_handshake(&info_hash)).await.unwrap();
        let theirs = ExtendedHandshake {
            client: Some("mock seeder".into()),
            request_queue: Some(REQQ as u32),
            ..Default::default()
        };
        let payload = theirs.encode().unwrap();
        send(&mut socket, PeerMessage::Extended { id: 0, payload }).await;
        let all: Vec<usize> = (0..num_pieces).collect();
        send_bitfield(&mut socket, num_pieces, &all).await;
        send(&mut socket, PeerMessage::Unchoke).await;

        // Read on a separate task, so waiting for the next message can time
        // out without losing part of one.
        let (mut reader, mut writer) = socket.into_split();
        let (messages_tx, mut messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Ok(len) = reader.read_u32().await {
                let mut payload = vec![0u8; len as usize];
                if reader.read_exact(&mut payload).await.is_err() {
                    break;
                }
                let _ = messages_tx.send(PeerMessage::decode(&payload).unwrap());
            }
        });

        // Ours comes first.
        let Some(PeerMessage::Extended { id: 0, payload }) = messages.recv().await else {
            panic!("no extension handshake");
        };
        let ours = ExtendedHandshake::decode(&payload).unwrap();
        assert!(ours.client.unwrap().starts_with("rtrnt "));
        assert!(ours.request_queue.is_some());

        let mut outstanding = Vec::new();
        let mut most_outstanding = 0;
        loop {
            match tokio::time::timeout(Duration::from_millis(100), messages.recv()).await {
                Ok(Some(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })) => {
                    outstanding.push((index, begin, length));
                    most_outstanding = most_outstanding.max(outstanding.len());
                }
                Ok(Some(_)) => {}
                Ok(None) => break,
                // No more requests are coming: answer them all.
                Err(_) => {
                    for (index, begin, length) in outstanding.drain(..) {
                        let start = index as usize * PIECE_LENGTH as usize + begin as usize;
                        let block = seeder_data[start..start + length as usize].to_vec();
                        let message = PeerMessage::Piece {
                            index,
                            begin,
                            block,
                        };
                        writer.write_all(&message.encode()).await.unwrap();
                    }
                }
            }
        }
        most_outstanding
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
    assert_eq!(seeder.await.unwrap(), REQQ);
}

/// The only seeder connects to us. The one peer we know of has nothing; it
/// just keeps the download from running out of peers until the seeder
/// arrives. Our own address is in the peer list too, as a tracker would
/// list it, and connecting to it must do no harm.
#[tokio::test]
async fn downloads_from_a_peer_that_connects_to_us() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let empty = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let empty_addr = empty.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_partial(&empty, info_hash, num_pieces, &[]).await;
        while read_message(&mut socket).await.is_some() {}
    });

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = connect_to_leecher(our_addr, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        serve(socket, &seeder_data).await;
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[empty_addr, our_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
}

/// No tracker and no peer list: the only seeder announced itself in the
/// DHT, and the download finds it there. The DHT runs over loopback: a hub
/// to bootstrap from and one node to hold the announce.
#[tokio::test]
async fn downloads_from_a_peer_found_in_the_dht() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let seeder = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_port = seeder.local_addr().unwrap().port();
    let seeder_data = Arc::clone(&data);
    tokio::spawn(async move {
        let mut socket = accept_leecher(&seeder, info_hash, num_pieces).await;
        send(&mut socket, PeerMessage::Unchoke).await;
        // Its first message is our DHT port, which `serve` ignores.
        serve(socket, &seeder_data).await;
    });

    let dht_node = |routers: Vec<String>| async move {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        Dht::start(socket, routers).unwrap()
    };
    let hub = dht_node(Vec::new()).await;
    let routers = vec![hub.local_addr().to_string()];
    let holder = dht_node(routers.clone()).await;
    holder.bootstrap().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while hub.node_count() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the hub never added the holder");
    let seeder_dht = dht_node(routers.clone()).await;
    seeder_dht.get_peers(info_hash, Some(seeder_port)).await;

    let out = tempfile::tempdir().unwrap();
    let mut session = session(torrent, info_hash, out.path());
    Arc::get_mut(&mut session).unwrap().dht = Some(dht_node(routers).await);
    tokio::time::timeout(
        Duration::from_secs(20),
        download_from_peers(
            session,
            &vec![false; num_pieces],
            &[],
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
}

/// `listen` listens over IPv6 too, on the port the IPv4 listener got. Two
/// seeders connect to us on that port, one over `127.0.0.1` with piece 0
/// and one over `[::1]` with the rest, so the download needs both. The one
/// peer we know of, which keeps the download from running out of peers
/// until the seeders arrive, is reached over IPv6 as well. Skipped where
/// IPv6 is unavailable.
#[tokio::test]
async fn downloads_from_peers_that_connect_over_ipv4_and_ipv6() {
    init_tracing();
    if TcpListener::bind("[::1]:0").await.is_err() {
        eprintln!("skipping: IPv6 is not available");
        return;
    }
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    assert_eq!(num_pieces, 3);

    let empty = TcpListener::bind("[::1]:0").await.unwrap();
    let empty_addr = empty.local_addr().unwrap();
    tokio::spawn(async move {
        let mut socket = accept_partial(&empty, info_hash, num_pieces, &[]).await;
        while read_message(&mut socket).await.is_some() {}
    });

    let listeners = listen(0).await;
    let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    assert_eq!(addrs.len(), 2, "not listening over both: {addrs:?}");
    assert!(addrs[0].is_ipv4() && addrs[1].is_ipv6(), "{addrs:?}");
    assert_eq!(addrs[0].port(), addrs[1].port());
    let port = addrs[0].port();

    for (ip, has) in [
        (IpAddr::from(Ipv4Addr::LOCALHOST), vec![0]),
        (IpAddr::from(Ipv6Addr::LOCALHOST), vec![1, 2]),
    ] {
        let seeder_data = Arc::clone(&data);
        tokio::spawn(async move {
            let our_addr = SocketAddr::new(ip, port);
            let mut socket = connect_partial(our_addr, info_hash, num_pieces, &has).await;
            send(&mut socket, PeerMessage::Unchoke).await;
            serve(socket, &seeder_data).await;
        });
    }

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &vec![false; num_pieces],
            &[empty_addr],
            10,
            listeners,
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    assert_files_match(out.path(), &data);
}
