use rtrnt::download::download_from_peers;
use rtrnt::peer::fast;
use rtrnt::peer::{BlockRef, PeerMessage, generate_peer_id, handshake_bytes, parse_handshake};
use std::future::pending;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::common::*;

/// Accepts the client's connection as a peer that supports the fast
/// extension (BEP 6), checking that the client does too.
async fn accept_fast(listener: &TcpListener, info_hash: [u8; 20]) -> TcpStream {
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert!(
        parse_handshake(&handshake, &info_hash)
            .unwrap()
            .supports_fast()
    );
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    socket
}

/// Connects to the client listening at `addr` as a peer that supports the
/// fast extension (BEP 6), checking that the client does too.
async fn connect_fast(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    let mut handshake = [0u8; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert!(
        parse_handshake(&handshake, &info_hash)
            .unwrap()
            .supports_fast()
    );
    socket
}

fn reject(index: u32, begin: u32, length: u32) -> PeerMessage {
    PeerMessage::RejectRequest(BlockRef {
        index,
        begin,
        length,
    })
}

/// Several messages encoded back to back, to send in one write.
fn encode_all(messages: &[PeerMessage]) -> Vec<u8> {
    messages.iter().flat_map(PeerMessage::encode).collect()
}

/// A fast peer (BEP 6) with no pieces connects to us while we download
/// piece 1. It says Have None, and gets our Bitfield and then our allowed
/// fast set, which in a torrent of three pieces is all of them. It gets an
/// allowed fast block while still choked, and once unchoked, a Reject for a
/// request it cancels before the block was sent.
#[tokio::test]
async fn serves_a_fast_peer_its_allowed_fast_pieces_while_choked() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let (release, release_rx) = oneshot::channel();
    let seeder_addr = gated_seeder(Arc::clone(&data), info_hash, num_pieces, release_rx).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let our_addr = listener.local_addr().unwrap();
    let leecher_data = Arc::clone(&data);
    let leecher = tokio::spawn(async move {
        let data = &leecher_data;
        let mut socket = connect_fast(our_addr, info_hash).await;
        send(&mut socket, PeerMessage::HaveNone).await;
        let bitfield = vec![0b1010_0000];
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { bitfield })
        );
        let set = fast::allowed_fast_set(
            IpAddr::from(Ipv4Addr::LOCALHOST),
            &info_hash,
            num_pieces as u32,
            fast::ALLOWED_FAST_COUNT,
        );
        assert_eq!(set.len(), num_pieces);
        for piece_index in set {
            assert_eq!(
                next_message(&mut socket).await,
                Some(PeerMessage::AllowedFast { piece_index })
            );
        }

        // Choked, and not even interested, but piece 0 is allowed fast.
        send(&mut socket, request(0, 0, BLOCK)).await;
        assert_eq!(
            next_message(&mut socket).await,
            Some(piece(data, 0, 0, BLOCK))
        );

        send(&mut socket, PeerMessage::Interested).await;
        assert_eq!(next_message(&mut socket).await, Some(PeerMessage::Unchoke));
        // The cancel arrives with the requests, before its block is sent.
        let batch = [
            request(2, 0, BLOCK),
            request(0, BLOCK, BLOCK),
            cancel(0, BLOCK, BLOCK),
        ];
        send_batch(&mut socket, &batch).await;
        let answers = [
            next_message(&mut socket).await.unwrap(),
            next_message(&mut socket).await.unwrap(),
        ];
        for expected in [reject(0, BLOCK, BLOCK), piece(data, 2, 0, BLOCK)] {
            assert!(answers.contains(&expected), "missing {expected:?}");
        }

        release.send(()).unwrap();
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Have { piece_index: 1 })
        );
    });

    tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[seeder_addr],
            10,
            vec![listener],
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out")
    .expect("download failed");
    leecher.await.unwrap();
    assert_files_match(out.path(), &data);
}

/// The next message from a split-off reader, or `None` after `wait` of
/// silence or once the client hangs up.
async fn recv_within(
    messages: &mut mpsc::UnboundedReceiver<PeerMessage>,
    wait: Duration,
) -> Option<PeerMessage> {
    tokio::time::timeout(wait, messages.recv())
        .await
        .ok()
        .flatten()
}

/// The whole download comes from a fast peer (BEP 6) that has every piece
/// and puts the extension's rules to work:
///
/// - We have nothing yet, so it hears Have None.
/// - While it chokes us we ask only for the piece it allows us fast. When
///   it rejects one of those blocks anyway, we stop asking for the piece
///   until unchoked, and let go of it once nothing of it is outstanding.
/// - A choke no longer drops our requests: a block sent after the Choke is
///   still taken, and the rest wait for their Rejects.
/// - A block rejected while we are unchoked is asked for again.
#[tokio::test]
async fn downloads_from_a_fast_peer_that_chokes_and_rejects() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seeder_addr = listener.local_addr().unwrap();
    let seeder_data = Arc::clone(&data);
    let seeder = tokio::spawn(async move {
        let data = &seeder_data;
        let socket = accept_fast(&listener, info_hash).await;
        // Read on a separate task, so waiting for the next message can time
        // out without losing part of one.
        let (mut reader, mut writer) = socket.into_split();
        let (messages_tx, mut messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(message) = read_message(&mut reader).await {
                if !matches!(
                    message,
                    PeerMessage::KeepAlive | PeerMessage::Extended { .. }
                ) {
                    let _ = messages_tx.send(message);
                }
            }
        });
        let long = Duration::from_secs(5);
        let quiet = Duration::from_millis(300);

        let hello = [
            PeerMessage::HaveAll,
            PeerMessage::AllowedFast { piece_index: 1 },
        ];
        writer.write_all(&encode_all(&hello)).await.unwrap();
        // A seed gets no allowed fast set from us.
        for expected in [PeerMessage::HaveNone, PeerMessage::Interested] {
            assert_eq!(recv_within(&mut messages, long).await, Some(expected));
        }
        for expected in [request(1, 0, BLOCK), request(1, BLOCK, BLOCK)] {
            assert_eq!(recv_within(&mut messages, long).await, Some(expected));
        }
        let answer = [reject(1, BLOCK, BLOCK), piece(data, 1, 0, BLOCK)];
        writer.write_all(&encode_all(&answer)).await.unwrap();
        assert_eq!(
            recv_within(&mut messages, quiet).await,
            None,
            "asked for a rejected piece while choked"
        );

        // Unchoked: everything the pipeline allows is asked for.
        let unchoke = PeerMessage::Unchoke.encode();
        writer.write_all(&unchoke).await.unwrap();
        let mut asked = Vec::new();
        while let Some(message) = recv_within(&mut messages, quiet).await {
            match message {
                PeerMessage::Request(BlockRef {
                    index,
                    begin,
                    length,
                }) => asked.push((index, begin, length)),
                other => panic!("expected a request, got {other:?}"),
            }
        }
        assert!(asked.len() >= 2, "asked for {asked:?}");
        let mut answer = vec![PeerMessage::Choke];
        for (i, &(index, begin, length)) in asked.iter().enumerate() {
            answer.push(if i == 0 {
                piece(data, index, begin, length)
            } else {
                reject(index, begin, length)
            });
        }
        writer.write_all(&encode_all(&answer)).await.unwrap();
        assert_eq!(
            recv_within(&mut messages, quiet).await,
            None,
            "asked for a block while choked"
        );

        // Unchoked again: serves everything, but rejects the first request
        // once, so it must be asked for again.
        writer.write_all(&unchoke).await.unwrap();
        let mut rejected = None;
        let mut asked_again = false;
        while let Some(message) = recv_within(&mut messages, long).await {
            let PeerMessage::Request(BlockRef {
                index,
                begin,
                length,
            }) = message
            else {
                continue;
            };
            let reply = match rejected {
                None => {
                    rejected = Some((index, begin, length));
                    reject(index, begin, length)
                }
                Some(first) => {
                    asked_again |= first == (index, begin, length);
                    piece(data, index, begin, length)
                }
            };
            writer.write_all(&reply.encode()).await.unwrap();
        }
        asked_again
    });

    let out = tempfile::tempdir().unwrap();
    tokio::time::timeout(
        Duration::from_secs(20),
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
    assert!(seeder.await.unwrap(), "rejected block never asked again");
}

/// We have pieces 0 and 2, and the only peer is a fast peer (BEP 6) that
/// has every piece and keeps us choked. Its request while we choke it gets
/// a Reject rather than silence, and a Reject for a request we never sent
/// gets it banned.
#[tokio::test]
async fn rejects_a_choked_fast_peer_and_bans_one_that_rejects_unsent_requests() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let out = tempfile::tempdir().unwrap();
    write_files(out.path(), &data);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let mut socket = accept_fast(&listener, info_hash).await;
        send(&mut socket, PeerMessage::HaveAll).await;
        let bitfield = vec![0b1010_0000];
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Bitfield { bitfield })
        );
        assert_eq!(
            next_message(&mut socket).await,
            Some(PeerMessage::Interested)
        );
        send(&mut socket, request(0, 0, BLOCK)).await;
        assert_eq!(next_message(&mut socket).await, Some(reject(0, 0, BLOCK)));
        send(&mut socket, reject(1, 0, BLOCK)).await;
        assert_eq!(next_message(&mut socket).await, None, "not banned");
    });

    let result = tokio::time::timeout(
        Duration::from_secs(10),
        download_from_peers(
            session(torrent, info_hash, out.path()),
            &[true, false, true],
            &[peer_addr],
            10,
            Vec::new(),
            None,
            pending(),
        ),
    )
    .await
    .expect("download timed out");
    assert!(result.is_err(), "downloaded without a peer");
    peer.await.unwrap();
}

/// A fast peer (BEP 6) takes our requests, answers the first, chokes us, and
/// from then on only sends keep-alives, so that we never time it out for
/// silence. The requests it ignores are cancelled after a minute, and it is
/// dropped for not unchoking us after another half. Time is paused once the
/// peer is connected, so the minutes pass at once.
#[tokio::test]
async fn cancels_requests_a_choked_fast_peer_never_answers() {
    init_tracing();
    let data = Arc::new(test_data());
    let (torrent, info_hash) = make_torrent(&data);
    let num_pieces = torrent.num_pieces();
    let out = tempfile::tempdir().unwrap();
    let session = session(torrent, info_hash, out.path());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer_addr = listener.local_addr().unwrap();
    let peer_data = Arc::clone(&data);
    let peer = tokio::spawn(async move {
        let socket = accept_fast(&listener, info_hash).await;
        let (mut reader, mut writer) = socket.into_split();
        let (messages_tx, mut messages) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(message) = read_message(&mut reader).await {
                let _ = messages_tx.send(message);
            }
        });
        writer
            .write_all(&encode_all(&[PeerMessage::HaveAll, PeerMessage::Unchoke]))
            .await
            .unwrap();

        let mut requested = Vec::new();
        let mut answered = None;
        let mut cancelled = Vec::new();
        let mut ticks = tokio::time::interval(Duration::from_secs(10));
        let mut round = 0;
        // Whether the client hung up, rather than we gave up waiting.
        let hung_up = loop {
            tokio::select! {
                message = messages.recv() => match message {
                    None => break true,
                    Some(PeerMessage::Request(BlockRef { index, begin, length })) => {
                        requested.push((index, begin, length));
                        if answered.is_none() {
                            // Connected, so the clock can stop without the
                            // connection timing out while it is set up.
                            tokio::time::pause();
                            ticks.reset();
                            answered = Some((index, begin, length));
                            let reply = piece(&peer_data, index, begin, length);
                            writer.write_all(&reply.encode()).await.unwrap();
                        }
                    }
                    Some(PeerMessage::Cancel(BlockRef { index, begin, length })) => {
                        cancelled.push((index, begin, length));
                    }
                    Some(_) => {}
                },
                _ = ticks.tick() => {
                    round += 1;
                    // Half a minute in, the choke; keep-alives otherwise.
                    let message = if round == 4 {
                        PeerMessage::Choke
                    } else {
                        PeerMessage::KeepAlive
                    };
                    let _ = writer.write_all(&message.encode()).await;
                    if round > 20 {
                        break false;
                    }
                }
            }
        };
        (requested, answered, cancelled, hung_up)
    });

    let have = vec![false; num_pieces];
    let peers = [peer_addr];
    let (mut requested, answered, mut cancelled, hung_up) = tokio::select! {
        result = download_from_peers(
            session,
            &have,
            &peers,
            10,
            Vec::new(),
            None,
            pending(),
        ) => panic!("download ended before the peer was done: {result:?}"),
        done = peer => done.unwrap(),
    };
    let answered = answered.expect("no request came");
    requested.retain(|request| *request != answered);
    requested.sort_unstable();
    cancelled.sort_unstable();
    assert!(!requested.is_empty(), "nothing was left unanswered");
    assert_eq!(cancelled, requested, "unanswered requests not cancelled");
    assert!(hung_up, "kept waiting for a peer that never unchokes us");
}
