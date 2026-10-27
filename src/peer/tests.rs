use super::*;
use crate::peer::extension::{self, Advertised, ExtendedHandshake};
use crate::peer::fast;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

#[test]
fn roundtrip_all_messages() {
    let messages = [
        PeerMessage::KeepAlive,
        PeerMessage::Choke,
        PeerMessage::Unchoke,
        PeerMessage::Interested,
        PeerMessage::NotInterested,
        PeerMessage::Have { piece_index: 42 },
        PeerMessage::Bitfield {
            bitfield: vec![0b1010_0000, 0xff],
        },
        PeerMessage::Request {
            index: 1,
            begin: 16384,
            length: 16384,
        },
        PeerMessage::Piece {
            index: 1,
            begin: 0,
            block: vec![1, 2, 3],
        },
        PeerMessage::Piece {
            index: 1,
            begin: 0,
            block: vec![],
        },
        PeerMessage::Cancel {
            index: 1,
            begin: 2,
            length: 3,
        },
        PeerMessage::Port { port: 6881 },
        PeerMessage::Extended {
            id: 0,
            payload: b"de".to_vec(),
        },
        PeerMessage::Extended {
            id: 3,
            payload: vec![],
        },
        PeerMessage::SuggestPiece { piece_index: 7 },
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::RejectRequest {
            index: 1,
            begin: 16384,
            length: 16384,
        },
        PeerMessage::AllowedFast { piece_index: 9 },
        PeerMessage::Unknown { id: 21 },
    ];
    for message in messages {
        let frame = message.encode();
        let len = u32::from_be_bytes(frame[..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4);
        assert_eq!(PeerMessage::decode(&frame[4..]).unwrap(), message);
    }
}

#[test]
fn rejects_malformed_payloads() {
    let cases: &[&[u8]] = &[
        &[0, 1],
        &[4, 0, 0, 0],
        &[6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        &[7, 0, 0, 0, 0, 0, 0, 0],
        &[8],
        &[9, 1],
        &[20],
        &[0x0d, 0, 0, 0],
        &[0x0e, 0],
        &[0x0f, 0],
        &[0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        &[0x11, 0, 0, 0, 0, 0],
    ];
    for case in cases {
        assert!(PeerMessage::decode(case).is_err(), "accepted {:?}", case);
    }
}

#[test]
fn handshake_roundtrip() {
    let info_hash = [3u8; 20];
    let peer_id = generate_peer_id();
    assert!(peer_id.starts_with(PEER_ID_PREFIX));
    assert!(peer_id[8..].iter().all(u8::is_ascii_alphanumeric));

    let handshake = handshake_bytes(&info_hash, &peer_id);
    assert_eq!(handshake[20..28], [0, 0, 0, 0, 0, 0x10, 0, 0x05]);
    let parsed = parse_handshake(&handshake, &info_hash).unwrap();
    assert_eq!(parsed.peer_id, peer_id);
    assert!(parsed.supports_extensions());
    assert!(parsed.supports_dht());
    assert!(parsed.supports_fast());
    assert!(parse_handshake(&handshake, &[4u8; 20]).is_err());

    let mut bad = handshake;
    bad[5] = b'x';
    assert!(parse_handshake(&bad, &info_hash).is_err());

    // Other clients' bits are recorded, and the extension bit read alone.
    let mut other = handshake;
    other[20..28].copy_from_slice(&[0x80, 0, 0, 0, 0, 0xef, 0, 0x0b]);
    let parsed = parse_handshake(&other, &info_hash).unwrap();
    assert_eq!(parsed.reserved, [0x80, 0, 0, 0, 0, 0xef, 0, 0x0b]);
    assert!(!parsed.supports_extensions());
    assert!(!parsed.supports_fast());
    assert!(parsed.supports_dht());
}

/// What our extension handshake says when we listen on `port`.
fn listening(port: Option<u16>) -> Advertised {
    Advertised {
        listen_port: port,
        ..Advertised::default()
    }
}

/// A mock peer's handshake, with the extension protocol bit cleared
/// unless `extensions`.
fn remote_handshake(info_hash: &[u8; 20], extensions: bool) -> [u8; 68] {
    let mut handshake = handshake_bytes(info_hash, &[1u8; 20]);
    if !extensions {
        handshake[20 + extension::RESERVED_BYTE] = 0;
    }
    handshake
}

#[tokio::test]
async fn sends_interest_changes_and_keep_alives_only_when_needed() {
    let info_hash = [5u8; 20];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        // Without the extension protocol, so no extension handshake.
        let reply = remote_handshake(&info_hash, false);
        socket.write_all(&reply).await.unwrap();
        let mut received = Vec::new();
        while let Ok(len) = socket.read_u32().await {
            let mut payload = vec![0u8; len as usize];
            socket.read_exact(&mut payload).await.unwrap();
            received.push(PeerMessage::decode(&payload).unwrap());
        }
        received
    });

    let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, listening(Some(6881)))
        .await
        .unwrap();
    assert!(!conn.supports_extensions());
    assert!(conn.supports_dht());
    conn.keep_alive().await.unwrap();
    conn.set_interested(false).await.unwrap();
    conn.set_interested(true).await.unwrap();
    conn.set_interested(true).await.unwrap();
    conn.set_interested(false).await.unwrap();
    conn.keep_alive().await.unwrap();
    conn.last_sent = Instant::now().checked_sub(KEEPALIVE_INTERVAL).unwrap();
    conn.keep_alive().await.unwrap();
    conn.keep_alive().await.unwrap();
    drop(conn);

    assert_eq!(
        remote.await.unwrap(),
        [
            PeerMessage::Interested,
            PeerMessage::NotInterested,
            PeerMessage::KeepAlive,
        ]
    );
}

#[test]
fn bitfield_encoding_matches_decoding() {
    let has = [
        true, false, false, true, false, false, false, false, true, true,
    ];
    let bitfield = encode_bitfield(&has);
    assert_eq!(bitfield, [0b1001_0000, 0b1100_0000]);
    assert!(encode_bitfield(&[]).is_empty());
    assert_eq!(encode_bitfield(&[false; 9]), [0, 0]);
}

/// Connects to a mock peer that answers the handshake, sends
/// `messages`, and returns everything it receives until we hang up. The
/// mock supports the fast extension if `fast`.
async fn mock_peer(
    num_pieces: usize,
    fast: bool,
    messages: Vec<PeerMessage>,
) -> (PeerConnection, tokio::task::JoinHandle<Vec<PeerMessage>>) {
    let info_hash = [5u8; 20];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let remote = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        // Without the extension protocol, so no extension handshake.
        let mut reply = remote_handshake(&info_hash, false);
        if !fast {
            reply[20 + fast::RESERVED_BYTE] &= !fast::RESERVED_BIT;
        }
        socket.write_all(&reply).await.unwrap();
        for message in messages {
            socket.write_all(&message.encode()).await.unwrap();
        }
        let mut received = Vec::new();
        while let Ok(len) = socket.read_u32().await {
            let mut payload = vec![0u8; len as usize];
            socket.read_exact(&mut payload).await.unwrap();
            received.push(PeerMessage::decode(&payload).unwrap());
        }
        received
    });
    let conn = PeerConnection::connect(addr, info_hash, [2u8; 20], num_pieces, listening(None))
        .await
        .unwrap();
    (conn, remote)
}

#[tokio::test]
async fn tracks_peer_interest_and_sends_choke_changes() {
    let has = [true, false, true];
    let messages = vec![
        PeerMessage::Bitfield {
            bitfield: encode_bitfield(&has),
        },
        PeerMessage::Interested,
        PeerMessage::NotInterested,
    ];
    let (mut conn, remote) = mock_peer(3, false, messages).await;
    assert!(!conn.supports_fast());
    assert!(conn.am_choking());
    assert!(!conn.peer_interested());
    conn.receive().await.unwrap();
    assert_eq!(conn.peer_has(), has);
    conn.receive().await.unwrap();
    assert!(conn.peer_interested());
    conn.receive().await.unwrap();
    assert!(!conn.peer_interested());

    conn.set_choking(true).await.unwrap();
    conn.set_choking(false).await.unwrap();
    conn.set_choking(false).await.unwrap();
    assert!(!conn.am_choking());
    conn.set_choking(true).await.unwrap();
    drop(conn);
    assert_eq!(
        remote.await.unwrap(),
        [PeerMessage::Unchoke, PeerMessage::Choke]
    );
}

#[tokio::test]
async fn defers_pieces_until_their_number_is_known() {
    let mut has = [false; 10];
    has[0] = true;
    has[9] = true;
    let messages = vec![
        PeerMessage::Bitfield {
            bitfield: encode_bitfield(&has),
        },
        PeerMessage::Have { piece_index: 2 },
        // Out of range once the count is known, so ignored.
        PeerMessage::Have { piece_index: 99 },
    ];
    let (mut conn, _remote) = mock_peer(0, false, messages).await;
    conn.defer_pieces();
    for _ in 0..3 {
        conn.receive().await.unwrap();
    }
    assert!(conn.peer_has().is_empty());
    conn.set_piece_count(10).unwrap();
    has[2] = true;
    assert_eq!(conn.peer_has(), has);

    // A bitfield that does not fit the torrent.
    let messages = vec![PeerMessage::Bitfield {
        bitfield: vec![0xff],
    }];
    let (mut conn, _remote) = mock_peer(0, false, messages).await;
    conn.defer_pieces();
    conn.receive().await.unwrap();
    assert!(conn.set_piece_count(10).is_err());

    // The fast extension's messages wait too.
    let messages = vec![
        PeerMessage::AllowedFast { piece_index: 1 },
        PeerMessage::HaveNone,
        PeerMessage::HaveAll,
        PeerMessage::AllowedFast { piece_index: 7 },
    ];
    let (mut conn, _remote) = mock_peer(0, true, messages).await;
    conn.defer_pieces();
    for _ in 0..4 {
        conn.receive().await.unwrap();
    }
    assert!(conn.peer_has().is_empty() && conn.allowed_fast().is_empty());
    conn.set_piece_count(3).unwrap();
    assert_eq!(conn.peer_has(), [true; 3]);
    assert_eq!(conn.allowed_fast(), [false, true, false]);
}

#[tokio::test]
async fn accept_answers_only_handshakes_for_our_torrent_from_others() {
    let info_hash = [5u8; 20];
    let ours = [2u8; 20];
    let theirs = [1u8; 20];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // (their info hash, their peer id, accepted, answered)
    let cases = [
        (info_hash, theirs, true, true),
        ([6u8; 20], theirs, false, false),
        (info_hash, ours, false, true),
    ];
    for (their_hash, their_id, accepted, answered) in cases {
        let remote = tokio::spawn(async move {
            let mut socket = TcpStream::connect(addr).await.unwrap();
            let handshake = handshake_bytes(&their_hash, &their_id);
            socket.write_all(&handshake).await.unwrap();
            let mut reply = [0u8; 68];
            socket.read_exact(&mut reply).await.ok().map(|_| reply)
        });
        let (socket, from) = listener.accept().await.unwrap();
        let result =
            PeerConnection::accept(socket, from, info_hash, ours, 1, listening(None)).await;
        assert_eq!(result.is_ok(), accepted, "{:?}", result.as_ref().err());
        if let Ok(conn) = &result {
            assert_eq!(conn.peer_id, theirs);
            assert_eq!(conn.addr, from);
            assert!(conn.supports_dht());
        }
        drop(result);
        let reply = remote.await.unwrap();
        assert_eq!(reply.is_some(), answered);
        if let Some(reply) = reply {
            assert_eq!(reply, handshake_bytes(&info_hash, &ours));
        }
    }
}

async fn read_message(socket: &mut TcpStream) -> PeerMessage {
    let len = socket.read_u32().await.unwrap();
    let mut payload = vec![0u8; len as usize];
    socket.read_exact(&mut payload).await.unwrap();
    PeerMessage::decode(&payload).unwrap()
}

#[tokio::test]
async fn exchanges_extension_handshakes() {
    let info_hash = [5u8; 20];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let metadata_request = b"d8:msg_typei0e5:piecei0ee".to_vec();
    let remote = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        assert!(
            parse_handshake(&handshake, &info_hash)
                .unwrap()
                .supports_extensions()
        );
        let reply = remote_handshake(&info_hash, true);
        socket.write_all(&reply).await.unwrap();
        // Ours comes first.
        let PeerMessage::Extended { id: 0, payload } = read_message(&mut socket).await else {
            panic!("no extension handshake");
        };
        let ours = ExtendedHandshake::decode(&payload).unwrap();
        let theirs = ExtendedHandshake {
            extensions: [("ut_metadata".to_string(), 3)].into(),
            client: Some("mock 1.0".into()),
            request_queue: Some(7),
            ..Default::default()
        };
        let message = PeerMessage::Extended {
            id: 0,
            payload: theirs.encode().unwrap(),
        };
        socket.write_all(&message.encode()).await.unwrap();
        (ours, read_message(&mut socket).await)
    });

    let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, listening(Some(6881)))
        .await
        .unwrap();
    assert!(conn.supports_extensions());
    assert!(conn.peer_extensions().is_none());
    assert_eq!(conn.request_limit(), None);
    assert!(conn.send_extended("ut_metadata", vec![]).await.is_err());

    let message = conn.receive().await.unwrap();
    assert!(matches!(message, PeerMessage::Extended { id: 0, .. }));
    assert_eq!(conn.request_limit(), Some(7));
    let theirs = conn.peer_extensions().unwrap();
    assert_eq!(theirs.client.as_deref(), Some("mock 1.0"));
    assert_eq!(theirs.id("ut_metadata"), Some(3));
    assert!(conn.send_extended("ut_pex", vec![]).await.is_err());
    conn.send_extended("ut_metadata", metadata_request.clone())
        .await
        .unwrap();

    let (ours, next) = remote.await.unwrap();
    let localhost = "127.0.0.1".parse().unwrap();
    assert_eq!(
        ours,
        ExtendedHandshake::ours(listening(Some(6881)), localhost)
    );
    assert_eq!(
        next,
        PeerMessage::Extended {
            id: 3,
            payload: metadata_request
        }
    );
}

#[tokio::test]
async fn malformed_extension_handshake_is_a_protocol_violation() {
    let info_hash = [5u8; 20];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0u8; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        let reply = remote_handshake(&info_hash, true);
        socket.write_all(&reply).await.unwrap();
        let message = PeerMessage::Extended {
            id: 0,
            payload: b"li7ee".to_vec(),
        };
        socket.write_all(&message.encode()).await.unwrap();
        // Until the other end hangs up.
        socket.read_to_end(&mut Vec::new()).await.unwrap();
    });

    let mut conn = PeerConnection::connect(addr, info_hash, [2u8; 20], 1, listening(None))
        .await
        .unwrap();
    let err = conn.receive().await.unwrap_err();
    assert!(err.is::<ProtocolViolation>(), "{err:#}");
    assert!(conn.peer_extensions().is_none());
}

#[tokio::test]
async fn tracks_have_all_have_none_and_allowed_fast() {
    let messages = vec![
        PeerMessage::HaveAll,
        PeerMessage::AllowedFast { piece_index: 1 },
        // Past the end: ignored.
        PeerMessage::AllowedFast { piece_index: 3 },
        PeerMessage::SuggestPiece { piece_index: 2 },
        PeerMessage::HaveNone,
    ];
    let (mut conn, _remote) = mock_peer(3, true, messages).await;
    assert!(conn.supports_fast());
    conn.receive().await.unwrap();
    assert_eq!(conn.peer_has(), [true; 3]);
    conn.receive().await.unwrap();
    conn.receive().await.unwrap();
    assert_eq!(conn.allowed_fast(), [false, true, false]);
    conn.receive().await.unwrap();
    assert_eq!(conn.receive().await.unwrap(), PeerMessage::HaveNone);
    assert_eq!(conn.peer_has(), [false; 3]);
}

#[tokio::test]
async fn fast_messages_without_the_fast_extension_are_a_protocol_violation() {
    let fast_messages = [
        PeerMessage::HaveAll,
        PeerMessage::HaveNone,
        PeerMessage::SuggestPiece { piece_index: 0 },
        PeerMessage::AllowedFast { piece_index: 0 },
        PeerMessage::RejectRequest {
            index: 0,
            begin: 0,
            length: 16384,
        },
    ];
    for message in fast_messages {
        assert!(message.is_fast());
        let (mut conn, _remote) = mock_peer(3, false, vec![message.clone()]).await;
        let err = conn.receive().await.unwrap_err();
        assert!(err.is::<ProtocolViolation>(), "{message:?}: {err:#}");
        assert_eq!(conn.peer_has(), [false; 3]);
    }
    assert!(!PeerMessage::Choke.is_fast());
}
