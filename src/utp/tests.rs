use super::*;
use crate::peer::extension::Advertised;
use crate::peer::{BlockRef, Encryption, PeerConnection, PeerMessage, Wire};
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::timeout;

async fn endpoint() -> Arc<Endpoint> {
    Endpoint::start(
        vec![UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap()],
        16,
        Arc::default(),
    )
    .unwrap()
}
fn address(endpoint: &Endpoint) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, endpoint.port()).into()
}

async fn peer_transfer(encryption: Encryption, wire: Wire) {
    let server = endpoint().await;
    let client = endpoint().await;
    let addr = address(&server);
    // Keep the endpoint alive until the client's final flush is ACKed.
    // Reading NotInterested does not mean a delayed transport ACK was sent.
    let server_hold = Arc::clone(&server);
    let hash = [7; 20];
    let payload: Vec<u8> = (0..64 * 1024).map(|n| (n % 251) as u8).collect();
    let expected = payload.clone();
    let responder = tokio::spawn(async move {
        let (socket, from) = server.accept().await.unwrap();
        let mut peer = PeerConnection::accept(
            socket,
            from,
            hash,
            [2; 20],
            1,
            Advertised::default(),
            encryption,
        )
        .await
        .unwrap();
        assert!(peer.is_utp());
        assert_eq!(peer.is_encrypted(), encryption == Encryption::Require);
        loop {
            if let PeerMessage::Request(block) = peer.receive().await.unwrap() {
                assert_eq!(
                    block,
                    BlockRef {
                        index: 0,
                        begin: 0,
                        length: 64 * 1024
                    }
                );
                peer.send(&PeerMessage::Piece {
                    index: 0,
                    begin: 0,
                    block: payload,
                })
                .await
                .unwrap();
                // Keep the endpoint alive until data has been consumed and
                // acknowledged; flushing uTP only queues bytes for transport.
                assert_eq!(peer.receive().await.unwrap(), PeerMessage::NotInterested);
                break;
            }
        }
    });
    let mut peer = PeerConnection::connect_via(
        addr,
        hash,
        [1; 20],
        1,
        Advertised::default(),
        &[wire],
        Some(&client),
        Policy::Require,
    )
    .await
    .unwrap();
    assert!(peer.is_utp());
    peer.send(
        &BlockRef {
            index: 0,
            begin: 0,
            length: 64 * 1024,
        }
        .request(),
    )
    .await
    .unwrap();
    loop {
        if let PeerMessage::Piece { block, .. } = peer.receive().await.unwrap() {
            assert_eq!(block, expected);
            break;
        }
    }
    peer.send(&PeerMessage::NotInterested).await.unwrap();
    responder.await.unwrap();
    drop(server_hold);
}

#[tokio::test]
async fn transfers_peer_messages_over_utp() {
    timeout(
        Duration::from_secs(10),
        peer_transfer(Encryption::Off, Wire::Plaintext),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn mse_encrypts_a_utp_piece_transfer() {
    timeout(
        Duration::from_secs(10),
        peer_transfer(Encryption::Require, Wire::Encrypted),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn prefer_falls_back_to_tcp_when_udp_does_not_answer() {
    let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = server.local_addr().unwrap();
    // Occupy the UDP port without answering, so ICMP timing cannot affect
    // whether the transport's bounded connect timeout drives fallback.
    let _silent = UdpSocket::bind(addr).await.unwrap();
    let client = endpoint().await;
    let responder = tokio::spawn(async move {
        let (socket, addr) = server.accept().await.unwrap();
        PeerConnection::accept(
            socket,
            addr,
            [7; 20],
            [2; 20],
            1,
            Advertised::default(),
            Encryption::Off,
        )
        .await
        .unwrap()
    });
    let peer = timeout(
        Duration::from_secs(9),
        PeerConnection::connect_via(
            addr,
            [7; 20],
            [1; 20],
            1,
            Advertised::default(),
            &[Wire::Plaintext],
            Some(&client),
            Policy::Prefer,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!peer.is_utp());
    assert!(!responder.await.unwrap().is_utp());
}

#[tokio::test]
async fn require_never_uses_tcp_without_an_endpoint() {
    let server = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let result = PeerConnection::connect_via(
        server.local_addr().unwrap(),
        [7; 20],
        [1; 20],
        1,
        Advertised::default(),
        &[Wire::Plaintext],
        None,
        Policy::Require,
    )
    .await;
    assert!(result.is_err());
    assert!(
        timeout(Duration::from_millis(20), server.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dht_and_utp_answer_on_the_same_udp_port() {
    let server = endpoint().await;
    let dht = server.dht().unwrap();
    assert_eq!(server.port(), dht.port());
    assert!(server.dht().is_err());
    let client = endpoint().await;
    let server_addr = address(&server);
    let responder = tokio::spawn({
        let server = Arc::clone(&server);
        async move {
            let (mut stream, _) = server.accept().await.unwrap();
            let mut payload = [0; 5];
            stream.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"hello");
            stream.write_all(b"world").await.unwrap();
            stream.flush().await.unwrap();
            let mut ack = [0];
            stream.read_exact(&mut ack).await.unwrap();
        }
    });
    let mut stream = timeout(Duration::from_secs(2), client.connect(server_addr))
        .await
        .unwrap()
        .unwrap();
    let udp = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    // KRPC ping and uTP data are live at once on the exact same socket.
    let mut ping = b"d1:ad2:id20:".to_vec();
    ping.extend([9; 20]);
    ping.extend(b"e1:q4:ping1:t2:aa1:y1:qe");
    udp.send_to(&ping, server_addr).await.unwrap();
    stream.write_all(b"hello").await.unwrap();
    stream.flush().await.unwrap();
    let mut reply = [0; 1024];
    let (len, from) = timeout(Duration::from_secs(2), udp.recv_from(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from.port(), dht.port());
    assert!(len > 20);
    assert_eq!(reply[0], b'd');
    let mut payload = [0; 5];
    timeout(Duration::from_secs(2), stream.read_exact(&mut payload))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&payload, b"world");
    stream.write_all(&[1]).await.unwrap();
    stream.flush().await.unwrap();
    responder.await.unwrap();
}

#[tokio::test]
async fn simultaneous_outgoing_connections_share_each_existing_port() {
    let a = endpoint().await;
    let b = endpoint().await;
    let a_addr = address(&a);
    let b_addr = address(&b);
    let connected = timeout(Duration::from_secs(3), async {
        tokio::try_join!(a.connect(b_addr), b.connect(a_addr), a.accept(), b.accept())
    })
    .await
    .unwrap()
    .unwrap();
    let (mut a_out, mut b_out, (mut a_in, from_b), (mut b_in, from_a)) = connected;
    assert_eq!(from_b, b_addr);
    assert_eq!(from_a, a_addr);
    a_out.write_all(b"a").await.unwrap();
    a_out.flush().await.unwrap();
    b_out.write_all(b"b").await.unwrap();
    b_out.flush().await.unwrap();
    let mut byte = [0];
    timeout(Duration::from_secs(2), b_in.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&byte, b"a");
    timeout(Duration::from_secs(2), a_in.read_exact(&mut byte))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&byte, b"b");
}

#[tokio::test]
async fn ipv6_utp_uses_its_existing_family_socket() {
    let policy = crate::network::Policy::default();
    let Ok(a_socket) = policy.udp_socket(true, 0) else {
        return;
    };
    let Ok(b_socket) = policy.udp_socket(true, 0) else {
        return;
    };
    let a = Endpoint::start(vec![a_socket], 4, Arc::default()).unwrap();
    let b = Endpoint::start(vec![b_socket], 4, Arc::default()).unwrap();
    let b_addr = (std::net::Ipv6Addr::LOCALHOST, b.port()).into();
    let (mut outgoing, incoming) = timeout(Duration::from_secs(3), async {
        tokio::try_join!(a.connect(b_addr), b.accept())
    })
    .await
    .unwrap()
    .unwrap();
    let (mut incoming, from) = incoming;
    assert!(from.is_ipv6());
    assert_eq!(from.port(), a.port());
    outgoing.write_all(b"ipv6").await.unwrap();
    outgoing.flush().await.unwrap();
    let mut bytes = [0; 4];
    timeout(Duration::from_secs(2), incoming.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&bytes, b"ipv6");
}

#[tokio::test]
async fn blocked_peers_are_refused_before_sending_a_syn() {
    let peer = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let (blocked, _) = Blocklist::parse("127.0.0.0/8");
    let endpoint = Endpoint::start(
        vec![UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap()],
        4,
        Arc::new(blocked),
    )
    .unwrap();
    let result = endpoint.connect(peer.local_addr().unwrap()).await;
    assert!(result.unwrap_err().to_string().contains("blocked"));
    let mut buf = [0; 64];
    assert!(
        timeout(Duration::from_millis(20), peer.recv_from(&mut buf))
            .await
            .is_err()
    );
}
