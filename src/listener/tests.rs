use super::*;
use crate::blocklist::Blocklist;
use crate::peer::{generate_peer_id, handshake_bytes};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn listener() -> (Arc<Listener>, SocketAddr) {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    (Listener::start(vec![socket], Arc::default()), addr)
}

/// Connects to `addr` and sends a handshake for `info_hash`.
async fn connect(addr: SocketAddr, info_hash: [u8; 20]) -> TcpStream {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(&handshake_bytes(&info_hash, &generate_peer_id()))
        .await
        .unwrap();
    socket
}

async fn next(registration: &mut Registration) -> Incoming {
    tokio::time::timeout(Duration::from_secs(5), registration.next())
        .await
        .expect("no incoming peer")
}

#[tokio::test]
async fn hands_each_peer_to_the_torrent_it_asks_for() {
    let (listener, addr) = listener().await;
    assert_eq!(listener.port(), Some(addr.port()));
    let mut a = listener.register([1; 20]).unwrap();
    let mut b = listener.register([2; 20]).unwrap();
    assert_eq!(b.port(), Some(addr.port()));

    let _to_b = connect(addr, [2; 20]).await;
    let _to_a = connect(addr, [1; 20]).await;
    assert_eq!(next(&mut b).await.info_hash, [2; 20]);
    assert_eq!(next(&mut a).await.info_hash, [1; 20]);
}

#[tokio::test]
async fn drops_peers_for_torrents_nobody_registered() {
    let (listener, addr) = listener().await;
    let mut a = listener.register([1; 20]).unwrap();

    let mut stranger = connect(addr, [9; 20]).await;
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(&mut stranger, &mut buf),
    )
    .await
    .expect("connection left open");
    assert_eq!(read.unwrap(), 0, "answered a peer for an unknown torrent");

    // Once dropped, the torrent may be registered again.
    drop(a);
    assert!(listener.routes.lock().is_empty());
    a = listener.register([1; 20]).unwrap();
    let _again = connect(addr, [1; 20]).await;
    assert_eq!(next(&mut a).await.info_hash, [1; 20]);
}

#[tokio::test]
async fn a_torrent_is_registered_once() {
    let (listener, _) = listener().await;
    let _a = listener.register([1; 20]).unwrap();
    assert!(listener.register([1; 20]).is_err());
}

#[tokio::test]
async fn without_sockets_nobody_connects() {
    let listener = Listener::start(Vec::new(), Arc::default());
    assert_eq!(listener.port(), None);
    let mut a = listener.register([1; 20]).unwrap();
    let next = tokio::time::timeout(Duration::from_millis(50), a.next()).await;
    assert!(next.is_err());
}

#[tokio::test]
async fn closes_connections_from_blocklisted_addresses() {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = socket.local_addr().unwrap();
    let (list, _) = Blocklist::parse("127.0.0.0/8\n");
    let list = Arc::new(list);
    let listener = Listener::start(vec![socket], Arc::clone(&list));
    let mut a = listener.register([1; 20]).unwrap();

    let mut blocked = connect(addr, [1; 20]).await;
    let mut buf = [0u8; 1];
    let read = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::io::AsyncReadExt::read(&mut blocked, &mut buf),
    )
    .await
    .expect("connection left open");
    assert!(matches!(read, Ok(0) | Err(_)), "answered a blocked peer");
    let handed = tokio::time::timeout(Duration::from_millis(100), a.next()).await;
    assert!(handed.is_err(), "handed over a blocked peer");
    assert_eq!(list.blocked(), 1);
}
