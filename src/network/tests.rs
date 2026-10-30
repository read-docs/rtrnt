use super::*;
use tokio::net::TcpListener;

#[test]
fn invalid_proxy_options_are_rejected() {
    for proxy in [
        "http://127.0.0.1:1080",
        "socks5h://localhost:1080",
        "socks5h://127.0.0.1",
        "socks5h://user:secret@127.0.0.1:1080",
        "socks5h://127.0.0.1:0",
    ] {
        assert!(Policy::new(None, None, Some(proxy)).is_err(), "{proxy}");
    }
    assert!(Policy::new(None, Some("rtrnt-no-such-interface".into()), None).is_err());
}

#[tokio::test]
async fn explicit_binding_uses_the_source_and_refuses_other_families() {
    let policy = Policy::new(Some(Ipv4Addr::LOCALHOST.into()), None, None).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream = policy
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    assert_eq!(
        stream.local_addr().unwrap().ip(),
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    );
    assert!(policy.bind_addr(true, 0).is_err());
    assert_eq!(
        policy
            .udp_socket(false, 0)
            .unwrap()
            .local_addr()
            .unwrap()
            .ip(),
        IpAddr::V4(Ipv4Addr::LOCALHOST)
    );
}

#[tokio::test]
async fn socks_routes_peer_bytes_and_disables_udp() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let policy = Policy::new(
        None,
        None,
        Some(&format!("socks5h://{}", listener.local_addr().unwrap())),
    )
    .unwrap();
    assert!(policy.udp_socket(false, 0).is_err());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).await.unwrap();
        assert_eq!(greeting, [5, 1, 0]);
        stream.write_all(&[5, 0]).await.unwrap();
        let mut request = [0; 10];
        stream.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [5, 1, 0, 1, 192, 0, 2, 1, 0x1a, 0xe1]);
        stream
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut message = [0; 4];
        stream.read_exact(&mut message).await.unwrap();
        assert_eq!(&message, b"peer");
    });
    let mut stream = policy
        .connect("192.0.2.1:6881".parse().unwrap())
        .await
        .unwrap();
    stream.write_all(b"peer").await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn a_refusing_proxy_never_connects_to_the_peer_directly() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let policy = Policy::new(
        None,
        None,
        Some(&format!("socks5h://{}", proxy.local_addr().unwrap())),
    )
    .unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.unwrap();
        let mut greeting = [0; 3];
        stream.read_exact(&mut greeting).await.unwrap();
        stream.write_all(&[5, 0xff]).await.unwrap();
    });
    assert!(policy.connect(peer.local_addr().unwrap()).await.is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_proxy_resolves_the_tracker_name_remotely() {
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let policy = Policy::new(
        None,
        None,
        Some(&format!("socks5h://{}", proxy.local_addr().unwrap())),
    )
    .unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = proxy.accept().await.unwrap();
        // reqwest may advertise more than one authentication method.
        assert_eq!(stream.read_u8().await.unwrap(), 5);
        let n = stream.read_u8().await.unwrap();
        let mut methods = vec![0; n.into()];
        stream.read_exact(&mut methods).await.unwrap();
        assert!(methods.contains(&0));
        stream.write_all(&[5, 0]).await.unwrap();
        let mut head = [0; 4];
        stream.read_exact(&mut head).await.unwrap();
        assert_eq!(head, [5, 1, 0, 3]);
        let n = stream.read_u8().await.unwrap();
        let mut name = vec![0; n.into()];
        stream.read_exact(&mut name).await.unwrap();
        assert_eq!(&name, b"tracker.invalid");
        assert_eq!(stream.read_u16().await.unwrap(), 80);
        stream
            .write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0])
            .await
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(stream.read_u8().await.unwrap());
        }
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .unwrap();
    });
    let response = policy
        .http_builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap()
        .get("http://tracker.invalid/announce")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), "ok");
    server.await.unwrap();
}

#[tokio::test]
async fn an_unavailable_source_address_never_falls_back_to_any_address() {
    let policy = Policy::new(Some("192.0.2.99".parse().unwrap()), None, None).unwrap();
    let peer = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(policy.connect(peer.local_addr().unwrap()).await.is_err());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), peer.accept())
            .await
            .is_err()
    );
}
