use super::*;
use tokio::net::UdpSocket;

#[test]
fn encodes_the_requests() {
    assert_eq!(external_address_request(), [0, 0]);
    let request = map_request(Protocol::Tcp, 6881, 6881, Duration::from_secs(3600));
    assert_eq!(
        request,
        [0, 2, 0, 0, 0x1A, 0xE1, 0x1A, 0xE1, 0, 0, 0x0E, 0x10]
    );
    let request = map_request(Protocol::Udp, 1, 2, Duration::from_secs(7));
    assert_eq!(request, [0, 1, 0, 0, 0, 1, 0, 2, 0, 0, 0, 7]);
}

#[test]
fn a_removal_has_no_lifetime_and_no_suggested_port() {
    let request = map_request(Protocol::Tcp, 6881, 0, Duration::ZERO);
    assert_eq!(request[6..], [0; 6]);
}

#[test]
fn parses_the_external_address() {
    let answer = [0, 128, 0, 0, 0, 0, 0x10, 0, 203, 0, 113, 7];
    let ip = parse_external_address(&answer).unwrap().unwrap();
    assert_eq!(ip, Ipv4Addr::new(203, 0, 113, 7));
}

#[test]
fn parses_a_map_response() {
    // Version 0, TCP (2 + 128), success, epoch 4096, ports 6881 and 6882,
    // a lifetime of 7200 s.
    let answer = [
        0, 130, 0, 0, 0, 0, 0x10, 0, 0x1A, 0xE1, 0x1A, 0xE2, 0, 0, 0x1C, 0x20,
    ];
    let granted = parse_map_response(&answer, Protocol::Tcp, 6881)
        .unwrap()
        .unwrap();
    assert_eq!(granted.external_port, 6882);
    assert_eq!(granted.lifetime, Duration::from_secs(7200));
    assert_eq!(granted.external_ip, None);
}

#[test]
fn a_result_code_is_an_error() {
    let answer = [0, 130, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0];
    let error = parse_map_response(&answer, Protocol::Tcp, 0)
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("not authorized"), "{error}");
    // The short answer to an unsupported version has the code too.
    let short = [0, 128, 0, 1, 0, 0, 0, 1];
    assert!(parse_external_address(&short).unwrap().is_err());
}

#[test]
fn other_packets_are_not_answers() {
    let udp = [
        0, 129, 0, 0, 0, 0, 0, 1, 0, 0x1A, 0xE1, 0x1A, 0xE1, 0, 0, 0xE,
    ];
    // The answer to a UDP request is not the answer to a TCP one.
    assert!(parse_map_response(&udp, Protocol::Tcp, 6881).is_none());
    // Nor is one for another port.
    assert!(parse_map_response(&udp, Protocol::Udp, 1).is_none());
    assert!(parse_map_response(&[], Protocol::Tcp, 1).is_none());
    assert!(parse_map_response(&[2, 130, 0, 0, 0, 0, 0, 0], Protocol::Tcp, 1).is_none());
    assert!(parse_external_address(&[0, 130, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1]).is_none());
}

#[test]
fn a_truncated_success_is_an_error() {
    let answer = [0, 130, 0, 0, 0, 0, 0, 1, 0x1A];
    let error = parse_map_response(&answer, Protocol::Tcp, 6881)
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("too short"), "{error}");
}

/// A gateway on loopback that answers each request as a router would.
async fn fake_gateway() -> (SocketAddr, tokio::task::JoinHandle<Vec<Vec<u8>>>) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let mut seen = Vec::new();
        let mut buf = [0u8; 64];
        // The test sends exactly three requests.
        for _ in 0..3 {
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            let request = buf[..len].to_vec();
            let answer: Vec<u8> = match request[..] {
                [0, 0] => vec![0, 128, 0, 0, 0, 0, 0, 9, 198, 51, 100, 2],
                [0, op, ..] => {
                    let lifetime = &request[8..12];
                    let mut answer = vec![0, 128 + op, 0, 0, 0, 0, 0, 9];
                    answer.extend_from_slice(&request[4..6]);
                    // Grants the port one up, to see the answer is used.
                    let port = u16::from_be_bytes([request[6], request[7]]);
                    let granted = if port == 0 { 0 } else { port + 1 };
                    answer.extend_from_slice(&granted.to_be_bytes());
                    answer.extend_from_slice(lifetime);
                    answer
                }
                _ => unreachable!(),
            };
            seen.push(request);
            socket.send_to(&answer, from).await.unwrap();
        }
        seen
    });
    (address, handle)
}

#[tokio::test]
async fn talks_to_a_gateway() {
    let (address, gateway) = fake_gateway().await;
    let pmp = Pmp::at(address);
    assert_eq!(
        pmp.external_address().await.unwrap(),
        Ipv4Addr::new(198, 51, 100, 2)
    );
    let granted = pmp
        .map(Protocol::Tcp, 6881, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(granted.external_port, 6882);
    assert_eq!(granted.lifetime, Duration::from_secs(3600));
    pmp.unmap(Protocol::Tcp, 6881).await.unwrap();
    let seen = gateway.await.unwrap();
    assert_eq!(seen[0], [0, 0]);
    assert_eq!(seen[1][..2], [0, 2]);
    // The removal has no lifetime.
    assert_eq!(seen[2][8..12], [0, 0, 0, 0]);
}

#[tokio::test]
async fn a_gateway_that_says_no_is_an_error() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 64];
        let (_, from) = socket.recv_from(&mut buf).await.unwrap();
        let answer = [0, 130, 0, 4, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, 0];
        socket.send_to(&answer, from).await.unwrap();
    });
    let error = Pmp::at(address)
        .map(Protocol::Tcp, 6881, Duration::from_secs(60))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("out of resources"), "{error}");
}
