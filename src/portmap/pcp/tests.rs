use super::*;
use tokio::net::UdpSocket;

const NONCE: [u8; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

#[test]
fn encodes_a_map_request() {
    let request = map_request(
        Ipv4Addr::new(192, 168, 1, 50),
        NONCE,
        Protocol::Tcp,
        6881,
        6881,
        Duration::from_secs(3600),
    );
    assert_eq!(request.len(), 60);
    // Version 2, MAP, and the lifetime.
    assert_eq!(request[..8], [2, 1, 0, 0, 0, 0, 0x0E, 0x10]);
    // The client address, IPv4-mapped.
    assert_eq!(request[8..18], [0; 10]);
    assert_eq!(request[18..24], [0xFF, 0xFF, 192, 168, 1, 50]);
    assert_eq!(request[24..36], NONCE);
    // TCP is protocol 6.
    assert_eq!(request[36..40], [6, 0, 0, 0]);
    assert_eq!(request[40..44], [0x1A, 0xE1, 0x1A, 0xE1]);
    assert_eq!(request[44..60], [0; 16]);
}

#[test]
fn udp_is_protocol_17_and_a_removal_has_no_lifetime() {
    let request = map_request(
        Ipv4Addr::LOCALHOST,
        NONCE,
        Protocol::Udp,
        6881,
        0,
        Duration::ZERO,
    );
    assert_eq!(request[36], 17);
    assert_eq!(request[4..8], [0; 4]);
    assert_eq!(request[42..44], [0, 0]);
}

/// An answer granting `port` for 1800 s on 203.0.113.9, with `code`.
fn answer(code: u8, nonce: [u8; 12], port: u16) -> Vec<u8> {
    let mut packet = vec![0; 60];
    packet[0] = 2;
    packet[1] = 0x81;
    packet[3] = code;
    packet[4..8].copy_from_slice(&1800u32.to_be_bytes());
    packet[24..36].copy_from_slice(&nonce);
    packet[36] = 6;
    packet[40..42].copy_from_slice(&6881u16.to_be_bytes());
    packet[42..44].copy_from_slice(&port.to_be_bytes());
    packet[54..58].copy_from_slice(&[0xFF, 0xFF, 203, 0]);
    packet[58..60].copy_from_slice(&[113, 9]);
    packet
}

#[test]
fn parses_a_map_response() {
    let granted = parse_map_response(&answer(0, NONCE, 6882), NONCE)
        .unwrap()
        .unwrap();
    assert_eq!(granted.external_port, 6882);
    assert_eq!(granted.lifetime, Duration::from_secs(1800));
    assert_eq!(granted.external_ip, Some(Ipv4Addr::new(203, 0, 113, 9)));
}

#[test]
fn a_result_code_is_an_error() {
    let error = parse_map_response(&answer(8, NONCE, 0), NONCE)
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("out of resources"), "{error}");
    // An error comes in a bare header too, whatever the nonce.
    let header = &answer(2, [0; 12], 0)[..24];
    assert!(parse_map_response(header, NONCE).unwrap().is_err());
}

#[test]
fn a_server_of_another_version_is_an_error() {
    // A NAT-PMP gateway's answer to a version 2 request, in its own format.
    let packet = [
        0, 0x81, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    assert!(parse_map_response(&packet, NONCE).unwrap().is_err());
}

#[test]
fn other_packets_are_not_answers() {
    // Another request's nonce.
    assert!(parse_map_response(&answer(0, [9; 12], 6882), NONCE).is_none());
    assert!(parse_map_response(&[], NONCE).is_none());
    // Not a MAP answer.
    let mut other = answer(0, NONCE, 6882);
    other[1] = 0x82;
    assert!(parse_map_response(&other, NONCE).is_none());
    // A success that is cut short.
    assert!(parse_map_response(&answer(0, NONCE, 1)[..40], NONCE).is_none());
}

#[test]
fn the_nonce_is_the_same_for_a_mapping_and_differs_between_them() {
    let pcp = Pcp::at("127.0.0.1:5351".parse().unwrap());
    assert_eq!(
        pcp.nonce(Protocol::Tcp, 6881),
        pcp.nonce(Protocol::Tcp, 6881)
    );
    assert_ne!(
        pcp.nonce(Protocol::Tcp, 6881),
        pcp.nonce(Protocol::Udp, 6881)
    );
    assert_ne!(
        pcp.nonce(Protocol::Tcp, 6881),
        pcp.nonce(Protocol::Tcp, 6882)
    );
}

#[tokio::test]
async fn talks_to_a_server() {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut buf = [0u8; 128];
        let (len, from) = socket.recv_from(&mut buf).await.unwrap();
        assert_eq!(len, 60);
        let nonce: [u8; 12] = buf[24..36].try_into().unwrap();
        let port = u16::from_be_bytes([buf[42], buf[43]]);
        socket.send_to(&answer(0, nonce, port), from).await.unwrap();
        buf[..len].to_vec()
    });
    let granted = Pcp::at(address)
        .map(Protocol::Tcp, 6881, Duration::from_secs(3600))
        .await
        .unwrap();
    assert_eq!(granted.external_port, 6881);
    assert_eq!(granted.external_ip, Some(Ipv4Addr::new(203, 0, 113, 9)));
    let request = server.await.unwrap();
    // The client address is the loopback one the socket used.
    assert_eq!(request[18..24], [0xFF, 0xFF, 127, 0, 0, 1]);
}
