use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use url::Url;

enum UrlProtocol {
    UDP,
    HTTP,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub ip: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug)]
struct TrackerResponse {
    #[serde(default)]
    interval: Option<u64>,
    peers: serde_bytes::ByteBuf,
}

pub async fn announce(
    announce_url: &str,
    info_hash: [u8; 20],
    downloaded: u64,
    uploaded: u64,
    left: u64,
) -> Result<Vec<Peer>> {
    let (url, protocol) = format_url(announce_url, info_hash, downloaded, uploaded, left)?;
    let peers = match protocol {
        UrlProtocol::UDP => udp_connect(&url, info_hash, downloaded, uploaded, left).await?,
        UrlProtocol::HTTP => announce_http(&url).await?,
    };
    Ok(peers)
}

async fn udp_connect(
    url: &str,
    info_hash: [u8; 20],
    downloaded: u64,
    uploaded: u64,
    left: u64,
) -> Result<Vec<Peer>> {
    let parsed = Url::parse(url)?;
    let host = parsed.host_str().ok_or_else(|| anyhow!("No host"))?;
    let port = parsed.port().ok_or_else(|| anyhow!("No port"))?;

    let addr = format!("{}:{}", host, port);
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    socket.connect(&addr).await?;
    let magic_number: u64 = 41727101980;
    let transaction_id = rand::random::<u32>();

    let mut connect_buf = Vec::new();
    connect_buf.extend_from_slice(&magic_number.to_be_bytes());
    connect_buf.extend_from_slice(&0u32.to_be_bytes());
    connect_buf.extend_from_slice(&transaction_id.to_be_bytes());
    socket.send(&connect_buf).await?;
    let mut response = vec![0u8; 16];
    let n = socket.recv(&mut response).await?;
    if n > 16 {
        return Err(anyhow!("Invalid connect response"));
    }
    let action = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    let recv_trans_id = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);
    if action != 0 {
        return Err(anyhow!("Wrong action in connect response"));
    }

    if recv_trans_id != transaction_id {
        return Err(anyhow!("Transaction ID missmatch"));
    }

    let connection_id_bytes: [u8; 8] = response[8..16].try_into()?;
    let connection_id = u64::from_be_bytes(connection_id_bytes);
    let peers = announce_udp(
        &socket,
        connection_id,
        info_hash,
        downloaded,
        uploaded,
        left,
    )
    .await?;
    Ok(peers)
}

async fn announce_udp(
    socket: &UdpSocket,
    connection_id: u64,
    info_hash: [u8; 20],
    downloaded: u64,
    uploaded: u64,
    left: u64,
) -> Result<Vec<Peer>> {
    let mut buf = Vec::new();
    let transaction_id = rand::random::<u32>();
    let peer_id = b"-RT0001-123456789012";

    // Build announce request packet
    buf.extend_from_slice(&connection_id.to_be_bytes());
    buf.extend_from_slice(&1u32.to_be_bytes()); // action = 1 (announce)
    buf.extend_from_slice(&transaction_id.to_be_bytes());
    buf.extend_from_slice(&info_hash);
    buf.extend_from_slice(peer_id);
    buf.extend_from_slice(&downloaded.to_be_bytes());
    buf.extend_from_slice(&uploaded.to_be_bytes());
    buf.extend_from_slice(&left.to_be_bytes());
    buf.extend_from_slice(&2u32.to_be_bytes()); // event = 2 (started)
    buf.extend_from_slice(&0u32.to_be_bytes()); // IP = 0 (default)
    buf.extend_from_slice(&rand::random::<u32>().to_be_bytes()); // key
    buf.extend_from_slice(&(-1i32).to_be_bytes()); // num_want = -1 (default)
    buf.extend_from_slice(&6881u16.to_be_bytes()); // port

    // Send announce request
    socket.send(&buf).await?;

    // Receive announce response
    let mut response = vec![0u8; 1024];
    let n = socket.recv(&mut response).await?;
    response.truncate(n);

    // Parse response
    if response.len() < 20 {
        return Err(anyhow!("Response too short"));
    }

    let action = u32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    let recv_trans_id = u32::from_be_bytes([response[4], response[5], response[6], response[7]]);

    if action != 1 {
        return Err(anyhow!("Wrong action in announce response"));
    }

    if recv_trans_id != transaction_id {
        return Err(anyhow!("Transaction ID mismatch in announce"));
    }

    // Extract peers (starting at byte 20)
    let peers_data = &response[20..];
    let peers = parse_compact_peers(peers_data)?;

    Ok(peers)
}

async fn announce_http(url: &str) -> Result<Vec<Peer>> {
    let response = reqwest::get(url).await?;
    let body = response.bytes().await?;
    println!("Response: {:?}", std::str::from_utf8(&body));
    let tracker_response: TrackerResponse = serde_bencode::from_bytes(&body)?;
    let peers = parse_compact_peers(&tracker_response.peers)?;
    Ok(peers)
}

fn url_encode_hash(hash: &[u8; 20]) -> String {
    hash.iter().map(|b| format!("%{:02x}", b)).collect()
}

fn parse_compact_peers(peers_bytes: &[u8]) -> Result<Vec<Peer>> {
    let mut peers = Vec::new();

    for chunk in peers_bytes.chunks_exact(6) {
        let ip = format!("{}.{}.{}.{}", chunk[0], chunk[1], chunk[2], chunk[3]);
        let port = u16::from_be_bytes([chunk[4], chunk[5]]);

        peers.push(Peer { ip, port });
    }

    Ok(peers)
}

fn format_url(
    announce_url: &str,
    info_hash: [u8; 20],
    downloaded: u64,
    uploaded: u64,
    left: u64,
) -> Result<(String, UrlProtocol)> {
    let url_type: UrlProtocol = if announce_url.starts_with("udp://") {
        UrlProtocol::UDP
    } else {
        UrlProtocol::HTTP
    };

    let url = match url_type {
        UrlProtocol::UDP => announce_url.to_string(),
        UrlProtocol::HTTP => {
            let peer_id = "-RT0001-123456789012";
            let info_hash_encoded = url_encode_hash(&info_hash);
            let query = format!(
                "info_hash={}&peer_id={}&port=6881&uploaded={}&downloaded={}&left={}&compact=1&event=started",
                info_hash_encoded, peer_id, uploaded, downloaded, left
            );

            let url = if announce_url.contains('?') {
                format!("{}&{}", announce_url, query)
            } else {
                format!("{}?{}", announce_url, query)
            };
            url
        }
    };

    Ok((url, url_type))
}
