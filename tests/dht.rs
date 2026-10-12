//! DHT nodes talking to each other over loopback: bootstrapping, announcing
//! and finding peers, without touching the internet.

use rtrnt::dht::Dht;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

const INFO_HASH: [u8; 20] = [7; 20];

/// A node on 127.0.0.1 that bootstraps from `routers`.
async fn node(routers: &[SocketAddr]) -> Dht {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let routers = routers.iter().map(SocketAddr::to_string).collect();
    Dht::start(socket, routers).unwrap()
}

/// Polls `done` until it holds, failing after 5 s.
async fn wait_until(what: &str, done: impl Fn() -> bool) {
    let wait = async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .unwrap_or_else(|_| panic!("timed out waiting until {what}"));
}

/// A hub that every node bootstraps from, and nodes that learn of each other
/// through it, all bootstrapped.
async fn network(nodes: usize) -> (Dht, Vec<Dht>) {
    let hub = node(&[]).await;
    let hub_addr = hub.local_addr();
    let mut others = Vec::new();
    for _ in 0..nodes {
        let dht = node(&[hub_addr]).await;
        dht.bootstrap().await;
        others.push(dht);
    }
    // The hub pings each node that queried it before adding it.
    wait_until("the hub knows every node", || hub.node_count() == nodes).await;
    for dht in &others {
        dht.bootstrap().await;
    }
    (hub, others)
}

#[tokio::test]
async fn lookup_finds_peers_announced_by_another_node() {
    let (hub, nodes) = network(6).await;
    let hub_addr = hub.local_addr();
    for dht in &nodes {
        assert!(
            dht.node_count() >= 3,
            "only {} nodes known",
            dht.node_count()
        );
    }

    // The seeder finds nobody, and announces its TCP port to the nodes
    // closest to the info hash.
    let seeder = &nodes[0];
    assert!(seeder.get_peers(INFO_HASH, Some(51413)).await.is_empty());
    // On its UDP port too, as `implied_port`.
    let implied = &nodes[1];
    implied.get_peers(INFO_HASH, Some(implied.port())).await;

    // A newcomer bootstraps from the hub and finds both.
    let leecher = node(&[hub_addr]).await;
    let mut peers = leecher.get_peers(INFO_HASH, None).await;
    peers.sort();
    let mut expected: Vec<SocketAddr> =
        vec!["127.0.0.1:51413".parse().unwrap(), implied.local_addr()];
    expected.sort();
    assert_eq!(peers, expected);
    assert!(leecher.node_count() > 0);
    // Nothing is known for another torrent.
    assert!(leecher.get_peers([8; 20], None).await.is_empty());
}

/// Raw bytes on the wire: garbage is ignored, and a ping from BEP 5 gets the
/// right answer.
#[tokio::test]
async fn answers_raw_queries_and_survives_garbage() {
    let dht = node(&[]).await;
    let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    client.connect(dht.local_addr()).await.unwrap();
    let garbage: &[&[u8]] = &[
        b"",
        b"hello",
        b"d1:ad2:id3:abce1:q4:ping1:t2:aa1:y1:qe",
        b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:zz1:y1:re",
        &[b'l'; 5000],
    ];
    for packet in garbage {
        client.send(packet).await.unwrap();
    }
    client
        .send(b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe")
        .await
        .unwrap();
    let reply = recv_answer(&client).await;
    assert!(reply.starts_with(b"d1:rd2:id20:"), "{reply:?}");
    assert!(
        reply.ends_with(b"e1:t2:aa1:v4:RT\x01\x001:y1:re"),
        "{reply:?}"
    );

    // An unknown method gets error 204.
    client
        .send(b"d1:ad2:id20:abcdefghij0123456789e1:q4:vote1:t2:bb1:y1:qe")
        .await
        .unwrap();
    assert_eq!(
        recv_answer(&client).await,
        b"d1:eli204e14:method unknowne1:t2:bb1:v4:RT\x01\x001:y1:ee"
    );
}

/// Receives the next answer on `client`, skipping the node's own queries:
/// it pings a newcomer before adding it to its table.
async fn recv_answer(client: &UdpSocket) -> Vec<u8> {
    let mut buf = [0u8; 1500];
    let receive = async {
        loop {
            let len = client.recv(&mut buf).await.unwrap();
            if !buf[..len].ends_with(b"1:y1:qe") {
                return buf[..len].to_vec();
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .expect("no answer")
}

/// A node that is not answering is given up on: lookups end instead of
/// hanging, and find nothing.
#[tokio::test]
async fn lookups_end_when_nobody_answers() {
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let dht = node(&[silent.local_addr().unwrap()]).await;
    let peers = tokio::time::timeout(Duration::from_secs(20), dht.get_peers(INFO_HASH, Some(1)))
        .await
        .expect("lookup hung");
    assert!(peers.is_empty());
    assert_eq!(dht.node_count(), 0);
}
