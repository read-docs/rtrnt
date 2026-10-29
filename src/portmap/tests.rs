use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::UdpSocket;

/// A NAT-PMP gateway on loopback that counts the maps and removals it gets.
struct FakeGateway {
    address: SocketAddr,
    maps: Arc<AtomicUsize>,
    removals: Arc<AtomicUsize>,
}

async fn fake_gateway() -> FakeGateway {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let maps = Arc::new(AtomicUsize::new(0));
    let removals = Arc::new(AtomicUsize::new(0));
    let (m, r) = (Arc::clone(&maps), Arc::clone(&removals));
    tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            let request = &buf[..len];
            let answer = match request {
                [0, 0] => vec![0, 128, 0, 0, 0, 0, 0, 1, 198, 51, 100, 2],
                [0, op, ..] if len == 12 => {
                    let lifetime = &request[8..12];
                    if lifetime == [0; 4] {
                        r.fetch_add(1, Ordering::SeqCst);
                    } else {
                        m.fetch_add(1, Ordering::SeqCst);
                    }
                    let mut answer = vec![0, 128 + op, 0, 0, 0, 0, 0, 1];
                    answer.extend_from_slice(&request[4..6]);
                    answer.extend_from_slice(&request[4..6]);
                    answer.extend_from_slice(lifetime);
                    answer
                }
                _ => continue,
            };
            socket.send_to(&answer, from).await.unwrap();
        }
    });
    FakeGateway {
        address,
        maps,
        removals,
    }
}

fn task_with(gateway: &FakeGateway) -> (Task, Arc<Shared>) {
    let shared = Arc::new(Shared {
        status: Mutex::new(Status::Searching),
    });
    let mut task = Task::new(Arc::clone(&shared));
    task.backend = Some(Backend::Pmp(Pmp::at(gateway.address)));
    (task, shared)
}

#[tokio::test]
async fn maps_ports_shows_the_address_and_removes_them_on_shutdown() {
    let gateway = fake_gateway().await;
    let (task, shared) = task_with(&gateway);
    let (commands, receiver) = mpsc::unbounded_channel();
    let handle = tokio::spawn(task.run(receiver));
    commands.send(Command::Map(Protocol::Tcp, 6881)).unwrap();
    commands.send(Command::Map(Protocol::Udp, 6881)).unwrap();
    // The same port twice is one mapping.
    commands.send(Command::Map(Protocol::Tcp, 6881)).unwrap();
    let (done, wait) = oneshot::channel();
    commands.send(Command::Shutdown(done)).unwrap();
    wait.await.unwrap();
    handle.await.unwrap();
    assert_eq!(gateway.maps.load(Ordering::SeqCst), 2);
    assert_eq!(gateway.removals.load(Ordering::SeqCst), 2);
    // The status was last published before the removal.
    assert_eq!(
        shared.get(),
        Status::Mapped {
            via: "NAT-PMP",
            external_ip: Some(Ipv4Addr::new(198, 51, 100, 2)),
            external_port: 6881,
        }
    );
}

#[tokio::test]
async fn renews_after_half_the_lease_and_asks_again_when_due() {
    let gateway = fake_gateway().await;
    let (mut task, _) = task_with(&gateway);
    task.entries.push(Entry {
        protocol: Protocol::Tcp,
        port: 6881,
        active: None,
    });
    let before = Instant::now();
    task.sync().await;
    assert_eq!(gateway.maps.load(Ordering::SeqCst), 1);
    // The lease is an hour, so the renewal is due after half of it.
    let renew_at = task.next_wake().unwrap();
    assert!(renew_at >= before + LEASE / 2);
    assert!(renew_at <= Instant::now() + LEASE / 2);
    // Nothing is asked before then.
    task.sync().await;
    assert_eq!(gateway.maps.load(Ordering::SeqCst), 1);
    // Once it is due, the mapping is asked for again.
    task.entries[0].active.as_mut().unwrap().renew_at = Instant::now();
    task.sync().await;
    assert_eq!(gateway.maps.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_router_that_stops_answering_is_dropped_and_looked_for_again_later() {
    let gateway = fake_gateway().await;
    let (mut task, shared) = task_with(&gateway);
    // A gateway that is not there.
    let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    task.backend = Some(Backend::Pmp(Pmp::at(silent.local_addr().unwrap())));
    task.entries.push(Entry {
        protocol: Protocol::Tcp,
        port: 6881,
        active: None,
    });
    let before = Instant::now();
    task.sync().await;
    assert!(task.backend.is_none());
    assert!(task.entries[0].active.is_none());
    assert!(task.retry_at >= before + RETRY_AFTER_LOSS);
    assert!(matches!(shared.get(), Status::Failed(_)));
}

#[tokio::test]
async fn a_disabled_mapper_does_nothing() {
    let mapper = Portmapper::new(false);
    mapper.map(Protocol::Tcp, 6881);
    assert_eq!(mapper.status(), Status::Off);
    // Nothing to wait for.
    mapper.shutdown().await;
}

#[tokio::test]
async fn an_unmapped_port_is_never_asked_for() {
    let mapper = Portmapper::new(true);
    mapper.map(Protocol::Tcp, 0);
    assert_eq!(mapper.status(), Status::Off);
}

#[test]
fn status_texts() {
    assert_eq!(Status::Searching.to_string(), "mapping port...");
    assert_eq!(
        Status::Mapped {
            via: "UPnP",
            external_ip: Some(Ipv4Addr::new(203, 0, 113, 7)),
            external_port: 6881
        }
        .to_string(),
        "mapped 203.0.113.7:6881 (UPnP)"
    );
    assert_eq!(
        Status::Mapped {
            via: "PCP",
            external_ip: None,
            external_port: 7000
        }
        .to_string(),
        "mapped port 7000 (PCP)"
    );
    assert_eq!(Status::Failed("x".into()).to_string(), "port not mapped");
}
