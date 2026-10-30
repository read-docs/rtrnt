use super::*;
use crate::client::ConnectionSlots;
use crate::rate_limit::RateLimits;
use crate::source::Source;
use crate::tui::log_buffer::LogLine;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;

fn stats() -> Arc<SessionStats> {
    Arc::new(SessionStats::new(
        Arc::new(RateLimits::default()),
        ConnectionSlots::new(10),
    ))
}

#[tokio::test]
async fn snapshots_commands_stop_and_detach_share_one_session() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("control.sock");
    let logs = LogBuffer::default();
    logs.push(LogLine {
        time: SystemTime::now(),
        level: tracing::Level::WARN,
        torrent: None,
        message: "test log".into(),
    });
    let stats = stats();
    stats.add_torrent([1; 20], "sample", dir.path());
    let (commands, mut received) = mpsc::unbounded_channel();
    let (_idle, idle_rx) = watch::channel(false);
    let stopped = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stopped);
    let server = Server::bind(path.clone(), stats, logs, idle_rx, commands, move || {
        flag.store(true, Ordering::SeqCst);
    })
    .unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let Response::Snapshot(snapshot) = request(
        &path,
        Action::Snapshot {
            selected: Some([1; 20]),
        },
    )
    .await
    .unwrap() else {
        panic!("no snapshot")
    };
    assert_eq!(snapshot.torrents[0].name, "sample");
    assert_eq!(snapshot.detail.unwrap().info_hash, [1; 20]);
    assert_eq!(snapshot.logs[0].message, "test log");
    let command = Command::Add(Source::File(dir.path().join("sample.torrent")));
    assert!(matches!(
        request(
            &path,
            Action::Command {
                command: command.clone()
            }
        )
        .await
        .unwrap(),
        Response::Accepted
    ));
    assert_eq!(received.recv().await, Some(command));
    let (mut feed, _) = RemoteFeed::connect(&path).await.unwrap();
    assert!(!feed.command(Command::Stop), "attached q should detach");
    drop(feed);
    assert!(!stopped.load(Ordering::SeqCst));
    assert!(matches!(
        request(
            &path,
            Action::Command {
                command: Command::Stop
            }
        )
        .await
        .unwrap(),
        Response::Accepted
    ));
    assert!(stopped.load(Ordering::SeqCst));
    drop(server);
    assert!(!path.exists());
}

#[tokio::test]
async fn refuses_a_second_server_and_survives_invalid_requests() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("control.sock");
    let (commands, _rx) = mpsc::unbounded_channel();
    let (_, idle) = watch::channel(true);
    let _server = Server::bind(
        path.clone(),
        stats(),
        LogBuffer::default(),
        idle.clone(),
        commands.clone(),
        || {},
    )
    .unwrap();
    assert!(
        Server::bind(
            path.clone(),
            stats(),
            LogBuffer::default(),
            idle,
            commands,
            || {}
        )
        .is_err()
    );
    let mut stream = BufReader::new(UnixStream::connect(&path).await.unwrap());
    stream.get_mut().write_all(b"garbage\n").await.unwrap();
    let bytes = line(&mut stream).await.unwrap().unwrap();
    assert!(matches!(
        serde_json::from_slice::<Response>(&bytes).unwrap(),
        Response::Error(_)
    ));
    write_json(
        stream.get_mut(),
        &Request {
            version: 99,
            action: Action::Snapshot { selected: None },
        },
    )
    .await
    .unwrap();
    let bytes = line(&mut stream).await.unwrap().unwrap();
    assert!(matches!(
        serde_json::from_slice::<Response>(&bytes).unwrap(),
        Response::Error(_)
    ));
    assert!(matches!(
        exchange(&mut stream, Action::Snapshot { selected: None })
            .await
            .unwrap(),
        Response::Snapshot(_)
    ));
}

#[tokio::test]
async fn framing_is_bounded_and_incomplete_lines_are_rejected() {
    let bytes = vec![b'x'; MAX_LINE + 1];
    assert!(line(&mut &bytes[..]).await.is_err());
    assert!(line(&mut &b"{}"[..]).await.is_err());
}

#[test]
fn mapping_status_roundtrips_without_borrowing_wire_data() {
    let status = crate::portmap::Status::Mapped {
        via: "PCP",
        external_ip: None,
        external_port: 123,
    };
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(
        serde_json::from_str::<crate::portmap::Status>(&json).unwrap(),
        status
    );
}
