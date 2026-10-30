#![cfg(unix)]

use rtrnt::manager::Command;
use rtrnt::rpc::{self, Action, Response};
use rtrnt::tui::Feed;
use std::time::Duration;

#[tokio::test]
async fn headless_daemon_stays_alive_empty_and_stops_over_rpc() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "").unwrap();
    let socket = dir.path().join("rpc/control.sock");
    let mut daemon = tokio::process::Command::new(env!("CARGO_BIN_EXE_rtrnt"))
        .args([
            "--daemon",
            "--no-session",
            "--no-dht",
            "--no-lsd",
            "--no-portmap",
            "--config",
        ])
        .arg(&config)
        .arg("--rpc-socket")
        .arg(&socket)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                daemon.try_wait().unwrap().is_none(),
                "daemon exited before binding"
            );
            if socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("daemon never bound socket");
    let Response::Snapshot(snapshot) = rpc::request(&socket, Action::Snapshot { selected: None })
        .await
        .unwrap()
    else {
        panic!("no snapshot")
    };
    assert!(snapshot.finished);
    assert!(snapshot.torrents.is_empty());
    let cli = tokio::process::Command::new(env!("CARGO_BIN_EXE_rtrnt"))
        .arg("--rpc-socket")
        .arg(&socket)
        .args(["--rpc-request", r#"{"method":"snapshot","selected":null}"#])
        .output()
        .await
        .unwrap();
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    assert!(matches!(
        serde_json::from_slice::<Response>(&cli.stdout).unwrap(),
        Response::Snapshot(_)
    ));
    let (mut attached, _) = rpc::RemoteFeed::connect(&socket).await.unwrap();
    assert!(attached.command(Command::SetRateLimits(123, 45)));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Response::Snapshot(next) =
                rpc::request(&socket, Action::Snapshot { selected: None })
                    .await
                    .unwrap()
                && next.session.rate_limits == (123, 45)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("attached command did not reach the engine");
    assert!(!attached.command(Command::Stop));
    drop(attached);
    assert!(
        daemon.try_wait().unwrap().is_none(),
        "detaching stopped daemon"
    );
    assert!(matches!(
        rpc::request(
            &socket,
            Action::Command {
                command: Command::Stop
            }
        )
        .await
        .unwrap(),
        Response::Accepted
    ));
    let status = tokio::time::timeout(Duration::from_secs(5), daemon.wait())
        .await
        .expect("daemon did not stop")
        .unwrap();
    assert!(status.success());
    assert!(!socket.exists(), "daemon did not remove its socket");
}
