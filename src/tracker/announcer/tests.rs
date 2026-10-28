use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

fn announcer(now: Instant) -> Announcer {
    let request = AnnounceRequest {
        info_hash: [1; 20],
        peer_id: [2; 20],
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: 1000,
        event: AnnounceEvent::Regular,
    };
    let trackers = Tiers::new(vec![vec!["http://t/announce".into()]]);
    Announcer::new(trackers, request, now)
}

fn response(interval: Option<u64>, min_interval: Option<u64>) -> AnnounceResponse {
    AnnounceResponse {
        peers: Vec::new(),
        interval,
        min_interval,
        seeders: None,
        leechers: None,
    }
}

#[test]
fn schedules_on_the_trackers_interval_within_bounds() {
    let now = Instant::now();
    let mut a = announcer(now);
    let secs = Duration::from_secs;
    for (interval, min_interval, expected) in [
        (Some(900), None, secs(900)),
        (None, None, DEFAULT_INTERVAL),
        // A tracker asking to be hammered, or to never hear from us.
        (Some(0), None, MIN_INTERVAL),
        (Some(u64::MAX), None, MAX_INTERVAL),
        // `interval` below `min interval` makes no sense; obey the latter.
        (Some(100), Some(300), secs(300)),
    ] {
        a.schedule(&response(interval, min_interval));
        assert_eq!(
            a.timer.next,
            now + expected,
            "interval {interval:?}, min {min_interval:?}"
        );
    }
}

#[test]
fn hurry_respects_min_interval() {
    let now = Instant::now();
    let mut a = announcer(now);
    a.schedule(&response(Some(1800), Some(120)));
    a.hurry();
    assert_eq!(a.timer.next, now + Duration::from_secs(120));
    // Never pushes an announce back.
    a.timer.next = now + Duration::from_secs(10);
    a.hurry();
    assert_eq!(a.timer.next, now + Duration::from_secs(10));
}

#[test]
fn requests_report_progress() {
    let a = announcer(Instant::now());
    assert_eq!(a.started.event, AnnounceEvent::Started);
    let request = a.request(AnnounceEvent::Regular, (400, 7000));
    assert_eq!(
        (request.downloaded, request.uploaded, request.left),
        (400, 7000, 600)
    );
    assert_eq!(request.event, AnnounceEvent::Regular);
    assert_eq!(a.request(AnnounceEvent::Completed, (5000, 0)).left, 0);
}

#[test]
fn left_can_be_set_once_the_size_is_known() {
    let mut a = announcer(Instant::now());
    a.set_left(5000);
    assert_eq!(a.request(AnnounceEvent::Regular, (400, 0)).left, 4600);
}

#[test]
fn completion_makes_the_next_announce_due_at_once() {
    let now = Instant::now();
    let mut a = announcer(now);
    a.schedule(&response(Some(1800), Some(600)));
    a.complete();
    assert_eq!(a.completion, Completion::Due);
    assert!(a.timer.next <= Instant::now());
    // Only once: a download finishes a single time.
    a.completion = Completion::Sent;
    a.timer.next = now + Duration::from_secs(1800);
    a.complete();
    assert_eq!(a.completion, Completion::Sent);
    assert_eq!(a.timer.next, now + Duration::from_secs(1800));
}

#[test]
fn completed_is_not_pushed_back_by_an_announce_in_flight() {
    let now = Instant::now();
    let min = Duration::from_secs(120);
    let mut a = announcer(now);
    a.answered = true;
    a.schedule(&response(Some(1800), Some(120)));
    // A regular announce went out at `now`; the download finishes while it
    // is on its way, and it answers with the usual interval.
    a.complete();
    a.announced(AnnounceEvent::Regular, Ok(response(Some(1800), Some(120))));
    assert_eq!(a.timer.next, now + min);
    // The same when it failed.
    a.announced(AnnounceEvent::Regular, Err(anyhow::anyhow!("down")));
    assert_eq!(a.timer.next, now + min);
    // Once the tracker has heard it, back to the interval.
    a.announced(
        AnnounceEvent::Completed,
        Ok(response(Some(1800), Some(120))),
    );
    assert_eq!(a.completion, Completion::Sent);
    assert_eq!(a.timer.next, now + Duration::from_secs(1800));
}

#[test]
fn started_is_retried_with_a_growing_wait() {
    let now = Instant::now();
    let mut a = announcer(now);
    let secs = Duration::from_secs;
    for wait in [30, 60, 120, 240, 480, 960, 1800, 1800] {
        a.announced(AnnounceEvent::Started, Err(anyhow::anyhow!("down")));
        assert!(!a.has_answered());
        assert_eq!(a.timer.next, now + secs(wait));
    }
    // Once a tracker answers, the tracker's interval rules.
    a.announced(AnnounceEvent::Started, Ok(response(Some(900), None)));
    assert!(a.has_answered());
    assert_eq!(a.timer.next, now + secs(900));
    // A later failure is not a failed start.
    a.announced(AnnounceEvent::Regular, Err(anyhow::anyhow!("down")));
    assert_eq!(a.timer.next, now + secs(900));
}

/// A tracker on loopback that hangs up on the first announce and answers the
/// second with one peer. Sends the target of each request it reads.
async fn flaky_tracker(peer: SocketAddr) -> (String, mpsc::UnboundedReceiver<String>) {
    let SocketAddr::V4(peer) = peer else {
        panic!("IPv4 peers only")
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let (targets, targets_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (refused, _) = listener.accept().await.unwrap();
        drop(refused);
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.ends_with(b"\r\n\r\n") {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0, "announce closed mid-request");
                request.extend_from_slice(&buf[..n]);
            }
            let request = String::from_utf8(request).unwrap();
            let _ = targets.send(request.split(' ').nth(1).unwrap().to_string());
            let mut body = b"d8:intervali1800e5:peers6:".to_vec();
            body.extend_from_slice(&peer.ip().octets());
            body.extend_from_slice(&peer.port().to_be_bytes());
            body.push(b'e');
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    (url, targets_rx)
}

#[tokio::test]
async fn started_is_announced_again_when_every_tracker_refused_it() {
    let peer: SocketAddr = "10.0.0.7:6881".parse().unwrap();
    let (url, mut targets) = flaky_tracker(peer).await;
    let request = announcer(Instant::now()).started;
    let (mut a, peers) = Announcer::start_reporting(vec![vec![url]], request, None).await;
    assert!(peers.is_empty());
    assert!(!a.has_answered());
    assert!(
        targets.try_recv().is_err(),
        "the first announce was refused"
    );

    // The retry is due in 30 s; the wait itself is covered above.
    assert_eq!(a.timer.next, a.timer.last + Duration::from_secs(30));
    a.timer.next = Instant::now();
    let peers = a.poll(&Transferred::default()).await;
    assert_eq!(peers, [peer]);
    assert!(a.has_answered());
    let target = targets.recv().await.unwrap();
    assert!(target.contains("event=started"), "{target}");
    // From here on, the tracker's interval.
    assert_eq!(a.timer.next, a.timer.last + Duration::from_secs(1800));
}

#[tokio::test]
async fn stop_says_nothing_to_a_tracker_that_never_heard_started() {
    let peer: SocketAddr = "10.0.0.7:6881".parse().unwrap();
    let (url, mut targets) = flaky_tracker(peer).await;
    let request = announcer(Instant::now()).started;
    let (a, _) = Announcer::start_reporting(vec![vec![url]], request, None).await;
    a.stop(&Transferred::default()).await;
    assert!(targets.try_recv().is_err(), "nothing was sent");
}
