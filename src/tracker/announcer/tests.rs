use super::*;

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
