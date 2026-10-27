use super::*;
use tracing::{info, info_span, warn};
use tracing_subscriber::layer::SubscriberExt;

fn capture(log: impl FnOnce()) -> Vec<LogLine> {
    let buffer = LogBuffer::default();
    let subscriber = tracing_subscriber::registry().with(buffer.layer());
    tracing::subscriber::with_default(subscriber, log);
    buffer.lines()
}

#[test]
fn keeps_the_last_lines_only() {
    let buffer = LogBuffer::default();
    for i in 0..CAPACITY + 5 {
        buffer.push(LogLine {
            time: SystemTime::now(),
            level: Level::INFO,
            torrent: None,
            message: i.to_string(),
        });
    }
    let lines = buffer.lines();
    assert_eq!(lines.len(), CAPACITY);
    assert_eq!(lines[0].message, "5");
    assert_eq!(lines[CAPACITY - 1].message, (CAPACITY + 4).to_string());
}

#[test]
fn takes_the_torrent_name_from_its_span() {
    let lines = capture(|| {
        info!(peers = 3, "outside");
        let span = info_span!("torrent", name = %"Some Name");
        let _entered = span.enter();
        let inner = info_span!("other");
        let _inner = inner.enter();
        warn!(path = "a b", "inside");
    });
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].torrent, None);
    assert_eq!(lines[0].message, "outside peers=3");
    assert_eq!(lines[1].torrent.as_deref(), Some("Some Name"));
    assert_eq!(lines[1].level, Level::WARN);
    assert_eq!(lines[1].message, "inside path=a b");
}

#[test]
fn session_spans_are_named_session() {
    let lines = capture(|| {
        let torrent = info_span!("torrent", name = %"t");
        let _torrent = torrent.enter();
        let session = info_span!(parent: None, "session");
        let _session = session.enter();
        info!("bound");
    });
    assert_eq!(lines[0].torrent.as_deref(), Some("session"));
}

#[test]
fn problems_are_recent_warnings_and_errors() {
    let buffer = LogBuffer::default();
    let now = SystemTime::now();
    let line = |secs_ago: u64, level: Level| LogLine {
        time: now - Duration::from_secs(secs_ago),
        level,
        torrent: None,
        message: format!("{level} {secs_ago}"),
    };
    buffer.push(line(30, Level::ERROR));
    buffer.push(line(5, Level::INFO));
    buffer.push(line(4, Level::WARN));
    buffer.push(line(1, Level::ERROR));
    let problems: Vec<String> = buffer
        .problems_since(now, Duration::from_secs(10))
        .into_iter()
        .map(|line| line.message)
        .collect();
    assert_eq!(problems, ["WARN 4", "ERROR 1"]);
}
