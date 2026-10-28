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

fn line(message: impl ToString) -> LogLine {
    LogLine {
        time: SystemTime::now(),
        level: Level::INFO,
        torrent: None,
        message: message.to_string(),
    }
}

fn messages(lines: Vec<LogLine>) -> Vec<String> {
    lines.into_iter().map(|line| line.message).collect()
}

#[test]
fn tail_returns_the_newest_lines_oldest_first() {
    let buffer = LogBuffer::default();
    for i in 0..5 {
        buffer.push(line(i));
    }
    assert_eq!(messages(buffer.tail(2)), ["3", "4"]);
    assert_eq!(messages(buffer.tail(5)), ["0", "1", "2", "3", "4"]);
}

#[test]
fn tail_of_more_than_the_buffer_holds_returns_everything() {
    let buffer = LogBuffer::default();
    assert!(buffer.tail(10).is_empty());
    buffer.push(line("only"));
    assert_eq!(messages(buffer.tail(10)), ["only"]);
    assert_eq!(messages(buffer.tail(usize::MAX)), ["only"]);
}

#[test]
fn tail_of_zero_is_empty() {
    let buffer = LogBuffer::default();
    buffer.push(line("a"));
    assert!(buffer.tail(0).is_empty());
}

#[test]
fn tail_after_the_buffer_wrapped_skips_the_dropped_lines() {
    let buffer = LogBuffer::default();
    for i in 0..CAPACITY + 3 {
        buffer.push(line(i));
    }
    let last = CAPACITY + 2;
    assert_eq!(
        messages(buffer.tail(3)),
        [last - 2, last - 1, last].map(|i| i.to_string())
    );
    let all = buffer.tail(CAPACITY + 10);
    assert_eq!(all.len(), CAPACITY);
    assert_eq!(all[0].message, "3");
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
