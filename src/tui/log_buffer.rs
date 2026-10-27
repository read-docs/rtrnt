//! Log lines kept in memory for the UI's log pane, in place of stderr.

use std::collections::VecDeque;
use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// Lines kept; older ones are dropped.
pub const CAPACITY: usize = 1000;

/// One log event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    pub time: SystemTime,
    pub level: Level,
    /// The torrent the event is about (its `torrent` span's `name`), or
    /// `session` for work shared by every torrent.
    pub torrent: Option<String>,
    /// The message, followed by the event's other fields as `key=value`.
    pub message: String,
}

impl fmt::Display for LogLine {
    /// As the log reads on stderr outside the UI.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let time = chrono::DateTime::<chrono::Local>::from(self.time);
        write!(f, "{} {:>5} ", time.format("%H:%M:%S"), self.level)?;
        if let Some(torrent) = &self.torrent {
            write!(f, "{torrent}: ")?;
        }
        f.write_str(&self.message)
    }
}

/// The last `CAPACITY` log lines, shared between the layer that writes them
/// and the UI that reads them.
#[derive(Clone, Default)]
pub struct LogBuffer(Arc<Mutex<VecDeque<LogLine>>>);

impl LogBuffer {
    fn lock(&self) -> MutexGuard<'_, VecDeque<LogLine>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn push(&self, line: LogLine) {
        let mut lines = self.lock();
        if lines.len() == CAPACITY {
            lines.pop_front();
        }
        lines.push_back(line);
    }

    /// Every line kept, oldest first.
    pub fn lines(&self) -> Vec<LogLine> {
        self.lock().iter().cloned().collect()
    }

    /// Warnings and errors logged in the last `within` before `now`.
    pub fn problems_since(&self, now: SystemTime, within: Duration) -> Vec<LogLine> {
        let since = now.checked_sub(within).unwrap_or(SystemTime::UNIX_EPOCH);
        self.lock()
            .iter()
            .filter(|line| line.level <= Level::WARN && line.time >= since)
            .cloned()
            .collect()
    }

    /// A layer that writes every event into this buffer.
    pub fn layer(&self) -> LogLayer {
        LogLayer {
            buffer: self.clone(),
        }
    }
}

/// Writes events into a `LogBuffer`.
pub struct LogLayer {
    buffer: LogBuffer,
}

/// The torrent name a span stands for, kept in its extensions.
struct TorrentName(String);

impl<S> Layer<S> for LogLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let name = match attrs.metadata().name() {
            "torrent" => {
                let mut visitor = FieldVisitor::new("name");
                attrs.record(&mut visitor);
                visitor.wanted
            }
            "session" => Some("session".to_string()),
            _ => None,
        };
        if let (Some(name), Some(span)) = (name, ctx.span(id)) {
            span.extensions_mut().insert(TorrentName(name));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let torrent = ctx.event_scope(event).and_then(|mut scope| {
            scope.find_map(|span| {
                span.extensions()
                    .get::<TorrentName>()
                    .map(|name| name.0.clone())
            })
        });
        let mut visitor = FieldVisitor::new("message");
        event.record(&mut visitor);
        let mut message = visitor.wanted.unwrap_or_default();
        if !visitor.rest.is_empty() {
            if !message.is_empty() {
                message.push(' ');
            }
            message.push_str(&visitor.rest);
        }
        self.buffer.push(LogLine {
            time: SystemTime::now(),
            level: *event.metadata().level(),
            torrent,
            message,
        });
    }
}

/// Takes the field `wanted` out, and writes the others as `key=value`.
struct FieldVisitor {
    name: &'static str,
    wanted: Option<String>,
    rest: String,
}

impl FieldVisitor {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            wanted: None,
            rest: String::new(),
        }
    }

    fn push(&mut self, field: &Field, value: fmt::Arguments<'_>) {
        if field.name() == self.name {
            self.wanted = Some(value.to_string());
            return;
        }
        if !self.rest.is_empty() {
            self.rest.push(' ');
        }
        let _ = write!(self.rest, "{}={value}", field.name());
    }
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push(field, format_args!("{value}"));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push(field, format_args!("{value:?}"));
    }
}

#[cfg(test)]
mod tests {
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
}
