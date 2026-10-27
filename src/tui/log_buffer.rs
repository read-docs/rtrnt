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
pub struct LogBuffer(Arc<Mutex<Lines>>);

#[derive(Default)]
struct Lines {
    kept: VecDeque<LogLine>,
    /// Also print each new line to stderr, once no UI shows them.
    echo: bool,
}

impl LogBuffer {
    fn lock(&self) -> MutexGuard<'_, Lines> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn push(&self, line: LogLine) {
        let mut lines = self.lock();
        if lines.echo {
            eprintln!("{line}");
        }
        if lines.kept.len() == CAPACITY {
            lines.kept.pop_front();
        }
        lines.kept.push_back(line);
    }

    /// From now on, prints every new line to stderr as well, for when the UI
    /// that showed them is gone and the session carries on.
    pub fn echo_to_stderr(&self) {
        self.lock().echo = true;
    }

    /// Every line kept, oldest first.
    pub fn lines(&self) -> Vec<LogLine> {
        self.lock().kept.iter().cloned().collect()
    }

    /// Warnings and errors logged in the last `within` before `now`.
    pub fn problems_since(&self, now: SystemTime, within: Duration) -> Vec<LogLine> {
        let since = now.checked_sub(within).unwrap_or(SystemTime::UNIX_EPOCH);
        self.lock()
            .kept
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
mod tests;
