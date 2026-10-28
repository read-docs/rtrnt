//! The terminal UI: a full-screen view of the session's torrents, their
//! peers, pieces and trackers, and the log.
//!
//! The engine publishes counters in `stats`; the UI takes a `Snapshot` of
//! them four times a second, works out rates from the difference with the
//! last ones, and draws.

pub mod app;
pub mod demo;
pub mod log_buffer;
pub mod snapshot;
pub mod theme;
pub mod view;

pub use crate::manager::Command;
use crate::stats::SessionStats;
use app::{Action, App};
use crossterm::event::{
    DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyEventKind, MouseEventKind,
};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{cursor, execute, terminal};
use futures_util::StreamExt;
use log_buffer::{LogBuffer, LogLine};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use snapshot::Snapshot;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread::ThreadId;
use std::time::{Duration, Instant, SystemTime};
use theme::Theme;
use tokio::sync::{mpsc, watch};
use tokio::time::MissedTickBehavior;
use tracing::Level;

/// How often the stats are read and the screen drawn.
pub const TICK: Duration = Duration::from_millis(250);

/// Where the UI gets what it shows, and what it asks of the session.
pub trait Feed {
    /// What to show now, with the insides of the torrent `selected`.
    /// `finished` says no torrent's task is running.
    fn snapshot(&mut self, selected: Option<[u8; 20]>, finished: bool) -> Snapshot;

    /// Passes `command` on. Returns false if the UI should quit at once:
    /// on `Stop` with nothing to wait for. What comes of other commands
    /// shows in later snapshots, or as a notice.
    fn command(&mut self, command: Command) -> bool;

    /// Where the log goes, which is also where a panic of another thread
    /// than the UI's is put while the UI has the terminal.
    fn logs(&self) -> Option<LogBuffer> {
        None
    }
}

/// The feed of a real session.
pub struct SessionFeed<S> {
    stats: Arc<SessionStats>,
    logs: LogBuffer,
    stop: S,
    commands: mpsc::UnboundedSender<Command>,
}

impl<S: FnMut() -> bool> SessionFeed<S> {
    /// Shows `stats` and `logs`. `Stop` calls `stop`, which returns false if
    /// there is nothing to wait for, and every other command goes to
    /// `commands`.
    pub fn new(
        stats: Arc<SessionStats>,
        logs: LogBuffer,
        stop: S,
        commands: mpsc::UnboundedSender<Command>,
    ) -> Self {
        Self {
            stats,
            logs,
            stop,
            commands,
        }
    }
}

impl<S: FnMut() -> bool> Feed for SessionFeed<S> {
    fn logs(&self) -> Option<LogBuffer> {
        Some(self.logs.clone())
    }

    fn snapshot(&mut self, selected: Option<[u8; 20]>, finished: bool) -> Snapshot {
        Snapshot::take(&self.stats, &self.logs, selected, finished)
    }

    fn command(&mut self, command: Command) -> bool {
        match command {
            Command::Stop => (self.stop)(),
            command => {
                if self.commands.send(command).is_err() {
                    self.stats.notice("the session takes no more commands");
                }
                true
            }
        }
    }
}

/// How the UI ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// Everything had finished and the user left.
    Quit,
    /// The user asked to leave at once, without waiting for the torrents.
    Now,
}

/// Runs the UI on the terminal until the user leaves, showing what `feed`
/// gives. `finished` is true while no torrent's task is running; the UI
/// then says so and waits for `q`. A torrent added by a relative path is
/// under `dir`.
///
/// The terminal is put back as it was on every way out, a panic included.
pub async fn run(
    mut feed: impl Feed,
    mut finished: watch::Receiver<bool>,
    dir: PathBuf,
) -> io::Result<Exit> {
    let _guard = TerminalGuard::enter(feed.logs())?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut app = App::new(Theme::detect());
    app.dir = dir;
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(TICK);
    // After a stall, one redraw is enough, not a burst of them.
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut done = *finished.borrow();
    let mut snapshot = Snapshot::default();
    // Where the last screen put everything, for the mouse.
    let mut areas = view::Areas::default();
    let mut mouse = app.mouse;
    loop {
        let mut sample = false;
        tokio::select! {
            _ = tick.tick() => sample = true,
            event = events.next() => {
                let action = match event {
                    Some(Ok(Event::Key(key))) if key.kind != KeyEventKind::Release => {
                        app.handle_key(key, &snapshot)
                    }
                    // Nothing to redraw for: no hover, no drag.
                    Some(Ok(Event::Mouse(event)))
                        if matches!(
                            event.kind,
                            MouseEventKind::Moved | MouseEventKind::Drag(_) | MouseEventKind::Up(_)
                        ) =>
                    {
                        continue;
                    }
                    Some(Ok(Event::Mouse(event))) => app.handle_mouse(event, &areas, &snapshot),
                    Some(Ok(_)) => Action::None,
                    Some(Err(e)) => return Err(e),
                    // No more input: nothing left to wait for.
                    None => return Ok(Exit::Now),
                };
                match action {
                    Action::None => {}
                    Action::Quit => return Ok(Exit::Quit),
                    Action::ForceQuit => return Ok(Exit::Now),
                    Action::Command(command) => {
                        if !feed.command(command) {
                            return Ok(Exit::Now);
                        }
                    }
                }
                if app.mouse != mouse {
                    mouse = app.mouse;
                    capture_mouse(mouse)?;
                }
            }
            Ok(()) = finished.changed() => done = *finished.borrow(),
        }
        snapshot = feed.snapshot(app.selected, done);
        let selected = app.selected;
        app.ensure_selection(&snapshot);
        if app.selected != selected {
            snapshot = feed.snapshot(app.selected, done);
        }
        if sample {
            app.rates.record(&snapshot, Instant::now());
        }
        app.apply_rates(&mut snapshot);
        terminal.draw(|frame| areas = view::draw(frame, &snapshot, &app))?;
        app.fit_detail(&areas, &snapshot);
    }
}

/// Turns the terminal's reporting of mouse events on or off. While it is
/// on, the terminal's own text selection needs Shift held in most of them.
fn capture_mouse(on: bool) -> io::Result<()> {
    if on {
        execute!(io::stdout(), EnableMouseCapture)
    } else {
        execute!(io::stdout(), DisableMouseCapture)
    }
}

/// Set while the terminal is in raw mode on the alternate screen.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Puts the terminal back as it was before the UI took it over, if it did.
/// Safe to call any number of times, from anywhere.
pub fn restore_terminal() {
    if ACTIVE.swap(false, Ordering::SeqCst) {
        let _ = terminal::disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            DisableMouseCapture,
            LeaveAlternateScreen,
            cursor::Show
        );
    }
}

/// The thread that draws, and where the panics of the others go.
struct Ui {
    thread: ThreadId,
    logs: Option<LogBuffer>,
}

/// Set while a `TerminalGuard` holds the terminal.
static UI: Mutex<Option<Ui>> = Mutex::new(None);

fn ui() -> MutexGuard<'static, Option<Ui>> {
    UI.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Holds the terminal in raw mode on the alternate screen, with the mouse
/// captured, until dropped.
struct TerminalGuard;

impl TerminalGuard {
    /// Takes the terminal for the calling thread. A panic of that thread
    /// puts it back; one of any other thread is logged to `logs`, if any,
    /// instead of being printed over the screen.
    fn enter(logs: Option<LogBuffer>) -> io::Result<Self> {
        install_panic_hook();
        terminal::enable_raw_mode()?;
        ACTIVE.store(true, Ordering::SeqCst);
        *ui() = Some(Ui {
            thread: std::thread::current().id(),
            logs,
        });
        // From here on a failure drops the guard, which restores.
        let guard = Self;
        execute!(
            io::stdout(),
            EnterAlternateScreen,
            cursor::Hide,
            EnableMouseCapture
        )?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        *ui() = None;
        restore_terminal();
    }
}

/// Makes a panic of the UI's thread restore the terminal before its message
/// is printed. A panic of another thread, which leaves the UI running, goes
/// to the log instead of over the screen.
fn install_panic_hook() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let elsewhere = ui()
                .as_ref()
                .filter(|ui| ui.thread != std::thread::current().id())
                .and_then(|ui| ui.logs.clone());
            if let Some(logs) = elsewhere {
                logs.push(LogLine {
                    time: SystemTime::now(),
                    level: Level::ERROR,
                    torrent: None,
                    message: panic_message(info),
                });
                return;
            }
            restore_terminal();
            previous(info);
        }));
    });
}

/// What `info` says, as the default hook would, on one line.
fn panic_message(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    let what = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("Box<dyn Any>");
    let thread = std::thread::current();
    let name = thread.name().unwrap_or("<unnamed>");
    match info.location() {
        Some(at) => format!("thread '{name}' panicked at {at}: {what}"),
        None => format!("thread '{name}' panicked: {what}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic of a thread other than the UI's lands in the log and leaves
    /// the terminal alone.
    #[test]
    fn a_panic_elsewhere_goes_to_the_log() {
        let logs = LogBuffer::default();
        install_panic_hook();
        // Some other thread than this test's is the UI's.
        let drawing = std::thread::spawn(|| std::thread::current().id())
            .join()
            .unwrap();
        *ui() = Some(Ui {
            thread: drawing,
            logs: Some(logs.clone()),
        });
        ACTIVE.store(true, Ordering::SeqCst);
        let result = std::thread::spawn(|| panic!("boom")).join();
        let active = ACTIVE.swap(false, Ordering::SeqCst);
        *ui() = None;
        assert!(result.is_err());
        assert!(active, "the terminal was restored");
        let lines = logs.lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].level, Level::ERROR);
        assert!(lines[0].message.contains("boom"), "{}", lines[0].message);
    }
}
