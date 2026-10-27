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

use crate::stats::SessionStats;
use app::{Action, App};
use crossterm::event::{Event, EventStream, KeyEventKind};
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::{cursor, execute, terminal};
use futures_util::StreamExt;
use log_buffer::LogBuffer;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use snapshot::Snapshot;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use theme::Theme;
use tokio::sync::{mpsc, watch};

/// How often the stats are read and the screen drawn.
pub const TICK: Duration = Duration::from_millis(250);

/// What the UI asks of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Stop the torrents, as Ctrl-C does.
    Stop,
    /// Change the download and upload limits to these KiB/s, 0 for none.
    SetRateLimits(u64, u64),
}

/// Where the UI gets what it shows, and what it asks of the session.
pub trait Feed {
    /// What to show now, with the insides of the torrent `selected`.
    /// `finished` says every torrent's task has ended.
    fn snapshot(&mut self, selected: Option<[u8; 20]>, finished: bool) -> Snapshot;

    /// Passes `command` on. Returns false if the UI should quit at once:
    /// on `Stop` with nothing to wait for. What comes of other commands
    /// shows in later snapshots, or as a notice.
    fn command(&mut self, command: Command) -> bool;
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
/// gives. `finished` turns true once every torrent's task has ended; the UI
/// then says so and waits for `q`.
///
/// The terminal is put back as it was on every way out, a panic included.
pub async fn run(mut feed: impl Feed, mut finished: watch::Receiver<bool>) -> io::Result<Exit> {
    let _guard = TerminalGuard::enter()?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;
    let mut app = App::new(Theme::detect());
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(TICK);
    let mut done = *finished.borrow();
    let mut snapshot = Snapshot::default();
    loop {
        let mut sample = false;
        tokio::select! {
            _ = tick.tick() => sample = true,
            event = events.next() => match event {
                Some(Ok(Event::Key(key))) if key.kind != KeyEventKind::Release => {
                    match app.handle_key(key, &snapshot) {
                        Action::None => {}
                        Action::Quit => return Ok(Exit::Quit),
                        Action::ForceQuit => return Ok(Exit::Now),
                        Action::Command(command) => {
                            if !feed.command(command) {
                                return Ok(Exit::Now);
                            }
                        }
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e),
                // No more input: nothing left to wait for.
                None => return Ok(Exit::Now),
            },
            Ok(()) = finished.changed(), if !done => done = *finished.borrow(),
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
        app.rates.apply(&mut snapshot);
        terminal.draw(|frame| view::draw(frame, &snapshot, &app))?;
    }
}

/// Set while the terminal is in raw mode on the alternate screen.
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Puts the terminal back as it was before the UI took it over, if it did.
/// Safe to call any number of times, from anywhere.
pub fn restore_terminal() {
    if ACTIVE.swap(false, Ordering::SeqCst) {
        let _ = terminal::disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, cursor::Show);
    }
}

/// Holds the terminal in raw mode on the alternate screen until dropped.
struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> io::Result<Self> {
        install_panic_hook();
        terminal::enable_raw_mode()?;
        ACTIVE.store(true, Ordering::SeqCst);
        // From here on a failure drops the guard, which restores.
        let guard = Self;
        execute!(io::stdout(), EnterAlternateScreen, cursor::Hide)?;
        Ok(guard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        restore_terminal();
    }
}

/// Makes a panic restore the terminal before its message is printed.
fn install_panic_hook() {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            restore_terminal();
            previous(info);
        }));
    });
}
