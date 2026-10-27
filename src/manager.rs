//! The torrents of a session: starting each one, what it is doing, what the
//! UI asks of them, and the exit code once they have all ended.

use crate::client::Client;
use crate::download::{self, Interrupted};
use crate::magnet::{self, FetchInterrupted, MagnetLink};
use crate::source::Source;
use crate::torrent::{self, Metainfo};
use anyhow::Result;
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::{self, JoinError, JoinSet};
use tracing::{Instrument, Span, error, info_span, warn};

/// Exit code for a run stopped by Ctrl-C (128 + SIGINT).
pub const EXIT_INTERRUPTED: u8 = 130;

/// What the UI asks of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Stop the torrents, as Ctrl-C does.
    Stop,
    /// Change the download and upload limits to these KiB/s, 0 for none.
    SetRateLimits(u64, u64),
}

/// Resolves when a torrent is to stop.
pub type StopSignal = BoxFuture<'static, ()>;

/// How a torrent is run for real: `download_metainfo` or `download_magnet`.
pub type RunTorrent = fn(Arc<Client>, Torrent, StopSignal) -> BoxFuture<'static, Result<()>>;

/// A torrent ready to start: the metainfo of a file, loaded already, or a
/// magnet link.
pub enum Torrent {
    File(Metainfo),
    Magnet(Box<MagnetLink>),
}

impl Torrent {
    pub fn info_hash(&self) -> [u8; 20] {
        match self {
            Torrent::File(metainfo) => metainfo.info_hash,
            Torrent::Magnet(link) => link.info_hash,
        }
    }
}

/// How the download of one torrent ended.
#[derive(Debug)]
pub enum Outcome {
    Done,
    Interrupted,
    Failed(anyhow::Error),
}

/// What a torrent of the session is doing.
pub enum State {
    /// Its task is running, and stops once `stop` is set.
    Running {
        stop: watch::Sender<bool>,
    },
    /// Its task was stopped, to be started again.
    Paused,
    Ended(Outcome),
}

/// A torrent of the session.
pub struct Entry {
    /// What it was started from, and is started again from.
    pub source: Source,
    pub info_hash: [u8; 20],
    pub state: State,
}

/// Runs the torrents of a session through one `Client`, each in a task of
/// its own, and carries out the UI's commands. Each torrent stops when the
/// session's stop future resolves, or when it is asked to on its own.
///
/// `run_torrent` runs one torrent until it ends; tests give a fake one.
pub struct Manager<S, R> {
    client: Arc<Client>,
    /// Makes the session's stop future, once for each torrent.
    stop: S,
    run_torrent: R,
    entries: Vec<Entry>,
    tasks: JoinSet<Outcome>,
    /// The info hash of the entry each task runs.
    running: HashMap<task::Id, [u8; 20]>,
    /// Torrent files that could not be loaded, and so have no entry.
    unloaded: Vec<anyhow::Error>,
    /// Whether the session was started with more than one torrent: each
    /// one's logs then carry its name.
    several: bool,
    /// True while no task is running.
    idle: watch::Sender<bool>,
}

impl<S, F> Manager<S, RunTorrent>
where
    S: Fn() -> F,
    F: Future<Output = ()> + Send + 'static,
{
    /// A manager with no torrents yet, running them through `client`. Every
    /// torrent stops when the future `stop` makes resolves.
    pub fn new(client: Arc<Client>, stop: S) -> Self {
        Self::with_runner(client, stop, run_torrent)
    }
}

impl<S, F, R, Fut> Manager<S, R>
where
    S: Fn() -> F,
    F: Future<Output = ()> + Send + 'static,
    R: Fn(Arc<Client>, Torrent, StopSignal) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    /// `new`, running each torrent through `run_torrent`.
    pub fn with_runner(client: Arc<Client>, stop: S, run_torrent: R) -> Self {
        Self {
            client,
            stop,
            run_torrent,
            entries: Vec::new(),
            tasks: JoinSet::new(),
            running: HashMap::new(),
            unloaded: Vec::new(),
            several: false,
            idle: watch::Sender::new(true),
        }
    }

    /// Turns true whenever no torrent's task is running.
    pub fn idle(&self) -> watch::Receiver<bool> {
        self.idle.subscribe()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Starts every torrent in `sources` at once, loading the files first.
    /// One that fails does not stop the others. With several, each one's
    /// logs carry its name.
    pub async fn start(&mut self, sources: Vec<Source>) {
        self.several = sources.len() > 1;
        for source in sources {
            let span = if self.several {
                info_span!("torrent", name = %source.label())
            } else {
                Span::none()
            };
            let torrent = match &source {
                Source::File(path) => {
                    match torrent::load_metainfo(path).instrument(span.clone()).await {
                        Ok(metainfo) => Torrent::File(metainfo),
                        Err(e) => {
                            if self.several {
                                span.in_scope(|| error!("{e:#}"));
                            }
                            self.unloaded.push(e);
                            continue;
                        }
                    }
                }
                Source::Magnet(link) => Torrent::Magnet(link.clone()),
            };
            self.spawn(source, torrent, span);
        }
        self.idle.send_replace(self.tasks.is_empty());
    }

    /// Runs `torrent` in a task of its own, as the entry for `source`.
    fn spawn(&mut self, source: Source, torrent: Torrent, span: Span) {
        let info_hash = torrent.info_hash();
        let (stop, stopped) = watch::channel(false);
        let session = (self.stop)();
        let stop_signal: StopSignal = Box::pin(async move {
            tokio::select! {
                () = session => {}
                () = own_stop(stopped) => {}
            }
        });
        let run = (self.run_torrent)(Arc::clone(&self.client), torrent, stop_signal);
        let several = self.several;
        let task = self
            .tasks
            .spawn(async move { outcome(run.await, several) }.instrument(span));
        self.running.insert(task.id(), info_hash);
        self.entries.push(Entry {
            source,
            info_hash,
            state: State::Running { stop },
        });
    }

    /// Runs until no torrent's task is running and `commands` is closed,
    /// carrying out the commands meanwhile.
    ///
    /// A single torrent's failure is returned as the error; with several,
    /// each failure is logged and the exit code is a failure. Otherwise the
    /// exit code says whether a download was cut short.
    pub async fn run(mut self, commands: mpsc::UnboundedReceiver<Command>) -> Result<ExitCode> {
        self.drive(commands).await;
        self.exit_code()
    }

    /// `run`, up to the exit code.
    async fn drive(&mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let mut open = true;
        while open || !self.tasks.is_empty() {
            tokio::select! {
                Some(joined) = self.tasks.join_next_with_id() => self.ended(joined),
                command = commands.recv(), if open => match command {
                    Some(command) => self.command(command),
                    None => open = false,
                },
            }
        }
    }

    /// Records how a task ended in its entry.
    fn ended(&mut self, joined: Result<(task::Id, Outcome), JoinError>) {
        let (id, outcome) = match joined {
            Ok(ended) => ended,
            Err(e) => (e.id(), Outcome::Failed(e.into())),
        };
        if let Some(info_hash) = self.running.remove(&id)
            && let Some(entry) = self.entries.iter_mut().find(|entry| {
                entry.info_hash == info_hash && matches!(entry.state, State::Running { .. })
            })
        {
            entry.state = State::Ended(outcome);
        }
        self.idle.send_replace(self.tasks.is_empty());
    }

    fn command(&mut self, command: Command) {
        let limit = |kib: u64| match kib {
            0 => "none".to_string(),
            kib => format!("{kib} KiB/s"),
        };
        match command {
            Command::SetRateLimits(down, up) => {
                self.client.set_rate_limits(down, up);
                self.client.stats().notice(format!(
                    "rate limits: download {}, upload {}",
                    limit(down),
                    limit(up)
                ));
            }
            // The feed stops the torrents itself.
            Command::Stop => {}
        }
    }

    fn exit_code(self) -> Result<ExitCode> {
        let total = self.entries.len() + self.unloaded.len();
        let mut failed = self.unloaded;
        let mut interrupted = false;
        for entry in self.entries {
            match entry.state {
                State::Ended(Outcome::Failed(e)) => failed.push(e),
                State::Ended(Outcome::Interrupted) | State::Paused => interrupted = true,
                State::Ended(Outcome::Done) | State::Running { .. } => {}
            }
        }
        if !failed.is_empty() {
            if !self.several
                && let Some(e) = failed.pop()
            {
                return Err(e);
            }
            error!("{} of {total} torrents failed", failed.len());
            return Ok(ExitCode::FAILURE);
        }
        if interrupted {
            return Ok(ExitCode::from(EXIT_INTERRUPTED));
        }
        Ok(ExitCode::SUCCESS)
    }
}

/// Runs `torrent` through `client` until it ends.
fn run_torrent(
    client: Arc<Client>,
    torrent: Torrent,
    stop: StopSignal,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        match torrent {
            Torrent::File(metainfo) => download::download_metainfo(metainfo, &client, stop).await,
            Torrent::Magnet(link) => magnet::download_magnet(&link, &client, stop).await,
        }
    })
}

/// Resolves once `stop` is set; never if its sender is dropped first.
async fn own_stop(mut stop: watch::Receiver<bool>) {
    if stop.wait_for(|&stop| stop).await.is_err() {
        std::future::pending().await
    }
}

/// What `result` says of how a torrent ended, logged. With `several`, a
/// failure is logged here, since the session goes on.
fn outcome(result: Result<()>, several: bool) -> Outcome {
    match result {
        Ok(()) => Outcome::Done,
        Err(e) if e.is::<Interrupted>() || e.is::<FetchInterrupted>() => {
            warn!("{e}; run again to resume");
            Outcome::Interrupted
        }
        Err(e) => {
            if several {
                error!("{e:#}");
            }
            Outcome::Failed(e)
        }
    }
}

#[cfg(test)]
mod tests;
