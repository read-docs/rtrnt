//! The torrents of a session: starting each one, what it is doing, what the
//! UI asks of them, and the exit code once they have all ended.

use crate::client::Client;
use crate::download::{self, CheckedOnly, Interrupted};
use crate::magnet::{self, FetchInterrupted, MagnetLink};
use crate::resume;
use crate::selection::FileSelection;
use crate::session::{SavedLimits, SavedSession, SavedTorrent, SessionStore};
use crate::source::Source;
use crate::stats::{TorrentState, TorrentStats};
use crate::storage::{self, Storage};
use crate::torrent::{self, Info, Metainfo};
use anyhow::{Result, anyhow};
use futures_util::future::BoxFuture;
use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::{self, JoinError, JoinSet};
use tracing::{Instrument, Span, error, info, info_span, warn};

/// Exit code for a run stopped by Ctrl-C (128 + SIGINT).
pub const EXIT_INTERRUPTED: u8 = 130;

/// What the UI asks of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Stop the torrents, as Ctrl-C does.
    Stop,
    /// Change the download and upload limits to these KiB/s, 0 for none.
    SetRateLimits(u64, u64),
    /// Stop the running torrent with this info hash, to start it again
    /// later.
    Pause([u8; 20]),
    /// Start the paused or ended torrent with this info hash again, from
    /// where it got to.
    Resume([u8; 20]),
    /// Hash the files of the torrent with this info hash again, ignoring its
    /// resume file: a running one stops and carries on, a paused one stays
    /// paused, and one that has ended runs again.
    Recheck([u8; 20]),
    /// Drop the torrent with this info hash from the session, stopping it
    /// first if it runs, and with `delete` its files and resume file too.
    Remove { info_hash: [u8; 20], delete: bool },
    /// Start the torrent from this source too, unless the session has it.
    Add(Source),
}

/// Resolves when a torrent is to stop.
pub type StopSignal = BoxFuture<'static, ()>;

/// How a torrent is run for real: `download_metainfo` or `download_magnet`.
pub type RunTorrent =
    fn(Arc<Client>, Torrent, StartOptions, StopSignal) -> BoxFuture<'static, Result<()>>;

/// What differs between runs of one torrent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartOptions {
    /// Whether to hash the data on disk rather than trust the resume file.
    pub recheck: Recheck,
    /// Which files to download and which first, or `None` to go by the
    /// resume file, and failing that to download every file.
    pub selection: Option<FileSelection>,
}

/// Whether, and what next, a run hashes the data on disk, ignoring the
/// resume file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Recheck {
    /// Trust the resume file where it can be.
    #[default]
    No,
    /// Hash everything, then carry on.
    ThenRun,
    /// Hash everything, save the result and stop, paused.
    ThenPause,
}

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
    /// Stopped after checking the data, as `Recheck::ThenPause` asks.
    Checked,
    Failed(anyhow::Error),
}

/// What a torrent of the session is doing.
pub enum State {
    /// Its task is running, and stops once `stop` is set.
    Running {
        stop: watch::Sender<bool>,
    },
    /// Its task was stopped by a pause, to be started again.
    Paused,
    Ended(Outcome),
    /// Its task was asked to stop, as a pause asks, to be started again
    /// with `then` once it has.
    Restarting {
        stop: watch::Sender<bool>,
        then: StartOptions,
    },
    /// Its task was asked to stop, as a pause asks, and the entry goes once
    /// it has; with `delete`, its files go too.
    Removing {
        stop: watch::Sender<bool>,
        delete: bool,
    },
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
    /// Whether the session was started with more than one torrent, or can
    /// get more: each one's logs then carry its name, and each failure is
    /// logged as it comes rather than returned.
    several: bool,
    /// True while no task is running.
    idle: watch::Sender<bool>,
    /// Where the session is remembered, if it is.
    store: Option<SessionStore>,
    /// The limits to save: those the session had, until the UI sets others.
    saved_limits: Option<SavedLimits>,
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
    R: Fn(Arc<Client>, Torrent, StartOptions, StopSignal) -> Fut,
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
            store: None,
            saved_limits: None,
        }
    }

    /// Remembers the session in `store` from now on: its torrents, as they
    /// change, and the rate limits the UI sets. A `.torrent` file a torrent
    /// is added from is copied into the store, and the entry runs from the
    /// copy, which goes when the torrent is removed.
    pub fn with_session(mut self, store: SessionStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Turns true whenever no torrent's task is running.
    pub fn idle(&self) -> watch::Receiver<bool> {
        self.idle.subscribe()
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// Runs the session as one of several torrents however many it starts
    /// with, since the UI can add more (see `several`).
    pub fn expect_more(&mut self) {
        self.several = true;
    }

    /// Starts every torrent in `sources` at once with `options`, loading
    /// the files first. One that fails does not stop the others. With
    /// several, each one's logs carry its name.
    pub async fn start(&mut self, sources: Vec<Source>, options: StartOptions) {
        self.several |= sources.len() > 1;
        for source in sources {
            let span = self.span(&source);
            let torrent = match load(&source, &span).await {
                Ok(torrent) => torrent,
                Err(e) => {
                    if self.several {
                        span.in_scope(|| error!("{e:#}"));
                    }
                    self.unloaded.push(e);
                    continue;
                }
            };
            let info_hash = torrent.info_hash();
            let source = self.keep_copy(source, info_hash).await;
            let state = self.spawn(torrent, options.clone(), span);
            self.entries.push(Entry {
                source,
                info_hash,
                state,
            });
        }
        self.idle.send_replace(self.tasks.is_empty());
        self.save_session().await;
    }

    /// Puts the torrents of a saved session back, in its order and before
    /// any `start`: a paused one as paused, with a row to show it, and the
    /// others running. One the session has twice, or whose source no longer
    /// loads, is left out, and the latter is logged and counts as failed.
    /// The session's rate limits are kept for the next save, unless the UI
    /// sets others; applying them is up to the caller (see `config`).
    pub async fn restore(&mut self, session: SavedSession) {
        self.saved_limits = session.rate_limits;
        for saved in session.torrents {
            if self.entries.iter().any(|e| e.info_hash == saved.info_hash) {
                continue;
            }
            let span = self.span(&saved.source);
            let torrent = match load(&saved.source, &span).await {
                Ok(torrent) if torrent.info_hash() == saved.info_hash => torrent,
                loaded => {
                    let e = loaded
                        .err()
                        .unwrap_or_else(|| anyhow!("the torrent file has changed"));
                    span.in_scope(|| error!("cannot restore: {e:#}"));
                    self.client
                        .stats()
                        .notice(format!("cannot restore {}: {e:#}", saved.source.label()));
                    self.unloaded.push(e);
                    continue;
                }
            };
            let state = if saved.paused {
                self.show_paused(&torrent, &saved.source);
                State::Paused
            } else {
                self.spawn(torrent, StartOptions::default(), span)
            };
            self.entries.push(Entry {
                source: saved.source,
                info_hash: saved.info_hash,
                state,
            });
        }
        self.idle.send_replace(self.tasks.is_empty());
        self.save_session().await;
    }

    /// Gives a paused torrent that has not run this session a row, in the
    /// state a pause leaves, which a resume takes the place of.
    fn show_paused(&self, torrent: &Torrent, source: &Source) {
        let name = source.label();
        let stats = self.client.stats().add_torrent(
            torrent.info_hash(),
            &name,
            &self.client.config().output_dir,
        );
        if let Torrent::File(metainfo) = torrent {
            stats.set_torrent(&metainfo.torrent);
        }
        stats.set_state(TorrentState::Paused);
    }

    /// With a session store, copies the `.torrent` file `source` names into
    /// it, and returns the source of the copy; anything else, or a copy
    /// that cannot be made (logged), is returned as it is.
    async fn keep_copy(&self, source: Source, info_hash: [u8; 20]) -> Source {
        let (Some(store), Source::File(path)) = (&self.store, &source) else {
            return source;
        };
        let (store, from) = (store.clone(), path.clone());
        let stored = task::spawn_blocking(move || store.store_torrent(&from, info_hash)).await;
        match stored {
            Ok(Ok(copy)) => Source::File(copy),
            Ok(Err(e)) => {
                warn!("cannot keep a copy of the torrent for the session: {e:#}");
                source
            }
            Err(e) => {
                warn!("cannot keep a copy of the torrent for the session: {e}");
                source
            }
        }
    }

    /// The session as it is now, for the session file. A torrent that is
    /// paused, or stopping to be, is saved as paused; one running or ended,
    /// as running, so that a session that quits (which ends its torrents)
    /// finds them running next time. One being removed is left out.
    fn session(&self) -> SavedSession {
        let torrents = self
            .entries
            .iter()
            .filter_map(|entry| {
                let paused = match &entry.state {
                    State::Paused => true,
                    State::Running { stop } => *stop.borrow(),
                    State::Restarting { .. } | State::Ended(_) => false,
                    State::Removing { .. } => return None,
                };
                Some(SavedTorrent {
                    info_hash: entry.info_hash,
                    source: entry.source.clone(),
                    paused,
                })
            })
            .collect();
        SavedSession {
            torrents,
            rate_limits: self.saved_limits,
        }
    }

    /// Writes the session file, if the session is remembered, on the
    /// blocking pool. A failure is logged and the session goes on.
    async fn save_session(&self) {
        let Some(store) = self.store.clone() else {
            return;
        };
        let session = self.session();
        match task::spawn_blocking(move || store.save(&session)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("cannot save the session: {e:#}"),
            Err(e) => warn!("cannot save the session: {e}"),
        }
    }

    /// What the logs of the torrent from `source` are under: its name, with
    /// several torrents.
    fn span(&self, source: &Source) -> Span {
        if self.several {
            info_span!("torrent", name = %source.label())
        } else {
            Span::none()
        }
    }

    /// Runs `torrent` with `options` in a task of its own, and returns the
    /// state of its entry.
    fn spawn(&mut self, torrent: Torrent, options: StartOptions, span: Span) -> State {
        let info_hash = torrent.info_hash();
        let (stop, stopped) = watch::channel(false);
        let paused = stopped.clone();
        let session = (self.stop)();
        let stop_signal: StopSignal = Box::pin(async move {
            tokio::select! {
                () = session => {}
                () = own_stop(stopped) => {}
            }
        });
        let run = (self.run_torrent)(Arc::clone(&self.client), torrent, options, stop_signal);
        let several = self.several;
        let task = self.tasks.spawn(
            async move {
                let result = run.await;
                outcome(result, several, *paused.borrow())
            }
            .instrument(span),
        );
        self.running.insert(task.id(), info_hash);
        State::Running { stop }
    }

    /// Runs until no torrent's task is running and `commands` is closed,
    /// carrying out the commands meanwhile.
    ///
    /// A single torrent's failure is returned as the error; with several,
    /// each failure is logged and the exit code is a failure. Otherwise the
    /// exit code says whether a download was cut short.
    pub async fn run(mut self, commands: mpsc::UnboundedReceiver<Command>) -> Result<ExitCode> {
        self.drive(commands).await;
        self.save_session().await;
        self.exit_code()
    }

    /// `run`, up to the exit code.
    async fn drive(&mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        let mut open = true;
        while open || !self.tasks.is_empty() {
            tokio::select! {
                Some(joined) = self.tasks.join_next_with_id() => self.ended(joined).await,
                command = commands.recv(), if open => match command {
                    Some(command) => self.command(command).await,
                    None => open = false,
                },
            }
        }
    }

    /// Records how a task ended in its entry, or drops the entry if it was
    /// being removed.
    async fn ended(&mut self, joined: Result<(task::Id, Outcome), JoinError>) {
        let (id, outcome) = match joined {
            Ok(ended) => ended,
            Err(e) => (e.id(), Outcome::Failed(e.into())),
        };
        let index = self.running.remove(&id).and_then(|info_hash| {
            self.entries.iter().position(|entry| {
                entry.info_hash == info_hash
                    && matches!(
                        entry.state,
                        State::Running { .. } | State::Restarting { .. } | State::Removing { .. }
                    )
            })
        });
        if let Some(index) = index
            && let State::Removing { delete, .. } = self.entries[index].state
        {
            let entry = self.entries.remove(index);
            self.forget(entry, delete).await;
        } else if let Some(index) = index
            && let State::Restarting { then, .. } = &self.entries[index].state
        {
            let then = then.clone();
            self.start_again(index, then, "restarting", "restart").await;
        } else if let Some(index) = index {
            let span = self.span(&self.entries[index].source);
            let entry = &mut self.entries[index];
            let info_hash = entry.info_hash;
            let paused = matches!(&entry.state, State::Running { stop } if *stop.borrow());
            entry.state = match outcome {
                Outcome::Interrupted if paused => {
                    let pausing = |state: &TorrentState| *state == TorrentState::Pausing;
                    if let Some(stats) = stats_of(&self.client, info_hash, pausing) {
                        stats.set_state(TorrentState::Paused);
                    }
                    span.in_scope(|| info!("paused"));
                    State::Paused
                }
                Outcome::Checked => {
                    span.in_scope(|| info!("checked; paused"));
                    State::Paused
                }
                Outcome::Failed(e) => {
                    // A torrent run again that failed before it had stats
                    // of its own still shows its earlier run's.
                    let paused = |state: &TorrentState| *state == TorrentState::Paused;
                    if let Some(stats) = stats_of(&self.client, info_hash, paused) {
                        stats.set_state(TorrentState::Failed(format!("{e:#}")));
                    }
                    State::Ended(Outcome::Failed(e))
                }
                outcome => State::Ended(outcome),
            };
        }
        self.idle.send_replace(self.tasks.is_empty());
        self.save_session().await;
    }

    /// Asks the running torrent with info hash `info_hash` to stop, for a
    /// pause; its task ends as it does on Ctrl-C. One that is to restart
    /// is paused instead, once it has stopped.
    fn pause(&mut self, info_hash: [u8; 20]) {
        let restarting = self.entries.iter_mut().find(|entry| {
            entry.info_hash == info_hash && matches!(entry.state, State::Restarting { .. })
        });
        if let Some(entry) = restarting {
            entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
                State::Restarting { stop, .. } => State::Running { stop },
                state => state,
            };
            return;
        }
        let Some(State::Running { stop }) = self
            .entries
            .iter()
            .find(|entry| {
                entry.info_hash == info_hash && matches!(entry.state, State::Running { .. })
            })
            .map(|entry| &entry.state)
        else {
            return;
        };
        if stop.send_replace(true) {
            return;
        }
        let running = |state: &TorrentState| !state.is_finished();
        if let Some(stats) = stats_of(&self.client, info_hash, running) {
            stats.set_state(TorrentState::Pausing);
        }
    }

    /// Asks the running torrent with info hash `info_hash` to stop, as a
    /// pause does, and starts it again from its source with `then` once it
    /// has. A torrent that is not running is left alone.
    pub fn restart(&mut self, info_hash: [u8; 20], then: StartOptions) {
        let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        let State::Running { stop } = &entry.state else {
            return;
        };
        if stop.send_replace(true) {
            // Already asked to stop, by a pause.
            return;
        }
        let rechecking = then.recheck != Recheck::No;
        entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
            State::Running { stop } => State::Restarting { stop, then },
            state => state,
        };
        let running = |state: &TorrentState| !state.is_finished();
        if let Some(stats) = stats_of(&self.client, info_hash, running) {
            stats.set_state(if rechecking {
                TorrentState::QueuedForChecking
            } else {
                TorrentState::Pausing
            });
        }
    }

    /// Hashes the files of the torrent with info hash `info_hash` again.
    /// A running torrent is restarted to do it and carries on after; a
    /// paused one is checked and stays paused; one that has ended is checked
    /// and then runs, so a Done torrent found incomplete fetches the rest and
    /// a failed one gets a fresh try. One being removed or already
    /// restarting is left alone, and so is a magnet link with no metadata
    /// yet; a notice says why.
    async fn recheck(&mut self, info_hash: [u8; 20]) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        let name = || {
            stats_of(&self.client, info_hash, |_| true)
                .map_or_else(|| self.entries[index].source.label(), |stats| stats.name())
        };
        let refused = match &self.entries[index].state {
            State::Removing { .. } => Some("being removed, nothing to check"),
            State::Restarting { .. } => Some("already restarting"),
            State::Running { stop } if *stop.borrow() => Some("pausing, nothing to check"),
            State::Running { .. }
                if matches!(
                    stats_of(&self.client, info_hash, |_| true).map(|stats| stats.state()),
                    Some(TorrentState::FetchingMetadata { .. })
                ) =>
            {
                Some("nothing to check yet")
            }
            State::Paused | State::Ended(_) => match &self.entries[index].source {
                Source::Magnet(link)
                    if magnet::saved_torrent(link, &self.client.config().output_dir)
                        .await
                        .is_none() =>
                {
                    Some("nothing to check yet")
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(why) = refused {
            self.client.stats().notice(format!("{}: {why}", name()));
            return;
        }
        let recheck = |recheck| StartOptions {
            recheck,
            selection: None,
        };
        match self.entries[index].state {
            State::Running { .. } => self.restart(info_hash, recheck(Recheck::ThenRun)),
            State::Paused => {
                self.start_again(index, recheck(Recheck::ThenPause), "rechecking", "recheck")
                    .await;
            }
            _ => {
                self.start_again(index, recheck(Recheck::ThenRun), "rechecking", "recheck")
                    .await;
            }
        }
    }

    /// Starts the paused or ended torrent with info hash `info_hash` again,
    /// from its source. A torrent file is loaded again; a magnet link finds
    /// the metadata its earlier run saved, if it got that far.
    async fn resume(&mut self, info_hash: [u8; 20]) {
        let Some(index) = self.entries.iter().position(|entry| {
            entry.info_hash == info_hash && matches!(entry.state, State::Paused | State::Ended(_))
        }) else {
            return;
        };
        self.start_again(index, StartOptions::default(), "resuming", "resume")
            .await;
    }

    /// Runs the torrent of entry `index` again with `options`, from its
    /// source, as `resume` and `restart` do, or ends it as failed, with a
    /// notice that says it cannot `verb`; `doing` is what the log says.
    async fn start_again(&mut self, index: usize, options: StartOptions, doing: &str, verb: &str) {
        let info_hash = self.entries[index].info_hash;
        let source = self.entries[index].source.clone();
        let span = self.span(&source);
        let state = match load(&source, &span).await {
            Ok(torrent) if torrent.info_hash() == info_hash => {
                span.in_scope(|| info!("{doing}"));
                self.spawn(torrent, options, span.clone())
            }
            Ok(_) => State::Ended(Outcome::Failed(anyhow::anyhow!(
                "the torrent file has changed"
            ))),
            Err(e) => State::Ended(Outcome::Failed(e)),
        };
        if let State::Ended(Outcome::Failed(e)) = &state {
            span.in_scope(|| error!("{e:#}"));
            self.client.stats().notice(format!("cannot {verb}: {e:#}"));
            let ended = |state: &TorrentState| state.is_finished();
            if let Some(stats) = stats_of(&self.client, info_hash, ended) {
                stats.set_state(TorrentState::Failed(format!("{e:#}")));
            }
        }
        self.entries[index].state = state;
        self.idle.send_replace(self.tasks.is_empty());
    }

    /// Removes the torrent with info hash `info_hash` from the session: at
    /// once if it is paused or has ended, and otherwise once its task,
    /// stopped as a pause stops it, has ended. Its row shows `removing`
    /// meanwhile. With `delete`, its files go too.
    async fn remove(&mut self, info_hash: [u8; 20], delete: bool) {
        let with_hash = |entry: &Entry| entry.info_hash == info_hash;
        let running = self.entries.iter().position(|entry| {
            with_hash(entry)
                && matches!(
                    entry.state,
                    State::Running { .. } | State::Restarting { .. }
                )
        });
        if let Some(index) = running {
            let entry = &mut self.entries[index];
            entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
                State::Running { stop } | State::Restarting { stop, .. } => {
                    stop.send_replace(true);
                    State::Removing { stop, delete }
                }
                state => state,
            };
            let running = |state: &TorrentState| !state.is_finished();
            if let Some(stats) = stats_of(&self.client, info_hash, running) {
                stats.set_state(TorrentState::Removing);
            }
            return;
        }
        let stopped = self.entries.iter().position(|entry| {
            with_hash(entry) && matches!(entry.state, State::Paused | State::Ended(_))
        });
        if let Some(index) = stopped {
            let entry = self.entries.remove(index);
            self.forget(entry, delete).await;
        }
    }

    /// Starts the torrent from `source` as `start` does, with the session's
    /// settings, unless the session has it already or its file cannot be
    /// loaded; a notice then says why, and no row is made.
    ///
    /// The file is loaded here rather than in a task of its own: it is
    /// small, and the check for a torrent the session has is then never
    /// raced by another `Add` of it. The torrents run on meanwhile.
    async fn add(&mut self, source: Source) {
        let span = self.span(&source);
        let torrent = match load(&source, &span).await {
            Ok(torrent) => torrent,
            Err(e) => {
                span.in_scope(|| warn!("cannot add: {e:#}"));
                self.client
                    .stats()
                    .notice(format!("cannot add {}: {e:#}", source.label()));
                return;
            }
        };
        let info_hash = torrent.info_hash();
        if let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.info_hash == info_hash)
        {
            let why = match entry.state {
                State::Running { .. } => "already running",
                State::Restarting { .. } => "already running, restarting",
                State::Paused => "already added, press p to resume",
                State::Ended(_) => "already added, press p to run it again",
                State::Removing { .. } => "still being removed, add it again once it has gone",
            };
            let name = stats_of(&self.client, info_hash, |_| true)
                .map_or_else(|| source.label(), |stats| stats.name());
            self.client.stats().notice(format!("{name}: {why}"));
            return;
        }
        span.in_scope(|| info!("added"));
        let source = self.keep_copy(source, info_hash).await;
        let state = self.spawn(torrent, StartOptions::default(), span);
        self.entries.push(Entry {
            source,
            info_hash,
            state,
        });
        self.idle.send_replace(self.tasks.is_empty());
    }

    /// Drops the row of `entry`, removed from the session, and with
    /// `delete` its files, its resume file and a magnet link's saved
    /// `.torrent`. What cannot be deleted is logged and shown as a notice.
    async fn forget(&self, entry: Entry, delete: bool) {
        let gone = |state: &TorrentState| *state == TorrentState::Removing || state.is_finished();
        let stats = stats_of(&self.client, entry.info_hash, gone);
        let name = stats
            .as_ref()
            .map_or_else(|| entry.source.label(), |stats| stats.name());
        if let Some(stats) = &stats {
            self.client.stats().remove_torrent(stats);
        }
        let span = self.span(&entry.source);
        if !delete {
            self.drop_copy(&entry).await;
            span.in_scope(|| info!("removed"));
            self.client.stats().notice(format!("removed {name}"));
            return;
        }
        let failures = delete_data(&self.client, &entry)
            .instrument(span.clone())
            .await;
        self.drop_copy(&entry).await;
        span.in_scope(|| {
            for e in &failures {
                warn!("{e:#}");
            }
            info!("removed, with its files");
        });
        let notice = match failures.as_slice() {
            [] => format!("removed {name} and deleted its files"),
            [e] => format!("removed {name}, but: {e:#}"),
            [e, ..] => format!(
                "removed {name}, but {} things were not deleted: {e:#}",
                failures.len()
            ),
        };
        self.client.stats().notice(notice);
    }

    /// Deletes the copy of the `.torrent` the session kept for `entry`, if
    /// it has one, since nothing refers to it any more.
    async fn drop_copy(&self, entry: &Entry) {
        let (Some(store), Source::File(path)) = (&self.store, &entry.source) else {
            return;
        };
        if !store.holds(path) {
            return;
        }
        let (store, info_hash) = (store.clone(), entry.info_hash);
        match task::spawn_blocking(move || store.forget_torrent(info_hash)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("{e:#}"),
            Err(e) => warn!("{e}"),
        }
    }

    async fn command(&mut self, command: Command) {
        let limit = |kib: u64| match kib {
            0 => "none".to_string(),
            kib => format!("{kib} KiB/s"),
        };
        match command {
            Command::SetRateLimits(down, up) => {
                self.client.set_rate_limits(down, up);
                self.saved_limits = Some(SavedLimits {
                    download: down,
                    upload: up,
                });
                self.client.stats().notice(format!(
                    "rate limits: download {}, upload {}",
                    limit(down),
                    limit(up)
                ));
            }
            Command::Pause(info_hash) => self.pause(info_hash),
            Command::Resume(info_hash) => self.resume(info_hash).await,
            Command::Recheck(info_hash) => self.recheck(info_hash).await,
            Command::Remove { info_hash, delete } => self.remove(info_hash, delete).await,
            Command::Add(source) => self.add(source).await,
            // The feed stops the torrents itself.
            Command::Stop => {}
        }
        self.save_session().await;
    }

    fn exit_code(self) -> Result<ExitCode> {
        let total = self.entries.len() + self.unloaded.len();
        let mut failed = self.unloaded;
        let mut interrupted = false;
        for entry in self.entries {
            match entry.state {
                State::Ended(Outcome::Failed(e)) => failed.push(e),
                State::Paused => {
                    warn!(name = %entry.source.label(), "paused; run again to resume");
                    interrupted = true;
                }
                State::Ended(Outcome::Interrupted) => interrupted = true,
                // A removed torrent has no entry, so it counts for nothing.
                State::Ended(Outcome::Done | Outcome::Checked)
                | State::Running { .. }
                | State::Restarting { .. }
                | State::Removing { .. } => {}
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
    options: StartOptions,
    stop: StopSignal,
) -> BoxFuture<'static, Result<()>> {
    Box::pin(async move {
        match torrent {
            Torrent::File(metainfo) => {
                download::download_metainfo(metainfo, &options, &client, stop).await
            }
            Torrent::Magnet(link) => magnet::download_magnet(&link, &options, &client, stop).await,
        }
    })
}

/// Gets `source` ready to start, loading a torrent file, with its logs
/// under `span`.
async fn load(source: &Source, span: &Span) -> Result<Torrent> {
    Ok(match source {
        Source::File(path) => Torrent::File(
            torrent::load_metainfo(path)
                .instrument(span.clone())
                .await?,
        ),
        Source::Magnet(link) => Torrent::Magnet(link.clone()),
    })
}

/// Deletes the files of the torrent of `entry`, then its resume file and a
/// magnet link's saved `.torrent`, and returns what could not be. The files
/// are those the torrent file lists, or the metadata the magnet link saved;
/// a magnet link whose metadata never arrived has none. Nothing is deleted
/// while a running torrent writes any of the files.
async fn delete_data(client: &Arc<Client>, entry: &Entry) -> Vec<anyhow::Error> {
    let output_dir = client.config().output_dir.clone();
    let mut failures = Vec::new();
    let mut others = Vec::new();
    let info: Option<Info> = match &entry.source {
        Source::File(path) => match torrent::load_metainfo(path).await {
            Ok(metainfo) if metainfo.info_hash == entry.info_hash => Some(metainfo.torrent.info),
            Ok(_) => {
                failures.push(anyhow!(
                    "the torrent file has changed, so its files are not known"
                ));
                None
            }
            Err(e) => {
                failures.push(e.context("cannot tell which files are the torrent's"));
                None
            }
        },
        Source::Magnet(link) => match magnet::saved_torrent(link, &output_dir).await {
            Some(path) => {
                let info = torrent::load_file(&path).await;
                others.push(path);
                info.map(|(torrent, _)| torrent.info).ok()
            }
            None => None,
        },
    };
    if let Some(target) = resume::Target::new(client.config(), entry.info_hash) {
        others.push(target.path().to_path_buf());
    }
    let client = Arc::clone(client);
    let deleted = task::spawn_blocking(move || {
        let mut failures = Vec::new();
        if let Some(info) = info {
            match Storage::paths(&info, &output_dir) {
                Ok(paths) => {
                    if let Some(owner) = client.file_owner(&paths) {
                        return vec![anyhow!(
                            "nothing deleted, since the torrent {owner} writes its files"
                        )];
                    }
                    failures.extend(storage::delete_files(&info, &output_dir));
                }
                Err(e) => failures.push(e),
            }
        }
        for path in others {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => failures.push(
                    anyhow::Error::new(e).context(format!("failed to delete {}", path.display())),
                ),
            }
        }
        failures
    })
    .await;
    match deleted {
        Ok(more) => failures.extend(more),
        Err(e) => failures.push(anyhow::Error::new(e).context("failed to delete the files")),
    }
    failures
}

/// The stats of the torrent with info hash `info_hash` whose state is
/// `wanted`: of the run the manager means, should two share it.
fn stats_of(
    client: &Client,
    info_hash: [u8; 20],
    wanted: impl Fn(&TorrentState) -> bool,
) -> Option<Arc<TorrentStats>> {
    client
        .stats()
        .torrents()
        .into_iter()
        .find(|stats| stats.info_hash() == info_hash && wanted(&stats.state()))
}

/// Resolves once `stop` is set; never if its sender is dropped first.
async fn own_stop(mut stop: watch::Receiver<bool>) {
    if stop.wait_for(|&stop| stop).await.is_err() {
        std::future::pending().await
    }
}

/// What `result` says of how a torrent ended, logged. With `several`, a
/// failure is logged here, since the session goes on. A torrent `paused`
/// (or removed) can be resumed in this session, so it does not say to run
/// again.
fn outcome(result: Result<()>, several: bool, paused: bool) -> Outcome {
    match result {
        Ok(()) => Outcome::Done,
        Err(e) if e.is::<CheckedOnly>() => Outcome::Checked,
        Err(e) if e.is::<Interrupted>() || e.is::<FetchInterrupted>() => {
            // Stopped on its own, it was paused or is being removed, and the
            // manager says which.
            if !paused {
                warn!("{e}; run again to resume");
            }
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
