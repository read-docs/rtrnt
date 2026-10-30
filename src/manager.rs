//! The torrents of a session: starting each one, what it is doing, what the
//! UI asks of them, and the exit code once they have all ended.

use crate::altspeed::{Limits, Schedule, SpeedControl};
use crate::client::Client;
use crate::download::{self, CheckedOnly, Interrupted};
use crate::magnet::{self, FetchInterrupted, MagnetLink};
use crate::queue::{self, QueueLimits, Slot};
use crate::resume;
use crate::selection::{FilePicks, FileSelection, Priority};
use crate::session::{SavedAltSpeed, SavedLimits, SavedSession, SavedTorrent, SessionStore};
use crate::source::Source;
use crate::stats::{TorrentState, TorrentStats};
use crate::storage::{self, Storage};
use crate::tags::{self, TagDirectories, TagSettings};
use crate::torrent::{self, Info, Metainfo};
use crate::torrent_limits::TorrentSettings;
use anyhow::{Result, anyhow};

mod relocate;

/// Validate tracker URLs accepted by the HTTP and UDP announce clients.
pub fn validate_trackers(trackers: Vec<String>) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for tracker in trackers {
        let url = url::Url::parse(&tracker)?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https" | "udp") && url.host_str().is_some(),
            "tracker must be an http, https or udp URL"
        );
        anyhow::ensure!(
            url.scheme() != "udp" || url.port().is_some(),
            "UDP tracker needs a port"
        );
        if !result.contains(&tracker) {
            result.push(tracker);
        }
    }
    Ok(result)
}
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::{self, JoinError, JoinSet};
use tracing::{Instrument, Span, error, info, info_span, warn};

/// Exit code for a run stopped by Ctrl-C (128 + SIGINT).
pub const EXIT_INTERRUPTED: u8 = 130;

/// How often the queue is looked at when nothing else happens, since a
/// download that completes and starts seeding is not an event the manager
/// is told of.
const QUEUE_TICK: Duration = Duration::from_secs(1);

/// What the UI asks of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Replace the trackers, one URL per tier, and restart a running torrent.
    SetTrackers {
        info_hash: [u8; 20],
        trackers: Vec<String>,
    },
    /// Move a paused torrent’s existing data into this directory.
    MoveData { info_hash: [u8; 20], dir: PathBuf },
    /// Stop the torrents, as Ctrl-C does.
    Stop,
    /// Change the download and upload limits to these KiB/s, 0 for none.
    SetRateLimits(u64, u64),
    /// Switch between the normal and the alternative speed limits, until
    /// the schedule next changes (see `altspeed`).
    ToggleAltSpeed,
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
    /// Download the files of the torrent with this info hash by these
    /// priorities, one per file: a running torrent restarts to do it, and
    /// one that is paused or has ended keeps them for when it runs again.
    SetPriorities {
        info_hash: [u8; 20],
        priorities: Vec<Priority>,
    },
    /// Turn sequential download on or off for the torrent with this info
    /// hash, whatever it is doing: a running download changes at once.
    ToggleSequential([u8; 20]),
    /// Give the torrent with this info hash its own limits and seed
    /// settings (see `torrent_limits`), live: a running torrent does not
    /// restart.
    SetTorrentLimits {
        info_hash: [u8; 20],
        settings: TorrentSettings,
    },
    /// Give the torrent with this info hash these tags, in place of its
    /// own (see `tags`): limits apply at once; tag directories move stopped
    /// torrents only, unless an explicit per-torrent directory wins.
    SetTags {
        info_hash: [u8; 20],
        tags: Vec<String>,
    },
    /// Drop the torrent with this info hash from the session, stopping it
    /// first if it runs, and with `delete` its files and resume file too.
    Remove { info_hash: [u8; 20], delete: bool },
    /// Start the torrent from this source too, unless the session has it.
    Add(Source),
    /// Add the torrent from this source as `Add` does, but paused: it is
    /// listed, and starts when it is resumed.
    AddPaused(Source),
    /// Add the torrent from this source as `Add` does, or as `AddPaused`
    /// does if `paused`, with its files in `dir` rather than in the
    /// session's output directory (see `Manager::dir_of`).
    AddIn {
        source: Source,
        paused: bool,
        dir: PathBuf,
    },
    /// Move the torrent with this info hash one place earlier in the queue
    /// and the table, which is the order torrents get a free slot in.
    MoveUp([u8; 20]),
    /// Move it one place later.
    MoveDown([u8; 20]),
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
    /// Files picked on the command line, for a torrent run the first time:
    /// they take the place of `selection` once the torrent's files are
    /// known (a magnet link's, not until its metadata arrives).
    pub picks: Option<FilePicks>,
    /// Whether to download in order, for a torrent the session had saved
    /// as doing so; `None` to keep what it had, and failing that the
    /// configured default.
    pub sequential: Option<bool>,
    /// The directory to download into, if the torrent has one of its own
    /// rather than the client's `output_dir`. The manager fills it in for
    /// each run from what it keeps for the torrent, whatever is set here.
    pub output_dir: Option<PathBuf>,
    /// Tracker URLs overriding the source, one per tier; an empty list disables trackers.
    pub trackers: Option<Vec<String>>,
}

impl StartOptions {
    /// Where a run with these options puts the torrent's files: its own
    /// directory, or else the one in `config`.
    pub fn output_dir<'a>(&'a self, config: &'a crate::download::DownloadConfig) -> &'a Path {
        self.output_dir.as_deref().unwrap_or(&config.output_dir)
    }
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
    File(Box<Metainfo>),
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
    /// Waiting for a slot (see `queue`): no task runs. It starts with
    /// `options` when there is one for it. `seeding` is whether it was
    /// complete when it was last running, so that it waits for a seed slot
    /// rather than a download slot.
    Queued {
        seeding: bool,
        options: StartOptions,
    },
    /// A seed over the limit of active seeds, stopping to be queued: its
    /// task was asked to stop as a pause asks.
    Requeuing {
        stop: watch::Sender<bool>,
    },
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
    /// Which limits apply: the normal ones, the alternative ones, the
    /// schedule between them and the user's switch.
    speed: SpeedControl,
    /// The alternative limits to save: those the UI set, if it did.
    saved_alt_limits: Option<SavedLimits>,
    /// Whether the alternative limits were on when last applied.
    alt_on: bool,
    /// How many torrents may run at once, before the rest queue.
    limits: QueueLimits,
    /// What each tag gives the torrents that have it.
    tag_settings: TagSettings,
    tag_directories: TagDirectories,
    /// Directories from tags may follow later tag edits; explicit moves win.
    tag_dirs: HashSet<[u8; 20]>,
    /// The torrents that are saved in a directory of their own, by info
    /// hash, rather than in the client's output directory.
    dirs: HashMap<[u8; 20], PathBuf>,
    trackers: HashMap<[u8; 20], Vec<String>>,
}

/// A limit in KiB/s as a notice says it.
fn limit_text(kib: u64) -> String {
    match kib {
        0 => "none".to_string(),
        kib => format!("{kib} KiB/s"),
    }
}

/// How often the schedule of the alternative speed limits is looked at.
const SPEED_TICK: Duration = Duration::from_secs(10);

/// The local time, to which the schedule of the alternative limits goes.
fn local_now() -> chrono::NaiveDateTime {
    chrono::Local::now().naive_local()
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
            limits: QueueLimits::default(),
            tag_settings: TagSettings::new(),
            tag_directories: TagDirectories::new(),
            tag_dirs: HashSet::new(),
            dirs: HashMap::new(),
            trackers: HashMap::new(),
            speed: SpeedControl::new(
                client.rate_limits().kib(),
                (
                    crate::config::DEFAULT_ALT_SPEED_DOWNLOAD,
                    crate::config::DEFAULT_ALT_SPEED_UPLOAD,
                ),
                Schedule::default(),
            ),
            saved_alt_limits: None,
            alt_on: false,
            client,
        }
    }

    /// Has the alternative limits `alt` apply on `schedule`, with the normal
    /// limits the client has now. They take effect when the manager runs,
    /// or at `restore`.
    pub fn with_alt_speed(mut self, alt: Limits, schedule: Schedule) -> Self {
        self.speed = SpeedControl::new(self.client.rate_limits().kib(), alt, schedule);
        self
    }

    /// Has the tags give their torrents the settings in `tag_settings` (see
    /// `tags`), for those with a tag already (see `restore`) and those
    /// tagged later.
    pub fn with_tag_settings(mut self, tag_settings: TagSettings) -> Self {
        self.tag_settings = tag_settings;
        self
    }

    /// Gives tags their download directories. A tag edit can move stopped
    /// data; active torrents require pausing and reapplying the tags first.
    pub fn with_tag_directories(mut self, directories: TagDirectories) -> Self {
        self.tag_directories = directories;
        self
    }

    /// Runs only as many torrents at once as `limits` allow: the others
    /// wait, shown as queued, in the order of the table, and start as
    /// others end, pause, or (for downloads) complete.
    pub fn with_queue_limits(mut self, limits: QueueLimits) -> Self {
        self.limits = limits;
        self
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
    /// the files first. One that fails does not stop the others, and one
    /// the session has already, as `restore` or an earlier source put
    /// there, is skipped with a notice. With
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
            if self.refuse_known(info_hash, &source) {
                continue;
            }
            let source = self.keep_copy(source, info_hash).await;
            let state = self.place(torrent, &source, options.clone(), span);
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
    /// others running, or queued if the limits leave no slot. One the session has twice, or whose source no longer
    /// loads, is left out, and the latter is logged and counts as failed.
    /// The session's rate limits are kept for the next save, unless the UI
    /// sets others; applying them is up to the caller (see `config`).
    pub async fn restore(&mut self, session: SavedSession) {
        self.saved_limits = session.rate_limits;
        if let Some(alt) = session.alt_speed {
            // The limits themselves reach `with_alt_speed` through the
            // config; they are only kept for the next save.
            self.saved_alt_limits = alt.limits;
            self.speed.manual = alt.manual;
        }
        self.apply_speed();
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
            if let Some(trackers) = saved.trackers
                && let Ok(trackers) = validate_trackers(trackers)
            {
                self.trackers.insert(saved.info_hash, trackers);
            }
            if let Some(dir) = saved.dir {
                self.dirs.insert(saved.info_hash, dir);
                if saved.dir_from_tag {
                    self.tag_dirs.insert(saved.info_hash);
                }
            }
            self.client
                .stats()
                .torrent_limits(saved.info_hash)
                .set(saved.limits);
            self.apply_tags(saved.info_hash, saved.tags);
            let state = if saved.paused {
                self.show_row(&torrent, &saved.source, TorrentState::Paused);
                if let Some(on) = saved.sequential
                    && let Some(stats) = stats_of(&self.client, saved.info_hash, |_| true)
                {
                    stats.set_sequential(on);
                }
                State::Paused
            } else {
                let options = StartOptions {
                    sequential: saved.sequential,
                    ..StartOptions::default()
                };
                self.place(torrent, &saved.source, options, span)
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

    /// Where the torrent with `info_hash` keeps its files: the directory of
    /// its own, if it has one, and otherwise the client's output directory.
    fn dir_of(&self, info_hash: [u8; 20]) -> &Path {
        self.dirs
            .get(&info_hash)
            .map_or(self.client.config().output_dir.as_path(), PathBuf::as_path)
    }

    /// Gives a torrent that has not run this session a row, in `state`
    /// (paused, or queued), which its first run takes the place of.
    fn show_row(&self, torrent: &Torrent, source: &Source, state: TorrentState) {
        let name = source.label();
        let dir = self.dir_of(torrent.info_hash());
        let stats = self
            .client
            .stats()
            .add_torrent(torrent.info_hash(), &name, dir);
        if let Torrent::File(metainfo) = torrent {
            stats.set_torrent(&metainfo.torrent);
            let target = resume::Target::in_dir(self.client.config(), dir, torrent.info_hash());
            let files = crate::selection::file_count(&metainfo.torrent.info);
            if let Some(selection) =
                target.and_then(|target| resume::load_selection(&target, files))
                && let Ok(wanted) = crate::selection::wanted_of(&metainfo.torrent, &selection)
            {
                stats.set_selection(selection.for_files(files), wanted);
            }
        }
        if let Some(trackers) = self.trackers.get(&torrent.info_hash()) {
            stats.set_trackers(
                &trackers
                    .iter()
                    .map(|url| vec![url.clone()])
                    .collect::<Vec<_>>(),
            );
        }
        stats.set_state(state);
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
                    State::Restarting { .. }
                    | State::Requeuing { .. }
                    | State::Queued { .. }
                    | State::Ended(_) => false,
                    State::Removing { .. } => return None,
                };
                Some(SavedTorrent {
                    info_hash: entry.info_hash,
                    source: entry.source.clone(),
                    paused,
                    sequential: stats_of(&self.client, entry.info_hash, |_| true)
                        .and_then(|stats| stats.sequential_choice()),
                    limits: self.client.stats().torrent_settings(entry.info_hash),
                    tags: self.client.stats().torrent_tags(entry.info_hash),
                    dir: self.dirs.get(&entry.info_hash).cloned(),
                    dir_from_tag: self.tag_dirs.contains(&entry.info_hash),
                    trackers: self.trackers.get(&entry.info_hash).cloned(),
                })
            })
            .collect();
        let alt_speed = SavedAltSpeed {
            limits: self.saved_alt_limits,
            manual: self.speed.manual,
        };
        SavedSession {
            torrents,
            rate_limits: self.saved_limits,
            alt_speed: (alt_speed != SavedAltSpeed::default()).then_some(alt_speed),
        }
    }

    /// Applies the limits that go with the time and the user's switch, and
    /// says so when the mode changes.
    fn apply_speed(&mut self) {
        let now = local_now();
        self.speed.expire(now);
        let (down, up) = self.speed.limits(now);
        if self.client.rate_limits().kib() != (down, up) {
            self.client.set_rate_limits(down, up);
        }
        let alt = self.speed.is_alt(now);
        self.client.stats().set_alt_speed(alt);
        if alt != self.alt_on {
            self.alt_on = alt;
            self.client.stats().notice(format!(
                "alternative speed limits {}: download {}, upload {}",
                if alt { "on" } else { "off" },
                limit_text(down),
                limit_text(up)
            ));
        }
    }

    /// Puts the rows of the table in the queue's order, which is the order
    /// of the entries.
    fn publish_order(&self) {
        let order: Vec<[u8; 20]> = self.entries.iter().map(|entry| entry.info_hash).collect();
        self.client.stats().arrange(&order);
    }

    /// Writes the session file, if the session is remembered, on the
    /// blocking pool, and settles the order of the table. A failure is
    /// logged and the session goes on.
    async fn save_session(&self) {
        self.publish_order();
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
        let options = StartOptions {
            output_dir: self.dirs.get(&info_hash).cloned(),
            trackers: self.trackers.get(&info_hash).cloned(),
            ..options
        };
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
        self.apply_speed();
        // Only a schedule needs looking at as time passes.
        let mut speed_tick = (!self.speed.schedule.is_empty()).then(|| {
            let mut tick = tokio::time::interval(SPEED_TICK);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick
        });
        let mut queue_tick = tokio::time::interval(QUEUE_TICK);
        queue_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while open || !self.tasks.is_empty() {
            tokio::select! {
                () = async {
                    match speed_tick.as_mut() {
                        Some(tick) => { tick.tick().await; }
                        None => std::future::pending().await,
                    }
                } => self.apply_speed(),
                _ = queue_tick.tick() => {
                    if self.schedule().await {
                        self.idle.send_replace(self.tasks.is_empty());
                        self.save_session().await;
                    } else {
                        self.publish_order();
                    }
                }
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
                        State::Running { .. }
                            | State::Restarting { .. }
                            | State::Requeuing { .. }
                            | State::Removing { .. }
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
            let requeued = matches!(&entry.state, State::Requeuing { .. });
            entry.state = match outcome {
                Outcome::Interrupted if requeued => {
                    let pausing = |state: &TorrentState| *state == TorrentState::Pausing;
                    if let Some(stats) = stats_of(&self.client, info_hash, pausing) {
                        stats.set_state(TorrentState::Queued);
                    }
                    span.in_scope(|| info!("queued: too many seeding"));
                    State::Queued {
                        seeding: true,
                        options: StartOptions::default(),
                    }
                }
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
        self.schedule().await;
        self.idle.send_replace(self.tasks.is_empty());
        self.save_session().await;
    }

    /// Asks the running torrent with info hash `info_hash` to stop, for a
    /// pause; its task ends as it does on Ctrl-C. One that is to restart
    /// is paused instead, once it has stopped.
    fn pause(&mut self, info_hash: [u8; 20]) {
        let stopping = self.entries.iter_mut().find(|entry| {
            entry.info_hash == info_hash
                && matches!(
                    entry.state,
                    State::Restarting { .. } | State::Requeuing { .. }
                )
        });
        if let Some(entry) = stopping {
            entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
                State::Restarting { stop, .. } | State::Requeuing { stop } => {
                    State::Running { stop }
                }
                state => state,
            };
            return;
        }
        let queued = self.entries.iter_mut().find(|entry| {
            entry.info_hash == info_hash && matches!(entry.state, State::Queued { .. })
        });
        if let Some(entry) = queued {
            entry.state = State::Paused;
            if let Some(stats) = stats_of(&self.client, info_hash, TorrentState::is_finished) {
                stats.set_state(TorrentState::Paused);
            }
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
            State::Queued { .. } => Some("queued, nothing to check yet"),
            State::Requeuing { .. } => Some("being queued, nothing to check yet"),
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
                    if magnet::saved_torrent(link, self.dir_of(info_hash))
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
            ..StartOptions::default()
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

    /// Has the torrent with info hash `info_hash` download its files by
    /// `priorities`. A running torrent is restarted with them, which hashes
    /// nothing when its resume file is good; one already restarting takes
    /// them in place of the options it had; a paused one or one that has
    /// ended saves them in its resume file, for its next run. One being
    /// removed or pausing is left alone, and so is one whose files are not
    /// known yet (a magnet link without metadata), or that lists another
    /// number of files; a notice says why.
    async fn set_priorities(&mut self, info_hash: [u8; 20], priorities: Vec<Priority>) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        let name = stats_of(&self.client, info_hash, |_| true)
            .map_or_else(|| self.entries[index].source.label(), |stats| stats.name());
        let selection = FileSelection::new(priorities);
        let refused = match &mut self.entries[index].state {
            State::Removing { .. } => Some("being removed, no files to choose"),
            State::Restarting { then, .. } => {
                then.selection = Some(selection);
                return;
            }
            State::Running { stop } if *stop.borrow() => {
                Some("pausing, choose the files once it has stopped")
            }
            State::Requeuing { .. } => Some("being queued, choose the files once it has stopped"),
            State::Running { .. } => {
                let files = stats_of(&self.client, info_hash, |_| true)
                    .and_then(|stats| stats.layout().map(|layout| layout.files.len()));
                match files {
                    None => Some("no files to choose yet"),
                    Some(files) if files != selection.priorities().len() => {
                        Some("that lists the wrong number of files")
                    }
                    Some(_) => None,
                }
            }
            State::Paused | State::Queued { .. } | State::Ended(_) => {
                let ended = matches!(self.entries[index].state, State::Ended(_));
                match self.keep_selection(index, &selection).await {
                    Ok(()) => {
                        let later = if ended {
                            "press p to download them"
                        } else {
                            "they apply once it resumes"
                        };
                        self.client
                            .stats()
                            .notice(format!("{name}: file choice saved, {later}"));
                    }
                    Err(why) => self.client.stats().notice(format!("{name}: {why}")),
                }
                return;
            }
        };
        if let Some(why) = refused {
            self.client.stats().notice(format!("{name}: {why}"));
            return;
        }
        let then = StartOptions {
            selection: Some(selection),
            ..StartOptions::default()
        };
        self.restart(info_hash, then);
    }

    /// Turns sequential download on or off for the torrent with info hash
    /// `info_hash`, with a notice saying which. It takes effect at once in a
    /// running download, and in any later run of one that is not.
    fn toggle_sequential(&self, info_hash: [u8; 20]) {
        let Some(stats) = stats_of(&self.client, info_hash, |state| {
            !matches!(state, TorrentState::Removing)
        }) else {
            return;
        };
        let how = if stats.toggle_sequential() {
            "in order, first and last pieces first"
        } else {
            "rarest first"
        };
        self.client
            .stats()
            .notice(format!("{}: downloading {how}", stats.name()));
    }

    /// Saves `selection` in the resume file of the torrent of entry
    /// `index`, which is not running, and shows it in the torrent's row.
    /// Fails with what to tell the user.
    async fn keep_selection(
        &self,
        index: usize,
        selection: &FileSelection,
    ) -> std::result::Result<(), String> {
        let entry = &self.entries[index];
        let torrent = match &entry.source {
            Source::File(path) => torrent::load_metainfo(path)
                .await
                .ok()
                .filter(|metainfo| metainfo.info_hash == entry.info_hash)
                .map(|metainfo| metainfo.torrent),
            Source::Magnet(link) => {
                match magnet::saved_torrent(link, self.dir_of(entry.info_hash)).await {
                    Some(path) => torrent::load_file(&path)
                        .await
                        .ok()
                        .map(|(torrent, _)| torrent),
                    None => None,
                }
            }
        };
        let torrent = torrent.ok_or("no files to choose yet")?;
        let files = crate::selection::file_count(&torrent.info);
        if selection.priorities().len() != files {
            return Err("that lists the wrong number of files".into());
        }
        let wanted = crate::selection::wanted_of(&torrent, selection)
            .map_err(|e| format!("cannot choose the files: {e:#}"))?;
        let dir = self.dir_of(entry.info_hash);
        let target = resume::Target::in_dir(self.client.config(), dir, entry.info_hash)
            .ok_or("no state directory, so the choice cannot be kept")?;
        target
            .save_selection(selection, files, torrent.num_pieces())
            .await
            .map_err(|e| format!("cannot save the choice of files: {e:#}"))?;
        if let Some(stats) = stats_of(&self.client, entry.info_hash, |_| true) {
            stats.set_selection(selection.for_files(files), wanted);
        }
        Ok(())
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
        // With no row, a queued torrent would not show; it starts anyway.
        let row = |state: &TorrentState| state.is_finished();
        if !self.fits()
            && let Some(stats) = stats_of(&self.client, info_hash, row)
        {
            stats.set_state(TorrentState::Queued);
            self.entries[index].state = State::Queued {
                seeding: false,
                options: StartOptions::default(),
            };
            return;
        }
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
                    State::Running { .. } | State::Restarting { .. } | State::Requeuing { .. }
                )
        });
        if let Some(index) = running {
            let entry = &mut self.entries[index];
            entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
                State::Running { stop }
                | State::Restarting { stop, .. }
                | State::Requeuing { stop } => {
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
            with_hash(entry)
                && matches!(
                    entry.state,
                    State::Paused | State::Queued { .. } | State::Ended(_)
                )
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
    /// raced by another `Add` of it. The torrents run on meanwhile. With
    /// `paused` the torrent is listed paused rather than started, and with
    /// `dir` its files go there rather than to the output directory.
    async fn add(&mut self, source: Source, paused: bool, dir: Option<PathBuf>) {
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
        if self.refuse_known(info_hash, &source) {
            return;
        }
        span.in_scope(|| info!("added"));
        let source = self.keep_copy(source, info_hash).await;
        if let Some(dir) = dir {
            let dir = match std::path::absolute(&dir) {
                Ok(dir) => dir,
                Err(error) => {
                    self.client
                        .stats()
                        .notice(format!("cannot add torrent in {}: {error}", dir.display()));
                    return;
                }
            };
            self.dirs.insert(info_hash, dir);
        }
        let state = if paused {
            self.show_row(&torrent, &source, TorrentState::Paused);
            State::Paused
        } else {
            self.place(torrent, &source, StartOptions::default(), span)
        };
        self.entries.push(Entry {
            source,
            info_hash,
            state,
        });
        self.idle.send_replace(self.tasks.is_empty());
    }

    /// Whether the session has the torrent with `info_hash` already, in
    /// which case a notice says so, in the terms of its state, and `source`
    /// is not to be started.
    fn refuse_known(&self, info_hash: [u8; 20], source: &Source) -> bool {
        let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.info_hash == info_hash)
        else {
            return false;
        };
        let why = match entry.state {
            State::Running { .. } => "already running",
            State::Restarting { .. } => "already running, restarting",
            State::Paused => "already added, press p to resume",
            State::Ended(_) => "already added, press p to run it again",
            State::Queued { .. } | State::Requeuing { .. } => "already added, waiting in the queue",
            State::Removing { .. } => "still being removed, add it again once it has gone",
        };
        let name = stats_of(&self.client, info_hash, |_| true)
            .map_or_else(|| source.label(), |stats| stats.name());
        self.client.stats().notice(format!("{name}: {why}"));
        true
    }

    /// Drops the row of `entry`, removed from the session, and with
    /// `delete` its files, its resume file and a magnet link's saved
    /// `.torrent`. What cannot be deleted is logged and shown as a notice.
    async fn forget(&mut self, entry: Entry, delete: bool) {
        let gone = |state: &TorrentState| *state == TorrentState::Removing || state.is_finished();
        let stats = stats_of(&self.client, entry.info_hash, gone);
        let name = stats
            .as_ref()
            .map_or_else(|| entry.source.label(), |stats| stats.name());
        if let Some(stats) = &stats {
            self.client.stats().remove_torrent(stats);
        }
        self.client.stats().forget_torrent_limits(entry.info_hash);
        let dir = self.dirs.remove(&entry.info_hash);
        self.tag_dirs.remove(&entry.info_hash);
        self.trackers.remove(&entry.info_hash);
        let span = self.span(&entry.source);
        if !delete {
            self.drop_copy(&entry).await;
            span.in_scope(|| info!("removed"));
            self.client.stats().notice(format!("removed {name}"));
            return;
        }
        let output_dir = dir.unwrap_or_else(|| self.client.config().output_dir.clone());
        let failures = delete_data(&self.client, &entry, &output_dir)
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

    /// Runs `torrent` if the limits leave a slot for one more download, and
    /// otherwise queues it, with a row to show so; `schedule` starts it
    /// later. Returns the state of its entry.
    fn place(
        &mut self,
        torrent: Torrent,
        source: &Source,
        options: StartOptions,
        span: Span,
    ) -> State {
        if self.fits() {
            return self.spawn(torrent, options, span);
        }
        span.in_scope(|| info!("queued"));
        self.show_row(&torrent, source, TorrentState::Queued);
        // The row keeps the saved choice to download in order, so that it
        // shows, is saved again, and a change made while queued holds.
        let mut options = options;
        if let Some(on) = options.sequential.take()
            && let Some(stats) = stats_of(&self.client, torrent.info_hash(), |_| true)
        {
            stats.set_sequential(on);
        }
        State::Queued {
            seeding: false,
            options,
        }
    }

    /// The torrents as the queue sees them, in its order.
    fn slots(&self) -> Vec<Slot> {
        let seeding = |info_hash| {
            stats_of(&self.client, info_hash, |state| {
                *state == TorrentState::Seeding
            })
            .is_some()
        };
        self.entries
            .iter()
            .map(|entry| match &entry.state {
                State::Running { stop } => Slot::Active {
                    seeding: seeding(entry.info_hash),
                    movable: !*stop.borrow(),
                },
                State::Restarting { .. } => Slot::Active {
                    seeding: false,
                    movable: false,
                },
                State::Requeuing { .. } => Slot::Active {
                    seeding: true,
                    movable: false,
                },
                State::Queued { seeding, .. } => Slot::Waiting { seeding: *seeding },
                State::Paused | State::Ended(_) | State::Removing { .. } => Slot::Out,
            })
            .collect()
    }

    /// Whether the limits leave a slot for one more download, after the
    /// torrents already waiting.
    fn fits(&self) -> bool {
        let mut slots = self.slots();
        slots.push(Slot::Waiting { seeding: false });
        let last = slots.len() - 1;
        queue::plan(self.limits, &slots).start.contains(&last)
    }

    /// Whether the session has been told to stop, in which case nothing
    /// more is to start.
    fn stopping(&self) -> bool {
        (self.stop)().now_or_never().is_some()
    }

    /// Carries out the queue: puts the seeds over the limit back in it,
    /// and starts the waiting torrents the limits leave room for, earliest
    /// first. Returns whether it changed anything.
    async fn schedule(&mut self) -> bool {
        if !self.limits.is_limited() {
            return false;
        }
        let plan = queue::plan(self.limits, &self.slots());
        let mut changed = false;
        for &index in &plan.requeue {
            changed |= self.requeue(index);
        }
        if plan.start.is_empty() || self.stopping() {
            return changed;
        }
        for &index in &plan.start {
            let State::Queued { options, .. } =
                std::mem::replace(&mut self.entries[index].state, State::Paused)
            else {
                continue;
            };
            self.start_again(index, options, "starting", "start").await;
            changed = true;
        }
        changed
    }

    /// Stops the running seed of entry `index` to put it back in the queue.
    /// Returns whether it did.
    fn requeue(&mut self, index: usize) -> bool {
        let entry = &mut self.entries[index];
        let State::Running { stop } = &entry.state else {
            return false;
        };
        if stop.send_replace(true) {
            return false;
        }
        entry.state = match std::mem::replace(&mut entry.state, State::Paused) {
            State::Running { stop } => State::Requeuing { stop },
            state => state,
        };
        let info_hash = entry.info_hash;
        let running = |state: &TorrentState| !state.is_finished();
        if let Some(stats) = stats_of(&self.client, info_hash, running) {
            stats.set_state(TorrentState::Pausing);
        }
        true
    }

    /// Swaps the torrent with `info_hash` with the one before it, or after
    /// it unless `earlier`, in the queue and the table. At either end it
    /// stays.
    fn move_in_queue(&mut self, info_hash: [u8; 20], earlier: bool) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        let other = if earlier {
            index.checked_sub(1)
        } else {
            Some(index + 1).filter(|&other| other < self.entries.len())
        };
        if let Some(other) = other {
            self.entries.swap(index, other);
        }
    }

    /// Replaces a torrent’s tracker list and restarts it after sending
    /// stopped to its old trackers. Paused torrents remain paused.
    async fn set_trackers(&mut self, info_hash: [u8; 20], trackers: Vec<String>) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        let trackers = match validate_trackers(trackers) {
            Ok(trackers) => trackers,
            Err(error) => {
                self.client
                    .stats()
                    .notice(format!("cannot edit trackers: {error:#}"));
                return;
            }
        };
        if matches!(
            self.entries[index].state,
            State::Removing { .. } | State::Requeuing { .. } | State::Restarting { .. }
        ) {
            self.client
                .stats()
                .notice("wait for the torrent to stop before editing its trackers");
            return;
        }
        if let State::Running { stop } = &self.entries[index].state
            && *stop.borrow()
        {
            self.client
                .stats()
                .notice("wait for the torrent to stop before editing its trackers");
            return;
        }
        self.trackers.insert(info_hash, trackers.clone());
        if matches!(self.entries[index].state, State::Running { .. }) {
            self.restart(info_hash, StartOptions::default());
        } else if let Some(stats) = stats_of(&self.client, info_hash, |_| true) {
            stats.set_trackers(
                &trackers
                    .into_iter()
                    .map(|url| vec![url])
                    .collect::<Vec<_>>(),
            );
        }
        self.client.stats().notice("tracker list saved");
    }

    /// Moves a stopped torrent’s data, keeping the original directory on
    /// any failure before the destination files have all been prepared.
    async fn move_data(&mut self, info_hash: [u8; 20], dir: PathBuf) {
        self.move_data_to(info_hash, dir, false).await;
    }

    async fn move_data_to(&mut self, info_hash: [u8; 20], dir: PathBuf, from_tag: bool) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.info_hash == info_hash)
        else {
            return;
        };
        if !matches!(self.entries[index].state, State::Paused | State::Ended(_)) {
            self.client
                .stats()
                .notice("pause the torrent and wait for it to stop before moving its data");
            return;
        }
        let source = self.entries[index].source.clone();
        let span = self.span(&source);
        let old_dir = self.dir_of(info_hash).to_path_buf();
        let loaded = load(&source, &span).await;
        let result = async {
            anyhow::ensure!(!dir.as_os_str().is_empty(), "enter a destination directory");
            let loaded = loaded?;
            anyhow::ensure!(
                loaded.info_hash() == info_hash,
                "the torrent file has changed"
            );
            let torrent = match loaded {
                Torrent::File(metainfo) => metainfo.torrent,
                Torrent::Magnet(ref link) => {
                    let path = magnet::saved_torrent(link, &old_dir)
                        .await
                        .ok_or_else(|| anyhow!("no metadata yet"))?;
                    torrent::load_file(&path).await?.0
                }
            };
            let dir = std::path::absolute(&dir)?;
            tokio::fs::create_dir_all(&dir).await?;
            let dir = tokio::fs::canonicalize(dir).await?;
            // A torrent added paused may never have created its old root.
            tokio::fs::create_dir_all(&old_dir).await?;
            let old_dir = tokio::fs::canonicalize(&old_dir).await?;
            if dir == old_dir {
                return Ok::<_, anyhow::Error>((dir, torrent));
            }
            let old = storage::layout(&torrent.info, &old_dir)?;
            let new = storage::layout(&torrent.info, &dir)?;
            let mut keys = Storage::paths_async(&torrent.info, &dir).await?;
            // A different active torrent may have claimed the old files
            // since this one paused. Protect both sides of the move.
            keys.extend(Storage::paths_async(&torrent.info, &old_dir).await?);
            let _claim = self
                .client
                .claim_files(info_hash, &torrent.info.name, &keys)?;
            let mut pairs: Vec<_> = old
                .into_iter()
                .zip(new)
                .map(|(old, new)| (old.path, new.path))
                .collect();
            if let Source::Magnet(link) = &source {
                pairs.push((
                    link.saved_torrent_path(&old_dir),
                    link.saved_torrent_path(&dir),
                ));
            }
            task::spawn_blocking(move || relocate::move_files(&pairs)).await??;
            if let Some(target) = resume::Target::in_dir(self.client.config(), &old_dir, info_hash)
                && let Err(error) = target
                    .relocate(
                        &dir,
                        crate::selection::file_count(&torrent.info),
                        torrent.num_pieces(),
                    )
                    .await
            {
                warn!(%error, "moved data; resume file will be rechecked");
            }
            Ok((dir, torrent))
        }
        .await;
        match result {
            Ok((dir, torrent)) => {
                self.dirs.insert(info_hash, dir.clone());
                if from_tag {
                    self.tag_dirs.insert(info_hash);
                } else {
                    self.tag_dirs.remove(&info_hash);
                }
                let previous = stats_of(&self.client, info_hash, |_| true);
                let state = previous
                    .as_ref()
                    .map_or(TorrentState::Paused, |stats| stats.state());
                // Replace the stopped row so its save path and layout agree.
                let metainfo = Torrent::File(Box::new(Metainfo {
                    torrent,
                    info_hash,
                    info_bytes: Vec::new(),
                }));
                self.show_row(&metainfo, &source, state);
                if let Some(previous) = previous
                    && let Some(current) = stats_of(&self.client, info_hash, |_| true)
                {
                    current.set_verified(previous.verified());
                    if let Some(pieces) = previous.pieces() {
                        current.set_final_pieces(pieces);
                    }
                    if let (Some(priorities), Some(wanted)) =
                        (previous.priorities(), previous.wanted())
                    {
                        current.set_selection(priorities, wanted);
                    }
                }
                self.client
                    .stats()
                    .notice(format!("data moved to {}", dir.display()));
            }
            Err(error) => self
                .client
                .stats()
                .notice(format!("cannot move data: {error:#}")),
        }
    }

    /// Applies `settings` to the torrent with `info_hash`, if the session
    /// has it, and says so.
    fn set_torrent_limits(&self, info_hash: [u8; 20], settings: TorrentSettings) {
        if !self
            .entries
            .iter()
            .any(|entry| entry.info_hash == info_hash)
        {
            return;
        }
        let stats = self.client.stats();
        stats.torrent_limits(info_hash).set(settings);
        let name = stats_of(&self.client, info_hash, |_| true)
            .map_or_else(|| "torrent".to_string(), |torrent| torrent.name());
        stats.notice(format!("{name}: {}", settings.describe()));
    }

    /// Gives the torrent with `info_hash` these tags, and the settings they
    /// give it.
    fn apply_tags(&self, info_hash: [u8; 20], tags: Vec<String>) {
        let stats = self.client.stats();
        let given = tags::tag_settings(&tags, &self.tag_settings);
        stats.torrent_limits(info_hash).set_tag_settings(given);
        stats.set_torrent_tags(info_hash, tags);
    }

    /// Tags the torrent with `info_hash`, if the session has it, and says
    /// so.
    async fn set_tags(&mut self, info_hash: [u8; 20], tags: Vec<String>) {
        if !self
            .entries
            .iter()
            .any(|entry| entry.info_hash == info_hash)
        {
            return;
        }
        let name = stats_of(&self.client, info_hash, |_| true)
            .map_or_else(|| "torrent".to_string(), |torrent| torrent.name());
        let notice = if tags.is_empty() {
            format!("{name}: no tags")
        } else {
            format!("{name}: tags {}", tags::join_tags(&tags))
        };
        let tag_dir = if !self.dirs.contains_key(&info_hash) || self.tag_dirs.contains(&info_hash) {
            tags.iter()
                .find_map(|tag| self.tag_directories.get(tag))
                .cloned()
                .or_else(|| {
                    self.tag_dirs
                        .contains(&info_hash)
                        .then(|| self.client.config().output_dir.clone())
                })
        } else {
            None
        };
        self.apply_tags(info_hash, tags);
        self.client.stats().notice(notice);
        if let Some(dir) = tag_dir {
            let absolute = |dir: &Path| dir.canonicalize().or_else(|_| std::path::absolute(dir));
            let same = matches!((absolute(&dir),absolute(self.dir_of(info_hash))), (Ok(a),Ok(b)) if a == b);
            // Even an equal path must keep its origin, so an explicit M
            // can override future tag edits without moving any bytes.
            if same {
                if let std::collections::hash_map::Entry::Vacant(entry) = self.dirs.entry(info_hash)
                {
                    entry.insert(std::path::absolute(&dir).unwrap_or(dir));
                    self.tag_dirs.insert(info_hash);
                }
                return;
            }
            let stopped = self
                .entries
                .iter()
                .find(|entry| entry.info_hash == info_hash)
                .is_some_and(|entry| matches!(entry.state, State::Paused | State::Ended(_)));
            if !stopped {
                self.client.stats().notice("tags saved; pause the torrent, wait, then reapply tags with e to use their download directory");
                return;
            }
            self.move_data_to(info_hash, dir, true).await;
        }
    }

    async fn command(&mut self, command: Command) {
        match command {
            // They are the limits of the mode that is on.
            Command::SetRateLimits(down, up) => {
                let now = local_now();
                let alt = self.speed.is_alt(now);
                self.speed.set_active(now, (down, up));
                self.client.set_rate_limits(down, up);
                let saved = Some(SavedLimits {
                    download: down,
                    upload: up,
                });
                if alt {
                    self.saved_alt_limits = saved;
                } else {
                    self.saved_limits = saved;
                }
                self.client.stats().notice(format!(
                    "{}rate limits: download {}, upload {}",
                    if alt { "alternative " } else { "" },
                    limit_text(down),
                    limit_text(up)
                ));
            }
            Command::ToggleAltSpeed => {
                self.speed.toggle(local_now());
                self.apply_speed();
            }
            Command::Pause(info_hash) => self.pause(info_hash),
            Command::Resume(info_hash) => self.resume(info_hash).await,
            Command::Recheck(info_hash) => self.recheck(info_hash).await,
            Command::SetPriorities {
                info_hash,
                priorities,
            } => self.set_priorities(info_hash, priorities).await,
            Command::ToggleSequential(info_hash) => self.toggle_sequential(info_hash),
            Command::SetTorrentLimits {
                info_hash,
                settings,
            } => self.set_torrent_limits(info_hash, settings),
            Command::SetTrackers {
                info_hash,
                trackers,
            } => self.set_trackers(info_hash, trackers).await,
            Command::MoveData { info_hash, dir } => self.move_data(info_hash, dir).await,
            Command::SetTags { info_hash, tags } => self.set_tags(info_hash, tags).await,
            Command::Remove { info_hash, delete } => self.remove(info_hash, delete).await,
            Command::Add(source) => self.add(source, false, None).await,
            Command::AddPaused(source) => self.add(source, true, None).await,
            Command::AddIn {
                source,
                paused,
                dir,
            } => self.add(source, paused, Some(dir)).await,
            Command::MoveUp(info_hash) => self.move_in_queue(info_hash, true),
            Command::MoveDown(info_hash) => self.move_in_queue(info_hash, false),
            // The feed stops the torrents itself.
            Command::Stop => {}
        }
        self.schedule().await;
        self.idle.send_replace(self.tasks.is_empty());
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
                State::Ended(Outcome::Interrupted) | State::Queued { .. } => interrupted = true,
                // A removed torrent has no entry, so it counts for nothing.
                State::Ended(Outcome::Done | Outcome::Checked)
                | State::Running { .. }
                | State::Restarting { .. }
                | State::Requeuing { .. }
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
                download::download_metainfo(*metainfo, &options, &client, stop).await
            }
            Torrent::Magnet(link) => magnet::download_magnet(&link, &options, &client, stop).await,
        }
    })
}

/// Gets `source` ready to start, loading a torrent file, with its logs
/// under `span`.
async fn load(source: &Source, span: &Span) -> Result<Torrent> {
    Ok(match source {
        Source::File(path) => Torrent::File(Box::new(
            torrent::load_metainfo(path)
                .instrument(span.clone())
                .await?,
        )),
        Source::Magnet(link) => Torrent::Magnet(link.clone()),
    })
}

/// Deletes the files of the torrent of `entry`, then its resume file and a
/// magnet link's saved `.torrent`, and returns what could not be. The files
/// are those the torrent file lists, or the metadata the magnet link saved;
/// a magnet link whose metadata never arrived has none. Nothing is deleted
/// while a running torrent writes any of the files.
async fn delete_data(client: &Arc<Client>, entry: &Entry, output_dir: &Path) -> Vec<anyhow::Error> {
    let output_dir = output_dir.to_path_buf();
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
    if let Some(target) = resume::Target::in_dir(client.config(), &output_dir, entry.info_hash) {
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
