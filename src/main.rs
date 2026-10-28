mod cli;
mod log_line;
mod shutdown;

use anyhow::bail;
use clap::Parser;
use cli::Args;
use indicatif::{MultiProgress, ProgressDrawTarget};
use log_line::LogLine;
use rtrnt::client::Client;
use rtrnt::download::{self, DownloadConfig};
use rtrnt::magnet;
use rtrnt::manager::{Command, EXIT_INTERRUPTED, Manager, Recheck, StartOptions};
use rtrnt::session::{self, SavedSession, SessionStore};
use rtrnt::source::Source;
use rtrnt::tui::log_buffer::LogBuffer;
use rtrnt::tui::{self, SessionFeed};
use rtrnt::watch::Watcher;
use rtrnt::{config, torrent, tracker};
use shutdown::Shutdown;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// How far back the warnings and errors printed after the UI closes go.
const PROBLEMS_SHOWN_AFTER_UI: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let args = Args::parse();
    // Relative paths given to the UI's `a` are under it. Should it be gone,
    // they stay relative, and fail to load.
    let start_dir = std::env::current_dir().unwrap_or_default();
    // Read before logging starts, which depends on it; any error is printed
    // on the way out.
    let file = config::load(args.config.as_deref());
    let terminals = || std::io::stdout().is_terminal() && std::io::stderr().is_terminal();
    let file_tui = file.as_ref().ok().and_then(|file| file.options.tui);
    let file_watches = file
        .as_ref()
        .is_ok_and(|file| file.options.watch_dir.is_some());
    let use_tui = args.use_tui(file_tui, terminals);
    let watching = args.watch_dir.is_some() || file_watches;
    if let Err(e) = args.check_torrents(use_tui, watching) {
        e.exit();
    }

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let logs = LogBuffer::default();
    let bars = if use_tui {
        tracing_subscriber::registry()
            .with(filter)
            .with(logs.layer())
            .init();
        // The bars keep counting, but nothing draws them.
        MultiProgress::with_draw_target(ProgressDrawTarget::hidden())
    } else {
        let bars = MultiProgress::new();
        let log_bars = bars.clone();
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(move || LogLine::new(log_bars.clone()))
            .init();
        bars
    };

    let file = file?;
    let cli_session = args.session();
    let cli_watch_delete = args.watch_dir_delete();
    // The session is kept by the terminal UI unless the settings say
    // otherwise; --print-config says what a run in the UI would load.
    let ui_would_run = if args.print_config {
        args.tui().or(file_tui).unwrap_or_else(terminals)
    } else {
        use_tui
    };
    let use_session = args.use_session(file.options.session, ui_would_run);
    let cli = config::Options {
        dht: args.dht(),
        tui: args.tui(),
        output: args.output,
        max_peers: args.max_peers,
        max_connections: args.max_connections,
        port: args.port,
        seed_ratio: args.seed_ratio,
        seed_time: args.seed_time,
        max_download_rate: args.max_download_rate,
        max_upload_rate: args.max_upload_rate,
        session: cli_session,
        watch_dir: args.watch_dir,
        watch_dir_delete: cli_watch_delete,
    };
    let store = if use_session {
        let store = SessionStore::in_state_dir();
        if store.is_none() {
            warn!("no state directory (set $XDG_STATE_HOME or $HOME), so the session is not kept");
        }
        store
    } else {
        None
    };
    let saved = match &store {
        Some(store) => store.load().unwrap_or_else(|e| {
            warn!("starting without the saved session: {e:#}");
            if !args.print_config {
                store.set_aside();
            }
            SavedSession::default()
        }),
        None => SavedSession::default(),
    };
    // Between the command line and the config file.
    let from_session = config::Options {
        max_download_rate: saved.rate_limits.map(|limits| limits.download),
        max_upload_rate: saved.rate_limits.map(|limits| limits.upload),
        ..config::Options::default()
    };
    if args.print_config {
        print!("{}", config::describe(&cli, &from_session, &file));
        return Ok(ExitCode::SUCCESS);
    }
    if let Some(path) = &file.path {
        info!(path = %path.display(), "loaded config file");
    }
    let watch_dir = cli.watch_dir.clone().or(file.options.watch_dir.clone());
    let watch_delete = cli
        .watch_dir_delete
        .or(file.options.watch_dir_delete)
        .unwrap_or(config::DEFAULT_WATCH_DIR_DELETE);
    let config = config::resolve(cli.or(from_session), file.options);
    if args.check || args.scrape {
        let (config, check_mode) = (&config, args.check);
        return each_in_turn(&args.torrents, |source| async move {
            if check_mode {
                check(source, config).await
            } else {
                scrape_source(source).await
            }
        })
        .await;
    }
    let client = Arc::new(Client::new(config, bars));
    let shutdown = Shutdown::new();
    // Every torrent runs at once until it is done, and Ctrl-C stops them all.
    let mut manager = Manager::new(Arc::clone(&client), {
        let shutdown = Arc::clone(&shutdown);
        move || Arc::clone(&shutdown).signal()
    });
    let mut sources = args.torrents;
    // The watch directory keeps its copies of .torrent files in the
    // session's store, or else in the state directory even though the
    // session is not kept, so the renamed original is not needed.
    let watch_store = store.clone().or_else(SessionStore::in_state_dir);
    if let Some(store) = store {
        manager = manager.with_session(store);
        // The session's torrents first, then the command line's.
        let mut cli = Vec::new();
        for source in sources {
            let info_hash = session::info_hash_of(&source).await;
            cli.push((source, info_hash));
        }
        let (torrents, rest) = session::merge(saved.torrents, cli);
        sources = rest;
        manager
            .restore(SavedSession {
                torrents,
                rate_limits: saved.rate_limits,
            })
            .await;
    }
    if use_tui || watch_dir.is_some() {
        manager.expect_more();
    }
    let options = StartOptions {
        recheck: if args.recheck {
            Recheck::ThenRun
        } else {
            Recheck::No
        },
        picks: args.select_file,
        ..StartOptions::default()
    };
    manager.start(sources, options).await;
    let (commands, commands_rx) = mpsc::unbounded_channel();
    if let Some(dir) = watch_dir {
        info!(dir = %dir.display(), "watching for torrents");
        client.stats().set_watch_dir(dir.clone());
        let watcher = Watcher::new(
            dir,
            watch_delete,
            watch_store,
            commands.clone(),
            Arc::clone(client.stats()),
        );
        // It lets go of its sender when Ctrl-C comes, so the run can end.
        tokio::spawn(watcher.run(Arc::clone(&shutdown).signal()));
    }
    if !use_tui {
        // Only the watcher, if there is one, sends commands: without it the
        // run ends with the last torrent.
        drop(commands);
        return manager.run(commands_rx).await;
    }
    let finished = manager.idle();
    let run = manager.run(commands_rx);
    with_tui(run, finished, commands, client, shutdown, logs, start_dir).await
}

/// Shows `run`, the manager's run, in the terminal UI, which sends it
/// `commands` until the user leaves; the run goes on until then. Leaving the
/// UI before every torrent has stopped exits at once, as a second Ctrl-C
/// does. Torrents added by path are under `start_dir`.
async fn with_tui(
    run: impl Future<Output = anyhow::Result<ExitCode>> + Send + 'static,
    finished: watch::Receiver<bool>,
    commands: mpsc::UnboundedSender<Command>,
    client: Arc<Client>,
    shutdown: Arc<Shutdown>,
    logs: LogBuffer,
    start_dir: PathBuf,
) -> anyhow::Result<ExitCode> {
    let downloads = tokio::spawn(run);
    let stop = move || shutdown.request_stop();
    let feed = SessionFeed::new(Arc::clone(client.stats()), logs.clone(), stop, commands);
    let exit = tui::run(feed, finished, start_dir).await;
    // The terminal is back to normal: say what went wrong lately, which the
    // UI showed but which is gone with it.
    for line in logs.problems_since(SystemTime::now(), PROBLEMS_SHOWN_AFTER_UI) {
        eprintln!("{line}");
    }
    match exit {
        Ok(tui::Exit::Quit) => {}
        Ok(tui::Exit::Now) => std::process::exit(EXIT_INTERRUPTED.into()),
        Err(e) => {
            eprintln!("cannot run the terminal UI: {e}; carrying on without it");
            logs.echo_to_stderr();
        }
    }
    downloads.await?
}

/// Runs `run` on each torrent in turn. With several, each one's output is
/// headed by its name, and one that fails is logged and the rest still run;
/// the exit code is then a failure if any failed.
async fn each_in_turn<'a, F, Fut>(torrents: &'a [Source], run: F) -> anyhow::Result<ExitCode>
where
    F: Fn(&'a Source) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    if let [torrent] = torrents {
        run(torrent).await?;
        return Ok(ExitCode::SUCCESS);
    }
    let mut failed = 0;
    for (i, torrent) in torrents.iter().enumerate() {
        if i > 0 {
            println!();
        }
        println!("{}:", torrent.label());
        if let Err(e) = run(torrent).await {
            error!(torrent = %torrent.label(), "{e:#}");
            failed += 1;
        }
    }
    if failed > 0 {
        error!("{failed} of {} torrents failed", torrents.len());
        return Ok(ExitCode::FAILURE);
    }
    Ok(ExitCode::SUCCESS)
}

/// Verifies the download of `source` in the output directory.
async fn check(source: &Source, config: &DownloadConfig) -> anyhow::Result<()> {
    let path = match source {
        Source::File(path) => path.clone(),
        // The metadata saved when the magnet link was downloaded.
        Source::Magnet(link) => match magnet::saved_torrent(link, &config.output_dir).await {
            Some(path) => path,
            None => bail!(
                "--check needs the torrent's metadata, and {} was not saved by a download \
                 of this magnet link; download it first, or pass the .torrent file",
                link.saved_torrent_path(&config.output_dir).display()
            ),
        },
    };
    download::check_file(&path, &config.output_dir).await
}

/// Scrapes the trackers of `source` (see `scrape`).
async fn scrape_source(source: &Source) -> anyhow::Result<()> {
    let (tiers, info_hash) = match source {
        Source::File(path) => {
            let (torrent, info_hash) = torrent::load_file(path).await?;
            (torrent.tracker_tiers(), info_hash)
        }
        Source::Magnet(link) => (link.tracker_tiers(), link.info_hash),
    };
    scrape(&tiers, info_hash).await
}

/// Scrapes every tracker in `tiers` (a torrent's, or a magnet link's `tr`
/// list) for `info_hash` and prints what each one knows of the swarm, tier
/// by tier. Trackers in different tiers often keep separate swarms, so each
/// one is asked rather than only the first to answer; they are asked all at
/// once, so a dead one costs its timeout only once. Fails if no tracker
/// answered.
async fn scrape(tiers: &[Vec<String>], info_hash: [u8; 20]) -> anyhow::Result<()> {
    if tiers.is_empty() {
        bail!("torrent has no trackers");
    }
    let scrapes = tracker::scrape_tiers(tiers, info_hash).await;
    for scrape in &scrapes {
        println!("{scrape}");
    }
    if !scrapes.iter().any(|scrape| scrape.result.is_ok()) {
        bail!("no tracker answered the scrape");
    }
    Ok(())
}
