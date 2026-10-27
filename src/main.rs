mod cli;
mod log_line;
mod shutdown;

use anyhow::bail;
use clap::Parser;
use cli::{Args, Source};
use indicatif::MultiProgress;
use log_line::LogLine;
use rtrnt::client::Client;
use rtrnt::download::{self, DownloadConfig, Interrupted};
use rtrnt::magnet::{self, FetchInterrupted};
use rtrnt::{config, torrent, tracker};
use shutdown::Shutdown;
use std::process::ExitCode;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{Instrument, Span, error, info, info_span, warn};
use tracing_subscriber::EnvFilter;

/// Exit code for a run stopped by Ctrl-C (128 + SIGINT).
const EXIT_INTERRUPTED: u8 = 130;

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let bars = MultiProgress::new();
    let log_bars = bars.clone();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(move || LogLine::new(log_bars.clone()))
        .init();

    let args = Args::parse();
    let file = config::load(args.config.as_deref())?;
    let cli = config::Options {
        dht: args.dht(),
        output: args.output,
        max_peers: args.max_peers,
        max_connections: args.max_connections,
        port: args.port,
        seed_ratio: args.seed_ratio,
        seed_time: args.seed_time,
        max_download_rate: args.max_download_rate,
        max_upload_rate: args.max_upload_rate,
    };
    if args.print_config {
        print!("{}", config::describe(&cli, &file));
        return Ok(ExitCode::SUCCESS);
    }
    if let Some(path) = &file.path {
        info!(path = %path.display(), "loaded config file");
    }
    let mut config = config::resolve(cli, file.options);
    config.recheck = args.recheck;
    // clap requires a torrent unless --print-config is given.
    if args.torrents.is_empty() {
        bail!("no torrent given");
    }

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
    download_all(args.torrents, Client::new(config, bars)).await
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

/// How the download of one torrent ended.
enum Outcome {
    Done,
    Interrupted,
    Failed(anyhow::Error),
}

/// Downloads (and seeds) every torrent in `torrents` at once through
/// `client`, until each is done. One that fails does not stop the others,
/// and Ctrl-C stops them all. With several, each one's logs carry its name.
///
/// A single torrent's failure is returned as the error; with several, each
/// failure is logged and the exit code is a failure. Otherwise the exit code
/// says whether Ctrl-C cut a download short.
async fn download_all(torrents: Vec<Source>, client: Client) -> anyhow::Result<ExitCode> {
    let client = Arc::new(client);
    let shutdown = Shutdown::new();
    let several = torrents.len() > 1;
    let mut tasks = JoinSet::new();
    for source in torrents {
        let span = if several {
            info_span!("torrent", name = %source.label())
        } else {
            Span::none()
        };
        let client = Arc::clone(&client);
        let stop = Arc::clone(&shutdown).signal();
        let download = async move {
            let result = match &source {
                Source::File(path) => download::download_file(path, &client, stop).await,
                Source::Magnet(link) => magnet::download_magnet(link, &client, stop).await,
            };
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
        };
        tasks.spawn(download.instrument(span));
    }

    let total = tasks.len();
    let mut failed = Vec::new();
    let mut interrupted = false;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Outcome::Done) => {}
            Ok(Outcome::Interrupted) => interrupted = true,
            Ok(Outcome::Failed(e)) => failed.push(e),
            Err(e) => failed.push(e.into()),
        }
    }
    if !failed.is_empty() {
        if !several && let Some(e) = failed.pop() {
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
