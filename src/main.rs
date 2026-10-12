use anyhow::{Context, bail};
use clap::Parser;
use rtrnt::download::{self, Interrupted, SeedLimits};
use rtrnt::{config, torrent, tracker};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Download a torrent from the terminal.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to the .torrent file
    #[arg(required_unless_present = "print_config")]
    torrent: Option<PathBuf>,
    // Settings that the config file can also set have no clap default, so
    // an unset flag falls back to the file. The defaults live in
    // `rtrnt::config` and are spelled out in the help (checked by a test).
    /// Directory to save the downloaded files in [default: .]
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Maximum number of peers to download from at once [default: 30]
    #[arg(long)]
    max_peers: Option<usize>,
    /// Port to listen on for incoming peers (0 picks a free one; so does a
    /// port that is taken) [default: 6881]
    #[arg(short, long)]
    port: Option<u16>,
    /// Verify an existing download against the torrent instead of
    /// downloading; exits with an error unless every piece is intact
    #[arg(long)]
    check: bool,
    /// Config file to read instead of $XDG_CONFIG_HOME/rtrnt/config.toml
    /// (or ~/.config/rtrnt/config.toml); command-line flags override it
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Print the effective settings and where each one came from, then exit
    #[arg(long)]
    print_config: bool,
    /// Ask every tracker of the torrent how many seeders and leechers it
    /// knows of instead of downloading; exits with an error unless one
    /// answers
    #[arg(long, conflicts_with = "check")]
    scrape: bool,
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size (0: no ratio limit)
    #[arg(long, default_value_t = 0.0, value_name = "RATIO", value_parser = parse_ratio)]
    seed_ratio: f64,
    /// Once the download is complete, keep seeding for this many minutes
    /// (0: no time limit). Seeding stops at whichever limit comes first, or
    /// on Ctrl-C; with neither, the run ends at completion
    #[arg(long, default_value_t = 0, value_name = "MINUTES")]
    seed_time: u64,
    /// Do not look for peers in the DHT, only ask the trackers
    #[arg(long)]
    no_dht: bool,
}

/// Parses `--seed-ratio`, which must be a finite number, 0 or more.
fn parse_ratio(arg: &str) -> Result<f64, String> {
    match arg.parse::<f64>() {
        Ok(ratio) if ratio.is_finite() && ratio >= 0.0 => Ok(ratio),
        _ => Err(format!("{arg:?} is not a ratio of 0 or more")),
    }
}

/// Exit code for a run stopped by Ctrl-C (128 + SIGINT).
const EXIT_INTERRUPTED: u8 = 130;

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    let file = config::load(args.config.as_deref())?;
    let cli = config::Options {
        output: args.output,
        max_peers: args.max_peers,
        port: args.port,
    };
    if args.print_config {
        print!("{}", config::describe(&cli, &file));
        return Ok(ExitCode::SUCCESS);
    }
    if let Some(path) = &file.path {
        info!(path = %path.display(), "loaded config file");
    }
    let mut config = config::resolve(cli, file.options);
    config.seed = SeedLimits {
        ratio: args.seed_ratio,
        time: Duration::from_secs(args.seed_time.saturating_mul(60)),
    };
    config.dht = !args.no_dht;
    // clap requires the torrent unless --print-config is given.
    let torrent = args.torrent.context("no torrent given")?;

    if args.check {
        download::check_file(&torrent, &config.output_dir).await?;
        return Ok(ExitCode::SUCCESS);
    }
    if args.scrape {
        scrape(&torrent).await?;
        return Ok(ExitCode::SUCCESS);
    }
    match download::download_file(&torrent, &config, ctrl_c()).await {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(e) if e.is::<Interrupted>() => {
            warn!("{e}; run again to resume");
            Ok(ExitCode::from(EXIT_INTERRUPTED))
        }
        Err(e) => Err(e),
    }
}

/// Scrapes every tracker of the torrent at `path` and prints what each one
/// knows of the swarm, tier by tier. Trackers in different tiers often keep
/// separate swarms, so each one is asked rather than only the first to
/// answer; they are asked all at once, so a dead one costs its timeout only
/// once. Fails if no tracker answered.
async fn scrape(path: &Path) -> anyhow::Result<()> {
    let (torrent, info_hash) = torrent::load_file(path).await?;
    let tiers = torrent.tracker_tiers();
    if tiers.is_empty() {
        bail!("torrent has no trackers");
    }
    let scrapes = tracker::scrape_tiers(&tiers, info_hash).await;
    for scrape in &scrapes {
        println!("{scrape}");
    }
    if !scrapes.iter().any(|scrape| scrape.result.is_ok()) {
        bail!("no tracker answered the scrape");
    }
    Ok(())
}

/// Resolves on the first Ctrl-C. A second one, or one that comes once
/// nobody waits for this any more, exits at once.
///
/// Until this is first polled, Ctrl-C keeps its default behavior of
/// killing the process.
async fn ctrl_c() {
    let (first, first_rx) = oneshot::channel();
    tokio::spawn(async move {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("cannot listen for Ctrl-C: {e}");
            return;
        }
        if first.send(()).is_err() {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
        if tokio::signal::ctrl_c().await.is_ok() {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
    });
    if first_rx.await.is_err() {
        // Not listening for Ctrl-C, so it never comes.
        std::future::pending().await
    }
    info!("stopping, press Ctrl-C again to quit at once");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use rtrnt::peer;

    #[test]
    fn cli_is_valid() {
        Args::command().debug_assert();
    }

    /// The defaults are written into the help by hand, since clap does not
    /// know them; this keeps the help in step with `rtrnt::config`.
    #[test]
    fn help_shows_the_defaults() {
        let command = Args::command();
        let defaults = [
            ("output", config::DEFAULT_OUTPUT_DIR.to_string()),
            ("max_peers", config::DEFAULT_MAX_PEERS.to_string()),
            ("port", peer::DEFAULT_LISTEN_PORT.to_string()),
        ];
        for (id, default) in defaults {
            let help = command
                .get_arguments()
                .find(|arg| arg.get_id() == id)
                .and_then(|arg| arg.get_help())
                .map(ToString::to_string)
                .unwrap_or_default();
            assert!(
                help.ends_with(&format!("[default: {default}]")),
                "{id}: {help}"
            );
        }
    }
}
