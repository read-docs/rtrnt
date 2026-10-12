use clap::Parser;
use rtrnt::download::{self, DownloadConfig, Interrupted, SeedLimits};
use rtrnt::peer;
use std::path::PathBuf;
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
    torrent: PathBuf,
    /// Directory to save the downloaded files in
    #[arg(short, long, default_value = ".")]
    output: PathBuf,
    /// Maximum number of peers to download from at once
    #[arg(long, default_value_t = 30)]
    max_peers: usize,
    /// Port to listen on for incoming peers (0 picks a free one; so does a
    /// port that is taken)
    #[arg(short, long, default_value_t = peer::DEFAULT_LISTEN_PORT)]
    port: u16,
    /// Verify an existing download against the torrent instead of
    /// downloading; exits with an error unless every piece is intact
    #[arg(long)]
    check: bool,
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size (0: no ratio limit)
    #[arg(long, default_value_t = 0.0, value_name = "RATIO", value_parser = parse_ratio)]
    seed_ratio: f64,
    /// Once the download is complete, keep seeding for this many minutes
    /// (0: no time limit). Seeding stops at whichever limit comes first, or
    /// on Ctrl-C; with neither, the run ends at completion
    #[arg(long, default_value_t = 0, value_name = "MINUTES")]
    seed_time: u64,
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
    if args.check {
        download::check_file(&args.torrent, &args.output).await?;
        return Ok(ExitCode::SUCCESS);
    }
    let config = DownloadConfig {
        output_dir: args.output,
        max_peers: args.max_peers,
        listen_port: args.port,
        seed: SeedLimits {
            ratio: args.seed_ratio,
            time: Duration::from_secs(args.seed_time.saturating_mul(60)),
        },
    };
    match download::download_file(&args.torrent, &config, ctrl_c()).await {
        Ok(()) => Ok(ExitCode::SUCCESS),
        Err(e) if e.is::<Interrupted>() => {
            warn!("{e}; run again to resume");
            Ok(ExitCode::from(EXIT_INTERRUPTED))
        }
        Err(e) => Err(e),
    }
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
