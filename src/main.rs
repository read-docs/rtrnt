use clap::Parser;
use rtrnt::download::{self, DownloadConfig, Interrupted};
use std::path::PathBuf;
use std::process::ExitCode;
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
    /// Verify an existing download against the torrent instead of
    /// downloading; exits with an error unless every piece is intact
    #[arg(long)]
    check: bool,
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
