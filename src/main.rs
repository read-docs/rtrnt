use clap::Parser;
use rtrnt::download::{self, DownloadConfig};
use std::path::PathBuf;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args = Args::parse();
    if args.check {
        return download::check_file(&args.torrent, &args.output).await;
    }
    let config = DownloadConfig {
        output_dir: args.output,
        max_peers: args.max_peers,
    };
    download::download_file(&args.torrent, &config).await
}
