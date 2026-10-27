use anyhow::bail;
use clap::Parser;
use indicatif::MultiProgress;
use rtrnt::client::Client;
use rtrnt::download::{self, DownloadConfig, Interrupted};
use rtrnt::magnet::{self, FetchInterrupted, MagnetLink};
use rtrnt::{config, torrent, tracker};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Once};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{Instrument, Span, error, info, info_span, warn};
use tracing_subscriber::EnvFilter;

/// Download torrents from the terminal.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// Path to a .torrent file, or a magnet link (magnet:?xt=urn:btih:...);
    /// several are downloaded at once
    #[arg(
        required_unless_present = "print_config",
        value_parser = parse_source,
        value_name = "TORRENT"
    )]
    torrents: Vec<Source>,
    // Settings that the config file can also set have no clap default, so
    // an unset flag falls back to the file. The defaults live in
    // `rtrnt::config` and are spelled out in the help (checked by a test).
    /// Directory to save the downloaded files in [default: .]
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Maximum number of peers to download from at once, for each torrent
    /// [default: 30]
    #[arg(long)]
    max_peers: Option<usize>,
    /// Maximum number of peer connections open at once, across all
    /// torrents [default: 200]
    #[arg(long)]
    max_connections: Option<usize>,
    /// Port to listen on for incoming peers (0 picks a free one; so does a
    /// port that is taken) [default: 6881]
    #[arg(short, long)]
    port: Option<u16>,
    /// Verify existing downloads against their torrents instead of
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
    /// Ignore the resume files and hash the data already on disk, as for a
    /// torrent that has none
    #[arg(long)]
    recheck: bool,
    /// Ask every tracker of each torrent how many seeders and leechers it
    /// knows of instead of downloading; exits with an error unless one
    /// answers for each torrent
    #[arg(long, conflicts_with = "check")]
    scrape: bool,
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size (0: no ratio limit) [default: 0]
    #[arg(long, value_name = "RATIO", value_parser = parse_ratio)]
    seed_ratio: Option<f64>,
    /// Once the download is complete, keep seeding for this many minutes
    /// (0: no time limit). Seeding stops at whichever limit comes first, or
    /// on Ctrl-C; with neither, the run ends at completion [default: 0]
    #[arg(long, value_name = "MINUTES")]
    seed_time: Option<u64>,
    /// Do not look for peers in the DHT, only ask the trackers (`dht =
    /// false` in the config file)
    #[arg(long, overrides_with = "dht")]
    no_dht: bool,
    /// Look for peers in the DHT, as by default, even if the config file
    /// says `dht = false`
    #[arg(long, overrides_with = "no_dht")]
    dht: bool,
    /// Most piece data to download per second in KiB, across all peers of
    /// all torrents (0: unlimited) [default: 0]
    #[arg(long, value_name = "KIB/S")]
    max_download_rate: Option<u64>,
    /// Most piece data to upload per second in KiB, across all peers of all
    /// torrents (0: unlimited) [default: 0]
    #[arg(long, value_name = "KIB/S")]
    max_upload_rate: Option<u64>,
}

impl Args {
    /// Whether the DHT was turned on or off on the command line: by the last
    /// of `--dht` and `--no-dht`, if either is given.
    fn dht(&self) -> Option<bool> {
        match (self.dht, self.no_dht) {
            (_, true) => Some(false),
            (true, false) => Some(true),
            (false, false) => None,
        }
    }
}

/// What to download.
#[derive(Clone)]
enum Source {
    File(PathBuf),
    Magnet(Box<MagnetLink>),
}

impl Source {
    /// A short name for the torrent, for logs and headings: the file's
    /// name, or the magnet link's `dn` or else its info hash.
    fn label(&self) -> String {
        match self {
            Source::File(path) => path.file_name().map_or_else(
                || path.display().to_string(),
                |name| name.to_string_lossy().into_owned(),
            ),
            Source::Magnet(link) => link
                .name
                .clone()
                .unwrap_or_else(|| link.info_hash.iter().map(|b| format!("{b:02x}")).collect()),
        }
    }
}

/// Parses a torrent argument: a magnet link if it starts with `magnet:`,
/// otherwise the path of a .torrent file.
fn parse_source(arg: &str) -> Result<Source, String> {
    if magnet::is_magnet(arg) {
        MagnetLink::parse(arg)
            .map(|link| Source::Magnet(Box::new(link)))
            .map_err(|e| format!("{e:#}"))
    } else {
        Ok(Source::File(arg.into()))
    }
}

/// Parses `--seed-ratio`, which must be a finite number, 0 or more.
fn parse_ratio(arg: &str) -> Result<f64, String> {
    match arg.parse::<f64>() {
        Ok(ratio) if config::is_valid_ratio(ratio) => Ok(ratio),
        _ => Err(format!("{arg:?} is not a ratio of 0 or more")),
    }
}

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

/// Ctrl-C, for every torrent of the session.
struct Shutdown {
    /// Set by the first Ctrl-C.
    stop: watch::Sender<bool>,
    handler: Once,
}

impl Shutdown {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            stop: watch::Sender::new(false),
            handler: Once::new(),
        })
    }

    /// Resolves on the first Ctrl-C, or at once if it has come already. A
    /// second one, or one that comes while no torrent waits for this, exits
    /// at once.
    ///
    /// Until a torrent first polls this, Ctrl-C keeps its default behavior
    /// of killing the process.
    async fn signal(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let handler = Arc::clone(&self);
        self.handler.call_once(move || {
            tokio::spawn(handler.handle_ctrl_c());
        });
        // Fails only once the sender is gone, but `self` holds it.
        let _ = stop.wait_for(|&stop| stop).await;
    }

    async fn handle_ctrl_c(self: Arc<Self>) {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!("cannot listen for Ctrl-C: {e}");
            return;
        }
        if self.stop.receiver_count() == 0 {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
        self.stop.send_replace(true);
        info!("stopping, press Ctrl-C again to quit at once");
        if tokio::signal::ctrl_c().await.is_ok() {
            std::process::exit(EXIT_INTERRUPTED.into());
        }
    }
}

/// One log event, written to stderr once complete, with the progress bars
/// hidden meanwhile and drawn again below it.
struct LogLine {
    bars: MultiProgress,
    line: Vec<u8>,
}

impl LogLine {
    fn new(bars: MultiProgress) -> Self {
        Self {
            bars,
            line: Vec::new(),
        }
    }
}

impl Write for LogLine {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.line.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for LogLine {
    fn drop(&mut self) {
        if !self.line.is_empty() {
            // Nowhere to report a failure to write a log line.
            let _ = self
                .bars
                .suspend(|| std::io::stderr().write_all(&self.line));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    use rtrnt::peer;
    use std::path::Path;

    #[test]
    fn cli_is_valid() {
        Args::command().debug_assert();
    }

    #[test]
    fn takes_a_torrent_file_or_a_magnet_link() {
        let hash = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
        let args = Args::try_parse_from(["rtrnt", "file.torrent"]).unwrap();
        assert!(
            matches!(&args.torrents[..], [Source::File(path)] if path == Path::new("file.torrent"))
        );
        let link = format!("magnet:?xt=urn:btih:{hash}&tr=udp%3A%2F%2Ft%3A1");
        let args = Args::try_parse_from(["rtrnt", &link, "--scrape"]).unwrap();
        let [Source::Magnet(link)] = &args.torrents[..] else {
            panic!("not parsed as a magnet link");
        };
        assert_eq!(link.trackers, ["udp://t:1"]);
        assert_eq!(args.torrents[0].label(), hash);
        let err = Args::try_parse_from(["rtrnt", "magnet:?dn=x"])
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("no info hash"), "{err}");
    }

    #[test]
    fn takes_several_torrents() {
        let hash = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
        let link = format!("magnet:?xt=urn:btih:{hash}&dn=Some+Name");
        let args = Args::try_parse_from(["rtrnt", "dir/a.torrent", &link, "b.torrent", "-o", "x"])
            .unwrap();
        let labels: Vec<_> = args.torrents.iter().map(Source::label).collect();
        assert_eq!(labels, ["a.torrent", "Some Name", "b.torrent"]);
        assert!(Args::try_parse_from(["rtrnt"]).is_err());
        let args = Args::try_parse_from(["rtrnt", "--print-config"]).unwrap();
        assert!(args.torrents.is_empty());
    }

    /// The defaults are written into the help by hand, since clap does not
    /// know them; this keeps the help in step with `rtrnt::config`.
    #[test]
    fn help_shows_the_defaults() {
        let command = Args::command();
        let defaults = [
            ("output", config::DEFAULT_OUTPUT_DIR.to_string()),
            ("max_peers", config::DEFAULT_MAX_PEERS.to_string()),
            (
                "max_connections",
                config::DEFAULT_MAX_CONNECTIONS.to_string(),
            ),
            ("port", peer::DEFAULT_LISTEN_PORT.to_string()),
            ("seed_ratio", config::DEFAULT_SEED_RATIO.to_string()),
            ("seed_time", config::DEFAULT_SEED_TIME.to_string()),
            (
                "max_download_rate",
                config::DEFAULT_MAX_DOWNLOAD_RATE.to_string(),
            ),
            (
                "max_upload_rate",
                config::DEFAULT_MAX_UPLOAD_RATE.to_string(),
            ),
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
        // `--dht` says the DHT is on by default.
        const { assert!(config::DEFAULT_DHT) };
    }

    /// Without `--dht` or `--no-dht` the config file decides; with both,
    /// the last one wins.
    #[test]
    fn dht_flags_fall_back_to_the_file() {
        let dht = |flags: &[&str]| {
            let args = ["rtrnt", "x.torrent"].iter().chain(flags);
            Args::try_parse_from(args).unwrap().dht()
        };
        assert_eq!(dht(&[]), None);
        assert_eq!(dht(&["--no-dht"]), Some(false));
        assert_eq!(dht(&["--dht"]), Some(true));
        assert_eq!(dht(&["--dht", "--no-dht"]), Some(false));
        assert_eq!(dht(&["--no-dht", "--dht"]), Some(true));
    }

    #[test]
    fn recheck_is_off_unless_asked_for() {
        assert!(
            !Args::try_parse_from(["rtrnt", "x.torrent"])
                .unwrap()
                .recheck
        );
        assert!(
            Args::try_parse_from(["rtrnt", "x.torrent", "--recheck"])
                .unwrap()
                .recheck
        );
    }

    #[test]
    fn seed_and_rate_flags_are_optional() {
        let args = Args::try_parse_from(["rtrnt", "x.torrent"]).unwrap();
        assert_eq!(args.seed_ratio, None);
        assert_eq!(args.seed_time, None);
        assert_eq!(args.max_download_rate, None);
        assert_eq!(args.max_upload_rate, None);
        let args = Args::try_parse_from([
            "rtrnt",
            "x.torrent",
            "--seed-ratio",
            "0",
            "--max-upload-rate",
            "0",
        ])
        .unwrap();
        // An explicit 0 is a setting, which beats the config file.
        assert_eq!(args.seed_ratio, Some(0.0));
        assert_eq!(args.max_upload_rate, Some(0));
        assert!(Args::try_parse_from(["rtrnt", "x.torrent", "--seed-ratio", "-1"]).is_err());
    }
}
