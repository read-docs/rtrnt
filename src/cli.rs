use clap::Parser;
use rtrnt::config;
use rtrnt::magnet::{self, MagnetLink};
use std::path::PathBuf;

/// Download torrents from the terminal.
#[derive(Parser)]
#[command(version, about)]
pub(crate) struct Args {
    /// Path to a .torrent file, or a magnet link (magnet:?xt=urn:btih:...);
    /// several are downloaded at once
    #[arg(
        required_unless_present = "print_config",
        value_parser = parse_source,
        value_name = "TORRENT"
    )]
    pub(crate) torrents: Vec<Source>,
    // Settings that the config file can also set have no clap default, so
    // an unset flag falls back to the file. The defaults live in
    // `rtrnt::config` and are spelled out in the help (checked by a test).
    /// Directory to save the downloaded files in [default: .]
    #[arg(short, long)]
    pub(crate) output: Option<PathBuf>,
    /// Maximum number of peers to download from at once, for each torrent
    /// [default: 30]
    #[arg(long)]
    pub(crate) max_peers: Option<usize>,
    /// Maximum number of peer connections open at once, across all
    /// torrents [default: 200]
    #[arg(long)]
    pub(crate) max_connections: Option<usize>,
    /// Port to listen on for incoming peers (0 picks a free one; so does a
    /// port that is taken) [default: 6881]
    #[arg(short, long)]
    pub(crate) port: Option<u16>,
    /// Verify existing downloads against their torrents instead of
    /// downloading; exits with an error unless every piece is intact
    #[arg(long)]
    pub(crate) check: bool,
    /// Config file to read instead of $XDG_CONFIG_HOME/rtrnt/config.toml
    /// (or ~/.config/rtrnt/config.toml); command-line flags override it
    #[arg(long, value_name = "PATH")]
    pub(crate) config: Option<PathBuf>,
    /// Print the effective settings and where each one came from, then exit
    #[arg(long)]
    pub(crate) print_config: bool,
    /// Ignore the resume files and hash the data already on disk, as for a
    /// torrent that has none
    #[arg(long)]
    pub(crate) recheck: bool,
    /// Ask every tracker of each torrent how many seeders and leechers it
    /// knows of instead of downloading; exits with an error unless one
    /// answers for each torrent
    #[arg(long, conflicts_with = "check")]
    pub(crate) scrape: bool,
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size (0: no ratio limit) [default: 0]
    #[arg(long, value_name = "RATIO", value_parser = parse_ratio)]
    pub(crate) seed_ratio: Option<f64>,
    /// Once the download is complete, keep seeding for this many minutes
    /// (0: no time limit). Seeding stops at whichever limit comes first, or
    /// on Ctrl-C; with neither, the run ends at completion [default: 0]
    #[arg(long, value_name = "MINUTES")]
    pub(crate) seed_time: Option<u64>,
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
    pub(crate) max_download_rate: Option<u64>,
    /// Most piece data to upload per second in KiB, across all peers of all
    /// torrents (0: unlimited) [default: 0]
    #[arg(long, value_name = "KIB/S")]
    pub(crate) max_upload_rate: Option<u64>,
}

impl Args {
    /// Whether the DHT was turned on or off on the command line: by the last
    /// of `--dht` and `--no-dht`, if either is given.
    pub(crate) fn dht(&self) -> Option<bool> {
        match (self.dht, self.no_dht) {
            (_, true) => Some(false),
            (true, false) => Some(true),
            (false, false) => None,
        }
    }
}

/// What to download.
#[derive(Clone)]
pub(crate) enum Source {
    File(PathBuf),
    Magnet(Box<MagnetLink>),
}

impl Source {
    /// A short name for the torrent, for logs and headings: the file's
    /// name, or the magnet link's `dn` or else its info hash.
    pub(crate) fn label(&self) -> String {
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

#[cfg(test)]
mod tests;
