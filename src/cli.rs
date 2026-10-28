use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use rtrnt::config;
use rtrnt::source::Source;
use std::path::PathBuf;

/// Download torrents from the terminal.
#[derive(Parser)]
#[command(version, about)]
pub(crate) struct Args {
    /// Path to a .torrent file, or a magnet link (magnet:?xt=urn:btih:...);
    /// several are downloaded at once. The terminal UI can start with none,
    /// and add them
    #[arg(value_parser = parse_source, value_name = "TORRENT")]
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
    /// Show the full-screen terminal UI, never with --check, --scrape or
    /// --print-config [default: when stdout and stderr are terminals]
    #[arg(long, overrides_with = "no_tui")]
    tui: bool,
    /// Print progress bars and log lines instead of the terminal UI (`tui =
    /// false` in the config file)
    #[arg(long, overrides_with = "tui")]
    no_tui: bool,
    /// Do not load or save the session file, which the terminal UI keeps
    /// (`session = false` in the config file); without the UI nothing is
    /// loaded or saved anyway, unless the config file says `session = true`
    #[arg(long)]
    no_session: bool,
    /// Add the .torrent files, and the .magnet files holding a magnet link,
    /// that appear in this directory, checked every 2 seconds; each is
    /// renamed to <name>.added once taken, or <name>.invalid if it does not
    /// parse. With it the run goes on with no torrent left, until Ctrl-C
    #[arg(long, value_name = "DIR")]
    pub(crate) watch_dir: Option<PathBuf>,
    /// Delete the files taken from the watch directory instead of renaming
    /// them to <name>.added (`watch-dir-delete = true` in the config file)
    #[arg(long)]
    watch_dir_delete: bool,
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

    /// Whether the terminal UI was turned on or off on the command line, as
    /// for `dht`.
    pub(crate) fn tui(&self) -> Option<bool> {
        match (self.tui, self.no_tui) {
            (_, true) => Some(false),
            (true, false) => Some(true),
            (false, false) => None,
        }
    }

    /// Whether the session was turned off on the command line: `Some(false)`
    /// with `--no-session`.
    pub(crate) fn session(&self) -> Option<bool> {
        self.no_session.then_some(false)
    }

    /// Whether to delete what the watch directory takes: `Some(true)` with
    /// `--watch-dir-delete`.
    pub(crate) fn watch_dir_delete(&self) -> Option<bool> {
        self.watch_dir_delete.then_some(true)
    }

    /// Whether to load and save the session: never with --check or
    /// --scrape, and otherwise as the command line says, or else the config
    /// file (`file_session`), or else when the terminal UI is on (`tui`).
    pub(crate) fn use_session(&self, file_session: Option<bool>, tui: bool) -> bool {
        !(self.check || self.scrape) && self.session().or(file_session).unwrap_or(tui)
    }

    /// Whether to show the terminal UI: never with --check, --scrape or
    /// --print-config, and otherwise as the command line says, or else the
    /// config file (`file_tui`), or else when `terminals` says stdout and
    /// stderr both are.
    pub(crate) fn use_tui(&self, file_tui: Option<bool>, terminals: impl FnOnce() -> bool) -> bool {
        !(self.check || self.scrape || self.print_config)
            && self.tui().or(file_tui).unwrap_or_else(terminals)
    }

    /// Fails as clap would without a torrent, unless --print-config needs
    /// none, or the UI, `use_tui`, is on to add them, or a watch directory
    /// (`watching`, from the command line or the config file) will bring
    /// them.
    pub(crate) fn check_torrents(&self, use_tui: bool, watching: bool) -> Result<(), clap::Error> {
        let watching = watching && !(self.check || self.scrape);
        if self.torrents.is_empty() && !use_tui && !watching && !self.print_config {
            return Err(Self::command().error(
                ErrorKind::MissingRequiredArgument,
                "a torrent is needed, unless the terminal UI is on or a watch directory is set to add one",
            ));
        }
        Ok(())
    }
}

/// Parses a torrent argument (see `Source::parse`).
fn parse_source(arg: &str) -> Result<Source, String> {
    Source::parse(arg).map_err(|e| format!("{e:#}"))
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
