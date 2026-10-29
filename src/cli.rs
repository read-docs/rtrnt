use clap::error::ErrorKind;
use clap::{CommandFactory, Parser, Subcommand};
use rtrnt::config;
use rtrnt::create::parse_piece_length;
use rtrnt::selection::FilePicks;
use rtrnt::source::Source;
use std::path::PathBuf;

/// Download torrents from the terminal.
///
/// Without a subcommand, the arguments are the torrents to download. A
/// file named `create` or `info` is taken for the subcommand: write
/// `./info` for the file.
#[derive(Parser)]
#[command(version, about, args_conflicts_with_subcommands = true)]
pub(crate) struct Args {
    #[command(subcommand)]
    pub(crate) command: Option<Subcommands>,
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
    /// Download only these files of each torrent, numbered from 1 in the
    /// order the torrent lists them: `1,3-5`. A file in a piece shared with
    /// a wanted one is fetched in part anyway. A number past the last file
    /// is warned of; a magnet link applies it once its metadata arrives.
    /// Not in the config file; the terminal UI's Files tab changes the
    /// choice later
    #[arg(long, value_name = "FILES", value_parser = FilePicks::parse)]
    pub(crate) select_file: Option<FilePicks>,
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

/// What to do instead of downloading.
#[derive(Subcommand)]
pub(crate) enum Subcommands {
    /// Print what a .torrent file says: its name, info hash, size, pieces,
    /// trackers, web seeds, files and magnet link
    Info {
        /// The .torrent file to read
        file: PathBuf,
    },
    /// Make a .torrent file of a file or a directory, and print its info
    /// hash and magnet link
    Create(CreateArgs),
}

/// The options of `rtrnt create`.
#[derive(clap::Args)]
pub(crate) struct CreateArgs {
    /// The file, or the directory of files, to make a torrent of. Files
    /// are listed in sorted order; symbolic links are left out, and
    /// listed as they are
    pub(crate) path: PathBuf,
    /// Where to write the torrent [default: <name>.torrent]
    #[arg(short, long, value_name = "FILE")]
    pub(crate) output: Option<PathBuf>,
    /// A tracker to announce to; repeat for more, each in a tier of its own
    #[arg(short, long = "tracker", value_name = "URL")]
    pub(crate) trackers: Vec<String>,
    /// A web seed (BEP 19) that serves the files over HTTP; repeat for more
    #[arg(short, long = "web-seed", value_name = "URL")]
    pub(crate) web_seeds: Vec<String>,
    /// Bytes in each piece: a power of two from 16k to 64m, written as
    /// 262144, 256k or 4m [default: for 1000 to 2000 pieces, within 16k to
    /// 16m]
    #[arg(long, value_name = "SIZE", value_parser = parse_piece_length)]
    pub(crate) piece_length: Option<u64>,
    /// Mark the torrent private: its peers come from its trackers only
    #[arg(long)]
    pub(crate) private: bool,
    /// A comment to put in the torrent
    #[arg(long)]
    pub(crate) comment: Option<String>,
    /// A tag to put in the torrent's info dictionary, as some private
    /// trackers ask for; it changes the info hash
    #[arg(long)]
    pub(crate) source: Option<String>,
}

impl Args {
    /// Whether the DHT was turned on or off on the command line: by the last
    /// of `--dht` and `--no-dht`, if either is given.
    pub(crate) fn dht(&self) -> Option<bool> {
        tri(self.dht, self.no_dht)
    }

    /// Whether the terminal UI was turned on or off on the command line, as
    /// for `dht`.
    pub(crate) fn tui(&self) -> Option<bool> {
        tri(self.tui, self.no_tui)
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

/// The setting a pair of on and off flags gives: `Some(false)` if `off` is
/// set, else `Some(true)` if `on` is, else `None`. Each flag overrides the
/// other, so at most one is set.
fn tri(on: bool, off: bool) -> Option<bool> {
    match (on, off) {
        (_, true) => Some(false),
        (true, false) => Some(true),
        (false, false) => None,
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
