use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use rtrnt::config;
use rtrnt::selection::FilePicks;
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
    /// The settings the config file can also set, but for the on and off
    /// flags below; see `options`.
    #[command(flatten)]
    pub(crate) settings: config::Options,
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
    /// Do not look for peers in the DHT, only ask the trackers (`dht =
    /// false` in the config file)
    #[arg(long, overrides_with = "dht")]
    no_dht: bool,
    /// Look for peers in the DHT, as by default, even if the config file
    /// says `dht = false`
    #[arg(long, overrides_with = "no_dht")]
    dht: bool,
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
    /// Delete the files taken from the watch directory instead of renaming
    /// them to <name>.added (`watch-dir-delete = true` in the config file)
    #[arg(long)]
    watch_dir_delete: bool,
    /// Download the pieces in order, the first and last of each file before
    /// the rest, so that media can play while it downloads (`sequential =
    /// true` in the config file); the terminal UI's `o` turns it on or off
    /// for one torrent
    #[arg(long, overrides_with = "no_sequential")]
    sequential: bool,
    /// Download the rarest pieces first, as by default, even if the config
    /// file says `sequential = true`
    #[arg(long, overrides_with = "sequential")]
    no_sequential: bool,
}

impl Args {
    /// The settings given on the command line.
    pub(crate) fn options(&self) -> config::Options {
        config::Options {
            dht: self.dht(),
            tui: self.tui(),
            session: self.session(),
            watch_dir_delete: self.watch_dir_delete(),
            sequential: self.sequential(),
            ..self.settings.clone()
        }
    }

    /// Whether the DHT was turned on or off on the command line: by the last
    /// of `--dht` and `--no-dht`, if either is given.
    pub(crate) fn dht(&self) -> Option<bool> {
        tri(self.dht, self.no_dht)
    }

    /// Whether sequential download was turned on or off on the command
    /// line, as for `dht`.
    pub(crate) fn sequential(&self) -> Option<bool> {
        tri(self.sequential, self.no_sequential)
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

#[cfg(test)]
mod tests;
