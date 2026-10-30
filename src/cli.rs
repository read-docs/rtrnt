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
    /// Print the state of the session as one JSON object per line, every
    /// second and once more at the end, instead of progress bars and the
    /// terminal UI; log lines still go to stderr
    #[arg(long, conflicts_with_all = ["tui", "check", "scrape", "print_config"])]
    pub(crate) json: bool,
    /// Run headless until stopped, keeping the session and serving local RPC
    #[arg(long, conflicts_with_all = ["attach", "tui", "check", "scrape", "print_config", "rpc_request", "json"])]
    pub(crate) daemon: bool,
    /// Attach the terminal UI to an existing daemon; q detaches
    #[arg(long, conflicts_with_all = ["daemon", "torrents", "check", "scrape", "print_config", "json", "rpc_request"])]
    pub(crate) attach: bool,
    /// Unix control socket [default: $XDG_STATE_HOME/rtrnt/rpc/control.sock]
    #[arg(long, value_name = "PATH")]
    pub(crate) rpc_socket: Option<PathBuf>,
    /// Send one JSON RPC action, print the response, and exit
    #[arg(long, value_name = "JSON", conflicts_with_all = ["daemon", "attach", "torrents", "check", "scrape", "print_config", "json", "tui"])]
    pub(crate) rpc_request: Option<String>,
    /// Do not look for peers in the DHT, only ask the trackers (`dht =
    /// false` in the config file)
    #[arg(long, overrides_with = "dht")]
    no_dht: bool,
    /// Look for peers in the DHT, as by default, even if the config file
    /// says `dht = false`
    #[arg(long, overrides_with = "no_dht")]
    dht: bool,
    /// Do not look for peers on the local network by multicast (Local
    /// Service Discovery; `lsd = false` in the config file)
    #[arg(long, overrides_with = "lsd")]
    no_lsd: bool,
    /// Look for peers on the local network, as by default, even if the
    /// config file says `lsd = false`
    #[arg(long, overrides_with = "no_lsd")]
    lsd: bool,
    /// Do not ask the router to forward the listening port with PCP,
    /// NAT-PMP or UPnP (`portmap = false` in the config file)
    #[arg(long, overrides_with = "portmap")]
    no_portmap: bool,
    /// Ask the router to forward the listening port, as by default, even
    /// if the config file says `portmap = false`
    #[arg(long, overrides_with = "no_portmap")]
    portmap: bool,
    /// Show the full-screen terminal UI, never with --check, --scrape,
    /// --json or --print-config [default: when stdout and stderr are
    /// terminals]
    #[arg(long, overrides_with = "no_tui")]
    tui: bool,
    /// Print progress bars and log lines instead of the terminal UI (`tui =
    /// false` in the config file)
    #[arg(long, overrides_with = "tui")]
    no_tui: bool,
    /// Do not load or save the session file, which the terminal UI and daemon
    /// keep (`session = false` in the config file); other modes keep it only
    /// when the config file says `session = true`
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
    /// Super-seed (BEP 16) the torrents we have complete: tell each peer of
    /// one piece at a time instead of every piece we have, and the next
    /// once another peer has that one, so that a first copy of a torrent
    /// reaches the swarm with the least upload (`super-seed = true` in the
    /// config file). Meant for the first seed of a torrent; off by default
    #[arg(long)]
    super_seed: bool,
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
    /// The settings given on the command line.
    pub(crate) fn options(&self) -> config::Options {
        config::Options {
            dht: self.dht(),
            lsd: self.lsd(),
            portmap: self.portmap(),
            tui: self.tui(),
            session: self.session(),
            watch_dir_delete: self.watch_dir_delete(),
            sequential: self.sequential(),
            super_seed: self.super_seed(),
            ..self.settings.clone()
        }
    }

    /// Whether the DHT was turned on or off on the command line: by the last
    /// of `--dht` and `--no-dht`, if either is given.
    pub(crate) fn dht(&self) -> Option<bool> {
        tri(self.dht, self.no_dht)
    }

    /// Whether Local Service Discovery was turned on or off on the command
    /// line, as for `dht`.
    pub(crate) fn lsd(&self) -> Option<bool> {
        tri(self.lsd, self.no_lsd)
    }

    /// Whether sequential download was turned on or off on the command
    /// line, as for `dht`.
    pub(crate) fn sequential(&self) -> Option<bool> {
        tri(self.sequential, self.no_sequential)
    }

    /// Whether to super-seed: `Some(true)` with `--super-seed`.
    pub(crate) fn super_seed(&self) -> Option<bool> {
        self.super_seed.then_some(true)
    }

    /// Whether port mapping was turned on or off on the command line, as for
    /// `dht`.
    pub(crate) fn portmap(&self) -> Option<bool> {
        tri(self.portmap, self.no_portmap)
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
        !(self.check || self.scrape)
            && self
                .session()
                .or(file_session)
                .unwrap_or(tui || self.daemon)
    }

    /// Whether to show the terminal UI: never with --check, --scrape,
    /// --print-config or --json, and otherwise as the command line says, or else the
    /// config file (`file_tui`), or else when `terminals` says stdout and
    /// stderr both are.
    pub(crate) fn use_tui(&self, file_tui: Option<bool>, terminals: impl FnOnce() -> bool) -> bool {
        !(self.check || self.scrape || self.print_config || self.json || self.daemon)
            && self.tui().or(file_tui).unwrap_or_else(terminals)
    }

    /// Fails as clap would without a torrent, unless --print-config needs
    /// none, or the UI, `use_tui`, is on to add them, or a watch directory
    /// (`watching`, from the command line or the config file) will bring
    /// them.
    pub(crate) fn check_torrents(&self, use_tui: bool, watching: bool) -> Result<(), clap::Error> {
        let watching = watching && !(self.check || self.scrape);
        if self.torrents.is_empty() && !use_tui && !watching && !self.print_config && !self.daemon {
            return Err(Self::command().error(
                ErrorKind::MissingRequiredArgument,
                "a torrent is needed, unless the terminal UI is on or a watch directory or RSS feed is set to add one",
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
