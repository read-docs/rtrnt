//! The config file: TOML defaults for the command-line options.
//!
//! Each setting comes from the command line if given there, else from the
//! config file, else from its built-in default. Keys in the file are the
//! long option names, so `--max-peers 50` is `max-peers = 50`; the one
//! exception is `--no-dht`, which is `dht = false` (and `--dht` overrides
//! it from the command line), and likewise `--no-lsd`, `lsd = false`, `--no-tui`, `tui = false` and
//! `--no-session`, `session = false`; and `--watch-dir-delete` is
//! `watch-dir-delete = true`; `--no-sequential` is `sequential = false`; and
//! `--super-seed` is `super-seed = true`.

use crate::altspeed::{Limits, Schedule};
use crate::download::{DownloadConfig, SeedLimits};
use crate::peer;
use crate::queue::QueueLimits;
use crate::resume;
use crate::rss::{self, Subscription};
use crate::storage::Preallocate;
use crate::tags::{TagSettings, TagTable};
use anyhow::{Context, Result, bail};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use std::ffi::OsString;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Directory downloads are saved in unless told otherwise.
pub const DEFAULT_OUTPUT_DIR: &str = ".";
/// Peers downloaded from at once unless told otherwise.
pub const DEFAULT_MAX_PEERS: usize = 30;
/// Connections open at once, across every torrent, unless told otherwise.
pub const DEFAULT_MAX_CONNECTIONS: usize = 200;
/// Upload ratio to seed to once the download is complete: none, so the run
/// ends at completion unless a seed time is set.
pub const DEFAULT_SEED_RATIO: f64 = 0.0;
/// Minutes to seed for once the download is complete: none.
pub const DEFAULT_SEED_TIME: u64 = 0;
/// Whether to look for peers in the DHT unless told otherwise.
pub const DEFAULT_DHT: bool = true;
/// Whether to look for peers on the local network unless told otherwise.
pub const DEFAULT_LSD: bool = true;
/// Whether to map the listening port on the router unless told otherwise.
pub const DEFAULT_PORTMAP: bool = true;
/// Whether to delete the files taken from the watch directory: they are
/// renamed instead.
pub const DEFAULT_WATCH_DIR_DELETE: bool = false;
/// Whether to download pieces in order, the first and last of each file
/// first: no, rarest first.
pub const DEFAULT_SEQUENTIAL: bool = false;
/// Whether to super-seed (BEP 16) a torrent we have complete: no.
pub const DEFAULT_SUPER_SEED: bool = false;
/// How files get their disk space: as data arrives (sparse).
pub const DEFAULT_PREALLOCATE: Preallocate = Preallocate::Sparse;
/// Memory each torrent may hold verified pieces in before writing them out.
pub const DEFAULT_DISK_CACHE: ByteSize = ByteSize(16 * 1024 * 1024);
/// Download limit in KiB/s: none.
pub const DEFAULT_MAX_DOWNLOAD_RATE: u64 = 0;
/// Upload limit in KiB/s: none.
pub const DEFAULT_MAX_UPLOAD_RATE: u64 = 0;

/// Download limit in KiB/s of the alternative speed mode.
pub const DEFAULT_ALT_SPEED_DOWNLOAD: u64 = 50;
/// Upload limit in KiB/s of the alternative speed mode.
pub const DEFAULT_ALT_SPEED_UPLOAD: u64 = 50;
/// Torrents downloading at once: no limit.
pub const DEFAULT_MAX_ACTIVE_DOWNLOADS: usize = 0;
/// Torrents seeding at once: no limit.
pub const DEFAULT_MAX_ACTIVE_SEEDS: usize = 0;

/// What a setting is when nothing sets it.
enum Fallback<T> {
    /// This built-in default.
    Value(T),
    /// None: the code that reads it decides, as this says.
    Unset(&'static str),
}

/// A setting's value written as TOML.
trait ToToml {
    fn to_toml(&self) -> String;
}

impl ToToml for PathBuf {
    fn to_toml(&self) -> String {
        toml::Value::from(self.display().to_string()).to_string()
    }
}

/// A whole ratio is written `1.0`, so it reads back as a float.
impl ToToml for f64 {
    fn to_toml(&self) -> String {
        toml::Value::from(*self).to_string()
    }
}

impl ToToml for Preallocate {
    fn to_toml(&self) -> String {
        toml::Value::from(self.as_str()).to_string()
    }
}

impl ToToml for ByteSize {
    fn to_toml(&self) -> String {
        match self.0 {
            0 => "0".to_string(),
            n if n % KIB == 0 => toml::Value::from(self.to_string()).to_string(),
            n => n.to_string(),
        }
    }
}

impl ToToml for SocketAddr {
    fn to_toml(&self) -> String {
        toml::Value::from(self.to_string()).to_string()
    }
}

impl ToToml for Vec<Subscription> {
    fn to_toml(&self) -> String {
        rss::to_toml(self)
    }
}

impl ToToml for Schedule {
    fn to_toml(&self) -> String {
        Schedule::to_toml(self)
    }
}

impl ToToml for TagTable {
    fn to_toml(&self) -> String {
        TagTable::to_toml(self)
    }
}

macro_rules! to_toml_by_display {
    ($($ty:ty),*) => {$(
        impl ToToml for $ty {
            fn to_toml(&self) -> String {
                self.to_string()
            }
        }
    )*};
}

/// An argument list is a TOML array of strings.
impl ToToml for Vec<String> {
    fn to_toml(&self) -> String {
        toml::Value::from(self.clone()).to_string()
    }
}

to_toml_by_display!(bool, u16, u64, usize);

/// One setting as `describe` sees it.
struct Setting {
    /// Its key in the config file.
    key: String,
    /// Its value as TOML, if set.
    value: Option<String>,
    /// What it is when unset, as TOML.
    fallback: Fallback<String>,
}

/// Declares `Options`, which both the command line (through clap) and the
/// config file (through serde) read, from one list of settings, each
/// followed by `= ` and its [`Fallback`]; and with it the code that goes
/// over every setting, so a new one is added in this list only.
macro_rules! options {
    ($(
        $(#[$attr:meta])*
        $field:ident: $ty:ty = $fallback:expr;
    )*) => {
        /// Settings that can come from the command line or the config
        /// file. A field left `None` is not set there and falls back to the
        /// next source. The doc comments are the command line's help.
        #[derive(Debug, Default, Clone, PartialEq, Deserialize, clap::Args)]
        #[serde(deny_unknown_fields, rename_all = "kebab-case")]
        pub struct Options {
            $($(#[$attr])* pub $field: Option<$ty>,)*
        }

        impl Options {
            /// Takes each setting from `self`, or from `fallback` where
            /// `self` leaves it unset.
            pub fn or(self, fallback: Options) -> Options {
                Options {
                    $($field: self.$field.or(fallback.$field),)*
                }
            }

            /// Each setting, in the order of the list.
            fn settings(&self) -> Vec<Setting> {
                use Fallback::{Unset, Value};
                vec![$({
                    let fallback: Fallback<$ty> = $fallback;
                    Setting {
                        key: stringify!($field).replace('_', "-"),
                        value: self.$field.as_ref().map(ToToml::to_toml),
                        fallback: match fallback {
                            Value(value) => Value(value.to_toml()),
                            Unset(text) => Unset(text),
                        },
                    }
                },)*]
            }
        }
    };
}

// Settings that the config file can also set have no clap default, so an
// unset flag falls back to the file. The defaults are spelled out in the
// help (checked by a test). The on and off flags of the settings marked
// `skip` are in `Args`, which fills them in.
options! {
    /// Directory to save the downloaded files in [default: .]
    #[arg(short, long)]
    output: PathBuf = Value(PathBuf::from(DEFAULT_OUTPUT_DIR));
    /// Maximum number of peers to download from at once, for each torrent
    /// [default: 30]
    #[arg(long)]
    max_peers: usize = Value(DEFAULT_MAX_PEERS);
    /// Maximum number of peer connections open at once, across all
    /// torrents [default: 200]
    #[arg(long)]
    max_connections: usize = Value(DEFAULT_MAX_CONNECTIONS);
    /// Port to listen on for incoming peers (0 picks a free one; so does a
    /// port that is taken) [default: 6881]
    #[arg(short, long)]
    port: u16 = Value(peer::DEFAULT_LISTEN_PORT);
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size (0: no ratio limit) [default: 0]
    #[arg(long, value_name = "RATIO", value_parser = parse_ratio)]
    #[serde(default, deserialize_with = "deserialize_ratio")]
    seed_ratio: f64 = Value(DEFAULT_SEED_RATIO);
    /// Once the download is complete, keep seeding for this many minutes
    /// (0: no time limit). Seeding stops at whichever limit comes first, or
    /// on Ctrl-C; with neither, the run ends at completion [default: 0]
    #[arg(long, value_name = "MINUTES")]
    seed_time: u64 = Value(DEFAULT_SEED_TIME);
    /// Whether to look for peers in the DHT too (`--no-dht` is
    /// `dht = false`).
    #[arg(skip)]
    dht: bool = Value(DEFAULT_DHT);
    /// Whether to look for peers on the local network by multicast (Local
    /// Service Discovery, BEP 14; `--no-lsd` is `lsd = false`).
    #[arg(skip)]
    lsd: bool = Value(DEFAULT_LSD);
    /// Whether to ask the router to forward the listening port, by PCP,
    /// NAT-PMP or UPnP (`--no-portmap` is `portmap = false`).
    #[arg(skip)]
    portmap: bool = Value(DEFAULT_PORTMAP);
    /// Most piece data to download per second in KiB, across all peers of
    /// all torrents (0: unlimited) [default: 0]
    #[arg(long, value_name = "KIB/S")]
    max_download_rate: u64 = Value(DEFAULT_MAX_DOWNLOAD_RATE);
    /// Most piece data to upload per second in KiB, across all peers of all
    /// torrents (0: unlimited) [default: 0]
    #[arg(long, value_name = "KIB/S")]
    max_upload_rate: u64 = Value(DEFAULT_MAX_UPLOAD_RATE);
    /// Most torrents downloading at once; the rest wait in the queue (0: no
    /// limit) [default: 0]
    #[arg(long, value_name = "N")]
    max_active_downloads: usize = Value(DEFAULT_MAX_ACTIVE_DOWNLOADS);
    /// Most complete torrents seeding at once; the rest wait in the queue
    /// (0: no limit) [default: 0]
    #[arg(long, value_name = "N")]
    max_active_seeds: usize = Value(DEFAULT_MAX_ACTIVE_SEEDS);
    /// Whether to show the terminal UI; unset, it shows when stdout and
    /// stderr are terminals.
    #[arg(skip)]
    tui: bool = Unset("when stdout and stderr are terminals");
    /// Whether to remember the torrents and rate limits across runs
    /// (`--no-session` is `session = false`); unset, the terminal UI does
    /// and the rest does not.
    #[arg(skip)]
    session: bool = Unset("only in the terminal UI");
    /// Add the .torrent files, and the .magnet files holding a magnet link,
    /// that appear in this directory, checked every 2 seconds; each is
    /// renamed to <name>.added once taken, or <name>.invalid if it does not
    /// parse. With it the run goes on with no torrent left, until Ctrl-C
    #[arg(long, value_name = "DIR")]
    watch_dir: PathBuf = Unset("none");
    /// Whether to delete the files taken from the watch directory instead
    /// of renaming them to `<name>.added`.
    #[arg(skip)]
    watch_dir_delete: bool = Value(DEFAULT_WATCH_DIR_DELETE);
    /// Refuse the peers listed in this file, whether they connect to us,
    /// come from a tracker, the DHT or peer exchange. It holds PeerGuardian
    /// (`name:1.2.3.4-1.2.3.255`) or eMule `ipfilter.dat` lines, CIDR blocks
    /// or single addresses, IPv4 or IPv6; gzip is not read
    #[arg(long, value_name = "FILE")]
    blocklist: PathBuf = Unset("none");
    /// Run a command when a torrent finishes downloading, with the name,
    /// path and info hash in $RTRNT_NAME, $RTRNT_PATH and
    /// $RTRNT_INFO_HASH. The command is run directly, not through a shell:
    /// give the program, then repeat the flag for each argument, as in
    /// `--completion-hook notify-send --completion-hook Done`. Not run for
    /// a torrent that was already complete (`completion-hook = ["..."]` in
    /// the config file) [default: none]
    #[arg(long, value_name = "ARG", allow_hyphen_values = true)]
    completion_hook: Vec<String> = Unset("none");
    /// Whether torrents start out downloading in order (`--no-sequential`
    /// is `sequential = false`); the UI's `o` turns it on or off for one.
    #[arg(skip)]
    sequential: bool = Value(DEFAULT_SEQUENTIAL);
    /// Whether to super-seed (BEP 16) the torrents we have complete: each
    /// peer is shown one piece at a time, which spreads a first copy of the
    /// torrent over the swarm faster (`--super-seed`)
    #[arg(skip)]
    super_seed: bool = Value(DEFAULT_SUPER_SEED);
    /// How files get their disk space: `sparse` sets each to its full size
    /// and lets the file system fill it in as pieces arrive; `full`
    /// reserves the space of every wanted file when it is created, so a full
    /// disk fails at the start. Skipped files are not reserved
    /// [default: sparse]
    #[arg(long, value_enum, value_name = "MODE")]
    preallocate: Preallocate = Value(DEFAULT_PREALLOCATE);
    /// Memory each torrent holds verified pieces in before writing them to
    /// disk together, in offset order: a size in bytes, or with a `k`, `m`
    /// or `g` suffix (KiB, MiB, GiB; `16MiB` works too). The cache is
    /// flushed when full, every few seconds, and when the torrent stops or
    /// is rechecked (0: write each piece at once) [default: 16MiB]
    #[arg(long, value_name = "SIZE")]
    disk_cache: ByteSize = Value(DEFAULT_DISK_CACHE);
    /// Download limit in KiB per second while the alternative speed mode
    /// ("turtle mode", `t` in the terminal UI) is on (0: unlimited)
    /// [default: 50]
    #[arg(long, value_name = "KIB/S")]
    alt_speed_download: u64 = Value(DEFAULT_ALT_SPEED_DOWNLOAD);
    /// Upload limit in KiB per second while the alternative speed mode is
    /// on (0: unlimited) [default: 50]
    #[arg(long, value_name = "KIB/S")]
    alt_speed_upload: u64 = Value(DEFAULT_ALT_SPEED_UPLOAD);
    /// When the alternative speed mode is on by itself: a list of rules
    /// like `{ days = ["mon", "tue"], from = "08:00", to = "23:00" }`.
    /// `days` (mon to sun, weekdays, weekend) is every day if left out; a
    /// `to` before `from` runs past midnight into the next day.
    #[arg(skip)]
    alt_speed_schedule: Schedule = Unset("only by hand");
    /// Serve Prometheus metrics at `http://ADDR/metrics`, such as
    /// 127.0.0.1:9100; there is no authentication, so bind it to a loopback
    /// or private address [default: off]
    #[arg(long, value_name = "ADDR")]
    metrics_listen: SocketAddr = Unset("off");
    /// The RSS and Atom feeds to add torrents from: a list of `[[rss]]`
    /// tables with `url`, `match` (regular expressions on the title),
    /// and optionally `interval` (minutes, at least 5, default 15),
    /// `exclude` and `paused`.
    #[arg(skip)]
    rss: Vec<Subscription> = Unset("none");
    /// What a tag gives the torrents that have it: a table for each tag,
    /// as in `[tags.private]` with `download`, `upload`, `seed-ratio` and
    /// `seed-time`, each applying unless the torrent sets its own.
    #[arg(skip)]
    tags: TagTable = Unset("none");
}

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;
const GIB: u64 = 1024 * MIB;

/// A size in bytes, written as a number of bytes or with a `k`, `m` or `g`
/// suffix (also `kb`, `kib` and so on), all powers of 1024.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(pub u64);

impl std::str::FromStr for ByteSize {
    type Err = String;

    fn from_str(arg: &str) -> Result<Self, String> {
        let lower = arg.trim().to_ascii_lowercase();
        let digits_end = lower
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(lower.len());
        let (digits, unit) = lower.split_at(digits_end);
        let unit = match unit.trim() {
            "" | "b" => 1,
            "k" | "kb" | "kib" => KIB,
            "m" | "mb" | "mib" => MIB,
            "g" | "gb" | "gib" => GIB,
            _ => return Err(format!("{arg:?} is not a size such as 16MiB or 4096")),
        };
        digits
            .parse::<u64>()
            .ok()
            .and_then(|n| n.checked_mul(unit))
            .map(ByteSize)
            .ok_or_else(|| format!("{arg:?} is not a size such as 16MiB or 4096"))
    }
}

/// The size as `from_str` reads it back: in the largest unit that divides
/// it.
impl std::fmt::Display for ByteSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            n if n != 0 && n % GIB == 0 => write!(f, "{}GiB", n / GIB),
            n if n != 0 && n % MIB == 0 => write!(f, "{}MiB", n / MIB),
            n if n != 0 && n % KIB == 0 => write!(f, "{}KiB", n / KIB),
            n => write!(f, "{n}"),
        }
    }
}

/// In the config file, a number of bytes or a string like `"16MiB"`.
impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Bytes(u64),
            Text(String),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Bytes(n) => Ok(ByteSize(n)),
            Raw::Text(text) => text.parse().map_err(D::Error::custom),
        }
    }
}

/// Whether `ratio` is a valid seed ratio: a finite number, 0 or more.
pub fn is_valid_ratio(ratio: f64) -> bool {
    ratio.is_finite() && ratio >= 0.0
}

/// Parses `--seed-ratio`, which must be a valid ratio (see
/// `is_valid_ratio`).
fn parse_ratio(arg: &str) -> Result<f64, String> {
    match arg.parse::<f64>() {
        Ok(ratio) if is_valid_ratio(ratio) => Ok(ratio),
        _ => Err(format!("{arg:?} is not a ratio of 0 or more")),
    }
}

/// Reads `seed-ratio`, which must be a valid ratio (see `is_valid_ratio`).
/// An integer is fine too.
pub(crate) fn deserialize_ratio<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<f64>, D::Error> {
    let ratio = f64::deserialize(deserializer)?;
    if !is_valid_ratio(ratio) {
        return Err(D::Error::custom(format!(
            "{ratio} is not a ratio of 0 or more"
        )));
    }
    Ok(Some(ratio))
}

impl Options {
    /// The alternative download and upload limits in KiB/s these settings
    /// give, and the schedule they apply on.
    pub fn alt_speed(&self) -> (Limits, Schedule) {
        (
            (
                self.alt_speed_download
                    .unwrap_or(DEFAULT_ALT_SPEED_DOWNLOAD),
                self.alt_speed_upload.unwrap_or(DEFAULT_ALT_SPEED_UPLOAD),
            ),
            self.alt_speed_schedule.clone().unwrap_or_default(),
        )
    }

    /// What each tag gives its torrents (see `tags`).
    pub fn tag_settings(&self) -> TagSettings {
        self.tags
            .as_ref()
            .map(TagTable::settings)
            .unwrap_or_default()
    }

    /// The queue limits these settings give, with the built-in default for
    /// each one left unset.
    pub fn queue_limits(&self) -> QueueLimits {
        QueueLimits {
            downloads: self
                .max_active_downloads
                .unwrap_or(DEFAULT_MAX_ACTIVE_DOWNLOADS),
            seeds: self.max_active_seeds.unwrap_or(DEFAULT_MAX_ACTIVE_SEEDS),
        }
    }

    /// The download config these settings give, with the built-in default
    /// for each one left unset.
    pub fn into_download_config(self) -> DownloadConfig {
        DownloadConfig {
            output_dir: self
                .output
                .unwrap_or_else(|| PathBuf::from(DEFAULT_OUTPUT_DIR)),
            max_peers: self.max_peers.unwrap_or(DEFAULT_MAX_PEERS),
            max_connections: self.max_connections.unwrap_or(DEFAULT_MAX_CONNECTIONS),
            listen_port: self.port.unwrap_or(peer::DEFAULT_LISTEN_PORT),
            seed: SeedLimits {
                ratio: self.seed_ratio.unwrap_or(DEFAULT_SEED_RATIO),
                time: minutes(self.seed_time.unwrap_or(DEFAULT_SEED_TIME)),
            },
            dht: self.dht.unwrap_or(DEFAULT_DHT),
            lsd: self.lsd.unwrap_or(DEFAULT_LSD),
            sequential: self.sequential.unwrap_or(DEFAULT_SEQUENTIAL),
            super_seed: self.super_seed.unwrap_or(DEFAULT_SUPER_SEED),
            portmap: self.portmap.unwrap_or(DEFAULT_PORTMAP),
            preallocate: self.preallocate.unwrap_or(DEFAULT_PREALLOCATE),
            disk_cache: self.disk_cache.unwrap_or(DEFAULT_DISK_CACHE).0,
            max_download_rate: self.max_download_rate.unwrap_or(DEFAULT_MAX_DOWNLOAD_RATE),
            max_upload_rate: self.max_upload_rate.unwrap_or(DEFAULT_MAX_UPLOAD_RATE),
            completion_hook: self.completion_hook,
            resume_dir: resume::default_dir(
                std::env::var_os("XDG_STATE_HOME"),
                std::env::home_dir().as_deref(),
            ),
        }
    }
}

/// `minutes` as a duration, saturating rather than overflowing.
fn minutes(minutes: u64) -> Duration {
    Duration::from_secs(minutes.saturating_mul(60))
}

/// The effective download config: each setting from `cli` if given there,
/// else from `file`, else its built-in default.
pub fn resolve(cli: Options, file: Options) -> DownloadConfig {
    cli.or(file).into_download_config()
}

/// Settings read from a config file.
#[derive(Debug, Default)]
pub struct ConfigFile {
    /// The file they were read from, or `None` if there was none.
    pub path: Option<PathBuf>,
    pub options: Options,
}

/// Reads the config file at `path`, or at [`default_path`] if `None`. A
/// missing default file gives empty settings; a missing `path` is an error.
pub fn load(path: Option<&Path>) -> Result<ConfigFile> {
    let home = std::env::home_dir();
    let (path, required) = match path {
        Some(path) => (path.to_path_buf(), true),
        None => match default_path(std::env::var_os("XDG_CONFIG_HOME"), home.as_deref()) {
            Some(path) => (path, false),
            None => return Ok(ConfigFile::default()),
        },
    };
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if !required && e.kind() == io::ErrorKind::NotFound => {
            return Ok(ConfigFile::default());
        }
        Err(e) => {
            return Err(e).with_context(|| format!("cannot read config file {}", path.display()));
        }
    };
    let options = parse(&text, home.as_deref())
        .with_context(|| format!("invalid config file {}", path.display()))?;
    Ok(ConfigFile {
        path: Some(path),
        options,
    })
}

/// Parses the text of a config file. A leading `~` in `output` stands for
/// `home`.
pub fn parse(text: &str, home: Option<&Path>) -> Result<Options> {
    let mut options: Options = toml::from_str(text)?;
    options.output = options
        .output
        .map(|dir| expand_tilde(dir, home))
        .transpose()?;
    options.watch_dir = options
        .watch_dir
        .map(|dir| expand_tilde(dir, home))
        .transpose()?;
    options.blocklist = options
        .blocklist
        .map(|path| expand_tilde(path, home))
        .transpose()?;
    if let Some(tags) = &options.tags {
        tags.validate()?;
    }
    Ok(options)
}

/// Where the config file is looked for when none is given:
/// `$XDG_CONFIG_HOME/rtrnt/config.toml`, or `~/.config/rtrnt/config.toml`
/// when that is unset. `None` if neither directory is known.
///
/// This is the XDG location on every platform, macOS included, which is
/// where terminal tools are usually configured.
pub fn default_path(xdg_config_home: Option<OsString>, home: Option<&Path>) -> Option<PathBuf> {
    // The XDG spec says to ignore a relative (or empty) value.
    let config_dir = match xdg_config_home.map(PathBuf::from) {
        Some(dir) if dir.is_absolute() => dir,
        _ => home?.join(".config"),
    };
    Some(config_dir.join("rtrnt").join("config.toml"))
}

/// Replaces a leading `~` in `path` with `home`. `~user` is left alone.
fn expand_tilde(path: PathBuf, home: Option<&Path>) -> Result<PathBuf> {
    let Ok(rest) = path.strip_prefix("~") else {
        return Ok(path);
    };
    let Some(home) = home else {
        bail!("cannot expand ~ in {}: no home directory", path.display());
    };
    if rest.as_os_str().is_empty() {
        return Ok(home.to_path_buf());
    }
    Ok(home.join(rest))
}

/// Describes the effective settings as TOML, each followed by a comment
/// saying whether it came from the command line, the session, the config
/// file or the built-in default. `session` holds the rate limits the
/// session file has, which rank between the command line and the config
/// file.
pub fn describe(cli: &Options, session: &Options, file: &ConfigFile) -> String {
    let header = match &file.path {
        Some(path) => format!("# config file: {}\n", path.display()),
        None => "# no config file\n".to_string(),
    };
    let lines = cli
        .settings()
        .into_iter()
        .zip(session.settings())
        .zip(file.options.settings())
        .map(|((cli, session), file)| {
            let set = [
                (cli.value, "command line"),
                (session.value, "session"),
                (file.value, "config file"),
            ]
            .into_iter()
            .find_map(|(value, source)| Some((value?, source)));
            let key = cli.key;
            match (set, cli.fallback) {
                (Some((value, source)), _) => format!("{key} = {value}  # {source}\n"),
                (None, Fallback::Value(value)) => format!("{key} = {value}  # default\n"),
                (None, Fallback::Unset(text)) => format!("# {key}: {text}  # default\n"),
            }
        });
    std::iter::once(header).chain(lines).collect()
}

#[cfg(test)]
mod tests;
