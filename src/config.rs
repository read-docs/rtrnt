//! The config file: TOML defaults for the command-line options.
//!
//! Each setting comes from the command line if given there, else from the
//! config file, else from its built-in default. Keys in the file are the
//! long option names, so `--max-peers 50` is `max-peers = 50`; the one
//! exception is `--no-dht`, which is `dht = false` (and `--dht` overrides
//! it from the command line).

use crate::download::{DownloadConfig, SeedLimits};
use crate::peer;
use crate::resume;
use anyhow::{Context, Result, bail};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use std::ffi::OsString;
use std::io;
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
/// Download limit in KiB/s: none.
pub const DEFAULT_MAX_DOWNLOAD_RATE: u64 = 0;
/// Upload limit in KiB/s: none.
pub const DEFAULT_MAX_UPLOAD_RATE: u64 = 0;

/// Settings that can come from the command line or the config file. A
/// field left `None` is not set there and falls back to the next source.
#[derive(Debug, Default, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Options {
    /// Directory to save the downloaded files in.
    pub output: Option<PathBuf>,
    /// Maximum number of peers to download from at once, for each torrent.
    pub max_peers: Option<usize>,
    /// Maximum number of peer connections open at once, across every
    /// torrent.
    pub max_connections: Option<usize>,
    /// Port to listen on for incoming peers; 0 picks a free one.
    pub port: Option<u16>,
    /// Once the download is complete, keep seeding until we have uploaded
    /// this many times the torrent's size; 0 for no ratio limit.
    #[serde(default, deserialize_with = "deserialize_ratio")]
    pub seed_ratio: Option<f64>,
    /// Once the download is complete, keep seeding for this many minutes; 0
    /// for no time limit.
    pub seed_time: Option<u64>,
    /// Whether to look for peers in the DHT too (`--no-dht` is
    /// `dht = false`).
    pub dht: Option<bool>,
    /// Most block data to download per second, in KiB; 0 for no limit.
    pub max_download_rate: Option<u64>,
    /// Most block data to upload per second, in KiB; 0 for no limit.
    pub max_upload_rate: Option<u64>,
}

/// Whether `ratio` is a valid seed ratio: a finite number, 0 or more.
pub fn is_valid_ratio(ratio: f64) -> bool {
    ratio.is_finite() && ratio >= 0.0
}

/// Reads `seed-ratio`, which must be a valid ratio (see `is_valid_ratio`).
/// An integer is fine too.
fn deserialize_ratio<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<f64>, D::Error> {
    let ratio = f64::deserialize(deserializer)?;
    if !is_valid_ratio(ratio) {
        return Err(D::Error::custom(format!(
            "{ratio} is not a ratio of 0 or more"
        )));
    }
    Ok(Some(ratio))
}

impl Options {
    /// Takes each setting from `self`, or from `fallback` where `self`
    /// leaves it unset.
    pub fn or(self, fallback: Options) -> Options {
        Options {
            output: self.output.or(fallback.output),
            max_peers: self.max_peers.or(fallback.max_peers),
            max_connections: self.max_connections.or(fallback.max_connections),
            port: self.port.or(fallback.port),
            seed_ratio: self.seed_ratio.or(fallback.seed_ratio),
            seed_time: self.seed_time.or(fallback.seed_time),
            dht: self.dht.or(fallback.dht),
            max_download_rate: self.max_download_rate.or(fallback.max_download_rate),
            max_upload_rate: self.max_upload_rate.or(fallback.max_upload_rate),
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
            max_download_rate: self.max_download_rate.unwrap_or(DEFAULT_MAX_DOWNLOAD_RATE),
            max_upload_rate: self.max_upload_rate.unwrap_or(DEFAULT_MAX_UPLOAD_RATE),
            resume_dir: resume::default_dir(
                std::env::var_os("XDG_STATE_HOME"),
                std::env::home_dir().as_deref(),
            ),
            recheck: false,
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
/// saying whether it came from the command line, the config file or the
/// built-in default.
pub fn describe(cli: &Options, file: &ConfigFile) -> String {
    let source = |in_cli: bool, in_file: bool| match (in_cli, in_file) {
        (true, _) => "command line",
        (false, true) => "config file",
        (false, false) => "default",
    };
    let config = resolve(cli.clone(), file.options.clone());
    let output = toml::Value::from(config.output_dir.display().to_string());
    let lines = [
        (
            "output",
            output.to_string(),
            source(cli.output.is_some(), file.options.output.is_some()),
        ),
        (
            "max-peers",
            config.max_peers.to_string(),
            source(cli.max_peers.is_some(), file.options.max_peers.is_some()),
        ),
        (
            "max-connections",
            config.max_connections.to_string(),
            source(
                cli.max_connections.is_some(),
                file.options.max_connections.is_some(),
            ),
        ),
        (
            "port",
            config.listen_port.to_string(),
            source(cli.port.is_some(), file.options.port.is_some()),
        ),
        (
            "seed-ratio",
            toml::Value::from(config.seed.ratio).to_string(),
            source(cli.seed_ratio.is_some(), file.options.seed_ratio.is_some()),
        ),
        (
            "seed-time",
            (config.seed.time.as_secs() / 60).to_string(),
            source(cli.seed_time.is_some(), file.options.seed_time.is_some()),
        ),
        (
            "dht",
            config.dht.to_string(),
            source(cli.dht.is_some(), file.options.dht.is_some()),
        ),
        (
            "max-download-rate",
            config.max_download_rate.to_string(),
            source(
                cli.max_download_rate.is_some(),
                file.options.max_download_rate.is_some(),
            ),
        ),
        (
            "max-upload-rate",
            config.max_upload_rate.to_string(),
            source(
                cli.max_upload_rate.is_some(),
                file.options.max_upload_rate.is_some(),
            ),
        ),
    ];

    let header = match &file.path {
        Some(path) => format!("# config file: {}\n", path.display()),
        None => "# no config file\n".to_string(),
    };
    let settings = lines
        .into_iter()
        .map(|(key, value, source)| format!("{key} = {value}  # {source}\n"));
    std::iter::once(header).chain(settings).collect()
}

#[cfg(test)]
mod tests;
