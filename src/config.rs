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
    /// Maximum number of peers to download from at once.
    pub max_peers: Option<usize>,
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
mod tests {
    use super::*;
    use std::io::Write;

    fn options(output: Option<&str>, max_peers: Option<usize>, port: Option<u16>) -> Options {
        Options {
            output: output.map(PathBuf::from),
            max_peers,
            port,
            ..Options::default()
        }
    }

    #[test]
    fn defaults_when_nothing_is_set() {
        let config = resolve(Options::default(), Options::default());
        assert_eq!(config.output_dir, PathBuf::from(DEFAULT_OUTPUT_DIR));
        assert_eq!(config.max_peers, DEFAULT_MAX_PEERS);
        assert_eq!(config.listen_port, peer::DEFAULT_LISTEN_PORT);
        assert_eq!(config.seed, SeedLimits::default());
        assert!(!config.seed.enabled());
        assert!(config.dht);
        assert_eq!(config.max_download_rate, 0);
        assert_eq!(config.max_upload_rate, 0);
    }

    #[test]
    fn file_overrides_defaults() {
        let file = options(Some("/data"), Some(80), Some(7000));
        let config = resolve(Options::default(), file);
        assert_eq!(config.output_dir, PathBuf::from("/data"));
        assert_eq!(config.max_peers, 80);
        assert_eq!(config.listen_port, 7000);
    }

    #[test]
    fn cli_overrides_file_per_setting() {
        let cli = options(None, Some(5), Some(0));
        let file = options(Some("/data"), Some(80), Some(7000));
        let config = resolve(cli, file);
        assert_eq!(config.output_dir, PathBuf::from("/data"));
        assert_eq!(config.max_peers, 5);
        // An explicit 0 is a setting, not "unset".
        assert_eq!(config.listen_port, 0);
    }

    #[test]
    fn parses_every_key() {
        let text = "output = \"/srv/torrents\"\nmax-peers = 50\nport = 51413\n";
        let parsed = parse(text, None).unwrap();
        assert_eq!(
            parsed,
            options(Some("/srv/torrents"), Some(50), Some(51413))
        );
    }

    #[test]
    fn parses_seeding_dht_and_rate_keys() {
        let text = "seed-ratio = 1.5\nseed-time = 90\ndht = false\n\
                    max-download-rate = 500\nmax-upload-rate = 64\n";
        let parsed = parse(text, None).unwrap();
        assert_eq!(
            parsed,
            Options {
                seed_ratio: Some(1.5),
                seed_time: Some(90),
                dht: Some(false),
                max_download_rate: Some(500),
                max_upload_rate: Some(64),
                ..Options::default()
            }
        );
        let config = resolve(Options::default(), parsed);
        assert_eq!(
            config.seed,
            SeedLimits {
                ratio: 1.5,
                time: Duration::from_secs(90 * 60),
            }
        );
        assert!(!config.dht);
        assert_eq!(config.max_download_rate, 500);
        assert_eq!(config.max_upload_rate, 64);
        // A whole ratio needs no decimal point.
        assert_eq!(
            parse("seed-ratio = 2\n", None).unwrap().seed_ratio,
            Some(2.0)
        );
    }

    #[test]
    fn invalid_values_of_the_new_keys_are_errors() {
        for bad in ["-1.0", "-1", "nan", "inf", "\"2\""] {
            let err = parse(&format!("seed-ratio = {bad}\n"), None).unwrap_err();
            assert!(format!("{err:#}").contains("seed-ratio"), "{bad}: {err:#}");
        }
        assert!(parse("seed-time = -5\n", None).is_err());
        assert!(parse("dht = \"no\"\n", None).is_err());
        assert!(parse("max-upload-rate = 1.5\n", None).is_err());
        assert!(parse("max-download-rate = -1\n", None).is_err());
    }

    #[test]
    fn cli_overrides_file_for_seeding_dht_and_rates() {
        let file = Options {
            seed_ratio: Some(2.0),
            seed_time: Some(30),
            dht: Some(false),
            max_download_rate: Some(100),
            ..Options::default()
        };
        // Flags not given fall back to the file.
        let config = resolve(Options::default(), file.clone());
        assert!(!config.dht);
        assert_eq!(config.seed.ratio, 2.0);
        assert_eq!(config.max_download_rate, 100);
        // Given ones win, even when they say "off" or "none".
        let cli = Options {
            seed_ratio: Some(0.0),
            dht: Some(true),
            max_download_rate: Some(0),
            ..Options::default()
        };
        let config = resolve(cli, file);
        assert!(config.dht);
        assert_eq!(config.seed.ratio, 0.0);
        assert_eq!(config.seed.time, Duration::from_secs(30 * 60));
        assert_eq!(config.max_download_rate, 0);
    }

    #[test]
    fn empty_file_sets_nothing() {
        assert_eq!(parse("", None).unwrap(), Options::default());
        assert_eq!(
            parse("# only a comment\n", None).unwrap(),
            Options::default()
        );
    }

    #[test]
    fn unknown_key_is_an_error() {
        let err = parse("max_peers = 50\n", None).unwrap_err();
        assert!(format!("{err:#}").contains("max_peers"), "{err:#}");
    }

    #[test]
    fn wrong_type_is_an_error() {
        assert!(parse("max-peers = \"many\"\n", None).is_err());
        assert!(parse("port = 70000\n", None).is_err());
        assert!(parse("port = -1\n", None).is_err());
        assert!(parse("output = 3\n", None).is_err());
    }

    #[test]
    fn expands_leading_tilde() {
        let home = Path::new("/home/me");
        let parsed = parse("output = \"~/Downloads\"\n", Some(home)).unwrap();
        assert_eq!(parsed.output, Some(PathBuf::from("/home/me/Downloads")));
        let parsed = parse("output = \"~\"\n", Some(home)).unwrap();
        assert_eq!(parsed.output, Some(PathBuf::from("/home/me")));
        // Only a whole leading `~` component is expanded.
        for literal in ["~other/x", "a/~/b", "/~"] {
            let parsed = parse(&format!("output = \"{literal}\"\n"), Some(home)).unwrap();
            assert_eq!(parsed.output, Some(PathBuf::from(literal)));
        }
        assert!(parse("output = \"~/x\"\n", None).is_err());
    }

    #[test]
    fn default_path_prefers_xdg_config_home() {
        let home = Some(Path::new("/home/me"));
        assert_eq!(
            default_path(Some("/xdg".into()), home),
            Some(PathBuf::from("/xdg/rtrnt/config.toml"))
        );
        let fallback = Some(PathBuf::from("/home/me/.config/rtrnt/config.toml"));
        assert_eq!(default_path(None, home), fallback);
        assert_eq!(default_path(Some("".into()), home), fallback);
        assert_eq!(default_path(Some("relative".into()), home), fallback);
        assert_eq!(default_path(None, None), None);
    }

    #[test]
    fn load_reads_the_given_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"max-peers = 12\n").unwrap();
        let loaded = load(Some(file.path())).unwrap();
        assert_eq!(loaded.path.as_deref(), Some(file.path()));
        assert_eq!(loaded.options, options(None, Some(12), None));
    }

    #[test]
    fn load_errors_name_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.toml");
        let err = load(Some(&missing)).unwrap_err();
        assert!(
            format!("{err:#}").contains(&*missing.to_string_lossy()),
            "{err:#}"
        );

        let bad = dir.path().join("bad.toml");
        std::fs::write(&bad, "max-peer = 1\n").unwrap();
        let err = format!("{:#}", load(Some(&bad)).unwrap_err());
        assert!(err.contains(&*bad.to_string_lossy()), "{err}");
        assert!(err.contains("max-peer"), "{err}");
    }

    #[test]
    fn describe_names_each_source() {
        let cli = Options {
            dht: Some(false),
            max_upload_rate: Some(64),
            ..options(None, Some(5), None)
        };
        let file = ConfigFile {
            path: Some(PathBuf::from("/etc/rtrnt.toml")),
            options: Options {
                seed_ratio: Some(1.5),
                dht: Some(true),
                ..options(Some("/data"), Some(80), None)
            },
        };
        let text = describe(&cli, &file);
        assert_eq!(
            text,
            "# config file: /etc/rtrnt.toml\n\
             output = \"/data\"  # config file\n\
             max-peers = 5  # command line\n\
             port = 6881  # default\n\
             seed-ratio = 1.5  # config file\n\
             seed-time = 0  # default\n\
             dht = false  # command line\n\
             max-download-rate = 0  # default\n\
             max-upload-rate = 64  # command line\n"
        );
        // The settings part is valid TOML that parses to the same values.
        let reparsed = parse(&text, None).unwrap();
        assert_eq!(
            reparsed,
            Options {
                seed_ratio: Some(1.5),
                seed_time: Some(0),
                dht: Some(false),
                max_download_rate: Some(0),
                max_upload_rate: Some(64),
                ..options(Some("/data"), Some(5), Some(6881))
            }
        );
        // A whole ratio is written as a float, and still parses.
        let text = describe(&Options::default(), &ConfigFile::default());
        assert!(text.starts_with("# no config file\n"), "{text}");
        assert!(text.contains("seed-ratio = 0.0  # default\n"), "{text}");
        parse(&text, None).unwrap();
    }
}
