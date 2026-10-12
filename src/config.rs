//! The config file: TOML defaults for the command-line options.
//!
//! Each setting comes from the command line if given there, else from the
//! config file, else from its built-in default. Keys in the file are the
//! long option names, so `--max-peers 50` is `max-peers = 50`.

use crate::download::DownloadConfig;
use crate::peer;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

/// Directory downloads are saved in unless told otherwise.
pub const DEFAULT_OUTPUT_DIR: &str = ".";
/// Peers downloaded from at once unless told otherwise.
pub const DEFAULT_MAX_PEERS: usize = 30;

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
}

impl Options {
    /// Takes each setting from `self`, or from `fallback` where `self`
    /// leaves it unset.
    pub fn or(self, fallback: Options) -> Options {
        Options {
            output: self.output.or(fallback.output),
            max_peers: self.max_peers.or(fallback.max_peers),
            port: self.port.or(fallback.port),
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
        }
    }
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
        }
    }

    #[test]
    fn defaults_when_nothing_is_set() {
        let config = resolve(Options::default(), Options::default());
        assert_eq!(config.output_dir, PathBuf::from(DEFAULT_OUTPUT_DIR));
        assert_eq!(config.max_peers, DEFAULT_MAX_PEERS);
        assert_eq!(config.listen_port, peer::DEFAULT_LISTEN_PORT);
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
        let cli = options(None, Some(5), None);
        let file = ConfigFile {
            path: Some(PathBuf::from("/etc/rtrnt.toml")),
            options: options(Some("/data"), Some(80), None),
        };
        let text = describe(&cli, &file);
        assert_eq!(
            text,
            "# config file: /etc/rtrnt.toml\n\
             output = \"/data\"  # config file\n\
             max-peers = 5  # command line\n\
             port = 6881  # default\n"
        );
        // The settings part is valid TOML that parses to the same values.
        let reparsed = parse(&text, None).unwrap();
        assert_eq!(reparsed, options(Some("/data"), Some(5), Some(6881)));
    }
}
