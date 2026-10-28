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
    assert_eq!(config.max_connections, DEFAULT_MAX_CONNECTIONS);
    assert_eq!(config.listen_port, peer::DEFAULT_LISTEN_PORT);
    assert_eq!(config.seed, SeedLimits::default());
    assert!(!config.seed.enabled());
    assert!(config.dht);
    assert_eq!(config.max_download_rate, 0);
    assert_eq!(config.max_upload_rate, 0);
}

#[test]
fn max_connections_comes_from_the_command_line_then_the_file() {
    let file = Options {
        max_connections: Some(50),
        ..Options::default()
    };
    assert_eq!(
        resolve(Options::default(), file.clone()).max_connections,
        50
    );
    let cli = Options {
        max_connections: Some(10),
        ..Options::default()
    };
    assert_eq!(resolve(cli, file).max_connections, 10);
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
                max-download-rate = 500\nmax-upload-rate = 64\ntui = true\n";
    let parsed = parse(text, None).unwrap();
    assert_eq!(
        parsed,
        Options {
            seed_ratio: Some(1.5),
            seed_time: Some(90),
            dht: Some(false),
            max_download_rate: Some(500),
            max_upload_rate: Some(64),
            tui: Some(true),
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
            max_connections: Some(150),
            dht: Some(true),
            ..options(Some("/data"), Some(80), None)
        },
    };
    let text = describe(&cli, &Options::default(), &file);
    assert_eq!(
        text,
        "# config file: /etc/rtrnt.toml\n\
         output = \"/data\"  # config file\n\
         max-peers = 5  # command line\n\
         max-connections = 150  # config file\n\
         port = 6881  # default\n\
         seed-ratio = 1.5  # config file\n\
         seed-time = 0  # default\n\
         dht = false  # command line\n\
         max-download-rate = 0  # default\n\
         max-upload-rate = 64  # command line\n\
         # tui: when stdout and stderr are terminals  # default\n\
         # session: only in the terminal UI  # default\n\
         # watch-dir: none  # default\n\
         watch-dir-delete = false  # default\n"
    );
    // The settings part is valid TOML that parses to the same values.
    let reparsed = parse(&text, None).unwrap();
    assert_eq!(
        reparsed,
        Options {
            max_connections: Some(150),
            seed_ratio: Some(1.5),
            seed_time: Some(0),
            dht: Some(false),
            max_download_rate: Some(0),
            max_upload_rate: Some(64),
            watch_dir_delete: Some(false),
            ..options(Some("/data"), Some(5), Some(6881))
        }
    );
    // A whole ratio is written as a float, and still parses.
    let text = describe(
        &Options::default(),
        &Options::default(),
        &ConfigFile::default(),
    );
    assert!(text.starts_with("# no config file\n"), "{text}");
    assert!(text.contains("seed-ratio = 0.0  # default\n"), "{text}");
    parse(&text, None).unwrap();
    let file = ConfigFile {
        path: None,
        options: Options {
            tui: Some(false),
            ..Options::default()
        },
    };
    let text = describe(&Options::default(), &Options::default(), &file);
    assert!(text.contains("tui = false  # config file\n"), "{text}");
    assert_eq!(parse(&text, None).unwrap().tui, Some(false));
}

#[test]
fn describe_ranks_the_session_between_the_command_line_and_the_file() {
    let cli = Options {
        max_upload_rate: Some(64),
        ..Options::default()
    };
    let session = Options {
        max_download_rate: Some(10),
        max_upload_rate: Some(20),
        ..Options::default()
    };
    let file = ConfigFile {
        path: None,
        options: Options {
            max_download_rate: Some(1),
            max_upload_rate: Some(2),
            session: Some(false),
            ..Options::default()
        },
    };
    let text = describe(&cli, &session, &file);
    assert!(
        text.contains("max-download-rate = 10  # session\n"),
        "{text}"
    );
    assert!(
        text.contains("max-upload-rate = 64  # command line\n"),
        "{text}"
    );
    assert!(text.contains("session = false  # config file\n"), "{text}");
    let none = describe(&Options::default(), &Options::default(), &file);
    assert!(
        none.contains("max-download-rate = 1  # config file\n"),
        "{none}"
    );
}

#[test]
fn the_session_setting_is_read_from_the_file() {
    assert_eq!(
        parse("session = false\n", None).unwrap().session,
        Some(false)
    );
}

#[test]
fn the_watch_directory_comes_from_the_command_line_or_the_file() {
    let file = parse(
        "watch-dir = \"~/incoming\"\nwatch-dir-delete = true\n",
        Some(Path::new("/home/me")),
    )
    .unwrap();
    assert_eq!(file.watch_dir, Some(PathBuf::from("/home/me/incoming")));
    assert_eq!(file.watch_dir_delete, Some(true));

    let cli = Options {
        watch_dir: Some(PathBuf::from("/drop")),
        ..Options::default()
    };
    let config = ConfigFile {
        path: None,
        options: file,
    };
    let text = describe(&cli, &Options::default(), &config);
    assert!(
        text.contains("watch-dir = \"/drop\"  # command line\n"),
        "{text}"
    );
    assert!(
        text.contains("watch-dir-delete = true  # config file\n"),
        "{text}"
    );
    let text = describe(&Options::default(), &Options::default(), &config);
    assert!(
        text.contains("watch-dir = \"/home/me/incoming\"  # config file\n"),
        "{text}"
    );
    // What is printed reads back.
    assert_eq!(
        parse(&text, None).unwrap().watch_dir,
        Some(PathBuf::from("/home/me/incoming"))
    );
}
