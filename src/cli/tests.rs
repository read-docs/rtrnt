use super::*;
use clap::CommandFactory;
use rtrnt::peer;
use rtrnt::selection::Priority;
use std::path::Path;

#[test]
fn cli_is_valid() {
    Args::command().debug_assert();
}

#[test]
fn takes_a_torrent_file_or_a_magnet_link() {
    let hash = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
    let args = Args::try_parse_from(["rtrnt", "file.torrent"]).unwrap();
    assert!(
        matches!(&args.torrents[..], [Source::File(path)] if path == Path::new("file.torrent"))
    );
    let link = format!("magnet:?xt=urn:btih:{hash}&tr=udp%3A%2F%2Ft%3A1");
    let args = Args::try_parse_from(["rtrnt", &link, "--scrape"]).unwrap();
    let [Source::Magnet(link)] = &args.torrents[..] else {
        panic!("not parsed as a magnet link");
    };
    assert_eq!(link.trackers, ["udp://t:1"]);
    assert_eq!(args.torrents[0].label(), hash);
    let err = Args::try_parse_from(["rtrnt", "magnet:?dn=x"])
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("no info hash"), "{err}");
}

#[test]
fn takes_several_torrents() {
    let hash = "c12fe1c06bba254a9dc9f519b335aa7c1367a88a";
    let link = format!("magnet:?xt=urn:btih:{hash}&dn=Some+Name");
    let args =
        Args::try_parse_from(["rtrnt", "dir/a.torrent", &link, "b.torrent", "-o", "x"]).unwrap();
    let labels: Vec<_> = args.torrents.iter().map(Source::label).collect();
    assert_eq!(labels, ["a.torrent", "Some Name", "b.torrent"]);
}

/// Only the UI, where `a` adds one, starts without a torrent, and
/// --print-config needs none.
#[test]
fn no_torrent_is_accepted_only_with_the_ui() {
    // Whether stdout and stderr are terminals, and the config file's `tui`.
    let accepted = |args: &[&str], terminals: bool, file_tui: Option<bool>| {
        let args = Args::try_parse_from(args).unwrap();
        let use_tui = args.use_tui(file_tui, || terminals);
        args.check_torrents(use_tui, false).is_ok()
    };
    assert!(accepted(&["rtrnt"], true, None));
    assert!(accepted(&["rtrnt", "--tui"], false, Some(false)));
    assert!(accepted(&["rtrnt"], false, Some(true)));
    assert!(!accepted(&["rtrnt"], false, None));
    assert!(!accepted(&["rtrnt", "--no-tui"], true, None));
    assert!(!accepted(&["rtrnt"], true, Some(false)));
    assert!(!accepted(&["rtrnt", "--tui", "--check"], true, None));
    assert!(!accepted(&["rtrnt", "--scrape"], true, None));
    assert!(accepted(&["rtrnt", "--print-config"], false, None));
    assert!(accepted(&["rtrnt", "x.torrent", "--no-tui"], false, None));
    let args = Args::try_parse_from(["rtrnt", "--no-tui"]).unwrap();
    // A watch directory, from the command line or the file, brings them.
    assert!(args.check_torrents(false, true).is_ok());
    let args = Args::try_parse_from(["rtrnt", "--no-tui", "--check"]).unwrap();
    assert!(args.check_torrents(false, true).is_err());
    let args = Args::try_parse_from(["rtrnt", "--no-tui"]).unwrap();
    let error = args.check_torrents(false, false).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
}

#[test]
fn the_watch_directory_flags() {
    let args = Args::try_parse_from(["rtrnt", "--watch-dir", "/drop"]).unwrap();
    assert_eq!(args.watch_dir, Some(PathBuf::from("/drop")));
    assert_eq!(args.watch_dir_delete(), None);
    let args =
        Args::try_parse_from(["rtrnt", "--watch-dir", "/drop", "--watch-dir-delete"]).unwrap();
    assert_eq!(args.watch_dir_delete(), Some(true));
}

/// The defaults are written into the help by hand, since clap does not
/// know them; this keeps the help in step with `rtrnt::config`.
#[test]
fn help_shows_the_defaults() {
    let command = Args::command();
    let defaults = [
        ("output", config::DEFAULT_OUTPUT_DIR.to_string()),
        ("max_peers", config::DEFAULT_MAX_PEERS.to_string()),
        (
            "max_connections",
            config::DEFAULT_MAX_CONNECTIONS.to_string(),
        ),
        ("port", peer::DEFAULT_LISTEN_PORT.to_string()),
        ("seed_ratio", config::DEFAULT_SEED_RATIO.to_string()),
        ("seed_time", config::DEFAULT_SEED_TIME.to_string()),
        (
            "max_download_rate",
            config::DEFAULT_MAX_DOWNLOAD_RATE.to_string(),
        ),
        (
            "max_upload_rate",
            config::DEFAULT_MAX_UPLOAD_RATE.to_string(),
        ),
    ];
    for (id, default) in defaults {
        let help = command
            .get_arguments()
            .find(|arg| arg.get_id() == id)
            .and_then(|arg| arg.get_help())
            .map(ToString::to_string)
            .unwrap_or_default();
        assert!(
            help.ends_with(&format!("[default: {default}]")),
            "{id}: {help}"
        );
    }
    // `--dht` says the DHT is on by default.
    const { assert!(config::DEFAULT_DHT) };
    // The UI's default depends on the terminal, which the help says.
    let tui = command
        .get_arguments()
        .find(|arg| arg.get_id() == "tui")
        .and_then(|arg| arg.get_help())
        .map(ToString::to_string)
        .unwrap_or_default();
    assert!(
        tui.ends_with("[default: when stdout and stderr are terminals]"),
        "{tui}"
    );
}

/// Without `--tui` or `--no-tui` the config file decides; with both,
/// the last one wins.
#[test]
fn tui_flags_fall_back_to_the_file() {
    let tui = |flags: &[&str]| {
        let args = ["rtrnt", "x.torrent"].iter().chain(flags);
        Args::try_parse_from(args).unwrap().tui()
    };
    assert_eq!(tui(&[]), None);
    assert_eq!(tui(&["--no-tui"]), Some(false));
    assert_eq!(tui(&["--tui"]), Some(true));
    assert_eq!(tui(&["--tui", "--no-tui"]), Some(false));
    assert_eq!(tui(&["--no-tui", "--tui"]), Some(true));
}

/// Without `--dht` or `--no-dht` the config file decides; with both,
/// the last one wins.
#[test]
fn dht_flags_fall_back_to_the_file() {
    let dht = |flags: &[&str]| {
        let args = ["rtrnt", "x.torrent"].iter().chain(flags);
        Args::try_parse_from(args).unwrap().dht()
    };
    assert_eq!(dht(&[]), None);
    assert_eq!(dht(&["--no-dht"]), Some(false));
    assert_eq!(dht(&["--dht"]), Some(true));
    assert_eq!(dht(&["--dht", "--no-dht"]), Some(false));
    assert_eq!(dht(&["--no-dht", "--dht"]), Some(true));
}

#[test]
fn recheck_is_off_unless_asked_for() {
    assert!(
        !Args::try_parse_from(["rtrnt", "x.torrent"])
            .unwrap()
            .recheck
    );
    assert!(
        Args::try_parse_from(["rtrnt", "x.torrent", "--recheck"])
            .unwrap()
            .recheck
    );
}

#[test]
fn seed_and_rate_flags_are_optional() {
    let args = Args::try_parse_from(["rtrnt", "x.torrent"]).unwrap();
    assert_eq!(args.seed_ratio, None);
    assert_eq!(args.seed_time, None);
    assert_eq!(args.max_download_rate, None);
    assert_eq!(args.max_upload_rate, None);
    let args = Args::try_parse_from([
        "rtrnt",
        "x.torrent",
        "--seed-ratio",
        "0",
        "--max-upload-rate",
        "0",
    ])
    .unwrap();
    // An explicit 0 is a setting, which beats the config file.
    assert_eq!(args.seed_ratio, Some(0.0));
    assert_eq!(args.max_upload_rate, Some(0));
    assert!(Args::try_parse_from(["rtrnt", "x.torrent", "--seed-ratio", "-1"]).is_err());
}

#[test]
fn the_session_follows_the_ui_unless_told_otherwise() {
    let args = Args::try_parse_from(["rtrnt", "a.torrent"]).unwrap();
    assert!(args.use_session(None, true));
    assert!(!args.use_session(None, false));
    assert!(!args.use_session(Some(false), true));
    assert!(args.use_session(Some(true), false));
    let args = Args::try_parse_from(["rtrnt", "a.torrent", "--no-session"]).unwrap();
    assert!(!args.use_session(Some(true), true));
    let args = Args::try_parse_from(["rtrnt", "a.torrent", "--check"]).unwrap();
    assert!(!args.use_session(Some(true), true));
}

#[test]
fn select_file_takes_numbers_and_ranges() {
    let args = Args::try_parse_from(["rtrnt", "x.torrent"]).unwrap();
    assert_eq!(args.select_file, None);
    let args = Args::try_parse_from(["rtrnt", "x.torrent", "--select-file", "1,3-5"]).unwrap();
    let picks = args.select_file.unwrap();
    assert_eq!(picks.last(), 5);
    assert_eq!(
        picks.selection(5).priorities(),
        [
            Priority::Normal,
            Priority::Skip,
            Priority::Normal,
            Priority::Normal,
            Priority::Normal
        ]
    );
    for bad in ["0", "3-1", "x", ""] {
        assert!(
            Args::try_parse_from(["rtrnt", "x.torrent", "--select-file", bad]).is_err(),
            "{bad:?}"
        );
    }
}
