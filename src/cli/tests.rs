use super::*;
use clap::CommandFactory;
use rtrnt::peer;
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
    assert!(Args::try_parse_from(["rtrnt"]).is_err());
    let args = Args::try_parse_from(["rtrnt", "--print-config"]).unwrap();
    assert!(args.torrents.is_empty());
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
