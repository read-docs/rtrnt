use super::*;

fn tags(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

#[test]
fn tags_are_trimmed_lowercased_and_deduplicated_in_order() {
    assert_eq!(
        parse_tags(" Linux ,ISOs,, linux , my show "),
        tags(&["linux", "isos", "my show"])
    );
    assert!(parse_tags("").is_empty());
    assert!(parse_tags(" , ,").is_empty());
    assert_eq!(join_tags(&tags(&["a", "b"])), "a, b");
    assert_eq!(
        parse_tags(&join_tags(&tags(&["a", "b c"]))),
        tags(&["a", "b c"])
    );
}

#[test]
fn a_tag_has_no_comma_or_control_character() {
    assert_eq!(normalize_tag("  Tv "), Some("tv".to_string()));
    assert_eq!(normalize_tag("   "), None);
    assert_eq!(normalize_tag("a,b"), None);
    assert_eq!(normalize_tag("a\tb"), None);
}

fn table() -> TagSettings {
    let mut table = TagSettings::new();
    table.insert(
        "private".into(),
        TorrentSettings {
            upload: 100,
            seed_ratio_milli: Some(2000),
            ..Default::default()
        },
    );
    table.insert(
        "slow".into(),
        TorrentSettings {
            download: 50,
            upload: 10,
            seed_time: Some(60),
            ..Default::default()
        },
    );
    table
}

#[test]
fn the_first_tag_that_sets_a_value_wins() {
    let table = table();
    assert_eq!(
        tag_settings(&tags(&["private", "slow"]), &table),
        TorrentSettings {
            download: 50,
            upload: 100,
            seed_ratio_milli: Some(2000),
            seed_time: Some(60),
        }
    );
    assert_eq!(tag_settings(&tags(&["slow", "private"]), &table).upload, 10);
}

#[test]
fn tags_without_a_table_give_nothing() {
    assert_eq!(
        tag_settings(&tags(&["other", "tv"]), &table()),
        TorrentSettings::default()
    );
    assert_eq!(tag_settings(&[], &table()), TorrentSettings::default());
}

#[test]
fn a_torrents_own_settings_win_over_its_tags() {
    let given = tag_settings(&tags(&["private"]), &table());
    let own = TorrentSettings {
        upload: 5,
        seed_ratio_milli: Some(0),
        ..Default::default()
    };
    let effective = own.over(given);
    assert_eq!(effective.upload, 5);
    assert_eq!(effective.seed_ratio_milli, Some(0));
}

#[test]
fn tag_tables_read_from_toml() {
    let table: TagTable = toml::from_str(
        r#"
        private = { upload = 100, seed-ratio = 2 }
        [slow]
        download = 50
        seed-time = 60
        "#,
    )
    .unwrap();
    table.validate().unwrap();
    let settings = table.settings();
    assert_eq!(settings["private"].upload, 100);
    assert_eq!(settings["private"].seed_ratio_milli, Some(2000));
    assert_eq!(settings["slow"].download, 50);
    assert_eq!(settings["slow"].seed_time, Some(60));
    assert_eq!(settings["slow"].seed_ratio_milli, None);
}

#[test]
fn a_bad_tag_table_is_refused() {
    assert!(toml::from_str::<TagTable>("[a]\nbogus = 1").is_err());
    assert!(toml::from_str::<TagTable>("[a]\nseed-ratio = -1").is_err());
    let upper: TagTable = toml::from_str("[Linux]\nupload = 1").unwrap();
    assert!(upper.validate().is_err());
    let comma: TagTable = toml::from_str("[\"a,b\"]\nupload = 1").unwrap();
    assert!(comma.validate().is_err());
}

#[test]
fn a_table_shows_as_toml_that_reads_back() {
    let table: TagTable = toml::from_str("[private]\nupload = 100\nseed-ratio = 2.0").unwrap();
    let text = table.to_toml();
    let wrapper: BTreeMap<String, TagTable> = toml::from_str(&format!("t = {text}")).unwrap();
    assert_eq!(wrapper["t"], table);
}
