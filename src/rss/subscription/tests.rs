use super::*;

fn filter(patterns: &[&str], exclude: Option<&str>) -> Filter {
    let patterns: Vec<String> = patterns.iter().map(|p| (*p).to_string()).collect();
    Filter::new(&patterns, exclude).unwrap()
}

#[test]
fn a_title_is_taken_if_any_pattern_matches() {
    let filter = filter(&[r"^Show A\b", r"Show B .*1080p"], None);
    assert!(filter.accepts("Show A S01E01"));
    assert!(filter.accepts("Show B S02E03 1080p WEB"));
    assert!(!filter.accepts("Show B S02E03 720p"));
    assert!(!filter.accepts("Another Show A S01E01"), "anchored");
    assert!(!filter.accepts(""));
}

#[test]
fn matching_ignores_case_unless_the_pattern_says_not_to() {
    assert!(filter(&["show a"], None).accepts("SHOW A S01"));
    assert!(!filter(&["(?-i)show a"], None).accepts("SHOW A S01"));
}

#[test]
fn the_exclude_pattern_vetoes_a_match() {
    let filter = filter(&["Show A"], Some(r"\b(CAM|HDTS)\b"));
    assert!(filter.accepts("Show A 1080p"));
    assert!(!filter.accepts("Show A CAM"));
    assert!(!filter.accepts("Show A hdts"));
    // The exclude pattern alone takes nothing.
    assert!(!filter.accepts("Other CAM"));
    assert!(!filter.accepts("Other"));
}

#[test]
fn a_filter_needs_a_pattern_and_valid_ones() {
    assert!(Filter::new(&[], None).is_err());
    let bad = Filter::new(&["fine".to_string(), "(unclosed".to_string()], None).unwrap_err();
    assert!(bad.starts_with("invalid `match`"), "{bad}");
    let bad = Filter::new(&["fine".to_string()], Some("[")).unwrap_err();
    assert!(bad.starts_with("invalid `exclude`"), "{bad}");
}

fn parse(text: &str) -> Result<Vec<Subscription>, String> {
    #[derive(Deserialize)]
    struct File {
        rss: Vec<Subscription>,
    }
    toml::from_str::<File>(text)
        .map(|file| file.rss)
        .map_err(|e| e.to_string())
}

#[test]
fn an_entry_has_defaults() {
    let feeds = parse(
        r#"
        [[rss]]
        url = "https://example.org/feed.xml"
        match = ["Show"]
        "#,
    )
    .unwrap();
    assert_eq!(feeds.len(), 1);
    assert_eq!(feeds[0].url.as_str(), "https://example.org/feed.xml");
    assert_eq!(
        feeds[0].interval,
        Duration::from_secs(DEFAULT_INTERVAL_MINUTES * 60)
    );
    assert!(!feeds[0].paused);
    assert_eq!(feeds[0].filter.exclude(), None);
}

#[test]
fn an_entry_reads_every_key_and_there_can_be_several() {
    let feeds = parse(
        r#"
        [[rss]]
        url = "http://a.example/rss"
        interval = 30
        match = ["one", "two"]
        exclude = "three"
        paused = true

        [[rss]]
        url = "https://b.example/atom"
        match = ["x"]
        "#,
    )
    .unwrap();
    assert_eq!(feeds.len(), 2);
    assert_eq!(feeds[0].interval, Duration::from_secs(1800));
    assert_eq!(feeds[0].filter.patterns(), ["one", "two"]);
    assert_eq!(feeds[0].filter.exclude(), Some("three"));
    assert!(feeds[0].paused);
    assert_eq!(feeds[1].url.host_str(), Some("b.example"));
}

#[test]
fn bad_entries_name_the_problem() {
    let bad = |body: &str| parse(&format!("[[rss]]\n{body}")).unwrap_err();
    let e = bad("url = \"ftp://x.example/f\"\nmatch = [\"a\"]");
    assert!(e.contains("not an http or https URL"), "{e}");
    let e = bad("url = \"nonsense\"\nmatch = [\"a\"]");
    assert!(e.contains("invalid `url`"), "{e}");
    let e = bad("url = \"https://x.example/\"\nmatch = [\"a\"]\ninterval = 1");
    assert!(e.contains("at least 5"), "{e}");
    let e = bad("url = \"https://x.example/\"\nmatch = []");
    assert!(e.contains("at least one"), "{e}");
    let e = bad("url = \"https://x.example/\"\nmatch = [\"(\"]");
    assert!(e.contains("invalid `match`"), "{e}");
    let e = bad("url = \"https://x.example/\"");
    assert!(e.contains("match"), "{e}");
    let e = bad("match = [\"a\"]");
    assert!(e.contains("url"), "{e}");
    let e = bad("url = \"https://x.example/\"\nmatch = [\"a\"]\ndownload-dir = \"\"");
    assert!(e.contains("download-dir"), "{e}");
}

#[test]
fn the_interval_floor_is_allowed() {
    let feeds = parse(&format!(
        "[[rss]]\nurl = \"https://x.example/\"\nmatch = [\"a\"]\ninterval = {MIN_INTERVAL_MINUTES}"
    ))
    .unwrap();
    assert_eq!(
        feeds[0].interval,
        Duration::from_secs(MIN_INTERVAL_MINUTES * 60)
    );
}

#[test]
fn feeds_are_written_as_the_file_spells_them() {
    let text = r#"
        [[rss]]
        url = "https://a.example/rss?key=1"
        interval = 20
        match = ["one", "t\"wo"]
        exclude = "three"
        paused = true

        [[rss]]
        url = "https://b.example/atom"
        match = ["x"]
    "#;
    let feeds = parse(text).unwrap();
    let written = to_toml(&feeds);
    // It reads back to the same feeds.
    let again = parse(&format!("rss = {written}")).unwrap();
    assert_eq!(again, feeds);
    assert!(written.contains("interval = 15"), "{written}");
}
