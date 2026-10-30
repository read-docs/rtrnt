use super::*;

fn feed(name: &str) -> Url {
    Url::parse(&format!("https://{name}.example/rss")).unwrap()
}

#[test]
fn an_item_is_seen_once_inserted_and_only_for_its_feed() {
    let mut seen = Seen::new(None);
    assert!(!seen.contains(&feed("a"), "1"));
    seen.insert(&feed("a"), "1");
    assert!(seen.contains(&feed("a"), "1"));
    assert!(!seen.contains(&feed("b"), "1"), "another feed's item 1");
    assert!(!seen.contains(&feed("a"), "2"));
    // Inserting it again changes nothing.
    seen.insert(&feed("a"), "1");
    assert_eq!(seen.len(), 1);
}

#[test]
fn the_feed_and_id_do_not_run_together() {
    let mut seen = Seen::new(None);
    let a = Url::parse("https://a.example/x").unwrap();
    seen.insert(&a, "yz");
    let b = Url::parse("https://a.example/xy").unwrap();
    assert!(!seen.contains(&b, "z"));
}

#[test]
fn it_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state").join("rss-seen.json");
    let mut seen = Seen::new(Some(path.clone()));
    seen.insert(&feed("a"), "one");
    seen.insert(&feed("a"), "two");
    seen.save().unwrap();

    let mut again = Seen::load(Some(path.clone()));
    assert_eq!(again.len(), 2);
    assert!(again.contains(&feed("a"), "one"));
    assert!(again.contains(&feed("a"), "two"));
    assert!(!again.contains(&feed("a"), "three"));

    // Nothing changed, so nothing is written.
    std::fs::remove_file(&path).unwrap();
    again.save().unwrap();
    assert!(!path.exists());
    again.insert(&feed("a"), "three");
    again.save().unwrap();
    assert!(path.exists());
}

#[test]
fn the_oldest_items_are_forgotten_past_the_limit() {
    let mut seen = Seen::new(None).with_limit(3);
    for id in ["1", "2", "3", "4", "5"] {
        seen.insert(&feed("a"), id);
    }
    assert_eq!(seen.len(), 3);
    assert!(!seen.contains(&feed("a"), "1"));
    assert!(!seen.contains(&feed("a"), "2"));
    for id in ["3", "4", "5"] {
        assert!(seen.contains(&feed("a"), id));
    }
}

#[test]
fn the_limit_holds_across_a_restart_and_in_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rss-seen.json");
    let mut seen = Seen::new(Some(path.clone())).with_limit(2);
    for id in ["1", "2", "3"] {
        seen.insert(&feed("a"), id);
    }
    seen.save().unwrap();
    let stored: Stored = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(stored.seen.len(), 2);

    // A file with more than the limit (written by a build with a higher
    // one) is cut to the newest.
    let mut big = Seen::new(Some(path.clone())).with_limit(10);
    for id in ["4", "5", "6"] {
        big.insert(&feed("a"), id);
    }
    big.save().unwrap();
    let small = Seen::load(Some(path)).with_limit(2);
    assert_eq!(small.len(), 2);
    assert!(small.contains(&feed("a"), "6"));
    assert!(!small.contains(&feed("a"), "3"));
}

#[test]
fn the_file_stays_small_whatever_the_ids_are() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rss-seen.json");
    let mut seen = Seen::new(Some(path.clone()));
    seen.insert(&feed("a"), &"x".repeat(100_000));
    seen.save().unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() < 200);
}

#[test]
fn a_missing_file_is_an_empty_cache() {
    let dir = tempfile::tempdir().unwrap();
    let seen = Seen::load(Some(dir.path().join("nope.json")));
    assert!(seen.is_empty());
}

#[test]
fn a_file_that_cannot_be_used_is_set_aside() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rss-seen.json");
    std::fs::write(&path, b"not json").unwrap();
    let mut seen = Seen::load(Some(path.clone()));
    assert!(seen.is_empty());
    assert!(!path.exists());
    assert_eq!(
        std::fs::read(dir.path().join("rss-seen.json.bad")).unwrap(),
        b"not json"
    );
    // The next save writes a new one.
    seen.insert(&feed("a"), "1");
    seen.save().unwrap();
    assert!(Seen::load(Some(path)).contains(&feed("a"), "1"));
}

#[test]
fn a_file_of_another_version_is_set_aside() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rss-seen.json");
    std::fs::write(&path, br#"{"version":99,"seen":["abc"]}"#).unwrap();
    assert!(Seen::load(Some(path.clone())).is_empty());
    assert!(dir.path().join("rss-seen.json.bad").exists());
}

#[test]
fn without_a_file_saving_does_nothing() {
    let mut seen = Seen::new(None);
    seen.insert(&feed("a"), "1");
    seen.save().unwrap();
}
