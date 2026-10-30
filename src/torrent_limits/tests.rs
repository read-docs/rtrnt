use super::*;

fn global(ratio: f64, minutes: u64) -> SeedLimits {
    SeedLimits {
        ratio,
        time: Duration::from_secs(minutes * 60),
    }
}

#[test]
fn nothing_set_goes_by_the_session() {
    let settings = TorrentSettings::default();
    assert!(settings.is_default());
    assert_eq!(settings.resolve_seed(global(2.0, 30)), global(2.0, 30));
    assert_eq!(
        settings.resolve_seed(SeedLimits::default()),
        SeedLimits::default()
    );
}

#[test]
fn seed_settings_override_one_by_one() {
    let ratio = TorrentSettings {
        seed_ratio_milli: Some(1500),
        ..Default::default()
    };
    assert_eq!(ratio.resolve_seed(global(2.0, 30)), global(1.5, 30));
    let time = TorrentSettings {
        seed_time: Some(90),
        ..Default::default()
    };
    assert_eq!(time.resolve_seed(global(2.0, 30)), global(2.0, 90));
}

#[test]
fn zero_overrides_the_session_with_no_limit() {
    let settings = TorrentSettings {
        seed_ratio_milli: Some(0),
        seed_time: Some(0),
        ..Default::default()
    };
    let seed = settings.resolve_seed(global(2.0, 30));
    assert!(!seed.enabled());
    // Only a ratio set here keeps a torrent seeding that the session would
    // not.
    let ratio = TorrentSettings {
        seed_ratio_milli: Some(1000),
        ..Default::default()
    };
    assert!(ratio.resolve_seed(SeedLimits::default()).enabled());
}

#[test]
fn ratios_show_without_trailing_zeros() {
    assert_eq!(format_ratio(0), "none");
    assert_eq!(format_ratio(1000), "1");
    assert_eq!(format_ratio(1500), "1.5");
    assert_eq!(format_ratio(2005), "2.005");
}

#[tokio::test]
async fn setting_applies_to_the_rate_limits_and_wakes_seed_waiters() {
    let limits = TorrentLimits::default();
    assert_eq!(limits.rates().kib(), (0, 0));
    limits.set(TorrentSettings {
        download: 10,
        upload: 20,
        ..Default::default()
    });
    assert_eq!(limits.rates().kib(), (10, 20));
    assert_eq!(limits.settings().upload, 20);

    let changed = limits.seed_changed();
    let mut changed = std::pin::pin!(changed);
    changed.as_mut().enable();
    // A rate change is not a seed change.
    limits.set(TorrentSettings {
        download: 11,
        upload: 20,
        ..Default::default()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), changed.as_mut())
            .await
            .is_err()
    );
    limits.set(TorrentSettings {
        seed_time: Some(5),
        ..limits.settings()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), changed.as_mut())
            .await
            .is_ok()
    );
    assert_eq!(limits.seed(global(1.0, 0)), global(1.0, 5));
}

#[test]
fn own_settings_win_over_the_base_one_by_one() {
    let own = TorrentSettings {
        download: 10,
        seed_ratio_milli: Some(0),
        ..Default::default()
    };
    let base = TorrentSettings {
        download: 99,
        upload: 77,
        seed_ratio_milli: Some(2000),
        seed_time: Some(60),
    };
    assert_eq!(
        own.over(base),
        TorrentSettings {
            download: 10,
            upload: 77,
            seed_ratio_milli: Some(0),
            seed_time: Some(60),
        }
    );
    assert_eq!(TorrentSettings::default().over(base), base);
    assert_eq!(base.over(TorrentSettings::default()), base);
}

#[tokio::test]
async fn tag_settings_rank_between_the_torrents_own_and_the_sessions() {
    let limits = TorrentLimits::default();
    let tags = TorrentSettings {
        download: 100,
        upload: 50,
        seed_ratio_milli: Some(2000),
        seed_time: None,
    };
    limits.set_tag_settings(tags);
    assert_eq!(limits.rates().kib(), (100, 50));
    assert_eq!(limits.seed(global(1.0, 30)), global(2.0, 30));
    // The torrent's own limit wins over its tag's, where it has one.
    limits.set(TorrentSettings {
        download: 10,
        seed_ratio_milli: Some(0),
        ..Default::default()
    });
    assert_eq!(limits.rates().kib(), (10, 50));
    assert_eq!(limits.seed(global(1.0, 30)), global(0.0, 30));
    // Only the own settings are what the session file keeps.
    assert_eq!(limits.settings().upload, 0);
    assert_eq!(limits.effective().upload, 50);
    // Clearing the torrent's own settings leaves its tag's, and clearing
    // the tag's leaves the session's.
    limits.set(TorrentSettings::default());
    assert_eq!(limits.rates().kib(), (100, 50));
    limits.set_tag_settings(TorrentSettings::default());
    assert_eq!(limits.rates().kib(), (0, 0));
    assert_eq!(limits.seed(global(1.0, 30)), global(1.0, 30));
}

#[tokio::test]
async fn a_tag_change_that_alters_the_seed_limit_wakes_waiters() {
    let limits = TorrentLimits::default();
    let changed = limits.seed_changed();
    let mut changed = std::pin::pin!(changed);
    changed.as_mut().enable();
    limits.set_tag_settings(TorrentSettings {
        seed_time: Some(5),
        ..Default::default()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), changed.as_mut())
            .await
            .is_ok()
    );
}
