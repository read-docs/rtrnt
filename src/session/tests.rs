use super::*;

fn magnet(byte: u8) -> Source {
    let hex: String = [byte; 20].iter().map(|b| format!("{b:02x}")).collect();
    Source::parse(&format!("magnet:?xt=urn:btih:{hex}")).unwrap()
}

fn saved(byte: u8, paused: bool) -> SavedTorrent {
    SavedTorrent {
        info_hash: [byte; 20],
        source: magnet(byte),
        paused,
        sequential: None,
        limits: TorrentSettings::default(),
        tags: Vec::new(),
    }
}

#[test]
fn a_session_round_trips() {
    let link = "magnet:?xt=urn:btih:0101010101010101010101010101010101010101\
                &dn=A+name+%26+more&tr=http%3A%2F%2Ft.example%2Fannounce&x.pe=127.0.0.1%3A6881";
    let session = SavedSession {
        torrents: vec![
            SavedTorrent {
                info_hash: [1; 20],
                source: Source::parse(link).unwrap(),
                paused: true,
                sequential: Some(true),
                limits: TorrentSettings {
                    download: 50,
                    upload: 20,
                    seed_ratio_milli: Some(1500),
                    seed_time: Some(0),
                },
                tags: vec!["linux".into(), "my show".into()],
            },
            SavedTorrent {
                info_hash: [2; 20],
                source: Source::File("/state/torrents/0202.torrent".into()),
                paused: false,
                sequential: Some(false),
                limits: TorrentSettings::default(),
                tags: Vec::new(),
            },
            saved(3, false),
        ],
        rate_limits: Some(SavedLimits {
            download: 100,
            upload: 0,
        }),
        alt_speed: None,
    };
    let decoded = SavedSession::decode(&session.encode().unwrap()).unwrap();
    assert_eq!(decoded, session);
}

#[test]
fn limits_are_only_there_once_set() {
    let session = SavedSession {
        torrents: vec![saved(1, false)],
        rate_limits: None,
        alt_speed: None,
    };
    let bytes = session.encode().unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("rate limits"));
    assert_eq!(SavedSession::decode(&bytes).unwrap().rate_limits, None);
}

#[test]
fn tags_are_only_there_once_set_and_keep_their_order() {
    let mut tagged = saved(1, false);
    tagged.tags = vec!["tv".into(), "archive".into()];
    let session = SavedSession {
        torrents: vec![tagged.clone(), saved(2, false)],
        rate_limits: None,
        alt_speed: None,
    };
    let bytes = session.encode().unwrap();
    // One torrent has tags, so the key is in the file once.
    let text = String::from_utf8_lossy(&bytes);
    assert_eq!(text.matches("4:tags").count(), 1, "{text}");
    let decoded = SavedSession::decode(&bytes).unwrap();
    assert_eq!(decoded.torrents[0].tags, tagged.tags);
    assert!(decoded.torrents[1].tags.is_empty());
}

#[test]
fn a_file_from_before_tags_loads_without_them() {
    // Version 1, one torrent, written by a build that had no tags (and no
    // limits): its entry has only the keys it had then.
    let mut old = b"d8:torrentsld9:info hash20:".to_vec();
    old.extend([1u8; 20]);
    old.extend(b"6:pausedi1e6:source10:/x.torrente");
    old.extend(b"e7:versioni1ee");
    let decoded = SavedSession::decode(&old).unwrap();
    assert_eq!(decoded.torrents.len(), 1);
    assert_eq!(decoded.torrents[0].info_hash, [1; 20]);
    assert!(decoded.torrents[0].paused);
    assert!(decoded.torrents[0].tags.is_empty());
}

#[test]
fn tags_that_make_no_sense_are_cleaned_up_on_loading() {
    let stored = Stored {
        version: VERSION,
        torrents: vec![StoredTorrent {
            info_hash: ByteBuf::from(vec![1; 20]),
            source: "/x.torrent".into(),
            paused: 0,
            sequential: None,
            limits: StoredLimits::default(),
            tags: vec!["Tv".into(), "".into(), "tv".into(), "a,b".into()],
        }],
        rate_limits: None,
        alt_speed: None,
    };
    let decoded = SavedSession::decode(&serde_bencode::to_bytes(&stored).unwrap()).unwrap();
    // "a,b" is two tags once split, as a comma separates them.
    assert_eq!(decoded.torrents[0].tags, ["tv", "a", "b"]);
}

#[test]
fn another_version_is_refused_by_name() {
    let e = SavedSession::decode(b"d8:torrentsle7:versioni2ee").unwrap_err();
    assert!(format!("{e:#}").contains("version 2"), "{e:#}");
    // Even when the rest is in a shape this version does not know.
    let e = SavedSession::decode(b"d8:torrents3:abc7:versioni9ee").unwrap_err();
    assert!(format!("{e:#}").contains("version 9"), "{e:#}");
}

#[test]
fn garbage_is_an_error() {
    assert!(SavedSession::decode(b"not bencode").is_err());
    assert!(SavedSession::decode(b"de").is_err());
}

#[test]
fn an_entry_that_makes_no_sense_is_skipped() {
    let stored = Stored {
        version: VERSION,
        torrents: vec![
            StoredTorrent {
                info_hash: ByteBuf::from(vec![1; 3]),
                source: "/x.torrent".into(),
                paused: 0,
                sequential: None,
                limits: StoredLimits::default(),
                tags: Vec::new(),
            },
            StoredTorrent {
                info_hash: ByteBuf::from(vec![2; 20]),
                source: "magnet:?dn=no-hash".into(),
                paused: 0,
                sequential: None,
                limits: StoredLimits::default(),
                tags: Vec::new(),
            },
            StoredTorrent {
                info_hash: ByteBuf::from(vec![3; 20]),
                source: "/x.torrent".into(),
                paused: 1,
                sequential: None,
                limits: StoredLimits::default(),
                tags: Vec::new(),
            },
        ],
        rate_limits: None,
        alt_speed: None,
    };
    let decoded = SavedSession::decode(&serde_bencode::to_bytes(&stored).unwrap()).unwrap();
    assert_eq!(
        decoded.torrents,
        vec![SavedTorrent {
            info_hash: [3; 20],
            source: Source::File("/x.torrent".into()),
            paused: true,
            sequential: None,
            limits: TorrentSettings::default(),
            tags: Vec::new(),
        }]
    );
}

#[test]
fn the_store_saves_and_loads_and_a_missing_file_is_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("rtrnt"));
    assert_eq!(store.load().unwrap(), SavedSession::default());
    let session = SavedSession {
        torrents: vec![saved(1, true), saved(2, false)],
        rate_limits: Some(SavedLimits {
            download: 5,
            upload: 6,
        }),
        alt_speed: None,
    };
    store.save(&session).unwrap();
    assert_eq!(store.load().unwrap(), session);
}

#[test]
fn a_file_that_cannot_be_used_is_set_aside() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    std::fs::write(store.path(), b"d7:versioni7ee").unwrap();
    assert!(store.load().is_err());
    store.set_aside();
    assert_eq!(store.load().unwrap(), SavedSession::default());
    assert!(dir.path().join("session.bad").exists());
}

#[test]
fn a_torrent_is_copied_into_the_store_and_forgotten() {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let original = dir.path().join("x.torrent");
    std::fs::write(&original, b"torrent bytes").unwrap();

    let copy = store.store_torrent(&original, [0xab; 20]).unwrap();
    assert_eq!(copy, store.torrent_path([0xab; 20]));
    assert!(copy.ends_with("torrents/abababababababababababababababababababab.torrent"));
    assert!(store.holds(&copy));
    assert!(!store.holds(&original));
    assert_eq!(std::fs::read(&copy).unwrap(), b"torrent bytes");

    // The copy outlives the original, and storing it again changes nothing.
    std::fs::remove_file(&original).unwrap();
    assert_eq!(store.store_torrent(&copy, [0xab; 20]).unwrap(), copy);
    assert_eq!(std::fs::read(&copy).unwrap(), b"torrent bytes");

    store.forget_torrent([0xab; 20]).unwrap();
    assert!(!copy.exists());
    store.forget_torrent([0xab; 20]).unwrap();
}

#[test]
fn the_command_line_joins_the_session_without_duplicates() {
    let saved_torrents = vec![saved(1, true), saved(2, false)];
    let cli = vec![
        // Paused in the session: resumed, and not started again.
        (magnet(1), Some([1; 20])),
        // Running in the session already.
        (magnet(2), Some([2; 20])),
        (magnet(3), Some([3; 20])),
        // A file that does not load has no hash, and is left to fail.
        (Source::File("/missing.torrent".into()), None),
    ];
    let (restored, start) = merge(saved_torrents, cli);
    assert_eq!(restored, vec![saved(1, false), saved(2, false)]);
    assert_eq!(
        start,
        vec![magnet(3), Source::File("/missing.torrent".into())]
    );
}

#[tokio::test]
async fn the_hash_of_a_source_is_read_from_its_file() {
    assert_eq!(info_hash_of(&magnet(7)).await, Some([7; 20]));
    assert_eq!(
        info_hash_of(&Source::File("/missing.torrent".into())).await,
        None
    );
}

#[test]
fn the_alt_speed_state_round_trips_and_is_only_there_once_set() {
    let session = SavedSession {
        torrents: vec![saved(1, false)],
        rate_limits: None,
        alt_speed: Some(SavedAltSpeed {
            limits: Some(SavedLimits {
                download: 30,
                upload: 0,
            }),
            manual: Some(Override {
                alt: true,
                scheduled: false,
            }),
        }),
    };
    assert_eq!(
        SavedSession::decode(&session.encode().unwrap()).unwrap(),
        session
    );
    let only_manual = SavedSession {
        alt_speed: Some(SavedAltSpeed {
            limits: None,
            manual: Some(Override {
                alt: false,
                scheduled: true,
            }),
        }),
        ..session.clone()
    };
    assert_eq!(
        SavedSession::decode(&only_manual.encode().unwrap()).unwrap(),
        only_manual
    );
    let none = SavedSession {
        alt_speed: None,
        ..session
    };
    let bytes = none.encode().unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("alt speed"));
    assert_eq!(SavedSession::decode(&bytes).unwrap().alt_speed, None);
}

#[test]
fn a_file_from_before_per_torrent_limits_loads_with_none() {
    // The entry as the first version wrote it: no limits.
    #[derive(Serialize)]
    struct OldTorrent {
        #[serde(rename = "info hash")]
        info_hash: ByteBuf,
        source: String,
        paused: i64,
    }
    #[derive(Serialize)]
    struct Old {
        version: i64,
        torrents: Vec<OldTorrent>,
    }
    let data = serde_bencode::to_bytes(&Old {
        version: 1,
        torrents: vec![OldTorrent {
            info_hash: ByteBuf::from(vec![4; 20]),
            source: "/x.torrent".into(),
            paused: 1,
        }],
    })
    .unwrap();
    let decoded = SavedSession::decode(&data).unwrap();
    assert_eq!(decoded.torrents.len(), 1);
    assert!(decoded.torrents[0].paused);
    assert!(decoded.torrents[0].limits.is_default());
}

#[test]
fn unset_limits_are_not_written() {
    let session = SavedSession {
        torrents: vec![saved(1, false)],
        rate_limits: None,
        alt_speed: None,
    };
    let data = session.encode().unwrap();
    assert!(!data.windows(6).any(|w| w == b"limits"));
}

#[test]
fn a_zero_seed_override_is_kept_apart_from_unset() {
    let mut torrent = saved(1, false);
    torrent.limits.seed_ratio_milli = Some(0);
    let session = SavedSession {
        torrents: vec![torrent],
        rate_limits: None,
        alt_speed: None,
    };
    let decoded = SavedSession::decode(&session.encode().unwrap()).unwrap();
    assert_eq!(decoded.torrents[0].limits.seed_ratio_milli, Some(0));
    assert_eq!(decoded.torrents[0].limits.seed_time, None);
}
