use super::*;

pub(super) fn request() -> AnnounceRequest {
    AnnounceRequest {
        info_hash: [0xab; 20],
        peer_id: *b"-RT0100-abcdefghijkl",
        port: 6881,
        uploaded: 1,
        downloaded: 2,
        left: 3,
        event: AnnounceEvent::Started,
    }
}

fn tiers(tiers: &[&[&str]]) -> Tiers {
    Tiers(
        tiers
            .iter()
            .map(|tier| tier.iter().map(|t| t.to_string()).collect())
            .collect(),
    )
}

/// Announces `event` with canned answers: a tracker listed in `answers`
/// returns that many peers, any other fails. Also returns the trackers
/// asked, in order.
async fn announce_fake(
    tiers: &mut Tiers,
    event: AnnounceEvent,
    answers: &[(&str, usize)],
) -> (Result<AnnounceResponse>, Vec<String>) {
    let mut asked = Vec::new();
    let request = AnnounceRequest { event, ..request() };
    let result = tiers
        .announce_with(
            &request,
            async |tracker: &str, _: &AnnounceRequest| -> Result<AnnounceResponse> {
                asked.push(tracker.to_string());
                let &(_, peers) = answers
                    .iter()
                    .find(|(t, _)| *t == tracker)
                    .ok_or_else(|| anyhow!("tracker down"))?;
                Ok(AnnounceResponse {
                    peers: vec!["1.2.3.4:80".parse().unwrap(); peers],
                    interval: None,
                    min_interval: None,
                    seeders: None,
                    leechers: None,
                })
            },
            |_, _| {},
        )
        .await;
    (result, asked)
}

#[tokio::test]
async fn the_tracker_that_answers_moves_to_the_front_of_its_tier() {
    let mut t = tiers(&[&["a", "b", "c"], &["d"]]);
    let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[("c", 1)]).await;
    assert_eq!(result.unwrap().peers.len(), 1);
    assert_eq!(asked, ["a", "b", "c"]);
    assert_eq!(t, tiers(&[&["c", "a", "b"], &["d"]]));

    let (_, asked) = announce_fake(&mut t, AnnounceEvent::Regular, &[("c", 1)]).await;
    assert_eq!(asked, ["c"]);
}

#[tokio::test]
async fn later_tiers_are_tried_when_a_tier_fails() {
    let mut t = tiers(&[&["a", "b"], &["c", "d"]]);
    let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[("d", 2)]).await;
    assert_eq!(result.unwrap().peers.len(), 2);
    assert_eq!(asked, ["a", "b", "c", "d"]);
    assert_eq!(t, tiers(&[&["a", "b"], &["d", "c"]]));
}

#[tokio::test]
async fn trackers_without_peers_are_skipped_only_when_looking_for_peers() {
    let answers = [("b", 0), ("c", 3)];
    let mut t = tiers(&[&["a", "b", "c"]]);
    let (result, asked) = announce_fake(&mut t, AnnounceEvent::Regular, &answers).await;
    assert_eq!(result.unwrap().peers.len(), 3);
    assert_eq!(asked, ["a", "b", "c"]);
    assert_eq!(t, tiers(&[&["c", "a", "b"]]));

    let mut t = tiers(&[&["a", "b", "c"]]);
    let (result, asked) = announce_fake(&mut t, AnnounceEvent::Stopped, &answers).await;
    assert!(result.unwrap().peers.is_empty());
    assert_eq!(asked, ["a", "b"]);
    assert_eq!(t, tiers(&[&["b", "a", "c"]]));

    // No tracker has peers: the first one that answered wins.
    let mut t = tiers(&[&["a"], &["b", "c"]]);
    let (result, _) = announce_fake(&mut t, AnnounceEvent::Started, &[("c", 0), ("a", 0)]).await;
    assert!(result.unwrap().peers.is_empty());
    assert_eq!(t, tiers(&[&["a"], &["b", "c"]]));
}

#[tokio::test]
async fn fails_when_no_tracker_answers() {
    let mut t = tiers(&[&["a"], &["b"]]);
    let (result, asked) = announce_fake(&mut t, AnnounceEvent::Started, &[]).await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("every tracker failed")
    );
    assert_eq!(asked, ["a", "b"]);
    assert_eq!(t, tiers(&[&["a"], &["b"]]));

    for mut empty in [tiers(&[]), tiers(&[&[], &[]])] {
        let (result, asked) = announce_fake(&mut empty, AnnounceEvent::Started, &[]).await;
        assert!(result.unwrap_err().to_string().contains("no trackers"));
        assert!(asked.is_empty());
    }
}

#[test]
fn new_shuffles_within_tiers_only() {
    let original = tiers(&[&["a", "b", "c"], &["d", "e"]]);
    let mut orders = std::collections::HashSet::new();
    for _ in 0..100 {
        let Tiers(mut shuffled) = Tiers::new(original.0.clone());
        orders.insert(shuffled[0].clone());
        shuffled.iter_mut().for_each(|tier| tier.sort());
        assert_eq!(Tiers(shuffled), original);
    }
    assert!(orders.len() > 1, "tiers were never shuffled");
}

#[test]
fn compact_peers() {
    let peers = parse_compact_v4(&[127, 0, 0, 1, 0x1a, 0xe1, 10, 0, 0, 2, 0, 80, 9]);
    assert_eq!(
        peers,
        [
            "127.0.0.1:6881".parse().unwrap(),
            "10.0.0.2:80".parse().unwrap()
        ]
    );
    let mut v6 = vec![0u8; 16];
    v6[15] = 1;
    v6.extend_from_slice(&[0, 80]);
    assert_eq!(parse_compact_v6(&v6), ["[::1]:80".parse().unwrap()]);
}

#[test]
fn tracker_scrape_display() {
    let line = |result| {
        TrackerScrape {
            tier: 2,
            tracker: "udp://t:1".into(),
            result,
        }
        .to_string()
    };
    let stats = ScrapeStats {
        seeders: 3,
        leechers: 4,
        completed: 5,
    };
    assert_eq!(
        line(Ok(Some(stats))),
        "tier 2  udp://t:1  3 seeders, 4 leechers, 5 completed"
    );
    assert_eq!(
        line(Ok(None)),
        "tier 2  udp://t:1  torrent not known to the tracker"
    );
    assert_eq!(
        line(Err(anyhow!("down"))),
        "tier 2  udp://t:1  failed: down"
    );
}

#[tokio::test]
async fn scrape_rejects_unknown_schemes() {
    for tracker in ["wss://t/announce", "not a url"] {
        assert!(scrape(tracker, &[[0; 20]]).await.is_err(), "{tracker}");
    }
}
