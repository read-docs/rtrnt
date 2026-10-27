use super::*;

fn request() -> AnnounceRequest {
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
                })
            },
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
fn http_response_compact() {
    let mut body = b"d8:intervali1800e12:min intervali60e5:peers6:".to_vec();
    body.extend_from_slice(&[1, 2, 3, 4, 0, 80]);
    body.push(b'e');
    let response = parse_http_response(&body).unwrap();
    assert_eq!(response.interval, Some(1800));
    assert_eq!(response.min_interval, Some(60));
    assert_eq!(response.peers, ["1.2.3.4:80".parse().unwrap()]);
}

#[test]
fn http_response_dict_list() {
    let body = b"d5:peersld2:ip7:1.2.3.47:peer id20:aaaaaaaaaaaaaaaaaaaa4:porti6881eed2:ip8:hostname4:porti1eeee";
    let response = parse_http_response(body).unwrap();
    assert_eq!(response.peers, ["1.2.3.4:6881".parse().unwrap()]);
}

#[test]
fn http_response_failure() {
    let err = parse_http_response(b"d14:failure reason9:not founde").unwrap_err();
    assert!(err.to_string().contains("not found"));
}

#[test]
fn http_url() {
    let url = http_announce_url("http://t/announce", &request());
    assert!(url.starts_with("http://t/announce?info_hash=%ab%ab"));
    assert!(url.contains("&peer_id=%2d%52%54"));
    assert!(url.contains("&port=6881&uploaded=1&downloaded=2&left=3&compact=1"));
    assert!(url.ends_with("&event=started"));
    let url = http_announce_url("http://t/announce?key=1", &request());
    assert!(url.starts_with("http://t/announce?key=1&info_hash="));
}

#[test]
fn http_url_events() {
    let url = |event| http_announce_url("http://t/a", &AnnounceRequest { event, ..request() });
    assert!(!url(AnnounceEvent::Regular).contains("event="));
    assert!(url(AnnounceEvent::Completed).ends_with("&event=completed"));
    assert!(url(AnnounceEvent::Stopped).ends_with("&event=stopped"));
}

#[test]
fn udp_event_codes() {
    // BEP 15: 0 none, 1 completed, 2 started, 3 stopped.
    let codes = [
        AnnounceEvent::Regular,
        AnnounceEvent::Completed,
        AnnounceEvent::Started,
        AnnounceEvent::Stopped,
    ]
    .map(AnnounceEvent::udp_code);
    assert_eq!(codes, [0, 1, 2, 3]);
}

#[test]
fn udp_announce_response() {
    let mut response = vec![0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 7, 8, 0, 0, 0, 0, 0, 0, 0, 0];
    response.extend_from_slice(&[1, 2, 3, 4, 0, 80]);
    let parsed = parse_udp_announce(&response, false).unwrap();
    assert_eq!(parsed.interval, Some(1800));
    assert_eq!(parsed.peers, ["1.2.3.4:80".parse().unwrap()]);
    assert!(parse_udp_announce(&response[..19], false).is_err());
}

#[test]
fn udp_scrape_request_layout() {
    let request = udp_scrape_request(0x0102_0304_0506_0708, &[[0xaa; 20], [0xbb; 20]]);
    assert_eq!(request.len(), 12 + 40);
    assert_eq!(request[..8], [1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(request[8..12], [0, 0, 0, 2]);
    assert_eq!(request[12..32], [0xaa; 20]);
    assert_eq!(request[32..], [0xbb; 20]);
}

#[test]
fn udp_scrape_response() {
    let mut response = vec![0, 0, 0, 2, 9, 9, 9, 9];
    response.extend_from_slice(&[0, 0, 0, 5, 0, 0, 1, 0, 0, 0, 0, 7]);
    response.extend_from_slice(&[0; 12]);
    let stats = parse_udp_scrape(&response, 2).unwrap();
    assert_eq!(
        stats,
        [
            ScrapeStats {
                seeders: 5,
                leechers: 7,
                completed: 256
            },
            ScrapeStats {
                seeders: 0,
                leechers: 0,
                completed: 0
            }
        ]
    );
    // Fewer entries than torrents asked about, or a cut-off entry.
    assert!(parse_udp_scrape(&response, 3).is_err());
    assert!(parse_udp_scrape(&response[..response.len() - 1], 2).is_err());
    assert!(parse_udp_scrape(&response[..4], 1).is_err());
    assert_eq!(parse_udp_scrape(&response[..4], 0).unwrap(), []);
}

#[test]
fn http_scrape_urls() {
    let scrape = |announce: &str| {
        http_scrape_url(&Url::parse(announce).unwrap(), &[[0xab; 20]])
            .map(|url| url.replace(&"%ab".repeat(20), "H"))
            .ok()
    };
    // The examples of BEP 48.
    let cases = [
        (
            "http://example.com/announce",
            Some("http://example.com/scrape?info_hash=H"),
        ),
        (
            "http://example.com/x/announce",
            Some("http://example.com/x/scrape?info_hash=H"),
        ),
        (
            "http://example.com/announce.php",
            Some("http://example.com/scrape.php?info_hash=H"),
        ),
        (
            "http://example.com/announce?x2%0644",
            Some("http://example.com/scrape?x2%0644&info_hash=H"),
        ),
        (
            "https://example.com:8080/announce#frag",
            Some("https://example.com:8080/scrape?info_hash=H"),
        ),
        ("http://example.com/a", None),
        (
            "http://example.com/announce?x=2/4",
            Some("http://example.com/scrape?x=2/4&info_hash=H"),
        ),
        ("http://example.com/x%064announce", None),
        ("http://example.com/announce/x", None),
        ("http://example.com/", None),
    ];
    for (announce, expected) in cases {
        assert_eq!(scrape(announce).as_deref(), expected, "{announce}");
    }
    let url = Url::parse("http://t/announce").unwrap();
    let url = http_scrape_url(&url, &[[1; 20], [2; 20]]).unwrap();
    assert!(url.starts_with("http://t/scrape?info_hash=%01%01"));
    assert!(url.contains("%01&info_hash=%02%02"));
}

#[test]
fn http_scrape_response() {
    let mut body = b"d5:filesd20:".to_vec();
    body.extend_from_slice(&[0xff; 20]);
    body.extend_from_slice(b"d8:completei5e10:downloadedi50e10:incompletei10ee20:");
    body.extend_from_slice(&[0xee; 20]);
    body.extend_from_slice(b"d8:completei1eee5:flagsd20:min_request_intervali900eee");
    let stats = parse_http_scrape(&body, &[[0xff; 20], [0x00; 20], [0xee; 20]]).unwrap();
    assert_eq!(
        stats,
        [
            Some(ScrapeStats {
                seeders: 5,
                leechers: 10,
                completed: 50
            }),
            None,
            Some(ScrapeStats {
                seeders: 1,
                leechers: 0,
                completed: 0
            }),
        ]
    );
    assert_eq!(parse_http_scrape(b"de", &[[0; 20]]).unwrap(), [None]);
}

#[test]
fn http_scrape_errors() {
    let err = parse_http_scrape(b"d14:failure reason9:not founde", &[[0; 20]]).unwrap_err();
    assert!(err.to_string().contains("not found"));
    let hash = [b'h'; 20];
    for body in [
        &b"not bencode"[..],
        b"d5:filesi1ee",
        b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhi1eee",
        b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhd8:completei-1eeee",
        b"d5:filesd20:hhhhhhhhhhhhhhhhhhhhd8:complete1:xeee",
    ] {
        assert!(
            parse_http_scrape(body, &[hash]).is_err(),
            "{}",
            String::from_utf8_lossy(body)
        );
    }
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
