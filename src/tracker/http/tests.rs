use super::*;
use crate::tracker::AnnounceEvent;
use crate::tracker::tests::request;

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
