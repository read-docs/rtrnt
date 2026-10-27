use super::*;
use crate::tracker::AnnounceEvent;

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
    let mut response = vec![0, 0, 0, 1, 0, 0, 0, 7, 0, 0, 7, 8, 0, 0, 0, 4, 0, 0, 0, 9];
    response.extend_from_slice(&[1, 2, 3, 4, 0, 80]);
    let parsed = parse_udp_announce(&response, false).unwrap();
    assert_eq!(parsed.interval, Some(1800));
    assert_eq!((parsed.seeders, parsed.leechers), (Some(9), Some(4)));
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
