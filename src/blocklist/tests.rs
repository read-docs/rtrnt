use super::*;

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

fn list(text: &str) -> Blocklist {
    Blocklist::parse(text).0
}

#[test]
fn parses_peerguardian_lines() {
    let list = list("Some org:1.2.3.4-1.2.3.255\nWith: colons: here:10.0.0.0-10.0.0.9\n");
    assert!(list.contains(ip("1.2.3.4")));
    assert!(list.contains(ip("1.2.3.255")));
    assert!(!list.contains(ip("1.2.3.3")));
    assert!(!list.contains(ip("1.2.4.0")));
    assert!(list.contains(ip("10.0.0.5")));
}

#[test]
fn parses_peerguardian_ipv6_ranges() {
    let list = list("v6 org:2001:db8::1-2001:db8::ff\n");
    assert!(list.contains(ip("2001:db8::80")));
    assert!(!list.contains(ip("2001:db8::100")));
}

#[test]
fn parses_emule_lines_and_honors_the_access_level() {
    let list = list(
        "1.2.3.4 - 1.2.3.255 , 000 , blocked\n\
         5.5.5.0 - 5.5.5.255 , 127 , still blocked\n\
         6.6.6.0 - 6.6.6.255 , 128 , allowed\n\
         7.7.7.0 - 7.7.7.255 , 255 , allowed\n",
    );
    assert!(list.contains(ip("1.2.3.100")));
    assert!(list.contains(ip("5.5.5.5")));
    assert!(!list.contains(ip("6.6.6.6")));
    assert!(!list.contains(ip("7.7.7.7")));
}

#[test]
fn parses_cidr_blocks_and_single_addresses() {
    let list = list("192.168.0.0/16\n8.8.8.8\n2001:db8::/32\n::1\n1.1.1.1/32\n");
    assert!(list.contains(ip("192.168.255.255")));
    assert!(!list.contains(ip("192.169.0.0")));
    assert!(list.contains(ip("8.8.8.8")));
    assert!(!list.contains(ip("8.8.8.9")));
    assert!(list.contains(ip("2001:db8:ffff::1")));
    assert!(!list.contains(ip("2001:db9::")));
    assert!(list.contains(ip("::1")));
    assert!(list.contains(ip("1.1.1.1")));
}

#[test]
fn a_zero_length_prefix_blocks_everything_of_its_family() {
    let list = list("0.0.0.0/0\n");
    assert!(list.contains(ip("255.255.255.255")));
    assert!(!list.contains(ip("::1")));
}

#[test]
fn skips_comments_and_blank_lines_and_counts_malformed_ones() {
    let (list, malformed) = Blocklist::parse(
        "# comment\n\n; another\n// and another\n   \n\
         garbage\n1.2.3.4-\n1.2.3.9-1.2.3.1\n1.2.3.0/33\n1.2.3.4-::1\nok:9.9.9.9-9.9.9.9\r\n",
    );
    assert_eq!(malformed, 5);
    assert_eq!(list.len(), 1);
    assert!(list.contains(ip("9.9.9.9")));
}

#[test]
fn merges_overlapping_and_touching_ranges() {
    let list = list(
        "a:10.0.0.0-10.0.0.10\nb:10.0.0.5-10.0.0.20\nc:10.0.0.21-10.0.0.30\n\
         d:10.0.0.40-10.0.0.50\ne:10.0.0.42-10.0.0.43\n",
    );
    assert_eq!(list.v4.len(), 2);
    assert!(list.contains(ip("10.0.0.25")));
    assert!(!list.contains(ip("10.0.0.35")));
    assert!(list.contains(ip("10.0.0.50")));
}

#[test]
fn merging_survives_the_end_of_the_address_space() {
    let list = list("a:255.255.255.0-255.255.255.255\nb:255.255.255.200-255.255.255.255\n");
    assert_eq!(list.v4.len(), 1);
    assert!(list.contains(ip("255.255.255.255")));
    assert!(list.contains(ip("255.255.255.0")));
    assert!(!list.contains(ip("255.255.254.255")));
}

#[test]
fn looks_up_either_family_without_mixing_them() {
    let list = list("a:0.0.0.0-0.0.0.5\nb:::-::5\n");
    assert!(list.contains(ip("0.0.0.0")));
    assert!(list.contains(ip("::3")));
    assert!(!list.contains(ip("0.0.0.6")));
    assert!(!list.contains(ip("::6")));
    assert!(!list.contains(ip("1.0.0.0")));
}

#[test]
fn an_ipv4_mapped_address_is_the_ipv4_address() {
    let list = list("1.2.3.0/24\n");
    assert!(list.contains(ip("::ffff:1.2.3.4")));
    assert!(!list.contains(ip("::ffff:1.2.4.4")));
}

#[test]
fn an_empty_list_blocks_nothing() {
    let list = Blocklist::default();
    assert!(list.is_empty());
    assert!(!list.contains(ip("1.2.3.4")));
}

#[test]
fn counts_the_peers_it_refuses() {
    let list = list("1.2.3.0/24\n");
    assert!(list.refuses(ip("1.2.3.4")));
    assert!(!list.refuses(ip("9.9.9.9")));
    assert!(list.refuses(ip("1.2.3.5")));
    assert_eq!(list.blocked(), 2);
}

#[test]
fn loads_a_file_and_refuses_a_gzipped_one() {
    let dir = std::env::temp_dir().join(format!("rtrnt-blocklist-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("list.txt");
    std::fs::write(&path, b"caf\xe9:1.2.3.4-1.2.3.4\n").unwrap();
    assert!(Blocklist::load(&path).unwrap().contains(ip("1.2.3.4")));
    let gz = dir.join("list.gz");
    std::fs::write(&gz, [0x1f, 0x8b, 8, 0]).unwrap();
    assert!(Blocklist::load(&gz).is_err());
    assert!(Blocklist::load(&dir.join("missing")).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}
