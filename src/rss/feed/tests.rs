use super::*;

const RSS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:torrent="http://xmlns.ezrss.it/0.1/">
  <channel>
    <title>Releases</title>
    <link>https://example.org/</link>
    <item>
      <title>Show S01E01 [1080p]</title>
      <guid isPermaLink="false">id-1</guid>
      <link>https://example.org/view/1</link>
      <enclosure url="https://example.org/t/1.torrent" length="1234" type="application/x-bittorrent"/>
    </item>
    <item>
      <title><![CDATA[Show S01E02 & more]]></title>
      <guid>id-2</guid>
      <media:title>Not this one</media:title>
      <enclosure url="magnet:?xt=urn:btih:0101010101010101010101010101010101010101&amp;dn=Show.S01E02" type="application/x-bittorrent"/>
    </item>
    <item>
      <title>Show S01E03</title>
      <link>https://example.org/view/3</link>
      <torrent:magnetURI>magnet:?xt=urn:btih:0202020202020202020202020202020202020202</torrent:magnetURI>
    </item>
    <item>
      <title>Show S01E04</title>
      <guid>id-4</guid>
    </item>
    <item>
      <title>Nothing to go by</title>
    </item>
    <item>
      <title>Relative</title>
      <enclosure url="/t/5.torrent"/>
    </item>
  </channel>
</rss>"#;

const ATOM: &str = r#"<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Releases</title>
  <entry>
    <title>Atom One</title>
    <id>tag:example.org,2024:1</id>
    <link rel="alternate" href="https://example.org/view/1"/>
    <link rel="enclosure" type="application/x-bittorrent" href="https://example.org/t/1.torrent"/>
  </entry>
  <entry>
    <title>Atom Two</title>
    <id>tag:example.org,2024:2</id>
    <link href="https://example.org/view/2"/>
    <link href="magnet:?xt=urn:btih:0303030303030303030303030303030303030303"/>
  </entry>
  <entry>
    <title>Atom Three</title>
    <id>tag:example.org,2024:3</id>
    <link rel="alternate" type="text/html" href="three.torrent"/>
  </entry>
  <entry>
    <title>Atom Four</title>
    <id>tag:example.org,2024:4</id>
    <link rel="self" href="https://example.org/self"/>
  </entry>
  <entry>
    <title>Atom Five, no id</title>
  </entry>
</feed>"#;

fn base() -> Url {
    Url::parse("https://example.org/feeds/all.xml").unwrap()
}

fn torrent(url: &str) -> Option<Target> {
    Some(Target::Torrent(Url::parse(url).unwrap()))
}

fn magnet(hash_byte: &str) -> Option<Target> {
    Some(Target::Magnet(format!(
        "magnet:?xt=urn:btih:{}",
        hash_byte.repeat(20)
    )))
}

#[test]
fn rss_items_give_title_id_and_torrent() {
    let items = parse_feed(RSS.as_bytes(), Some(&base())).unwrap();
    assert_eq!(items.len(), 6);

    // The enclosure beats the page link.
    assert_eq!(items[0].title, "Show S01E01 [1080p]");
    assert_eq!(items[0].id.as_deref(), Some("id-1"));
    assert_eq!(items[0].target, torrent("https://example.org/t/1.torrent"));

    // A magnet link as the enclosure; CDATA is read as text, and the
    // title of another namespace is not the title.
    assert_eq!(items[1].title, "Show S01E02 & more");
    assert_eq!(
        items[1].target,
        Some(Target::Magnet(
            "magnet:?xt=urn:btih:0101010101010101010101010101010101010101&dn=Show.S01E02"
                .to_string()
        ))
    );

    // No guid: the link is the id. The magnetURI element beats the link.
    assert_eq!(items[2].id.as_deref(), Some("https://example.org/view/3"));
    assert_eq!(items[2].target, magnet("02"));
}

#[test]
fn rss_items_without_a_link_have_no_torrent() {
    let items = parse_feed(RSS.as_bytes(), Some(&base())).unwrap();
    // A guid but nothing to download.
    assert_eq!(items[3].id.as_deref(), Some("id-4"));
    assert_eq!(items[3].target, None);
    // Not even an id.
    assert_eq!(items[4].title, "Nothing to go by");
    assert_eq!(items[4].id, None);
    assert_eq!(items[4].target, None);
}

#[test]
fn a_relative_link_is_under_the_feed() {
    let items = parse_feed(RSS.as_bytes(), Some(&base())).unwrap();
    assert_eq!(items[5].target, torrent("https://example.org/t/5.torrent"));
    // With no feed URL to resolve it against, it is no place at all.
    let items = parse_feed(RSS.as_bytes(), None).unwrap();
    assert_eq!(items[5].target, None);
}

#[test]
fn atom_entries_prefer_the_enclosure_then_the_magnet_link() {
    let items = parse_feed(ATOM.as_bytes(), Some(&base())).unwrap();
    assert_eq!(items.len(), 5);

    assert_eq!(items[0].title, "Atom One");
    assert_eq!(items[0].id.as_deref(), Some("tag:example.org,2024:1"));
    assert_eq!(items[0].target, torrent("https://example.org/t/1.torrent"));

    // A magnet link beats the page it comes with.
    assert_eq!(items[1].target, magnet("03"));

    // A relative link is under the feed.
    assert_eq!(
        items[2].target,
        torrent("https://example.org/feeds/three.torrent")
    );
}

#[test]
fn atom_entries_without_a_usable_link_or_id() {
    let items = parse_feed(ATOM.as_bytes(), Some(&base())).unwrap();
    // `rel="self"` is not a torrent.
    assert_eq!(items[3].target, None);
    assert_eq!(items[3].id.as_deref(), Some("tag:example.org,2024:4"));
    assert_eq!(items[4].id, None);
    assert_eq!(items[4].target, None);
}

#[test]
fn only_http_and_magnet_links_are_places_to_get_a_torrent() {
    let xml = r#"<rss version="2.0"><channel>
        <item><title>ftp</title><guid>1</guid><enclosure url="ftp://example.org/a.torrent"/></item>
        <item><title>file</title><guid>2</guid><enclosure url="file:///etc/passwd"/></item>
        <item><title>junk</title><guid>3</guid><link>not a url</link></item>
    </channel></rss>"#;
    let items = parse_feed(xml.as_bytes(), None).unwrap();
    assert_eq!(items.len(), 3);
    assert!(items.iter().all(|item| item.target.is_none()));
}

#[test]
fn a_document_that_is_not_a_feed_is_an_error() {
    let html = "<html><body>Hello</body></html>";
    let e = parse_feed(html.as_bytes(), None).unwrap_err();
    assert!(format!("{e}").contains("<html>"), "{e}");
    assert!(parse_feed(b"this is not xml", None).is_err());
    assert!(parse_feed(b"", None).is_err());
    // A feed with no items is fine.
    let empty = r#"<rss version="2.0"><channel><title>x</title></channel></rss>"#;
    assert!(parse_feed(empty.as_bytes(), None).unwrap().is_empty());
}

#[test]
fn a_byte_order_mark_or_blank_lines_before_the_declaration_are_fine() {
    let xml = format!("\u{feff}\n  {RSS}");
    assert_eq!(parse_feed(xml.as_bytes(), Some(&base())).unwrap().len(), 6);
}

#[test]
fn a_dtd_is_refused_rather_than_expanded() {
    let xml = r#"<?xml version="1.0"?>
<!DOCTYPE rss [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;">]>
<rss version="2.0"><channel><item><title>&b;</title></item></channel></rss>"#;
    assert!(parse_feed(xml.as_bytes(), None).is_err());
}

#[test]
fn bytes_that_are_not_utf8_only_spoil_the_text() {
    let mut xml = br#"<rss version="2.0"><channel><item><title>caf"#.to_vec();
    xml.push(0xe9);
    xml.extend_from_slice(br#"</title><guid>1</guid></item></channel></rss>"#);
    let items = parse_feed(&xml, None).unwrap();
    assert_eq!(items[0].title, "caf\u{fffd}");
}
