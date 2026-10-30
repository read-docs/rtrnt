use super::*;
use crate::client::ConnectionSlots;
use crate::rate_limit::RateLimits;
use crate::torrent::{Info, TorrentFile};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;

const FEED: &str = "https://feed.example/rss";
const MAGNET_A: &str = "magnet:?xt=urn:btih:0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a&amp;dn=A";
const MAGNET_HASH_A: [u8; 20] = [0x0a; 20];

type Answer = Result<Vec<u8>, FetchError>;

/// Answers from a table instead of the network, and records what was
/// asked for.
#[derive(Clone, Default)]
struct Fake {
    answers: Arc<Mutex<HashMap<String, Answer>>>,
    asked: Arc<Mutex<Vec<(String, usize)>>>,
}

impl Fake {
    fn serve(&self, url: &str, body: impl Into<Vec<u8>>) {
        self.answers
            .lock()
            .unwrap()
            .insert(url.to_string(), Ok(body.into()));
    }

    fn fail(&self, url: &str, retry: bool) {
        let error = FetchError::new("it broke", retry);
        self.answers
            .lock()
            .unwrap()
            .insert(url.to_string(), Err(error));
    }

    /// How many times `url` was asked for.
    fn count(&self, url: &str) -> usize {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(asked, _)| asked == url)
            .count()
    }
}

impl Fetch for Fake {
    fn get(
        &self,
        url: &Url,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<u8>, FetchError>> + Send {
        self.asked.lock().unwrap().push((url.to_string(), limit));
        let answer = self
            .answers
            .lock()
            .unwrap()
            .get(url.as_str())
            .cloned()
            .unwrap_or_else(|| Err(FetchError::new("the server answered 404 Not Found", false)));
        std::future::ready(answer)
    }
}

/// A feed document with these items, each its title, guid and enclosure
/// URL (left out if empty).
fn rss(items: &[(&str, &str, &str)]) -> String {
    let items: String = items
        .iter()
        .map(|(title, guid, enclosure)| {
            let guid = if guid.is_empty() {
                String::new()
            } else {
                format!("<guid>{guid}</guid>")
            };
            let enclosure = if enclosure.is_empty() {
                String::new()
            } else {
                format!(r#"<enclosure url="{enclosure}"/>"#)
            };
            format!("<item><title>{title}</title>{guid}{enclosure}</item>")
        })
        .collect();
    format!(r#"<rss version="2.0"><channel><title>t</title>{items}</channel></rss>"#)
}

/// The bytes of a valid one-piece `.torrent` called `name`, and its info
/// hash.
fn torrent_bytes(name: &str) -> (Vec<u8>, [u8; 20]) {
    let torrent = TorrentFile {
        announce: None,
        announce_list: None,
        url_list: Vec::new(),
        httpseeds: Vec::new(),
        info: Info {
            name: name.to_string(),
            piece_length: 16 * 1024,
            pieces: serde_bytes::ByteBuf::from(vec![0; 20]),
            private: None,
            length: Some(16 * 1024),
            files: None,
        },
    };
    let data = serde_bencode::to_bytes(&torrent).unwrap();
    let info_hash = torrent::parse_metainfo(&data).unwrap().info_hash;
    (data, info_hash)
}

fn subscription(text: &str) -> Subscription {
    toml::from_str(text).unwrap()
}

/// A feed at `FEED` taking titles with "Show" but not "CAM".
fn show_feed(extra: &str) -> Subscription {
    subscription(&format!(
        "url = \"{FEED}\"\nmatch = [\"^Show\"]\nexclude = \"CAM\"\n{extra}"
    ))
}

fn stats() -> Arc<SessionStats> {
    Arc::new(SessionStats::new(
        Arc::new(RateLimits::default()),
        ConnectionSlots::new(5),
    ))
}

/// A poller with everything it needs, and the other ends.
struct Rig {
    poller: Poller<Fake>,
    commands: UnboundedReceiver<Command>,
    fake: Fake,
    stats: Arc<SessionStats>,
    store: SessionStore,
    seen_path: PathBuf,
    /// Kept so the files are deleted when the rig is dropped.
    _dir: tempfile::TempDir,
}

fn rig(subscriptions: Vec<Subscription>) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().join("state"));
    let seen_path = dir.path().join("state").join("rss-seen.json");
    let fake = Fake::default();
    let (tx, commands) = mpsc::unbounded_channel();
    let stats = stats();
    let poller = Poller::new(
        subscriptions,
        fake.clone(),
        Some(store.clone()),
        Seen::new(Some(seen_path.clone())),
        tx,
        Arc::clone(&stats),
    );
    Rig {
        poller,
        commands,
        fake,
        stats,
        store,
        seen_path,
        _dir: dir,
    }
}

fn received(commands: &mut UnboundedReceiver<Command>) -> Vec<Command> {
    std::iter::from_fn(|| commands.try_recv().ok()).collect()
}

fn notice(stats: &SessionStats) -> String {
    stats
        .last_notice()
        .map(|(_, text)| text)
        .unwrap_or_default()
}

fn magnet_source(hash: [u8; 20]) -> Source {
    Source::Magnet(Box::new(MagnetLink {
        info_hash: hash,
        name: None,
        trackers: Vec::new(),
        peers: Vec::new(),
        select_only: None,
    }))
}

#[tokio::test]
async fn the_items_that_match_are_added_once() {
    let mut rig = rig(vec![show_feed("")]);
    let (data, hash) = torrent_bytes("Show B");
    rig.fake.serve(
        FEED,
        rss(&[
            ("Show A", "a", MAGNET_A),
            ("Show B", "b", "https://feed.example/t/b.torrent"),
            ("Other C", "c", MAGNET_A),
            ("Show D CAM", "d", MAGNET_A),
        ]),
    );
    rig.fake
        .serve("https://feed.example/t/b.torrent", data.clone());

    assert!(rig.poller.poll(0).await);
    let copy = rig.store.torrent_path(hash);
    let commands = received(&mut rig.commands);
    assert_eq!(commands.len(), 2);
    // The magnet link with only the info hash compared: the rest is the
    // link's own.
    let Command::Add(Source::Magnet(link)) = &commands[0] else {
        panic!("{:?}", commands[0]);
    };
    assert_eq!(link.info_hash, MAGNET_HASH_A);
    assert_eq!(commands[1], Command::Add(Source::File(copy.clone())));
    // The downloaded torrent is kept where the session keeps its own.
    assert_eq!(std::fs::read(&copy).unwrap(), data);
    // The torrent URL was asked for with the size cap, the unmatched items'
    // not at all.
    assert_eq!(
        *rig.fake.asked.lock().unwrap(),
        [
            (FEED.to_string(), FEED_LIMIT),
            (
                "https://feed.example/t/b.torrent".to_string(),
                TORRENT_LIMIT
            ),
        ]
    );
    assert!(notice(&rig.stats).starts_with("rss: added "));

    // The same items again: nothing is added or downloaded.
    assert!(rig.poller.poll(0).await);
    assert!(received(&mut rig.commands).is_empty());
    assert_eq!(rig.fake.count("https://feed.example/t/b.torrent"), 1);

    // A new item is.
    rig.fake.serve(
        FEED,
        rss(&[("Show A", "a", MAGNET_A), ("Show E", "e", MAGNET_A)]),
    );
    assert!(rig.poller.poll(0).await);
    assert_eq!(received(&mut rig.commands).len(), 1);
}

#[tokio::test]
async fn a_restart_does_not_add_the_items_again() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(FEED, rss(&[("Show A", "a", MAGNET_A)]));
    assert!(rig.poller.poll(0).await);
    assert_eq!(received(&mut rig.commands).len(), 1);

    // A new poller reading the file the old one left.
    let (tx, mut commands) = mpsc::unbounded_channel();
    let mut again = Poller::new(
        vec![show_feed("")],
        rig.fake.clone(),
        Some(rig.store.clone()),
        Seen::load(Some(rig.seen_path.clone())),
        tx,
        stats(),
    );
    assert!(again.poll(0).await);
    assert!(received(&mut commands).is_empty());
}

#[tokio::test]
async fn a_feed_can_add_its_torrents_paused() {
    let mut rig = rig(vec![show_feed("paused = true")]);
    rig.fake.serve(FEED, rss(&[("Show A", "a", MAGNET_A)]));
    assert!(rig.poller.poll(0).await);
    let commands = received(&mut rig.commands);
    assert!(matches!(
        commands.as_slice(),
        [Command::AddPaused(Source::Magnet(_))]
    ));
}

#[tokio::test]
async fn an_item_with_no_link_is_reported_and_not_tried_again() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(
        FEED,
        rss(&[
            ("Show A", "a", ""),
            ("Show B", "", ""),
            ("Show C", "c", MAGNET_A),
        ]),
    );
    assert!(rig.poller.poll(0).await);
    // Only the one with a link; the one with nothing to go by is skipped
    // silently, the other said so.
    assert_eq!(received(&mut rig.commands).len(), 1);
    assert_eq!(rig.poller.seen.len(), 2);
    assert!(rig.seen_path.exists());
}

#[tokio::test]
async fn a_download_that_failed_is_tried_again_only_if_it_may_pass() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(
        FEED,
        rss(&[
            ("Show A", "a", "https://feed.example/t/a.torrent"),
            ("Show B", "b", "https://feed.example/t/b.torrent"),
            ("Show C", "c", "https://feed.example/t/c.torrent"),
        ]),
    );
    // A is down for now, B is gone, C is not a torrent.
    rig.fake.fail("https://feed.example/t/a.torrent", true);
    rig.fake.fail("https://feed.example/t/b.torrent", false);
    rig.fake
        .serve("https://feed.example/t/c.torrent", "<html>log in</html>");

    assert!(rig.poller.poll(0).await);
    assert!(received(&mut rig.commands).is_empty());
    assert!(notice(&rig.stats).starts_with("rss: cannot add Show C"));

    let (data, hash) = torrent_bytes("A");
    rig.fake.serve("https://feed.example/t/a.torrent", data);
    assert!(rig.poller.poll(0).await);
    assert_eq!(
        received(&mut rig.commands),
        [Command::Add(Source::File(rig.store.torrent_path(hash)))]
    );
    assert_eq!(rig.fake.count("https://feed.example/t/a.torrent"), 2);
    assert_eq!(rig.fake.count("https://feed.example/t/b.torrent"), 1);
    assert_eq!(rig.fake.count("https://feed.example/t/c.torrent"), 1);
}

#[tokio::test]
async fn a_magnet_link_that_does_not_parse_is_not_tried_again() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake
        .serve(FEED, rss(&[("Show A", "a", "magnet:?dn=nohash")]));
    assert!(rig.poller.poll(0).await);
    assert!(received(&mut rig.commands).is_empty());
    assert_eq!(rig.poller.seen.len(), 1);
}

#[tokio::test]
async fn without_a_place_for_torrent_files_only_magnet_links_are_taken() {
    let (tx, mut commands) = mpsc::unbounded_channel();
    let fake = Fake::default();
    let mut poller = Poller::new(
        vec![show_feed("")],
        fake.clone(),
        None,
        Seen::new(None),
        tx,
        stats(),
    );
    fake.serve(
        FEED,
        rss(&[
            ("Show A", "a", MAGNET_A),
            ("Show B", "b", "https://feed.example/t/b.torrent"),
        ]),
    );
    assert!(poller.poll(0).await);
    assert_eq!(received(&mut commands).len(), 1);
    assert_eq!(fake.count("https://feed.example/t/b.torrent"), 0);
}

#[tokio::test]
async fn a_feed_that_fails_is_noted_once_and_backs_off() {
    let mut rig = rig(vec![show_feed("")]);
    let interval = rig.poller.slots[0].subscription.interval;
    rig.fake.fail(FEED, true);

    assert!(rig.poller.poll(0).await);
    assert_eq!(rig.poller.slots[0].failures, 1);
    assert!(notice(&rig.stats).starts_with("rss: cannot read feed.example"));
    // The notice does not give away the rest of the URL.
    assert!(!notice(&rig.stats).contains("/rss"));

    // A later failure does not replace what the user has since been told.
    rig.stats.notice("something else");
    assert!(rig.poller.poll(0).await);
    assert_eq!(rig.poller.slots[0].failures, 2);
    assert_eq!(notice(&rig.stats), "something else");

    // It answers again: back to the interval.
    rig.fake.serve(FEED, rss(&[]));
    assert!(rig.poller.poll(0).await);
    assert_eq!(rig.poller.slots[0].failures, 0);
    let wait = rig.poller.slots[0].due - Instant::now();
    assert!(wait <= interval && wait > interval - Duration::from_secs(5));
}

#[tokio::test]
async fn a_document_that_is_not_a_feed_counts_as_a_failure() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(FEED, "<html>Please log in</html>");
    assert!(rig.poller.poll(0).await);
    assert_eq!(rig.poller.slots[0].failures, 1);
    assert!(received(&mut rig.commands).is_empty());
}

#[test]
fn the_backoff_doubles_up_to_the_cap() {
    let minutes = |m: u64| Duration::from_secs(m * 60);
    let interval = minutes(15);
    assert_eq!(backoff(interval, 0), interval);
    assert_eq!(backoff(interval, 1), interval);
    assert_eq!(backoff(interval, 2), minutes(30));
    assert_eq!(backoff(interval, 3), minutes(60));
    assert_eq!(backoff(interval, 5), minutes(240));
    assert_eq!(backoff(interval, 6), MAX_BACKOFF);
    assert_eq!(backoff(interval, u32::MAX), MAX_BACKOFF);
    // An interval longer than the cap is never cut short.
    let long = Duration::from_secs(24 * 3600);
    assert_eq!(backoff(long, 1), long);
    assert_eq!(backoff(long, 9), long);
}

#[tokio::test]
async fn a_closed_command_channel_stops_the_poll() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(FEED, rss(&[("Show A", "a", MAGNET_A)]));
    drop(std::mem::replace(
        &mut rig.commands,
        mpsc::unbounded_channel().1,
    ));
    assert!(!rig.poller.poll(0).await);
    // Not remembered as taken, since it was not.
    assert!(rig.poller.seen.is_empty());
}

/// Runs `poller` until `for_how_long` has passed on the paused clock, then
/// stops it and returns once it has finished.
async fn run_for(poller: Poller<Fake>, for_how_long: Duration) {
    let (stop, stopped) = oneshot::channel::<()>();
    let run = tokio::spawn(poller.run(async move {
        let _ = stopped.await;
    }));
    tokio::time::sleep(for_how_long).await;
    drop(stop);
    run.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn each_feed_is_polled_at_its_own_interval() {
    let rig = rig(vec![
        subscription("url = \"https://a.example/rss\"\nmatch = [\"x\"]\ninterval = 5"),
        subscription("url = \"https://b.example/rss\"\nmatch = [\"x\"]\ninterval = 12"),
    ]);
    rig.fake.serve("https://a.example/rss", rss(&[]));
    rig.fake.serve("https://b.example/rss", rss(&[]));
    let minute = Duration::from_secs(60);

    // Both at the start, then A at 5, 10 and 15, B at 12.
    run_for(rig.poller, 16 * minute).await;
    assert_eq!(rig.fake.count("https://a.example/rss"), 4);
    assert_eq!(rig.fake.count("https://b.example/rss"), 2);
}

#[tokio::test(start_paused = true)]
async fn a_failing_feed_is_asked_less_and_less_often() {
    let rig = rig(vec![show_feed("interval = 5")]);
    rig.fake.fail(FEED, true);
    let minute = Duration::from_secs(60);

    // At 0, then 5, then 5 + 10 = 15, then 15 + 20 = 35.
    let fake = rig.fake.clone();
    run_for(rig.poller, 16 * minute).await;
    assert_eq!(fake.count(FEED), 3);
}

#[tokio::test(start_paused = true)]
async fn the_run_adds_what_the_feeds_offer_and_lets_go_of_the_channel() {
    let mut rig = rig(vec![show_feed("")]);
    rig.fake.serve(FEED, rss(&[("Show A", "a", MAGNET_A)]));
    run_for(rig.poller, Duration::from_secs(1)).await;
    assert_eq!(received(&mut rig.commands).len(), 1);
    // The poller is gone, with its sender, so the manager's run can end.
    assert!(rig.commands.recv().await.is_none());
}

#[tokio::test(start_paused = true)]
async fn with_no_feeds_the_run_waits_to_be_stopped() {
    let rig = rig(Vec::new());
    run_for(rig.poller, Duration::from_secs(3600)).await;
}

#[test]
fn the_seen_cache_is_in_the_state_directory() {
    let dir = default_state_dir().map(|dir| dir.join("rss-seen.json"));
    assert_eq!(seen_path(), dir);
}

#[test]
fn magnet_sources_compare_by_their_link() {
    // The helper builds what `MagnetLink::parse` gives for a bare hash.
    let parsed = MagnetLink::parse(&format!("magnet:?xt=urn:btih:{}", "0a".repeat(20))).unwrap();
    assert_eq!(
        Source::Magnet(Box::new(parsed)),
        magnet_source(MAGNET_HASH_A)
    );
}

/// Serves each connection one canned response, and returns the address.
async fn serve(response: Vec<u8>) -> std::net::SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let response = response.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let _ = socket.write_all(&response).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    addr
}

fn http_response(status: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut response =
        format!("HTTP/1.1 {status}\r\n{headers}Connection: close\r\n\r\n").into_bytes();
    response.extend_from_slice(body);
    response
}

#[tokio::test]
async fn the_http_fetcher_gets_a_document_and_reports_what_went_wrong() {
    let body = rss(&[("Show A", "a", MAGNET_A)]);
    let ok = serve(http_response(
        "200 OK",
        &format!("Content-Length: {}\r\n", body.len()),
        body.as_bytes(),
    ))
    .await;
    let url = Url::parse(&format!("http://{ok}/feed")).unwrap();
    let got = HttpFetcher.get(&url, 1 << 20).await.unwrap();
    assert_eq!(got, body.as_bytes());
    // What it got reads as a feed.
    assert_eq!(parse_feed(&got, Some(&url)).unwrap().len(), 1);

    // Too big by what the header says, and by what arrives with no header.
    let e = HttpFetcher.get(&url, 10).await.unwrap_err();
    assert!(!e.retry, "{e}");
    let unsized_body = serve(http_response("200 OK", "", &[b'x'; 5000])).await;
    let url = Url::parse(&format!("http://{unsized_body}/f")).unwrap();
    assert_eq!(HttpFetcher.get(&url, 5000).await.unwrap().len(), 5000);
    let e = HttpFetcher.get(&url, 4999).await.unwrap_err();
    assert!(!e.retry && e.message.contains("4999"), "{e}");

    // A refusal is final, a server's trouble is not.
    let missing = serve(http_response("404 Not Found", "Content-Length: 0\r\n", b"")).await;
    let url = Url::parse(&format!("http://{missing}/f")).unwrap();
    let e = HttpFetcher.get(&url, 100).await.unwrap_err();
    assert!(!e.retry && e.message.contains("404"), "{e}");
    let broken = serve(http_response(
        "503 Service Unavailable",
        "Content-Length: 0\r\n",
        b"",
    ))
    .await;
    let url = Url::parse(&format!("http://{broken}/f")).unwrap();
    assert!(HttpFetcher.get(&url, 100).await.unwrap_err().retry);

    // Nobody listening is the network's trouble, and does not give the
    // URL away.
    let gone = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    };
    let url = Url::parse(&format!("http://{gone}/secret-key")).unwrap();
    let e = HttpFetcher.get(&url, 100).await.unwrap_err();
    assert!(e.retry);
    assert!(!e.message.contains("secret-key"), "{e}");
}

#[tokio::test]
async fn a_feed_is_followed_over_http_end_to_end() {
    let (data, hash) = torrent_bytes("Show Z");
    let torrent_addr = serve(http_response(
        "200 OK",
        &format!("Content-Length: {}\r\n", data.len()),
        &data,
    ))
    .await;
    let feed = rss(&[("Show Z", "z", &format!("http://{torrent_addr}/z.torrent"))]);
    let feed_addr = serve(http_response(
        "200 OK",
        &format!("Content-Length: {}\r\n", feed.len()),
        feed.as_bytes(),
    ))
    .await;
    let dir = tempfile::tempdir().unwrap();
    let store = SessionStore::new(dir.path().to_path_buf());
    let (tx, mut commands) = mpsc::unbounded_channel();
    let mut poller = Poller::new(
        vec![subscription(&format!(
            "url = \"http://{feed_addr}/rss\"\nmatch = [\"Show\"]"
        ))],
        HttpFetcher,
        Some(store.clone()),
        Seen::new(None),
        tx,
        stats(),
    );
    assert!(poller.poll(0).await);
    assert_eq!(
        received(&mut commands),
        [Command::Add(Source::File(store.torrent_path(hash)))]
    );
    assert_eq!(std::fs::read(store.torrent_path(hash)).unwrap(), data);
    drop(dir);
}

#[tokio::test]
async fn feed_directory_is_forwarded_with_the_pause_setting() {
    let mut rig = rig(vec![show_feed(
        "download-dir = '/downloads/shows'\npaused = true",
    )]);
    rig.fake.serve(FEED, rss(&[("Show A", "a", MAGNET_A)]));
    assert!(rig.poller.poll(0).await);
    let commands = received(&mut rig.commands);
    assert!(
        matches!(&commands[..], [Command::AddIn {paused:true,dir,..}] if dir == &PathBuf::from("/downloads/shows"))
    );
}
