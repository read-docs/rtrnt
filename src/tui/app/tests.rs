use super::*;
use crate::stats::TorrentState;
use crate::tui::snapshot::Detail;
use crossterm::event::KeyEvent;
use std::path::PathBuf;

pub(crate) fn torrent(hash: u8, name: &str) -> TorrentSnapshot {
    TorrentSnapshot {
        info_hash: [hash; 20],
        name: name.into(),
        state: TorrentState::Downloading,
        layout: None,
        save_path: PathBuf::new(),
        verified: 0,
        received: 0,
        downloaded: 0,
        uploaded: 0,
        peers: 0,
        pool: 0,
        active: Duration::ZERO,
        order: hash as usize,
        rate_down: 0.0,
        rate_up: 0.0,
        eta: None,
    }
}

fn snapshot(names: &[&str]) -> Snapshot {
    Snapshot {
        torrents: names
            .iter()
            .enumerate()
            .map(|(i, name)| torrent(i as u8, name))
            .collect(),
        ..Snapshot::default()
    }
}

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn peer(port: u16, rate_down: f64) -> crate::tui::snapshot::PeerSnapshot {
    crate::tui::snapshot::PeerSnapshot {
        addr: addr(port),
        inbound: false,
        client: None,
        flags: crate::stats::PeerFlags::initial(),
        downloaded: 0,
        uploaded: 0,
        pieces: 0,
        rate_down,
        rate_up: 0.0,
    }
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn names<'a>(app: &App, snapshot: &'a Snapshot) -> Vec<&'a str> {
    app.visible(snapshot)
        .iter()
        .map(|torrent| torrent.name.as_str())
        .collect()
}

#[test]
fn selection_moves_within_the_table() {
    let mut snapshot = snapshot(&["a", "b", "c"]);
    snapshot.detail = Some(Detail {
        info_hash: [2; 20],
        peers: vec![peer(1, 0.0), peer(2, 0.0), peer(3, 0.0)],
        ..Detail::default()
    });
    let mut app = App::new(Theme::default());
    app.ensure_selection(&snapshot);
    assert_eq!(app.selected, Some([0; 20]));
    app.handle_key(key(KeyCode::Up), &snapshot);
    assert_eq!(app.selected, Some([0; 20]));
    app.handle_key(key(KeyCode::Down), &snapshot);
    app.handle_key(key(KeyCode::Char('j')), &snapshot);
    app.handle_key(key(KeyCode::Char('j')), &snapshot);
    assert_eq!(app.selected, Some([2; 20]));
    // With the detail panel focused, the arrows move the peer cursor.
    app.handle_key(key(KeyCode::Tab), &snapshot);
    app.handle_key(key(KeyCode::Char('k')), &snapshot);
    assert_eq!(app.selected, Some([2; 20]));
    app.handle_key(key(KeyCode::Down), &snapshot);
    assert_eq!((app.peer, app.detail_scroll), (Some(addr(2)), 0));
    // On the other tabs they scroll it, no further than its last row: the
    // DHT, PeX and incoming rows after a blank one.
    app.handle_key(key(KeyCode::Char('3')), &snapshot);
    app.handle_key(key(KeyCode::Down), &snapshot);
    assert_eq!(app.detail_scroll, 1);
    app.handle_key(key(KeyCode::PageDown), &snapshot);
    assert_eq!(app.detail_scroll, 3);
    app.handle_key(key(KeyCode::PageUp), &snapshot);
    assert_eq!(app.detail_scroll, 0);
    app.handle_key(key(KeyCode::Down), &snapshot);
    app.handle_key(key(KeyCode::Char('4')), &snapshot);
    assert_eq!((app.tab, app.detail_scroll), (Tab::Files, 0));
}

/// `snapshot` with torrent "a" selected, and peers on ports `ports`, each
/// as fast as its port.
fn with_peers(ports: &[u16]) -> (Snapshot, App) {
    let mut snapshot = snapshot(&["a"]);
    snapshot.detail = Some(Detail {
        info_hash: [0; 20],
        peers: ports.iter().map(|&port| peer(port, port.into())).collect(),
        ..Detail::default()
    });
    let mut app = App::new(Theme::default());
    app.ensure_selection(&snapshot);
    app.ensure_peer(&snapshot);
    app.focus = Focus::Detail;
    (snapshot, app)
}

#[test]
fn the_peer_cursor_moves_fastest_first_and_stops_at_the_ends() {
    let ports: Vec<u16> = (1..=15).collect();
    let (snapshot, mut app) = with_peers(&ports);
    // The fastest peer first.
    assert_eq!(app.peer, Some(addr(15)));
    app.handle_key(key(KeyCode::Up), &snapshot);
    assert_eq!(app.peer, Some(addr(15)));
    app.handle_key(key(KeyCode::Char('j')), &snapshot);
    assert_eq!(app.peer, Some(addr(14)));
    app.handle_key(key(KeyCode::PageDown), &snapshot);
    assert_eq!(app.peer, Some(addr(4)));
    app.handle_key(key(KeyCode::PageDown), &snapshot);
    assert_eq!(app.peer, Some(addr(1)));
    app.handle_key(key(KeyCode::PageUp), &snapshot);
    assert_eq!(app.peer, Some(addr(11)));
    // A page moves it with the torrents focused too, so the table scrolls.
    app.focus = Focus::Torrents;
    app.handle_key(key(KeyCode::PageUp), &snapshot);
    assert_eq!(app.peer, Some(addr(15)));
    // Another torrent starts it again at the top.
    app.select_peer(&snapshot, 3);
    assert_eq!(app.peer, Some(addr(12)));
    app.ensure_selection(&Snapshot::default());
    assert_eq!(app.peer, None);
}

#[test]
fn the_peer_cursor_keeps_its_peer_when_the_order_changes() {
    let (mut snapshot, mut app) = with_peers(&[1, 2, 3]);
    app.handle_key(key(KeyCode::Down), &snapshot);
    assert_eq!(app.peer, Some(addr(2)));
    // Port 2 is now the slowest, and the cursor goes with it.
    snapshot.detail.as_mut().unwrap().peers[1].rate_down = 0.0;
    app.ensure_peer(&snapshot);
    assert_eq!(app.peer, Some(addr(2)));
    app.handle_key(key(KeyCode::Up), &snapshot);
    assert_eq!(app.peer, Some(addr(1)));
}

#[test]
fn the_peer_cursor_takes_the_nearest_row_when_its_peer_goes() {
    let (mut snapshot, mut app) = with_peers(&[1, 2, 3, 4]);
    app.select_peer(&snapshot, 1);
    assert_eq!(app.peer, Some(addr(3)));
    let peers = &mut snapshot.detail.as_mut().unwrap().peers;
    peers.retain(|peer| peer.addr != addr(3));
    app.ensure_peer(&snapshot);
    assert_eq!(app.peer, Some(addr(2)));
    // The last row, when the peer was on it.
    app.select_peer(&snapshot, 2);
    let peers = &mut snapshot.detail.as_mut().unwrap().peers;
    peers.retain(|peer| peer.addr != addr(1));
    app.ensure_peer(&snapshot);
    assert_eq!(app.peer, Some(addr(2)));
    // None while there are no peers, and the top one when they come back.
    snapshot.detail.as_mut().unwrap().peers.clear();
    app.ensure_peer(&snapshot);
    assert_eq!(app.peer, None);
    snapshot.detail.as_mut().unwrap().peers = vec![peer(7, 1.0), peer(8, 2.0)];
    app.ensure_peer(&snapshot);
    assert_eq!(app.peer, Some(addr(7)));
}

#[test]
fn only_the_cursor_peer_gets_its_history() {
    let start = Instant::now();
    let (mut snapshot, mut app) = with_peers(&[1, 2]);
    for tick in 0..=4u64 {
        let peers = &mut snapshot.detail.as_mut().unwrap().peers;
        peers[0].downloaded = tick * 100;
        peers[1].downloaded = tick * 200;
        app.rates
            .record(&snapshot, start + Duration::from_secs(tick));
    }
    app.apply_rates(&mut snapshot);
    let detail = snapshot.detail.as_ref().unwrap();
    assert_eq!(
        detail.peer_history,
        Some((addr(2), vec![200.0; 4], vec![0.0; 4]))
    );
    // The peer goes: the cursor and its history move to the one left.
    snapshot.detail.as_mut().unwrap().peers.remove(1);
    app.apply_rates(&mut snapshot);
    let (peer, down, _) = snapshot.detail.unwrap().peer_history.unwrap();
    assert_eq!((peer, down), (addr(1), vec![100.0; 4]));
}

#[test]
fn quitting_stops_first_and_leaves_on_the_second_press() {
    let mut snapshot = snapshot(&["a"]);
    let mut app = App::new(Theme::default());
    assert_eq!(
        app.handle_key(key(KeyCode::Char('q')), &snapshot),
        Action::Command(Command::Stop)
    );
    let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
    assert_eq!(app.handle_key(ctrl_c, &snapshot), Action::ForceQuit);
    snapshot.finished = true;
    assert_eq!(
        app.handle_key(key(KeyCode::Char('q')), &snapshot),
        Action::Quit
    );
}

#[test]
fn sorts_and_filters() {
    let mut snapshot = snapshot(&["debian", "Arch", "ubuntu"]);
    snapshot.torrents[2].rate_down = 5.0;
    snapshot.torrents[0].rate_down = 1.0;
    let mut app = App::new(Theme::default());
    assert_eq!(names(&app, &snapshot), ["debian", "Arch", "ubuntu"]);
    app.handle_key(key(KeyCode::Char('s')), &snapshot);
    assert_eq!(app.sort, Sort::Name);
    assert_eq!(names(&app, &snapshot), ["Arch", "debian", "ubuntu"]);
    app.handle_key(key(KeyCode::Char('S')), &snapshot);
    assert_eq!(names(&app, &snapshot), ["ubuntu", "debian", "Arch"]);
    app.sort = Sort::Down;
    app.reverse = false;
    assert_eq!(names(&app, &snapshot), ["Arch", "debian", "ubuntu"]);

    app.ensure_selection(&snapshot);
    for c in "/BIAN".chars() {
        app.handle_key(key(KeyCode::Char(c)), &snapshot);
    }
    assert_eq!(names(&app, &snapshot), ["debian"]);
    assert_eq!(app.selected, Some([0; 20]));
    // Typing goes to the filter, not to the keys.
    app.handle_key(key(KeyCode::Char('q')), &snapshot);
    assert_eq!(app.filter, "BIANq");
    assert!(!app.stopping);
    app.handle_key(key(KeyCode::Backspace), &snapshot);
    app.handle_key(key(KeyCode::Enter), &snapshot);
    assert_eq!(app.input, None);
    assert_eq!(app.filter, "BIAN");
    app.handle_key(key(KeyCode::Esc), &snapshot);
    assert_eq!(names(&app, &snapshot).len(), 3);
}

#[test]
fn rates_are_smoothed_and_the_eta_averaged() {
    let start = Instant::now();
    let mut snapshot = snapshot(&["a"]);
    snapshot.torrents[0].layout = Some(crate::stats::Layout {
        size: 10_000,
        piece_length: 100,
        pieces: 100,
        private: false,
        files: Vec::new(),
    });
    snapshot.detail = Some(Detail {
        info_hash: [0; 20],
        ..Detail::default()
    });
    let mut rates = Rates::default();
    // 100 B/s for 10 s, then 500 B/s.
    for tick in 0..=60u64 {
        let at = start + Duration::from_millis(250 * tick);
        let secs = tick as f64 / 4.0;
        let bytes = if secs <= 10.0 {
            secs * 100.0
        } else {
            1000.0 + (secs - 10.0) * 500.0
        };
        snapshot.torrents[0].received = bytes as u64;
        snapshot.torrents[0].uploaded = tick * 10;
        rates.record(&snapshot, at);
    }
    snapshot.torrents[0].verified = 4000;
    rates.apply(&mut snapshot, None);
    let torrent = &snapshot.torrents[0];
    assert!(
        (torrent.rate_down - 500.0).abs() < 1.0,
        "{}",
        torrent.rate_down
    );
    assert!((torrent.rate_up - 40.0).abs() < 1.0, "{}", torrent.rate_up);
    // 6000 B left at the 15 s average of (1000 + 2500) / 15.
    let eta = torrent.eta.unwrap().as_secs_f64();
    assert!((eta - 6000.0 / (3500.0 / 15.0)).abs() < 1.0, "{eta}");
    let detail = snapshot.detail.as_ref().unwrap();
    assert_eq!(detail.down_history.len(), 60);
    assert_eq!(detail.down_history[0], 100.0);
    assert_eq!(*detail.down_history.last().unwrap(), 500.0);

    // A torrent that leaves is forgotten.
    snapshot.torrents.clear();
    rates.record(&snapshot, start + Duration::from_secs(20));
    assert!(rates.torrents.is_empty());
}

#[test]
fn limit_keys_open_the_prompt_with_the_current_limit() {
    let mut snapshot = snapshot(&["a"]);
    snapshot.session.rate_limits = (2048, 0);
    let mut app = App::new(Theme::default());
    app.handle_key(key(KeyCode::Char('d')), &snapshot);
    let input = app.input.as_ref().unwrap();
    assert_eq!(
        (input.kind, input.text.as_str()),
        (InputKind::DownloadLimit, "2048")
    );
    // Esc cancels, leaving the filter alone.
    app.filter = "a".into();
    app.handle_key(key(KeyCode::Esc), &snapshot);
    assert_eq!((&app.input, app.filter.as_str()), (&None, "a"));
    app.handle_key(key(KeyCode::Char('u')), &snapshot);
    let input = app.input.as_ref().unwrap();
    assert_eq!(
        (input.kind, input.text.as_str()),
        (InputKind::UploadLimit, "0")
    );
}

#[test]
fn a_limit_that_is_not_a_number_keeps_the_prompt_open() {
    let mut snapshot = snapshot(&["a"]);
    snapshot.session.rate_limits = (2048, 0);
    let mut app = App::new(Theme::default());
    app.handle_key(key(KeyCode::Char('u')), &snapshot);
    for c in "5k".chars() {
        app.handle_key(key(KeyCode::Char(c)), &snapshot);
    }
    assert_eq!(app.handle_key(key(KeyCode::Enter), &snapshot), Action::None);
    let input = app.input.as_ref().unwrap();
    assert_eq!(input.text, "05k");
    assert!(input.error.is_some());
    // Editing clears the error. Keys go to the text, not to the app.
    app.handle_key(key(KeyCode::Backspace), &snapshot);
    app.handle_key(key(KeyCode::Char('q')), &snapshot);
    let input = app.input.as_ref().unwrap();
    assert_eq!((input.text.as_str(), &input.error), ("05q", &None));
    assert!(!app.stopping);
    // Empty is not a number either.
    for _ in 0..3 {
        app.handle_key(key(KeyCode::Backspace), &snapshot);
    }
    app.handle_key(key(KeyCode::Enter), &snapshot);
    assert!(app.input.as_ref().unwrap().error.is_some());
}

#[test]
fn enter_sends_the_new_limit_with_the_other_unchanged() {
    let mut snapshot = snapshot(&["a"]);
    snapshot.session.rate_limits = (2048, 512);
    let mut app = App::new(Theme::default());
    app.handle_key(key(KeyCode::Char('d')), &snapshot);
    for _ in 0..4 {
        app.handle_key(key(KeyCode::Backspace), &snapshot);
    }
    for c in " 100 ".chars() {
        app.handle_key(key(KeyCode::Char(c)), &snapshot);
    }
    assert_eq!(
        app.handle_key(key(KeyCode::Enter), &snapshot),
        Action::Command(Command::SetRateLimits(100, 512))
    );
    assert_eq!(app.input, None);
    app.handle_key(key(KeyCode::Char('u')), &snapshot);
    app.handle_key(key(KeyCode::Backspace), &snapshot);
    app.handle_key(key(KeyCode::Backspace), &snapshot);
    app.handle_key(key(KeyCode::Backspace), &snapshot);
    app.handle_key(key(KeyCode::Char('0')), &snapshot);
    assert_eq!(
        app.handle_key(key(KeyCode::Enter), &snapshot),
        Action::Command(Command::SetRateLimits(2048, 0))
    );
}
