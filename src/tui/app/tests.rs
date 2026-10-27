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

#[test]
fn m_turns_the_mouse_off_and_on() {
    let snapshot = snapshot(&["a"]);
    let mut app = App::new(Theme::default());
    assert!(app.mouse);
    app.handle_key(key(KeyCode::Char('m')), &snapshot);
    assert!(!app.mouse);
    app.handle_key(key(KeyCode::Char('m')), &snapshot);
    assert!(app.mouse);
}

/// A screen with torrents "a" to "c" in rows 3 to 5, the Name and Size
/// headings above them, and under them the detail panel with its Peers
/// and Files tabs, and peer rows 1 and 2 at lines 9 and 10.
fn clickable() -> (Snapshot, App, Areas) {
    use crate::tui::view::Rows;
    let mut snapshot = snapshot(&["a", "b", "c"]);
    snapshot.detail = Some(Detail {
        info_hash: [0; 20],
        peers: (1..=5).map(|port| peer(port, port.into())).collect(),
        ..Detail::default()
    });
    let mut app = App::new(Theme::default());
    app.ensure_selection(&snapshot);
    let row = |y| Rect::new(1, y, 38, 1);
    let areas = Areas {
        torrents: Rect::new(0, 1, 40, 6),
        headings: vec![
            (Sort::Name, Rect::new(2, 2, 10, 1)),
            (Sort::Size, Rect::new(13, 2, 5, 1)),
        ],
        torrent_rows: Rows {
            first: 0,
            rects: vec![row(3), row(4), row(5)],
        },
        detail: Rect::new(0, 7, 40, 10),
        tabs: vec![
            (Tab::Peers, Rect::new(2, 7, 7, 1)),
            (Tab::Files, Rect::new(12, 7, 7, 1)),
        ],
        peer_rows: Rows {
            first: 1,
            rects: vec![row(9), row(10)],
        },
        ..Areas::default()
    };
    (snapshot, app, areas)
}

fn mouse(kind: MouseEventKind, column: u16, row: u16) -> MouseEvent {
    MouseEvent {
        kind,
        column,
        row,
        modifiers: KeyModifiers::NONE,
    }
}

fn click(column: u16, row: u16) -> MouseEvent {
    mouse(MouseEventKind::Down(MouseButton::Left), column, row)
}

#[test]
fn a_click_on_a_torrent_selects_it_and_focuses_the_table() {
    let (snapshot, mut app, areas) = clickable();
    app.focus = Focus::Detail;
    assert_eq!(
        app.handle_mouse(click(20, 5), &areas, &snapshot),
        Action::None
    );
    assert_eq!((app.selected, app.focus), (Some([2; 20]), Focus::Torrents));
    // The border and the heading are not rows.
    app.handle_mouse(click(0, 4), &areas, &snapshot);
    app.handle_mouse(click(25, 2), &areas, &snapshot);
    assert_eq!(app.selected, Some([2; 20]));
    // Nor does a right click or a release select.
    app.handle_mouse(
        mouse(MouseEventKind::Down(MouseButton::Right), 20, 3),
        &areas,
        &snapshot,
    );
    app.handle_mouse(
        mouse(MouseEventKind::Up(MouseButton::Left), 20, 3),
        &areas,
        &snapshot,
    );
    assert_eq!(app.selected, Some([2; 20]));
}

#[test]
fn a_click_on_a_heading_sorts_and_a_second_reverses() {
    let (snapshot, mut app, areas) = clickable();
    app.handle_mouse(click(14, 2), &areas, &snapshot);
    assert_eq!((app.sort, app.reverse), (Sort::Size, false));
    app.handle_mouse(click(17, 2), &areas, &snapshot);
    assert_eq!((app.sort, app.reverse), (Sort::Size, true));
    // Another column sorts it the right way round again.
    app.handle_mouse(click(2, 2), &areas, &snapshot);
    assert_eq!((app.sort, app.reverse), (Sort::Name, false));
    // Between headings is nothing.
    app.handle_mouse(click(12, 2), &areas, &snapshot);
    assert_eq!((app.sort, app.reverse), (Sort::Name, false));
}

#[test]
fn a_click_on_a_tab_title_shows_it() {
    let (snapshot, mut app, areas) = clickable();
    app.tab = Tab::Peers;
    app.detail_scroll = 3;
    app.handle_mouse(click(18, 7), &areas, &snapshot);
    assert_eq!((app.tab, app.detail_scroll), (Tab::Files, 0));
    app.handle_mouse(click(30, 7), &areas, &snapshot);
    assert_eq!(app.tab, Tab::Files);
    app.handle_mouse(click(2, 7), &areas, &snapshot);
    assert_eq!(app.tab, Tab::Peers);
}

#[test]
fn a_click_on_a_peer_moves_the_cursor_and_focuses_the_panel() {
    let (snapshot, mut app, areas) = clickable();
    // The rows shown start at the second peer, the second fastest.
    app.handle_mouse(click(5, 10), &areas, &snapshot);
    assert_eq!((app.peer, app.focus), (Some(addr(3)), Focus::Detail));
    app.handle_mouse(click(5, 9), &areas, &snapshot);
    assert_eq!(app.peer, Some(addr(4)));
    // Under the rows shown is nothing.
    app.handle_mouse(click(5, 11), &areas, &snapshot);
    assert_eq!(app.peer, Some(addr(4)));
}

#[test]
fn a_click_anywhere_closes_the_help_and_does_nothing_else() {
    let (snapshot, mut app, areas) = clickable();
    app.show_help = true;
    app.handle_mouse(click(20, 5), &areas, &snapshot);
    assert!(!app.show_help);
    assert_eq!(app.selected, Some([0; 20]));
    app.show_help = true;
    app.handle_mouse(click(70, 30), &areas, &snapshot);
    assert!(!app.show_help);
}

#[test]
fn the_wheel_moves_in_the_box_under_it() {
    let (snapshot, mut app, areas) = clickable();
    let wheel = |down: bool, row| {
        let kind = if down {
            MouseEventKind::ScrollDown
        } else {
            MouseEventKind::ScrollUp
        };
        mouse(kind, 20, row)
    };
    app.handle_mouse(wheel(true, 1), &areas, &snapshot);
    app.handle_mouse(wheel(true, 4), &areas, &snapshot);
    assert_eq!((app.selected, app.focus), (Some([2; 20]), Focus::Torrents));
    app.handle_mouse(wheel(false, 3), &areas, &snapshot);
    assert_eq!(app.selected, Some([1; 20]));

    // Over the Peers tab, the cursor, from the fastest peer.
    let (snapshot, mut app, areas) = clickable();
    app.ensure_peer(&snapshot);
    app.handle_mouse(wheel(true, 12), &areas, &snapshot);
    assert_eq!((app.peer, app.focus), (Some(addr(4)), Focus::Detail));
    app.handle_mouse(wheel(false, 7), &areas, &snapshot);
    assert_eq!(app.peer, Some(addr(5)));
    // Over another tab, the scroll.
    app.tab = Tab::Trackers;
    app.handle_mouse(wheel(true, 12), &areas, &snapshot);
    assert_eq!(app.detail_scroll, 1);
    // Elsewhere, nothing.
    app.handle_mouse(wheel(true, 30), &areas, &snapshot);
    assert_eq!(app.detail_scroll, 1);
}

#[test]
fn the_mouse_leaves_the_input_line_alone() {
    let (snapshot, mut app, areas) = clickable();
    app.handle_key(key(KeyCode::Char('d')), &snapshot);
    app.handle_mouse(click(20, 5), &areas, &snapshot);
    app.handle_mouse(mouse(MouseEventKind::ScrollDown, 20, 4), &areas, &snapshot);
    assert_eq!(app.selected, Some([0; 20]));
    assert_eq!(app.input.as_ref().unwrap().text, "0");
}

#[test]
fn p_pauses_a_running_torrent_and_resumes_a_stopped_one() {
    let mut snapshot = snapshot(&["a"]);
    let mut app = App::new(Theme::default());
    app.ensure_selection(&snapshot);
    let hash = [0; 20];
    assert_eq!(
        app.handle_key(key(KeyCode::Char('p')), &snapshot),
        Action::Command(Command::Pause(hash))
    );
    snapshot.torrents[0].state = TorrentState::Pausing;
    assert_eq!(
        app.handle_key(key(KeyCode::Char('p')), &snapshot),
        Action::None
    );
    for state in [
        TorrentState::Paused,
        TorrentState::Done,
        TorrentState::Interrupted,
        TorrentState::Failed("x".into()),
    ] {
        snapshot.torrents[0].state = state;
        assert_eq!(
            app.handle_key(key(KeyCode::Char('p')), &snapshot),
            Action::Command(Command::Resume(hash))
        );
    }
    // With nothing selected, nothing.
    assert_eq!(
        app.handle_key(key(KeyCode::Char('p')), &Snapshot::default()),
        Action::None
    );
}
