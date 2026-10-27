use super::*;
use crate::tui::app::App;
use crate::tui::demo;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

/// The text of the cells in `area` of the buffer, row by row, for tests.
pub(crate) fn text(buffer: &ratatui::buffer::Buffer, area: ratatui::layout::Rect) -> Vec<String> {
    (area.top()..area.bottom())
        .map(|y| {
            (area.left()..area.right())
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

pub(crate) fn render(width: u16, height: u16, snapshot: &Snapshot, app: &App) -> Buffer {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(|frame| draw(frame, snapshot, app)).unwrap();
    terminal.backend().buffer().clone()
}

/// An app showing `snapshot`, without the log, so the detail panel
/// has the room the tests expect; the log has a test of its own.
pub(crate) fn app(snapshot: &Snapshot) -> App {
    let mut app = App::new(Theme::default());
    app.ensure_selection(snapshot);
    app.show_log = false;
    app
}

#[test]
fn header_and_torrent_table_at_100_columns() {
    let snapshot = demo::fixture();
    let app = app(&snapshot);
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(
        lines[0].starts_with(
            " rtrnt │ ↓ 6.6 MiB/s  ↑ 1.1 MiB/s (2 MiB/s) │ 5 torrents  71/200 peers  DHT 312 nodes "
        ),
        "{}",
        lines[0]
    );
    assert!(lines[0].ends_with(" 14:02 "));
    assert!(lines[1].starts_with("╭ Torrents ─"));
    assert!(lines[1].ends_with(" sort: added ▾ ╮"));
    assert_eq!(
        lines[2],
        "│ Name                           Size Progress        Status               ↓          ↑ Peers  ETA │"
    );
    assert_eq!(
        lines[3],
        "│▌debian-13.7.0-amd64-netin…  754 MiB ██████▎───  61% downloading  4.2 MiB/s  367 KiB/s    38   1m │"
    );
    assert_eq!(
        lines[6],
        "│ Big Buck Bunny                    — ████▎─────  42% metadata             —          —     4    — │"
    );
    // The selected row is on the selection background, the others not.
    let theme = Theme::default();
    assert_eq!(buffer[(10, 3)].bg, theme.selection());
    assert_eq!(buffer[(1, 3)].fg, theme.down());
    assert_ne!(buffer[(10, 4)].bg, theme.selection());
    assert!(lines[9].starts_with("╭ 1 Peers ─ 2 Pieces ─ 3 Trackers ─ 4 Files ─ 5 Info ─"));
    assert!(lines[9].ends_with(" debian-13.7.0-amd64-netinst.iso ╮"));
    assert!(lines[31].starts_with(" ↑↓ select  tab panel  1-5 view"));
}

#[test]
fn columns_drop_at_80_columns() {
    let snapshot = demo::fixture();
    let app = app(&snapshot);
    let buffer = render(80, 24, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert_eq!(
        lines[2],
        "│ Name                       Size Progress        Status               ↓ Peers │"
    );
    assert!(lines[3].starts_with("│▌debian-13.7.0-amd64-n…  754 MiB "));
    assert!(lines[0].contains("71/200 peers") && lines[0].ends_with(" 14:02 "));
}

#[test]
fn peers_tab_lists_peers_fastest_first() {
    let snapshot = demo::fixture();
    let app = app(&snapshot);
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[10].starts_with(
        "│ Address                  Client                  Flags              ↓          ↑  Has"
    ));
    assert_eq!(
        lines[11],
        "│ 185.21.216.143:51413     Transmission 4.0.6      D U   F X  1.1 MiB/s   48 KiB/s  ██████████ seed│"
    );
    assert_eq!(
        lines[13],
        "│ 91.121.44.7:6881         libtorrent 2.0.11       D U   F    640 KiB/s  112 KiB/s  ████████▎─  82%│"
    );
    assert!(
        lines[18]
            .starts_with("│ 73.92.10.211:42069       Deluge 2.2.0            d U I F        0 B/s")
    );
    assert!(lines[26].starts_with("│ 9 peers  ·  D/d down unchoked/interested"));
    assert!(lines[28].starts_with("│ ↓ ▇"), "{}", lines[28]);
    assert!(
        lines[28].ends_with("   4.2 MiB/s  peak 5.2 MiB/s │"),
        "{}",
        lines[28]
    );
    assert!(lines[29].contains("367 KiB/s  peak 402 KiB/s"));
    let theme = Theme::default();
    assert_eq!(buffer[(50, 11)].fg, theme.warn());
    assert_eq!(buffer[(60, 11)].fg, theme.down());
}

#[test]
fn peers_scroll_and_lose_the_client_column_when_narrow() {
    let snapshot = demo::fixture();
    let mut app = app(&snapshot);
    app.select_peer(&snapshot, 100);
    let buffer = render(80, 24, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[10].starts_with("│ Address                  Flags "));
    // Scrolled to the cursor on the last row; it shows only with the focus.
    assert!(lines[11].starts_with("│ 91.121.44.7:6881"), "{}", lines[11]);
    assert!(lines[18].starts_with("│ 3-9 of 9 peers"), "{}", lines[18]);
}

#[test]
fn the_peer_cursor_shows_its_peer_under_the_table() {
    let mut snapshot = demo::fixture();
    let mut app = app(&snapshot);
    app.focus = crate::tui::app::Focus::Detail;
    app.select_peer(&snapshot, 2);
    let mib = 1024.0 * 1024.0;
    let addr = "91.121.44.7:6881".parse().unwrap();
    snapshot.detail.as_mut().unwrap().peer_history =
        Some((addr, vec![0.0, mib, 2.0 * mib], vec![0.0; 3]));
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[11].starts_with("│ 185.21.216.143:51413 "));
    assert!(
        lines[13].starts_with("│▌91.121.44.7:6881 "),
        "{}",
        lines[13]
    );
    let theme = Theme::default();
    assert_eq!(buffer[(30, 13)].bg, theme.selection());
    assert_ne!(buffer[(30, 11)].bg, theme.selection());
    assert!(
        lines[27].starts_with("│ 91.121.44.7:6881 · libtorrent 2.0.11 "),
        "{}",
        lines[27]
    );
    assert!(
        lines[28].ends_with("▁▄█   640 KiB/s  peak 2.0 MiB/s │"),
        "{}",
        lines[28]
    );
    assert!(lines[29].ends_with("▁▁▁   112 KiB/s  peak 112 KiB/s │"));

    // With the torrents focused, the torrent's, and no cursor.
    app.focus = crate::tui::app::Focus::Torrents;
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[13].starts_with("│ 91.121.44.7:6881 "));
    assert_eq!(lines[27].trim_matches(['│', ' ']), "");
    assert!(lines[28].ends_with("   4.2 MiB/s  peak 5.2 MiB/s │"));
}

#[test]
fn pieces_tab_maps_a_known_bitfield() {
    let mut snapshot = demo::fixture();
    let detail = snapshot.detail.as_mut().unwrap();
    // 10 had, one in flight, one rare, one missing, one had.
    let mut have = vec![true; 10];
    have.extend([false, false, false, true]);
    let mut in_flight = vec![false; 14];
    in_flight[10] = true;
    let mut availability = vec![9; 14];
    availability[11] = 1;
    detail.pieces = Some(crate::stats::PieceSnapshot {
        have,
        in_flight,
        availability,
    });
    let mut app = app(&snapshot);
    app.tab = crate::tui::app::Tab::Pieces;
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[9].ends_with(" 1 cell ≈ 1 pieces ╮"), "{}", lines[9]);
    assert!(lines[10].starts_with("│ ██████████▓░░█ "), "{}", lines[10]);
    let theme = Theme::default();
    let fg = |x| buffer[(x, 10)].fg;
    assert_eq!(fg(2), theme.up());
    assert_eq!(fg(12), theme.warn());
    assert_eq!(fg(13), theme.error());
    assert_eq!(fg(14), theme.dim());
    assert!(
        lines[27]
            .starts_with("│ █ have   ▒ partial   ▓ downloading   ░ missing   ░ rare (<4 peers)")
    );
    assert!(
        lines[28].starts_with(
            "│ seeds 9  peers 0  pool 418  copies 1.9  rarest piece 1 peers  pieces 11/14"
        ),
        "{}",
        lines[28]
    );
    assert!(lines[29].starts_with("│ sources ■ tracker 212  ■ dht 118  ■ pex 70  ■ incoming 12"));

    // A big torrent shares cells.
    let snapshot = demo::fixture();
    let buffer = render(100, 32, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[9].ends_with(" 1 cell ≈ 2 pieces ╮"));
}

fn tab_lines(snapshot: &Snapshot, tab: crate::tui::app::Tab) -> Vec<String> {
    let mut app = app(snapshot);
    app.tab = tab;
    let buffer = render(100, 32, snapshot, &app);
    text(&buffer, buffer.area)[10..30].to_vec()
}

#[test]
fn trackers_tab_groups_by_tier() {
    let lines = tab_lines(&demo::fixture(), crate::tui::app::Tab::Trackers);
    assert_eq!(
        lines[0],
        "│ Tier URL                                       Status                  Peers  Seeds  Leech   Next│"
    );
    assert_eq!(
        lines[1],
        "│ 1    udp://tracker.opentrackr.org:1337/announ… ok                         50   1204     87    24m│"
    );
    assert!(lines[3].starts_with(
        "│      udp://tracker.example.org:6969/announce   connection timed out        —"
    ));
    assert!(lines[4].starts_with("│ 3    https://tracker.example.net/announce      not contacted"));
    assert!(lines[6].starts_with(
        "│      DHT                                       ok, 312 nodes             118"
    ));
    assert!(lines[7].starts_with(
        "│      PeX                                       ok                         70"
    ));
}

#[test]
fn files_tab_counts_pieces_pro_rata() {
    let mut snapshot = demo::fixture();
    // Two files over three pieces of 256 KiB: the middle piece is
    // shared, and only it is had.
    let layout = crate::stats::Layout {
        size: 640 * 1024,
        piece_length: 256 * 1024,
        pieces: 3,
        private: true,
        files: vec![
            ("dir/a.bin".into(), 384 * 1024),
            ("dir/b.bin".into(), 256 * 1024),
        ],
    };
    snapshot.torrents[0].layout = Some(layout);
    snapshot.detail.as_mut().unwrap().pieces = Some(crate::stats::PieceSnapshot {
        have: vec![false, true, false],
        in_flight: vec![false; 3],
        availability: vec![1; 3],
    });
    let lines = tab_lines(&snapshot, crate::tui::app::Tab::Files);
    assert!(lines[0].starts_with("│ Path "));
    assert!(lines[1].starts_with("│ dir/a.bin "));
    assert!(
        lines[1].ends_with(" 384 KiB  ███▍──────  33%│"),
        "{}",
        lines[1]
    );
    assert!(
        lines[2].ends_with(" 256 KiB  █████─────  50%│"),
        "{}",
        lines[2]
    );
    // A private torrent uses neither the DHT nor peer exchange.
    let lines = tab_lines(&snapshot, crate::tui::app::Tab::Trackers);
    assert!(lines[6].contains("DHT") && lines[6].contains("off (private torrent)"));
}

#[test]
fn info_tab_lists_the_facts() {
    let mut snapshot = demo::fixture();
    snapshot.torrents[0].downloaded = 4 * 1024 * 1024;
    snapshot.torrents[0].uploaded = 1024 * 1024;
    let lines = tab_lines(&snapshot, crate::tui::app::Tab::Info);
    let expected = [
        "│ state         downloading",
        "│ info hash     0101010101010101010101010101010101010101",
        "│ size          754 MiB  3016 pieces × 256 KiB",
        "│ pieces        1875 / 3016",
        "│ private       no",
        "│ files         1",
        "│ save path     /home/me/Downloads",
        "│ this session  ↓ 4.0 MiB  ↑ 1.0 MiB  ratio 0.25",
        "│ active        2h 42m 00s",
    ];
    for (line, expected) in lines.iter().zip(expected) {
        assert!(line.starts_with(expected), "{line}");
    }
    // A magnet link says how its metadata is coming along.
    let mut app = app(&snapshot);
    app.selected = Some(snapshot.torrents[3].info_hash);
    app.tab = crate::tui::app::Tab::Info;
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(
        lines[10].starts_with("│ state         metadata  3/7 pieces from 4 peers"),
        "{}",
        lines[10]
    );
    assert!(lines[12].starts_with("│ size          not known yet"));
}

#[test]
fn the_log_sits_under_the_detail_panel() {
    let snapshot = demo::fixture();
    let mut app = app(&snapshot);
    app.show_log = true;
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(lines[24].starts_with("╰────"), "{}", lines[24]);
    assert!(lines[25].starts_with("╭ Log ─"), "{}", lines[25]);
    // The newest four lines, oldest first.
    assert!(
        lines[26].contains(" INFO  Big Buck Bunny     metadata 3/7 pieces from 4 peers"),
        "{}",
        lines[26]
    );
    assert!(
        lines[27].contains(" WARN  debian-13.7.0-amd… banning peer: too many corrupt pieces"),
        "{}",
        lines[27]
    );
    assert!(lines[29].contains(" INFO  debian-13.7.0-amd… tracker announce succeeded peers=50"));
    assert!(lines[30].starts_with("╰────"));
    let theme = Theme::default();
    let warn = lines[27].find("WARN").unwrap();
    let x = lines[27][..warn].chars().count() as u16;
    assert_eq!(buffer[(x, 27)].fg, theme.warn());
    assert_eq!(buffer[(x + 6, 27)].fg, theme.name());
    assert_eq!(buffer[(2, 27)].fg, theme.dim());
    assert!(lines[31].contains("l log"));

    // At 80x24 it still fits, and on a short screen it gives way.
    let buffer = render(80, 24, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[17].starts_with("╭ Log ─"));
    let buffer = render(80, 20, &snapshot, &app);
    assert!(
        !text(&buffer, buffer.area)
            .iter()
            .any(|line| line.starts_with("╭ Log"))
    );
}

#[test]
fn help_lists_the_keys_over_the_screen() {
    let snapshot = demo::fixture();
    let mut app = app(&snapshot);
    app.show_help = true;
    let buffer = render(80, 24, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    let top = lines
        .iter()
        .position(|line| line.contains("╭ Keys ─"))
        .unwrap();
    assert!(lines[top].contains(" any key closes ╮"));
    assert!(lines[top + 2].contains("  ↑ ↓  j k  select a torrent or a peer, or scroll a tab"));
    assert!(
        lines
            .iter()
            .any(|line| line.contains("  q  Ctrl-C  stop the torrents; again to quit at once"))
    );
    // Drawn over what is under it, which shows around it.
    assert!(lines[3].starts_with("│▌debian"));
}

#[test]
fn a_small_terminal_gets_a_note() {
    let snapshot = demo::fixture();
    let app = app(&snapshot);
    let buffer = render(59, 24, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[0].starts_with("terminal too small"));
    let buffer = render(60, 16, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[0].starts_with(" rtrnt"));
}

#[test]
fn the_key_bar_says_when_everything_has_finished() {
    let mut snapshot = demo::fixture();
    snapshot.finished = true;
    let app = app(&snapshot);
    let buffer = render(100, 32, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[31].starts_with(" all torrents finished, press q to exit"));
}

#[test]
fn the_header_shows_the_limits_next_to_the_rates() {
    let mut snapshot = demo::fixture();
    snapshot.session.rate_limits = (0, 0);
    let app = app(&snapshot);
    let header = |snapshot: &Snapshot| {
        text(&render(100, 32, snapshot, &app), Rect::new(0, 0, 100, 1))[0].clone()
    };
    assert!(
        header(&snapshot).starts_with(
            " rtrnt │ ↓ 6.6 MiB/s  ↑ 1.1 MiB/s │ 5 torrents  71/200 peers  DHT 312 nodes  port 6881"
        ),
        "{}",
        header(&snapshot)
    );
    snapshot.session.rate_limits = (2048, 300);
    let line = header(&snapshot);
    assert!(
        line.starts_with(" rtrnt │ ↓ 6.6 MiB/s (2 MiB/s)  ↑ 1.1 MiB/s (300 KiB/s) │ 5 torrents "),
        "{line}"
    );
    let theme = Theme::default();
    let buffer = render(100, 32, &snapshot, &app);
    let x = line[..line.find("(2 MiB/s)").unwrap()].chars().count() as u16;
    assert_eq!(buffer[(x, 0)].fg, theme.dim());
}

#[test]
fn the_prompt_line_sits_over_the_key_bar() {
    let snapshot = demo::fixture();
    let mut app = app(&snapshot);
    app.handle_key(
        KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE),
        &snapshot,
    );
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(
        lines[31].starts_with(" download limit, KiB/s (0: none): 0▏   enter set  esc cancel"),
        "{}",
        lines[31]
    );
    // The line above is left alone until the text is refused.
    assert!(lines[30].starts_with("╰────"), "{}", lines[30]);
    app.handle_key(
        KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE),
        &snapshot,
    );
    app.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), &snapshot);
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(
        lines[30].starts_with(" enter a whole number of KiB/s, 0 for none "),
        "{}",
        lines[30]
    );
    assert_eq!(buffer[(1, 30)].fg, Theme::default().error());
    assert!(lines[31].starts_with(" download limit, KiB/s (0: none): 0x▏"));

    // The filter is typed on it too.
    let mut app = self::app(&snapshot);
    for c in "/deb".chars() {
        app.handle_key(
            KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE),
            &snapshot,
        );
    }
    let buffer = render(100, 32, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[31].starts_with(" /deb▏   enter keep  esc clear"));
}

#[test]
fn a_notice_shows_in_place_of_the_keys() {
    let mut snapshot = demo::fixture();
    let app = app(&snapshot);
    // Without one, the keys; the limit keys where there is room.
    let buffer = render(100, 32, &snapshot, &app);
    assert!(text(&buffer, buffer.area)[31].contains("  q quit  d u limits"));
    let buffer = render(80, 24, &snapshot, &app);
    let line = &text(&buffer, buffer.area)[23];
    assert!(
        line.contains("  q quit") && !line.contains("limits"),
        "{line}"
    );
    snapshot.notice = Some("rate limits: download 100 KiB/s, upload none".into());
    let buffer = render(100, 32, &snapshot, &app);
    let lines = text(&buffer, buffer.area);
    assert!(
        lines[31].starts_with(" rate limits: download 100 KiB/s, upload none "),
        "{}",
        lines[31]
    );
    assert_eq!(buffer[(1, 31)].fg, Theme::default().warn());
}
