//! Drawing: one module per area of the screen. Each draws from the
//! snapshot and the app state alone, into the area it is given.

mod detail;
mod files;
mod header;
mod info;
mod keybar;
mod peers;
mod pieces;
mod torrents;
mod trackers;

use super::app::App;
use super::snapshot::Snapshot;
use super::theme::Theme;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, BorderType, Paragraph};
use std::time::Duration;
use unicode_width::UnicodeWidthStr;

/// Smallest screen drawn; anything less gets a note instead.
pub const MIN_WIDTH: u16 = 60;
pub const MIN_HEIGHT: u16 = 16;
/// Most torrent rows shown at once.
const MAX_TORRENT_ROWS: u16 = 10;

/// Draws the whole screen.
pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        let note = Paragraph::new("terminal too small").style(app.theme.dim_style());
        frame.render_widget(note, area);
        return;
    }
    let rows = app.visible(snapshot).len() as u16;
    // Rows, heading and borders, leaving the detail panel at least half.
    let torrents = (rows.clamp(1, MAX_TORRENT_ROWS) + 3).min((area.height - 2) / 2);
    let [header, torrents_area, detail_area, keybar] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(torrents),
        Constraint::Fill(1),
        Constraint::Length(1),
    ])
    .areas(area);
    header::draw(frame, snapshot, app, header);
    torrents::draw(frame, snapshot, app, torrents_area);
    detail::draw(frame, snapshot, app, detail_area);
    keybar::draw(frame, snapshot, app, keybar);
}

/// A rounded box in the border color, titled `title` in bold, with `hint`
/// dim on the right of the top border.
fn boxed<'a>(theme: Theme, title: Line<'a>, hint: Option<String>, focused: bool) -> Block<'a> {
    let border = if focused {
        Style::new().fg(theme.dim())
    } else {
        theme.border_style()
    };
    let mut block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(border)
        .title_top(title);
    if let Some(hint) = hint {
        block = block.title_top(
            Line::from(Span::styled(format!(" {hint} "), theme.dim_style())).right_aligned(),
        );
    }
    block
}

/// A box title: ` title `, bold.
fn title(theme: Theme, text: &str) -> Line<'static> {
    Line::from(Span::styled(format!(" {text} "), theme.title_style()))
}

const EIGHTHS: [char; 8] = [' ', '▏', '▎', '▍', '▌', '▋', '▊', '▉'];

/// A progress bar `width` cells wide, `fraction` full, in eighth blocks of
/// `color` on a thin dim track.
fn bar(theme: Theme, width: usize, fraction: f64, color: Color) -> Vec<Span<'static>> {
    let eighths = (fraction.clamp(0.0, 1.0) * width as f64 * 8.0).round() as usize;
    let (full, part) = (eighths / 8, eighths % 8);
    let mut filled = "█".repeat(full);
    if part > 0 {
        filled.push(EIGHTHS[part]);
    }
    let track = width.saturating_sub(full + usize::from(part > 0));
    vec![
        Span::styled(filled, Style::new().fg(color)),
        Span::styled("─".repeat(track), theme.border_style()),
    ]
}

const SPARKS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

/// The last `width` of `values` as a sparkline, scaled to their peak.
fn sparkline(values: &[f64], width: usize) -> String {
    let values = &values[values.len().saturating_sub(width)..];
    let peak = values.iter().copied().fold(0.0, f64::max);
    let line: String = values
        .iter()
        .map(|&value| {
            if peak <= 0.0 {
                SPARKS[0]
            } else {
                SPARKS[((value / peak * 7.99) as usize).min(7)]
            }
        })
        .collect();
    format!("{line:>width$}")
}

/// `bytes` with a binary unit: one decimal under 10, none above.
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if value < 9.95 {
        format!("{value:.1} {}", UNITS[unit])
    } else {
        format!("{value:.0} {}", UNITS[unit])
    }
}

/// A rate in bytes a second.
pub fn human_rate(rate: f64) -> String {
    format!("{}/s", human_bytes(rate.max(0.0).round() as u64))
}

/// A duration in its largest unit, as in an ETA column: `45s`, `43m`.
pub fn short_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    match secs {
        0..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86400 => format!("{}h", secs / 3600),
        _ if secs / 86400 < 100 => format!("{}d", secs / 86400),
        _ => "∞".to_string(),
    }
}

/// A duration in full: `1h 02m 03s`.
pub fn long_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    match h {
        0 if m == 0 => format!("{s}s"),
        0 => format!("{m}m {s:02}s"),
        _ => format!("{h}h {m:02}m {s:02}s"),
    }
}

/// `text` cut to `width` columns, with an ellipsis if cut.
fn truncate(text: &str, width: usize) -> String {
    if text.width() <= width {
        return text.to_string();
    }
    let mut cut = String::new();
    let mut used = 0;
    for c in text.chars() {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w + 1 > width {
            break;
        }
        cut.push(c);
        used += w;
    }
    if width > 0 {
        cut.push('…');
    }
    cut
}

/// `text` cut or padded to exactly `width` columns, on the left or right.
fn fit(text: &str, width: usize, right: bool) -> String {
    let text = truncate(text, width);
    let pad = " ".repeat(width.saturating_sub(text.width()));
    if right { pad + &text } else { text + &pad }
}

/// `key text` pairs as in the key bar: the key bold, its text dim.
fn keys(theme: Theme, pairs: &[(&str, &str)]) -> Vec<Span<'static>> {
    let mut spans = Vec::new();
    for (i, (key, text)) in pairs.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("  "));
        }
        spans.push(Span::styled(key.to_string(), theme.key_style()));
        spans.push(Span::styled(format!(" {text}"), theme.dim_style()));
    }
    spans
}

/// The style of the selected row: the selection background.
fn selected_style(theme: Theme) -> Style {
    Style::new().bg(theme.selection())
}

fn bold(color: Color) -> Style {
    Style::new().fg(color).add_modifier(Modifier::BOLD)
}

/// Rows `offset..` of a table of `len` rows, scrolled as little as needed
/// for row `selected` to show in `height` rows.
fn scroll_to(selected: usize, offset: usize, height: usize) -> usize {
    if height == 0 {
        return 0;
    }
    if selected < offset {
        selected
    } else if selected >= offset + height {
        selected + 1 - height
    } else {
        offset
    }
}

/// The text of the cells in `area` of the buffer, row by row, for tests.
#[cfg(test)]
pub(crate) fn text(buffer: &ratatui::buffer::Buffer, area: ratatui::layout::Rect) -> Vec<String> {
    (area.top()..area.bottom())
        .map(|y| {
            (area.left()..area.right())
                .map(|x| buffer[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_and_rates_read_like_the_mockups() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1023), "1023 B");
        assert_eq!(human_bytes(310 * 1024), "310 KiB");
        assert_eq!(human_bytes(754 * 1024 * 1024), "754 MiB");
        assert_eq!(human_bytes(1288490188), "1.2 GiB");
        assert_eq!(human_rate(4.2 * 1024.0 * 1024.0), "4.2 MiB/s");
        assert_eq!(human_rate(-1.0), "0 B/s");
    }

    #[test]
    fn durations() {
        assert_eq!(short_duration(Duration::from_secs(45)), "45s");
        assert_eq!(short_duration(Duration::from_secs(62)), "1m");
        assert_eq!(short_duration(Duration::from_secs(43 * 60 + 10)), "43m");
        assert_eq!(short_duration(Duration::from_secs(5 * 3600)), "5h");
        assert_eq!(short_duration(Duration::from_secs(1000 * 86400)), "∞");
        assert_eq!(long_duration(Duration::from_secs(62)), "1m 02s");
        assert_eq!(long_duration(Duration::from_secs(3723)), "1h 02m 03s");
    }

    #[test]
    fn bars_fill_in_eighths() {
        let theme = Theme::default();
        let text = |fraction| -> String {
            bar(theme, 4, fraction, Color::Cyan)
                .iter()
                .map(|span| span.content.to_string())
                .collect()
        };
        assert_eq!(text(0.0), "────");
        assert_eq!(text(0.5), "██──");
        assert_eq!(text(0.5 + 1.0 / 32.0), "██▏─");
        assert_eq!(text(1.0), "████");
    }

    #[test]
    fn sparklines_scale_to_the_peak() {
        assert_eq!(sparkline(&[0.0, 1.0, 2.0], 3), "▁▄█");
        assert_eq!(sparkline(&[1.0], 3), "  █");
        assert_eq!(sparkline(&[0.0; 2], 2), "▁▁");
    }

    #[test]
    fn text_is_cut_with_an_ellipsis() {
        assert_eq!(truncate("debian.iso", 20), "debian.iso");
        assert_eq!(truncate("debian-13.7.0-amd64.iso", 10), "debian-13…");
        assert_eq!(fit("ab", 4, true), "  ab");
        assert_eq!(fit("日本語の名前", 7, false), "日本語…");
    }
}

#[cfg(test)]
mod screen_tests {
    use super::*;
    use crate::tui::app::App;
    use crate::tui::demo;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;

    pub(crate) fn render(width: u16, height: u16, snapshot: &Snapshot, app: &App) -> Buffer {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal.draw(|frame| draw(frame, snapshot, app)).unwrap();
        terminal.backend().buffer().clone()
    }

    pub(crate) fn app(snapshot: &Snapshot) -> App {
        let mut app = App::new(Theme::default());
        app.ensure_selection(snapshot);
        app
    }

    #[test]
    fn header_and_torrent_table_at_100_columns() {
        let snapshot = demo::fixture();
        let app = app(&snapshot);
        let buffer = render(100, 32, &snapshot, &app);
        let lines = text(&buffer, buffer.area);
        assert!(lines[0].starts_with(" rtrnt │ ↓ 6.6 MiB/s  ↑ 1.1 MiB/s │ 5 torrents  71/200 peers  DHT 312 nodes  port 6881"), "{}", lines[0]);
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
        assert!(lines[10].starts_with("│ Address                  Client                  Flags              ↓          ↑  Has"));
        assert_eq!(
            lines[11],
            "│ 185.21.216.143:51413     Transmission 4.0.6      D U   F X  1.1 MiB/s   48 KiB/s  ██████████ seed│"
        );
        assert_eq!(
            lines[13],
            "│ 91.121.44.7:6881         libtorrent 2.0.11       D U   F    640 KiB/s  112 KiB/s  ████████▎─  82%│"
        );
        assert!(lines[18].starts_with(
            "│ 73.92.10.211:42069       Deluge 2.2.0            d U I F        0 B/s"
        ));
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
        app.detail_scroll = 100;
        let buffer = render(80, 24, &snapshot, &app);
        let lines = text(&buffer, buffer.area);
        assert!(lines[10].starts_with("│ Address                  Flags "));
        // Scrolled as far as the rows go, and no further.
        assert!(lines[11].starts_with("│ 91.121.44.7:6881"), "{}", lines[11]);
        assert!(lines[18].starts_with("│ 3-9 of 9 peers"), "{}", lines[18]);
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
            lines[27].starts_with(
                "│ █ have   ▒ partial   ▓ downloading   ░ missing   ░ rare (<4 peers)"
            )
        );
        assert!(
            lines[28].starts_with(
                "│ seeds 9  peers 0  pool 418  copies 1.9  rarest piece 1 peers  pieces 11/14"
            ),
            "{}",
            lines[28]
        );
        assert!(
            lines[29].starts_with("│ sources ■ tracker 212  ■ dht 118  ■ pex 70  ■ incoming 12")
        );

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
        assert!(
            lines[4].starts_with("│ 3    https://tracker.example.net/announce      not contacted")
        );
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
        assert!(
            text(&buffer, buffer.area)[31].starts_with(" all torrents finished, press q to exit")
        );
    }
}
