//! Drawing: one module per area of the screen. Each draws from the
//! snapshot and the app state alone, into the area it is given.

mod detail;
mod files;
mod header;
mod help;
mod info;
mod keybar;
mod log;
mod peers;
mod pieces;
mod prompt;
mod remove;
mod torrents;
mod trackers;

pub use log::LINES as LOG_LINES;

use super::app::{App, Sort, Tab};
use super::snapshot::Snapshot;
use super::theme::Theme;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Position, Rect};
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
/// Fewest lines left to the detail panel, borders included, for the log to
/// show.
const MIN_DETAIL: u16 = 7;

/// Where `draw` puts each part of the screen, for telling what a click
/// hit. Empty on a screen too small to draw.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Areas {
    pub header: Rect,
    /// The torrent table's box.
    pub torrents: Rect,
    /// The column headings that sort the table, each as wide as its column.
    pub headings: Vec<(Sort, Rect)>,
    pub torrent_rows: Rows,
    /// The detail panel's box.
    pub detail: Rect,
    /// The tab titles on its top border.
    pub tabs: Vec<(Tab, Rect)>,
    /// The Peers tab's rows, while it shows.
    pub peer_rows: Rows,
    pub log: Option<Rect>,
    pub keybar: Rect,
}

/// The rows of a table shown on screen: `rects[i]` is row `first + i`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rows {
    pub first: usize,
    pub rects: Vec<Rect>,
}

impl Rows {
    /// `shown` rows from row `first`, one a line, under the heading on the
    /// top line of `area`.
    fn under_heading(area: Rect, first: usize, shown: usize) -> Self {
        let rects = (0..shown)
            .map(|i| Rect {
                y: area.y + 1 + i as u16,
                height: 1,
                ..area
            })
            .collect();
        Self { first, rects }
    }

    /// The row of the table at `at`, if one is shown there.
    pub fn at(&self, at: Position) -> Option<usize> {
        let i = self.rects.iter().position(|rect| rect.contains(at))?;
        Some(self.first + i)
    }
}

fn too_small(area: Rect) -> bool {
    area.width < MIN_WIDTH || area.height < MIN_HEIGHT
}

/// Where everything goes on a screen `area`.
pub fn layout(area: Rect, snapshot: &Snapshot, app: &App) -> Areas {
    if too_small(area) {
        return Areas::default();
    }
    let rows = app.visible(snapshot).len() as u16;
    // Rows, heading and borders, leaving the detail panel at least half.
    let torrents = (rows.clamp(1, MAX_TORRENT_ROWS) + 3).min((area.height - 2) / 2);
    // The log, if it leaves the detail panel room for a few rows.
    let log = if app.show_log && area.height >= torrents + log::HEIGHT + MIN_DETAIL + 2 {
        log::HEIGHT
    } else {
        0
    };
    let [header, torrents_area, detail_area, log_area, keybar] = Layout::vertical([
        Constraint::Length(1),
        Constraint::Length(torrents),
        Constraint::Fill(1),
        Constraint::Length(log),
        Constraint::Length(1),
    ])
    .areas(area);
    // The Peers tab shows rows once the selected torrent's insides are in.
    let peer_rows = snapshot
        .detail
        .as_ref()
        .filter(|detail| app.tab == Tab::Peers && Some(detail.info_hash) == app.selected)
        .filter(|detail| snapshot.torrent(detail.info_hash).is_some())
        .map(|detail| peers::rows(detail, app, inner_of(detail_area)))
        .unwrap_or_default();
    Areas {
        header,
        torrents: torrents_area,
        headings: torrents::headings(torrents_area, area.width),
        torrent_rows: torrents::rows(snapshot, app, torrents_area),
        detail: detail_area,
        tabs: detail::tabs(detail_area),
        peer_rows,
        log: (log > 0).then_some(log_area),
        keybar,
    }
}

/// Draws the whole screen, and says where everything went.
pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App) -> Areas {
    let area = frame.area();
    if too_small(area) {
        let note = Paragraph::new("terminal too small").style(app.theme.dim_style());
        frame.render_widget(note, area);
        return Areas::default();
    }
    let areas = layout(area, snapshot, app);
    header::draw(frame, snapshot, app, areas.header);
    torrents::draw(frame, snapshot, app, areas.torrents);
    detail::draw(frame, snapshot, app, areas.detail);
    if let Some(log) = areas.log {
        log::draw(frame, snapshot, app, log);
    }
    keybar::draw(frame, snapshot, app, areas.keybar);
    if let Some(input) = &app.input {
        prompt::draw(frame, app, input, areas.keybar);
    }
    if let Some(removal) = &app.removal {
        remove::draw(frame, app, removal, area);
    }
    if app.show_help {
        help::draw(frame, app, area);
    }
    areas
}

/// The area inside the box drawn in `area`.
fn inner_of(area: Rect) -> Rect {
    Block::bordered().inner(area)
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
mod screen_tests;
