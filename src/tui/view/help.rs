//! The help overlay: every key, in a box over the middle of the screen.

use super::{boxed, title};
use crate::tui::app::App;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

const KEYS: &[(&str, &str)] = &[
    ("↑ ↓  j k", "select a torrent or a peer, or scroll a tab"),
    ("PgUp PgDn", "a page of peers, or scroll the detail panel"),
    ("tab", "switch between the torrents and the detail panel"),
    ("1-5", "peers, pieces, trackers, files, info"),
    ("l", "show or hide the log"),
    ("s  S", "sort by the next column, reverse the order"),
    ("/", "filter torrents by name; esc clears it"),
    ("d  u", "set the download or upload limit, in KiB/s"),
    ("?", "this help"),
    ("q  Ctrl-C", "stop the torrents; again to quit at once"),
];

pub fn draw(frame: &mut Frame, app: &App, area: Rect) {
    let theme = app.theme;
    let width = 62.min(area.width);
    let height = (KEYS.len() as u16 + 4).min(area.height);
    let popup = Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    };
    let block = boxed(
        theme,
        title(theme, "Keys"),
        Some("any key closes".to_string()),
        true,
    );
    let mut lines = vec![Line::default()];
    for (key, text) in KEYS {
        lines.push(Line::from(vec![
            Span::styled(format!(" {key:>10}  "), theme.key_style()),
            Span::styled(*text, theme.dim_style()),
        ]));
    }
    frame.render_widget(Clear, popup);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}
