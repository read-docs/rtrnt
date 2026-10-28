//! The bottom line: the keys, a notice, or what the session is doing.

use super::keys;
use crate::tui::Command;
use crate::tui::app::App;
use crate::tui::snapshot::Snapshot;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

/// The keys always shown.
const KEYS: &[(&str, &str)] = &[
    ("↑↓", "select"),
    ("tab", "panel"),
    ("1-5", "view"),
    ("l", "log"),
    ("s", "sort"),
    ("/", "filter"),
    ("?", "help"),
    ("q", "quit"),
];

/// Keys shown after them where there is room, after `p` and `x` for the
/// selected torrent.
const MORE_KEYS: &[(&str, &str)] = &[("d u", "limits"), ("m", "mouse")];

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let line = if let Some(notice) = &snapshot.notice {
        Line::from(Span::styled(
            format!(" {notice}"),
            Style::new().fg(theme.warn()),
        ))
    } else if snapshot.finished {
        let text = match app.pause_command(snapshot) {
            Some((Command::Resume(_), _)) => " nothing running, press p to resume or q to exit",
            _ => " nothing running, press q to exit",
        };
        Line::from(Span::styled(
            text,
            Style::new().fg(theme.warn()).add_modifier(Modifier::BOLD),
        ))
    } else {
        let mut spans = vec![Span::raw(" ")];
        spans.extend(keys(theme, KEYS));
        let width = |spans: &[Span]| spans.iter().map(Span::width).sum::<usize>();
        let pause = app.pause_command(snapshot).map(|(_, word)| ("p", word));
        let remove = app.removable(snapshot).map(|_| ("x", "remove"));
        for pair in pause.iter().chain(&remove).chain(MORE_KEYS) {
            let more: Vec<Span> = [Span::raw("  ")]
                .into_iter()
                .chain(keys(theme, std::slice::from_ref(pair)))
                .collect();
            if width(&spans) + width(&more) > area.width as usize {
                break;
            }
            spans.extend(more);
        }
        if app.stopping {
            spans.push(Span::styled(
                "   stopping, q again to quit at once",
                Style::new().fg(theme.warn()),
            ));
        }
        Line::from(spans)
    };
    frame.render_widget(Paragraph::new(line), area);
}
