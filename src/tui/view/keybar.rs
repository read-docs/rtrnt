//! The bottom line: the keys, or what the session is doing.

use super::keys;
use crate::tui::app::App;
use crate::tui::snapshot::Snapshot;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let line = if snapshot.finished {
        Line::from(Span::styled(
            " all torrents finished, press q to exit",
            Style::new().fg(theme.warn()).add_modifier(Modifier::BOLD),
        ))
    } else if app.editing_filter {
        let mut spans = vec![
            Span::styled(" /", theme.key_style()),
            Span::styled(app.filter.clone(), Style::new().fg(theme.bright())),
            Span::styled("▏", Style::new().fg(theme.down())),
            Span::raw("   "),
        ];
        spans.extend(keys(theme, &[("enter", "keep"), ("esc", "clear")]));
        Line::from(spans)
    } else {
        let mut spans = vec![Span::raw(" ")];
        spans.extend(keys(
            theme,
            &[
                ("↑↓", "select"),
                ("tab", "panel"),
                ("1-5", "view"),
                ("q", "quit"),
            ],
        ));
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
