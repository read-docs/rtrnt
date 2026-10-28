//! The question before a torrent is removed, in a box over the middle of
//! the screen.

use super::{boxed, human_bytes, title, truncate};
use crate::tui::app::{App, Removal};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

/// The answers, each a key and what it does.
const KEYS: &[(&str, &str)] = &[
    ("y", "keep the files"),
    ("D", "delete them too"),
    ("esc", "cancel"),
];

pub fn draw(frame: &mut Frame, app: &App, removal: &Removal, area: Rect) {
    let theme = app.theme;
    let width = 60.min(area.width);
    let height = 6.min(area.height);
    let popup = Rect {
        x: area.x + (area.width - width) / 2,
        y: area.y + (area.height - height) / 2,
        width,
        height,
    };
    let files = match removal.files {
        Some((1, size)) => format!(" (1 file, {})", human_bytes(size)),
        Some((files, size)) => format!(" ({files} files, {})", human_bytes(size)),
        None => " (no files yet)".to_string(),
    };
    // Borders, the margin and "remove " and "?".
    let room = (width as usize).saturating_sub(files.len() + 11);
    let question = Line::from(vec![
        Span::raw(" remove "),
        Span::styled(
            truncate(&removal.name, room),
            Style::new().fg(theme.bright()),
        ),
        Span::raw(files),
        Span::raw("?"),
    ]);
    let mut answers = vec![Span::raw(" ")];
    for (i, (key, text)) in KEYS.iter().enumerate() {
        if i > 0 {
            answers.push(Span::styled(" · ", theme.dim_style()));
        }
        answers.push(Span::styled(*key, theme.key_style()));
        answers.push(Span::styled(format!(" {text}"), theme.dim_style()));
    }
    let lines = vec![
        Line::default(),
        question,
        Line::default(),
        Line::from(answers),
    ];
    let block = boxed(theme, title(theme, "Remove"), None, true);
    frame.render_widget(Clear, popup);
    frame.render_widget(Paragraph::new(lines).block(block), popup);
}
