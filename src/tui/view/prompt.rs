//! The input line, over the key bar while something is typed, and the
//! reason the text was refused, on the line above it.

use super::keys;
use crate::tui::app::{App, Input, InputKind};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

/// Draws `input` over the key bar, `area`.
pub fn draw(frame: &mut Frame, app: &App, input: &Input, area: Rect) {
    let theme = app.theme;
    let mut spans = vec![
        Span::styled(format!(" {}", input.kind.prompt()), theme.key_style()),
        Span::styled(input.text.clone(), Style::new().fg(theme.bright())),
        Span::styled("▏", Style::new().fg(theme.down())),
        Span::raw("   "),
    ];
    let hints = match input.kind {
        InputKind::Filter => [("enter", "keep"), ("esc", "clear")],
        InputKind::DownloadLimit | InputKind::UploadLimit => [("enter", "set"), ("esc", "cancel")],
    };
    spans.extend(keys(theme, &hints));
    frame.render_widget(Clear, area);
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
    if let Some(error) = &input.error
        && area.y > 0
    {
        let above = Rect {
            y: area.y - 1,
            ..area
        };
        let line = Span::styled(format!(" {error}"), Style::new().fg(theme.error()));
        frame.render_widget(Clear, above);
        frame.render_widget(Paragraph::new(line), above);
    }
}
