//! The Files tab: each file of the torrent with whether it is wanted and how
//! much of it we have.

use super::{bar, bold, fit, human_bytes, scroll_to, selected_style};
use crate::selection::Priority;
use crate::tui::app::{App, Focus};
use crate::tui::snapshot::{Detail, TorrentSnapshot, file_progress};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

const SIZE: usize = 9;
const BAR: usize = 10;
const PRIORITY: usize = 6;

/// How a priority reads in the table.
fn priority_word(priority: Priority) -> &'static str {
    match priority {
        Priority::Skip => "skip",
        Priority::Normal => "normal",
        Priority::High => "high",
    }
}

pub fn draw(
    frame: &mut Frame,
    torrent: &TorrentSnapshot,
    detail: Option<&Detail>,
    app: &App,
    area: Rect,
) {
    let theme = app.theme;
    let Some(layout) = &torrent.layout else {
        let note = Span::styled(" the files are not known yet", theme.dim_style());
        frame.render_widget(Paragraph::new(note), area);
        return;
    };
    let have = detail.and_then(|detail| detail.pieces.as_ref());
    let done = have.map(|pieces| file_progress(layout, &pieces.have));
    let path = (area.width as usize).saturating_sub(PRIORITY + SIZE + BAR + 10);
    let mut lines = vec![Line::from(Span::styled(
        format!(
            " {} {} {} {}",
            fit("Path", path, false),
            fit("Get", PRIORITY, false),
            fit("Size", SIZE, true),
            fit("Progress", BAR + 5, false)
        ),
        theme.heading_style(),
    ))];
    let height = (area.height as usize).saturating_sub(1);
    let cursor = app.file.min(layout.files.len().saturating_sub(1));
    let offset = scroll_to(cursor, 0, height);
    // The cursor shows while the panel has the focus.
    let cursor = (app.focus == Focus::Detail).then_some(cursor);
    for (index, (file, length)) in layout.files.iter().enumerate().skip(offset).take(height) {
        let priority = torrent.priorities.get(index).copied().unwrap_or_default();
        let skipped = priority == Priority::Skip;
        let is_cursor = cursor == Some(index);
        let text = |text: String| {
            if skipped {
                Span::styled(text, theme.dim_style())
            } else {
                Span::raw(text)
            }
        };
        let name = fit(&file.display().to_string(), path, false);
        let word = fit(priority_word(priority), PRIORITY, false);
        let mut spans = vec![
            if is_cursor {
                Span::styled("▌", Style::new().fg(theme.down()))
            } else {
                Span::raw(" ")
            },
            if is_cursor {
                Span::styled(name, bold(theme.bright()))
            } else {
                text(name)
            },
            Span::raw(" "),
            match priority {
                Priority::Skip => Span::styled(word, theme.dim_style()),
                Priority::Normal => Span::raw(word),
                Priority::High => Span::styled(word, bold(theme.warn())),
            },
            text(format!(" {}  ", fit(&human_bytes(*length), SIZE, true))),
        ];
        match done.as_ref().map(|done| done[index]) {
            Some(bytes) => {
                let fraction = if *length == 0 {
                    1.0
                } else {
                    bytes as f64 / *length as f64
                };
                let color = if bytes == *length {
                    theme.up()
                } else {
                    theme.down()
                };
                spans.extend(bar(theme, BAR, fraction, color));
                spans.push(Span::styled(
                    format!(" {:>3.0}%", (fraction * 100.0).floor()),
                    theme.dim_style(),
                ));
            }
            None => spans.push(Span::styled("—", Style::new().fg(theme.dim()))),
        }
        let line = Line::from(spans);
        lines.push(if is_cursor {
            line.style(selected_style(theme))
        } else {
            line
        });
    }
    frame.render_widget(Paragraph::new(lines), area);
}
