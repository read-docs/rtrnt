//! The log pane: the latest log lines, newest at the bottom.

use super::{boxed, fit, title};
use crate::tui::app::App;
use crate::tui::snapshot::Snapshot;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use tracing::Level;

/// Height of the pane, borders included.
pub const HEIGHT: u16 = 6;
/// Log lines the pane shows: its height less the borders.
pub const LINES: usize = HEIGHT as usize - 2;
/// Width of the torrent name column.
const NAME: usize = 18;

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let block = boxed(theme, title(theme, "Log"), None, false);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let height = inner.height as usize;
    let start = snapshot.logs.len().saturating_sub(height);
    let lines: Vec<Line> = snapshot.logs[start..]
        .iter()
        .map(|line| {
            let time = chrono::DateTime::<chrono::Local>::from(line.time);
            let level = match line.level {
                Level::ERROR => theme.error(),
                Level::WARN => theme.warn(),
                Level::INFO => theme.up(),
                _ => theme.dim(),
            };
            Line::from(vec![
                Span::styled(format!(" {} ", time.format("%H:%M:%S")), theme.dim_style()),
                Span::styled(format!("{:<5} ", line.level), Style::new().fg(level)),
                Span::styled(
                    format!(
                        "{} ",
                        fit(line.torrent.as_deref().unwrap_or(""), NAME, false)
                    ),
                    Style::new().fg(theme.name()),
                ),
                Span::raw(line.message.clone()),
            ])
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), inner);
}
