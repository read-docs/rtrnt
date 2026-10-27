//! The Files tab: each file of the torrent with how much of it we have.

use super::{bar, fit, human_bytes};
use crate::tui::app::App;
use crate::tui::snapshot::{Detail, TorrentSnapshot, file_progress};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

const SIZE: usize = 9;
const BAR: usize = 10;

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
    let path = (area.width as usize).saturating_sub(SIZE + BAR + 9);
    let mut lines = vec![Line::from(Span::styled(
        format!(
            " {} {} {}",
            fit("Path", path, false),
            fit("Size", SIZE, true),
            fit("Progress", BAR + 5, false)
        ),
        theme.heading_style(),
    ))];
    let height = (area.height as usize).saturating_sub(1);
    let offset = app
        .detail_scroll
        .min(layout.files.len().saturating_sub(height));
    for (index, (file, length)) in layout.files.iter().enumerate().skip(offset).take(height) {
        let mut spans = vec![
            Span::raw(format!(
                " {}",
                fit(&file.display().to_string(), path, false)
            )),
            Span::raw(format!(" {}  ", fit(&human_bytes(*length), SIZE, true))),
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
        lines.push(Line::from(spans));
    }
    frame.render_widget(Paragraph::new(lines), area);
}
