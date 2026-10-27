//! The Info tab: facts about the selected torrent.

use super::{human_bytes, long_duration, torrents};
use crate::stats::TorrentState;
use crate::tui::app::App;
use crate::tui::snapshot::{Detail, TorrentSnapshot};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};

/// Width of the labels.
const LABEL: usize = 14;

pub fn draw(
    frame: &mut Frame,
    torrent: &TorrentSnapshot,
    detail: Option<&Detail>,
    app: &App,
    area: Rect,
) {
    let theme = app.theme;
    let value = |text: String| Span::styled(text, Style::new().fg(theme.bright()));
    let mut facts: Vec<(&str, Vec<Span>)> = Vec::new();
    let (status, color) = torrents::status(theme, torrent);
    let mut state = vec![Span::styled(status, Style::new().fg(color))];
    match &torrent.state {
        TorrentState::FetchingMetadata {
            pieces,
            total,
            peers,
        } => state.push(Span::styled(
            match total {
                0 => format!("  waiting for a peer to say its size, {peers} peers"),
                _ => format!("  {pieces}/{total} pieces from {peers} peers"),
            },
            theme.dim_style(),
        )),
        TorrentState::Checking { done, total } => state.push(Span::styled(
            format!("  {} of {}", human_bytes(*done), human_bytes(*total)),
            theme.dim_style(),
        )),
        TorrentState::Failed(e) => state.push(Span::styled(
            format!("  {e}"),
            Style::new().fg(theme.error()),
        )),
        _ => {}
    }
    facts.push(("state", state));
    let hash: String = torrent
        .info_hash
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    facts.push(("info hash", vec![value(hash)]));
    match &torrent.layout {
        Some(layout) => {
            facts.push((
                "size",
                vec![
                    value(human_bytes(layout.size)),
                    Span::styled(
                        format!(
                            "  {} pieces × {}",
                            layout.pieces,
                            human_bytes(layout.piece_length)
                        ),
                        theme.dim_style(),
                    ),
                ],
            ));
            let had = detail
                .and_then(|detail| detail.pieces.as_ref())
                .map(|pieces| pieces.have.iter().filter(|&&have| have).count());
            let had = had.map_or("—".to_string(), |had| had.to_string());
            facts.push(("pieces", vec![value(format!("{had} / {}", layout.pieces))]));
            facts.push((
                "private",
                vec![value(if layout.private { "yes" } else { "no" }.to_string())],
            ));
            facts.push(("files", vec![value(layout.files.len().to_string())]));
        }
        None => facts.push((
            "size",
            vec![Span::styled("not known yet", theme.dim_style())],
        )),
    }
    facts.push((
        "save path",
        vec![value(torrent.save_path.display().to_string())],
    ));
    let ratio = match (torrent.downloaded, torrent.uploaded) {
        (0, 0) => "—".to_string(),
        (0, _) => "∞".to_string(),
        (down, up) => format!("{:.2}", up as f64 / down as f64),
    };
    facts.push((
        "this session",
        vec![
            Span::styled(
                format!("↓ {}", human_bytes(torrent.downloaded)),
                Style::new().fg(theme.down()),
            ),
            Span::raw("  "),
            Span::styled(
                format!("↑ {}", human_bytes(torrent.uploaded)),
                Style::new().fg(theme.up()),
            ),
            Span::styled("  ratio ", theme.dim_style()),
            value(ratio),
        ],
    ));
    facts.push(("active", vec![value(long_duration(torrent.active))]));

    let lines: Vec<Line> = facts
        .into_iter()
        .skip(app.detail_scroll)
        .map(|(label, spans)| {
            let mut line = vec![Span::styled(format!(" {label:<LABEL$}"), theme.dim_style())];
            line.extend(spans);
            Line::from(line)
        })
        .collect();
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}
