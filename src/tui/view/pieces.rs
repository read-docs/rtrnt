//! The Pieces tab: a map of the torrent's pieces, a cell for each run of
//! them, and the swarm's numbers under it.

use crate::stats::PieceSnapshot;
use crate::tui::app::App;
use crate::tui::snapshot::{Detail, TorrentSnapshot};
use crate::tui::theme::Theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

/// A piece missing from fewer than this many connected peers is rare.
const RARE: u32 = 4;
/// Lines under the map: the legend and the swarm's numbers.
const FOOTER: u16 = 3;

/// What one cell of the map stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cell {
    /// Every piece had.
    Have,
    /// Some pieces had.
    Partial,
    /// A piece being downloaded.
    InFlight,
    /// Missing, and fewer than `RARE` connected peers have one of them.
    Rare,
    Missing,
}

impl Cell {
    fn glyph(self, theme: Theme) -> (char, Style) {
        match self {
            Cell::Have => ('█', Style::new().fg(theme.up())),
            Cell::Partial => ('▒', Style::new().fg(theme.partial())),
            Cell::InFlight => ('▓', Style::new().fg(theme.warn())),
            Cell::Rare => ('░', Style::new().fg(theme.error())),
            Cell::Missing => ('░', theme.dim_style()),
        }
    }
}

/// Pieces each cell stands for, with `cells` cells for `pieces` pieces.
pub fn pieces_per_cell(pieces: usize, cells: usize) -> usize {
    pieces.div_ceil(cells.max(1)).max(1)
}

/// The map of `pieces` in at most `cells` cells.
pub fn cells(pieces: &PieceSnapshot, cells: usize) -> Vec<Cell> {
    let group = pieces_per_cell(pieces.have.len(), cells);
    (0..pieces.have.len())
        .step_by(group)
        .map(|start| {
            let range = start..(start + group).min(pieces.have.len());
            let have = &pieces.have[range.clone()];
            if have.iter().all(|&have| have) {
                Cell::Have
            } else if pieces.in_flight[range.clone()].iter().any(|&f| f) {
                Cell::InFlight
            } else if have.iter().any(|&have| have) {
                Cell::Partial
            } else if range
                .clone()
                .any(|i| !pieces.have[i] && pieces.availability[i] < RARE)
            {
                Cell::Rare
            } else {
                Cell::Missing
            }
        })
        .collect()
}

/// The area the map takes in a tab of size `area`, one cell per column.
pub fn map_area(area: Rect) -> Rect {
    Rect {
        x: area.x + 1,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(FOOTER),
        ..area
    }
}

/// The detail box's hint for this tab.
pub fn hint(detail: Option<&Detail>, area: Rect) -> Option<String> {
    let pieces = detail?.pieces.as_ref()?.have.len();
    let map = map_area(area);
    let cells = usize::from(map.width) * usize::from(map.height);
    Some(format!(
        "1 cell ≈ {} pieces",
        pieces_per_cell(pieces, cells)
    ))
}

/// Copies of the whole torrent among connected peers: the rarest piece's
/// count, plus the share of pieces that more peers have.
pub fn distributed_copies(availability: &[u32]) -> f64 {
    let Some(&least) = availability.iter().min() else {
        return 0.0;
    };
    let above = availability.iter().filter(|&&count| count > least).count();
    f64::from(least) + above as f64 / availability.len() as f64
}

pub fn draw(
    frame: &mut Frame,
    torrent: &TorrentSnapshot,
    detail: Option<&Detail>,
    app: &App,
    area: Rect,
) {
    let theme = app.theme;
    let Some((detail, pieces)) = detail.and_then(|d| Some((d, d.pieces.as_ref()?))) else {
        let note = Span::styled(" the pieces are not known yet", theme.dim_style());
        frame.render_widget(Paragraph::new(note), area);
        return;
    };
    let map = map_area(area);
    let map_cells = cells(pieces, usize::from(map.width) * usize::from(map.height));
    let lines: Vec<Line> = map_cells
        .chunks(usize::from(map.width).max(1))
        .take(usize::from(map.height))
        .map(|row| {
            let mut spans: Vec<Span> = Vec::new();
            let mut run = String::new();
            let mut style = None;
            for &cell in row {
                let (glyph, cell_style) = cell.glyph(theme);
                if style.is_some_and(|style| style != cell_style) {
                    spans.push(Span::styled(
                        std::mem::take(&mut run),
                        style.unwrap_or_default(),
                    ));
                }
                style = Some(cell_style);
                run.push(glyph);
            }
            if let Some(style) = style {
                spans.push(Span::styled(run, style));
            }
            Line::from(spans)
        })
        .collect();
    frame.render_widget(Paragraph::new(lines), map);

    let mut legend = vec![Span::raw(" ")];
    for (cell, text) in [
        (Cell::Have, "have"),
        (Cell::Partial, "partial"),
        (Cell::InFlight, "downloading"),
        (Cell::Missing, "missing"),
        (Cell::Rare, "rare (<4 peers)"),
    ] {
        let (glyph, style) = cell.glyph(theme);
        legend.push(Span::styled(glyph.to_string(), style));
        legend.push(Span::styled(format!(" {text}   "), theme.dim_style()));
    }

    let total = pieces.have.len();
    let seeds = detail
        .peers
        .iter()
        .filter(|peer| total > 0 && peer.pieces as usize >= total)
        .count();
    let had = pieces.have.iter().filter(|&&have| have).count();
    let rarest = pieces.availability.iter().min().copied().unwrap_or(0);
    let value = |text: String| Span::styled(text, Style::new().fg(theme.bright()));
    let label = |text: &str| Span::styled(text.to_string(), theme.dim_style());
    let swarm = Line::from(vec![
        label(" seeds "),
        value(seeds.to_string()),
        label("  peers "),
        value((detail.peers.len() - seeds).to_string()),
        label("  pool "),
        value(torrent.pool.to_string()),
        label("  copies "),
        value(format!("{:.1}", distributed_copies(&pieces.availability))),
        label("  rarest piece "),
        Span::styled(
            format!("{rarest} peers"),
            Style::new().fg(if rarest < RARE {
                theme.error()
            } else {
                theme.warn()
            }),
        ),
        label("  pieces "),
        value(format!("{had}/{total}")),
    ]);
    let sources = detail.sources;
    let mut from = vec![label(" sources ")];
    for (name, count, color) in [
        ("tracker", sources.tracker, theme.down()),
        ("dht", sources.dht, theme.metadata()),
        ("pex", sources.pex, theme.warn()),
        ("incoming", sources.incoming, theme.up()),
    ] {
        from.push(Span::styled("■ ", Style::new().fg(color)));
        from.push(label(&format!("{name} ")));
        from.push(value(count.to_string()));
        from.push(Span::raw("  "));
    }
    let footer = Rect {
        y: map.bottom(),
        height: area.bottom().saturating_sub(map.bottom()),
        ..area
    };
    frame.render_widget(
        Paragraph::new(vec![Line::from(legend), swarm, Line::from(from)]),
        footer,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot(have: &[bool], in_flight: &[usize], availability: &[u32]) -> PieceSnapshot {
        let mut flight = vec![false; have.len()];
        for &i in in_flight {
            flight[i] = true;
        }
        PieceSnapshot {
            have: have.to_vec(),
            in_flight: flight,
            availability: availability.to_vec(),
        }
    }

    #[test]
    fn cells_follow_the_pieces_they_stand_for() {
        let (t, f) = (true, false);
        // Pairs of pieces: all had; half had; one in flight; missing and
        // rare; missing; and a last cell of one piece.
        let pieces = snapshot(
            &[t, t, t, f, f, f, f, f, f, f, t],
            &[5],
            &[0, 0, 9, 9, 9, 9, 3, 9, 9, 9, 9],
        );
        assert_eq!(pieces_per_cell(11, 6), 2);
        assert_eq!(
            cells(&pieces, 6),
            [
                Cell::Have,
                Cell::Partial,
                Cell::InFlight,
                Cell::Rare,
                Cell::Missing,
                Cell::Have
            ]
        );
        // A piece in flight shows over pieces had in the same cell.
        let pieces = snapshot(&[t, f], &[1], &[0, 0]);
        assert_eq!(cells(&pieces, 1), [Cell::InFlight]);
        // One piece a cell when there is room.
        assert_eq!(cells(&pieces, 100).len(), 2);
    }

    #[test]
    fn distributed_copies_count_the_rarest_and_the_rest() {
        assert_eq!(distributed_copies(&[]), 0.0);
        assert_eq!(distributed_copies(&[2, 2, 2, 2]), 2.0);
        assert_eq!(distributed_copies(&[1, 2, 3, 1]), 1.5);
    }
}
