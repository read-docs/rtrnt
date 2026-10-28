//! The torrent table.

use super::{
    Rows, bar, bold, boxed, fit, human_bytes, human_rate, inner_of, scroll_to, selected_style,
    short_duration, title,
};
use crate::stats::TorrentState;
use crate::tui::app::{App, Focus, Sort};
use crate::tui::snapshot::{Snapshot, TorrentSnapshot};
use crate::tui::theme::Theme;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

/// Width of the progress bar, before its percentage.
const BAR: usize = 10;
/// Narrowest the name column gets before more columns are dropped.
const MIN_NAME: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    Name,
    Size,
    Progress,
    Status,
    Down,
    Up,
    Peers,
    Eta,
}

impl Column {
    fn heading(self) -> &'static str {
        match self {
            Column::Name => "Name",
            Column::Size => "Size",
            Column::Progress => "Progress",
            Column::Status => "Status",
            Column::Down => "↓",
            Column::Up => "↑",
            Column::Peers => "Peers",
            Column::Eta => "ETA",
        }
    }

    /// Fixed width; the name takes what is left.
    fn width(self) -> usize {
        match self {
            Column::Name => 0,
            Column::Size => 8,
            Column::Progress => BAR + 5,
            Column::Status => 11,
            Column::Down | Column::Up => 10,
            Column::Peers => 5,
            Column::Eta => 4,
        }
    }

    fn right_aligned(self) -> bool {
        !matches!(self, Column::Name | Column::Progress | Column::Status)
    }

    /// What a click on the heading sorts by.
    fn sort(self) -> Option<Sort> {
        match self {
            Column::Name => Some(Sort::Name),
            Column::Size => Some(Sort::Size),
            Column::Progress => Some(Sort::Progress),
            Column::Status => None,
            Column::Down => Some(Sort::Down),
            Column::Up => Some(Sort::Up),
            Column::Peers => Some(Sort::Peers),
            Column::Eta => Some(Sort::Eta),
        }
    }
}

/// The columns that fit in `width`, and the name column's width. Below
/// 100 columns of screen ETA goes, then ↑ and Size while the name would be
/// too narrow.
fn columns(width: usize, screen_width: u16) -> (Vec<Column>, usize) {
    use Column::*;
    let mut columns = vec![Name, Size, Progress, Status, Down, Up, Peers, Eta];
    if screen_width < 100 {
        columns.retain(|&c| c != Eta);
    }
    let name_width = |columns: &[Column]| {
        let fixed: usize = columns.iter().map(|c| c.width() + 1).sum();
        width.saturating_sub(fixed)
    };
    for drop in [Up, Size] {
        if name_width(&columns) >= MIN_NAME {
            break;
        }
        columns.retain(|&c| c != drop);
    }
    let name = name_width(&columns);
    (columns, name)
}

/// The columns shown inside the table's box, `inner`, each with its width.
/// One column goes first, for the selection marker.
fn shown_columns(inner: Rect, screen_width: u16) -> Vec<(Column, usize)> {
    let (columns, name_width) = columns((inner.width as usize).saturating_sub(1), screen_width);
    columns
        .into_iter()
        .map(|column| match column {
            Column::Name => (column, name_width),
            column => (column, column.width()),
        })
        .collect()
}

/// The first of the `visible` torrents shown in `height` rows: scrolled to
/// the selection.
fn first_row(app: &App, visible: &[&TorrentSnapshot], height: usize) -> usize {
    let selected = visible
        .iter()
        .position(|torrent| Some(torrent.info_hash) == app.selected)
        .unwrap_or(0);
    scroll_to(selected, 0, height)
}

/// Where the headings that sort are, in the table's box `area`.
pub fn headings(area: Rect, screen_width: u16) -> Vec<(Sort, Rect)> {
    let inner = inner_of(area);
    if inner.height == 0 {
        return Vec::new();
    }
    let mut x = inner.x + 1;
    let mut headings = Vec::new();
    for (column, width) in shown_columns(inner, screen_width) {
        let width = width as u16;
        if let Some(sort) = column.sort() {
            headings.push((sort, Rect::new(x, inner.y, width, 1).intersection(inner)));
        }
        x += width + 1;
    }
    headings
}

/// The torrent rows shown in the table's box `area`.
pub fn rows(snapshot: &Snapshot, app: &App, area: Rect) -> Rows {
    let inner = inner_of(area);
    let visible = app.visible(snapshot);
    let height = (inner.height as usize).saturating_sub(1);
    let first = first_row(app, &visible, height);
    Rows::under_heading(
        inner,
        first,
        visible.len().saturating_sub(first).min(height),
    )
}

/// What the status column says, and its color.
pub fn status(theme: Theme, torrent: &TorrentSnapshot) -> (String, Color) {
    match &torrent.state {
        TorrentState::FetchingMetadata { .. } => ("metadata".into(), theme.metadata()),
        TorrentState::QueuedForChecking => ("queued".into(), theme.dim()),
        TorrentState::Checking { .. } => ("checking".into(), theme.warn()),
        TorrentState::Downloading => ("downloading".into(), theme.down()),
        TorrentState::Seeding => ("seeding".into(), theme.up()),
        TorrentState::Pausing => ("pausing".into(), theme.warn()),
        TorrentState::Paused => ("paused".into(), theme.dim()),
        TorrentState::Removing => ("removing".into(), theme.warn()),
        TorrentState::Done => ("done".into(), theme.up()),
        TorrentState::Interrupted => ("stopped".into(), theme.dim()),
        TorrentState::Failed(_) => ("failed".into(), theme.error()),
    }
}

/// Whether the torrent moves data, so its rates mean something.
fn active(torrent: &TorrentSnapshot) -> bool {
    matches!(
        torrent.state,
        TorrentState::Downloading | TorrentState::Seeding
    )
}

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let mut hint = format!(
        "sort: {} {}",
        app.sort.name(),
        if app.reverse { "▴" } else { "▾" }
    );
    if !app.filter.is_empty() {
        hint = format!("/{}  {hint}", app.filter);
    }
    let block = boxed(
        theme,
        title(theme, "Torrents"),
        Some(hint),
        app.focus == Focus::Torrents,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 {
        return;
    }
    let columns = shown_columns(inner, frame.area().width);
    let mut heading = vec![Span::raw(" ")];
    for &(column, width) in &columns {
        heading.push(Span::styled(
            fit(column.heading(), width, column.right_aligned()),
            theme.heading_style(),
        ));
        heading.push(Span::raw(" "));
    }
    let mut lines = vec![Line::from(heading)];

    let visible = app.visible(snapshot);
    let height = inner.height as usize - 1;
    let offset = first_row(app, &visible, height);
    for torrent in visible.iter().skip(offset).take(height) {
        let is_selected = Some(torrent.info_hash) == app.selected;
        let mut spans = vec![if is_selected {
            Span::styled("▌", Style::new().fg(theme.down()))
        } else {
            Span::raw(" ")
        }];
        for &(column, width) in &columns {
            spans.extend(cell(theme, torrent, column, width, is_selected));
            spans.push(Span::raw(" "));
        }
        let line = Line::from(spans);
        lines.push(if is_selected {
            line.style(selected_style(theme))
        } else {
            line
        });
    }
    if visible.is_empty() {
        let note = if snapshot.torrents.is_empty() {
            " no torrents yet: press a to add a torrent"
        } else {
            " no torrent matches the filter"
        };
        lines.push(Line::from(Span::styled(note, theme.dim_style())));
    }
    frame.render_widget(Paragraph::new(lines), inner);
}

fn cell(
    theme: Theme,
    torrent: &TorrentSnapshot,
    column: Column,
    width: usize,
    selected: bool,
) -> Vec<Span<'static>> {
    let right = column.right_aligned();
    let dash = || vec![Span::styled(fit("—", width, right), theme.dim_style())];
    match column {
        Column::Name => {
            let style = if selected {
                bold(theme.bright())
            } else {
                Style::new()
            };
            vec![Span::styled(fit(&torrent.name, width, false), style)]
        }
        Column::Size => match &torrent.layout {
            Some(layout) => vec![Span::raw(fit(&human_bytes(layout.size), width, true))],
            None => dash(),
        },
        Column::Progress => {
            let progress = torrent.progress();
            let (_, color) = status(theme, torrent);
            let color = match torrent.state {
                TorrentState::Done | TorrentState::Seeding => theme.up(),
                _ => color,
            };
            let mut spans = bar(theme, BAR, progress, color);
            spans.push(Span::styled(
                format!(" {:>3.0}%", (progress * 100.0).floor()),
                theme.dim_style(),
            ));
            spans
        }
        Column::Status => {
            let (text, color) = status(theme, torrent);
            vec![Span::styled(
                fit(&text, width, false),
                Style::new().fg(color),
            )]
        }
        Column::Down | Column::Up => {
            let (rate, color, idle) = if column == Column::Down {
                let seeding = matches!(torrent.state, TorrentState::Seeding);
                (torrent.rate_down, theme.down(), seeding)
            } else {
                (torrent.rate_up, theme.up(), false)
            };
            if !active(torrent) || idle {
                return dash();
            }
            let style = if rate > 0.0 {
                Style::new().fg(color)
            } else {
                theme.dim_style()
            };
            vec![Span::styled(fit(&human_rate(rate), width, true), style)]
        }
        Column::Peers => {
            let peers = match torrent.state {
                TorrentState::FetchingMetadata { peers, .. } => peers,
                _ => torrent.peers,
            };
            vec![Span::raw(fit(&peers.to_string(), width, true))]
        }
        Column::Eta => {
            let text = match (&torrent.state, torrent.eta) {
                (TorrentState::Seeding, _) => "∞".to_string(),
                (TorrentState::Downloading, Some(eta)) => short_duration(eta),
                (TorrentState::Downloading, None) => "∞".to_string(),
                _ => "—".to_string(),
            };
            vec![Span::styled(fit(&text, width, true), theme.dim_style())]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_drop_on_narrow_screens() {
        use Column::*;
        let (all, name) = columns(97, 100);
        assert_eq!(all, [Name, Size, Progress, Status, Down, Up, Peers, Eta]);
        assert_eq!(name, 26);
        let (narrow, name) = columns(77, 80);
        assert_eq!(narrow, [Name, Size, Progress, Status, Down, Peers]);
        assert_eq!(name, 22);
        let (narrowest, name) = columns(57, 60);
        assert_eq!(narrowest, [Name, Progress, Status, Down, Peers]);
        assert_eq!(name, 11);
    }
}
