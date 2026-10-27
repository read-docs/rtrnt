//! The detail panel: the selected torrent's peers, pieces, trackers, files
//! or info, one tab at a time.

use super::{boxed, inner_of, truncate};
use crate::tui::app::{App, Focus, Tab};
use crate::tui::snapshot::{Detail, Snapshot, TorrentSnapshot};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use unicode_width::UnicodeWidthStr;

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let mut tabs = vec![Span::raw(" ")];
    for (i, &tab) in Tab::ALL.iter().enumerate() {
        if i > 0 {
            tabs.push(Span::styled(TAB_GAP, theme.border_style()));
        }
        let text = tab_title(i, tab);
        tabs.push(if tab == app.tab {
            Span::styled(
                text,
                Style::new()
                    .fg(theme.down())
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED),
            )
        } else {
            Span::styled(text, theme.dim_style())
        });
    }
    tabs.push(Span::raw(" "));
    let torrent = app.selected.and_then(|hash| snapshot.torrent(hash));
    let tabs_width: usize = tabs.iter().map(Span::width).sum();
    let detail = torrent.and_then(|torrent| {
        snapshot
            .detail
            .as_ref()
            .filter(|detail| detail.info_hash == torrent.info_hash)
    });
    let room = (area.width as usize).saturating_sub(tabs_width + 8);
    let hint = match app.tab {
        Tab::Pieces => super::pieces::hint(detail, inner_of(area)),
        _ => torrent.map(|torrent| torrent.name.clone()),
    }
    .map(|hint| truncate(&hint, room));
    let block = boxed(
        theme,
        Line::from(tabs),
        hint.filter(|hint| !hint.is_empty()),
        app.focus == Focus::Detail,
    );
    let inner = block.inner(area);
    frame.render_widget(block, area);
    let Some(torrent) = torrent else {
        let note = Span::styled(" no torrent selected", theme.dim_style());
        frame.render_widget(Paragraph::new(note), inner);
        return;
    };
    draw_tab(frame, snapshot, torrent, detail, app, inner);
}

/// Between two tab titles.
const TAB_GAP: &str = " ─ ";

/// The title of `tab`, the `index`th: `1 Peers`.
fn tab_title(index: usize, tab: Tab) -> String {
    format!("{} {}", index + 1, tab.title())
}

/// Where the tab titles are on the top border of the panel's box `area`.
pub fn tabs(area: Rect) -> Vec<(Tab, Rect)> {
    // After the corner and the space before the first title.
    let mut x = area.x + 2;
    let mut tabs = Vec::new();
    for (i, &tab) in Tab::ALL.iter().enumerate() {
        if i > 0 {
            x += TAB_GAP.width() as u16;
        }
        let width = tab_title(i, tab).width() as u16;
        tabs.push((tab, Rect::new(x, area.y, width, 1).intersection(area)));
        x += width;
    }
    tabs
}

fn draw_tab(
    frame: &mut Frame,
    snapshot: &Snapshot,
    torrent: &TorrentSnapshot,
    detail: Option<&Detail>,
    app: &App,
    area: Rect,
) {
    match app.tab {
        Tab::Peers => super::peers::draw(frame, torrent, detail, app, area),
        Tab::Pieces => super::pieces::draw(frame, torrent, detail, app, area),
        Tab::Trackers => super::trackers::draw(frame, snapshot, torrent, detail, app, area),
        Tab::Files => super::files::draw(frame, torrent, detail, app, area),
        Tab::Info => super::info::draw(frame, torrent, detail, app, area),
    }
}
