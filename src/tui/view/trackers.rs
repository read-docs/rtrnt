//! The Trackers tab: each tracker by tier, with what it last said, then
//! the DHT and peer exchange.

use super::{fit, short_duration};
use crate::stats::TrackerResult;
use crate::tui::app::App;
use crate::tui::snapshot::{Detail, Snapshot, TorrentSnapshot};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

const TIER: usize = 4;
const NUMBER: usize = 6;
const STATUS: usize = 22;
const MIN_URL: usize = 16;

pub fn draw(
    frame: &mut Frame,
    snapshot: &Snapshot,
    torrent: &TorrentSnapshot,
    detail: Option<&Detail>,
    app: &App,
    area: Rect,
) {
    let theme = app.theme;
    let Some(detail) = detail else {
        return;
    };
    let width = area.width as usize;
    let fixed = TIER + STATUS + 4 * NUMBER + 7;
    let url = width.saturating_sub(fixed).max(MIN_URL);
    let number = |n: Option<String>| fit(n.as_deref().unwrap_or("—"), NUMBER, true);
    let heading = format!(
        " {} {} {} {} {} {} {}",
        fit("Tier", TIER, false),
        fit("URL", url, false),
        fit("Status", STATUS, false),
        number(Some("Peers".into())),
        number(Some("Seeds".into())),
        number(Some("Leech".into())),
        number(Some("Next".into())),
    );
    let mut rows = Vec::new();
    let mut last_tier = None;
    for tracker in &detail.trackers {
        let tier = if last_tier == Some(tracker.tier) {
            String::new()
        } else {
            (tracker.tier + 1).to_string()
        };
        last_tier = Some(tracker.tier);
        let (status, style) = match &tracker.result {
            TrackerResult::Ok => ("ok".to_string(), Style::new().fg(theme.up())),
            TrackerResult::Error(e) => (e.clone(), Style::new().fg(theme.error())),
            TrackerResult::NotContacted => ("not contacted".to_string(), theme.dim_style()),
        };
        let next = match tracker.result {
            TrackerResult::Ok => detail.next_announce.map(short_duration),
            _ => None,
        };
        rows.push(Line::from(vec![
            Span::styled(format!(" {}", fit(&tier, TIER, false)), theme.dim_style()),
            Span::raw(format!(" {}", fit(&tracker.url, url, false))),
            Span::styled(format!(" {}", fit(&status, STATUS, false)), style),
            Span::raw(format!(
                " {} {} {}",
                number(tracker.peers.map(|n| n.to_string())),
                number(tracker.seeders.map(|n| n.to_string())),
                number(tracker.leechers.map(|n| n.to_string())),
            )),
            Span::styled(format!(" {}", number(next)), theme.dim_style()),
        ]));
    }
    if detail.trackers.is_empty() {
        rows.push(Line::from(Span::styled(" no trackers", theme.dim_style())));
    }
    let private = torrent.layout.as_ref().is_some_and(|layout| layout.private);
    let off = |why: &str| (why.to_string(), theme.dim_style());
    let on = |text: String| (text, Style::new().fg(theme.up()));
    let dht = match snapshot.session.dht_nodes {
        _ if private => off("off (private torrent)"),
        Some(nodes) => on(format!("ok, {nodes} nodes")),
        None => off("off"),
    };
    let pex = if private {
        off("off (private torrent)")
    } else {
        on("ok".to_string())
    };
    rows.push(Line::default());
    for (name, (status, style), peers) in [
        ("DHT", dht, detail.sources.dht),
        ("PeX", pex, detail.sources.pex),
        (
            "incoming",
            ("—".to_string(), theme.dim_style()),
            detail.sources.incoming,
        ),
    ] {
        rows.push(Line::from(vec![
            Span::raw(format!(
                " {} {}",
                fit("", TIER, false),
                fit(name, url, false)
            )),
            Span::styled(format!(" {}", fit(&status, STATUS, false)), style),
            Span::raw(format!(" {}", number(Some(peers.to_string())))),
        ]));
    }

    let height = (area.height as usize).saturating_sub(1);
    let offset = app.detail_scroll.min(rows.len().saturating_sub(height));
    let mut lines = vec![Line::from(Span::styled(heading, theme.heading_style()))];
    lines.extend(rows.into_iter().skip(offset).take(height));
    frame.render_widget(Paragraph::new(lines), area);
}
