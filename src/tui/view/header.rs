//! The top line: session rates and counts, and the time.

use super::{bold, human_rate};
use crate::tui::app::App;
use crate::tui::snapshot::Snapshot;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

pub fn draw(frame: &mut Frame, snapshot: &Snapshot, app: &App, area: Rect) {
    let theme = app.theme;
    let dim = theme.dim_style();
    let (down, up) = snapshot.rates();
    let session = &snapshot.session;
    let count = snapshot.torrents.len();
    let mut facts = vec![
        format!("{count} torrent{}", if count == 1 { "" } else { "s" }),
        format!("{}/{} peers", session.connections, session.max_connections),
    ];
    facts.push(match session.dht_nodes {
        Some(nodes) => format!("DHT {nodes} nodes"),
        None => "DHT off".to_string(),
    });
    if let Some(port) = session.port {
        facts.push(format!("port {port}"));
    }
    let mut spans = vec![
        Span::styled(" rtrnt ", bold(theme.down())),
        Span::styled("│ ", Style::new().fg(theme.border())),
        Span::styled(
            format!("↓ {}", human_rate(down)),
            Style::new().fg(theme.down()),
        ),
        Span::raw("  "),
        Span::styled(format!("↑ {}", human_rate(up)), Style::new().fg(theme.up())),
        Span::styled(" │ ", Style::new().fg(theme.border())),
    ];
    // Facts that do not fit are left out, from the last.
    let clock = format!(" {} ", snapshot.clock);
    let mut used: usize = spans.iter().map(Span::width).sum::<usize>() + clock.len();
    for fact in facts {
        let width = fact.chars().count() + 2;
        if used + width > area.width as usize {
            break;
        }
        used += width;
        spans.push(Span::styled(format!("{fact}  "), dim));
    }
    frame.render_widget(Paragraph::new(Line::from(spans)), area);
    let clock_area = Rect {
        x: area.right().saturating_sub(clock.len() as u16),
        width: (clock.len() as u16).min(area.width),
        ..area
    };
    frame.render_widget(Paragraph::new(Span::styled(clock, dim)), clock_area);
}
