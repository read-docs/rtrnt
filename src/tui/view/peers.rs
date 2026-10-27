//! The Peers tab: one row per connected peer, a flag legend, and the
//! torrent's rates over the last half minute.

use super::{bar, fit, human_rate, sparkline};
use crate::stats::PeerFlags;
use crate::tui::app::App;
use crate::tui::snapshot::{Detail, PeerSnapshot, TorrentSnapshot};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;

/// Width of the Has bar, before its `seed` or percentage.
const BAR: usize = 10;
const ADDRESS: usize = 24;
const FLAGS: usize = 9;
const RATE: usize = 10;
const HAS: usize = BAR + 5;
/// Narrowest client column worth showing.
const MIN_CLIENT: usize = 8;

/// The flags column: `D`/`d` we download (unchoked) or are interested but
/// choked, `U`/`u` we upload or the peer is interested but choked, `I`
/// incoming, `F` fast extension, `X` peer exchange.
pub fn flag_letters(flags: PeerFlags, inbound: bool) -> String {
    let down = match (flags.am_interested(), flags.peer_choking()) {
        (true, false) => 'D',
        (true, true) => 'd',
        _ => ' ',
    };
    let up = if !flags.am_choking() {
        'U'
    } else if flags.peer_interested() {
        'u'
    } else {
        ' '
    };
    let flag = |set: bool, letter| if set { letter } else { ' ' };
    let letters = [
        down,
        up,
        flag(inbound, 'I'),
        flag(flags.fast(), 'F'),
        flag(flags.pex(), 'X'),
    ];
    let spaced: Vec<String> = letters.iter().map(char::to_string).collect();
    spaced.join(" ")
}

pub fn draw(
    frame: &mut Frame,
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
    // Below: the legend, and the two rate lines if there is room.
    let footer = if area.height >= 8 { 4 } else { 1 };
    let table_height = (area.height as usize).saturating_sub(footer);
    let mut client = width.saturating_sub(ADDRESS + FLAGS + 2 * RATE + HAS + 7);
    if client < MIN_CLIENT {
        client = 0;
    }
    let address = if client == 0 {
        width
            .saturating_sub(FLAGS + 2 * RATE + HAS + 5)
            .min(ADDRESS)
    } else {
        ADDRESS
    };

    let mut heading = vec![
        Span::raw(" "),
        Span::styled(fit("Address", address, false), theme.heading_style()),
    ];
    if client > 0 {
        heading.push(Span::styled(
            format!(" {}", fit("Client", client, false)),
            theme.heading_style(),
        ));
    }
    heading.push(Span::styled(
        format!(
            " {} {} {}  {}",
            fit("Flags", FLAGS, false),
            fit("↓", RATE, true),
            fit("↑", RATE, true),
            fit("Has", HAS - 1, false),
        ),
        theme.heading_style(),
    ));
    let mut lines = vec![Line::from(heading)];

    let mut peers: Vec<&PeerSnapshot> = detail.peers.iter().collect();
    peers.sort_by(|a, b| {
        b.rate_down
            .total_cmp(&a.rate_down)
            .then(b.rate_up.total_cmp(&a.rate_up))
            .then(a.addr.cmp(&b.addr))
    });
    let rows = table_height.saturating_sub(1);
    let offset = app.detail_scroll.min(peers.len().saturating_sub(rows));
    let pieces = torrent.layout.as_ref().map_or(0, |layout| layout.pieces);
    for peer in peers.iter().skip(offset).take(rows) {
        let mut spans = vec![
            Span::raw(" "),
            Span::raw(fit(&peer.addr.to_string(), address, false)),
        ];
        if client > 0 {
            spans.push(Span::styled(
                format!(
                    " {}",
                    fit(peer.client.as_deref().unwrap_or("?"), client, false)
                ),
                theme.dim_style(),
            ));
        }
        spans.push(Span::styled(
            format!(
                " {}",
                fit(&flag_letters(peer.flags, peer.inbound), FLAGS, false)
            ),
            Style::new().fg(theme.warn()),
        ));
        for (rate, color) in [(peer.rate_down, theme.down()), (peer.rate_up, theme.up())] {
            let style = if rate >= 1.0 {
                Style::new().fg(color)
            } else {
                theme.dim_style()
            };
            spans.push(Span::styled(
                format!(" {}", fit(&human_rate(rate), RATE, true)),
                style,
            ));
        }
        spans.push(Span::raw("  "));
        let seed = pieces > 0 && peer.pieces as usize >= pieces;
        let fraction = if pieces == 0 {
            0.0
        } else {
            f64::from(peer.pieces) / pieces as f64
        };
        let color = if seed { theme.up() } else { theme.down() };
        spans.extend(bar(theme, BAR, fraction, color));
        let has = if seed {
            "seed".to_string()
        } else {
            format!("{:.0}%", (fraction * 100.0).floor())
        };
        spans.push(Span::styled(format!(" {has:>4}"), theme.dim_style()));
        lines.push(Line::from(spans));
    }
    if detail.peers.is_empty() {
        lines.push(Line::from(Span::styled(
            " no peers connected",
            theme.dim_style(),
        )));
    }
    let table = Rect {
        height: table_height as u16,
        ..area
    };
    frame.render_widget(Paragraph::new(lines), table);

    let count = detail.peers.len();
    let shown = if count > rows {
        format!(
            "{}-{} of {count} peers",
            offset + 1,
            (offset + rows).min(count)
        )
    } else {
        format!("{count} peer{}", if count == 1 { "" } else { "s" })
    };
    let legend =
        format!(" {shown}  ·  D/d down unchoked/interested  U/u up  I incoming  F fast  X pex");
    let mut footer_lines = vec![Line::from(Span::styled(legend, theme.dim_style()))];
    if footer > 1 {
        footer_lines.push(Line::default());
        for (arrow, history, rate, color) in [
            ("↓", &detail.down_history, torrent.rate_down, theme.down()),
            ("↑", &detail.up_history, torrent.rate_up, theme.up()),
        ] {
            let peak = history.iter().copied().fold(rate, f64::max);
            let text = format!("  {:>10}  peak {}", human_rate(rate), human_rate(peak));
            let spark = width.saturating_sub(text.chars().count() + 4);
            footer_lines.push(Line::from(vec![
                Span::styled(format!(" {arrow} "), Style::new().fg(color)),
                Span::styled(sparkline(history, spark), Style::new().fg(color)),
                Span::styled(text, theme.dim_style()),
            ]));
        }
    }
    let footer_area = Rect {
        y: area.y + table_height as u16,
        height: area.height - table_height as u16,
        ..area
    };
    frame.render_widget(Paragraph::new(footer_lines), footer_area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_spell_out_each_side() {
        let letters = |am_choking, peer_choking, am_interested, peer_interested, inbound| {
            let flags = PeerFlags::new(
                am_choking,
                peer_choking,
                am_interested,
                peer_interested,
                true,
                false,
            );
            flag_letters(flags, inbound)
        };
        assert_eq!(letters(false, false, true, false, false), "D U   F  ");
        assert_eq!(letters(true, true, true, true, true), "d u I F  ");
        assert_eq!(letters(true, false, false, false, false), "      F  ");
        let pex = PeerFlags::new(true, true, false, false, false, true);
        assert_eq!(flag_letters(pex, false), "        X");
    }
}
