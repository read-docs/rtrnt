//! The UI's own state: what is selected and shown, and the rate history
//! that turns byte counters into rates.

use super::snapshot::{Snapshot, TorrentSnapshot};
use super::theme::Theme;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Samples of rate history kept: 30 s at 4 a second.
pub const HISTORY: usize = 120;
/// Samples a rate is averaged over.
const SMOOTHING: usize = 4;
/// How far back the average behind the ETA goes.
const ETA_WINDOW: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Focus {
    Torrents,
    Detail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    Peers,
    Pieces,
    Trackers,
    Files,
    Info,
}

impl Tab {
    pub const ALL: [Tab; 5] = [
        Tab::Peers,
        Tab::Pieces,
        Tab::Trackers,
        Tab::Files,
        Tab::Info,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Tab::Peers => "Peers",
            Tab::Pieces => "Pieces",
            Tab::Trackers => "Trackers",
            Tab::Files => "Files",
            Tab::Info => "Info",
        }
    }
}

/// What the torrent table is sorted by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Added,
    Name,
    Size,
    Progress,
    Down,
    Up,
    Peers,
    Eta,
}

impl Sort {
    const ALL: [Sort; 8] = [
        Sort::Added,
        Sort::Name,
        Sort::Size,
        Sort::Progress,
        Sort::Down,
        Sort::Up,
        Sort::Peers,
        Sort::Eta,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Sort::Added => "added",
            Sort::Name => "name",
            Sort::Size => "size",
            Sort::Progress => "progress",
            Sort::Down => "↓",
            Sort::Up => "↑",
            Sort::Peers => "peers",
            Sort::Eta => "eta",
        }
    }

    fn next(self) -> Self {
        let index = Self::ALL.iter().position(|&sort| sort == self).unwrap_or(0);
        Self::ALL[(index + 1) % Self::ALL.len()]
    }

    fn compare(self, a: &TorrentSnapshot, b: &TorrentSnapshot) -> Ordering {
        let size = |t: &TorrentSnapshot| t.layout.as_ref().map(|layout| layout.size);
        match self {
            Sort::Added => a.order.cmp(&b.order),
            Sort::Name => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
            Sort::Size => size(a).cmp(&size(b)),
            Sort::Progress => a.progress().total_cmp(&b.progress()),
            Sort::Down => a.rate_down.total_cmp(&b.rate_down),
            Sort::Up => a.rate_up.total_cmp(&b.rate_up),
            Sort::Peers => a.peers.cmp(&b.peers),
            // No ETA sorts last.
            Sort::Eta => match (a.eta, b.eta) {
                (Some(a), Some(b)) => a.cmp(&b),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
        }
    }
}

/// What a key press asks of the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    None,
    /// Stop the torrents gracefully.
    Stop,
    /// Leave the UI: everything has finished.
    Quit,
    /// Leave at once, without waiting for the torrents to stop.
    ForceQuit,
}

pub struct App {
    pub theme: Theme,
    /// Info hash of the selected torrent.
    pub selected: Option<[u8; 20]>,
    pub focus: Focus,
    pub tab: Tab,
    /// First row shown of the detail panel's table.
    pub detail_scroll: usize,
    pub show_log: bool,
    pub sort: Sort,
    pub reverse: bool,
    /// Torrents whose name contains this (ignoring case) are shown.
    pub filter: String,
    /// Set while the filter is being typed.
    pub editing_filter: bool,
    pub show_help: bool,
    /// Set once a stop was asked for.
    pub stopping: bool,
    pub rates: Rates,
}

impl App {
    pub fn new(theme: Theme) -> Self {
        Self {
            theme,
            selected: None,
            focus: Focus::Torrents,
            tab: Tab::Peers,
            detail_scroll: 0,
            show_log: true,
            sort: Sort::Added,
            reverse: false,
            filter: String::new(),
            editing_filter: false,
            show_help: false,
            stopping: false,
            rates: Rates::default(),
        }
    }

    /// The torrents shown, filtered and sorted.
    pub fn visible<'a>(&self, snapshot: &'a Snapshot) -> Vec<&'a TorrentSnapshot> {
        let filter = self.filter.to_lowercase();
        let mut torrents: Vec<&TorrentSnapshot> = snapshot
            .torrents
            .iter()
            .filter(|torrent| torrent.name.to_lowercase().contains(&filter))
            .collect();
        torrents.sort_by(|a, b| {
            let order = self.sort.compare(a, b).then(a.order.cmp(&b.order));
            if self.reverse { order.reverse() } else { order }
        });
        torrents
    }

    /// Selects the first torrent shown if the selected one is not shown.
    pub fn ensure_selection(&mut self, snapshot: &Snapshot) {
        let visible = self.visible(snapshot);
        if !visible
            .iter()
            .any(|torrent| Some(torrent.info_hash) == self.selected)
        {
            self.selected = visible.first().map(|torrent| torrent.info_hash);
            self.detail_scroll = 0;
        }
    }

    /// Moves the selection `by` rows down the table (up if negative).
    fn move_selection(&mut self, snapshot: &Snapshot, by: isize) {
        let visible = self.visible(snapshot);
        if visible.is_empty() {
            return;
        }
        let current = visible
            .iter()
            .position(|torrent| Some(torrent.info_hash) == self.selected)
            .unwrap_or(0);
        let next = current.saturating_add_signed(by).min(visible.len() - 1);
        if next != current {
            self.selected = Some(visible[next].info_hash);
            self.detail_scroll = 0;
        }
    }

    fn scroll_detail(&mut self, by: isize) {
        self.detail_scroll = self.detail_scroll.saturating_add_signed(by);
    }

    pub fn handle_key(&mut self, key: KeyEvent, snapshot: &Snapshot) -> Action {
        let ctrl_c =
            key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c');
        if ctrl_c {
            return self.quit(snapshot);
        }
        if self.show_help {
            self.show_help = false;
            return Action::None;
        }
        if self.editing_filter {
            match key.code {
                KeyCode::Esc => {
                    self.filter.clear();
                    self.editing_filter = false;
                }
                KeyCode::Enter => self.editing_filter = false,
                KeyCode::Backspace => {
                    self.filter.pop();
                }
                KeyCode::Char(c) => self.filter.push(c),
                _ => {}
            }
            self.ensure_selection(snapshot);
            return Action::None;
        }
        const PAGE: isize = 10;
        match key.code {
            KeyCode::Char('q') => return self.quit(snapshot),
            KeyCode::Up | KeyCode::Char('k') => match self.focus {
                Focus::Torrents => self.move_selection(snapshot, -1),
                Focus::Detail => self.scroll_detail(-1),
            },
            KeyCode::Down | KeyCode::Char('j') => match self.focus {
                Focus::Torrents => self.move_selection(snapshot, 1),
                Focus::Detail => self.scroll_detail(1),
            },
            KeyCode::PageUp => self.scroll_detail(-PAGE),
            KeyCode::PageDown => self.scroll_detail(PAGE),
            KeyCode::Tab | KeyCode::BackTab => {
                self.focus = match self.focus {
                    Focus::Torrents => Focus::Detail,
                    Focus::Detail => Focus::Torrents,
                };
            }
            KeyCode::Char(c @ '1'..='5') => {
                let tab = Tab::ALL[c as usize - '1' as usize];
                if tab != self.tab {
                    self.tab = tab;
                    self.detail_scroll = 0;
                }
            }
            KeyCode::Char('l') => self.show_log = !self.show_log,
            KeyCode::Char('s') => self.sort = self.sort.next(),
            KeyCode::Char('S') => self.reverse = !self.reverse,
            KeyCode::Char('/') => self.editing_filter = true,
            KeyCode::Esc => self.filter.clear(),
            KeyCode::Char('?') => self.show_help = true,
            _ => {}
        }
        self.ensure_selection(snapshot);
        Action::None
    }

    /// `q` or Ctrl-C: once everything has finished, leave; otherwise ask
    /// the torrents to stop, and leave at once if asked again.
    fn quit(&mut self, snapshot: &Snapshot) -> Action {
        if snapshot.finished {
            Action::Quit
        } else if self.stopping {
            Action::ForceQuit
        } else {
            self.stopping = true;
            Action::Stop
        }
    }
}

/// Byte counters seen at one tick.
#[derive(Debug, Clone, Copy)]
struct Sample {
    at: Instant,
    down: u64,
    up: u64,
}

/// One torrent's or peer's recent samples, and the rates between them.
#[derive(Debug, Default)]
struct History {
    samples: VecDeque<Sample>,
    down: VecDeque<f64>,
    up: VecDeque<f64>,
}

impl History {
    fn record(&mut self, sample: Sample) {
        if let Some(last) = self.samples.back() {
            let (down, up) = rates(last, &sample);
            push_capped(&mut self.down, down);
            push_capped(&mut self.up, up);
        }
        // One more sample than rates, so `HISTORY` rates span them.
        if self.samples.len() > HISTORY {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    /// Rates averaged over the last `SMOOTHING` samples.
    fn current(&self) -> (f64, f64) {
        let (Some(last), Some(first)) = (
            self.samples.back(),
            self.samples
                .get(self.samples.len().saturating_sub(SMOOTHING + 1)),
        ) else {
            return (0.0, 0.0);
        };
        rates(first, last)
    }

    /// The download rate averaged over the last `ETA_WINDOW`.
    fn average_down(&self) -> f64 {
        let Some(last) = self.samples.back() else {
            return 0.0;
        };
        let first = self
            .samples
            .iter()
            .find(|sample| last.at.duration_since(sample.at) <= ETA_WINDOW)
            .unwrap_or(last);
        rates(first, last).0
    }
}

fn push_capped(rates: &mut VecDeque<f64>, rate: f64) {
    if rates.len() == HISTORY {
        rates.pop_front();
    }
    rates.push_back(rate);
}

/// Bytes a second each way between two samples.
fn rates(from: &Sample, to: &Sample) -> (f64, f64) {
    let secs = to.at.saturating_duration_since(from.at).as_secs_f64();
    if secs <= 0.0 {
        return (0.0, 0.0);
    }
    (
        to.down.saturating_sub(from.down) as f64 / secs,
        to.up.saturating_sub(from.up) as f64 / secs,
    )
}

/// Rate history of every torrent, and of the selected torrent's peers.
#[derive(Debug, Default)]
pub struct Rates {
    torrents: HashMap<[u8; 20], History>,
    peers: HashMap<([u8; 20], SocketAddr), History>,
}

impl Rates {
    /// Records the byte counters in `snapshot`, taken at `at`. Torrents and
    /// peers no longer in it are forgotten.
    pub fn record(&mut self, snapshot: &Snapshot, at: Instant) {
        self.torrents
            .retain(|hash, _| snapshot.torrent(*hash).is_some());
        for torrent in &snapshot.torrents {
            self.torrents
                .entry(torrent.info_hash)
                .or_default()
                .record(Sample {
                    at,
                    down: torrent.received,
                    up: torrent.uploaded,
                });
        }
        let Some(detail) = &snapshot.detail else {
            self.peers.clear();
            return;
        };
        self.peers.retain(|(hash, addr), _| {
            *hash == detail.info_hash && detail.peers.iter().any(|peer| peer.addr == *addr)
        });
        for peer in &detail.peers {
            self.peers
                .entry((detail.info_hash, peer.addr))
                .or_default()
                .record(Sample {
                    at,
                    down: peer.downloaded,
                    up: peer.uploaded,
                });
        }
    }

    /// Fills in the rates, ETAs and histories of `snapshot` from what was
    /// recorded.
    pub fn apply(&self, snapshot: &mut Snapshot) {
        for torrent in &mut snapshot.torrents {
            let Some(history) = self.torrents.get(&torrent.info_hash) else {
                continue;
            };
            (torrent.rate_down, torrent.rate_up) = history.current();
            torrent.eta = match torrent.left() {
                Some(0) | None => None,
                Some(left) => {
                    let rate = history.average_down();
                    (rate > 0.0).then(|| Duration::from_secs_f64(left as f64 / rate))
                }
            };
        }
        let Some(detail) = &mut snapshot.detail else {
            return;
        };
        if let Some(history) = self.torrents.get(&detail.info_hash) {
            detail.down_history = history.down.iter().copied().collect();
            detail.up_history = history.up.iter().copied().collect();
        }
        for peer in &mut detail.peers {
            if let Some(history) = self.peers.get(&(detail.info_hash, peer.addr)) {
                (peer.rate_down, peer.rate_up) = history.current();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::TorrentState;
    use crate::tui::snapshot::Detail;
    use crossterm::event::KeyEvent;
    use std::path::PathBuf;

    pub(crate) fn torrent(hash: u8, name: &str) -> TorrentSnapshot {
        TorrentSnapshot {
            info_hash: [hash; 20],
            name: name.into(),
            state: TorrentState::Downloading,
            layout: None,
            save_path: PathBuf::new(),
            verified: 0,
            received: 0,
            downloaded: 0,
            uploaded: 0,
            peers: 0,
            pool: 0,
            active: Duration::ZERO,
            order: hash as usize,
            rate_down: 0.0,
            rate_up: 0.0,
            eta: None,
        }
    }

    fn snapshot(names: &[&str]) -> Snapshot {
        Snapshot {
            torrents: names
                .iter()
                .enumerate()
                .map(|(i, name)| torrent(i as u8, name))
                .collect(),
            ..Snapshot::default()
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn names<'a>(app: &App, snapshot: &'a Snapshot) -> Vec<&'a str> {
        app.visible(snapshot)
            .iter()
            .map(|torrent| torrent.name.as_str())
            .collect()
    }

    #[test]
    fn selection_moves_within_the_table() {
        let snapshot = snapshot(&["a", "b", "c"]);
        let mut app = App::new(Theme::default());
        app.ensure_selection(&snapshot);
        assert_eq!(app.selected, Some([0; 20]));
        app.handle_key(key(KeyCode::Up), &snapshot);
        assert_eq!(app.selected, Some([0; 20]));
        app.handle_key(key(KeyCode::Down), &snapshot);
        app.handle_key(key(KeyCode::Char('j')), &snapshot);
        app.handle_key(key(KeyCode::Char('j')), &snapshot);
        assert_eq!(app.selected, Some([2; 20]));
        // With the detail panel focused, the arrows scroll it instead.
        app.handle_key(key(KeyCode::Tab), &snapshot);
        app.handle_key(key(KeyCode::Char('k')), &snapshot);
        assert_eq!(app.selected, Some([2; 20]));
        app.handle_key(key(KeyCode::Down), &snapshot);
        assert_eq!(app.detail_scroll, 1);
        app.handle_key(key(KeyCode::Char('3')), &snapshot);
        assert_eq!((app.tab, app.detail_scroll), (Tab::Trackers, 0));
    }

    #[test]
    fn quitting_stops_first_and_leaves_on_the_second_press() {
        let mut snapshot = snapshot(&["a"]);
        let mut app = App::new(Theme::default());
        assert_eq!(
            app.handle_key(key(KeyCode::Char('q')), &snapshot),
            Action::Stop
        );
        let ctrl_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
        assert_eq!(app.handle_key(ctrl_c, &snapshot), Action::ForceQuit);
        snapshot.finished = true;
        assert_eq!(
            app.handle_key(key(KeyCode::Char('q')), &snapshot),
            Action::Quit
        );
    }

    #[test]
    fn sorts_and_filters() {
        let mut snapshot = snapshot(&["debian", "Arch", "ubuntu"]);
        snapshot.torrents[2].rate_down = 5.0;
        snapshot.torrents[0].rate_down = 1.0;
        let mut app = App::new(Theme::default());
        assert_eq!(names(&app, &snapshot), ["debian", "Arch", "ubuntu"]);
        app.handle_key(key(KeyCode::Char('s')), &snapshot);
        assert_eq!(app.sort, Sort::Name);
        assert_eq!(names(&app, &snapshot), ["Arch", "debian", "ubuntu"]);
        app.handle_key(key(KeyCode::Char('S')), &snapshot);
        assert_eq!(names(&app, &snapshot), ["ubuntu", "debian", "Arch"]);
        app.sort = Sort::Down;
        app.reverse = false;
        assert_eq!(names(&app, &snapshot), ["Arch", "debian", "ubuntu"]);

        app.ensure_selection(&snapshot);
        for c in "/BIAN".chars() {
            app.handle_key(key(KeyCode::Char(c)), &snapshot);
        }
        assert_eq!(names(&app, &snapshot), ["debian"]);
        assert_eq!(app.selected, Some([0; 20]));
        // Typing goes to the filter, not to the keys.
        app.handle_key(key(KeyCode::Char('q')), &snapshot);
        assert_eq!(app.filter, "BIANq");
        assert!(!app.stopping);
        app.handle_key(key(KeyCode::Backspace), &snapshot);
        app.handle_key(key(KeyCode::Enter), &snapshot);
        assert!(!app.editing_filter);
        assert_eq!(app.filter, "BIAN");
        app.handle_key(key(KeyCode::Esc), &snapshot);
        assert_eq!(names(&app, &snapshot).len(), 3);
    }

    #[test]
    fn rates_are_smoothed_and_the_eta_averaged() {
        let start = Instant::now();
        let mut snapshot = snapshot(&["a"]);
        snapshot.torrents[0].layout = Some(crate::stats::Layout {
            size: 10_000,
            piece_length: 100,
            pieces: 100,
            private: false,
            files: Vec::new(),
        });
        snapshot.detail = Some(Detail {
            info_hash: [0; 20],
            ..Detail::default()
        });
        let mut rates = Rates::default();
        // 100 B/s for 10 s, then 500 B/s.
        for tick in 0..=60u64 {
            let at = start + Duration::from_millis(250 * tick);
            let secs = tick as f64 / 4.0;
            let bytes = if secs <= 10.0 {
                secs * 100.0
            } else {
                1000.0 + (secs - 10.0) * 500.0
            };
            snapshot.torrents[0].received = bytes as u64;
            snapshot.torrents[0].uploaded = tick * 10;
            rates.record(&snapshot, at);
        }
        snapshot.torrents[0].verified = 4000;
        rates.apply(&mut snapshot);
        let torrent = &snapshot.torrents[0];
        assert!(
            (torrent.rate_down - 500.0).abs() < 1.0,
            "{}",
            torrent.rate_down
        );
        assert!((torrent.rate_up - 40.0).abs() < 1.0, "{}", torrent.rate_up);
        // 6000 B left at the 15 s average of (1000 + 2500) / 15.
        let eta = torrent.eta.unwrap().as_secs_f64();
        assert!((eta - 6000.0 / (3500.0 / 15.0)).abs() < 1.0, "{eta}");
        let detail = snapshot.detail.as_ref().unwrap();
        assert_eq!(detail.down_history.len(), 60);
        assert_eq!(detail.down_history[0], 100.0);
        assert_eq!(*detail.down_history.last().unwrap(), 500.0);

        // A torrent that leaves is forgotten.
        snapshot.torrents.clear();
        rates.record(&snapshot, start + Duration::from_secs(20));
        assert!(rates.torrents.is_empty());
    }
}
