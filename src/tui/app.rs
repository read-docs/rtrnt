//! The UI's own state: what is selected and shown, and the rate history
//! that turns byte counters into rates.

use super::Command;
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

/// What the input line is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    /// The torrent filter, applied as it is typed.
    Filter,
    DownloadLimit,
    UploadLimit,
}

impl InputKind {
    /// What the input line says before the text.
    pub fn prompt(self) -> &'static str {
        match self {
            InputKind::Filter => "/",
            InputKind::DownloadLimit => "download limit, KiB/s (0: none): ",
            InputKind::UploadLimit => "upload limit, KiB/s (0: none): ",
        }
    }
}

/// The input line, drawn over the key bar while something is typed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Input {
    pub kind: InputKind,
    pub text: String,
    /// Why the text was refused, until it is edited.
    pub error: Option<String>,
}

impl Input {
    fn new(kind: InputKind, text: String) -> Self {
        Self {
            kind,
            text,
            error: None,
        }
    }
}

/// What a key press asks of the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    None,
    /// Pass this on to the session (see `Feed::command`).
    Command(Command),
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
    /// Set while something is being typed.
    pub input: Option<Input>,
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
            input: None,
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

    /// Scrolls the detail panel `by` rows, no further than its last row.
    fn scroll_detail(&mut self, snapshot: &Snapshot, by: isize) {
        let detail = snapshot.detail.as_ref();
        let rows = match self.tab {
            Tab::Peers => detail.map_or(0, |detail| detail.peers.len()),
            // The DHT, PeX and incoming rows, after a blank one.
            Tab::Trackers => detail.map_or(0, |detail| detail.trackers.len() + 4),
            Tab::Files => self
                .selected
                .and_then(|hash| snapshot.torrent(hash))
                .and_then(|torrent| torrent.layout.as_ref())
                .map_or(0, |layout| layout.files.len()),
            Tab::Pieces | Tab::Info => 0,
        };
        self.detail_scroll = self
            .detail_scroll
            .saturating_add_signed(by)
            .min(rows.saturating_sub(1));
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
        if self.input.is_some() {
            let action = self.edit_input(key, snapshot);
            self.ensure_selection(snapshot);
            return action;
        }
        const PAGE: isize = 10;
        match key.code {
            KeyCode::Char('q') => return self.quit(snapshot),
            KeyCode::Up | KeyCode::Char('k') => match self.focus {
                Focus::Torrents => self.move_selection(snapshot, -1),
                Focus::Detail => self.scroll_detail(snapshot, -1),
            },
            KeyCode::Down | KeyCode::Char('j') => match self.focus {
                Focus::Torrents => self.move_selection(snapshot, 1),
                Focus::Detail => self.scroll_detail(snapshot, 1),
            },
            KeyCode::PageUp => self.scroll_detail(snapshot, -PAGE),
            KeyCode::PageDown => self.scroll_detail(snapshot, PAGE),
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
            KeyCode::Char('/') => {
                self.input = Some(Input::new(InputKind::Filter, self.filter.clone()));
            }
            KeyCode::Char('d') => {
                let limit = snapshot.session.rate_limits.0.to_string();
                self.input = Some(Input::new(InputKind::DownloadLimit, limit));
            }
            KeyCode::Char('u') => {
                let limit = snapshot.session.rate_limits.1.to_string();
                self.input = Some(Input::new(InputKind::UploadLimit, limit));
            }
            KeyCode::Esc => self.filter.clear(),
            KeyCode::Char('?') => self.show_help = true,
            _ => {}
        }
        self.ensure_selection(snapshot);
        Action::None
    }

    /// A key pressed while the input line is open: Enter submits, Esc
    /// cancels (and clears the filter), Backspace deletes a character.
    fn edit_input(&mut self, key: KeyEvent, snapshot: &Snapshot) -> Action {
        let Some(input) = self.input.as_mut() else {
            return Action::None;
        };
        match key.code {
            KeyCode::Esc => {
                if input.kind == InputKind::Filter {
                    self.filter.clear();
                }
                self.input = None;
            }
            KeyCode::Enter => return self.submit(snapshot),
            KeyCode::Backspace => {
                input.text.pop();
                input.error = None;
            }
            KeyCode::Char(c) => {
                input.text.push(c);
                input.error = None;
            }
            _ => {}
        }
        if let Some(Input {
            kind: InputKind::Filter,
            text,
            ..
        }) = &self.input
        {
            self.filter.clone_from(text);
        }
        Action::None
    }

    /// Enter on the input line: closes it with the command it asks for, or
    /// keeps it open, saying why, if the text will not do.
    fn submit(&mut self, snapshot: &Snapshot) -> Action {
        let Some(mut input) = self.input.take() else {
            return Action::None;
        };
        let (down, up) = snapshot.session.rate_limits;
        let limit = || input.text.trim().parse::<u64>();
        let command = match input.kind {
            // Applied already, as it was typed.
            InputKind::Filter => return Action::None,
            InputKind::DownloadLimit => limit().map(|down| Command::SetRateLimits(down, up)),
            InputKind::UploadLimit => limit().map(|up| Command::SetRateLimits(down, up)),
        };
        match command {
            Ok(command) => Action::Command(command),
            Err(_) => {
                input.error = Some("enter a whole number of KiB/s, 0 for none".to_string());
                self.input = Some(input);
                Action::None
            }
        }
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
            Action::Command(Command::Stop)
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
mod tests;
