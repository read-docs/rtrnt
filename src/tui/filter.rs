//! The views of the torrent table: all of it, or the torrents in some state,
//! or with some tag, or both. They combine with the name filter (`/`).
//!
//! Everything here is pure, so the keys and the table can be tested without
//! a screen.

use crate::stats::TorrentState;

/// Which states of torrent the table shows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StateView {
    #[default]
    All,
    /// Downloading, or fetching the metadata of a magnet link.
    Downloading,
    Seeding,
    /// Paused, or pausing.
    Paused,
    /// Waiting for a slot (see `queue`) or for another torrent's check.
    Queued,
    /// Failed.
    Error,
}

impl StateView {
    /// Every view, in the order `f` goes through them.
    pub const ALL: [StateView; 6] = [
        StateView::All,
        StateView::Downloading,
        StateView::Seeding,
        StateView::Paused,
        StateView::Queued,
        StateView::Error,
    ];

    /// What the table's title calls the view.
    pub fn name(self) -> &'static str {
        match self {
            StateView::All => "all",
            StateView::Downloading => "downloading",
            StateView::Seeding => "seeding",
            StateView::Paused => "paused",
            StateView::Queued => "queued",
            StateView::Error => "error",
        }
    }

    /// The view after this one, the first after the last.
    pub fn next(self) -> Self {
        self.step(1)
    }

    /// The view before this one, the last before the first.
    pub fn previous(self) -> Self {
        self.step(Self::ALL.len() - 1)
    }

    fn step(self, by: usize) -> Self {
        let index = Self::ALL.iter().position(|&view| view == self).unwrap_or(0);
        Self::ALL[(index + by) % Self::ALL.len()]
    }

    /// Whether a torrent in `state` is in this view. A torrent that is
    /// checking, done or stopped is in `All` only.
    pub fn includes(self, state: &TorrentState) -> bool {
        match self {
            StateView::All => true,
            StateView::Downloading => matches!(
                state,
                TorrentState::Downloading | TorrentState::FetchingMetadata { .. }
            ),
            StateView::Seeding => matches!(state, TorrentState::Seeding),
            StateView::Paused => matches!(state, TorrentState::Paused | TorrentState::Pausing),
            StateView::Queued => matches!(
                state,
                TorrentState::Queued | TorrentState::QueuedForChecking
            ),
            StateView::Error => matches!(state, TorrentState::Failed(_)),
        }
    }
}

/// What the torrent table shows apart from the name filter: a state and a
/// tag, both of which a torrent must match.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    pub state: StateView,
    /// Only the torrents with this tag, if set.
    pub tag: Option<String>,
}

impl View {
    /// Whether every torrent is in the view.
    pub fn is_all(&self) -> bool {
        self.state == StateView::All && self.tag.is_none()
    }

    /// Whether a torrent in `state` with `tags` is in the view.
    pub fn includes(&self, state: &TorrentState, tags: &[String]) -> bool {
        self.state.includes(state)
            && self
                .tag
                .as_ref()
                .is_none_or(|wanted| tags.iter().any(|tag| tag == wanted))
    }

    /// The view in words for the table's title, such as `seeding #linux`;
    /// empty for every torrent.
    pub fn label(&self) -> String {
        let state = (self.state != StateView::All).then(|| self.state.name().to_string());
        let tag = self.tag.as_ref().map(|tag| format!("#{tag}"));
        [state, tag]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The tags in use, sorted and without repeats, from the tags of each
/// torrent.
pub fn known_tags<'a>(torrents: impl IntoIterator<Item = &'a [String]>) -> Vec<String> {
    let mut known: Vec<String> = torrents.into_iter().flatten().cloned().collect();
    known.sort();
    known.dedup();
    known
}

/// The tag filter after `current`, going through no tag, then each of
/// `known` (sorted), then round to no tag again; backwards if not
/// `forward`. A `current` not among `known` (nothing has the tag any more)
/// counts as no tag.
pub fn cycle_tag(current: Option<&str>, known: &[String], forward: bool) -> Option<String> {
    // Position 0 is no tag, then each known tag.
    let places = known.len() + 1;
    let here = current
        .and_then(|tag| known.iter().position(|known| known == tag))
        .map_or(0, |index| index + 1);
    let next = if forward {
        (here + 1) % places
    } else {
        (here + places - 1) % places
    };
    next.checked_sub(1).map(|index| known[index].clone())
}

#[cfg(test)]
mod tests;
