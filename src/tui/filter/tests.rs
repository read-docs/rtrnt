use super::*;

fn tags(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| name.to_string()).collect()
}

fn failed() -> TorrentState {
    TorrentState::Failed("disk full".into())
}

#[test]
fn each_state_view_takes_its_states() {
    use TorrentState::*;
    let fetching = FetchingMetadata {
        pieces: 0,
        total: 0,
        peers: 0,
    };
    let checking = Checking { done: 1, total: 2 };
    let states = [
        Downloading,
        fetching,
        Seeding,
        Paused,
        Pausing,
        Queued,
        QueuedForChecking,
        failed(),
        checking,
        Done,
        Interrupted,
        Removing,
    ];
    let in_view = |view: StateView| -> Vec<bool> {
        states.iter().map(|state| view.includes(state)).collect()
    };
    assert!(in_view(StateView::All).iter().all(|&yes| yes));
    let only = |indexes: &[usize]| -> Vec<bool> {
        (0..states.len()).map(|i| indexes.contains(&i)).collect()
    };
    assert_eq!(in_view(StateView::Downloading), only(&[0, 1]));
    assert_eq!(in_view(StateView::Seeding), only(&[2]));
    assert_eq!(in_view(StateView::Paused), only(&[3, 4]));
    assert_eq!(in_view(StateView::Queued), only(&[5, 6]));
    assert_eq!(in_view(StateView::Error), only(&[7]));
}

#[test]
fn state_views_cycle_both_ways() {
    let mut view = StateView::All;
    let mut seen = Vec::new();
    for _ in 0..StateView::ALL.len() {
        view = view.next();
        seen.push(view);
    }
    assert_eq!(
        seen,
        [
            StateView::Downloading,
            StateView::Seeding,
            StateView::Paused,
            StateView::Queued,
            StateView::Error,
            StateView::All
        ]
    );
    assert_eq!(StateView::All.previous(), StateView::Error);
    assert_eq!(StateView::Seeding.previous(), StateView::Downloading);
    for view in StateView::ALL {
        assert_eq!(view.next().previous(), view);
    }
}

#[test]
fn a_view_needs_the_state_and_the_tag() {
    let linux = tags(&["linux", "iso"]);
    let none: Vec<String> = Vec::new();
    let all = View::default();
    assert!(all.is_all());
    assert!(all.includes(&TorrentState::Done, &none));

    let seeding = View {
        state: StateView::Seeding,
        tag: None,
    };
    assert!(seeding.includes(&TorrentState::Seeding, &none));
    assert!(!seeding.includes(&TorrentState::Downloading, &linux));

    let tagged = View {
        state: StateView::All,
        tag: Some("iso".into()),
    };
    assert!(!tagged.is_all());
    assert!(tagged.includes(&TorrentState::Done, &linux));
    assert!(!tagged.includes(&TorrentState::Done, &none));
    assert!(!tagged.includes(&TorrentState::Done, &tags(&["isos"])));

    let both = View {
        state: StateView::Seeding,
        tag: Some("iso".into()),
    };
    assert!(both.includes(&TorrentState::Seeding, &linux));
    assert!(!both.includes(&TorrentState::Seeding, &none));
    assert!(!both.includes(&TorrentState::Paused, &linux));
}

#[test]
fn a_view_is_labelled_for_the_title() {
    assert_eq!(View::default().label(), "");
    let state = View {
        state: StateView::Paused,
        tag: None,
    };
    assert_eq!(state.label(), "paused");
    let tag = View {
        state: StateView::All,
        tag: Some("tv".into()),
    };
    assert_eq!(tag.label(), "#tv");
    let both = View {
        state: StateView::Seeding,
        tag: Some("tv".into()),
    };
    assert_eq!(both.label(), "seeding #tv");
}

#[test]
fn known_tags_are_sorted_and_unique() {
    let a = tags(&["tv", "linux"]);
    let b = tags(&["linux", "archive"]);
    let none: Vec<String> = Vec::new();
    let known = known_tags([a.as_slice(), none.as_slice(), b.as_slice()]);
    assert_eq!(known, tags(&["archive", "linux", "tv"]));
    assert!(known_tags(std::iter::empty()).is_empty());
}

#[test]
fn the_tag_filter_goes_through_no_tag_and_each_tag() {
    let known = tags(&["archive", "linux", "tv"]);
    let mut current: Option<String> = None;
    let mut seen = Vec::new();
    for _ in 0..4 {
        current = cycle_tag(current.as_deref(), &known, true);
        seen.push(current.clone());
    }
    assert_eq!(
        seen,
        [
            Some("archive".to_string()),
            Some("linux".to_string()),
            Some("tv".to_string()),
            None
        ]
    );
    assert_eq!(cycle_tag(None, &known, false), Some("tv".to_string()));
    assert_eq!(
        cycle_tag(Some("linux"), &known, false),
        Some("archive".to_string())
    );
    assert_eq!(cycle_tag(Some("archive"), &known, false), None);
}

#[test]
fn a_tag_nothing_has_any_more_counts_as_no_tag() {
    let known = tags(&["archive", "linux"]);
    assert_eq!(
        cycle_tag(Some("gone"), &known, true),
        Some("archive".to_string())
    );
    assert_eq!(
        cycle_tag(Some("gone"), &known, false),
        Some("linux".to_string())
    );
    // With no tags at all there is nothing to go to.
    assert_eq!(cycle_tag(Some("gone"), &[], true), None);
    assert_eq!(cycle_tag(None, &[], false), None);
}
