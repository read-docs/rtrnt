use super::*;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn rng() -> StdRng {
    StdRng::seed_from_u64(7)
}

#[test]
fn the_least_offered_piece_is_chosen() {
    let chosen = choose_piece(&[2, 1, 3, 2], &[0; 4], &[false; 4], &[true; 4], &mut rng());
    assert_eq!(chosen, Some(1));
}

#[test]
fn ties_go_to_the_piece_fewer_peers_have() {
    let chosen = choose_piece(&[1, 1, 1], &[3, 1, 2], &[false; 3], &[true; 3], &mut rng());
    assert_eq!(chosen, Some(1));
}

#[test]
fn pieces_the_peer_has_or_we_lack_are_not_offered() {
    let have = [true, true, false, true];
    // Piece 1 is the peer's and piece 2 is not ours: 0 is the least offered
    // of the rest.
    let peer_has = [false, true, false, false];
    let chosen = choose_piece(&[5, 0, 0, 9], &[0; 4], &peer_has, &have, &mut rng());
    assert_eq!(chosen, Some(0));
    let peer_has = [true, true, false, false];
    let chosen = choose_piece(&[0, 0, 0, 9], &[0; 4], &peer_has, &have, &mut rng());
    assert_eq!(chosen, Some(3));
}

#[test]
fn a_peer_with_everything_we_have_is_offered_nothing() {
    let have = [true, false, true];
    let chosen = choose_piece(&[0; 3], &[0; 3], &[true, false, true], &have, &mut rng());
    assert_eq!(chosen, None);
}

#[test]
fn offers_spread_over_all_pieces_before_any_repeats() {
    let offers = Offers::new(5);
    let mut rng = rng();
    let mut seen: Vec<usize> = (0..5)
        .map(|_| offers.offer(&[false; 5], &[true; 5], &[0; 5], &mut rng))
        .map(Option::unwrap)
        .collect();
    seen.sort_unstable();
    assert_eq!(seen, [0, 1, 2, 3, 4]);
    let counts = offers.counts.lock().unwrap().clone();
    assert_eq!(counts, [1; 5]);
}

#[test]
fn a_new_peer_is_due_at_once() {
    let mut peer = PeerOffer::default();
    assert!(peer.due(Instant::now(), &[false; 3], |_| 0));
    assert_eq!(peer.piece, None);
}

#[test]
fn a_peer_is_not_due_until_it_has_the_piece() {
    let mut peer = PeerOffer::default();
    peer.offered(1);
    // Not even if others have it and ages pass.
    assert!(!peer.due(Instant::now() + PATIENCE * 2, &[false; 3], |_| 5));
    assert_eq!(peer.piece, Some(1));
}

#[test]
fn a_peer_is_due_once_another_peer_has_the_piece() {
    let mut peer = PeerOffer::default();
    peer.offered(1);
    let now = Instant::now();
    let has = [false, true, false];
    // Only this peer has it.
    assert!(!peer.due(now, &has, |piece| {
        assert_eq!(piece, 1);
        1
    }));
    assert!(peer.due(now, &has, |_| 2));
}

#[test]
fn a_lone_peer_is_due_after_the_patience() {
    let mut peer = PeerOffer::default();
    peer.offered(0);
    let now = Instant::now();
    let has = [true, false];
    assert!(!peer.due(now, &has, |_| 1));
    assert!(!peer.due(now + PATIENCE / 2, &has, |_| 1));
    assert!(peer.due(now + PATIENCE, &has, |_| 1));
}

#[test]
fn the_patience_counts_from_when_the_peer_was_seen_with_the_piece() {
    let mut peer = PeerOffer::default();
    peer.offered(0);
    let start = Instant::now();
    // Offered long ago, but only seen holding it now.
    assert!(!peer.due(start + PATIENCE * 3, &[false, false], |_| 1));
    let later = start + PATIENCE * 4;
    assert!(!peer.due(later, &[true, false], |_| 1));
    assert!(peer.due(later + PATIENCE, &[true, false], |_| 1));
}

#[test]
fn a_new_offer_starts_the_wait_afresh() {
    let mut peer = PeerOffer::default();
    peer.offered(0);
    let now = Instant::now();
    assert!(peer.due(now + PATIENCE, &[true, false], |_| 2));
    peer.offered(1);
    assert!(!peer.due(now + PATIENCE, &[true, false], |_| 2));
    assert!(!peer.due(now + PATIENCE, &[true, true], |_| 1));
}
