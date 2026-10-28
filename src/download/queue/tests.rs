use super::*;
use crate::peer;

use rand::SeedableRng;
use rand::rngs::StdRng;
use std::time::Duration;

fn peer<'a>(queue: &'a PieceQueue, has: &[bool]) -> PeerPieces<'a> {
    let mut peer = queue.register_peer();
    peer.update(has);
    peer
}

fn availability(queue: &PieceQueue) -> Vec<u32> {
    queue.lock().availability.clone()
}

#[test]
fn claims_only_pieces_the_peer_has() {
    let queue = PieceQueue::all_wanted(&[false; 3]);
    assert!(queue.claim(&peer(&queue, &[false; 3]), &[]).is_none());
    let partial = peer(&queue, &[false, true, true]);
    let claim = queue.claim(&partial, &[]).unwrap();
    let second = queue.claim(&partial, &[]).unwrap();
    let mut claimed = [claim.index, second.index];
    claimed.sort();
    assert_eq!(claimed, [1, 2]);
    assert!(queue.claim(&partial, &[]).is_none());
    drop(second);
    claim.complete();
    assert_eq!(queue.remaining(), 2);
    // Piece 0 is now the rarest: only `full` has it.
    let full = peer(&queue, &[true; 3]);
    assert_eq!(queue.claim(&full, &[]).unwrap().index, 0);
}

#[test]
fn snapshot_shows_have_in_flight_and_availability() {
    let queue = PieceQueue::all_wanted(&[true, false, false, false]);
    let first = peer(&queue, &[true, true, false, false]);
    let _second = peer(&queue, &[false, true, true, false]);
    // Piece 1 is the only one `first` has that we lack.
    let claim = queue.claim(&first, &[]).unwrap();
    assert_eq!(claim.index, 1);
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.have, [true, false, false, false]);
    assert_eq!(snapshot.in_flight, [false, true, false, false]);
    assert_eq!(snapshot.availability, [1, 2, 1, 0]);
    claim.complete();
    drop(first);
    let snapshot = queue.snapshot();
    assert_eq!(snapshot.have, [true, true, false, false]);
    assert_eq!(snapshot.in_flight, [false; 4]);
    assert_eq!(snapshot.availability, [0, 1, 1, 0]);
}

#[test]
fn dropped_claim_returns_piece() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    drop(queue.claim(&seed, &[]).unwrap());
    let claim = queue.claim(&seed, &[]).unwrap();
    claim.complete();
    assert!(queue.is_done());
    assert!(queue.claim(&seed, &[]).is_none());
}

#[test]
fn pieces_already_on_disk_are_never_claimed() {
    let queue = PieceQueue::all_wanted(&[true, false, true]);
    assert_eq!(queue.remaining(), 1);
    let seed = peer(&queue, &[true; 3]);
    let claim = queue.claim(&seed, &[]).unwrap();
    assert_eq!(claim.index, 1);
    // Endgame only ever offers piece 1 again, never one we already have.
    assert!(queue.claim(&seed, &[1]).is_none());
    claim.complete();
    assert!(queue.is_done());
    assert!(!queue.wants(&seed));
}

#[test]
fn claims_rarest_piece_first() {
    let queue = PieceQueue::all_wanted(&[false; 4]);
    let _a = peer(&queue, &[true, true, false, true]);
    let _b = peer(&queue, &[true, false, false, true]);
    let seed = peer(&queue, &[true; 4]);
    assert_eq!(availability(&queue), [3, 2, 1, 3]);
    let first = queue.claim(&seed, &[]).unwrap();
    assert_eq!(first.index, 2);
    let second = queue.claim(&seed, &[]).unwrap();
    assert_eq!(second.index, 1);
}

#[test]
fn availability_follows_peer_updates_and_disconnects() {
    let queue = PieceQueue::all_wanted(&[false; 3]);
    let mut a = peer(&queue, &[true, false, false]);
    let b = peer(&queue, &[true, true, false]);
    assert_eq!(availability(&queue), [2, 1, 0]);
    // A Have message, then a (replacement) bitfield that drops a piece.
    a.update(&[true, false, true]);
    assert_eq!(availability(&queue), [2, 1, 1]);
    a.update(&[false, false, true]);
    assert_eq!(availability(&queue), [1, 1, 1]);
    drop(b);
    assert_eq!(availability(&queue), [0, 0, 1]);
    drop(a);
    assert_eq!(availability(&queue), [0, 0, 0]);
}

#[test]
fn wants_pieces_until_they_are_completed() {
    let queue = PieceQueue::all_wanted(&[false; 3]);
    let a = peer(&queue, &[true, true, false]);
    let b = peer(&queue, &[false, true, false]);
    assert!(!queue.wants(&peer(&queue, &[false; 3])));
    assert!(queue.wants(&b));

    // In flight with another peer still counts: endgame may need it.
    let claim = queue.claim(&b, &[]).unwrap();
    assert_eq!(claim.index, 1);
    assert!(queue.wants(&b));
    assert!(claim.complete());
    assert!(!queue.wants(&b));
    assert!(queue.wants(&a));
    queue.claim(&a, &[]).unwrap().complete();
    assert!(!queue.wants(&a));
}

#[test]
fn rarest_ties_are_broken_at_random() {
    let candidates = [(0, 1u32), (1, 5), (2, 1), (3, 1)];
    let has = [true; 4];
    let mut rng = StdRng::seed_from_u64(7);
    let mut seen = [0u32; 4];
    for _ in 0..300 {
        let i = pick_least(candidates, &has, &mut rng).unwrap();
        seen[i] += 1;
    }
    assert_eq!(seen[1], 0, "picked a more common piece");
    for i in [0, 2, 3] {
        assert!(seen[i] > 50, "piece {i} picked {} times: {seen:?}", seen[i]);
    }
}

#[test]
fn pick_least_skips_pieces_the_peer_lacks() {
    let candidates = [(1, 9), (2, 9)];
    let mut rng = StdRng::seed_from_u64(0);
    assert_eq!(
        pick_least(candidates, &[true, false, true], &mut rng),
        Some(2)
    );
    assert_eq!(
        pick_least(candidates, &[true, false, false], &mut rng),
        None
    );
}

#[test]
fn endgame_duplicates_in_flight_pieces() {
    let queue = PieceQueue::all_wanted(&[false; 2]);
    let seed = peer(&queue, &[true; 2]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    // Nothing pending: further claims join in-flight pieces, spreading
    // over the one with the fewest downloaders.
    let c = queue.claim(&seed, &[]).unwrap();
    let d = queue.claim(&seed, &[]).unwrap();
    assert_ne!(c.index, d.index);
    let (first, dup) = if c.index == a.index { (a, c) } else { (b, c) };

    assert!(!dup.is_completed());
    assert!(first.complete());
    assert!(dup.is_completed());
    assert!(!dup.complete(), "a piece must only be completed once");
    assert_eq!(queue.remaining(), 1);
    // The completed piece is never handed out again.
    let e = queue.claim(&seed, &[]).unwrap();
    assert_eq!(e.index, d.index);
}

#[test]
fn endgame_piece_is_requeued_only_when_every_claim_is_dropped() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    assert_eq!(queue.lock().in_flight[&0], 2);
    drop(a);
    assert!(queue.lock().pending.is_empty());
    assert_eq!(queue.lock().in_flight[&0], 1);
    drop(b);
    assert!(queue.lock().pending.contains(&0));
    assert!(queue.lock().in_flight.is_empty());
    assert_eq!(queue.remaining(), 1);
}

#[test]
fn dropping_a_duplicate_after_completion_changes_nothing() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    assert!(a.complete());
    drop(b);
    assert!(queue.is_done());
    assert!(queue.lock().pending.is_empty());
    assert!(queue.claim(&seed, &[]).is_none());
}

#[tokio::test]
async fn closed_queue_hands_out_nothing_and_wakes_workers() {
    let queue = PieceQueue::all_wanted(&[false; 2]);
    let seed = peer(&queue, &[true; 2]);
    let held = queue.claim(&seed, &[]).unwrap();
    assert!(!queue.is_finished());

    let close_later = async {
        tokio::task::yield_now().await;
        queue.close();
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(queue.wait_for(|| queue.is_closed()), close_later)
    })
    .await
    .expect("not woken by close");
    assert!(queue.is_finished());
    assert!(!queue.is_done());
    assert!(queue.claim(&seed, &[]).is_none());

    // A piece written after the close still counts.
    assert!(held.complete());
    assert_eq!(queue.remaining(), 1);
}

#[test]
fn choked_claims_take_only_allowed_fast_pieces() {
    let queue = PieceQueue::all_wanted(&[false, false, true, false]);
    let partial = peer(&queue, &[true, true, false, true]);
    let allowed = [false, true, true, false];
    // Piece 2 is allowed, but we have it already.
    let claim = queue.claim_allowed(&partial, &[], &allowed).unwrap();
    assert_eq!(claim.index, 1);
    assert!(queue.claim_allowed(&partial, &[], &allowed).is_none());
    // Endgame duplicates are limited the same way.
    let others = queue.claim(&partial, &[]).unwrap();
    let rest = queue.claim(&partial, &[others.index]).unwrap();
    assert!(queue.lock().pending.is_empty());
    assert!(queue.claim_allowed(&partial, &[1], &allowed).is_none());
    let dup = queue.claim_allowed(&partial, &[], &allowed).unwrap();
    assert_eq!(dup.index, 1);
    drop((claim, others, rest, dup));
}

#[test]
fn endgame_never_duplicates_a_piece_the_worker_holds() {
    let queue = PieceQueue::all_wanted(&[false; 2]);
    let seed = peer(&queue, &[true; 2]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    assert!(queue.claim(&seed, &[a.index, b.index]).is_none());
    let dup = queue.claim(&seed, &[a.index]).unwrap();
    assert_eq!(dup.index, b.index);
}

#[test]
fn bitfield_and_haves_follow_completed_pieces() {
    let queue = PieceQueue::all_wanted(&[false; 3]);
    assert_eq!(queue.have_message(false), (None, 0));
    assert_eq!(queue.have_message(true), (Some(PeerMessage::HaveNone), 0));
    assert!(!queue.has(0));
    let seed = peer(&queue, &[true; 3]);
    let first = queue.claim(&seed, &[]).unwrap();
    let second = queue.claim(&seed, &[]).unwrap();
    let (a, b) = (first.index, second.index);

    assert!(second.complete());
    assert!(queue.has(b));
    let (message, seen) = queue.have_message(false);
    let bitfield = peer::encode_bitfield(&[0, 1, 2].map(|i| i == b));
    assert_eq!(message, Some(PeerMessage::Bitfield { bitfield }));
    assert_eq!(seen, 1);
    // A worker that sent this bitfield announces only what follows.
    assert!(queue.completed_since(seen).is_empty());
    assert!(first.complete());
    assert_eq!(queue.completed_since(seen), [a]);
    assert_eq!(queue.completed_since(0), [b, a]);
    assert_eq!(queue.completed_count(), 2);
    assert_eq!(queue.have_bitfield(), [0, 1, 2].map(|i| i == a || i == b));
    assert!(queue.completed_since(5).is_empty());
    assert!(!queue.has(3));
    drop(seed);
    queue
        .claim(&peer(&queue, &[true; 3]), &[])
        .unwrap()
        .complete();
    assert_eq!(queue.have_message(true), (Some(PeerMessage::HaveAll), 3));
}

#[test]
fn a_seeding_queue_keeps_workers_until_closed() {
    let queue = PieceQueue::all_wanted(&[true, true]);
    assert!(queue.is_done() && queue.is_finished());
    let queue = PieceQueue::all_wanted(&[true, true]).seeding(true);
    assert!(queue.is_done() && !queue.is_finished());
    queue.close();
    assert!(queue.is_finished());
}

#[test]
fn pieces_on_disk_are_in_the_bitfield_but_never_announced() {
    let queue = PieceQueue::all_wanted(&[true, false, true]);
    assert!(queue.has(0) && !queue.has(1) && queue.has(2));
    let bitfield = vec![0b1010_0000];
    let message = Some(PeerMessage::Bitfield { bitfield });
    assert_eq!(queue.have_message(false), (message.clone(), 0));
    assert_eq!(queue.have_message(true), (message, 0));
    assert!(queue.completed_since(0).is_empty());
}

#[tokio::test]
async fn wait_for_wakes_on_completion() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    let short = Duration::from_millis(50);
    assert!(
        tokio::time::timeout(short, queue.wait_for(|| b.is_completed()))
            .await
            .is_err(),
        "resolved before the piece was completed"
    );
    let complete_later = async {
        tokio::task::yield_now().await;
        a.complete();
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(queue.wait_for(|| b.is_completed()), complete_later)
    })
    .await
    .expect("not woken by completion");
}

const SKIP: Option<Priority> = None;
const NORMAL: Option<Priority> = Some(Priority::Normal);
const HIGH: Option<Priority> = Some(Priority::High);

#[test]
fn skipped_pieces_are_never_claimed() {
    let queue = PieceQueue::new(&[false; 3], &[NORMAL, SKIP, NORMAL]);
    assert_eq!(queue.remaining(), 2);
    assert_eq!(queue.wanted(), 2);
    let seed = peer(&queue, &[true; 3]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    let mut claimed = [a.index, b.index];
    claimed.sort();
    assert_eq!(claimed, [0, 2]);
    // Endgame joins an in-flight piece, never the skipped one.
    let c = queue.claim(&seed, &[a.index]).unwrap();
    assert_eq!(c.index, b.index);
}

#[test]
fn high_goes_before_a_rarer_normal_piece() {
    let queue = PieceQueue::new(&[false; 2], &[NORMAL, HIGH]);
    // Piece 1 is the more common one.
    let _a = peer(&queue, &[false, true]);
    let _b = peer(&queue, &[false, true]);
    let seed = peer(&queue, &[true, true]);
    for _ in 0..20 {
        // Dropping the claim returns the piece to the queue.
        assert_eq!(queue.claim(&seed, &[]).unwrap().index, 1);
    }
}

#[test]
fn rarest_still_decides_within_a_priority() {
    let queue = PieceQueue::new(&[false; 3], &[HIGH, HIGH, NORMAL]);
    let _a = peer(&queue, &[true, false, true]);
    let _b = peer(&queue, &[true, false, true]);
    let seed = peer(&queue, &[true, true, true]);
    assert_eq!(queue.claim(&seed, &[]).unwrap().index, 1);
}

#[test]
fn is_done_without_the_skipped_pieces() {
    let queue = PieceQueue::new(&[false; 3], &[NORMAL, SKIP, SKIP]);
    assert!(!queue.is_done());
    let seed = peer(&queue, &[true; 3]);
    queue.claim(&seed, &[]).unwrap().complete();
    assert!(queue.is_done());
    assert!(queue.is_finished());
    assert_eq!(queue.have_bitfield(), [true, false, false]);
    // A skipped piece that was on disk already is not counted either way.
    let queue = PieceQueue::new(&[false, true, false], &[NORMAL, SKIP, NORMAL]);
    assert_eq!(queue.remaining(), 2);
}

#[test]
fn a_peer_with_only_skipped_pieces_is_not_wanted() {
    let queue = PieceQueue::new(&[false; 3], &[NORMAL, SKIP, SKIP]);
    assert!(!queue.wants(&peer(&queue, &[false, true, true])));
    assert!(queue.wants(&peer(&queue, &[true, false, false])));
}
