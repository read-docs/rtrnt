use super::*;

use rand::SeedableRng;
use rand::rngs::StdRng;

fn peer<'a>(queue: &'a PieceQueue, has: &[bool]) -> PeerPieces<'a> {
    let mut peer = queue.register_peer();
    peer.update(has);
    peer
}

fn availability(queue: &PieceQueue) -> Vec<u32> {
    queue.lock().availability.clone()
}

#[test]
fn bar_labels_line_up() {
    assert_eq!(bar_label("a.iso"), format!("{:<20}", "a.iso"));
    let long = bar_label("ubuntu-24.04-desktop-amd64.iso");
    assert_eq!(long, "ubuntu-24.04-deskto…");
    assert_eq!(bar_label("żółw-ąęść-łódź-źdźbło-x").chars().count(), 20);
}

#[test]
fn claims_only_pieces_the_peer_has() {
    let queue = PieceQueue::new(&[false; 3]);
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
fn dropped_claim_returns_piece() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    drop(queue.claim(&seed, &[]).unwrap());
    let claim = queue.claim(&seed, &[]).unwrap();
    claim.complete();
    assert!(queue.is_done());
    assert!(queue.claim(&seed, &[]).is_none());
}

#[test]
fn pieces_already_on_disk_are_never_claimed() {
    let queue = PieceQueue::new(&[true, false, true]);
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
    let queue = PieceQueue::new(&[false; 4]);
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
    let queue = PieceQueue::new(&[false; 3]);
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
    let queue = PieceQueue::new(&[false; 3]);
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
    let candidates = [(0, 1), (1, 5), (2, 1), (3, 1)];
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
    let queue = PieceQueue::new(&[false; 2]);
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
    let queue = PieceQueue::new(&[false; 1]);
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
    let queue = PieceQueue::new(&[false; 1]);
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
    let queue = PieceQueue::new(&[false; 2]);
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

/// A piece of two and a half blocks, its hash, and an `ActivePiece` for it.
fn active<'a>(queue: &'a PieceQueue, seed: &PeerPieces<'_>) -> (Vec<u8>, ActivePiece<'a>) {
    let len = BLOCK_SIZE as usize * 5 / 2;
    let data: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
    let hash = sha1::Sha1::digest(&data).into();
    let claim = queue.claim(seed, &[]).unwrap();
    let piece = ActivePiece::new(claim, len as u64, hash).unwrap();
    (data, piece)
}

fn block(data: &[u8], block: usize) -> &[u8] {
    let start = block * BLOCK_SIZE as usize;
    &data[start..data.len().min(start + BLOCK_SIZE as usize)]
}

#[test]
fn active_piece_requests_blocks_in_order_and_assembles_them() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    let mut lengths = Vec::new();
    while piece.has_unrequested() {
        let PeerMessage::Request { begin, length, .. } = piece.request_next() else {
            panic!("not a request");
        };
        lengths.push((begin, length));
    }
    let half = BLOCK_SIZE / 2;
    assert_eq!(
        lengths,
        [
            (0, BLOCK_SIZE),
            (BLOCK_SIZE, BLOCK_SIZE),
            (2 * BLOCK_SIZE, half)
        ]
    );
    assert_eq!(piece.in_flight(), 3);

    for b in [2, 0, 1] {
        assert!(!piece.is_complete());
        assert!(
            piece
                .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                .unwrap()
        );
    }
    assert_eq!(piece.in_flight(), 0);
    assert!(piece.is_complete());
    let (claim, assembled) = piece.verify().expect("hash mismatch");
    assert_eq!(assembled, data);
    assert!(claim.complete());
}

#[test]
fn active_piece_ignores_duplicate_and_unrequested_blocks() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    piece.request_next();
    // A late reply to a request abandoned on a choke, for a block we
    // have not asked for again yet.
    assert!(!piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
    assert_eq!(piece.in_flight(), 1);
    assert!(piece.add_block(0, block(&data, 0)).unwrap());
    assert!(!piece.add_block(0, block(&data, 0)).unwrap());
    assert_eq!(piece.in_flight(), 0);
    assert_eq!(piece.done, 1);
}

#[test]
fn active_piece_rejects_invalid_blocks() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    while piece.has_unrequested() {
        piece.request_next();
    }
    let full = vec![0u8; BLOCK_SIZE as usize];
    for (begin, block) in [
        (1, block(&data, 0)),
        (0, &full[1..]),
        (2 * BLOCK_SIZE, &full[..]),
        (3 * BLOCK_SIZE, block(&data, 2)),
    ] {
        let err = piece.add_block(begin, block).unwrap_err();
        assert!(err.is::<Misbehaved>(), "begin {begin}: {err:#}");
    }
}

#[test]
fn active_piece_cancels_only_outstanding_requests() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    piece.request_next();
    piece.request_next();
    piece.add_block(0, block(&data, 0)).unwrap();
    let cancels: Vec<PeerMessage> = piece.outstanding().map(BlockRequest::cancel).collect();
    assert_eq!(
        cancels,
        [PeerMessage::Cancel {
            index: 0,
            begin: BLOCK_SIZE,
            length: BLOCK_SIZE,
        }]
    );
}

#[test]
fn rejected_blocks_are_requested_again_first() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    let request = |block: u32| PeerMessage::Request {
        index: 0,
        begin: block * BLOCK_SIZE,
        length: BLOCK_SIZE.min(data.len() as u32 - block * BLOCK_SIZE),
    };
    assert_eq!(piece.request_next(), request(0));
    assert_eq!(piece.request_next(), request(1));
    // Not outstanding: unrequested, wrong length, not a block start.
    assert!(!piece.reject(2 * BLOCK_SIZE, BLOCK_SIZE / 2));
    assert!(!piece.reject(0, BLOCK_SIZE - 1));
    assert!(!piece.reject(1, BLOCK_SIZE));
    assert!(!piece.reject(9 * BLOCK_SIZE, BLOCK_SIZE));

    assert!(piece.reject(0, BLOCK_SIZE));
    assert!(!piece.reject(0, BLOCK_SIZE), "rejected twice");
    assert_eq!(piece.in_flight(), 1);
    // A block that arrives after all is ignored until asked for again.
    assert!(!piece.add_block(0, block(&data, 0)).unwrap());
    assert_eq!(piece.request_next(), request(0));
    assert_eq!(piece.request_next(), request(2));
    assert!(!piece.has_unrequested());
    let outstanding: Vec<BlockRequest> = piece.outstanding().collect();
    assert_eq!(outstanding.len(), 3);

    assert!(piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
    assert!(!piece.reject(BLOCK_SIZE, BLOCK_SIZE), "already received");
    assert!(piece.reject(2 * BLOCK_SIZE, BLOCK_SIZE / 2));
    assert_eq!(piece.request_next(), request(2));
    for b in [0, 2] {
        assert!(
            piece
                .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                .unwrap()
        );
    }
    assert!(piece.is_complete());
    assert_eq!(piece.in_flight(), 0);
    assert!(piece.verify().is_some());
}

#[test]
fn choked_claims_take_only_allowed_fast_pieces() {
    let queue = PieceQueue::new(&[false, false, true, false]);
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
fn a_choke_releases_pieces_with_nothing_outstanding_unless_allowed_fast() {
    let queue = PieceQueue::new(&[false; 3]);
    let seed = peer(&queue, &[true; 3]);
    let piece = |index: usize| {
        let claim = queue
            .claim_from(&[0, 1, 2].map(|i| i == index), &[])
            .unwrap();
        ActivePiece::new(claim, u64::from(2 * BLOCK_SIZE), [0; 20]).unwrap()
    };
    let (mut waiting, idle, allowed) = (piece(0), piece(1), piece(2));
    waiting.request_next();
    let mut active = vec![waiting, idle, allowed];
    release_choked(&mut active, &[false, false, true]);
    let held: Vec<usize> = active.iter().map(|piece| piece.claim.index).collect();
    assert_eq!(held, [0, 2]);
    assert!(queue.lock().pending.contains(&1));

    // Once its request is rejected, the first piece goes too.
    assert!(active[0].reject(0, BLOCK_SIZE));
    release_choked(&mut active, &[false, false, true]);
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].claim.index, 2);
    assert!(queue.lock().pending.contains(&0));
    drop(seed);
}

#[test]
fn corrupt_piece_fails_verification_and_is_returned() {
    let queue = PieceQueue::new(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (mut data, mut piece) = active(&queue, &seed);
    data[0] ^= 0xff;
    for b in 0..3 {
        piece.request_next();
        piece
            .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
            .unwrap();
    }
    assert!(piece.verify().is_none());
    assert!(queue.lock().pending.contains(&0));
}

#[test]
fn endgame_never_duplicates_a_piece_the_worker_holds() {
    let queue = PieceQueue::new(&[false; 2]);
    let seed = peer(&queue, &[true; 2]);
    let a = queue.claim(&seed, &[]).unwrap();
    let b = queue.claim(&seed, &[]).unwrap();
    assert!(queue.claim(&seed, &[a.index, b.index]).is_none());
    let dup = queue.claim(&seed, &[a.index]).unwrap();
    assert_eq!(dup.index, b.index);
}

#[test]
fn pipeline_follows_the_download_rate() {
    let start = Instant::now();
    let mut pipeline = Pipeline::new(start);
    assert_eq!(pipeline.depth(), INITIAL_PIPELINE);
    let block = u64::from(BLOCK_SIZE);

    // Nothing changes until a whole window has passed.
    pipeline.received(100 * block, start + RATE_WINDOW / 2);
    assert_eq!(pipeline.depth(), INITIAL_PIPELINE);
    // 100 blocks in a second: three seconds' worth.
    pipeline.received(0, start + RATE_WINDOW);
    assert_eq!(pipeline.depth(), 300.min(MAX_PIPELINE));

    // 10 blocks over the next two seconds.
    let later = start + RATE_WINDOW + Duration::from_secs(2);
    pipeline.received(10 * block, later);
    assert_eq!(pipeline.depth(), 15);

    // One block in ten seconds: never below the minimum.
    let much_later = later + Duration::from_secs(10);
    pipeline.received(block, much_later);
    assert_eq!(pipeline.depth(), MIN_PIPELINE);

    // Way past the maximum.
    pipeline.received(10_000 * block, much_later + RATE_WINDOW);
    assert_eq!(pipeline.depth(), MAX_PIPELINE);
}

#[test]
fn pipeline_restart_discards_idle_time() {
    let start = Instant::now();
    let mut pipeline = Pipeline::new(start);
    let block = u64::from(BLOCK_SIZE);
    // Choked for a minute, then 20 blocks in the first second.
    let resumed = start + Duration::from_secs(60);
    pipeline.restart(resumed);
    pipeline.received(20 * block, resumed + RATE_WINDOW);
    assert_eq!(pipeline.depth(), 60);
}

#[test]
fn a_peer_is_connected_to_once_at_a_time() {
    let peers = ConnectedPeers::default();
    let a = peers.register([1; 20]).unwrap();
    assert!(peers.register([1; 20]).is_none());
    let _b = peers.register([2; 20]).unwrap();
    drop(a);
    assert!(peers.register([1; 20]).is_some());
}

#[test]
fn bitfield_and_haves_follow_completed_pieces() {
    let queue = PieceQueue::new(&[false; 3]);
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
fn seed_limits_stop_at_whichever_comes_first() {
    let minute = Duration::from_secs(60);
    let none = SeedLimits::default();
    assert!(!none.enabled());
    let ratio = SeedLimits {
        ratio: 1.5,
        time: Duration::ZERO,
    };
    assert!(ratio.enabled());
    assert!(!ratio.reached(149, 100, 1000 * minute));
    assert!(ratio.reached(150, 100, Duration::ZERO));
    let time = SeedLimits {
        ratio: 0.0,
        time: minute,
    };
    assert!(time.enabled());
    assert!(!time.reached(u64::MAX, 100, minute / 2));
    assert!(time.reached(0, 100, minute));
    let both = SeedLimits {
        ratio: 2.0,
        time: minute,
    };
    assert!(both.reached(200, 100, Duration::ZERO));
    assert!(both.reached(0, 100, minute));
    assert!(!both.reached(199, 100, minute / 2));
}

#[test]
fn a_seeding_queue_keeps_workers_until_closed() {
    let queue = PieceQueue::new(&[true, true]);
    assert!(queue.is_done() && queue.is_finished());
    let queue = PieceQueue::new(&[true, true]).seeding(true);
    assert!(queue.is_done() && !queue.is_finished());
    queue.close();
    assert!(queue.is_finished());
}

#[test]
fn pieces_on_disk_are_in_the_bitfield_but_never_announced() {
    let queue = PieceQueue::new(&[true, false, true]);
    assert!(queue.has(0) && !queue.has(1) && queue.has(2));
    let bitfield = vec![0b1010_0000];
    let message = Some(PeerMessage::Bitfield { bitfield });
    assert_eq!(queue.have_message(false), (message.clone(), 0));
    assert_eq!(queue.have_message(true), (message, 0));
    assert!(queue.completed_since(0).is_empty());
}

#[test]
fn request_must_lie_inside_the_piece_and_be_one_block_at_most() {
    let request = |begin, length| BlockRequest {
        index: 0,
        begin,
        length,
    };
    let piece_len = 3 * u64::from(BLOCK_SIZE) - 100;
    for (begin, length) in [
        (0, BLOCK_SIZE),
        (BLOCK_SIZE, BLOCK_SIZE),
        // The short last block, and odd but valid ranges.
        (2 * BLOCK_SIZE, BLOCK_SIZE - 100),
        (7, 1),
        (2 * BLOCK_SIZE - 100, BLOCK_SIZE),
    ] {
        assert!(request(begin, length).check(piece_len).is_ok());
    }
    for (begin, length) in [
        (0, 0),
        (0, BLOCK_SIZE + 1),
        (0, 2 * BLOCK_SIZE),
        (2 * BLOCK_SIZE, BLOCK_SIZE),
        (3 * BLOCK_SIZE, 1),
        (u32::MAX, BLOCK_SIZE),
    ] {
        let err = request(begin, length).check(piece_len).unwrap_err();
        assert!(err.is::<Misbehaved>(), "{begin}+{length}: {err:#}");
    }
}

#[test]
fn uploads_are_served_in_order_cancelled_and_capped() {
    let request = |begin| BlockRequest {
        index: 1,
        begin,
        length: BLOCK_SIZE,
    };
    let mut uploads = Uploads::default();
    assert!(uploads.push(request(0)));
    assert!(uploads.push(request(1)));
    assert!(uploads.push(request(2)));
    // Asked twice, sent once.
    assert!(uploads.push(request(0)));
    assert!(uploads.cancel(&request(1)));
    assert!(!uploads.cancel(&request(1)));
    assert!(!uploads.cancel(&request(9)));
    assert_eq!(uploads.pop(), Some(request(0)));
    assert_eq!(uploads.pop(), Some(request(2)));
    assert_eq!(uploads.pop(), None);

    for begin in 0..MAX_QUEUED_UPLOADS as u32 {
        assert!(uploads.push(request(begin)));
    }
    assert!(!uploads.push(request(u32::MAX)));
    assert_eq!(uploads.retain(|_| false).len(), MAX_QUEUED_UPLOADS);
    assert!(uploads.is_empty());
    assert!(uploads.push(request(u32::MAX)));
}

#[test]
fn retained_uploads_keep_their_order_and_dropped_ones_are_returned() {
    let request = |index| BlockRequest {
        index,
        begin: 0,
        length: BLOCK_SIZE,
    };
    let mut uploads = Uploads::default();
    for index in [3, 1, 4, 1, 5, 9, 2, 6] {
        uploads.push(request(index));
    }
    let dropped = uploads.retain(|request| request.index % 2 == 0);
    assert_eq!(dropped, [3, 1, 5, 9].map(request));
    assert_eq!(uploads.pop(), Some(request(4)));
    assert_eq!(uploads.pop(), Some(request(2)));
    assert_eq!(uploads.pop(), Some(request(6)));
    assert!(uploads.is_empty());
}

#[tokio::test]
async fn wait_for_wakes_on_completion() {
    let queue = PieceQueue::new(&[false; 1]);
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
