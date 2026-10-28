use super::*;
use crate::peer::BlockRef;
use sha1::Digest;

fn peer<'a>(queue: &'a PieceQueue, has: &[bool]) -> PeerPieces<'a> {
    let mut peer = queue.register_peer();
    peer.update(has);
    peer
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
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    let mut lengths = Vec::new();
    while piece.has_unrequested() {
        let PeerMessage::Request(BlockRef { begin, length, .. }) =
            piece.request_next(Instant::now())
        else {
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
    let (claim, assembled, hash) = piece.into_parts();
    assert_eq!(assembled, data);
    assert_eq!(hash, <[u8; 20]>::from(sha1::Sha1::digest(&data)));
    assert!(claim.complete());
}

#[test]
fn active_piece_ignores_duplicate_and_unrequested_blocks() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    piece.request_next(Instant::now());
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
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    while piece.has_unrequested() {
        piece.request_next(Instant::now());
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
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    piece.request_next(Instant::now());
    piece.request_next(Instant::now());
    piece.add_block(0, block(&data, 0)).unwrap();
    let cancels: Vec<PeerMessage> = piece.outstanding().map(BlockRef::cancel).collect();
    assert_eq!(
        cancels,
        [PeerMessage::Cancel(BlockRef {
            index: 0,
            begin: BLOCK_SIZE,
            length: BLOCK_SIZE,
        })]
    );
}

#[test]
fn rejected_blocks_are_requested_again_first() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    let request = |block: u32| {
        PeerMessage::Request(BlockRef {
            index: 0,
            begin: block * BLOCK_SIZE,
            length: BLOCK_SIZE.min(data.len() as u32 - block * BLOCK_SIZE),
        })
    };
    assert_eq!(piece.request_next(Instant::now()), request(0));
    assert_eq!(piece.request_next(Instant::now()), request(1));
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
    assert_eq!(piece.request_next(Instant::now()), request(0));
    assert_eq!(piece.request_next(Instant::now()), request(2));
    assert!(!piece.has_unrequested());
    let outstanding: Vec<BlockRef> = piece.outstanding().collect();
    assert_eq!(outstanding.len(), 3);

    assert!(piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
    assert!(!piece.reject(BLOCK_SIZE, BLOCK_SIZE), "already received");
    assert!(piece.reject(2 * BLOCK_SIZE, BLOCK_SIZE / 2));
    assert_eq!(piece.request_next(Instant::now()), request(2));
    for b in [0, 2] {
        assert!(
            piece
                .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
                .unwrap()
        );
    }
    assert!(piece.is_complete());
    assert_eq!(piece.in_flight(), 0);
}

#[test]
fn unanswered_requests_expire_and_are_asked_for_again() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    let timeout = Duration::from_secs(60);
    let start = Instant::now();
    let at = |secs| start + Duration::from_secs(secs);
    let request = |block: u32, length: u32| BlockRef {
        index: 0,
        begin: block * BLOCK_SIZE,
        length,
    };
    for secs in [0, 30, 40] {
        piece.request_next(at(secs));
    }
    // The second block is answered in time, and so never expires.
    assert!(piece.add_block(BLOCK_SIZE, block(&data, 1)).unwrap());
    assert_eq!(piece.in_flight(), 2);

    assert!(piece.expire(at(59), timeout).is_empty());
    assert_eq!(piece.in_flight(), 2);
    assert!(!piece.has_unrequested());

    // Only the first request is old enough.
    assert_eq!(piece.expire(at(60), timeout), [request(0, BLOCK_SIZE)]);
    assert_eq!(piece.in_flight(), 1);
    assert!(piece.has_unrequested());
    // Nothing expires twice, and the block waits for a new request.
    assert!(piece.expire(at(60), timeout).is_empty());
    assert_eq!(
        piece.request_next(at(61)),
        PeerMessage::Request(BlockRef {
            index: 0,
            begin: 0,
            length: BLOCK_SIZE,
        })
    );

    // The third block expires on its own clock, the first one's new request
    // not yet.
    assert_eq!(piece.expire(at(100), timeout), [request(2, BLOCK_SIZE / 2)]);
    assert_eq!(piece.in_flight(), 1);
    assert_eq!(piece.expire(at(121), timeout), [request(0, BLOCK_SIZE)]);
    assert_eq!(piece.in_flight(), 0);
    // A late answer to an expired request is ignored, not an error.
    assert!(!piece.add_block(0, block(&data, 0)).unwrap());
}

#[test]
fn a_choke_releases_pieces_with_nothing_outstanding_unless_allowed_fast() {
    let queue = PieceQueue::all_wanted(&[false; 3]);
    let seed = peer(&queue, &[true; 3]);
    let piece = |index: usize| {
        let claim = queue
            .claim_from(&[0, 1, 2].map(|i| i == index), &[])
            .unwrap();
        ActivePiece::new(claim, u64::from(2 * BLOCK_SIZE), [0; 20]).unwrap()
    };
    let (mut waiting, idle, allowed) = (piece(0), piece(1), piece(2));
    waiting.request_next(Instant::now());
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
fn a_complete_piece_is_returned_when_its_claim_is_dropped() {
    let queue = PieceQueue::all_wanted(&[false; 1]);
    let seed = peer(&queue, &[true]);
    let (data, mut piece) = active(&queue, &seed);
    for b in 0..3 {
        piece.request_next(Instant::now());
        piece
            .add_block(b as u32 * BLOCK_SIZE, block(&data, b))
            .unwrap();
    }
    let (claim, _, _) = piece.into_parts();
    drop(claim);
    assert!(queue.lock().pending.contains(&0));
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
