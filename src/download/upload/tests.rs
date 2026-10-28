use super::*;

#[test]
fn request_must_lie_inside_the_piece_and_be_one_block_at_most() {
    let request = |begin, length| BlockRef {
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
    let request = |begin| BlockRef {
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
    let request = |index| BlockRef {
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
