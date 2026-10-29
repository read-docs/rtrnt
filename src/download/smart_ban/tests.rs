use super::*;

fn sender(port: u16) -> Sender {
    Sender {
        addr: SocketAddr::from(([10, 0, 0, 1], port)),
        inbound: false,
    }
}

/// A piece of one block per byte of `fill`, each block filled with its byte.
fn piece(fill: &[u8]) -> Vec<u8> {
    fill.iter()
        .flat_map(|&byte| vec![byte; BLOCK_SIZE as usize])
        .collect()
}

#[test]
fn block_hashes_cover_a_short_last_block() {
    let mut data = piece(&[1, 2]);
    data.push(3);
    let hashes = block_hashes(&data);
    assert_eq!(hashes.len(), 3);
    assert_ne!(hashes[0], hashes[1]);
    assert_eq!(hashes[2], <[u8; 20]>::from(Sha1::digest([3])));
}

#[test]
fn a_whole_bad_copy_blames_its_sender() {
    let mut suspects = Suspects::default();
    suspects.record(4, Attempt::whole(&piece(&[1, 9]), sender(1)));
    assert!(suspects.watching(4));
    let guilty = suspects.resolve(4, &block_hashes(&piece(&[1, 2])));
    assert_eq!(guilty, vec![sender(1)]);
    assert!(!suspects.watching(4));
}

#[test]
fn only_the_senders_of_differing_blocks_are_blamed() {
    let hashes = block_hashes(&piece(&[1, 2, 3]));
    let attempt = Attempt::assembled([
        (hashes[0], sender(1)),
        ([0; 20], sender(2)),
        (hashes[2], sender(3)),
    ]);
    let mut suspects = Suspects::default();
    suspects.record(0, attempt);
    assert_eq!(suspects.resolve(0, &hashes), vec![sender(2)]);
}

#[test]
fn a_sender_of_several_bad_blocks_is_blamed_once() {
    let hashes = block_hashes(&piece(&[1, 2]));
    let attempt = Attempt::assembled([([0; 20], sender(1)), ([1; 20], sender(1))]);
    let mut suspects = Suspects::default();
    suspects.record(0, attempt);
    assert_eq!(suspects.resolve(0, &hashes), vec![sender(1)]);
}

#[test]
fn senders_of_every_failed_copy_are_blamed() {
    let mut suspects = Suspects::default();
    suspects.record(0, Attempt::whole(&piece(&[1, 7]), sender(1)));
    suspects.record(0, Attempt::whole(&piece(&[8, 2]), sender(2)));
    let mut guilty = suspects.resolve(0, &block_hashes(&piece(&[1, 2])));
    guilty.sort_by_key(|s| s.addr.port());
    assert_eq!(guilty, vec![sender(1), sender(2)]);
}

#[test]
fn a_copy_equal_to_the_good_one_blames_nobody() {
    let good = piece(&[1, 2]);
    let mut suspects = Suspects::default();
    suspects.record(0, Attempt::whole(&good, sender(1)));
    assert!(suspects.resolve(0, &block_hashes(&good)).is_empty());
}

#[test]
fn a_piece_nobody_failed_blames_nobody() {
    let mut suspects = Suspects::default();
    assert!(suspects.resolve(0, &block_hashes(&piece(&[1]))).is_empty());
}

#[test]
fn blocks_are_compared_one_to_one_and_extra_ones_differ() {
    let hashes = block_hashes(&piece(&[1, 2]));
    let mut suspects = Suspects::default();
    // Fewer blocks than the good copy: the ones present decide.
    suspects.record(0, Attempt::assembled([(hashes[0], sender(1))]));
    assert!(suspects.resolve(0, &hashes).is_empty());
    // More blocks than the good copy: the extra one differs.
    suspects.record(
        0,
        Attempt::assembled([
            (hashes[0], sender(1)),
            (hashes[1], sender(1)),
            (hashes[1], sender(2)),
        ]),
    );
    assert_eq!(suspects.resolve(0, &hashes), vec![sender(2)]);
}

#[test]
fn memory_is_bounded_in_pieces_and_attempts() {
    let mut suspects = Suspects::default();
    for index in 0..MAX_PIECES + 10 {
        suspects.record(index, Attempt::whole(&piece(&[1]), sender(1)));
    }
    assert!(!suspects.watching(0));
    assert!(suspects.watching(MAX_PIECES + 9));
    assert_eq!(suspects.pieces.len(), MAX_PIECES);
    assert_eq!(suspects.order.len(), MAX_PIECES);

    let mut suspects = Suspects::default();
    for port in 0..10 {
        suspects.record(0, Attempt::whole(&piece(&[1]), sender(port)));
    }
    assert_eq!(suspects.pieces[&0].len(), MAX_ATTEMPTS);
    // The oldest copies are the ones forgotten.
    let guilty = suspects.resolve(0, &block_hashes(&piece(&[2])));
    assert_eq!(guilty, vec![sender(7), sender(8), sender(9)]);
}

#[test]
fn verified_reports_guilty_senders_and_marks_them_banned() {
    let (smart, mut verdicts) = SmartBan::new();
    smart.failed(3, &piece(&[1, 9]), sender(5));
    assert!(smart.watching(3));
    assert!(!smart.is_banned(sender(5).addr));
    smart.verified(3, &piece(&[1, 2]));
    assert!(smart.is_banned(sender(5).addr));
    assert_eq!(verdicts.try_recv().unwrap(), sender(5));
    assert!(!smart.watching(3));
    // Nothing is left to blame the next time the piece is verified.
    smart.verified(3, &piece(&[1, 2]));
    assert!(verdicts.try_recv().is_err());
}
