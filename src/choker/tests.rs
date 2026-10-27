use super::*;
use rand::SeedableRng;
use rand::rngs::StdRng;

fn candidate(key: PeerKey, rate: u64, interested: bool) -> Candidate {
    Candidate {
        key,
        rate,
        interested,
    }
}

#[test]
fn unchokes_the_four_fastest_interested_peers() {
    let mut rng = StdRng::seed_from_u64(1);
    let candidates = [
        candidate(0, 10, true),
        candidate(1, 500, false),
        candidate(2, 40, true),
        candidate(3, 30, true),
        candidate(4, 20, true),
        candidate(5, 50, true),
    ];
    let choice = choose(&candidates, None, true, &mut rng);
    assert_eq!(choice.regular, [5, 2, 3, 4]);
    // The only interested peer left gets the optimistic slot.
    assert_eq!(choice.optimistic, Some(0));
    assert!(!choice.is_unchoked(1), "not interested");
}

#[test]
fn fewer_interested_peers_than_slots_are_all_unchoked() {
    let mut rng = StdRng::seed_from_u64(1);
    let candidates = [candidate(7, 0, true), candidate(8, 0, false)];
    let choice = choose(&candidates, Some(8), false, &mut rng);
    assert_eq!(choice.regular, [7]);
    assert_eq!(choice.optimistic, None);
    assert_eq!(choose(&[], Some(1), true, &mut rng), Choice::default());
}

#[test]
fn equal_rates_favor_the_peer_connected_longest() {
    let mut rng = StdRng::seed_from_u64(1);
    let candidates: Vec<Candidate> = (0..6).map(|key| candidate(key, 0, true)).collect();
    for _ in 0..10 {
        let choice = choose(&candidates, Some(5), false, &mut rng);
        assert_eq!(choice.regular, [0, 1, 2, 3]);
        assert_eq!(choice.optimistic, Some(5));
    }
}

#[test]
fn optimistic_slot_is_kept_until_rotated() {
    let mut rng = StdRng::seed_from_u64(1);
    let candidates: Vec<Candidate> = (0..8).map(|key| candidate(key, 100 - key, true)).collect();
    // Peers 4 to 7 compete for the optimistic slot.
    let first = choose(&candidates, None, true, &mut rng)
        .optimistic
        .unwrap();
    assert!((4..8).contains(&first));
    assert_eq!(
        choose(&candidates, Some(first), false, &mut rng).optimistic,
        Some(first)
    );
    // Rotating always moves the slot on while there is someone else.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..100 {
        let next = choose(&candidates, Some(first), true, &mut rng)
            .optimistic
            .unwrap();
        assert_ne!(next, first);
        seen.insert(next);
    }
    assert_eq!(seen.len(), 3, "picked {seen:?}");
    // With nobody else, the old peer keeps it.
    let pair = [candidates[0], candidates[7]];
    assert_eq!(choose(&pair, Some(7), true, &mut rng).optimistic, None);
    let five = &candidates[..5];
    assert_eq!(choose(five, Some(4), true, &mut rng).optimistic, Some(4));
}

#[test]
fn optimistic_slot_moves_when_its_peer_leaves_loses_interest_or_speeds_up() {
    let mut rng = StdRng::seed_from_u64(1);
    let mut candidates: Vec<Candidate> = (0..6).map(|key| candidate(key, 10, true)).collect();
    // Gone.
    let choice = choose(&candidates, Some(9), false, &mut rng);
    assert!(matches!(choice.optimistic, Some(4 | 5)));
    // No longer interested: the slot goes to the other one left.
    candidates[5].interested = false;
    assert_eq!(
        choose(&candidates, Some(5), false, &mut rng).optimistic,
        Some(4)
    );
    // Fast enough for a regular slot, which pushes peer 3 out.
    candidates[5].interested = true;
    candidates[5].rate = 1000;
    let choice = choose(&candidates, Some(5), false, &mut rng);
    assert_eq!(choice.regular, [5, 0, 1, 2]);
    assert!(matches!(choice.optimistic, Some(3 | 4)));
}

#[test]
fn rates_are_bytes_per_second() {
    let second = Duration::from_secs(1);
    assert_eq!(per_second(1000, second), 1000);
    assert_eq!(per_second(1000, 10 * second), 100);
    assert_eq!(per_second(1000, Duration::ZERO), 0);
    assert_eq!(per_second(u64::MAX, Duration::from_millis(1)), u64::MAX);
}

#[test]
fn deals_slots_by_download_rate_then_by_upload_rate_when_seeding() {
    let choker = Choker::default();
    let mut slots: Vec<ChokerSlot<'_>> = (0..6).map(|_| choker.register()).collect();
    let start = Instant::now();
    choker.measure(start, false);
    for (i, slot) in slots.iter().enumerate() {
        slot.set_interested(true);
        // Peer 5 sends us the most, peer 0 the least, and the other way
        // round for what we send them.
        slot.downloaded(1000 * i as u64);
        slot.uploaded(1000 * (5 - i) as u64);
    }
    let unchoked = |slots: &mut [ChokerSlot<'_>]| -> Vec<usize> {
        (0..slots.len())
            .filter(|&i| slots[i].is_unchoked())
            .collect()
    };

    let optimistic = |choker: &Choker| choker.lock().optimistic.unwrap();

    // Peers 2 to 5 sent us the most; 0 or 1 is unchoked optimistically.
    let round = start + ROUND;
    choker.measure(round, false);
    choker.rechoke(round);
    let first = optimistic(&choker);
    assert!(first < 2);
    let mut expected = vec![first as usize, 2, 3, 4, 5];
    expected.sort();
    assert_eq!(unchoked(&mut slots), expected);

    // Seeding: what we send counts, so peers 0 to 3 win. The optimistic
    // peer earned a regular slot, so 4 or 5 gets the optimistic one.
    for (i, slot) in slots.iter().enumerate() {
        slot.uploaded(1000 * (5 - i) as u64);
    }
    let later = round + ROUND;
    choker.measure(later, true);
    choker.rechoke(later);
    let second = optimistic(&choker);
    assert!(second >= 4);
    let mut expected = vec![0, 1, 2, 3, second as usize];
    expected.sort();
    assert_eq!(unchoked(&mut slots), expected);

    // Kept until its time is up, then rotated to the other one.
    choker.rechoke(later + OPTIMISTIC_PERIOD / 2);
    assert_eq!(optimistic(&choker), second);
    let rotated = later + OPTIMISTIC_PERIOD;
    choker.rechoke(rotated);
    assert_eq!(optimistic(&choker), 9 - second);
    assert!(!slots[second as usize].is_unchoked());

    // A peer that leaves frees its slot for the one left waiting.
    slots.remove(0);
    choker.rechoke(rotated);
    assert!(slots.iter_mut().all(|slot| slot.is_unchoked()));
}
