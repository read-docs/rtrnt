use super::*;

const DOWN: Slot = Slot::Active {
    seeding: false,
    movable: true,
};
const SEED: Slot = Slot::Active {
    seeding: true,
    movable: true,
};
const WAIT: Slot = Slot::Waiting { seeding: false };
const WAIT_SEED: Slot = Slot::Waiting { seeding: true };

fn limits(downloads: usize, seeds: usize) -> QueueLimits {
    QueueLimits { downloads, seeds }
}

#[test]
fn no_limit_starts_everything() {
    let plan = plan(limits(0, 0), &[WAIT, WAIT_SEED, DOWN, WAIT]);
    assert_eq!(plan.start, [0, 1, 3]);
    assert!(plan.requeue.is_empty());
    assert!(!limits(0, 0).is_limited());
    assert!(limits(0, 1).is_limited());
}

#[test]
fn waiting_downloads_start_in_order_up_to_the_limit() {
    let plan = plan(limits(2, 0), &[Slot::Out, WAIT, DOWN, WAIT, WAIT]);
    assert_eq!(plan.start, [1]);
    let plan = super::plan(limits(2, 0), &[WAIT, WAIT, WAIT]);
    assert_eq!(plan.start, [0, 1]);
}

#[test]
fn downloads_and_seeds_have_slots_of_their_own() {
    let slots = [DOWN, SEED, WAIT, WAIT_SEED, WAIT_SEED];
    let plan = plan(limits(1, 2), &slots);
    assert_eq!(plan.start, [3]);
    assert!(plan.requeue.is_empty());
}

#[test]
fn seeds_over_the_limit_go_back_in_the_queue_latest_first() {
    let plan = plan(limits(0, 1), &[SEED, DOWN, SEED, SEED]);
    assert_eq!(plan.requeue, [2, 3]);
    assert!(plan.start.is_empty());
}

#[test]
fn a_seed_that_is_stopping_keeps_its_slot_until_it_has_stopped() {
    let stopping = Slot::Active {
        seeding: true,
        movable: false,
    };
    let plan = plan(limits(0, 1), &[SEED, stopping, WAIT_SEED]);
    assert_eq!(plan.requeue, [0]);
    assert!(plan.start.is_empty());
}

#[test]
fn a_download_is_never_stopped_for_the_limit() {
    let plan = plan(limits(1, 0), &[DOWN, DOWN, DOWN]);
    assert!(plan.requeue.is_empty());
}
