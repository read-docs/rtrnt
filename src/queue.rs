//! The queue: which torrents of a session run, given how many may download
//! and how many may seed at once.
//!
//! This is the decision only. The manager describes its torrents, in the
//! queue's order, as [`Slot`]s, and [`plan`] says which waiting ones to
//! start and which seeds to put back in the queue.

/// How many torrents run at once. 0 for a count means no limit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueLimits {
    /// Most torrents that are not yet complete (checking, fetching metadata
    /// or downloading) running at once.
    pub downloads: usize,
    /// Most complete torrents seeding at once.
    pub seeds: usize,
}

impl QueueLimits {
    /// Whether any limit is set.
    pub fn is_limited(&self) -> bool {
        self.downloads > 0 || self.seeds > 0
    }

    /// The limit for downloads, or seeds if `seeding`.
    fn of(&self, seeding: bool) -> usize {
        if seeding { self.seeds } else { self.downloads }
    }
}

/// What one torrent is doing, for [`plan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// Not in the queue: paused, ended or being removed.
    Out,
    /// Running, and holding a slot of the kind `seeding` says. `movable` is
    /// whether it can be stopped to give the slot up; one already stopping
    /// cannot, and holds its slot until it has.
    Active { seeding: bool, movable: bool },
    /// Waiting for a slot of the kind `seeding` says: a torrent that has
    /// been complete, as far as is known, waits for a seed slot, and any
    /// other for a download slot.
    Waiting { seeding: bool },
}

/// What to do about the torrents, as indexes into the slots given.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Seeds over the limit to stop and put back in the queue: the later
    /// ones in the order.
    pub requeue: Vec<usize>,
    /// Waiting torrents to start: the earlier ones in the order, as far as
    /// the slots go.
    pub start: Vec<usize>,
}

/// Decides what to start and what to stop. `slots` are in the queue's order,
/// which decides who goes first: a seed over the limit that is later in it
/// than the others is the one stopped, and a waiting torrent earlier in it
/// than another of its kind is the one started.
pub fn plan(limits: QueueLimits, slots: &[Slot]) -> Plan {
    let mut plan = Plan::default();
    let mut used = [0usize; 2];
    // Those that cannot move hold their slots before any other.
    for slot in slots {
        if let Slot::Active {
            seeding,
            movable: false,
        } = slot
        {
            used[usize::from(*seeding)] += 1;
        }
    }
    for (index, slot) in slots.iter().enumerate() {
        if let Slot::Active {
            seeding,
            movable: true,
        } = slot
        {
            let limit = limits.of(*seeding);
            if *seeding && limit > 0 && used[1] >= limit {
                plan.requeue.push(index);
            } else {
                used[usize::from(*seeding)] += 1;
            }
        }
    }
    for (index, slot) in slots.iter().enumerate() {
        if let Slot::Waiting { seeding } = slot {
            let limit = limits.of(*seeding);
            if limit == 0 || used[usize::from(*seeding)] < limit {
                used[usize::from(*seeding)] += 1;
                plan.start.push(index);
            }
        }
    }
    plan
}

#[cfg(test)]
mod tests;
