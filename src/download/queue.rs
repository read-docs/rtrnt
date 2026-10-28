use crate::peer::PeerMessage;
use crate::peer::fast;
use crate::selection::Priority;
use crate::stats::PieceSnapshot;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};
use tokio::sync::Notify;
use tracing::debug;

/// Pieces not yet downloaded, shared between all peer workers.
pub(super) struct PieceQueue {
    state: Mutex<QueueState>,
    /// Signalled when a piece is returned to the queue or completed, and
    /// when the queue is closed.
    pub(super) changed: Notify,
}

pub(super) struct QueueState {
    /// Pieces nobody is downloading.
    pub(super) pending: BTreeSet<usize>,
    /// Pieces being downloaded but not yet completed, with how many workers
    /// hold a claim on each (more than one only in endgame).
    in_flight: BTreeMap<usize, u32>,
    /// Pieces not yet verified and written, whether pending or in flight.
    /// Pieces that are not wanted are never counted.
    remaining: usize,
    /// How much each piece is wanted, `None` for one that is not.
    priorities: Vec<Option<Priority>>,
    /// How many connected peers have each piece.
    availability: Vec<u32>,
    /// Pieces verified and on disk, which peers may download from us.
    have: Vec<bool>,
    /// Pieces completed this session, in order, so that each worker can tell
    /// its peer about the ones it has not announced yet.
    completed: Vec<usize>,
    /// Set when the download stops: no more pieces are handed out, and
    /// workers exit.
    closed: bool,
    /// Whether workers keep serving their peers once every piece is done,
    /// until the queue is closed.
    seed: bool,
}

impl PieceQueue {
    /// A queue of every wanted piece not marked in `have`. `priorities` has
    /// an entry for each piece: `None` for one that is not wanted, which is
    /// never handed out and does not count towards being done.
    pub(super) fn new(have: &[bool], priorities: &[Option<Priority>]) -> Self {
        let wanted = |i: usize| priorities.get(i).is_some_and(Option::is_some);
        let pending: BTreeSet<usize> = (0..have.len()).filter(|&i| !have[i] && wanted(i)).collect();
        Self {
            state: Mutex::new(QueueState {
                remaining: pending.len(),
                priorities: (0..have.len())
                    .map(|i| priorities.get(i).copied().flatten())
                    .collect(),
                pending,
                in_flight: BTreeMap::new(),
                availability: vec![0; have.len()],
                have: have.to_vec(),
                completed: Vec::new(),
                closed: false,
                seed: false,
            }),
            changed: Notify::new(),
        }
    }

    /// A queue in which every piece is wanted, as `Priority::Normal`.
    #[cfg(test)]
    pub(super) fn all_wanted(have: &[bool]) -> Self {
        Self::new(have, &vec![Some(Priority::Normal); have.len()])
    }

    /// Keeps workers going once every piece is done if `seed` is set, so
    /// they can upload to their peers, until the queue is closed.
    pub(super) fn seeding(mut self, seed: bool) -> Self {
        self.state
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .seed = seed;
        self
    }

    pub(super) fn lock(&self) -> MutexGuard<'_, QueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts counting a peer's pieces towards availability. Its pieces are
    /// uncounted again when the returned handle is dropped.
    pub(super) fn register_peer(&self) -> PeerPieces<'_> {
        PeerPieces {
            queue: self,
            has: vec![false; self.lock().availability.len()],
        }
    }

    /// Takes the pending piece the peer has that is wanted most, and of
    /// those the rarest, breaking ties at random.
    ///
    /// Endgame: once no piece is pending, every remaining piece is already
    /// being downloaded, so this joins the in-flight piece with the fewest
    /// downloaders instead. Whichever copy finishes first wins, so one slow
    /// peer can no longer hold up the end of the download.
    ///
    /// The piece goes back to the queue when its last claim is dropped
    /// without `complete()`. Pieces in `held`, which the caller already
    /// claimed, are never duplicated. A closed queue hands out nothing.
    pub(super) fn claim(&self, peer: &PeerPieces<'_>, held: &[usize]) -> Option<PieceClaim<'_>> {
        self.claim_from(&peer.has, held)
    }

    /// Like `claim`, but only takes a piece marked in `allowed`: one the
    /// peer lets us download while it chokes us (fast extension).
    pub(super) fn claim_allowed(
        &self,
        peer: &PeerPieces<'_>,
        held: &[usize],
        allowed: &[bool],
    ) -> Option<PieceClaim<'_>> {
        let has: Vec<bool> = peer
            .has
            .iter()
            .zip(allowed)
            .map(|(&has, &allowed)| has && allowed)
            .collect();
        self.claim_from(&has, held)
    }

    /// `claim` for a peer that has the pieces marked in `has`.
    pub(super) fn claim_from(&self, has: &[bool], held: &[usize]) -> Option<PieceClaim<'_>> {
        let mut guard = self.lock();
        let state = &mut *guard;
        if state.closed {
            return None;
        }
        let mut rng = rand::rng();
        let index = if state.pending.is_empty() {
            let candidates = state
                .in_flight
                .iter()
                .filter(|(i, _)| !held.contains(i))
                .map(|(&i, &holders)| (i, holders));
            let index = pick_least(candidates, has, &mut rng)?;
            let holders = state.in_flight.entry(index).or_insert(0);
            *holders += 1;
            debug!(index, holders = *holders, "endgame: duplicating piece");
            index
        } else {
            let availability = &state.availability;
            let priorities = &state.priorities;
            let candidates = state.pending.iter().map(|&i| {
                let priority = priorities.get(i).copied().flatten();
                (
                    i,
                    (Reverse(priority), availability.get(i).copied().unwrap_or(0)),
                )
            });
            let index = pick_least(candidates, has, &mut rng)?;
            state.pending.remove(&index);
            state.in_flight.insert(index, 1);
            index
        };
        Some(PieceClaim {
            queue: self,
            index,
            completed: false,
        })
    }

    /// Whether the peer has a wanted piece that is not yet completed,
    /// whether pending or in flight.
    pub(super) fn wants(&self, peer: &PeerPieces<'_>) -> bool {
        let state = self.lock();
        let has = |i: &usize| peer.has.get(*i).copied().unwrap_or(false);
        state.pending.iter().any(has) || state.in_flight.keys().any(has)
    }

    /// Resolves once `done` returns true, checking again whenever a piece is
    /// returned or completed.
    pub(super) async fn wait_for(&self, mut done: impl FnMut() -> bool) {
        loop {
            let mut changed = std::pin::pin!(self.changed.notified());
            // Register before checking, so a change in between still wakes
            // us.
            changed.as_mut().enable();
            if done() {
                return;
            }
            changed.await;
        }
    }

    /// Whether piece `index` is verified and on disk.
    pub(super) fn has(&self, index: usize) -> bool {
        self.lock().have.get(index).copied().unwrap_or(false)
    }

    /// The message that tells a peer which pieces we have (see
    /// `fast::have_message`), if any, and how many completions it covers:
    /// pass that to `completed_since` for the pieces completed after it was
    /// taken.
    pub(super) fn have_message(&self, fast: bool) -> (Option<PeerMessage>, usize) {
        let state = self.lock();
        let message = fast::have_message(&state.have, fast);
        (message, state.completed.len())
    }

    /// A Have for each piece we have, for a peer told earlier that we had
    /// none (one the metadata of a magnet link came from), and how many
    /// completions they cover, as for `have_message`.
    pub(super) fn have_messages(&self) -> (Vec<PeerMessage>, usize) {
        let state = self.lock();
        let haves = (0..state.have.len())
            .filter(|&index| state.have[index])
            .filter_map(|index| u32::try_from(index).ok())
            .map(|piece_index| PeerMessage::Have { piece_index })
            .collect();
        (haves, state.completed.len())
    }

    /// Pieces completed after the first `seen` completions of the session.
    pub(super) fn completed_since(&self, seen: usize) -> Vec<usize> {
        self.lock()
            .completed
            .get(seen..)
            .map(<[usize]>::to_vec)
            .unwrap_or_default()
    }

    /// Pieces completed this session.
    pub(super) fn completed_count(&self) -> usize {
        self.lock().completed.len()
    }

    /// Which pieces we have, which are being downloaded, and how many
    /// connected peers have each.
    pub(super) fn snapshot(&self) -> PieceSnapshot {
        let state = self.lock();
        let mut in_flight = vec![false; state.have.len()];
        for &index in state.in_flight.keys() {
            in_flight[index] = true;
        }
        PieceSnapshot {
            have: state.have.clone(),
            in_flight,
            availability: state.availability.clone(),
        }
    }

    /// Which pieces are verified and on disk.
    pub(super) fn have_bitfield(&self) -> Vec<bool> {
        self.lock().have.clone()
    }

    /// Wanted pieces not yet verified and written.
    pub(super) fn remaining(&self) -> usize {
        self.lock().remaining
    }

    /// How many pieces are wanted at all.
    pub(super) fn wanted(&self) -> usize {
        self.lock().priorities.iter().flatten().count()
    }

    pub(super) fn is_done(&self) -> bool {
        self.remaining() == 0
    }

    /// Stops handing out pieces and wakes every worker so it can exit.
    pub(super) fn close(&self) {
        self.lock().closed = true;
        self.changed.notify_waiters();
    }

    pub(super) fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Whether workers should exit: the queue is closed, or every piece is
    /// done and we are not seeding.
    pub(super) fn is_finished(&self) -> bool {
        let state = self.lock();
        state.closed || (state.remaining == 0 && !state.seed)
    }
}

pub(super) struct PieceClaim<'a> {
    queue: &'a PieceQueue,
    pub(super) index: usize,
    completed: bool,
}

impl PieceClaim<'_> {
    /// Marks the piece verified and written. Returns false if another worker
    /// completed it first.
    pub(super) fn complete(mut self) -> bool {
        self.completed = true;
        let first = {
            let mut state = self.queue.lock();
            let first = state.in_flight.remove(&self.index).is_some();
            if first {
                state.remaining -= 1;
                state.have[self.index] = true;
                state.completed.push(self.index);
            }
            first
        };
        self.queue.changed.notify_waiters();
        first
    }

    /// Whether the piece has been completed (by any worker).
    pub(super) fn is_completed(&self) -> bool {
        // While a claim exists its piece is never pending, so it is either
        // in flight or done.
        !self.queue.lock().in_flight.contains_key(&self.index)
    }
}

impl Drop for PieceClaim<'_> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        {
            let mut state = self.queue.lock();
            // No entry means another worker already completed the piece.
            if let Some(holders) = state.in_flight.get_mut(&self.index) {
                *holders -= 1;
                if *holders == 0 {
                    state.in_flight.remove(&self.index);
                    state.pending.insert(self.index);
                }
            }
        }
        self.queue.changed.notify_waiters();
    }
}

/// One peer's pieces as currently counted in the queue's availability.
pub(super) struct PeerPieces<'a> {
    queue: &'a PieceQueue,
    has: Vec<bool>,
}

impl PeerPieces<'_> {
    /// Brings the counted pieces in line with what the peer has advertised.
    pub(super) fn update(&mut self, peer_has: &[bool]) {
        let changed: Vec<usize> = self
            .has
            .iter()
            .zip(peer_has)
            .enumerate()
            .filter(|(_, (counted, has))| counted != has)
            .map(|(i, _)| i)
            .collect();
        if changed.is_empty() {
            return;
        }
        let mut state = self.queue.lock();
        for i in changed {
            let has = peer_has[i];
            let count = &mut state.availability[i];
            *count = if has {
                count.saturating_add(1)
            } else {
                count.saturating_sub(1)
            };
            self.has[i] = has;
        }
    }
}

impl Drop for PeerPieces<'_> {
    fn drop(&mut self) {
        let mut state = self.queue.lock();
        for (count, _) in state
            .availability
            .iter_mut()
            .zip(&self.has)
            .filter(|(_, has)| **has)
        {
            *count = count.saturating_sub(1);
        }
    }
}

/// Picks the `(piece, key)` candidate in `has` with the lowest key. Ties
/// are broken uniformly at random (reservoir sampling) so that peers spread
/// out over equally good pieces instead of all fetching the lowest index.
fn pick_least<K: Ord + Copy>(
    candidates: impl IntoIterator<Item = (usize, K)>,
    has: &[bool],
    rng: &mut impl Rng,
) -> Option<usize> {
    let mut best = None;
    let mut ties = 0u32;
    for (i, count) in candidates {
        if !has.get(i).copied().unwrap_or(false) {
            continue;
        }
        match best {
            Some((_, best_count)) if count > best_count => {}
            Some((_, best_count)) if count == best_count => {
                ties += 1;
                if rng.random_range(0..ties) == 0 {
                    best = Some((i, count));
                }
            }
            _ => {
                best = Some((i, count));
                ties = 1;
            }
        }
    }
    best.map(|(i, _)| i)
}

#[cfg(test)]
mod tests;
