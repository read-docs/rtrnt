//! Web seeds as a source of pieces next to the peers: each takes whole
//! pieces from the queue, fetches them over HTTP, and has them verified
//! like any other piece.

use super::Session;
use super::queue::PieceQueue;
use super::swarm::{Swarm, sleep_until_some};
use crate::rate_limit::Allowance;
use crate::stats::{WebSeedState, WebSeedStats};
use crate::webseed::{WebSeed, fetch_piece};
use anyhow::{Result, anyhow};
use futures_util::future::join_all;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, info, warn};

/// Pieces fetched from one web seed at once.
const LANES: usize = 2;
/// How many failures in a row make us give up on a web seed.
const MAX_FAILURES: u32 = 6;
/// The wait after the first failure; it doubles with each one after.
const BACKOFF_BASE: Duration = Duration::from_secs(5);
/// The longest wait after a failure.
const BACKOFF_MAX: Duration = Duration::from_secs(300);
/// How often an idle lane re-checks the queue.
const IDLE_POLL: Duration = Duration::from_secs(2);

/// How long to wait before trying a web seed again after `failures` failures
/// in a row.
pub(super) fn backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    BACKOFF_BASE.saturating_mul(1 << doublings).min(BACKOFF_MAX)
}

/// What the lanes of one web seed share.
#[derive(Default)]
struct Shared {
    /// Failures since a piece last came through.
    failures: AtomicU32,
    /// Lanes fetching a piece now.
    active: AtomicU32,
}

/// Downloads from `seed` until the torrent is complete or the queue is
/// closed, or until the web seed has failed too often. Fails only if the
/// disk does.
pub(super) async fn web_seed_worker(
    session: &Session,
    swarm: &Swarm,
    seed: WebSeed,
    stats: Arc<WebSeedStats>,
) -> Result<()> {
    let shared = Shared::default();
    let lanes = (0..LANES).map(|_| lane(session, swarm, &seed, &stats, &shared));
    let results = join_all(lanes).await;
    stats.set_state(if stats.state() == WebSeedState::GaveUp {
        WebSeedState::GaveUp
    } else {
        WebSeedState::Idle
    });
    results.into_iter().collect()
}

/// One piece at a time from `seed`.
async fn lane(
    session: &Session,
    swarm: &Swarm,
    seed: &WebSeed,
    stats: &WebSeedStats,
    shared: &Shared,
) -> Result<()> {
    let Swarm {
        queue, progress, ..
    } = swarm;
    let torrent = &session.torrent;
    // A web seed has every piece.
    let everything = vec![true; torrent.num_pieces()];
    // The torrent's own download limit comes first, inside the session's,
    // as for its peers.
    let own = &session.torrent_limits.rates().download;
    let mut budget = own.allowance_within(&session.rate_limits.download);
    let stopped = || queue.is_closed() || queue.is_done();
    loop {
        if stopped() || stats.state() == WebSeedState::GaveUp {
            return Ok(());
        }
        // Registered before the queue is looked at, so a piece returned
        // meanwhile still wakes the wait below.
        let mut changed = std::pin::pin!(queue.changed.notified());
        changed.as_mut().enable();
        let Some(claim) = claim_pending(queue, &everything) else {
            tokio::select! {
                () = &mut changed => {}
                () = tokio::time::sleep(IDLE_POLL) => {}
            }
            continue;
        };
        let index = claim.index;
        let len = torrent.piece_len(index)?;
        if !wait_for_turn(&mut budget, session, queue, len).await {
            continue;
        }
        shared.active.fetch_add(1, Ordering::Relaxed);
        stats.set_state(WebSeedState::Downloading);
        let fetch = fetch_piece(seed, torrent, &session.info_hash, index, |bytes| {
            stats.add_downloaded(bytes);
            session.stats.add_received(bytes);
        });
        let fetched = tokio::select! {
            fetched = fetch => Some(fetched),
            () = queue.wait_for(|| stopped() || claim.is_completed()) => None,
        };
        if shared.active.fetch_sub(1, Ordering::Relaxed) == 1
            && stats.state() == WebSeedState::Downloading
        {
            stats.set_state(WebSeedState::Idle);
        }
        let Some(fetched) = fetched else {
            continue;
        };
        let error = match fetched {
            Ok(data) => {
                let hash = torrent.piece_hash(index)?;
                if session.storage.verify_and_write(index, data, hash).await? {
                    if claim.complete() {
                        progress.add(len);
                        debug!(url = %seed.url, index, "piece complete");
                    }
                    shared.failures.store(0, Ordering::Relaxed);
                    stats.set_failures(0);
                    continue;
                }
                anyhow!("piece {index} failed its hash check")
            }
            Err(e) => e,
        };
        // Back to the queue for someone else, or this seed's next try.
        drop(claim);
        let failures = shared.failures.fetch_add(1, Ordering::Relaxed) + 1;
        stats.set_failures(failures);
        if failures >= MAX_FAILURES {
            warn!(url = %seed.url, failures, "giving up on web seed: {error:#}");
            stats.set_state(WebSeedState::GaveUp);
            return Ok(());
        }
        let delay = backoff(failures);
        info!(url = %seed.url, failures, ?delay, "web seed failed: {error:#}");
        stats.set_state(WebSeedState::Backoff);
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = queue.wait_for(stopped) => {}
        }
        if stats.state() == WebSeedState::Backoff {
            stats.set_state(WebSeedState::Idle);
        }
    }
}

/// Takes a pending piece for a web seed. Unlike a peer it never joins a
/// piece already in flight in the endgame: that would only cost the server
/// the same bytes twice.
fn claim_pending<'a>(
    queue: &'a PieceQueue,
    everything: &[bool],
) -> Option<super::queue::PieceClaim<'a>> {
    if queue.lock().pending.is_empty() {
        return None;
    }
    queue.claim_from(everything, &[])
}

/// Waits for the torrent's and the session's download limits to allow
/// `bytes`, which are then taken from both. Returns false if the download
/// stops first.
async fn wait_for_turn(
    budget: &mut Allowance<'_>,
    session: &Session,
    queue: &PieceQueue,
    bytes: u64,
) -> bool {
    let own = &session.torrent_limits.rates().download;
    let outer = &session.rate_limits.download;
    loop {
        // Made before the try, so that a change of either limit in between
        // still wakes us (a change notifies every waiter made before it).
        let mut changed = std::pin::pin!(own.changed_with(outer));
        if budget.try_take(bytes, Instant::now()) {
            return true;
        }
        tokio::select! {
            () = sleep_until_some(budget.ready_at()) => {}
            () = &mut changed => {}
            () = queue.wait_for(|| queue.is_closed() || queue.is_done()) => {
                budget.release();
                return false;
            }
        }
    }
}

#[cfg(test)]
mod tests;
