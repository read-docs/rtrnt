use crate::peer::{self};
use crate::stats::{TorrentState, TorrentStats};
use crate::storage::Storage;
use crate::storage::verify;
use crate::torrent::{self};
use anyhow::{Result, bail};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;
use tracing::info;

use super::progress::bar_label;
use super::{Interrupted, SeedLimits, Session};

/// Verifies a download against its torrent without changing anything on
/// disk, and prints which files are missing, the wrong size or corrupt.
/// Fails unless every piece is intact.
pub async fn check_file(torrent_path: &Path, output_dir: &Path) -> Result<()> {
    let (torrent, info_hash) = torrent::load_file(torrent_path).await?;
    let storage = Arc::new(Storage::open_existing_async(&torrent.info, output_dir).await?);
    let pieces = verify::present(&torrent, &storage)?;
    let stats = TorrentStats::detached(&torrent, info_hash, output_dir);
    let session = Arc::new(Session {
        torrent,
        info_hash,
        peer_id: peer::generate_peer_id(),
        storage,
        seed: SeedLimits::default(),
        dht: None,
        rate_limits: Arc::default(),
        metadata: None,
        bars: MultiProgress::new(),
        resume: None,
        stats,
    });
    // Nothing else is hashing, and nothing stops the check.
    let have = hash_pieces(&session, pieces, &Semaphore::new(1), std::future::pending()).await?;

    let report = verify::Report::new(&session.torrent, &session.storage, &have);
    print!("{report}");
    if !report.is_complete() {
        bail!("{} is incomplete", session.torrent.info.name);
    }
    Ok(())
}

/// Hashes `pieces` on disk, showing progress, and returns which pieces of
/// the torrent are there and intact.
///
/// Only the holder of `hashing`'s one permit hashes at a time, so torrents
/// that share it take turns; the bar says `queued for checking` while this
/// one waits. If `shutdown` resolves while it waits or hashes, the check
/// stops at the next piece and the result is an `Interrupted` error.
pub(crate) async fn hash_pieces(
    session: &Arc<Session>,
    pieces: Vec<usize>,
    hashing: &Semaphore,
    shutdown: impl Future<Output = ()>,
) -> Result<Vec<bool>> {
    let torrent = &session.torrent;
    if pieces.is_empty() {
        return Ok(vec![false; torrent.num_pieces()]);
    }
    let total = pieces
        .iter()
        .map(|&index| torrent.piece_len(index))
        .sum::<Result<u64>>()?;
    let bar = ProgressBar::new(total);
    bar.set_prefix(bar_label(&torrent.info.name));
    if let Ok(style) = ProgressStyle::with_template("{prefix} queued for checking") {
        bar.set_style(style);
    }
    let bar = session.bars.add(bar);
    // Drawn now, not at its first update, which only comes with our turn.
    bar.tick();
    session.stats.set_state(TorrentState::QueuedForChecking);
    let interrupted = || Interrupted {
        remaining: torrent.num_pieces(),
        pieces: torrent.num_pieces(),
    };
    let mut shutdown = std::pin::pin!(shutdown);

    let turn = tokio::select! {
        turn = hashing.acquire() => turn?,
        () = &mut shutdown => {
            bar.finish_and_clear();
            session.bars.remove(&bar);
            bail!(interrupted());
        }
    };
    let style = ProgressStyle::with_template(
        "checking {prefix} {bar:40.cyan/blue} {bytes}/{total_bytes} eta {eta}",
    );
    if let Ok(style) = style {
        bar.set_style(style);
    }
    bar.reset_eta();
    session
        .stats
        .set_state(TorrentState::Checking { done: 0, total });

    let blocking = Arc::clone(session);
    let check_bar = bar.clone();
    let cancel = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::clone(&cancel);
    let mut check = tokio::task::spawn_blocking(move || {
        verify::verify(
            &blocking.torrent,
            &blocking.storage,
            &pieces,
            &cancelled,
            &|len| check_bar.set_position(blocking.stats.checked(len)),
        )
    });
    let result = tokio::select! {
        result = &mut check => result?,
        () = &mut shutdown => {
            cancel.store(true, Ordering::Relaxed);
            // The threads stop at their next piece.
            let _ = check.await;
            Err(interrupted().into())
        }
    };
    drop(turn);
    bar.finish_and_clear();
    session.bars.remove(&bar);
    let have = result?;

    let done = have.iter().filter(|&&have| have).count();
    info!(done, pieces = have.len(), "checked data on disk");
    Ok(have)
}
