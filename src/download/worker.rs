use crate::magnet::metadata;
use crate::peer::extension::{self, Advertised};
use crate::peer::fast;
use crate::peer::pex::{self};
use crate::peer::{PeerConnection, PeerMessage};
use crate::stats::{
    PeerFlags, PeerStats,
};
use anyhow::{Result, anyhow, bail};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, warn};

use super::queue::PieceQueue;
use super::requests::{ActivePiece, Pipeline, drop_completed, release_choked, request_blocks};
use super::swarm::{Link, Swarm, sleep_until_some};
use super::upload::{BlockRequest, Uploads, serve_block};
use super::{Misbehaved, Session};

/// Peers that send this many corrupt pieces are dropped.
const MAX_HASH_FAILURES: u32 = 3;
/// A peer that has no piece we need for this long is dropped.
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
/// A peer that keeps us choked this long while we are interested is dropped.
const UNCHOKE_TIMEOUT: Duration = Duration::from_secs(60);
/// How often an idle worker re-checks the queue.
const IDLE_POLL: Duration = Duration::from_secs(2);

/// Messages a worker handles from a peer that keeps sending, before it sends
/// the next block that peer asked for.
const MAX_MESSAGES_PER_BLOCK: usize = 16;

/// Downloads from one peer, and uploads to it while the choker unchokes it,
/// until the torrent is complete, the queue is closed or the peer fails.
/// Reaches the peer at `addr` over `link`. `listen_port` is where we accept
/// peers, if anywhere. `pieces_done` counts the pieces this peer delivered
/// first, even on error.
pub(super) async fn peer_worker(
    session: &Session,
    swarm: &Swarm,
    addr: SocketAddr,
    link: Link,
    listen_port: Option<u16>,
    pieces_done: &mut usize,
) -> Result<()> {
    let Swarm {
        queue,
        progress,
        connected,
        choker,
        pex,
    } = swarm;
    let torrent = &session.torrent;
    let (info_hash, peer_id, num_pieces) =
        (session.info_hash, session.peer_id, torrent.num_pieces());
    let inbound = link.is_inbound();
    // Set for a connection the metadata of a magnet link came over, on
    // which we have already said we had no piece.
    let reopened = matches!(link, Link::Open { .. });
    let advertised = Advertised {
        listen_port,
        metadata_size: session
            .metadata
            .as_ref()
            .and_then(|metadata| u32::try_from(metadata.len()).ok()),
        private: torrent.is_private(),
    };
    let mut conn = match link {
        Link::Accepted(incoming) => {
            PeerConnection::answer(incoming, peer_id, num_pieces, advertised).await?
        }
        Link::Connect => {
            PeerConnection::connect(addr, info_hash, peer_id, num_pieces, advertised).await?
        }
        Link::Open { conn, .. } => *conn,
    };
    if conn.peer_id == peer_id {
        // A tracker listed our own address.
        bail!(Misbehaved("connected to ourselves"));
    }
    let Some(_connected) = connected.register(conn.peer_id) else {
        bail!("already connected to this peer");
    };
    // Whether both sides support the fast extension (BEP 6): then every
    // request gets a Piece or a Reject both ways, and a choke no longer
    // drops requests.
    let fast = conn.supports_fast();
    debug!(%addr, inbound, fast, "connected");
    let stats = Arc::new(PeerStats::new(addr, inbound));
    session.stats.add_peer(&stats);
    let mut pex = pex.join(conn.peer_id, addr, !inbound);

    // Our pieces, first thing after the handshakes (see
    // `fast::have_message`), or one Have each on a connection that heard we
    // had none. Pieces completed from now on are announced with Have.
    let (have, mut announced) = if reopened {
        queue.have_messages()
    } else {
        let (have, announced) = queue.have_message(fast);
        (have.into_iter().collect(), announced)
    };
    if !have.is_empty() {
        conn.send_all(&have).await?;
    }
    // BEP 5: tell a peer that runs a DHT node where ours listens.
    if let Some(dht) = &session.dht
        && conn.supports_dht()
    {
        conn.send(&PeerMessage::Port { port: dht.port() }).await?;
    }

    let mut pieces = queue.register_peer();
    let mut slot = choker.register();
    // Pieces we hold a claim on, with requests spread over all of them so the
    // pipeline does not drain at every piece boundary.
    let mut active: Vec<ActivePiece<'_>> = Vec::new();
    let mut pipeline = Pipeline::new(Instant::now());
    let mut hash_failures = 0;
    let mut last_useful = Instant::now();
    // Set while we are interested but choked.
    let mut unchoke_deadline = None;
    let mut has_changed = true;
    // Blocks the peer asked us for, sent one per turn of the loop.
    let mut uploads = Uploads::default();
    // Messages handled since the last block we sent, and whether the last
    // look for one found nothing waiting.
    let mut messages_since_block = 0;
    let mut drained = false;
    // This peer's turns at the session's rate limits: for the blocks we ask
    // it for, and for the blocks it asks us for.
    let mut download_budget = session.rate_limits.download.allowance();
    let mut upload_budget = session.rate_limits.upload.allowance();
    // Fast extension: the allowed fast set we gave the peer, decided when it
    // first tells us its pieces (`None` until then, and empty unless it was
    // just starting out). Its requests for these pieces are served even
    // while we choke it.
    // A connection the metadata of a magnet link came over has had its
    // say on its pieces already.
    let mut offered: Option<Vec<u32>> = if reopened && fast {
        Some(offer_allowed_fast(&mut conn, &info_hash).await?)
    } else {
        None
    };
    // Fast extension: requests we cancelled that the peer has not answered
    // yet. It still answers each, with the block or a Reject.
    let mut cancelled: HashSet<BlockRequest> = HashSet::new();
    loop {
        // Registered before anything is checked, so that a change in the
        // queue from here on still wakes the idle wait below.
        let mut changed = std::pin::pin!(queue.changed.notified());
        changed.as_mut().enable();

        drop_completed(&mut conn, &mut active, fast.then_some(&mut cancelled)).await?;
        announce_pieces(&mut conn, queue, &mut announced).await?;
        if queue.is_finished() {
            return Ok(());
        }
        if has_changed {
            pieces.update(conn.peer_has());
            stats.set_pieces(conn.peer_has().iter().filter(|&&has| has).count());
            pex.set_seed(conn.peer_has().iter().all(|&has| has));
            has_changed = false;
        }
        pex.tick(&mut conn, Instant::now()).await?;
        if queue.is_done() && conn.peer_has().iter().all(|&has| has) {
            debug!(%addr, "both seeding, letting go");
            return Ok(());
        }
        // Interested only while the peer has a piece we still need, even if
        // another peer is downloading it right now. Any piece we hold counts.
        let wanted = !active.is_empty() || queue.wants(&pieces);
        conn.set_interested(wanted).await?;
        // The choker hears of interest changes at once, and its answer
        // wakes the waits below.
        slot.set_interested(conn.peer_interested());
        let choking = !slot.is_unchoked();
        conn.set_choking(choking).await?;
        publish_peer(&stats, &conn, fast, torrent.is_private());
        if choking {
            // A choke drops the peer's requests, except those for its
            // allowed fast pieces. With the fast extension each dropped one
            // is rejected, after the Choke as BEP 6 asks; without it there
            // is no way to reject them one by one.
            let allowed = offered.as_deref().unwrap_or_default();
            let dropped = uploads.retain(|request| allowed.contains(&request.index));
            if fast && !dropped.is_empty() {
                let rejects: Vec<PeerMessage> =
                    dropped.into_iter().map(BlockRequest::reject).collect();
                conn.send_all(&rejects).await?;
            }
        }
        let now = Instant::now();
        // Worth keeping while either side wants something from the other.
        if wanted || conn.peer_interested() {
            last_useful = now;
        }
        unchoke_deadline = match unchoke_deadline {
            _ if !wanted || !conn.is_choked() => None,
            Some(deadline) => Some(deadline),
            None => Some(now + UNCHOKE_TIMEOUT),
        };
        if fast && conn.is_choked() {
            release_choked(&mut active, conn.allowed_fast());
        }
        // While choked, the fast extension still lets us ask for the pieces
        // the peer allowed.
        if !conn.is_choked() || (fast && conn.allowed_fast().contains(&true)) {
            request_blocks(
                &mut conn,
                queue,
                &pieces,
                torrent,
                &mut active,
                &mut pipeline,
                &mut download_budget,
            )
            .await?;
        } else {
            // Nothing to ask for until unchoked: the turn goes to others.
            download_budget.release();
        }

        // Send the next block the peer asked for once the messages that had
        // already arrived are handled, so that a Cancel among them is
        // honored, or after `MAX_MESSAGES_PER_BLOCK` of them, so that a peer
        // that keeps sending still gets its blocks. Under an upload limit,
        // the block also waits for this peer's turn.
        if uploads.is_empty() {
            messages_since_block = 0;
            upload_budget.release();
        } else if (drained || messages_since_block >= MAX_MESSAGES_PER_BLOCK)
            && let Some(&request) = uploads.front()
            && upload_budget.try_take(request.length.into(), Instant::now())
        {
            uploads.pop();
            serve_block(&mut conn, &session.storage, request).await?;
            slot.uploaded(request.length.into());
            stats.add_uploaded(request.length.into());
            progress.uploaded(request.length.into());
            messages_since_block = 0;
        }

        // When our next turn at a rate limit comes, if we are waiting for
        // one. A turn that has come but is not used yet (say, the pipeline
        // is full) is not waited for.
        let waiting_since = Instant::now();
        let next_turn = [&download_budget, &upload_budget]
            .into_iter()
            .filter(|budget| budget.is_waiting(waiting_since))
            .filter_map(|budget| budget.ready_at())
            .min();
        // Set if the wait below ends because our turn came, rather than
        // because nothing was left to read.
        let mut turn_came = false;

        // `receive` and `receive_untimed` are cancel-safe, so losing a race
        // loses no data.
        let message = if !uploads.is_empty() && !upload_budget.is_waiting(waiting_since) {
            // More blocks to send: only take a message that is already here.
            tokio::select! {
                biased;
                message = conn.receive_untimed() => Some(message?),
                () = std::future::ready(()) => None,
            }
        } else if active.is_empty() {
            if unchoke_deadline.is_some_and(|deadline| now >= deadline) {
                bail!("peer did not unchoke us");
            }
            if now - last_useful > IDLE_TIMEOUT {
                bail!("neither side has pieces the other needs");
            }
            conn.keep_alive().await?;
            // Wait for an unchoke, for a piece to be returned or completed,
            // for the queue to close, for the peer to announce a new piece or
            // ask for one of ours, or for the choker to (un)choke it.
            tokio::select! {
                () = &mut changed => None,
                message = conn.receive_untimed() => Some(message?),
                () = slot.changed() => None,
                _ = tokio::time::sleep(IDLE_POLL) => None,
                () = sleep_until_some(next_turn) => {
                    turn_came = true;
                    None
                }
            }
        } else {
            conn.keep_alive().await?;
            // A piece completed by any worker is announced to this peer.
            let woken = || {
                queue.is_closed()
                    || queue.completed_count() > announced
                    || active.iter().any(|piece| piece.claim.is_completed())
            };
            // With nothing in flight, we are waiting for our turn at the
            // download limit, so the peer has nothing to send and its
            // silence is no reason to drop it.
            let in_flight: usize = active.iter().map(ActivePiece::in_flight).sum();
            let receive = async {
                if in_flight > 0 {
                    conn.receive().await
                } else {
                    conn.receive_untimed().await
                }
            };
            tokio::select! {
                message = receive => Some(message?),
                () = queue.wait_for(woken) => None,
                () = slot.changed() => None,
                () = sleep_until_some(next_turn) => {
                    turn_came = true;
                    None
                }
            }
        };
        // Messages may have arrived while we waited for our turn, and are
        // handled before the next block goes out.
        drained = message.is_none() && !turn_came;
        let Some(message) = message else {
            continue;
        };
        messages_since_block += 1;

        match message {
            PeerMessage::Have { .. } => has_changed = true,
            PeerMessage::Bitfield { .. } | PeerMessage::HaveAll | PeerMessage::HaveNone => {
                has_changed = true;
                // The peer's first word on its pieces: if it is just
                // starting out, it gets our allowed fast set.
                if fast && offered.is_none() {
                    offered = Some(offer_allowed_fast(&mut conn, &info_hash).await?);
                }
            }
            PeerMessage::Port { port } => {
                if let Some(dht) = &session.dht {
                    dht.add_node(SocketAddr::new(addr.ip(), port));
                }
            }
            PeerMessage::Extended {
                id: pex::ID,
                payload,
            } => pex.received(&payload, Instant::now(), queue.is_done())?,
            // Without the fast extension a choke drops our requests:
            // dropping the claims returns the pieces, and the next turn
            // waits for an unchoke. With it they stand until rejected (see
            // `release_choked`).
            PeerMessage::Choke if !fast && !active.is_empty() => {
                debug!(%addr, pieces = active.len(), "choked mid-download");
                active.clear();
            }
            PeerMessage::Piece {
                index,
                begin,
                block,
            } => {
                let answered = BlockRequest {
                    index,
                    begin,
                    length: block.len() as u32,
                };
                // Blocks of pieces we no longer hold are late replies to
                // requests abandoned on a choke or cancelled in endgame.
                let Some(pos) = active.iter().position(|piece| piece.index == index) else {
                    cancelled.remove(&answered);
                    continue;
                };
                if !active[pos].add_block(begin, &block)? {
                    cancelled.remove(&answered);
                    continue;
                }
                pipeline.received(block.len() as u64, Instant::now());
                slot.downloaded(block.len() as u64);
                stats.add_downloaded(block.len() as u64);
                session.stats.add_received(block.len() as u64);
                if !active[pos].is_complete() {
                    continue;
                }
                let index = active[pos].claim.index;
                let Some((claim, data)) = active.swap_remove(pos).verify() else {
                    hash_failures += 1;
                    warn!(%addr, index, "piece failed hash check");
                    if hash_failures >= MAX_HASH_FAILURES {
                        bail!(Misbehaved("too many corrupt pieces"));
                    }
                    continue;
                };
                // In endgame another peer may have finished it meanwhile. If
                // both pass this check, both write the same verified bytes.
                if claim.is_completed() {
                    continue;
                }
                let piece_len = data.len() as u64;
                session.storage.write_piece(index, data).await?;
                if claim.complete() {
                    *pieces_done += 1;
                    progress.add(piece_len);
                    debug!(%addr, index, "piece complete");
                }
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // We only ever advertise pieces we have, so asking for
                // another is a protocol violation.
                if !queue.has(index as usize) {
                    bail!(Misbehaved("peer requested a piece we do not have"));
                }
                request.check(torrent.piece_len(index as usize)?)?;
                let allowed = offered.as_ref().is_some_and(|set| set.contains(&index));
                if conn.am_choking() && !allowed {
                    // Sent before our Choke arrived, which dropped it. With
                    // the fast extension it is rejected.
                    debug!(%addr, ?request, "turning down request from choked peer");
                    if fast {
                        conn.send(&request.reject()).await?;
                    }
                } else if !uploads.push(request) {
                    debug!(%addr, ?request, "dropping request: too many queued");
                    if fast {
                        conn.send(&request.reject()).await?;
                    }
                }
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // With the fast extension even a cancelled request gets an
                // answer: a Reject if its block was not sent yet.
                if uploads.cancel(&request) && fast {
                    conn.send(&request.reject()).await?;
                }
            }
            PeerMessage::RejectRequest {
                index,
                begin,
                length,
            } => {
                let request = BlockRequest {
                    index,
                    begin,
                    length,
                };
                // The answer to a request we cancelled.
                if cancelled.remove(&request) {
                    continue;
                }
                let rejected = active
                    .iter_mut()
                    .find(|piece| piece.index == index)
                    .is_some_and(|piece| piece.reject(begin, length));
                if !rejected {
                    bail!(Misbehaved("peer rejected a request we did not send"));
                }
                debug!(%addr, ?request, choked = conn.is_choked(), "request rejected");
                // The block is asked for again: at once while unchoked, and
                // by whoever claims the piece next once it is released. A
                // piece the peer allowed us while choked but rejects anyway
                // is not asked for again until it unchokes us, or we would
                // ask over and over.
                if conn.is_choked() {
                    conn.disallow_fast(index);
                }
            }
            // A hint we do without: rarest first picks our pieces.
            PeerMessage::SuggestPiece { .. } => {}
            PeerMessage::Extended {
                id: extension::UT_METADATA_ID,
                payload,
            } => metadata::serve(&mut conn, session.metadata.as_deref(), &payload).await?,
            _ => {}
        }
    }
}

/// Tells `stats` how the connection `conn` stands, and the peer's client
/// once its extension handshake names it.
fn publish_peer(stats: &PeerStats, conn: &PeerConnection, fast: bool, private: bool) {
    let extensions = conn.peer_extensions();
    let pex = !private && extensions.is_some_and(|handshake| handshake.id(pex::NAME).is_some());
    stats.set_flags(PeerFlags::new(
        conn.am_choking(),
        conn.is_choked(),
        conn.am_interested(),
        conn.peer_interested(),
        fast,
        pex,
    ));
    if !stats.has_client()
        && let Some(client) = extensions.and_then(|handshake| handshake.client.as_deref())
    {
        stats.set_client(client);
    }
}

/// Sends Have for every piece completed since the last call, except those
/// the peer has itself (Have suppression, which the spec allows).
async fn announce_pieces(
    conn: &mut PeerConnection,
    queue: &PieceQueue,
    announced: &mut usize,
) -> Result<()> {
    let completed = queue.completed_since(*announced);
    *announced += completed.len();
    let peer_has = conn.peer_has();
    let haves = completed
        .into_iter()
        .filter(|&index| !peer_has.get(index).copied().unwrap_or(false))
        .map(|index| {
            let piece_index = u32::try_from(index).map_err(|_| anyhow!("piece index too large"))?;
            Ok(PeerMessage::Have { piece_index })
        })
        .collect::<Result<Vec<_>>>()?;
    if !haves.is_empty() {
        conn.send_all(&haves).await?;
    }
    Ok(())
}

/// Sends the peer our allowed fast set (fast extension) if it is just
/// starting out (see `fast::is_starting`), and returns the set, empty if
/// the peer did not get one. The set may name pieces we do not have yet;
/// the peer can only ask for those once we announce them.
async fn offer_allowed_fast(conn: &mut PeerConnection, info_hash: &[u8; 20]) -> Result<Vec<u32>> {
    let pieces = conn.peer_has().iter().filter(|&&has| has).count();
    if !fast::is_starting(pieces, conn.peer_has().len()) {
        return Ok(Vec::new());
    }
    let num_pieces = u32::try_from(conn.peer_has().len())?;
    let set = fast::allowed_fast_set(
        conn.addr.ip(),
        info_hash,
        num_pieces,
        fast::ALLOWED_FAST_COUNT,
    );
    let messages: Vec<PeerMessage> = set
        .iter()
        .map(|&piece_index| PeerMessage::AllowedFast { piece_index })
        .collect();
    if !messages.is_empty() {
        conn.send_all(&messages).await?;
    }
    debug!(addr = %conn.addr, ?set, "sent allowed fast set");
    Ok(set)
}
