# rtrnt roadmap

Each phase builds on the one before. Items link to the relevant BitTorrent Enhancement Proposal (BEP) where one applies.

## Phase 1: Hardening ✅

Make the existing download path correct and robust.

- [x] **Info hash from raw bytes**: hash the exact `info` bytes (`bencode::info_span`). Re-encoding the parsed struct dropped unknown keys such as `private`, which gave a wrong hash.
- [x] **Pieces are never lost**: a failed or abandoned piece goes back to the queue through an RAII claim. Before, one error left a permanent hole in the file.
- [x] **Peer bitfield tracking**: Bitfield and Have messages are recorded, and peers are only asked for pieces they have.
- [x] **Timeouts everywhere**: connect, handshake, read, write, unchoke and UDP tracker (with retries). A silent peer can no longer hang the client.
- [x] **Choke handling**: a choke mid-piece drops the piece back into the queue and waits for an unchoke.
- [x] **Safe message parsing**: frame size cap, exact-length checks, unknown message IDs skipped, block bounds validated. A malicious peer can no longer cause a panic or a 4 GB allocation.
- [x] **Request pipelining**: 16 outstanding block requests per peer.
- [x] **Disk storage**: verified pieces are written straight to disk (no whole file in RAM). Multi-file torrents are supported, and path traversal (`..`, absolute paths) is rejected.
- [x] **Trackers**: tries `announce` then all `announce-list` URLs. UDP: fixed short-read check, announce field order, retries, larger buffer, error action. HTTP: failure reason, compact and dict peer lists, `peers6`, request timeout.
- [x] **CLI and UX**: clap arguments, `tracing` logs (`RUST_LOG`), indicatif progress bar, random peer id.
- [x] **Tests**: unit tests per module, plus a loopback integration test against a mock seeder that chokes once and sends a corrupt block.

## Phase 2: Download engine

Faster, steadier downloads in real swarms.

- [x] **Rarest-first piece selection**: piece availability is counted across connected peers (Bitfield, Have, disconnects), and each peer claims the rarest piece it has, with random tie-breaks.
- [x] **Endgame mode**: once every remaining piece is in flight, idle peers also download those pieces, starting with the one fewest peers are on. The first verified copy wins, and the others send `Cancel` for their outstanding blocks. A stalled peer no longer holds up the finish. (This works per piece; per-block endgame is not done.)
- [x] **Peer pool and replacement**: every tracker peer is kept in a pool, and a worker that dies is replaced by the next one. Failed peers are retried with exponential backoff (30 s, 60 s) and dropped after 3 failures in a row; a peer that delivered a piece starts counting again. Peers that send corrupt or invalid data are banned. When the download completes, workers still connecting are aborted after a short grace period.
- [x] **Periodic re-announce**: the tracker is announced to again on its `interval`, kept between 60 s and 2 h and never shorter than `min interval`. Each announce runs in the background and reports real `downloaded`/`left` counts, and the peers it returns join the pool. When the pool runs dry, the tracker is asked once more, as early as `min interval` allows, before the download gives up. At exit the tracker gets `completed` (if the download finished) and then `stopped`, with a 10 s limit on both.
- [x] **Tracker tiers ([BEP 12](https://www.bittorrent.org/beps/bep_0012.html))**: `announce-list` is kept as tiers and replaces `announce` when present. Each tier is shuffled once at startup. Tiers are tried in order, and the tracker that answers moves to the front of its tier, so re-announces and the final `completed`/`stopped` go to it first. (One deviation: when looking for peers, a tracker that answers with none is passed over for the next one.)
- [x] **Keep-alives and interest**: a keep-alive goes out on any connection we have not written to for 90 s, under the usual 2-minute peer timeout. We are `Interested` only while a peer has a piece we still need, including one another peer is downloading, and send `NotInterested` once it has none. Waiting for an unchoke is part of the worker's idle wait, so a choked worker notices when its pieces arrive from elsewhere. A peer is dropped after 60 s choked while we are interested, or after 2 minutes with nothing we need. A peer that has something we need, even if we cannot claim it right now, is kept.
- [x] **Adaptive pipeline depth**: each peer's request queue holds about 3 s of its measured download rate, between 2 and 250 blocks (4 to start). The rate is measured over 1 s windows, and time spent choked or with nothing to request does not count against it. Requests now span pieces: a worker claims another piece when every block of the pieces it holds has been requested, so the pipeline no longer drains at each piece boundary. A slow peer gets a short queue and so holds few pieces. (Without the extension handshake, a peer's real request limit is unknown, so the cap is a conservative 250.)
- [x] **Cached file handles** in `Storage`: files are opened once and kept in a cache of up to 64 handles, least recently used out first, so a torrent with thousands of files stays well under the descriptor limit. Reads and writes use positional I/O (`pread`/`pwrite`), so concurrent piece writes share a handle without seeking and the cache lock is held only for the lookup.

## Phase 3: Persistence

- [ ] **Resume**: at startup, hash existing file data and mark verified pieces done. `Storage` already preserves existing contents.
- [ ] **`--check` mode**: verify a finished download against the torrent.
- [ ] **Graceful shutdown**: on Ctrl-C, finish in-flight writes and send `stopped` to the tracker.

## Phase 4: Seeding

Give back to the swarm.

- [ ] **Listen for incoming peers** on `LISTEN_PORT` (configurable; currently only reported to trackers).
- [ ] **Serve requests**: answer `Request` from `Storage::read_at`, send our `Bitfield` after the handshake and `Have` after each completed piece.
- [ ] **Choking algorithm**: tit-for-tat with 4 unchoke slots, plus an optimistic unchoke every 30 s.
- [ ] **Upload stats** reported to trackers; a seed-ratio or seed-time limit.

## Phase 5: Trackerless and extensions

- [ ] **Extension protocol ([BEP 10](https://www.bittorrent.org/beps/bep_0010.html))**: set reserved bit 20 and add the extended handshake.
- [ ] **Magnet links ([BEP 9](https://www.bittorrent.org/beps/bep_0009.html))**: fetch the info dict from peers via `ut_metadata`.
- [ ] **DHT ([BEP 5](https://www.bittorrent.org/beps/bep_0005.html))**: Kademlia peer discovery without trackers.
- [ ] **Peer exchange ([BEP 11](https://www.bittorrent.org/beps/bep_0011.html))**.
- [ ] **Fast extension ([BEP 6](https://www.bittorrent.org/beps/bep_0006.html))**: `Have All`/`Have None`, reject messages, allowed-fast pieces.
- [ ] **UDP tracker scrape** and **IPv6 listening**.

## Phase 6: User experience

- [ ] Config file (download dir, ports, peer limits).
- [ ] Upload and download rate limits.
- [ ] Several torrents in one session.
- [ ] TUI (e.g. `ratatui`): per-peer stats, piece map, swarm health.
