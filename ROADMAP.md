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

- [ ] **Rarest-first piece selection**: count piece availability across peers and download rare pieces first, so the swarm stays healthy and we don't stall on the last piece.
- [ ] **Endgame mode**: once only a few blocks are left, request them from several peers and send `Cancel` when one arrives. This fixes slow tails.
- [ ] **Peer pool and replacement**: keep the full tracker peer list and replace dropped peers. Today a worker that dies is never replaced.
- [ ] **Periodic re-announce**: announce again on the tracker's `interval` with real `downloaded`/`left`, and send `completed`/`stopped` events.
- [ ] **Tracker tiers ([BEP 12](https://www.bittorrent.org/beps/bep_0012.html))**: shuffle within a tier and promote the tracker that works.
- [ ] **Keep-alives**: send a keep-alive every ~2 minutes on idle connections, and `NotInterested` when a peer has nothing we need.
- [ ] **Adaptive pipeline depth**: size the request queue to each peer's throughput instead of a fixed 16.
- [ ] **Cached file handles** in `Storage`: open files once instead of on every write.

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
