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
- [x] **Adaptive pipeline depth**: each peer's request queue holds about 3 s of its measured download rate, between 2 and 250 blocks (4 to start). The rate is measured over 1 s windows, and time spent choked or with nothing to request does not count against it. Requests now span pieces: a worker claims another piece when every block of the pieces it holds has been requested, so the pipeline no longer drains at each piece boundary. A slow peer gets a short queue and so holds few pieces. (The cap is a conservative 250, lowered to a peer's `reqq` when its extension handshake gives one; a peer that allows more still gets at most 250.)
- [x] **Cached file handles** in `Storage`: files are opened once and kept in a cache of up to 64 handles, least recently used out first, so a torrent with thousands of files stays well under the descriptor limit. Reads and writes use positional I/O (`pread`/`pwrite`), so concurrent piece writes share a handle without seeking and the cache lock is held only for the lookup.

## Phase 3: Persistence

- [x] **Resume**: at startup, pieces that overlap a file that already had data are hashed (on up to 4 threads, with a progress bar), and those that match are marked done. Pieces lying entirely in newly created files are skipped, so a new download starts at once. The tracker hears the real `left`, the progress bar starts at the data already on disk, and a download that is already complete exits without contacting the tracker. (There is no resume file, so an interrupted run's zero-filled pieces are hashed too.)
- [x] **`--check` mode**: verify a finished download against the torrent. Files are opened read-only and nothing is created or resized. Every piece whose bytes are all on disk is hashed, and each file is reported as missing, the wrong size, or with its number of bad pieces (a piece spanning two files counts against both). The exit code is non-zero unless every piece is intact.
- [x] **Graceful shutdown**: on Ctrl-C the piece queue closes and workers exit at their next wait. A worker writing a piece finishes the write first, and a worker aborted after the 2 s grace period cannot tear a write, because writes run to the end on the blocking pool. The tracker gets `stopped` (with the real `downloaded`/`left`), the progress bar stays where it was, and the exit code is 130. A second Ctrl-C quits at once. The handler is only installed once the download starts, so Ctrl-C while hashing or during the first announce still kills the process at once. (Blocks of partly downloaded pieces are lost.)

## Phase 4: Seeding

Give back to the swarm.

- [x] **Listen for incoming peers** on `--port` (default 6881), over IPv4 and IPv6. If the port is taken, a free one is used instead, and trackers are told the port actually bound. Peers that connect to us and ask for our torrent are downloaded from like any other, while there is room under `--max-peers`. A peer is used over one connection at a time, so one we connect to that also connects to us is only downloaded from once. Our own address, which trackers list, is recognized by our peer id and never connected to again. An incoming peer that misbehaves is banned by IP. (We never unchoke anyone yet, so nothing is uploaded.)
- [ ] **Serve requests**: answer `Request` from `Storage::read_at`, send our `Bitfield` after the handshake and `Have` after each completed piece.
- [ ] **Choking algorithm**: tit-for-tat with 4 unchoke slots, plus an optimistic unchoke every 30 s.
- [ ] **Upload stats** reported to trackers; a seed-ratio or seed-time limit.

## Phase 5: Trackerless and extensions

- [x] **Extension protocol ([BEP 10](https://www.bittorrent.org/beps/bep_0010.html))**: our handshake sets reserved bit 20, and when the peer sets it too, our extension handshake follows at once with our client name and version (`v`), listen port (`p`), request queue size (`reqq`, 250) and the peer's address as we see it (`yourip`). The peer's extension handshake is kept on the connection, its client name is logged, and a later one updates it. Its `reqq` caps how many requests we keep in flight to that peer. Parsing is lenient: unknown keys, and known keys with the wrong type or an out-of-range value, are ignored, but a payload that is not a bencoded dictionary gets the peer banned. The connection can look up the id a peer wants for an extension name and send an extended message under it, ready for `ut_metadata` and `ut_pex`. (Our `m` is empty, since no extension is implemented yet, and extended messages other than the handshake are ignored.)
- [ ] **Magnet links ([BEP 9](https://www.bittorrent.org/beps/bep_0009.html))**: fetch the info dict from peers via `ut_metadata`.
- [ ] **DHT ([BEP 5](https://www.bittorrent.org/beps/bep_0005.html))**: Kademlia peer discovery without trackers.
- [ ] **Peer exchange ([BEP 11](https://www.bittorrent.org/beps/bep_0011.html))**.
- [ ] **Fast extension ([BEP 6](https://www.bittorrent.org/beps/bep_0006.html))**: `Have All`/`Have None`, reject messages, allowed-fast pieces.
- [x] **UDP tracker scrape and IPv6 listening**: `--scrape` asks every tracker of the torrent for its seeder, leecher and completed counts, prints one line per tracker, tier by tier, and exits. The exit code is non-zero unless a tracker answered. Every tracker is asked, not just the first to answer, because tiers often keep separate swarms. They are all asked at once, so a dead tracker costs its timeout only once. UDP scrape ([BEP 15](https://www.bittorrent.org/beps/bep_0015.html)) reuses the connect and retry logic of announces and sends up to 74 info hashes per request. HTTP scrape ([BEP 48](https://www.bittorrent.org/beps/bep_0048.html)) was added too: the scrape URL comes from the announce URL, and counts are read from the `files` dict. Incoming peers are also accepted on an IPv6-only `[::]` listener, on the port the IPv4 one got, and go through the same IP bans and `--max-peers` limit. Without IPv6 we listen over IPv4 only. Outgoing connections already handled IPv6 peers from `peers6` and from UDP trackers reached over IPv6. (An HTTP tracker whose announce URL does not end in `announce`, or `announce.php` and the like, cannot be scraped. We send no `ipv6=` to HTTP trackers, because there is no clean way to find our global IPv6 address; a tracker reached over IPv6 sees it anyway. An IPv6 peer is banned by its exact address, not its /64.)

## Phase 6: User experience

- [x] **Config file**: settings are read from the TOML file given with `--config`, or else from `$XDG_CONFIG_HOME/rtrnt/config.toml` (`~/.config/rtrnt/config.toml` when that is unset), on macOS too. The keys are the long option names: `output`, `max-peers` and `port`. A command-line flag beats the file, and the file beats the built-in default; `--print-config` prints the effective settings and where each came from. A leading `~` in `output` is expanded. An unknown key or a value of the wrong type is an error that names the file and line, and so is a missing `--config` file, while a missing default file just means the defaults. (Only these three settings can be set so far, and a relative `output` is taken from the current directory, not the config file's.)
- [ ] Upload and download rate limits.
- [ ] Several torrents in one session.
- [ ] TUI (e.g. `ratatui`): per-peer stats, piece map, swarm health.
