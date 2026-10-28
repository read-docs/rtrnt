
# rtrnt

A terminal-based BitTorrent client written in Rust.

## Overview

**rtrnt** is a lightweight BitTorrent client implementation built with Rust, designed to download files from torrent networks through the terminal. It supports both HTTP and UDP tracker protocols and implements the core BitTorrent peer protocol for efficient file downloads.

## Features

- 🚀 **Async/Await Architecture** - Built with Tokio for high-performance concurrent downloads
- 🌐 **Multi-Protocol Support** - Compatible with both UDP and HTTP trackers
- 🔗 **Peer Protocol Implementation** - Full BitTorrent peer wire protocol support, with the extension protocol (BEP 10) and the fast extension (BEP 6)
- ⚡ **Parallel Downloads** - Concurrent piece downloading from multiple peers
- 🔒 **Data Integrity** - SHA-1 hash verification for downloaded pieces
- 📦 **Bencode Support** - Native torrent file parsing

## Usage

```sh
cargo run --release -- path/to/file.torrent -o ~/Downloads
cargo run --release -- 'magnet:?xt=urn:btih:...&tr=udp://tracker.example:1337' -o ~/Downloads
cargo run --release -- first.torrent second.torrent 'magnet:?xt=urn:btih:...' -o ~/Downloads
```

### Terminal UI

When stdout and stderr are terminals, a run shows a full-screen UI instead of progress bars and log lines (`--no-tui`, or `tui = false` in the config file, turns it off; `--tui` turns it on regardless). It is never used with `--check`, `--scrape` or `--print-config`. Only the UI can start without a torrent: `rtrnt` alone opens it empty, saying `press a to add a torrent`. At the top are the session's rates and counts, then the torrent table, then a detail panel for the selected torrent with five tabs: its peers (client, flags, rates and how much each has, with the torrent's rates over the last half minute, or the selected peer's while the panel has the focus), a map of its pieces with the swarm's numbers, its trackers, its files, and facts about it. The log sits below, and the keys at the bottom:

| Key | Action |
|-----|--------|
| `↑`/`↓`, `j`/`k` | select a torrent; when the detail panel has the focus, select a peer on the Peers tab or scroll the other tabs |
| `PgUp`/`PgDn` | move the peer selection a page, or scroll the other tabs |
| `tab` | move the focus between the torrent table and the detail panel |
| `1`–`5` | peers, pieces, trackers, files, info |
| `l` | show or hide the log |
| `s` / `S` | sort by the next column / reverse the order |
| `/` | filter torrents by name; `esc` clears the filter |
| `p` | pause the selected torrent, or resume it; a finished or failed torrent is run again |
| `x` | remove the selected torrent from the session, after asking: `y` keeps its files, `D` deletes them too |
| `r` | recheck the selected torrent: hash its files again, ignoring the resume file (see below) |
| `a` | add a torrent: the path of a `.torrent` file (a relative one is taken from the directory rtrnt was started in) or a magnet link. It starts as one given on the command line would, with the same output directory and settings; one the session has already is refused, and so is a file that does not load, with a note at the bottom |
| `d` / `u` | set the download / upload limit in KiB/s, 0 for none, until the run ends; the header shows the limits next to the rates |
| `m` | turn the mouse off, so the terminal can select text, or on again |
| `?` | help |
| `q`, `Ctrl-C` | stop the torrents, as Ctrl-C does without the UI; pressed again, quit at once |

A pause stops the torrent as Ctrl-C does: the tracker hears `stopped`, its peers are let go, and the resume file is saved once the pieces being written are on disk. The files stay as they are. Resuming starts it again from that resume file, so nothing is hashed, and a magnet link whose metadata had arrived from the `.torrent` it saved, so the metadata is not fetched again. A complete torrent resumed seeds if `--seed-ratio` or `--seed-time` says to, and is otherwise done at once. The row keeps its place, and its totals, ratio and time active count the earlier runs; so does the seed ratio, so a pause does not start the share over (the seed time does start over). A torrent still paused when the session ends makes the exit code 130, as Ctrl-C does, and the next run resumes it.

A recheck of a running torrent stops it as a pause does and starts it again with every piece hashed, its row showing `queued` at once and then the checking progress; it then carries on downloading or seeding. A paused torrent is checked and stays paused, with the result saved in its resume file. A finished or failed torrent is checked and then runs again, so one found incomplete fetches the rest. A torrent being removed or already restarting is left alone, and so is a magnet link whose metadata has not arrived; a note at the bottom says so.

Removing a torrent stops it first if it runs, as a pause does, and its row shows `removing` until it has; then the row goes. `y` leaves its files and its resume file as they are, so adding it again later starts without hashing. `D` (uppercase only) also deletes its files, the directories they leave empty (never the output directory itself), its resume file and, for a magnet link, the `.torrent` it saved; the `.torrent` file it was started from stays. Only regular files are deleted: a symbolic link where a file or directory of the torrent's should be is left alone, and so is what it points to. Nothing is deleted while another running torrent writes one of the files, and whatever could not be deleted is logged and shown at the bottom. A magnet link still fetching its metadata has no files yet. A removed torrent no longer counts toward the exit code.

The mouse works too: click a torrent or a peer to select it, a column heading to sort by it (again to reverse), or a tab title to show it; the wheel moves the selection, or scrolls the detail panel, under the pointer. While the UI has the mouse, most terminals still select text with Shift held down, or press `m` to give the mouse back.

Peer flags: `D` we download from the peer, `d` we want to but it chokes us; `U` we upload to it, `u` it wants us to but we choke it; `I` it connected to us; `F` fast extension; `X` peer exchange. When every torrent has finished the UI says so and waits for `q`, and the warnings and errors of the last 10 seconds are printed once the terminal is back. `cargo run --example tui_demo` shows the UI with a made-up session.

Give several torrents and they are downloaded at once, each with its own progress bar, sharing one listening port, one DHT node, the rate limits and the connection limit, and checking the data already on disk one torrent at a time. One that fails does not stop the others, Ctrl-C stops them all, and the run ends when every one is done (and seeded, with `--seed-ratio` or `--seed-time`). A torrent that would write a file another running one writes is refused, naming the file, before it touches anything. With `--check` or `--scrape`, each torrent is handled in turn under its name.

A magnet link works wherever a .torrent file does. The torrent's metadata is fetched from peers found through the link's trackers (`tr`), its peers (`x.pe`) and the DHT, then the download carries on with the same peers. The metadata is saved as `<info hash>.torrent` in the output directory, so running the same link again resumes without fetching it, and `--check` uses that file (it fails without it). `--scrape` asks the link's trackers.

Options: `-o, --output <DIR>` (default `.`), `--max-peers <N>` per torrent (default 30), `--max-connections <N>` across all torrents (default 200), `-p, --port <PORT>` to listen on for incoming peers over IPv4 and IPv6 (default 6881), `--no-dht` to find peers through trackers only (`--dht` turns the DHT back on if the config file turned it off), `--check` to verify a finished download (for a magnet link, against the saved `.torrent`), `--recheck` to ignore the resume files and hash the data already on disk, `--scrape` to print each tracker's seeder, leecher and completed counts without downloading (a magnet link's `tr` trackers), `--seed-ratio <RATIO>` and `--seed-time <MINUTES>` to keep seeding once the download is complete, until we have uploaded that many times the torrent's size or for that long, whichever comes first (both default to 0, which exits at completion), and `--max-download-rate <KIB/S>` and `--max-upload-rate <KIB/S>` to limit how fast piece data moves each way, in KiB per second across all peers of all torrents (both default to 0, unlimited). The limits count piece data only, not protocol messages or TCP/IP headers, so the traffic on the wire is slightly higher. Without `--no-dht`, peers are also looked up in the BitTorrent DHT (on UDP, same port), so torrents without trackers download too. Connected peers that support peer exchange also tell us about more peers, and we tell them about ours (never for private torrents). Set `RUST_LOG=debug` for per-peer logs.

Progress is saved in a resume file per torrent, `$XDG_STATE_HOME/rtrnt/resume/<info hash>.resume` (`~/.local/state/rtrnt/resume/` when that is unset), every 30 seconds while pieces complete and at exit. A restart trusts the pieces it lists as long as the files still have the size and modification time they had then, so a clean exit rehashes nothing, and after a crash only the files written since the last save are hashed. `--recheck` ignores the file; `--check` never reads it. The file also keeps which files of the torrent are skipped or come first, once something sets that.

### Config file

Every option except `--check`, `--recheck`, `--scrape`, `--config` and `--print-config` can also be set in `~/.config/rtrnt/config.toml` (or `$XDG_CONFIG_HOME/rtrnt/config.toml`), or in a file given with `--config <PATH>`. Keys are the long option names, except that `--no-dht` is `dht = false` and `--no-tui` is `tui = false`, and every key is optional:

```toml
output = "~/Downloads"
max-peers = 50
max-connections = 150
port = 51413
seed-ratio = 1.5
seed-time = 120
dht = false
max-download-rate = 2048
max-upload-rate = 256
tui = false
```

A flag on the command line overrides the file, and the file overrides the built-in defaults. `--print-config` shows the effective settings and where each one came from. Unknown keys are errors, so typos don't go unnoticed.

Run the tests with `cargo test`. They include a loopback integration test against a mock seeder.

See [ROADMAP.md](ROADMAP.md) for what's done and what's next.

## Dependencies

- **tokio** - Async runtime
- **serde** & **serde_bencode** - Torrent file parsing
- **sha1** - Piece hash verification
- **reqwest** - HTTP tracker communication
- **clap** - Command-line interface
- **toml** - Config file parsing
- **indicatif** - Progress tracking
- **ratatui**, **crossterm** & **futures-util** - Terminal UI and its key events
- **chrono** - Local time in the UI
- **unicode-width** - Column widths of torrent names
- **anyhow** - Error handling

## Project Structure

```
src/
├── main.rs        # CLI entry point: hands the torrents to the manager, prints the result
├── cli.rs         # Command-line arguments (clap)
├── shutdown.rs    # Ctrl-C handling: first stops gracefully, second exits
├── log_line.rs    # Log output that does not break the progress bars
├── lib.rs         # Library root
├── bencode.rs     # Raw bencode scanning (info-hash span)
├── client.rs      # What the torrents of a session share: port, DHT, rate limits, bars
├── config.rs      # Config file loading and CLI > file > default precedence
├── dht/           # Trackerless peer discovery (BEP 5)
│   ├── mod.rs       # DHT node: UDP socket, queries and answers, lookups
│   ├── krpc.rs      # KRPC messages, bencode values, compact node/peer info
│   ├── routing.rs   # Node ids, XOR distance, k-bucket routing table
│   ├── lookup.rs    # Iterative lookup state machine
│   ├── token.rs     # get_peers tokens with rotating secrets
│   ├── store.rs     # Peers announced to us
│   └── search.rs    # Periodic lookups for the torrent being downloaded
├── download.rs    # Download config, Session and the entry points
├── download/
│   ├── swarm.rs     # A torrent's main loop: starts, replaces and bans peer workers
│   ├── worker.rs    # One peer: download and upload over its connection
│   ├── queue.rs     # Piece queue: rarest first, endgame, availability
│   ├── requests.rs  # Pieces in progress and request pipelining
│   ├── upload.rs    # Blocks peers asked us for
│   ├── check.rs     # Hashing the data on disk
│   ├── choker.rs    # Choking algorithm: who may download from us
│   ├── peer_pool.rs # Known peers, retry backoff and bans
│   └── progress.rs  # Progress bar
├── listener.rs    # Incoming peers, handed to the torrent their handshake asks for
├── magnet.rs      # Magnet links (BEP 9): parsing, metadata fetch, then download
├── magnet/
│   └── metadata.rs  # ut_metadata: messages, assembling and fetching the info dict
├── manager.rs     # The session's torrents: starts each, tracks its state, exit code
├── peer.rs        # Peer wire protocol
├── peer/
│   ├── message.rs   # Message encode/decode
│   ├── handshake.rs # Handshakes, ours and incoming
│   ├── connection.rs # A connection to one peer
│   ├── extension.rs # Extension protocol (BEP 10) handshake
│   ├── fast.rs      # Fast extension (BEP 6): allowed fast set, Have All/Have None
│   └── pex.rs       # Peer exchange (BEP 11): telling peers about each other
├── rate_limit.rs  # Upload and download rate limits (shared token buckets)
├── resume.rs      # Resume files: what was on disk last time
├── selection.rs   # Which files to download, and which first; piece priorities
├── source.rs      # What to download: a .torrent file or a magnet link
├── stats.rs       # Counters the engine publishes for the UI: torrents, peers, trackers
├── stats/
│   ├── torrent.rs   # One torrent: state, pieces, trackers, peer sources
│   └── peer.rs      # One peer: flags, client, bytes
├── storage.rs     # Maps pieces onto files on disk (single and multi-file)
├── storage/
│   └── verify.rs    # Hash checks of the data on disk
├── torrent.rs     # Torrent file parsing and validation
├── tracker.rs     # Tracker announce and scrape
├── tracker/
│   ├── http.rs      # HTTP trackers
│   ├── udp.rs       # UDP trackers (BEP 15)
│   └── announcer.rs # Re-announces and completed/stopped events
└── tui/           # Terminal UI
    ├── mod.rs        # Event loop, terminal setup and restore
    ├── app.rs        # Selection, tabs, sort, input line; rate history
    ├── snapshot.rs   # Plain copies of the stats, taken each tick
    ├── log_buffer.rs # Log lines kept in memory for the log pane
    ├── theme.rs      # Colors, true color or ANSI
    ├── demo.rs       # A made-up session for the demo and the view tests
    └── view/         # One file per area of the screen
examples/
└── tui_demo.rs    # The UI with a made-up session
tests/
├── dht.rs         # DHT nodes on loopback: bootstrap, announce, lookup
└── loopback/      # End-to-end runs against mock seeders, trackers and peers,
                   # one file per topic, shared helpers in common.rs
```

Unit tests live next to the code: inline in small modules, in `foo/tests.rs`
for `foo.rs` otherwise.

## How It Works

1. **Torrent Parsing** - Reads and parses .torrent files, extracting metadata and calculating info hash, or for a magnet link fetches the metadata from peers
2. **Tracker Announce, DHT and Peer Exchange** - Contacts trackers, searches the DHT and asks connected peers to discover peers in the swarm
3. **Peer Handshake** - Establishes connections with peers using the BitTorrent protocol
4. **Piece Download** - Downloads file pieces in 16KB blocks from multiple peers concurrently
5. **Verification** - Validates each piece against its SHA-1 hash before assembly
