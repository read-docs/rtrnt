
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
```

A magnet link works wherever a .torrent file does. The torrent's metadata is fetched from peers found through the link's trackers (`tr`), its peers (`x.pe`) and the DHT, then the download carries on with the same peers. The metadata is saved as `<info hash>.torrent` in the output directory, so running the same link again resumes without fetching it, and `--check` uses that file (it fails without it). `--scrape` asks the link's trackers.

Options: `-o, --output <DIR>` (default `.`), `--max-peers <N>` (default 30), `-p, --port <PORT>` to listen on for incoming peers over IPv4 and IPv6 (default 6881), `--no-dht` to find peers through trackers only (`--dht` turns the DHT back on if the config file turned it off), `--check` to verify a finished download (for a magnet link, against the saved `.torrent`), `--scrape` to print each tracker's seeder, leecher and completed counts without downloading (a magnet link's `tr` trackers), `--seed-ratio <RATIO>` and `--seed-time <MINUTES>` to keep seeding once the download is complete, until we have uploaded that many times the torrent's size or for that long, whichever comes first (both default to 0, which exits at completion), and `--max-download-rate <KIB/S>` and `--max-upload-rate <KIB/S>` to limit how fast piece data moves each way, in KiB per second across all peers (both default to 0, unlimited). The limits count piece data only, not protocol messages or TCP/IP headers, so the traffic on the wire is slightly higher. Without `--no-dht`, peers are also looked up in the BitTorrent DHT (on UDP, same port), so torrents without trackers download too. Connected peers that support peer exchange also tell us about more peers, and we tell them about ours (never for private torrents). Set `RUST_LOG=debug` for per-peer logs.

### Config file

Every option except `--check`, `--scrape`, `--config` and `--print-config` can also be set in `~/.config/rtrnt/config.toml` (or `$XDG_CONFIG_HOME/rtrnt/config.toml`), or in a file given with `--config <PATH>`. Keys are the long option names, except that `--no-dht` is `dht = false`, and every key is optional:

```toml
output = "~/Downloads"
max-peers = 50
port = 51413
seed-ratio = 1.5
seed-time = 120
dht = false
max-download-rate = 2048
max-upload-rate = 256
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
- **anyhow** - Error handling

## Project Structure

```
src/
├── main.rs      # CLI entry point (clap + tracing)
├── lib.rs       # Library root
├── announcer.rs # Re-announces and completed/stopped events
├── bencode.rs   # Raw bencode scanning (info-hash span)
├── choker.rs    # Choking algorithm: who may download from us
├── config.rs    # Config file loading and CLI > file > default precedence
├── dht/         # Trackerless peer discovery (BEP 5)
│   ├── mod.rs     # DHT node: UDP socket, queries and answers, lookups
│   ├── krpc.rs    # KRPC messages, bencode values, compact node/peer info
│   ├── routing.rs # Node ids, XOR distance, k-bucket routing table
│   ├── lookup.rs  # Iterative lookup state machine
│   ├── token.rs   # get_peers tokens with rotating secrets
│   ├── store.rs   # Peers announced to us
│   └── search.rs  # Periodic lookups for the torrent being downloaded
├── download.rs  # Piece queue, peer workers, block pipelining, seeding
├── extension.rs # Extension protocol (BEP 10) handshake
├── fast.rs      # Fast extension (BEP 6): allowed fast set, Have All/Have None
├── magnet.rs    # Magnet links (BEP 9): parsing, metadata fetch, then download
├── metadata.rs  # ut_metadata messages, assembling and serving the info dict
├── peer.rs      # Peer wire protocol: handshake, message encode/decode
├── peer_pool.rs # Known peers, retry backoff and bans
├── pex.rs       # Peer exchange (BEP 11): telling peers about each other
├── rate_limit.rs # Upload and download rate limits (shared token buckets)
├── storage.rs   # Maps pieces onto files on disk (single and multi-file)
├── torrent.rs   # Torrent file parsing and validation
└── tracker.rs   # Tracker communication (UDP/HTTP)
tests/
├── dht.rs       # DHT nodes on loopback: bootstrap, announce, lookup
└── loopback.rs  # End-to-end downloads from mock seeders, a mock tracker, the DHT, peer exchange and magnet links
```

## How It Works

1. **Torrent Parsing** - Reads and parses .torrent files, extracting metadata and calculating info hash, or for a magnet link fetches the metadata from peers
2. **Tracker Announce, DHT and Peer Exchange** - Contacts trackers, searches the DHT and asks connected peers to discover peers in the swarm
3. **Peer Handshake** - Establishes connections with peers using the BitTorrent protocol
4. **Piece Download** - Downloads file pieces in 16KB blocks from multiple peers concurrently
5. **Verification** - Validates each piece against its SHA-1 hash before assembly
