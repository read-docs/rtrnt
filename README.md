
# rtrnt

A terminal-based BitTorrent client written in Rust.

## Overview

**rtrnt** is a lightweight BitTorrent client implementation built with Rust, designed to download files from torrent networks through the terminal. It supports both HTTP and UDP tracker protocols and implements the core BitTorrent peer protocol for efficient file downloads.

## Features

- 🚀 **Async/Await Architecture** - Built with Tokio for high-performance concurrent downloads
- 🌐 **Multi-Protocol Support** - Compatible with both UDP and HTTP trackers
- 🔗 **Peer Protocol Implementation** - Full BitTorrent peer wire protocol support
- ⚡ **Parallel Downloads** - Concurrent piece downloading from multiple peers
- 🔒 **Data Integrity** - SHA-1 hash verification for downloaded pieces
- 📦 **Bencode Support** - Native torrent file parsing

## Usage

```sh
cargo run --release -- path/to/file.torrent -o ~/Downloads
```

Options: `-o, --output <DIR>` (default `.`), `--max-peers <N>` (default 30), `-p, --port <PORT>` to listen on for incoming peers (default 6881), `--check` to verify a finished download, and `--seed-ratio <RATIO>` and `--seed-time <MINUTES>` to keep seeding once the download is complete, until we have uploaded that many times the torrent's size or for that long, whichever comes first (both default to 0, which exits at completion). Set `RUST_LOG=debug` for per-peer logs.

Run the tests with `cargo test`. They include a loopback integration test against a mock seeder.

See [ROADMAP.md](ROADMAP.md) for what's done and what's next.

## Dependencies

- **tokio** - Async runtime
- **serde** & **serde_bencode** - Torrent file parsing
- **sha1** - Piece hash verification
- **reqwest** - HTTP tracker communication
- **clap** - Command-line interface
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
├── download.rs  # Piece queue, peer workers, block pipelining, seeding
├── peer.rs      # Peer wire protocol: handshake, message encode/decode
├── peer_pool.rs # Known peers, retry backoff and bans
├── storage.rs   # Maps pieces onto files on disk (single and multi-file)
├── torrent.rs   # Torrent file parsing and validation
└── tracker.rs   # Tracker communication (UDP/HTTP)
tests/
└── loopback.rs  # End-to-end downloads from mock seeders and a mock tracker
```

## How It Works

1. **Torrent Parsing** - Reads and parses .torrent files, extracting metadata and calculating info hash
2. **Tracker Announce** - Contacts trackers to discover peers in the swarm
3. **Peer Handshake** - Establishes connections with peers using the BitTorrent protocol
4. **Piece Download** - Downloads file pieces in 16KB blocks from multiple peers concurrently
5. **Verification** - Validates each piece against its SHA-1 hash before assembly
