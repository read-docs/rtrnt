
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

Options: `-o, --output <DIR>` (default `.`) and `--max-peers <N>` (default 30). Set `RUST_LOG=debug` for per-peer logs.

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
├── bencode.rs   # Raw bencode scanning (info-hash span)
├── download.rs  # Piece queue, peer workers, block pipelining
├── peer.rs      # Peer wire protocol: handshake, message encode/decode
├── storage.rs   # Maps pieces onto files on disk (single and multi-file)
├── torrent.rs   # Torrent file parsing and validation
└── tracker.rs   # Tracker communication (UDP/HTTP)
tests/
└── loopback.rs  # End-to-end download from a mock seeder
```

## How It Works

1. **Torrent Parsing** - Reads and parses .torrent files, extracting metadata and calculating info hash
2. **Tracker Announce** - Contacts trackers to discover peers in the swarm
3. **Peer Handshake** - Establishes connections with peers using the BitTorrent protocol
4. **Piece Download** - Downloads file pieces in 16KB blocks from multiple peers concurrently
5. **Verification** - Validates each piece against its SHA-1 hash before assembly
