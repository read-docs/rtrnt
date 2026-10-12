
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

Options: `-o, --output <DIR>` (default `.`), `--max-peers <N>` (default 30), `-p, --port <PORT>` to listen on for incoming peers (default 6881) and `--check` to verify a finished download. Set `RUST_LOG=debug` for per-peer logs.

### Config file

The first three options can also be set in `~/.config/rtrnt/config.toml` (or `$XDG_CONFIG_HOME/rtrnt/config.toml`), or in a file given with `--config <PATH>`. Keys are the long option names, and every key is optional:

```toml
output = "~/Downloads"
max-peers = 50
port = 51413
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
├── config.rs    # Config file loading and CLI > file > default precedence
├── download.rs  # Piece queue, peer workers, block pipelining
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
