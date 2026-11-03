# rtrnt

A BitTorrent client for the terminal, written in Rust. Run `rtrnt`, press `a`, paste a magnet link.

```
 rtrnt │ ↓ 6.6 MiB/s  ↑ 1.1 MiB/s (2 MiB/s) │ 5 torrents  71/200 peers  DHT 312 nodes         14:02
╭ Torrents ───────────────────────────────────────────────────────────────────────── sort: added ▾ ╮
│ Name                           Size Progress        Status               ↓          ↑ Peers  ETA │
│▌debian-13.7.0-amd64-netin…  754 MiB ██████▎───  61% downloading  4.2 MiB/s  367 KiB/s    38   1m │
│ archlinux-2026.10.01-x86_…  1.2 GiB ██████████ 100% seeding              —  706 KiB/s    12    ∞ │
│ ubuntu-26.04-desktop-amd6…  6.1 GiB █▎────────  11% downloading  2.4 MiB/s  103 KiB/s    21  38m │
│ Big Buck Bunny                    — ████▎─────  42% metadata             —          —     4    — │
│ fedora-Workstation-Live-4…  2.4 GiB ███▊──────  36% checking             —          —     0    — │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
╭ 1 Peers ─ 2 Pieces ─ 3 Trackers ─ 4 Files ─ 5 Info ───────────── debian-13.7.0-amd64-netinst.iso ╮
│ Address                  Client                  Flags              ↓          ↑  Has            │
│ 185.21.216.143:51413     Transmission 4.0.6      D U   F X  1.1 MiB/s   48 KiB/s  ██████████ seed│
│ [2a01:4f8:c17::9]:6881   qBittorrent 5.1.2       D u   F X  812 KiB/s      0 B/s  ██████████ seed│
│ 91.121.44.7:6881         libtorrent 2.0.11       D U   F    640 KiB/s  112 KiB/s  ████████▎─  82%│
│ 46.4.88.120:51413        Transmission 4.1.0      D u   F X  511 KiB/s      0 B/s  ██████████ seed│
│ 5.39.71.22:6881          rtrnt 0.1.0             D U   F    402 KiB/s   54 KiB/s  ██████▋───  66%│
│ 178.62.3.98:55555        BitComet 2.10           D          298 KiB/s      0 B/s  ██████████ seed│
│ [2001:db8:5::1]:51413    Transmission 3.00       D U   F X  201 KiB/s      0 B/s  ██████████ seed│
│ 73.92.10.211:42069       Deluge 2.2.0            d U I F        0 B/s   96 KiB/s  ████▏─────  41%│
│ 82.64.211.5:6881         qBittorrent 4.6.7       d     F        0 B/s      0 B/s  █▎────────  12%│
│ 9 peers  ·  D/d down unchoked/interested  U/u up  I incoming  F fast  X pex                      │
│                                                                                                  │
│ ↓ ▇▇▇▇▇▆▆▆▅▅▅▆▆▆▇▇▇▇▇▇▇▇▇▇█████████▇▇▇▆▆▆▆▇▇▇▆▆▆▆▆▆▆▆▆▇▇▇██████▇▇███   4.2 MiB/s  peak 5.2 MiB/s │
│ ↑ ▅▅▅▅▅▅▆▆▆▇▇▇▇▇▇▇▇▇▇███████▇▇▇▆▆▆▆▆▆▆▆▆▆▅▅▅▅▅▅▆▆▇▇▇██▇▇▇▇▇▇██████▇▇   367 KiB/s  peak 402 KiB/s │
╰──────────────────────────────────────────────────────────────────────────────────────────────────╯
 ↑↓ select  tab panel  1-5 view  l log  s sort  / filter  ? help  q quit  p pause  x remove
```

(`cargo run --example tui_demo` shows this screen with a made-up session.)

## Install

```sh
cargo install --git https://github.com/read-docs/rtrnt
```

Or from a clone: `cargo install --path .`

## Use it

```sh
rtrnt                          # open the UI, empty; press a to add a torrent
rtrnt file.torrent 'magnet:?xt=urn:btih:...' -o ~/Downloads
```

The UI remembers its torrents between runs, so the next `rtrnt` picks up where you quit. `q` stops everything cleanly and saves progress; nothing is hashed again on the next start.

| Key | Action |
|-----|--------|
| `a` | add a `.torrent` file or a magnet link |
| `p` | pause or resume the selected torrent |
| `x` | remove it (`y` keeps the files, `D` deletes them) |
| `↑`/`↓`, `j`/`k` | select a torrent |
| `tab`, `1`–`5` | focus the detail panel; peers, pieces, trackers, files, info |
| `space`, `+`/`-` | on the Files tab: skip a file, raise or lower its priority |
| `o` | download in order, for watching while it downloads |
| `d` / `u` | session download / upload limit (`D` / `U` for the selected torrent) |
| `t` | alternative speed limits ("turtle mode") on or off |
| `/`, `s` | filter by name, change the sort |
| `?` | every key |
| `q` | quit |

The full list, with what each key does to a running, paused or queued torrent, is in [docs/tui.md](docs/tui.md).

## Features

- **Finding peers:** HTTP and UDP trackers, the DHT (BEP 5), peer exchange (BEP 11), magnet links (BEP 9) and web seeds (BEP 19 and 17)
- **Peer protocol:** extension protocol (BEP 10), fast extension (BEP 6), IPv4 and IPv6
- **Choosing what to download:** skip files, give files priority, or download in order (first and last pieces of each file first)
- **Many torrents at once:** a queue with active download and seed limits that you reorder by hand (`K` / `J`)
- **Speed limits:** session-wide and per torrent, plus alternative limits switched by a weekly schedule
- **Seeding:** stop at a ratio or after some time, session-wide or per torrent
- **Resume and session files:** quitting and starting again does not rehash, and the torrent list comes back
- **Networking:** router port mapping (PCP, NAT-PMP, UPnP IGD) and an IP blocklist (PeerGuardian, eMule, CIDR)
- **Automation:** a watch directory for `.torrent` and `.magnet` files, and a command to run when a torrent completes
- **Config file:** every setting can live in `~/.config/rtrnt/config.toml`

## Without the UI

When stdout isn't a terminal, or with `--no-tui`, rtrnt prints progress bars and log lines, downloads what it was given and exits. Combined with `--watch-dir` it keeps running until Ctrl-C, which works as a basic headless seedbox:

```sh
rtrnt --no-tui --watch-dir ~/torrents -o ~/Downloads --seed-ratio 2
```

There's no remote control yet (no RPC or web UI). To keep the UI running after you log out, start it inside `tmux` or `screen`.

`--check` verifies a finished download, and `--scrape` asks the trackers for seeder and leecher counts without downloading.

## Documentation

- [Terminal UI](docs/tui.md): every key, pause, recheck, file selection, sequential download, removal, the mouse, peer flags
- [Usage and options](docs/usage.md): command-line flags, several torrents, magnet links, resume files
- [Queue and session](docs/queue-and-session.md): active limits, queue order, what the session file keeps
- [Config file](docs/config.md): every key, alternative speed limits and their schedule
- [Watch directory and completion hook](docs/automation.md)
- [Networking](docs/networking.md): IP blocklist, port mapping
- [Development](docs/development.md): tests, dependencies, source layout
- [ROADMAP.md](ROADMAP.md): what's done and what's next
