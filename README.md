# Torx

[![Rust](https://img.shields.io/badge/Rust-000000?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub Release](https://img.shields.io/github/v/release/flippedpants/Torx?include_prereleases&label=Release)](https://github.com/flippedpants/Torx/releases)

## Overview

**Torx** is a fast, concurrent BitTorrent client written in Rust with a real-time terminal user interface. It implements the full BitTorrent protocol — peer discovery via HTTP and UDP trackers, rarest-first piece selection, SHA-1 verification, multi-file downloads, and a Tit-for-Tat seeding engine — all driven by an async Tokio runtime.

Built for speed and transparency, Torx renders a live TUI (powered by Ratatui + Crossterm) that displays download progress, peer statistics, file listings, a color-coded piece map, and a scrollable event logger. Whether you're downloading a single ISO or a multi-gigabyte torrent with hundreds of files, Torx handles it with minimal overhead and maximum visibility.

### Key Features

- **Concurrent peer connections** — 50 async worker tasks for saturated bandwidth
- **Rarest-first piece selection** — minimizes download stalls across the swarm
- **Tit-for-Tat choking engine** — fair upload strategy with optimistic unchoking
- **HTTP + UDP tracker support** — full announce/announce-list discovery
- **Multi-file torrent support** — automatic file mapping and preallocation
- **Block request pipelining** — 50 in-flight blocks per peer for max throughput
- **Interactive TUI** — real-time stats, piece map, peer table, file list, and logger
- **Cross-platform** — builds for Linux, macOS, and Windows

<img width="959" height="573" alt="Screenshot 2026-07-22 201524" src="https://github.com/user-attachments/assets/cb3a1baa-d763-44b8-810b-1ff17098d4ad" />

## Installation

### Linux & macOS

#### Homebrew

First, add the Torx tap:

```bash
brew tap flippedpants/torx
brew trust flippedpants/torx
```

Then install:

```bash
brew install torx
```

Or do both in a single command:

```bash
brew install flippedpants/torx/torx
```

### Windows

#### Scoop
If you dont have scoop package manager, run these commmands on your terminal - 
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression
```

Add the bucket - 
```powershell
scoop bucket add torx https://github.com/flippedpants/scoop-bucket
```

Then install - 
```powershell
scoop install torx
```


### Build from Source

Requires [Rust](https://www.rust-lang.org/tools/install) (stable toolchain).

```bash
git clone https://github.com/flippedpants/Torx.git
cd Torx
cargo build --release
```

The binary will be at `target/release/torx`.

## Usage

Launch Torx — the interactive TUI will guide you through setup:

```bash
torx
```

1. **Setup tab** — enter the path to your `.torrent` file and download directory
2. **Download** begins automatically after setup
3. **Navigate tabs** with arrow keys to view stats, peers, files, piece map, and logs

Check version:

```bash
torx --version
```

## TUI Tabs

| Tab | Description |
|-----|-------------|
| **Trackers** | List of all the trackers available|
| **Overview** | Real-time download/upload speed, progress, and ETA|
| **Peers** | Connected peer table with per-peer transfer rates  |
| **Files** | Multi-file listing with individual progress|
| **Logger** | Scrollable event log for debugging |

## Repository Structure

```
torx/
├── src/
│   ├── main.rs             # Entry point — orchestrates setup, spawns tasks
│   ├── cli.rs              # Clap-based CLI argument parsing
│   ├── parser.rs           # Bencode torrent metadata deserialization
│   ├── build_request.rs    # Info-hash calculation, HTTP/UDP tracker protocols
│   ├── response.rs         # HTTP tracker response parsing
│   ├── download.rs         # Worker pool, TCP handshakes, block pipelining
│   ├── upload.rs           # Tit-for-Tat choking engine, inbound peer listener
│   ├── peer.rs             # Peer state machine, rarest-first selection
│   ├── piece.rs            # Block accumulation, SHA-1 verification
│   ├── storage.rs          # Multi-file disk mapping and async writes
│   ├── logger.rs           # Debug log writer (torx_debug.log)
│   └── ui/
│       └── mod.rs          # Ratatui TUI 
├── scripts/
│   ├── install.sh          # Linux/macOS installer
│   └── install.ps1         # Windows PowerShell installer
├── .github/
│   └── workflows/
│       └── release.yml     # CI: cross-compile & publish GitHub Releases
├── Cargo.toml              # Dependencies and release profile
├── LICENSE                  # MIT License
└── README.md               # This file
```

## Reporting Issues

Found a bug or have a suggestion? Open an issue:

[GitHub Issues](https://github.com/flippedpants/Torx/issues)

When reporting, please include:

- Description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Torrent file details (single/multi-file, size)
- OS and architecture

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the [MIT License](LICENSE).

Copyright (c) 2026 Daksh Gupta.

## Acknowledgements

- **[Rust](https://www.rust-lang.org/)** — systems programming language
- **[Tokio](https://tokio.rs/)** — async runtime for Rust
- **[Ratatui](https://ratatui.rs/)** — terminal UI framework
- **[Crossterm](https://github.com/crossterm-rs/crossterm)** — cross-platform terminal manipulation
- **[Clap](https://docs.rs/clap)** — command-line argument parser
- **[Reqwest](https://docs.rs/reqwest)** — HTTP client
- **[serde_bencode](https://docs.rs/serde_bencode)** — Bencode serialization
