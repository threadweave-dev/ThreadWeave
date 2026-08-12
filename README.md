# ThreadWeave

🇬🇧 English | 🇫🇷 [Français](README.fr.md)

> **A resource-oriented distributed execution engine for modern workloads.**
>
> Build distributed task systems without coupling your infrastructure to a programming language.

ThreadWeave is an open-source distributed execution platform built around a simple idea:

**The execution engine should not care which language your code is written in.**

Instead of being another Python task queue, ThreadWeave provides a language-agnostic execution engine responsible for scheduling, resource management, fault recovery, retries, observability, and distributed execution.

Application code stays in your favorite language.

The infrastructure stays in Rust.

---

## Why ThreadWeave?

Modern distributed systems need more than a task queue.

They need:

* Resource-aware scheduling (CPU, memory, GPU…)
* Fault tolerance and recovery
* Long-running workloads
* Observability by design
* Horizontal scalability
* Multi-language support

ThreadWeave provides these capabilities through a generic execution protocol and pluggable runtimes.

---

## Core Principles

ThreadWeave is built around a few fundamental ideas:

* 🌍 **Language Agnostic First** — the core never depends on a programming language.
* ⚙️ **Engine Orchestrates, Runtimes Execute** — execution is delegated to language runtimes.
* 🧠 **Resources Are First-Class Citizens** — tasks request resources, not workers.
* 📡 **Everything Is Observable** — every state transition produces events.
* 🔌 **Plugin-First Architecture** — brokers, schedulers, storage backends and runtimes are replaceable.
* 🦀 **Rust at the Core** — reliability, performance and safety where they matter most.

---

## Architecture

```text
             +----------------------+
             |    Rust Core Engine  |
             +----------------------+
                      |
      +---------------+---------------+
      |               |               |
  Python Runtime   JS Runtime    Java Runtime
      |               |               |
  User Tasks      User Tasks      User Tasks
```

The Rust engine is responsible for:

* Scheduling
* Resource allocation
* Distributed coordination
* Retries
* Timeouts
* Monitoring
* Fault recovery

Language runtimes are responsible only for executing user code.

---

## Current Status

ThreadWeave is currently in active development.

### Redis submission POC

The Rust core currently implements the first submission boundary only:

1. `SubmitTask` receives a protobuf command over gRPC.
2. The core wraps it in a versioned `BrokerEnvelope`.
3. The envelope is appended to the `threadweave:broker:tasks` Redis list.
4. The job is returned as `ACCEPTED` only after Redis confirms the write.

Build the optimized image and start the complete stack with:

```bash
export BUF_TOKEN="<your-buf-token>"
docker compose up --build -d
```

The token is used only as a BuildKit secret to download the generated Rust
protocol crates from Buf; it is not stored in the image. For local Cargo
commands, authenticate once with
`cargo login --registry buf "Bearer <your-buf-token>"`.

The gRPC service is then available at `localhost:50051`. Override the published
port without rebuilding with, for example,
`THREADWEAVE_PORT=6000 docker compose up --build -d`. Use
`docker compose logs -f threadweave` to follow the engine and
`docker compose down` to stop the stack (add `-v` to also remove Redis data).

The image uses a multi-stage build: the Rust toolchain stays in the build stage
and the final static image contains only the optimized binary and its
configuration. It runs unprivileged with a read-only filesystem.

For local development outside Docker, start Redis only and then the core:

```bash
docker compose up -d redis
cargo run
```

By default, the core loads `threadweave.yaml`. Another configuration file can
be selected from the CLI:

```bash
cargo run -- --config /path/to/threadweave.yaml
```

The YAML file configures the gRPC bind address, Redis URL, broker key prefix,
and task destination. See `threadweave.yaml` for the default values.

The broker is behind the `Broker` trait, and result storage has a separate
`BackendResult` trait. Scheduling, consumption and result persistence are not
implemented yet.

The project is focused on building a solid architecture before implementing production features.

Current priorities include:

* Documentation
* RFCs
* Core architecture
* Rust execution engine
* Python runtime

---

## Roadmap

* ✅ Project vision
* ✅ RFC process
* 🚧 Rust core
* 🚧 Documentation website
* ⏳ Python runtime
* ⏳ Distributed scheduler
* ⏳ Resource manager
* ⏳ JavaScript runtime
* ⏳ Java runtime
* ⏳ Web dashboard

---

## Open Source

ThreadWeave is developed in the open.

We welcome discussions, RFCs, ideas, bug reports and contributions from the community.

Whether you want to build a runtime, a scheduler, a storage backend or developer tooling, the architecture is designed to make it possible.

---

## Documentation

Documentation is being written before implementation.

This repository contains the source code.

The complete documentation, RFCs and design documents are available in the documentation website.

---

## Publishing a release

Add a crates.io API token to the repository's GitHub Actions secrets under the
name `CRATES_IO_TOKEN`. GitHub Actions publishes the crate to crates.io and
prebuilt Linux, macOS and Windows binaries when a semantic-version tag is
pushed. The tag must match the version in `Cargo.toml`.

```bash
git tag v0.1.0
git push origin v0.1.0
```

The workflow publishes the crate first, then creates the corresponding GitHub
Release and generates its release notes automatically.

---

## License

Licensed under the Apache License 2.0.

---

## Vision

We believe developers should choose a programming language because it fits their application—not because it dictates their distributed infrastructure.

Our long-term goal is simple:

**One execution engine. Any language. Any workload.**
