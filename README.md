# dirwatch

[![CI](https://github.com/Cordagr/dirwatch/actions/workflows/ci.yml/badge.svg)](https://github.com/Cordagr/dirwatch/actions)
[![Release](https://img.shields.io/github/v/release/Cordagr/dirwatch)](https://github.com/Cordagr/dirwatch/releases)
![License](https://img.shields.io/badge/license-MIT-blue)

A bounded, backpressure-aware filesystem watcher for Rust.

When filesystem events arrive faster than your application can process them, dirwatch lets you choose what happens next: block, drop, coalesce, or signal overflow.

* Bounded queue with a configurable memory budget
* Five backpressure policies for different workloads
* Recursive watching with include/exclude filters
* CLI and Rust API with text and JSONL output
* Metrics snapshots to inspect queue pressure and event loss

## Why use dirwatch?

Events from the filesystem come in waves. Building, checking out using `git
checkout` or saving files by an editor can cause thousands of events to be
generated. If your consumer cannot keep pace with them, sacrifices must be made.

With the simple usage of the [notify] crate, the common way is having an
unbounded channel. No events get lost in this case, but there will be delays
in delivering events, since they get accumulated in memory in large amounts.
As seen in the [benchmark below](#benchmarks), the slow consumer got lagged
for 2.7 seconds with 4,765 paths accumulated in the queue, while dirwatch
managed to stay 16 events behind with 8 ms median latency.

The solution to that would be the usage of bounded channel. However, this would
cause other issues:

- Blocking the callback can cause stalling of the operating system’s event
  reader, which in turn can overflow its buffer and lose events without any
  notice.
- Dropping events means choosing the events to drop and informing the consumer
  about that.
- Repeated writing to the file results in the generation of many redundant
  events.

dirwatch allows a user to define a budget of memory and choose a policy
(`block`, `drop-newest`, `drop-oldest`, `coalesce`, or `overflow-signal`)
for what to do when the queue is full. The metric snapshots will give you
an idea of how much pressure the queue takes and how many events it drops.

**Use dirwatch when** a spike of changes exceeds the processing capacity of
the consumer: build tools, sync daemons, log shippers, dev servers,
indexers.

**Use plain `notify` when** the consumer processes all events right away and
needs every single event as it was received from the OS.

## Use the library

Create a watcher, add a directory, and consume events:

```rust,no_run
use dirwatch::Watcher;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut watcher = Watcher::new()?;
    watcher.watch(r"C:\work\my-project")?;

    for event in watcher.iter() {
        let event = event?;
        println!("{:?}: {:?}", event.kind, event.path);
    }
    Ok(())
}
```

`Watcher::new()` uses the default `EngineConfig`; use
`Watcher::with_config(config)` to customize it. Directories can be managed with
`watch` and `unwatch`. Read events with `iter`, `next`, or `next_timeout`, or use
`EventStream::try_next` to poll without blocking. `metrics()` returns a snapshot.

## Use the command line

Install the CLI from crates.io and start watching a directory:

```powershell
cargo install dirwatch
dirwatch watch C:\Users\me\Documents
```

To install from a local checkout instead, run `cargo install --path .` from
the repository root.

### Examples

While working on a Rust project, watch its source tree for edits:

```powershell
dirwatch watch src --include "**/*.rs" --event modified
```

To keep a JSONL log of a service's changes without recording generated build
files, write the log outside the watched directory:

```powershell
dirwatch watch C:\work\service --exclude "target/**" --format json --output C:\temp\service-events.jsonl
```

Before starting a watch, check that a queue configuration is valid:

```powershell
dirwatch validate --queue-capacity-bytes 1048576 --backpressure drop-oldest
```

### Flags

`watch PATH...` takes one or more directories and watches recursively by
default. `validate` accepts the queue and watcher configuration flags.

- `--non-recursive` watches only the supplied directories.
- `--event KIND` can be repeated to select `created`, `modified`, `removed`,
  `renamed`, or `overflow`. All event kinds are included by default.
- `--include GLOB` and `--exclude GLOB` can be repeated. Exclusions take
  precedence; overflow notifications bypass event and path filters.
- `--format text|json` selects tab-separated text (the default) or JSON Lines.
  `--output FILE` writes events to a file instead of standard output.
- `--queue-capacity-bytes N` sets the event queue budget (default: `4194304`
  bytes). `--backpressure POLICY` accepts `block`, `drop-newest`, `drop-oldest`,
  `coalesce` (the default), or `overflow-signal`.
- `--coalesce-window-ms N` sets the coalescing window (default: `50`), and
  `--coalesce-max-entries N` sets its entry limit (default: `4096`).
- `--max-watches N` limits watched directories (default: `1024`).
  `--shutdown-timeout-secs N` sets the shutdown timeout (default: `20`).
- `--max-events N` and `--timeout-secs N` stop after an event count or elapsed
  time. Both are unlimited by default.
- `--summary` prints a JSON metrics snapshot to standard error when the run
  ends.

Run `dirwatch --help` or `dirwatch watch --help` for the complete option list.

## Benchmarks

The release-mode comparison rewrites the same 16 files 250 times (4,000
writes), with a 200 us delay per delivered event. It runs five paired trials,
alternating backend order. The `notify` baseline uses an unbounded channel;
dirwatch uses its default 4 MiB bounded queue and 50 ms coalescing window.

With a deliberately slow consumer, plain notify fell about 2.7 seconds behind and queued 4,765 pending paths. 
dirwatch stayed at 16 queued events and ~8 ms median latency by coalescing repeated writes to the same files.

Median results on Windows:

| Metric | dirwatch | plain `notify` |
| --- | ---: | ---: |
| Input paths received | 8,000 | 8,000 |
| Events delivered | 3,200 | 8,000 |
| Events coalesced | 4,801 | 0 |
| Events dropped / overflow signals | 0 / 0 | 0 / 0 |
| Peak pending items | 16 queue events | 4,765 paths |
| Peak queue bytes | 2,928 bytes | Not bounded / not measured |
| Approx. p50 latency | 8.39 ms | 2,684.35 ms |
| Approx. p95 latency | 18.87 ms | 3,221.23 ms |
| Input rate | 2,153 paths/s | 1,239 paths/s |

This workload emphasizes repeated changes to the same paths. dirwatch coalesces
events instead of delivering every raw notification; it kept its queue bounded
while the deliberately slowed consumer fell behind. The `notify` baseline
preserves all notifications but accumulates pending paths in an unbounded
channel. Queue peak counts are not directly equivalent: dirwatch reports exact
queue occupancy, while the baseline counts pending event paths. Latency is
queue-enqueue-to-delivery for dirwatch and callback-to-consumer for `notify`.
Percentiles are approximate upper bounds from a fixed-size logarithmic
histogram. Results are a local workload comparison, not a cross-platform or
general-purpose throughput claim.

Run the benchmark on your machine with:

```powershell
cargo bench --bench watcher_compare
```

## Development

Run `cargo fmt --all -- --check`, `cargo test --all-features`, and
`cargo clippy --all-targets --all-features -- -D warnings` before submitting a
change. Pushing a version tag such as `v0.2.0` builds release archives for
Windows x86-64, Linux x86-64, and macOS Apple Silicon.
