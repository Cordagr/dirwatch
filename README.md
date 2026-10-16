 ![SomeRabbit logo](src/assets/SomeRabbit-export.png)

 # dirwatch

dirwatch watches directories and reports file creations, edits, removals,
renames, and overflow. It uses the `notify` crate for native notifications;
Windows uses `ReadDirectoryChangesW`.

If events arrive faster than they can be handled, the bounded queue can block,
drop, or coalesce them. A metrics snapshot shows what happened during a run.

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

Install the CLI from this checkout and start watching a directory:

```powershell
cargo install --path .
dirwatch watch C:\Users\me\Documents
```

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

## Development

Run `cargo fmt --all -- --check`, `cargo test --all-features`, and
`cargo clippy --all-targets --all-features -- -D warnings` before submitting a
change. Pushing a version tag such as `v0.2.0` builds release archives for
Windows x86-64, Linux x86-64, and macOS Apple Silicon.
