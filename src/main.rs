use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use dirwatch::{Event, EventKind, Watcher};

mod args;
mod filters;

use args::{build_config, Cli, Command, OutputFormat, WatchArgs};
use filters::{event_matches, PathFilters};

fn main() {
    if let Err(error) = run() {
        eprintln!("dirwatch: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    match Cli::parse().command {
        Command::Watch(args) => run_watch(args),
        Command::Validate(args) => {
            build_config(&args)?;
            println!("configuration is valid");
            Ok(())
        }
    }
}

fn run_watch(args: WatchArgs) -> Result<(), Box<dyn Error>> {
    let config = build_config(&args.config)?;
    let filters = PathFilters::new(&args.include, &args.exclude)?;
    let mut watcher = Watcher::with_config(config)?;
    for path in &args.paths {
        watcher.watch(path)?;
    }
    let interrupted = Arc::new(AtomicBool::new(false));
    let signal_flag = Arc::clone(&interrupted);
    ctrlc::set_handler(move || signal_flag.store(true, Ordering::Relaxed))?;

    let output: Box<dyn Write> = match args.output.as_deref() {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(std::io::stdout()),
    };
    let mut output = BufWriter::new(output);
    let start = Instant::now();
    let timeout = args.timeout_secs.map(Duration::from_secs);
    let mut emitted = 0u64;

    loop {
        if interrupted.load(Ordering::Relaxed) {
            break;
        }
        if timeout.is_some_and(|limit| start.elapsed() >= limit) {
            break;
        }
        if args.max_events.is_some_and(|limit| emitted >= limit) {
            break;
        }

        let poll_interval = Duration::from_millis(20);
        let wait = timeout
            .map(|limit| poll_interval.min(limit.saturating_sub(start.elapsed())))
            .unwrap_or(poll_interval);
        if let Some(event) = watcher.next_timeout(wait)? {
            if event_matches(&event, &args.events) && filters.matches(&event) {
                write_event(&mut output, args.format, &event)?;
                emitted += 1;
            }
        }
    }

    output.flush()?;
    watcher.shutdown()?;
    let shutdown_deadline =
        Instant::now().checked_add(Duration::from_secs(args.config.shutdown_timeout_secs));
    while emitted < args.max_events.unwrap_or(u64::MAX) {
        if shutdown_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            break;
        }
        let Some(event) = watcher.try_next()? else {
            break;
        };
        if event_matches(&event, &args.events) && filters.matches(&event) {
            write_event(&mut output, args.format, &event)?;
            emitted += 1;
        }
    }
    output.flush()?;
    let summary = watcher.metrics();
    if args.summary {
        eprintln!("{}", serde_json::to_string(&summary)?);
    }
    Ok(())
}

fn write_event(
    output: &mut impl Write,
    format: OutputFormat,
    event: &Event,
) -> Result<(), Box<dyn Error>> {
    match format {
        OutputFormat::Text => {
            let path = event
                .path
                .as_deref()
                .map(Path::display)
                .map_or_else(|| String::from("-"), |path| path.to_string());
            writeln!(output, "{}\t{path}", event_kind_name(event.kind))?;
        }
        OutputFormat::Json => {
            serde_json::to_writer(&mut *output, event)?;
            output.write_all(b"\n")?;
        }
    }
    Ok(())
}

fn event_kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::Created => "created",
        EventKind::Modified => "modified",
        EventKind::Removed => "removed",
        EventKind::Renamed => "renamed",
        EventKind::Overflow => "overflow",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_output_is_one_serialized_event() {
        let event = Event::new(EventKind::Created, "file.txt");
        let mut output = Vec::new();
        write_event(&mut output, OutputFormat::Json, &event).unwrap();

        let value: serde_json::Value =
            serde_json::from_slice(output.strip_suffix(b"\n").unwrap()).unwrap();
        assert_eq!(value["kind"], "created");
        assert_eq!(value["path"], "file.txt");
    }
}
