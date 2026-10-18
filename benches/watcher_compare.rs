use std::error::Error;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, TryRecvError};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use dirwatch::metrics::Metrics;
use dirwatch::{BackpressureOptions, EngineConfig, Watcher as Dirwatch};
use notify::event::EventKind as NotifyEventKind;
use notify::{Event as NotifyEvent, RecursiveMode, Watcher as NotifyWatcher};
use tempfile::TempDir;

const TRIALS: usize = 5;
const FILES: usize = 16;
const ROUNDS: usize = 250;
const CONSUMER_DELAY: Duration = Duration::from_micros(200);
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const QUIET_PERIOD: Duration = Duration::from_millis(250);
const QUEUE_CAPACITY_BYTES: usize = 4 * 1024 * 1024;

struct Trial {
    backend: &'static str,
    trial: usize,
    writes: u64,
    received: u64,
    delivered: u64,
    dropped: u64,
    overflows: u64,
    coalesced: u64,
    backend_errors: u64,
    queue_peak_events: u64,
    queue_peak_bytes: Option<u64>,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    elapsed: Duration,
}

fn main() -> Result<(), Box<dyn Error>> {
    println!("backend,trial,writes,received,delivered,dropped,overflows,coalesced,backend_errors,peak_queue_events,peak_queue_bytes,p50_us,p95_us,elapsed_ms,received_per_second");
    let mut results = Vec::with_capacity(TRIALS * 2);

    for trial in 1..=TRIALS {
        if trial % 2 == 0 {
            results.push(run_plain_notify(trial)?);
            results.push(run_dirwatch(trial)?);
        } else {
            results.push(run_dirwatch(trial)?);
            results.push(run_plain_notify(trial)?);
        }
    }

    for result in &results {
        print_trial(result);
    }
    print_median("dirwatch", &results);
    print_median("notify", &results);
    Ok(())
}

fn run_dirwatch(trial: usize) -> Result<Trial, Box<dyn Error>> {
    let directory = TempDir::new()?;
    let paths = prepare_files(directory.path())?;
    let config = EngineConfig::builder()
        .queue_capacity_bytes(QUEUE_CAPACITY_BYTES)
        .backpressure(BackpressureOptions::Coalesce)
        .recursive(false)
        .build()?;
    let mut watcher = Dirwatch::with_config(config)?;
    watcher.watch(directory.path())?;

    let (done_sender, done_receiver) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        let _ = done_sender.send(write_workload(&paths));
    });

    let started = Instant::now();
    barrier.wait();
    let mut writes = None;
    let mut delivered = 0u64;
    let mut last_activity = started;

    loop {
        if watcher.next_timeout(POLL_INTERVAL)?.is_some() {
            delivered += 1;
            last_activity = Instant::now();
            thread::sleep(CONSUMER_DELAY);
        }
        poll_writer(&done_receiver, &mut writes, &mut last_activity)?;
        if writes.is_some() && last_activity.elapsed() >= QUIET_PERIOD {
            break;
        }
    }

    let writes = writes.ok_or_else(|| io::Error::other("writer did not finish"))?;
    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))?;
    watcher.shutdown()?;
    while watcher.try_next()?.is_some() {
        delivered += 1;
        thread::sleep(CONSUMER_DELAY);
    }
    let elapsed = started.elapsed();
    let snapshot = watcher.metrics();
    let received = snapshot.events_received.iter().sum();

    Ok(Trial {
        backend: "dirwatch",
        trial,
        writes,
        received,
        delivered,
        dropped: snapshot.events_dropped,
        overflows: snapshot.overflows,
        coalesced: snapshot.events_coalesced,
        backend_errors: snapshot.backend_errors,
        queue_peak_events: snapshot.queue_peak_events,
        queue_peak_bytes: Some(snapshot.queue_peak_bytes),
        latency_p50_ns: snapshot.event_latency_p50_ns,
        latency_p95_ns: snapshot.event_latency_p95_ns,
        elapsed,
    })
}

fn run_plain_notify(trial: usize) -> Result<Trial, Box<dyn Error>> {
    let directory = TempDir::new()?;
    let paths = prepare_files(directory.path())?;
    let (sender, receiver) = mpsc::channel();
    let pending = Arc::new(AtomicUsize::new(0));
    let peak_pending = Arc::new(AtomicUsize::new(0));
    let received_paths = Arc::new(AtomicUsize::new(0));
    let callback_pending = Arc::clone(&pending);
    let callback_peak = Arc::clone(&peak_pending);
    let callback_received = Arc::clone(&received_paths);
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<NotifyEvent>| {
        let path_count = match &result {
            Ok(event) if !matches!(&event.kind, NotifyEventKind::Access(_)) => event.paths.len(),
            _ => 0,
        };
        if path_count == 0 && result.is_ok() {
            return;
        }
        let received_at = Instant::now();
        callback_received.fetch_add(path_count, Ordering::Relaxed);
        let pending_count = callback_pending.fetch_add(path_count, Ordering::Relaxed) + path_count;
        callback_peak.fetch_max(pending_count, Ordering::Relaxed);
        if sender.send((received_at, result, path_count)).is_err() {
            callback_pending.fetch_sub(path_count, Ordering::Relaxed);
        }
    })?;
    watcher.watch(directory.path(), RecursiveMode::NonRecursive)?;

    let (done_sender, done_receiver) = mpsc::channel();
    let barrier = Arc::new(Barrier::new(2));
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        let _ = done_sender.send(write_workload(&paths));
    });

    let metrics = Metrics::default();
    let started = Instant::now();
    barrier.wait();
    let mut writes = None;
    let mut delivered = 0u64;
    let mut backend_errors = 0u64;
    let mut last_activity = started;

    loop {
        match receiver.recv_timeout(POLL_INTERVAL) {
            Ok((received_at, result, path_count)) => {
                if path_count > 0 {
                    pending.fetch_sub(path_count, Ordering::Relaxed);
                }
                match result {
                    Ok(_) if path_count > 0 => {
                        last_activity = Instant::now();
                        for _ in 0..path_count {
                            metrics.record_event_latency(received_at.elapsed());
                            delivered += 1;
                            thread::sleep(CONSUMER_DELAY);
                        }
                    }
                    Err(_) => {
                        backend_errors += 1;
                        last_activity = Instant::now();
                    }
                    _ => {}
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
        poll_writer(&done_receiver, &mut writes, &mut last_activity)?;
        if writes.is_some() && last_activity.elapsed() >= QUIET_PERIOD {
            break;
        }
    }

    let writes = writes.ok_or_else(|| io::Error::other("writer did not finish"))?;
    writer
        .join()
        .map_err(|_| io::Error::other("writer thread panicked"))?;
    watcher.unwatch(directory.path())?;
    drop(watcher);
    while let Ok((received_at, result, path_count)) = receiver.try_recv() {
        if path_count > 0 {
            pending.fetch_sub(path_count, Ordering::Relaxed);
        }
        match result {
            Ok(_) if path_count > 0 => {
                for _ in 0..path_count {
                    metrics.record_event_latency(received_at.elapsed());
                    delivered += 1;
                    thread::sleep(CONSUMER_DELAY);
                }
            }
            Err(_) => backend_errors += 1,
            _ => {}
        }
    }

    let elapsed = started.elapsed();
    let snapshot = metrics.snapshot();
    Ok(Trial {
        backend: "notify",
        trial,
        writes,
        received: received_paths.load(Ordering::Relaxed) as u64,
        delivered,
        dropped: 0,
        overflows: 0,
        coalesced: 0,
        backend_errors,
        queue_peak_events: peak_pending.load(Ordering::Relaxed) as u64,
        queue_peak_bytes: None,
        latency_p50_ns: snapshot.event_latency_p50_ns,
        latency_p95_ns: snapshot.event_latency_p95_ns,
        elapsed,
    })
}

fn prepare_files(directory: &Path) -> io::Result<Vec<PathBuf>> {
    (0..FILES)
        .map(|index| {
            let path = directory.join(format!("watched-{index:02}.txt"));
            fs::write(&path, b"initial")?;
            Ok(path)
        })
        .collect()
}

fn write_workload(paths: &[PathBuf]) -> io::Result<u64> {
    let mut writes = 0;
    for round in 0..ROUNDS {
        for (index, path) in paths.iter().enumerate() {
            fs::write(path, format!("{round}:{index}"))?;
            writes += 1;
        }
    }
    Ok(writes)
}

fn poll_writer(
    receiver: &mpsc::Receiver<io::Result<u64>>,
    writes: &mut Option<u64>,
    last_activity: &mut Instant,
) -> Result<(), Box<dyn Error>> {
    if writes.is_some() {
        return Ok(());
    }
    match receiver.try_recv() {
        Ok(result) => {
            *writes = Some(result?);
            *last_activity = Instant::now();
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return Err(io::Error::other("writer exited without reporting results").into());
        }
    }
    Ok(())
}

fn print_trial(trial: &Trial) {
    let seconds = trial.elapsed.as_secs_f64();
    let peak_bytes = trial
        .queue_peak_bytes
        .map_or_else(|| "na".to_owned(), |bytes| bytes.to_string());
    println!(
        "{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.1}",
        trial.backend,
        trial.trial,
        trial.writes,
        trial.received,
        trial.delivered,
        trial.dropped,
        trial.overflows,
        trial.coalesced,
        trial.backend_errors,
        trial.queue_peak_events,
        peak_bytes,
        trial.latency_p50_ns as f64 / 1_000.0,
        trial.latency_p95_ns as f64 / 1_000.0,
        seconds * 1_000.0,
        trial.received as f64 / seconds,
    );
}

fn print_median(backend: &'static str, trials: &[Trial]) {
    let trials: Vec<_> = trials
        .iter()
        .filter(|trial| trial.backend == backend)
        .collect();
    let peak_bytes = if backend == "dirwatch" {
        median_u64(
            trials
                .iter()
                .filter_map(|trial| trial.queue_peak_bytes)
                .collect(),
        )
        .to_string()
    } else {
        "na".to_owned()
    };
    println!(
        "median,{backend},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.1}",
        median_u64(trials.iter().map(|trial| trial.writes).collect()),
        median_u64(trials.iter().map(|trial| trial.received).collect()),
        median_u64(trials.iter().map(|trial| trial.delivered).collect()),
        median_u64(trials.iter().map(|trial| trial.dropped).collect()),
        median_u64(trials.iter().map(|trial| trial.overflows).collect()),
        median_u64(trials.iter().map(|trial| trial.coalesced).collect()),
        median_u64(trials.iter().map(|trial| trial.backend_errors).collect()),
        median_u64(trials.iter().map(|trial| trial.queue_peak_events).collect()),
        peak_bytes,
        median_u64(trials.iter().map(|trial| trial.latency_p50_ns).collect()) as f64 / 1_000.0,
        median_u64(trials.iter().map(|trial| trial.latency_p95_ns).collect()) as f64 / 1_000.0,
        median_u64(
            trials
                .iter()
                .map(|trial| trial.elapsed.as_micros() as u64)
                .collect(),
        ) as f64
            / 1_000.0,
        median_f64(
            trials
                .iter()
                .map(|trial| trial.received as f64 / trial.elapsed.as_secs_f64())
                .collect(),
        ),
    );
}

fn median_u64(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    values[values.len() / 2]
}

fn median_f64(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}
