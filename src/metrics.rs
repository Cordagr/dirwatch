use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge, histogram};

use crate::event::EventKind;

const LATENCY_SUB_BUCKETS: usize = 8;
const LATENCY_BUCKETS: usize = 64 * LATENCY_SUB_BUCKETS;

#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<Inner>,
}

struct Buckets([AtomicU64; LATENCY_BUCKETS]);

impl Default for Buckets {
    fn default() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }
}

#[derive(Default)]
struct Inner {
    received: [AtomicU64; 5],
    dropped: AtomicU64,
    overflows: AtomicU64,
    coalesced: AtomicU64,
    backend_errors: AtomicU64,
    queued_events: AtomicU64,
    queued_bytes: AtomicU64,
    peak_events: AtomicU64,
    peak_bytes: AtomicU64,
    watched_dirs: AtomicU64,
    lat_count: AtomicU64,
    lat_total_ns: AtomicU64,
    lat_max_ns: AtomicU64,
    lat_buckets: Buckets,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct MetricsSnapshot {
    pub events_received: [u64; 5],
    pub events_dropped: u64,
    pub overflows: u64,
    pub events_coalesced: u64,
    pub backend_errors: u64,
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub queue_peak_events: u64,
    pub queue_peak_bytes: u64,
    pub watched_directories: u64,
    pub event_latency_count: u64,
    pub event_latency_total_ns: u64,
    pub event_latency_max_ns: u64,
    pub event_latency_p50_ns: u64,
    pub event_latency_p95_ns: u64,
}

impl Metrics {
    pub fn record_event(&self, kind: EventKind) {
        self.inner.received[kind_index(kind)].fetch_add(1, Relaxed);
        counter!("dirwatch_events_received_total", "kind" => kind_name(kind)).increment(1);

        if kind == EventKind::Overflow {
            self.inner.overflows.fetch_add(1, Relaxed);
            counter!("dirwatch_overflows_total").increment(1);
        }
    }

    pub fn record_dropped_event(&self, kind: EventKind) {
        self.inner.dropped.fetch_add(1, Relaxed);
        counter!("dirwatch_events_dropped_total", "kind" => kind_name(kind)).increment(1);
    }

    pub fn record_dropped_events(&self, n: usize) {
        self.inner.dropped.fetch_add(n as u64, Relaxed);
        counter!("dirwatch_events_dropped_total").increment(n as u64);
    }

    pub fn record_coalesced_event(&self) {
        self.inner.coalesced.fetch_add(1, Relaxed);
        counter!("dirwatch_events_coalesced_total").increment(1);
    }

    pub fn record_backend_error(&self) {
        self.inner.backend_errors.fetch_add(1, Relaxed);
        counter!("dirwatch_backend_errors_total").increment(1);
    }

    pub fn set_queue_state(&self, events: usize, bytes: usize) {
        self.inner.queued_events.store(events as u64, Relaxed);
        self.inner.queued_bytes.store(bytes as u64, Relaxed);
        gauge!("dirwatch_queue_events").set(events as f64);
        gauge!("dirwatch_queue_bytes").set(bytes as f64);
    }

    pub fn set_queue_peak(&self, events: usize, bytes: usize) {
        self.inner.peak_events.fetch_max(events as u64, Relaxed);
        self.inner.peak_bytes.fetch_max(bytes as u64, Relaxed);
    }

    pub fn set_watched_directories(&self, n: usize) {
        self.inner.watched_dirs.store(n as u64, Relaxed);
        gauge!("dirwatch_watched_directories").set(n as f64);
    }

    pub fn record_event_latency(&self, latency: Duration) {
        let ns = u64::try_from(latency.as_nanos()).unwrap_or(u64::MAX);
        let i = &self.inner;
        i.lat_buckets.0[bucket_index(ns)].fetch_add(1, Relaxed);
        i.lat_count.fetch_add(1, Relaxed);
        i.lat_total_ns.fetch_add(ns, Relaxed);
        i.lat_max_ns.fetch_max(ns, Relaxed);
        histogram!("dirwatch_event_latency_seconds").record(latency.as_secs_f64());
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let i = &self.inner;
        let count = i.lat_count.load(Relaxed);
        MetricsSnapshot {
            events_received: std::array::from_fn(|n| i.received[n].load(Relaxed)),
            events_dropped: i.dropped.load(Relaxed),
            overflows: i.overflows.load(Relaxed),
            events_coalesced: i.coalesced.load(Relaxed),
            backend_errors: i.backend_errors.load(Relaxed),
            queued_events: i.queued_events.load(Relaxed),
            queued_bytes: i.queued_bytes.load(Relaxed),
            queue_peak_events: i.peak_events.load(Relaxed),
            queue_peak_bytes: i.peak_bytes.load(Relaxed),
            watched_directories: i.watched_dirs.load(Relaxed),
            event_latency_count: count,
            event_latency_total_ns: i.lat_total_ns.load(Relaxed),
            event_latency_max_ns: i.lat_max_ns.load(Relaxed),
            event_latency_p50_ns: percentile_ns(&i.lat_buckets.0, count, 50),
            event_latency_p95_ns: percentile_ns(&i.lat_buckets.0, count, 95),
        }
    }
}

fn bucket_index(ns: u64) -> usize {
    if ns <= 1 {
        0
    } else {
        let exponent = ns.ilog2() as usize;
        let base = 1u64 << exponent;
        let fraction =
            (((ns - base) as u128 * LATENCY_SUB_BUCKETS as u128) / base as u128) as usize;
        exponent * LATENCY_SUB_BUCKETS + fraction
    }
}

fn bucket_upper_bound_ns(index: usize) -> u64 {
    let exponent = index / LATENCY_SUB_BUCKETS;
    let fraction = index % LATENCY_SUB_BUCKETS;
    let base = 1u128 << exponent;
    let numerator = base * (fraction as u128 + 1);
    let upper_bound = base + numerator.div_ceil(LATENCY_SUB_BUCKETS as u128) - 1;
    upper_bound.min(u64::MAX as u128) as u64
}

fn percentile_ns(buckets: &[AtomicU64], count: u64, pct: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    let target = count.saturating_mul(pct).saturating_add(99) / 100;
    let mut seen = 0;
    for (i, b) in buckets.iter().enumerate() {
        seen += b.load(Relaxed);
        if seen >= target {
            return bucket_upper_bound_ns(i);
        }
    }
    u64::MAX
}

fn kind_index(kind: EventKind) -> usize {
    match kind {
        EventKind::Created => 0,
        EventKind::Modified => 1,
        EventKind::Removed => 2,
        EventKind::Renamed => 3,
        EventKind::Overflow => 4,
    }
}

fn kind_name(kind: EventKind) -> &'static str {
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
    fn overflow_counts_twice() {
        let m = Metrics::default();
        m.record_event(EventKind::Created);
        m.record_event(EventKind::Overflow);

        let s = m.snapshot();
        assert_eq!(s.events_received, [1, 0, 0, 0, 1]);
        assert_eq!(s.overflows, 1);
    }

    #[test]
    fn clones_share_state() {
        let m = Metrics::default();
        let c = m.clone();

        c.set_queue_state(3, 1024);
        c.set_watched_directories(2);
        c.record_event_latency(Duration::from_millis(5));
        m.record_dropped_event(EventKind::Modified);
        m.record_coalesced_event();
        m.record_backend_error();

        let s = m.snapshot();
        assert_eq!((s.queued_events, s.queued_bytes), (3, 1024));
        assert_eq!(s.watched_directories, 2);
        assert_eq!(s.events_dropped, 1);
        assert_eq!(s.events_coalesced, 1);
        assert_eq!(s.backend_errors, 1);
        assert_eq!(s.event_latency_count, 1);
        assert_eq!(s.event_latency_total_ns, 5_000_000);
        assert_eq!(s.event_latency_max_ns, 5_000_000);
    }

    #[test]
    fn peak_and_percentiles() {
        let m = Metrics::default();
        m.set_queue_peak(8, 4096);
        m.set_queue_peak(3, 1024);
        for ms in [1, 2, 3, 4, 10, 20, 30, 40, 50, 100] {
            m.record_event_latency(Duration::from_millis(ms));
        }

        let s = m.snapshot();
        assert_eq!((s.queue_peak_events, s.queue_peak_bytes), (8, 4096));
        assert!((10_000_000..20_000_000).contains(&s.event_latency_p50_ns));
        assert!((100_000_000..200_000_000).contains(&s.event_latency_p95_ns));
    }
}
