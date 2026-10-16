use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metrics::{counter, gauge, histogram};

use crate::event::EventKind;

#[derive(Clone, Default)]
/// Shared metrics for filesystem watching and event delivery.
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    events_received: [AtomicU64; 5],
    events_dropped: AtomicU64,
    overflows: AtomicU64,
    events_coalesced: AtomicU64,
    backend_errors: AtomicU64,
    queued_events: AtomicU64,
    queued_bytes: AtomicU64,
    watched_directories: AtomicU64,
    event_latency_count: AtomicU64,
    event_latency_total_ns: AtomicU64,
    event_latency_max_ns: AtomicU64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
/// Point-in-time values collected by [`Metrics::snapshot`].
pub struct MetricsSnapshot {
    /// Counts in order: created, modified, removed, renamed, overflow.
    pub events_received: [u64; 5],
    pub events_dropped: u64,
    pub overflows: u64,
    pub events_coalesced: u64,
    pub backend_errors: u64,
    pub queued_events: u64,
    pub queued_bytes: u64,
    pub watched_directories: u64,
    pub event_latency_count: u64,
    pub event_latency_total_ns: u64,
    pub event_latency_max_ns: u64,
}

impl Metrics {
    /// Records an event received from a filesystem backend.
    pub fn record_event(&self, kind: EventKind) {
        let index = event_kind_index(kind);
        self.inner.events_received[index].fetch_add(1, Ordering::Relaxed);
        counter!("dirwatch_events_received_total", "kind" => event_kind_name(kind)).increment(1);

        if kind == EventKind::Overflow {
            self.inner.overflows.fetch_add(1, Ordering::Relaxed);
            counter!("dirwatch_overflows_total").increment(1);
        }
    }

    /// Records an event discarded by queue backpressure or shutdown.
    pub fn record_dropped_event(&self, kind: EventKind) {
        self.inner.events_dropped.fetch_add(1, Ordering::Relaxed);
        counter!("dirwatch_events_dropped_total", "kind" => event_kind_name(kind)).increment(1);
    }

    pub fn record_dropped_events(&self, count: usize) {
        self.inner
            .events_dropped
            .fetch_add(count as u64, Ordering::Relaxed);
        counter!("dirwatch_events_dropped_total").increment(count as u64);
    }

    /// Records an event absorbed into a coalesced event.
    pub fn record_coalesced_event(&self) {
        self.inner.events_coalesced.fetch_add(1, Ordering::Relaxed);
        counter!("dirwatch_events_coalesced_total").increment(1);
    }

    /// Records a backend operation failure.
    pub fn record_backend_error(&self) {
        self.inner.backend_errors.fetch_add(1, Ordering::Relaxed);
        counter!("dirwatch_backend_errors_total").increment(1);
    }

    /// Updates current queue occupancy.
    pub fn set_queue_state(&self, queued_events: usize, queued_bytes: usize) {
        self.inner
            .queued_events
            .store(queued_events as u64, Ordering::Relaxed);
        self.inner
            .queued_bytes
            .store(queued_bytes as u64, Ordering::Relaxed);
        gauge!("dirwatch_queue_events").set(queued_events as f64);
        gauge!("dirwatch_queue_bytes").set(queued_bytes as f64);
    }

    /// Updates the number of directories currently watched.
    pub fn set_watched_directories(&self, count: usize) {
        self.inner
            .watched_directories
            .store(count as u64, Ordering::Relaxed);
        gauge!("dirwatch_watched_directories").set(count as f64);
    }

    /// Records the time from backend receipt to delivery.
    pub fn record_event_latency(&self, latency: Duration) {
        let latency_ns = latency.as_nanos().min(u64::MAX as u128) as u64;
        self.inner
            .event_latency_count
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .event_latency_total_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.inner
            .event_latency_max_ns
            .fetch_max(latency_ns, Ordering::Relaxed);
        histogram!("dirwatch_event_latency_seconds").record(latency.as_secs_f64());
    }

    /// Returns a snapshot of the counters, gauges, and latency aggregates.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            events_received: std::array::from_fn(|index| {
                self.inner.events_received[index].load(Ordering::Relaxed)
            }),
            events_dropped: self.inner.events_dropped.load(Ordering::Relaxed),
            overflows: self.inner.overflows.load(Ordering::Relaxed),
            events_coalesced: self.inner.events_coalesced.load(Ordering::Relaxed),
            backend_errors: self.inner.backend_errors.load(Ordering::Relaxed),
            queued_events: self.inner.queued_events.load(Ordering::Relaxed),
            queued_bytes: self.inner.queued_bytes.load(Ordering::Relaxed),
            watched_directories: self.inner.watched_directories.load(Ordering::Relaxed),
            event_latency_count: self.inner.event_latency_count.load(Ordering::Relaxed),
            event_latency_total_ns: self.inner.event_latency_total_ns.load(Ordering::Relaxed),
            event_latency_max_ns: self.inner.event_latency_max_ns.load(Ordering::Relaxed),
        }
    }
}

fn event_kind_index(kind: EventKind) -> usize {
    match kind {
        EventKind::Created => 0,
        EventKind::Modified => 1,
        EventKind::Removed => 2,
        EventKind::Renamed => 3,
        EventKind::Overflow => 4,
    }
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
    fn records_event_categories_and_overflow() {
        let metrics = Metrics::default();
        metrics.record_event(EventKind::Created);
        metrics.record_event(EventKind::Overflow);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.events_received, [1, 0, 0, 0, 1]);
        assert_eq!(snapshot.overflows, 1);
    }

    #[test]
    fn clones_share_metrics_and_snapshot_includes_queue_and_latency() {
        let metrics = Metrics::default();
        let clone = metrics.clone();

        clone.set_queue_state(3, 1024);
        clone.set_watched_directories(2);
        clone.record_event_latency(Duration::from_millis(5));
        metrics.record_dropped_event(EventKind::Modified);
        metrics.record_coalesced_event();
        metrics.record_backend_error();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.queued_events, 3);
        assert_eq!(snapshot.queued_bytes, 1024);
        assert_eq!(snapshot.watched_directories, 2);
        assert_eq!(snapshot.events_dropped, 1);
        assert_eq!(snapshot.events_coalesced, 1);
        assert_eq!(snapshot.backend_errors, 1);
        assert_eq!(snapshot.event_latency_count, 1);
        assert_eq!(snapshot.event_latency_total_ns, 5_000_000);
        assert_eq!(snapshot.event_latency_max_ns, 5_000_000);
    }
}
