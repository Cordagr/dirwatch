use std::path::Path;

use crate::backend::{Backend, NotifyBackend};
use crate::config::EngineConfig;
use crate::error::{Error, Result};
use crate::event::Event;
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::queue::{EventQueue, PushOutcome};

pub struct Engine {
    config: EngineConfig,
    backend: NotifyBackend,
    queue: EventQueue,
    metrics: Metrics,
    closed: bool,
}

impl Engine {
    pub fn start(config: EngineConfig) -> Result<Self> {
        config.validate()?;
        let queue = EventQueue::with_coalescing(
            config.queue_capacity_bytes,
            config.backpressure,
            config.coalesce_window,
            config.coalesce_max_entries,
        )?;
        let metrics = Metrics::default();
        let backend = NotifyBackend::new(queue.clone(), metrics.clone());
        Ok(Self {
            config,
            backend,
            queue,
            metrics,
            closed: false,
        })
    }

    pub fn watch(&mut self, path: impl AsRef<Path>, recursive: bool) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        if self.backend.contains_path(path.as_ref())? {
            return Ok(());
        }
        if self.backend.watch_count() >= self.config.max_watches {
            return Err(Error::WatchLimitReached);
        }
        self.backend.watch(path.as_ref(), recursive)?;
        self.metrics
            .set_watched_directories(self.backend.watch_count());
        Ok(())
    }

    pub fn unwatch(&mut self, path: impl AsRef<Path>) -> Result<()> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.backend.unwatch(path.as_ref())?;
        self.metrics
            .set_watched_directories(self.backend.watch_count());
        Ok(())
    }

    pub fn next(&self) -> Result<Option<Event>> {
        self.finish_pop(self.queue.pop_with_latency()?)
    }

    pub fn next_timeout(&self, timeout: std::time::Duration) -> Result<Option<Event>> {
        self.finish_pop(self.queue.pop_timeout_with_latency(timeout)?)
    }

    fn finish_pop(&self, queued: Option<(Event, std::time::Duration)>) -> Result<Option<Event>> {
        if let Some((_, latency)) = queued.as_ref() {
            self.metrics.record_event_latency(*latency);
        }
        self.update_queue_metrics()?;
        Ok(queued.map(|(event, _)| event))
    }

    pub fn try_next(&self) -> Result<Option<Event>> {
        let queued = self.queue.try_pop_with_latency()?;
        if let Some((_, latency)) = queued.as_ref() {
            self.metrics.record_event_latency(*latency);
        }
        self.update_queue_metrics()?;
        Ok(queued.map(|(event, _)| event))
    }

    pub fn push(&self, event: Event) -> Result<PushOutcome> {
        let kind = event.kind;
        self.metrics.record_event(kind);
        let outcome = self.queue.push(event)?;
        match outcome {
            PushOutcome::DroppedNewest => self.metrics.record_dropped_event(kind),
            PushOutcome::DroppedOldest(dropped) => self.metrics.record_dropped_events(dropped),
            PushOutcome::Overflowed(dropped) => {
                self.metrics.record_dropped_events(dropped);
                if matches!(outcome, PushOutcome::Overflowed(_)) {
                    self.metrics.record_event(crate::event::EventKind::Overflow);
                }
            }
            PushOutcome::Coalesced => self.metrics.record_coalesced_event(),
            PushOutcome::Enqueued => {}
        }
        self.update_queue_metrics()?;
        Ok(outcome)
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    pub(crate) fn metrics_recorder(&self) -> Metrics {
        self.metrics.clone()
    }

    pub fn queue(&self) -> EventQueue {
        self.queue.clone()
    }

    pub fn shutdown(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        let backend_result = self.backend.close();
        self.closed = true;
        self.queue.close()?;
        backend_result?;
        Ok(())
    }

    fn update_queue_metrics(&self) -> Result<()> {
        self.metrics
            .set_queue_state(self.queue.len()?, self.queue.queued_bytes()?);
        let (peak_events, peak_bytes) = self.queue.peak_state()?;
        self.metrics.set_queue_peak(peak_events, peak_bytes);
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if !self.closed {
            let backend_result = self.backend.close();
            let queue_result = self.queue.close();
            if let Err(error) = backend_result {
                eprintln!("failed to close filesystem backend: {error}");
            }
            if let Err(error) = queue_result {
                eprintln!("failed to close event queue: {error}");
            }
            self.closed = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackpressureOptions;
    use crate::event::EventKind;
    use std::time::Duration;

    #[test]
    fn records_coalesced_pushes() -> Result<()> {
        let config = EngineConfig::builder()
            .backpressure(BackpressureOptions::Coalesce)
            .coalesce_window(Duration::from_secs(1))
            .build()?;
        let engine = Engine::start(config)?;
        engine.push(Event::new(EventKind::Modified, "same.txt"))?;
        engine.push(Event::new(EventKind::Modified, "same.txt"))?;

        assert_eq!(engine.metrics().events_coalesced, 1);
        Ok(())
    }
}
