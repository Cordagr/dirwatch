use crate::error::Result;
use crate::event::Event;
use crate::metrics::Metrics;
use crate::queue::EventQueue;

pub struct EventStream {
    queue: EventQueue,
    metrics: Metrics,
}

impl EventStream {
    pub(crate) fn new(queue: EventQueue, metrics: Metrics) -> Self {
        Self { queue, metrics }
    }
}

impl Iterator for EventStream {
    type Item = Result<Event>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.queue.pop_with_latency() {
            Ok(Some((event, latency))) => {
                self.metrics.record_event_latency(latency);
                match (self.queue.len(), self.queue.queued_bytes()) {
                    (Ok(length), Ok(bytes)) => self.metrics.set_queue_state(length, bytes),
                    (Err(error), _) | (_, Err(error)) => return Some(Err(error)),
                }
                Some(Ok(event))
            }
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
}
