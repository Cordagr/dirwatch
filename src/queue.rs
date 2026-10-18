use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::config::BackpressureOptions;
use crate::error::{Error, Result};
use crate::event::Event;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushOutcome {
    Enqueued,
    Coalesced,
    DroppedNewest,
    DroppedOldest(usize),
    Overflowed(usize),
}

struct QueueState {
    events: VecDeque<(Instant, Event)>,
    queued_bytes: usize,
    closed: bool,
    peak_queued_events: usize,
    peak_queued_bytes: usize,
}

impl QueueState {
    fn record_high_water(&mut self) {
        self.peak_queued_events = self.peak_queued_events.max(self.events.len());
        self.peak_queued_bytes = self.peak_queued_bytes.max(self.queued_bytes);
    }
}

struct QueueInner {
    state: Mutex<QueueState>,
    not_empty: Condvar,
    not_full: Condvar,
    capacity_bytes: usize,
    policy: BackpressureOptions,
    coalesce_window: Duration,
    coalesce_max_entries: usize,
}

#[derive(Clone)]
pub struct EventQueue {
    inner: Arc<QueueInner>,
}

impl EventQueue {
    pub fn new(capacity_bytes: usize, policy: BackpressureOptions) -> Result<Self> {
        Self::with_coalescing(capacity_bytes, policy, Duration::from_millis(50), 4096)
    }

    pub fn with_coalescing(
        capacity_bytes: usize,
        policy: BackpressureOptions,
        coalesce_window: Duration,
        coalesce_max_entries: usize,
    ) -> Result<Self> {
        if capacity_bytes == 0 {
            return Err(Error::InvalidConfig(
                "queue_capacity_bytes must be greater than zero",
            ));
        }
        if coalesce_max_entries == 0 {
            return Err(Error::InvalidConfig(
                "coalesce_max_entries must be greater than zero",
            ));
        }
        Ok(Self {
            inner: Arc::new(QueueInner {
                state: Mutex::new(QueueState {
                    events: VecDeque::new(),
                    queued_bytes: 0,
                    closed: false,
                    peak_queued_events: 0,
                    peak_queued_bytes: 0,
                }),
                not_empty: Condvar::new(),
                not_full: Condvar::new(),
                capacity_bytes,
                policy,
                coalesce_window,
                coalesce_max_entries,
            }),
        })
    }

    pub fn push(&self, event: Event) -> Result<PushOutcome> {
        let event_bytes = event.size_bytes();
        if event_bytes > self.inner.capacity_bytes {
            return Err(Error::InvalidConfig(
                "an event is larger than queue_capacity_bytes",
            ));
        }

        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        if state.closed {
            return Err(Error::Closed);
        }

        if self.inner.policy == BackpressureOptions::Coalesce {
            let now = Instant::now();
            if let Some(index) = state.events.iter().position(|(inserted, queued)| {
                now.duration_since(*inserted) <= self.inner.coalesce_window
                    && queued.path == event.path
                    && queued.kind == event.kind
            }) {
                let old_bytes = state.events[index].1.size_bytes();
                if state.queued_bytes - old_bytes + event_bytes > self.inner.capacity_bytes {
                    return self.replace_with_overflow(&mut state, 1);
                }
                state.events[index].1 = event;
                state.queued_bytes = state.queued_bytes - old_bytes + event_bytes;
                state.record_high_water();
                self.inner.not_empty.notify_one();
                return Ok(PushOutcome::Coalesced);
            }
            if state.events.len() >= self.inner.coalesce_max_entries {
                return self.replace_with_overflow(&mut state, 1);
            }
        }

        match self.inner.policy {
            BackpressureOptions::Block => {
                while state.queued_bytes + event_bytes > self.inner.capacity_bytes {
                    if state.closed {
                        return Err(Error::Closed);
                    }
                    state = self
                        .inner
                        .not_full
                        .wait(state)
                        .map_err(|_| Error::Poisoned)?;
                }
                state.queued_bytes += event_bytes;
                state.events.push_back((Instant::now(), event));
                state.record_high_water();
                self.inner.not_empty.notify_one();
                Ok(PushOutcome::Enqueued)
            }
            BackpressureOptions::DropNewest => {
                if state.queued_bytes + event_bytes > self.inner.capacity_bytes {
                    Ok(PushOutcome::DroppedNewest)
                } else {
                    state.queued_bytes += event_bytes;
                    state.events.push_back((Instant::now(), event));
                    state.record_high_water();
                    self.inner.not_empty.notify_one();
                    Ok(PushOutcome::Enqueued)
                }
            }
            BackpressureOptions::DropOldest => {
                let mut dropped = 0;
                while state.queued_bytes + event_bytes > self.inner.capacity_bytes {
                    let Some((_, oldest)) = state.events.pop_front() else {
                        return Err(Error::InvalidConfig("event cannot fit in the queue"));
                    };
                    state.queued_bytes -= oldest.size_bytes();
                    dropped += 1;
                }
                state.queued_bytes += event_bytes;
                state.events.push_back((Instant::now(), event));
                state.record_high_water();
                self.inner.not_empty.notify_one();
                if dropped == 0 {
                    Ok(PushOutcome::Enqueued)
                } else {
                    Ok(PushOutcome::DroppedOldest(dropped))
                }
            }
            BackpressureOptions::Coalesce | BackpressureOptions::OverflowSignal => {
                if state.queued_bytes + event_bytes <= self.inner.capacity_bytes {
                    state.queued_bytes += event_bytes;
                    state.events.push_back((Instant::now(), event));
                    state.record_high_water();
                    self.inner.not_empty.notify_one();
                    return Ok(PushOutcome::Enqueued);
                }
                self.replace_with_overflow(&mut state, 1)
            }
        }
    }

    fn replace_with_overflow(
        &self,
        state: &mut QueueState,
        additional_dropped: usize,
    ) -> Result<PushOutcome> {
        let dropped = state.events.len() + additional_dropped;
        state.events.clear();
        state.queued_bytes = 0;
        let overflow = Event::overflow(None);
        let overflow_bytes = overflow.size_bytes();
        if overflow_bytes > self.inner.capacity_bytes {
            return Err(Error::InvalidConfig(
                "queue capacity is too small for an overflow event",
            ));
        }
        state.queued_bytes = overflow_bytes;
        state.events.push_back((Instant::now(), overflow));
        state.record_high_water();
        self.inner.not_empty.notify_one();
        Ok(PushOutcome::Overflowed(dropped))
    }

    pub fn pop(&self) -> Result<Option<Event>> {
        self.pop_with_latency()
            .map(|queued| queued.map(|(event, _)| event))
    }

    pub fn pop_timeout(&self, timeout: Duration) -> Result<Option<Event>> {
        self.pop_timeout_with_latency(timeout)
            .map(|queued| queued.map(|(event, _)| event))
    }

    pub(crate) fn pop_with_latency(&self) -> Result<Option<(Event, Duration)>> {
        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        while state.events.is_empty() && !state.closed {
            state = self
                .inner
                .not_empty
                .wait(state)
                .map_err(|_| Error::Poisoned)?;
        }
        let Some((inserted, event)) = state.events.pop_front() else {
            return Ok(None);
        };
        state.queued_bytes -= event.size_bytes();
        self.inner.not_full.notify_all();
        Ok(Some((event, inserted.elapsed())))
    }

    pub(crate) fn pop_timeout_with_latency(
        &self,
        timeout: Duration,
    ) -> Result<Option<(Event, Duration)>> {
        let deadline = Instant::now().checked_add(timeout);
        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        while state.events.is_empty() && !state.closed {
            let remaining = match deadline {
                Some(deadline) => match deadline.checked_duration_since(Instant::now()) {
                    Some(remaining) if !remaining.is_zero() => remaining,
                    _ => return Ok(None),
                },
                None => Duration::from_secs(24 * 60 * 60),
            };
            let (next_state, timed_out) = self
                .inner
                .not_empty
                .wait_timeout(state, remaining)
                .map_err(|_| Error::Poisoned)?;
            state = next_state;
            if timed_out.timed_out() && state.events.is_empty() {
                return Ok(None);
            }
        }
        let Some((inserted, event)) = state.events.pop_front() else {
            return Ok(None);
        };
        state.queued_bytes -= event.size_bytes();
        self.inner.not_full.notify_all();
        Ok(Some((event, inserted.elapsed())))
    }

    pub fn try_pop(&self) -> Result<Option<Event>> {
        self.try_pop_with_latency()
            .map(|queued| queued.map(|(event, _)| event))
    }

    pub(crate) fn try_pop_with_latency(&self) -> Result<Option<(Event, Duration)>> {
        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        let Some((inserted, event)) = state.events.pop_front() else {
            return Ok(None);
        };
        state.queued_bytes -= event.size_bytes();
        self.inner.not_full.notify_all();
        Ok(Some((event, inserted.elapsed())))
    }

    pub fn drain_to_overflow(&self, path: Option<std::path::PathBuf>) -> Result<usize> {
        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        if state.closed {
            return Err(Error::Closed);
        }
        let overflow = Event::overflow(path);
        if overflow.size_bytes() > self.inner.capacity_bytes {
            return Err(Error::InvalidConfig(
                "queue capacity is too small for an overflow event",
            ));
        }
        let dropped = state.events.len();
        state.events.clear();
        state.queued_bytes = overflow.size_bytes();
        state.events.push_back((Instant::now(), overflow));
        state.record_high_water();
        self.inner.not_empty.notify_one();
        Ok(dropped)
    }

    pub fn close(&self) -> Result<()> {
        let mut state = self.inner.state.lock().map_err(|_| Error::Poisoned)?;
        state.closed = true;
        self.inner.not_empty.notify_all();
        self.inner.not_full.notify_all();
        Ok(())
    }

    pub fn len(&self) -> Result<usize> {
        self.inner
            .state
            .lock()
            .map(|state| state.events.len())
            .map_err(|_| Error::Poisoned)
    }

    pub fn is_empty(&self) -> Result<bool> {
        self.len().map(|length| length == 0)
    }

    pub fn queued_bytes(&self) -> Result<usize> {
        self.inner
            .state
            .lock()
            .map(|state| state.queued_bytes)
            .map_err(|_| Error::Poisoned)
    }

    pub fn peak_state(&self) -> Result<(usize, usize)> {
        self.inner
            .state
            .lock()
            .map(|state| (state.peak_queued_events, state.peak_queued_bytes))
            .map_err(|_| Error::Poisoned)
    }

    pub fn is_closed(&self) -> Result<bool> {
        self.inner
            .state
            .lock()
            .map(|state| state.closed)
            .map_err(|_| Error::Poisoned)
    }
}

pub fn apply_policy(queue: &EventQueue, event: Event) -> Result<PushOutcome> {
    queue.push(event)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackpressureOptions;
    use crate::event::EventKind;

    fn queue(policy: BackpressureOptions) -> EventQueue {
        EventQueue::new(512, policy).unwrap()
    }

    #[test]
    fn coalesces_repeated_path_and_kind() {
        let queue = queue(BackpressureOptions::Coalesce);
        let path = std::path::PathBuf::from("file.txt");
        assert_eq!(
            queue.push(Event::new(EventKind::Modified, &path)).unwrap(),
            PushOutcome::Enqueued
        );
        let first_inserted = queue.inner.state.lock().unwrap().events[0].0;
        assert_eq!(
            queue.push(Event::new(EventKind::Modified, &path)).unwrap(),
            PushOutcome::Coalesced
        );
        assert_eq!(
            queue.inner.state.lock().unwrap().events[0].0,
            first_inserted
        );
        assert_eq!(queue.len().unwrap(), 1);
    }

    #[test]
    fn drop_oldest_makes_room_and_close_wakes_consumers() {
        let event = Event::new(EventKind::Created, "a");
        let capacity = event.size_bytes();
        let queue = EventQueue::new(capacity, BackpressureOptions::DropOldest).unwrap();
        queue.push(event.clone()).unwrap();
        assert_eq!(queue.push(event).unwrap(), PushOutcome::DroppedOldest(1));
        queue.close().unwrap();
        assert!(queue.pop().unwrap().is_some());
        assert!(queue.pop().unwrap().is_none());
    }

    #[test]
    fn overflow_signal_replaces_queued_events() {
        let event = Event::new(EventKind::Created, "a");
        let capacity = event.size_bytes().max(Event::overflow(None).size_bytes());
        let queue = EventQueue::new(capacity, BackpressureOptions::OverflowSignal).unwrap();
        queue.push(event.clone()).unwrap();
        assert!(matches!(
            queue.push(event).unwrap(),
            PushOutcome::Overflowed(2)
        ));
        assert_eq!(queue.try_pop().unwrap().unwrap().kind, EventKind::Overflow);
    }

    #[test]
    fn coalescing_never_exceeds_the_byte_budget() {
        let first = Event::new(EventKind::Modified, "a");
        let other = Event::new(EventKind::Created, "b");
        let capacity = first.size_bytes() + other.size_bytes();
        let queue = EventQueue::new(capacity, BackpressureOptions::Coalesce).unwrap();
        queue.push(first.clone()).unwrap();
        queue.push(other).unwrap();

        let mut larger = first;
        larger.class = Some("xx".to_owned());
        assert_eq!(queue.push(larger).unwrap(), PushOutcome::Overflowed(3));
        assert!(queue.queued_bytes().unwrap() <= capacity);
        assert_eq!(queue.try_pop().unwrap().unwrap().kind, EventKind::Overflow);
    }

    #[test]
    fn timed_pop_returns_when_no_event_arrives() {
        let queue = queue(BackpressureOptions::Block);
        assert!(queue
            .pop_timeout(Duration::from_millis(1))
            .unwrap()
            .is_none());
    }

    #[test]
    fn retains_queue_high_water_after_drain() {
        let queue = queue(BackpressureOptions::Block);
        for path in ["a", "b", "c"] {
            queue.push(Event::new(EventKind::Modified, path)).unwrap();
        }

        let peak = queue.peak_state().unwrap();
        assert_eq!(peak.0, 3);
        assert!(peak.1 > 0);

        while queue.try_pop().unwrap().is_some() {}
        assert_eq!(queue.peak_state().unwrap(), peak);
    }
}
