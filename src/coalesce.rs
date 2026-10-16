use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::event::Event;

pub struct Coalescer {
    window: Duration,
    max_entries: usize,
    entries: VecDeque<(Instant, Event)>,
}

impl Coalescer {
    pub fn new(window: Duration, max_entries: usize) -> Self {
        Self {
            window,
            max_entries,
            entries: VecDeque::new(),
        }
    }

    pub fn insert(&mut self, event: Event) -> Option<Event> {
        let now = Instant::now();
        let window = self.window;
        if let Some((_, existing)) = self.entries.iter_mut().find(|(inserted, queued)| {
            now.saturating_duration_since(*inserted) <= window
                && queued.path == event.path
                && queued.kind == event.kind
        }) {
            *existing = event;
            return None;
        }
        if self.entries.len() >= self.max_entries {
            self.entries.clear();
            return Some(Event::overflow(None));
        }
        self.entries.push_back((now, event));
        None
    }

    pub fn flush_due(&mut self, now: Instant) -> Vec<Event> {
        let mut due = Vec::new();
        while let Some((inserted, _)) = self.entries.front() {
            if now.saturating_duration_since(*inserted) < self.window {
                break;
            }
            match self.entries.pop_front() {
                Some((_, event)) => due.push(event),
                None => break,
            }
        }
        due
    }

    pub fn flush_all(&mut self) -> Vec<Event> {
        self.entries.drain(..).map(|(_, event)| event).collect()
    }

    pub fn is_over_cap(&self) -> bool {
        self.entries.len() >= self.max_entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::EventKind;

    #[test]
    fn a_duplicate_does_not_extend_the_window() {
        let window = Duration::from_secs(2);
        let mut coalescer = Coalescer::new(window, 4);
        coalescer.insert(Event::new(EventKind::Modified, "a"));
        let first_inserted = coalescer.entries[0].0;
        coalescer.insert(Event::new(EventKind::Modified, "a"));

        assert_eq!(coalescer.entries.len(), 1);
        assert_eq!(coalescer.entries[0].0, first_inserted);
        assert!(coalescer
            .flush_due(first_inserted + window - Duration::from_millis(1))
            .is_empty());
        assert_eq!(coalescer.flush_due(first_inserted + window).len(), 1);
    }

    #[test]
    fn same_path_after_window_is_kept_as_a_new_entry() {
        let window = Duration::from_secs(1);
        let mut coalescer = Coalescer::new(window, 4);
        coalescer.insert(Event::new(EventKind::Modified, "a"));
        coalescer.entries[0].0 = Instant::now()
            .checked_sub(window + Duration::from_millis(1))
            .unwrap();
        coalescer.insert(Event::new(EventKind::Modified, "a"));

        assert_eq!(coalescer.flush_all().len(), 2);
    }

    #[test]
    fn flush_due_stops_at_the_first_pending_entry() {
        let window = Duration::from_secs(2);
        let mut coalescer = Coalescer::new(window, 4);
        let start = Instant::now();
        coalescer.insert(Event::new(EventKind::Modified, "a"));
        coalescer.insert(Event::new(EventKind::Modified, "b"));
        coalescer.entries[0].0 = start;
        coalescer.entries[1].0 = start + window / 2;

        let due = coalescer.flush_due(start + window);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].path.as_deref(), Some(std::path::Path::new("a")));
        assert_eq!(coalescer.flush_due(start + window * 2).len(), 1);
    }

    #[test]
    fn reaching_entry_limit_returns_overflow() {
        let mut coalescer = Coalescer::new(Duration::from_secs(1), 1);
        assert!(coalescer
            .insert(Event::new(EventKind::Modified, "a"))
            .is_none());
        let overflow = coalescer
            .insert(Event::new(EventKind::Created, "b"))
            .unwrap();

        assert_eq!(overflow.kind, EventKind::Overflow);
        assert!(coalescer.flush_all().is_empty());
    }
}
