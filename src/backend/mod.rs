use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use notify::event::ModifyKind;
use notify::{
    Event as NotifyEvent, EventKind as NotifyEventKind, RecommendedWatcher, RecursiveMode,
    Watcher as NotifyWatcher,
};

use crate::error::{Error, Result};
use crate::event::{Event, EventKind};
use crate::metrics::Metrics;
use crate::queue::{EventQueue, PushOutcome};

pub trait Backend: Send {
    fn watch(&mut self, path: &Path, recursive: bool) -> Result<()>;
    fn unwatch(&mut self, path: &Path) -> Result<()>;
    fn close(&mut self) -> Result<()>;
    fn watch_count(&self) -> usize;
}

pub struct NotifyBackend {
    watches: HashMap<PathBuf, RecommendedWatcher>,
    watch_aliases: HashMap<PathBuf, PathBuf>,
    queue: EventQueue,
    metrics: Metrics,
}

impl NotifyBackend {
    pub fn new(queue: EventQueue, metrics: Metrics) -> Self {
        Self {
            watches: HashMap::new(),
            watch_aliases: HashMap::new(),
            queue,
            metrics,
        }
    }

    pub fn contains_path(&self, path: &Path) -> Result<bool> {
        match fs::canonicalize(path) {
            Ok(path) => Ok(self.watches.contains_key(&path)),
            Err(error) => match self.watch_aliases.get(path) {
                Some(canonical_path) => Ok(self.watches.contains_key(canonical_path)),
                None => Err(Error::Io(error)),
            },
        }
    }

    fn emit_overflow(queue: &EventQueue, metrics: &Metrics) {
        metrics.record_event(EventKind::Overflow);
        match queue.drain_to_overflow(None) {
            Ok(dropped) => {
                metrics.record_dropped_events(dropped);
                Self::update_queue_metrics(queue, metrics);
            }
            Err(_) => metrics.record_backend_error(),
        }
    }

    fn update_queue_metrics(queue: &EventQueue, metrics: &Metrics) {
        match (queue.len(), queue.queued_bytes(), queue.peak_state()) {
            (Ok(length), Ok(bytes), Ok((peak_events, peak_bytes))) => {
                metrics.set_queue_state(length, bytes);
                metrics.set_queue_peak(peak_events, peak_bytes);
            }
            _ => metrics.record_backend_error(),
        }
    }

    fn process_notify_result(
        result: notify::Result<NotifyEvent>,
        queue: &EventQueue,
        metrics: &Metrics,
    ) {
        let event = match result {
            Ok(event) => event,
            Err(_) => {
                metrics.record_backend_error();
                Self::emit_overflow(queue, metrics);
                return;
            }
        };

        let Some(kind) = map_event_kind(event.kind) else {
            return;
        };
        if event.paths.is_empty() {
            Self::emit_overflow(queue, metrics);
            return;
        }

        for path in event.paths {
            let mut normalized = Event::new(kind, path);
            normalized.class = Some(format!("{:?}", event.kind));
            metrics.record_event(kind);
            match queue.push(normalized) {
                Ok(PushOutcome::DroppedNewest) => metrics.record_dropped_event(kind),
                Ok(PushOutcome::DroppedOldest(dropped)) => metrics.record_dropped_events(dropped),
                Ok(PushOutcome::Overflowed(dropped)) => {
                    metrics.record_dropped_events(dropped);
                    metrics.record_event(EventKind::Overflow);
                }
                Ok(PushOutcome::Coalesced) => metrics.record_coalesced_event(),
                Ok(PushOutcome::Enqueued) => {}
                Err(_) => metrics.record_backend_error(),
            }
            Self::update_queue_metrics(queue, metrics);
        }
    }
}

impl Backend for NotifyBackend {
    fn watch(&mut self, path: &Path, recursive: bool) -> Result<()> {
        let canonical_path = fs::canonicalize(path)?;
        if !fs::metadata(&canonical_path)?.is_dir() {
            return Err(Error::InvalidConfig("watch path must be a directory"));
        }
        if self.watches.contains_key(&canonical_path) {
            self.watch_aliases
                .insert(path.to_path_buf(), canonical_path);
            return Ok(());
        }

        let queue = self.queue.clone();
        let metrics = self.metrics.clone();
        let mut watcher = notify::recommended_watcher(move |result| {
            Self::process_notify_result(result, &queue, &metrics);
        })
        .map_err(|error| Error::Notify(error.to_string()))?;
        watcher
            .watch(
                &canonical_path,
                if recursive {
                    RecursiveMode::Recursive
                } else {
                    RecursiveMode::NonRecursive
                },
            )
            .map_err(|error| Error::Notify(error.to_string()))?;
        self.watch_aliases
            .insert(path.to_path_buf(), canonical_path.clone());
        self.watches.insert(canonical_path, watcher);
        Ok(())
    }

    fn unwatch(&mut self, path: &Path) -> Result<()> {
        let canonical_path = match fs::canonicalize(path) {
            Ok(path) => path,
            Err(error) => self
                .watch_aliases
                .get(path)
                .cloned()
                .ok_or(Error::Io(error))?,
        };
        let mut watcher = self
            .watches
            .remove(&canonical_path)
            .ok_or(Error::WatchNotFound)?;
        self.watch_aliases
            .retain(|_, target| target != &canonical_path);
        watcher
            .unwatch(&canonical_path)
            .map_err(|error| Error::Notify(error.to_string()))
    }

    fn close(&mut self) -> Result<()> {
        let watches = std::mem::take(&mut self.watches);
        self.watch_aliases.clear();
        let mut first_error = None;
        for (path, mut watcher) in watches {
            if let Err(error) = watcher.unwatch(&path) {
                if first_error.is_none() {
                    first_error = Some(Error::Notify(error.to_string()));
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn watch_count(&self) -> usize {
        self.watches.len()
    }
}

fn map_event_kind(kind: NotifyEventKind) -> Option<EventKind> {
    match kind {
        NotifyEventKind::Create(_) => Some(EventKind::Created),
        NotifyEventKind::Modify(ModifyKind::Name(_)) => Some(EventKind::Renamed),
        NotifyEventKind::Modify(_) => Some(EventKind::Modified),
        NotifyEventKind::Remove(_) => Some(EventKind::Removed),
        NotifyEventKind::Any | NotifyEventKind::Other => Some(EventKind::Modified),
        NotifyEventKind::Access(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{CreateKind, RemoveKind};

    #[test]
    fn maps_notify_event_categories() {
        assert_eq!(
            map_event_kind(NotifyEventKind::Create(CreateKind::File)),
            Some(EventKind::Created)
        );
        assert_eq!(
            map_event_kind(NotifyEventKind::Remove(RemoveKind::File)),
            Some(EventKind::Removed)
        );
        assert_eq!(
            map_event_kind(NotifyEventKind::Access(notify::event::AccessKind::Read)),
            None
        );
    }
}
