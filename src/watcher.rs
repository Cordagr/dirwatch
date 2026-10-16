use std::path::Path;

use crate::config::EngineConfig;
use crate::engine::Engine;
use crate::error::Result;
use crate::event::Event;
use crate::metrics::MetricsSnapshot;
use crate::stream::EventStream;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WatchKind {
    Recursive,
    NonRecursive,
}

pub struct Watcher {
    recursive: bool,
    engine: Engine,
}

impl Watcher {
    pub fn new() -> Result<Self> {
        Self::with_config(EngineConfig::default())
    }

    pub fn with_config(config: EngineConfig) -> Result<Self> {
        let recursive = config.recursive;
        let engine = Engine::start(config)?;
        Ok(Self { recursive, engine })
    }

    pub fn watch(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.engine.watch(path, self.recursive)
    }

    pub fn watch_with_kind(&mut self, path: impl AsRef<Path>, kind: WatchKind) -> Result<()> {
        self.engine.watch(path, kind == WatchKind::Recursive)
    }

    pub fn unwatch(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.engine.unwatch(path)
    }

    pub fn next(&self) -> Result<Option<Event>> {
        self.engine.next()
    }

    pub fn next_timeout(&self, timeout: std::time::Duration) -> Result<Option<Event>> {
        self.engine.next_timeout(timeout)
    }

    pub fn try_next(&self) -> Result<Option<Event>> {
        self.engine.try_next()
    }

    /// Returns a blocking iterator over events from the watcher's queue.
    ///
    /// The stream owns a handle to the shared queue rather than borrowing the
    /// watcher. Shutting down or dropping the watcher closes the queue.
    pub fn iter(&self) -> EventStream {
        EventStream::new(self.engine.queue(), self.engine.metrics_recorder())
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.engine.metrics()
    }

    pub fn shutdown(&mut self) -> Result<()> {
        self.engine.shutdown()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{Duration, Instant};

    #[test]
    fn watches_a_directory_and_delivers_a_created_file() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let file = directory.path().join("observed.txt");
        let mut watcher = Watcher::new()?;
        watcher.watch(directory.path())?;
        fs::write(&file, "test")?;

        let deadline = Instant::now() + Duration::from_secs(5);
        let created = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break false;
            }
            match watcher.next_timeout(remaining)? {
                Some(event) => {
                    let is_created = event.kind == crate::event::EventKind::Created;
                    let event_file_name = event.path.as_deref().and_then(|path| path.file_name());
                    if is_created && event_file_name == file.file_name() {
                        break true;
                    }
                }
                None => break false,
            }
        };
        watcher.shutdown()?;
        assert!(created, "backend did not deliver the file creation event");
        Ok(())
    }
}
