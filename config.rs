use std::time::Duration;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackpressureOptions {
    Block,
    DropNewest,
    DropOldest,
    Coalesce,
    OverflowSignal,
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub queue_capacity_bytes: usize,
    pub backpressure: BackpressureOptions,
    pub recursive: bool,
    pub coalesce_window: Duration,
    pub coalesce_max_entries: usize,
    pub shutdown_timeout: Duration,
    pub max_watches: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            queue_capacity_bytes: 4 * 1024 * 1024,
            backpressure: BackpressureOptions::Coalesce,
            recursive: true,
            coalesce_window: Duration::from_millis(50),
            coalesce_max_entries: 4096,
            shutdown_timeout: Duration::from_secs(20),
            max_watches: 1024,
        }
    }
}

impl EngineConfig {
    pub fn validate(&self) -> Result<()> {
        if self.queue_capacity_bytes < crate::event::Event::overflow(None).size_bytes() {
            return Err(Error::InvalidConfig(
                "queue_capacity_bytes must fit an overflow event",
            ));
        }
        if self.coalesce_max_entries == 0 {
            return Err(Error::InvalidConfig(
                "coalesce_max_entries must be greater than zero",
            ));
        }
        if self.shutdown_timeout.is_zero() {
            return Err(Error::InvalidConfig(
                "shutdown_timeout must be greater than zero",
            ));
        }
        if self.max_watches == 0 {
            return Err(Error::InvalidConfig(
                "max_watches must be greater than zero",
            ));
        }
        Ok(())
    }

    pub fn builder() -> EngineConfigBuilder {
        EngineConfigBuilder::default()
    }
}

#[derive(Clone, Debug, Default)]
pub struct EngineConfigBuilder {
    config: EngineConfig,
}

impl EngineConfigBuilder {
    pub fn queue_capacity_bytes(mut self, capacity: usize) -> Self {
        self.config.queue_capacity_bytes = capacity;
        self
    }

    pub fn backpressure(mut self, policy: BackpressureOptions) -> Self {
        self.config.backpressure = policy;
        self
    }

    pub fn recursive(mut self, recursive: bool) -> Self {
        self.config.recursive = recursive;
        self
    }

    pub fn coalesce_window(mut self, window: Duration) -> Self {
        self.config.coalesce_window = window;
        self
    }

    pub fn coalesce_max_entries(mut self, max_entries: usize) -> Self {
        self.config.coalesce_max_entries = max_entries;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    pub fn max_watches(mut self, max_watches: usize) -> Self {
        self.config.max_watches = max_watches;
        self
    }

    pub fn build(self) -> Result<EngineConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

pub fn default_engine_config() -> EngineConfig {
    EngineConfig::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_capacity_and_timeout() {
        assert!(EngineConfig::builder()
            .queue_capacity_bytes(0)
            .build()
            .is_err());
        assert!(EngineConfig::builder()
            .shutdown_timeout(Duration::ZERO)
            .build()
            .is_err());
    }

    #[test]
    fn defaults_are_valid() {
        EngineConfig::default().validate().unwrap();
    }
}
