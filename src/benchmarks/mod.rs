use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{Error, Result};

#[derive(Default)]
pub struct BenchMetrics {
    counters: Mutex<HashMap<String, u64>>,
}

impl BenchMetrics {
    pub fn increment(&self, name: &str) -> Result<()> {
        let mut counters = self.counters.lock().map_err(|_| Error::Poisoned)?;
        *counters.entry(name.to_owned()).or_default() += 1;
        Ok(())
    }

    pub fn value(&self, name: &str) -> Result<Option<u64>> {
        Ok(self
            .counters
            .lock()
            .map_err(|_| Error::Poisoned)?
            .get(name)
            .copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_named_benchmark_events() {
        let metrics = BenchMetrics::default();
        metrics.increment("received").unwrap();
        metrics.increment("received").unwrap();
        assert_eq!(metrics.value("received").unwrap(), Some(2));
    }
}
