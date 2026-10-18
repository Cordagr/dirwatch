#![forbid(unsafe_code)]

pub mod backend;
#[cfg(feature = "sim-support")]
pub mod benchmarks;
pub mod coalesce;
pub mod config;
#[path = "client/engine.rs"]
pub mod engine;
pub mod error;
pub mod event;
pub mod metrics;
pub mod queue;
pub mod stream;
pub mod watcher;

pub use config::{default_engine_config, BackpressureOptions, EngineConfig, EngineConfigBuilder};
pub use engine::Engine;
pub use error::{Error, Result};
pub use event::{Event, EventKind, EventSet};
pub use queue::{EventQueue, PushOutcome};
pub use stream::EventStream;
pub use watcher::{WatchKind, Watcher};

pub mod prelude {
    pub use crate::{BackpressureOptions, Event, EventKind, EventSet, WatchKind, Watcher};
}

#[cfg(feature = "sim-support")]
pub mod sim_support {
    pub use crate::backend::Backend;
    pub use crate::coalesce::Coalescer;
    pub use crate::queue::{apply_policy, EventQueue, PushOutcome};
}

#[cfg(feature = "fault-injection")]
pub mod fault_injection {
    pub use crate::backend::Backend;
    pub use crate::queue::{apply_policy, EventQueue, PushOutcome};
}

#[cfg(feature = "expose-internals")]
pub mod expose_internals {
    pub use crate::backend;
    pub use crate::coalesce;
    pub use crate::config;
    pub use crate::engine;
    pub use crate::error;
    pub use crate::event;
    pub use crate::metrics;
    pub use crate::queue;
    pub use crate::stream;
    pub use crate::watcher;
}
