use std::error::Error;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use dirwatch::{BackpressureOptions, EngineConfig};

#[derive(Parser)]
#[command(
    name = "dirwatch",
    version,
    about = "Watch filesystem changes and emit normalized events asynchronously"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Subcommand)]
pub(crate) enum Command {
    Watch(WatchArgs),
    Validate(ConfigArgs),
}

#[derive(Args, Clone)]
pub(crate) struct ConfigArgs {
    #[arg(long, default_value_t = EngineConfig::default().queue_capacity_bytes)]
    queue_capacity_bytes: usize,

    #[arg(long, value_enum, default_value = "coalesce")]
    backpressure: BackpressureArg,

    #[arg(long)]
    non_recursive: bool,

    #[arg(
        long,
        default_value_t = EngineConfig::default().coalesce_window.as_millis() as u64
    )]
    coalesce_window_ms: u64,

    #[arg(long, default_value_t = EngineConfig::default().coalesce_max_entries)]
    coalesce_max_entries: usize,

    #[arg(long, default_value_t = EngineConfig::default().max_watches)]
    max_watches: usize,

    #[arg(long, default_value_t = EngineConfig::default().shutdown_timeout.as_secs())]
    pub(crate) shutdown_timeout_secs: u64,
}

#[derive(Args)]
pub(crate) struct WatchArgs {
    #[arg(required = true, value_name = "PATH")]
    pub(crate) paths: Vec<PathBuf>,

    #[command(flatten)]
    pub(crate) config: ConfigArgs,

    #[arg(long = "event", value_enum)]
    pub(crate) events: Vec<EventFilter>,

    #[arg(long = "include", value_name = "GLOB")]
    pub(crate) include: Vec<String>,

    #[arg(long = "exclude", value_name = "GLOB")]
    pub(crate) exclude: Vec<String>,

    #[arg(long, value_enum, default_value = "text")]
    pub(crate) format: OutputFormat,

    #[arg(long, value_name = "FILE")]
    pub(crate) output: Option<PathBuf>,

    #[arg(long)]
    pub(crate) max_events: Option<u64>,

    #[arg(long)]
    pub(crate) timeout_secs: Option<u64>,

    #[arg(long)]
    pub(crate) summary: bool,
}

#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum BackpressureArg {
    Block,
    DropNewest,
    DropOldest,
    Coalesce,
    OverflowSignal,
}

#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "snake_case")]
pub(crate) enum EventFilter {
    Created,
    Modified,
    Removed,
    Renamed,
    Overflow,
}

#[derive(Clone, Copy, ValueEnum)]
#[value(rename_all = "lower")]
pub(crate) enum OutputFormat {
    Text,
    Json,
}

pub(crate) fn build_config(args: &ConfigArgs) -> Result<EngineConfig, Box<dyn Error>> {
    let policy = match args.backpressure {
        BackpressureArg::Block => BackpressureOptions::Block,
        BackpressureArg::DropNewest => BackpressureOptions::DropNewest,
        BackpressureArg::DropOldest => BackpressureOptions::DropOldest,
        BackpressureArg::Coalesce => BackpressureOptions::Coalesce,
        BackpressureArg::OverflowSignal => BackpressureOptions::OverflowSignal,
    };
    Ok(EngineConfig::builder()
        .queue_capacity_bytes(args.queue_capacity_bytes)
        .backpressure(policy)
        .recursive(!args.non_recursive)
        .coalesce_window(Duration::from_millis(args.coalesce_window_ms))
        .coalesce_max_entries(args.coalesce_max_entries)
        .max_watches(args.max_watches)
        .shutdown_timeout(Duration::from_secs(args.shutdown_timeout_secs))
        .build()?)
}
