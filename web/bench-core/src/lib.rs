//! Workload generation and measurement for the MATCH_RECOGNIZE bench.
//!
//! This crate knows nothing about CLIs or HTTP. It is driven by `bench` (the CLI) and by
//! `bench-web` (the demo console), which must both see identical generator behaviour.

pub mod gen;
pub mod measure;
pub mod pace;
pub mod pipeline;
pub mod run;
pub mod sink;
