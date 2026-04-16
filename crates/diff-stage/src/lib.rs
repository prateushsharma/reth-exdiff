//! diff-stage — custom Reth pipeline stage for diff compaction.
//!
//! Exports DiffCompactionStage and the stage ID constant.
//! Schema and index builder modules are internal.

mod index;
mod schema;
mod stage;

pub use stage::{DiffCompactionStage, DIFF_COMPACTION_STAGE_ID};