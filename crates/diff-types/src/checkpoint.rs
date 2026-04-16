use alloy_primitives::BlockNumber;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// The four progress cursors for the pipeline.
///
/// Invariant that must always hold:
///   proof_indexed_until <= compacted_until <= durable_cursor <= streaming_cursor
///
/// Never advance a cursor until the work it represents is durably written.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StageCheckpoint {
    /// Auto-incremented in the database.
    pub id: Option<i64>,

    /// Highest block the ExEx has seen in memory.
    /// NOT durable — do not trust after restart.
    pub streaming_cursor: BlockNumber,

    /// Highest block whose diffs are durably written to SQLite.
    /// Safe to resume from after restart.
    pub durable_cursor: BlockNumber,

    /// Highest block the compaction stage has indexed.
    pub compacted_until: BlockNumber,

    /// Highest block for which proof blobs are cached.
    pub proof_indexed_until: BlockNumber,

    /// The canonical tip block number at checkpoint time.
    pub canonical_tip: BlockNumber,

    /// The ExEx FinishedHeight value at checkpoint time.
    pub exex_finished_height: BlockNumber,
}

impl StageCheckpoint {
    /// Returns true if all four cursors satisfy the ordering invariant.
    pub fn is_consistent(&self) -> bool {
        self.proof_indexed_until <= self.compacted_until
            && self.compacted_until <= self.durable_cursor
            && self.durable_cursor <= self.streaming_cursor
    }

    /// Returns a new zeroed checkpoint. Used on first startup.
    pub fn genesis() -> Self {
        Self {
            id:                   None,
            streaming_cursor:     0,
            durable_cursor:       0,
            compacted_until:      0,
            proof_indexed_until:  0,
            canonical_tip:        0,
            exex_finished_height: 0,
        }
    }
}