use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageCheckpoint {
    pub checkpoint_id:       u64,
    /// ExEx in-memory cursor — not persisted, resets on restart
    pub streaming:           u64,
    /// ExEx durably written cursor — resume point on restart
    pub durable:             u64,
    /// Stage compaction cursor
    pub compacted_until:     u64,
    /// Proof cache cursor
    pub proof_indexed_until: u64,
    /// Last known safe block from consensus layer
    pub safe_block:          Option<u64>,
    /// Current canonical tip
    pub canonical_tip:       Option<u64>,
    /// Mirrors durable — sent to Reth pruner via FinishedHeight
    pub exex_finished_height: u64,
}

impl StageCheckpoint {
    /// Genesis / empty state checkpoint.
    pub fn genesis() -> Self {
        Self {
            checkpoint_id:        0,
            streaming:            0,
            durable:              0,
            compacted_until:      0,
            proof_indexed_until:  0,
            safe_block:           None,
            canonical_tip:        None,
            exex_finished_height: 0,
        }
    }

    /// Check the four-cursor ordering invariant.
    ///
    /// proof_indexed_until ≤ compacted_until ≤ durable ≤ streaming
    pub fn is_consistent(&self) -> bool {
        self.proof_indexed_until <= self.compacted_until
            && self.compacted_until     <= self.durable
            && self.durable             <= self.streaming
    }
}

impl Default for StageCheckpoint {
    fn default() -> Self { Self::genesis() }
}