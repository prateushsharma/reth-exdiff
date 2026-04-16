use alloy_primitives::BlockNumber;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Which table this revert operation targets.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RevertTable {
    AccountDiffs,
    StorageDiffs,
    ReceiptArtifacts,
    CanonicalBlocks,
}

impl RevertTable {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AccountDiffs      => "account_diffs",
            Self::StorageDiffs      => "storage_diffs",
            Self::ReceiptArtifacts  => "receipt_artifacts",
            Self::CanonicalBlocks   => "canonical_blocks",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "account_diffs"     => Some(Self::AccountDiffs),
            "storage_diffs"     => Some(Self::StorageDiffs),
            "receipt_artifacts" => Some(Self::ReceiptArtifacts),
            "canonical_blocks"  => Some(Self::CanonicalBlocks),
            _                   => None,
        }
    }
}

/// One undo operation recorded at commit time, replayed at reorg time.
/// All revert ops for a block are applied in reverse op_sequence order.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RevertOp {
    /// Auto-incremented in the database.
    pub id:                 Option<i64>,
    /// The block whose commit produced this revert op.
    pub reorg_target_block: BlockNumber,
    /// Execution order. Apply in descending order on revert.
    pub op_sequence:        i64,
    /// Which table to touch.
    pub table_name:         RevertTable,
    /// JSON-encoded primary key identifying the row to delete.
    /// e.g. {"block_hash":"0xabc...","address":"0x123..."}
    pub primary_key_ref:    String,
    /// JSON-encoded previous state to restore.
    /// Empty string for pure-delete operations (created rows).
    pub inverse_payload:    String,
}