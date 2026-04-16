use alloy_primitives::{BlockNumber, B256};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Whether this block is currently on the canonical chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum CanonicalStatus {
    /// Currently on the canonical chain.
    Active,
    /// Was canonical but got replaced by a reorg.
    Reorged,
}

impl CanonicalStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active  => "active",
            Self::Reorged => "reorged",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "active"  => Some(Self::Active),
            "reorged" => Some(Self::Reorged),
            _         => None,
        }
    }
}

/// One entry in the canonical_blocks table.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CanonicalBlock {
    pub block_number:          BlockNumber,
    pub block_hash:            B256,
    pub parent_hash:           B256,
    pub canonical_status:      CanonicalStatus,
    /// Set when the consensus client marks this block finalized.
    /// None until we receive that signal.
    pub finalized_hint:        Option<bool>,
    /// Which stage checkpoint covered this block.
    /// None until the compaction stage processes it.
    pub derived_checkpoint_id: Option<i64>,
}