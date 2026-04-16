use alloy_primitives::{Address, BlockNumber, B256, U256};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Records how one storage slot changed during one specific block.
/// Keyed by (block_number, block_hash, address, slot) in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StorageDiff {
    /// The block this diff belongs to.
    pub block_number: BlockNumber,
    /// Block hash — distinguishes branches at the same height.
    pub block_hash: B256,
    /// The contract whose storage changed.
    pub address: Address,
    /// The storage slot key (32 bytes).
    pub slot: U256,
    /// Value before this block. Zero if slot was unset.
    pub old_value: U256,
    /// Value after this block. Zero if slot was cleared.
    pub new_value: U256,
}