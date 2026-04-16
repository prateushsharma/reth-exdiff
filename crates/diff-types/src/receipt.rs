use alloy_primitives::{BlockNumber, Bloom, B256};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// All data needed to generate and verify a receipt inclusion proof.
/// Stored once per transaction per block.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ReceiptArtifact {
    pub block_number:       BlockNumber,
    pub block_hash:         B256,
    /// Position of the transaction in the block (0-indexed).
    pub tx_index:           u32,
    /// Transaction hash — primary lookup key for users.
    pub tx_hash:            B256,
    /// Same as tx_index for non-EIP-2930 blocks.
    pub receipt_index:      u32,
    /// Raw RLP encoding of the receipt. This is the trie leaf value.
    pub receipt_rlp:        Vec<u8>,
    /// The receiptsRoot from the block header. Proof anchor.
    pub receipt_root_anchor: B256,
    /// Logs bloom for this receipt.
    pub log_bloom:          Bloom,
    /// true = success, false = revert.
    pub status:             bool,
    pub cumulative_gas_used: u64,
}