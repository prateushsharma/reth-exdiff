//! JSON request and response types for the three RPC endpoints.
//!
//! Request types are deserialized from the POST body.
//! Response types are serialized into the response body.
//!
//! alloy_primitives types (B256, Address, U256) serialize as lowercase
//! hex strings with 0x prefix by default, which is the Ethereum standard.

use alloy_primitives::{Address, BlockNumber, B256, U256};
use diff_proof::types::ReceiptProof;
use diff_types::{AccountDiff, StorageDiff};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// receipt_proof
// ---------------------------------------------------------------------------

/// POST /receipt_proof request body.
#[derive(Debug, Deserialize)]
pub struct ReceiptProofRequest {
    /// Transaction hash to generate a proof for.
    pub tx_hash: B256,
}

/// POST /receipt_proof response body.
///
/// The full ReceiptProof is returned directly. It contains:
///   block_hash, block_number, receipts_root, receipt_rlp,
///   proof_nodes, tx_index, canonical_anchor
#[derive(Debug, Serialize)]
pub struct ReceiptProofResponse {
    pub proof: ReceiptProof,
}

// ---------------------------------------------------------------------------
// account_diff
// ---------------------------------------------------------------------------

/// POST /account_diff request body.
#[derive(Debug, Deserialize)]
pub struct AccountDiffRequest {
    /// Ethereum address to query.
    pub address: Address,

    /// Inclusive start block number.
    pub from_block: BlockNumber,

    /// Inclusive end block number.
    pub to_block: BlockNumber,
}

/// POST /account_diff response body.
#[derive(Debug, Serialize)]
pub struct AccountDiffResponse {
    pub address:    Address,
    pub from_block: BlockNumber,
    pub to_block:   BlockNumber,
    /// All canonical account state changes in the range, ascending by block.
    pub diffs:      Vec<AccountDiff>,
}

// ---------------------------------------------------------------------------
// storage_diff
// ---------------------------------------------------------------------------

/// POST /storage_diff request body.
#[derive(Debug, Deserialize)]
pub struct StorageDiffRequest {
    /// Contract address.
    pub address: Address,

    /// Storage slot key (32-byte value).
    pub slot: U256,

    /// Inclusive start block number.
    pub from_block: BlockNumber,

    /// Inclusive end block number.
    pub to_block: BlockNumber,
}

/// POST /storage_diff response body.
#[derive(Debug, Serialize)]
pub struct StorageDiffResponse {
    pub address:    Address,
    pub slot:       U256,
    pub from_block: BlockNumber,
    pub to_block:   BlockNumber,
    /// All canonical storage slot changes in the range, ascending by block.
    pub diffs:      Vec<StorageDiff>,
}