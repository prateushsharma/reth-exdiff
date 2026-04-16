//! Output types for the receipt proof subsystem.
//!
//! These types are serializable so they can be returned over HTTP/gRPC
//! by reth-proof-rpc.

use alloy_primitives::{BlockHash, BlockNumber, Bytes, B256};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Canonicality anchor
// ---------------------------------------------------------------------------

/// Local node's attestation that a block was canonical at proof-generation time.
///
/// This is NOT a consensus-layer proof. It is a local claim from the execution
/// client: "when I generated this proof, my node considered this block to be
/// on the canonical chain."
///
/// For use cases that require stronger guarantees, the verifier should check
/// that `local_head_number` is above the finalized checkpoint and that
/// `finalized_hint` covers the proven block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalAnchor {
    /// Block number of our node's canonical head at proof-generation time.
    pub local_head_number: BlockNumber,

    /// Block hash of our node's canonical head at proof-generation time.
    pub local_head_hash: BlockHash,

    /// Whether the proven block was on the canonical branch at generation time.
    ///
    /// This is false if the block has since been reorged out. Callers should
    /// reject proofs where this is false.
    pub block_is_canonical: bool,

    /// Block number of the last known finalized checkpoint, if any.
    ///
    /// If `Some(n)` and the proven block's number <= n, then the proven block
    /// is finalized and will never be reorged. This is the strongest claim
    /// this system can make without consensus-layer attestation.
    pub finalized_hint: Option<BlockNumber>,
}

// ---------------------------------------------------------------------------
// Receipt proof artifact
// ---------------------------------------------------------------------------

/// A complete receipt inclusion proof tied to a canonical block.
///
/// Contains everything a verifier needs to confirm that a specific receipt
/// was included in a specific block, and that block was canonical according
/// to the generating node.
///
/// ## Verification procedure (for the consumer)
///
/// 1. Confirm `canonical_anchor.block_is_canonical == true`.
/// 2. Verify the MPT proof:
///    a. Compute `key = rlp_encode(tx_index)` (nibble-encoded for trie traversal)
///    b. Traverse `proof_nodes` from root to leaf using the key
///    c. Check the leaf value == `receipt_rlp`
///    d. Check the reconstructed root == `receipts_root`
/// 3. Optionally confirm `receipts_root` matches the block header
///    (requires fetching the header via the node's eth_getBlockByHash).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptProof {
    /// The block this receipt belongs to.
    pub block_hash: BlockHash,

    /// Block number (height) of the proven block.
    pub block_number: BlockNumber,

    /// The receipts trie root from the block header.
    ///
    /// This is the anchor for MPT verification. The verifier uses this
    /// as the expected root when reconstructing the proof path.
    pub receipts_root: B256,

    /// RLP-encoded receipt (EIP-2718 typed encoding for typed transactions).
    ///
    /// This is the leaf value in the receipts trie. For legacy transactions
    /// this is plain RLP. For EIP-1559/EIP-4844 transactions this includes
    /// the transaction type prefix byte.
    pub receipt_rlp: Bytes,

    /// MPT inclusion proof nodes, ordered from root to leaf.
    ///
    /// Each element is the RLP encoding of one trie node. The verifier
    /// traverses these nodes using the nibble-encoded key derived from
    /// `tx_index`.
    ///
    /// For a block with N receipts the proof length is O(log_16(N)).
    pub proof_nodes: Vec<Bytes>,

    /// Transaction index within the block (0-based).
    ///
    /// The trie key is `rlp_encode(tx_index)`. The verifier must recompute
    /// this to traverse the proof.
    pub tx_index: u64,

    /// Local canonicality attestation at proof-generation time.
    pub canonical_anchor: CanonicalAnchor,
}