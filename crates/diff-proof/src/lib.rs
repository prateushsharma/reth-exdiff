//! diff-proof — receipt MPT inclusion proofs with canonicality anchors.
//!
//! Public API:
//!   ProofExtractor::get_receipt_proof_by_tx_hash(tx_hash) -> ReceiptProof
//!
//! Types:
//!   ReceiptProof, CanonicalAnchor

mod trie;
mod extractor;
pub mod types;

pub use extractor::ProofExtractor;
pub use types::{CanonicalAnchor, ReceiptProof};