//! MPT trie builder for receipt inclusion proofs.
//!
//! Wraps alloy-trie's HashBuilder to construct the receipts trie from a
//! list of RLP-encoded receipts and extract a Merkle-Patricia proof for a
//! specific receipt index.
//!
//! ## Key encoding
//!
//! The receipts trie uses RLP(tx_index) as the key for each receipt.
//! - receipt 0   → key 0x80  (RLP of integer 0)
//! - receipt 1   → key 0x01
//! - receipt 127 → key 0x7f
//! - receipt 128 → key 0x8180
//!
//! Keys are then nibble-unpacked for trie traversal (each byte becomes
//! two 4-bit nibbles).

use alloy_primitives::{Bytes, B256};
use alloy_trie::{
    proof::ProofRetainer,
    HashBuilder, Nibbles,
};
use eyre::{ensure, Context};

/// Build the receipts MPT for a block and extract the inclusion proof for
/// one specific receipt.
///
/// # Arguments
///
/// * `encoded_receipts` — RLP-encoded receipts in transaction order (index 0
///   first). These are the raw bytes stored in `receipt_artifacts.receipt_rlp`.
///
/// * `target_index` — the transaction index of the receipt to prove.
///
/// # Returns
///
/// `(receipts_root, proof_nodes)` where:
/// - `receipts_root` is the keccak256 root of the full trie
/// - `proof_nodes` is the ordered list of RLP-encoded trie nodes from root
///   to the target leaf
///
/// # Errors
///
/// Returns an error if `target_index >= encoded_receipts.len()` or if the
/// trie builder encounters inconsistent input.
pub fn build_receipt_trie_and_proof(
    encoded_receipts: &[Vec<u8>],
    target_index: usize,
) -> eyre::Result<(B256, Vec<Bytes>)> {
    ensure!(
        !encoded_receipts.is_empty(),
        "cannot build receipt trie: no receipts provided"
    );
    ensure!(
        target_index < encoded_receipts.len(),
        "target_index {} out of range (block has {} receipts)",
        target_index,
        encoded_receipts.len()
    );

    // Compute the trie key for the target receipt.
    // Key = RLP encoding of the integer index.
    let target_key_rlp = alloy_rlp::encode(target_index);
    let target_nibbles = Nibbles::unpack(&target_key_rlp);

    // Construct the ProofRetainer with the single path we want a proof for.
    // ProofRetainer intercepts node hashing during trie construction and
    // retains the RLP of any node that lies on the path to target_nibbles.
    let retainer = ProofRetainer::from_iter([target_nibbles.clone()]);

    // Construct the HashBuilder with proof retention enabled.
    let mut builder = HashBuilder::default().with_proof_retainer(retainer);

    // Feed all receipts as leaves in ascending key order.
    // HashBuilder requires strictly ascending keys — the receipts are already
    // in tx order (0, 1, 2, ...) so we just iterate in order.
    for (index, receipt_rlp) in encoded_receipts.iter().enumerate() {
        let key_rlp = alloy_rlp::encode(index);
        let key_nibbles = Nibbles::unpack(&key_rlp);

        // add_leaf(path, value): path is the nibble-unpacked key,
        // value is the raw bytes of the leaf value (the encoded receipt).
        builder.add_leaf(key_nibbles, receipt_rlp.as_slice());
    }

    // Finalise: compute the root hash. This triggers hashing of all pending
    // nodes and causes the ProofRetainer to capture witness nodes.
    let root = builder.root();

    // Extract the proof nodes for our target path.
    // take_proof_nodes() returns ProofNodes — a BTreeMap<Nibbles, Bytes>
    // where the key is the node's path prefix and the value is its RLP.
    //
    // We sort by path length (root has shortest path, leaf has longest) to
    // get the nodes in root-to-leaf order for the verifier.
    let proof_nodes_map = builder.take_proof_nodes();

    // Convert to ordered Vec<Bytes> from root to leaf.
    // proof_nodes_map is already a BTreeMap keyed by Nibbles (which has
    // lexicographic ordering = path order = root-to-leaf order).
    let proof_nodes: Vec<Bytes> = proof_nodes_map
        .into_nodes_sorted()
        .into_iter()
        .map(|(_path, node_rlp)| Bytes::from(node_rlp.to_vec()))
        .collect();

    Ok((root, proof_nodes))
}

/// Compute just the receipts root without generating a proof.
///
/// Cheaper than build_receipt_trie_and_proof when you only need the root
/// for validation (e.g. checking stored receipt_root_anchor is correct).
pub fn compute_receipts_root(encoded_receipts: &[Vec<u8>]) -> eyre::Result<B256> {
    ensure!(
        !encoded_receipts.is_empty(),
        "cannot compute receipts root: empty receipt list"
    );

    let mut builder = HashBuilder::default();

    for (index, receipt_rlp) in encoded_receipts.iter().enumerate() {
        let key_rlp = alloy_rlp::encode(index);
        let key_nibbles = Nibbles::unpack(&key_rlp);
        builder.add_leaf(key_nibbles, receipt_rlp.as_slice());
    }

    Ok(builder.root())
}