//! ProofExtractor — generates ReceiptProof from DiffDb contents.
//!
//! Reads receipt artifacts stored by the ExEx, reconstructs the receipts MPT,
//! generates the inclusion proof for a target receipt, and attaches a local
//! canonicality anchor.

use std::sync::Arc;

use alloy_primitives::{BlockHash, BlockNumber, B256};
use diff_db::DiffDb;
use eyre::{bail, Context};
use rusqlite::params;
use tracing::{debug, info};

use crate::{
    trie::build_receipt_trie_and_proof,
    types::{CanonicalAnchor, ReceiptProof},
};

/// Generates receipt inclusion proofs from the derived diff database.
///
/// Owned by reth-proof-rpc. Reads from DiffDb; never writes.
pub struct ProofExtractor {
    db: Arc<DiffDb>,
}

impl ProofExtractor {
    pub fn new(db: Arc<DiffDb>) -> Self {
        Self { db }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Generate a receipt inclusion proof for a transaction identified by hash.
    ///
    /// Looks up the receipt in receipt_artifacts (canonical blocks only),
    /// fetches all receipts for the same block, reconstructs the receipts MPT,
    /// extracts the inclusion proof, and attaches a canonicality anchor.
    ///
    /// # Errors
    ///
    /// - Receipt not found (not indexed, or block is non-canonical)
    /// - Block has no canonical entry in canonical_blocks
    /// - MPT root mismatch between stored anchor and reconstructed root
    pub fn get_receipt_proof_by_tx_hash(
        &self,
        tx_hash: B256,
    ) -> eyre::Result<ReceiptProof> {
        let conn = self.db.connection();

        // ------------------------------------------------------------------
        // Step 1: look up the target receipt in receipt_artifacts.
        //
        // We join canonical_blocks to ensure we only return proofs for
        // canonical receipts. If this block was reorged, the query returns
        // nothing and we bail with a clear error.
        // ------------------------------------------------------------------
        let tx_hash_hex = format!("{:?}", tx_hash);

        let row: Option<(i64, String, i64, Vec<u8>, String)> = conn
            .query_row(
                "SELECT ra.block_number, ra.block_hash, ra.tx_index,
                        ra.receipt_rlp, ra.receipt_root_anchor
                 FROM receipt_artifacts ra
                 JOIN canonical_blocks cb
                   ON ra.block_number = cb.block_number
                  AND ra.block_hash   = cb.block_hash
                 WHERE ra.tx_hash = ?1
                   AND cb.canonical_status = 'active'",
                params![tx_hash_hex],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, Vec<u8>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()
            .context("query receipt_artifacts for tx_hash")?;

        let (block_number_i64, block_hash_str, tx_index_i64, _target_rlp, root_anchor_str) =
            match row {
                Some(r) => r,
                None => bail!(
                    "receipt not found or block is non-canonical for tx_hash {}",
                    tx_hash_hex
                ),
            };

        let block_number = block_number_i64 as u64;
        let tx_index = tx_index_i64 as usize;

        // Parse the stored receipts_root anchor (hex string → B256).
        let receipts_root: B256 = root_anchor_str
            .parse()
            .with_context(|| format!("parse receipts_root anchor: {}", root_anchor_str))?;

        let block_hash: BlockHash = block_hash_str
            .parse()
            .with_context(|| format!("parse block_hash: {}", block_hash_str))?;

        info!(
            block_number,
            tx_index,
            "generating receipt proof"
        );

        // ------------------------------------------------------------------
        // Step 2: fetch ALL receipts for this block (canonical only), sorted
        // by receipt_index ascending. We need the full ordered list to
        // reconstruct the trie.
        // ------------------------------------------------------------------
        let all_receipts = self.fetch_all_receipts_for_block(block_number, &block_hash_str)
            .with_context(|| format!("fetch receipts for block {block_number}"))?;

        if all_receipts.is_empty() {
            bail!("no receipts found for block {} ({})", block_number, block_hash_str);
        }

        debug!(
            block_number,
            receipt_count = all_receipts.len(),
            target_tx_index = tx_index,
            "fetched receipts for trie construction"
        );

        // ------------------------------------------------------------------
        // Step 3: build the receipts MPT and extract the inclusion proof.
        // ------------------------------------------------------------------
        let (computed_root, proof_nodes) =
            build_receipt_trie_and_proof(&all_receipts, tx_index)
                .context("build receipt trie and proof")?;

        // ------------------------------------------------------------------
        // Step 4: validate that the computed root matches the stored anchor.
        //
        // If these differ it means either:
        // (a) the ExEx stored a wrong receipts_root_anchor (bug in ExEx), or
        // (b) the stored receipt_rlp bytes are corrupt.
        //
        // We treat this as a fatal error — do not return a proof with a
        // mismatched root, it would be unprovable.
        // ------------------------------------------------------------------
        if computed_root != receipts_root {
            bail!(
                "receipts_root mismatch for block {}: \
                 stored anchor {} != computed {}",
                block_number,
                receipts_root,
                computed_root
            );
        }

        debug!(block_number, root = %computed_root, "receipts root validated");

        // ------------------------------------------------------------------
        // Step 5: build the canonicality anchor.
        // ------------------------------------------------------------------
        let anchor = self.build_canonical_anchor(block_number, &block_hash_str)
            .context("build canonical anchor")?;

        // The target receipt RLP from the all_receipts list.
        let receipt_rlp = alloy_primitives::Bytes::from(all_receipts[tx_index].clone());

        Ok(ReceiptProof {
            block_hash,
            block_number,
            receipts_root,
            receipt_rlp,
            proof_nodes,
            tx_index: tx_index as u64,
            canonical_anchor: anchor,
        })
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Fetch all receipt RLP bytes for a block in receipt_index order.
    ///
    /// Returns a Vec where index i is the RLP of the receipt at tx position i.
    fn fetch_all_receipts_for_block(
        &self,
        block_number: u64,
        block_hash: &str,
    ) -> eyre::Result<Vec<Vec<u8>>> {
        let conn = self.db.connection();

        let mut stmt = conn
            .prepare_cached(
                "SELECT receipt_rlp
                 FROM receipt_artifacts
                 WHERE block_number = ?1 AND block_hash = ?2
                 ORDER BY receipt_index ASC",
            )
            .context("prepare receipt fetch")?;

        let rows: Vec<Vec<u8>> = stmt
            .query_map(params![block_number as i64, block_hash], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .context("query receipts")?
            .collect::<Result<_, _>>()
            .context("collect receipt rows")?;

        Ok(rows)
    }

    /// Build a CanonicalAnchor for a block.
    ///
    /// Queries canonical_blocks for the block's status and finds the current
    /// canonical head (max block_number with status='active').
    fn build_canonical_anchor(
        &self,
        proven_block_number: BlockNumber,
        proven_block_hash: &str,
    ) -> eyre::Result<CanonicalAnchor> {
        let conn = self.db.connection();

        // Check the proven block's canonical status.
        let status: Option<String> = conn
            .query_row(
                "SELECT canonical_status FROM canonical_blocks
                 WHERE block_number = ?1 AND block_hash = ?2",
                params![proven_block_number as i64, proven_block_hash],
                |row| row.get(0),
            )
            .optional()
            .context("query canonical status of proven block")?;

        let block_is_canonical = status.as_deref() == Some("active");

        // Find the current canonical head: highest block_number with active status.
        let (local_head_number, local_head_hash_str): (i64, String) = conn
            .query_row(
                "SELECT block_number, block_hash FROM canonical_blocks
                 WHERE canonical_status = 'active'
                 ORDER BY block_number DESC
                 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .context("query canonical head")?;

        let local_head_hash: BlockHash = local_head_hash_str
            .parse()
            .context("parse local head hash")?;

        // Check finalized_hint: if proven block <= last finalized block number,
        // set finalized_hint. We read this from canonical_blocks.finalized_hint
        // column (set by ExEx when the consensus client sends finalization info).
        let finalized_hint: Option<BlockNumber> = conn
            .query_row(
                "SELECT MAX(finalized_hint) FROM canonical_blocks
                 WHERE finalized_hint IS NOT NULL",
                [],
                |row| row.get::<_, Option<i64>>(0),
            )
            .optional()
            .context("query finalized hint")?
            .flatten()
            .map(|n| n as u64);

        Ok(CanonicalAnchor {
            local_head_number: local_head_number as u64,
            local_head_hash,
            block_is_canonical,
            finalized_hint,
        })
    }
}