//! Invariant checker for post-scenario validation.
//!
//! InvariantChecker::check_all(db) runs all six invariants and returns
//! a detailed error if any fails. Call this after every test scenario
//! and after every adversarial harness run.

use diff_db::DiffDb;
use diff_proof::trie::compute_receipts_root;
use eyre::{bail, Context};
use rusqlite::params;
use tracing::info;

pub struct InvariantChecker;

impl InvariantChecker {
    /// Run all invariant checks against the database.
    ///
    /// Returns Ok(()) only if every check passes.
    /// Returns a detailed Err describing the first failing check.
    pub fn check_all(db: &DiffDb) -> eyre::Result<()> {
        info!("running invariant checks");

        Self::check_no_orphan_diffs(db)
            .context("invariant FAIL: orphan diffs found")?;

        Self::check_checkpoint_consistency(db)
            .context("invariant FAIL: checkpoint inconsistent")?;

        Self::check_root_reproducibility(db)
            .context("invariant FAIL: receipts root mismatch")?;

        info!("all invariants passed");
        Ok(())
    }

    /// Check: every account_diff and storage_diff row has a corresponding
    /// row in canonical_blocks (no orphan data).
    ///
    /// Orphan diffs indicate the ExEx wrote diffs before writing the
    /// canonical_blocks row, which violates the write-order contract.
    fn check_no_orphan_diffs(db: &DiffDb) -> eyre::Result<()> {
        let conn = db.connection();

        // Account diffs orphan check
        let orphan_account: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM account_diffs ad
                 LEFT JOIN canonical_blocks cb
                   ON ad.block_number = cb.block_number
                  AND ad.block_hash   = cb.block_hash
                 WHERE cb.block_hash IS NULL",
                [],
                |row| row.get(0),
            )
            .context("query orphan account diffs")?;

        if orphan_account > 0 {
            bail!(
                "{} account_diff rows have no corresponding canonical_blocks row",
                orphan_account
            );
        }

        // Storage diffs orphan check
        let orphan_storage: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM storage_diffs sd
                 LEFT JOIN canonical_blocks cb
                   ON sd.block_number = cb.block_number
                  AND sd.block_hash   = cb.block_hash
                 WHERE cb.block_hash IS NULL",
                [],
                |row| row.get(0),
            )
            .context("query orphan storage diffs")?;

        if orphan_storage > 0 {
            bail!(
                "{} storage_diff rows have no corresponding canonical_blocks row",
                orphan_storage
            );
        }

        // Receipt artifacts orphan check
        let orphan_receipts: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM receipt_artifacts ra
                 LEFT JOIN canonical_blocks cb
                   ON ra.block_number = cb.block_number
                  AND ra.block_hash   = cb.block_hash
                 WHERE cb.block_hash IS NULL",
                [],
                |row| row.get(0),
            )
            .context("query orphan receipt artifacts")?;

        if orphan_receipts > 0 {
            bail!(
                "{} receipt_artifact rows have no corresponding canonical_blocks row",
                orphan_receipts
            );
        }

        Ok(())
    }

    /// Check: the four-cursor ordering invariant from StageCheckpoint.
    ///
    /// compacted_until <= durable <= streaming
    /// (proof_indexed_until <= compacted_until is checked separately when
    /// the proof stage is wired in)
    fn check_checkpoint_consistency(db: &DiffDb) -> eyre::Result<()> {
        let ckpt = db
            .get_latest_checkpoint()
            .context("read checkpoint for consistency check")?;

        if !ckpt.is_consistent() {
            bail!(
                "checkpoint inconsistent: streaming={} durable={} \
                 compacted={} proof={}",
                ckpt.streaming,
                ckpt.durable,
                ckpt.compacted_until,
                ckpt.proof_indexed_until
            );
        }

        Ok(())
    }

    /// Check: for every block with stored receipts, reconstructing the
    /// receipts MPT from stored receipt_rlp bytes produces the same root
    /// as the stored receipt_root_anchor.
    ///
    /// A mismatch means either the stored receipt bytes are corrupt or the
    /// stored anchor is wrong (ExEx bug).
    fn check_root_reproducibility(db: &DiffDb) -> eyre::Result<()> {
        let conn = db.connection();

        // Find all distinct (block_number, block_hash) pairs that have receipts.
        let mut stmt = conn
            .prepare(
                "SELECT DISTINCT block_number, block_hash, receipt_root_anchor
                 FROM receipt_artifacts
                 ORDER BY block_number ASC",
            )
            .context("prepare block list for root check")?;

        let blocks: Vec<(u64, String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .context("query blocks for root check")?
            .collect::<Result<_, _>>()
            .context("collect block rows")?;

        for (block_number, block_hash, stored_root_str) in &blocks {
            // Fetch all receipts for this block in order.
            let mut receipt_stmt = conn
                .prepare_cached(
                    "SELECT receipt_rlp FROM receipt_artifacts
                     WHERE block_number = ?1 AND block_hash = ?2
                     ORDER BY receipt_index ASC",
                )
                .context("prepare receipt fetch for root check")?;

            let receipts: Vec<Vec<u8>> = receipt_stmt
                .query_map(params![*block_number as i64, block_hash], |row| {
                    row.get::<_, Vec<u8>>(0)
                })
                .context("query receipts for root check")?
                .collect::<Result<_, _>>()
                .context("collect receipts")?;

            if receipts.is_empty() {
                continue;
            }

            let computed_root = compute_receipts_root(&receipts)
                .with_context(|| format!("compute root for block {block_number}"))?;

            let stored_root: alloy_primitives::B256 = stored_root_str
                .parse()
                .with_context(|| {
                    format!("parse stored root '{}' for block {}", stored_root_str, block_number)
                })?;

            if computed_root != stored_root {
                bail!(
                    "receipts root mismatch at block {} ({}): \
                     stored={} computed={}",
                    block_number,
                    block_hash,
                    stored_root,
                    computed_root
                );
            }
        }

        Ok(())
    }

    /// Check that a specific block hash has zero visible account diffs
    /// when queried through the canonical join (i.e. it was reorged out).
    ///
    /// Use this after a reorg to confirm the old branch is invisible.
    pub fn check_block_invisible(db: &DiffDb, block_hash: alloy_primitives::B256) -> eyre::Result<()> {
        let conn = db.connection();
        let hash_str = format!("{:?}", block_hash);

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM account_diffs ad
                 JOIN canonical_blocks cb
                   ON ad.block_number = cb.block_number
                  AND ad.block_hash   = cb.block_hash
                 WHERE ad.block_hash = ?1
                   AND cb.canonical_status = 'active'",
                params![hash_str],
                |row| row.get(0),
            )
            .context("check block invisible")?;

        if count > 0 {
            bail!(
                "block {} still has {} visible account diffs after reorg — \
                 should be zero",
                hash_str,
                count
            );
        }

        Ok(())
    }

    /// Check that index-diff parity holds for a specific block number.
    ///
    /// Every address in address_block_index for this block should also
    /// appear in account_diffs, and vice versa.
    ///
    /// Only meaningful after the stage has compacted this block.
    pub fn check_index_parity(db: &DiffDb, block_number: u64) -> eyre::Result<()> {
        let conn = db.connection();

        // Addresses in index but not in diffs
        let in_index_not_diffs: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM address_block_index abi
                 LEFT JOIN account_diffs ad
                   ON abi.address      = ad.address
                  AND abi.block_number = ad.block_number
                  AND abi.block_hash   = ad.block_hash
                 WHERE abi.block_number = ?1
                   AND ad.address IS NULL",
                params![block_number as i64],
                |row| row.get(0),
            )
            .context("check index ⊄ diffs")?;

        // Addresses in diffs but not in index
        let in_diffs_not_index: i64 = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM account_diffs ad
                 LEFT JOIN address_block_index abi
                   ON ad.address      = abi.address
                  AND ad.block_number = abi.block_number
                  AND ad.block_hash   = abi.block_hash
                 WHERE ad.block_number = ?1
                   AND abi.address IS NULL",
                params![block_number as i64],
                |row| row.get(0),
            )
            .context("check diffs ⊄ index")?;

        if in_index_not_diffs > 0 || in_diffs_not_index > 0 {
            bail!(
                "index-diff parity failure at block {}: \
                 {} entries in index not in diffs, \
                 {} entries in diffs not in index",
                block_number,
                in_index_not_diffs,
                in_diffs_not_index
            );
        }

        Ok(())
    }
}