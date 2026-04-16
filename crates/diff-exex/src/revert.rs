use alloy_primitives::BlockNumber;
use diff_db::DiffDb;
use diff_types::RevertTable;

/// Apply all revert ops for a block, undoing its diffs from the database.
/// Ops must be loaded in descending op_sequence order (which the DB query does).
/// After applying, deletes the revert ops themselves.
pub fn apply_revert(db: &DiffDb, block_number: BlockNumber) -> Result<(), diff_db::DbError> {
    let ops = db.get_revert_ops_for_block(block_number)?;

    if ops.is_empty() {
        tracing::debug!(block_number, "no revert ops found for block, skipping");
        return Ok(());
    }

    tracing::info!(
        block_number,
        op_count = ops.len(),
        "applying revert ops"
    );

    for op in &ops {
        // Parse the primary key JSON to get the identifiers we need.
        let pk: serde_json::Value = serde_json::from_str(&op.primary_key_ref)
            .map_err(|e| diff_db::DbError::Json(e))?;

        match op.table_name {
            RevertTable::AccountDiffs => {
                let block_hash = pk["block_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                let address = pk["address"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                db.delete_account_diff(&block_hash, &address)?;
            }
            RevertTable::StorageDiffs => {
                use std::str::FromStr;
                let block_hash = pk["block_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                let address = pk["address"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                let slot = alloy_primitives::U256::from_str(
                    pk["slot"].as_str().unwrap_or("0")
                ).unwrap_or_default();
                db.delete_storage_diff(&block_hash, &address, &slot)?;
            }
            RevertTable::ReceiptArtifacts => {
                let block_hash = pk["block_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                let tx_index = pk["tx_index"]
                    .as_u64()
                    .unwrap_or(0) as u32;
                db.delete_receipt_artifact(&block_hash, tx_index)?;
            }
            RevertTable::CanonicalBlocks => {
                let block_hash = pk["block_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .parse()
                    .unwrap_or_default();
                db.mark_reorged(&block_hash)?;
            }
        }
    }

    // Clean up revert ops after successful application.
    db.delete_revert_ops_for_block(block_number)?;

    tracing::info!(block_number, "revert complete");
    Ok(())
}