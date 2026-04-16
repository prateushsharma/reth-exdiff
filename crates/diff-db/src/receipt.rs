use alloy_primitives::{Bloom, B256};
use diff_types::ReceiptArtifact;
use crate::{DbError, DbResult, DiffDb};

impl DiffDb {
    pub fn insert_receipt_artifact(&self, r: &ReceiptArtifact) -> DbResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO receipt_artifacts
             (block_number, block_hash, tx_index, tx_hash, receipt_index,
              receipt_rlp, receipt_root_anchor, log_bloom, status,
              cumulative_gas_used)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                r.block_number,
                format!("{:?}", r.block_hash),
                r.tx_index,
                format!("{:?}", r.tx_hash),
                r.receipt_index,
                r.receipt_rlp.clone(),
                format!("{:?}", r.receipt_root_anchor),
                r.log_bloom.to_vec(),
                r.status as i64,
                r.cumulative_gas_used as i64,
            ],
        )?;
        Ok(())
    }

    pub fn delete_receipt_artifact(
        &self,
        block_hash: &B256,
        tx_index: u32,
    ) -> DbResult<()> {
        self.conn.execute(
            "DELETE FROM receipt_artifacts
             WHERE block_hash = ?1 AND tx_index = ?2",
            rusqlite::params![format!("{:?}", block_hash), tx_index],
        )?;
        Ok(())
    }

    /// Look up receipt by tx_hash. Only returns if block is canonical.
    pub fn get_receipt_by_tx_hash(
        &self,
        tx_hash: &B256,
    ) -> DbResult<Option<ReceiptArtifact>> {
        let mut stmt = self.conn.prepare(
            "SELECT r.block_number, r.block_hash, r.tx_index, r.tx_hash,
                    r.receipt_index, r.receipt_rlp, r.receipt_root_anchor,
                    r.log_bloom, r.status, r.cumulative_gas_used
             FROM receipt_artifacts r
             JOIN canonical_blocks c ON r.block_hash = c.block_hash
             WHERE r.tx_hash = ?1
               AND c.canonical_status = 'active'
             LIMIT 1",
        )?;

        let result = stmt.query_row(
            rusqlite::params![format!("{:?}", tx_hash)],
            row_to_receipt_artifact,
        );

        match result {
            Ok(r)  => Ok(Some(r)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DbError::Sqlite(e)),
        }
    }

    pub fn get_receipts_for_block(
        &self,
        block_hash: &B256,
    ) -> DbResult<Vec<ReceiptArtifact>> {
        let mut stmt = self.conn.prepare(
            "SELECT block_number, block_hash, tx_index, tx_hash,
                    receipt_index, receipt_rlp, receipt_root_anchor,
                    log_bloom, status, cumulative_gas_used
             FROM receipt_artifacts
             WHERE block_hash = ?1
             ORDER BY tx_index ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![format!("{:?}", block_hash)],
            row_to_receipt_artifact,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }
}

fn row_to_receipt_artifact(
    row: &rusqlite::Row<'_>,
) -> Result<ReceiptArtifact, rusqlite::Error> {
    let block_hash_str: String  = row.get(1)?;
    let tx_hash_str: String     = row.get(3)?;
    let receipt_rlp: Vec<u8>    = row.get(5)?;
    let anchor_str: String      = row.get(6)?;
    let bloom_bytes: Vec<u8>    = row.get(7)?;
    let status: i64             = row.get(8)?;
    let gas: i64                = row.get(9)?;

    let mut bloom_arr = [0u8; 256];
    let len = bloom_bytes.len().min(256);
    bloom_arr[..len].copy_from_slice(&bloom_bytes[..len]);

    Ok(ReceiptArtifact {
        block_number:        row.get(0)?,
        block_hash:          block_hash_str.parse::<B256>().unwrap_or_default(),
        tx_index:            row.get(2)?,
        tx_hash:             tx_hash_str.parse::<B256>().unwrap_or_default(),
        receipt_index:       row.get(4)?,
        receipt_rlp,
        receipt_root_anchor: anchor_str.parse::<B256>().unwrap_or_default(),
        log_bloom:           Bloom::from(bloom_arr),
        status:              status != 0,
        cumulative_gas_used: gas as u64,
    })
}