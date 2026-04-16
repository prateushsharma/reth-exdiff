use alloy_primitives::{Address, BlockNumber, B256, U256};
use diff_types::StorageDiff;
use crate::{DbError, DbResult, DiffDb};
use std::str::FromStr;

impl DiffDb {
    pub fn insert_storage_diff(&self, diff: &StorageDiff) -> DbResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO storage_diffs
             (block_number, block_hash, address, slot, old_value, new_value)
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                diff.block_number,
                format!("{:?}", diff.block_hash),
                format!("{:?}", diff.address),
                diff.slot.to_string(),
                diff.old_value.to_string(),
                diff.new_value.to_string(),
            ],
        )?;
        Ok(())
    }

    pub fn delete_storage_diff(
        &self,
        block_hash: &B256,
        address: &Address,
        slot: &U256,
    ) -> DbResult<()> {
        self.conn.execute(
            "DELETE FROM storage_diffs
             WHERE block_hash = ?1 AND address = ?2 AND slot = ?3",
            rusqlite::params![
                format!("{:?}", block_hash),
                format!("{:?}", address),
                slot.to_string(),
            ],
        )?;
        Ok(())
    }

    pub fn get_storage_diffs_for_block(
        &self,
        block_hash: &B256,
    ) -> DbResult<Vec<StorageDiff>> {
        let mut stmt = self.conn.prepare(
            "SELECT block_number, block_hash, address, slot, old_value, new_value
             FROM storage_diffs
             WHERE block_hash = ?1
             ORDER BY address ASC, slot ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![format!("{:?}", block_hash)],
            row_to_storage_diff,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }

    pub fn get_storage_diffs_for_address(
        &self,
        address: &Address,
        slot: &U256,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> DbResult<Vec<StorageDiff>> {
        let mut stmt = self.conn.prepare(
            "SELECT s.block_number, s.block_hash, s.address,
                    s.slot, s.old_value, s.new_value
             FROM storage_diffs s
             JOIN canonical_blocks c ON s.block_hash = c.block_hash
             WHERE s.address = ?1
               AND s.slot    = ?2
               AND s.block_number >= ?3
               AND s.block_number <= ?4
               AND c.canonical_status = 'active'
             ORDER BY s.block_number ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![
                format!("{:?}", address),
                slot.to_string(),
                from_block,
                to_block,
            ],
            row_to_storage_diff,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }
}

fn row_to_storage_diff(
    row: &rusqlite::Row<'_>,
) -> Result<StorageDiff, rusqlite::Error> {
    let block_hash_str: String = row.get(1)?;
    let address_str: String    = row.get(2)?;
    let slot_str: String       = row.get(3)?;
    let old_str: String        = row.get(4)?;
    let new_str: String        = row.get(5)?;

    Ok(StorageDiff {
        block_number: row.get(0)?,
        block_hash:   block_hash_str.parse::<B256>().unwrap_or_default(),
        address:      address_str.parse::<Address>().unwrap_or_default(),
        slot:         U256::from_str(&slot_str).unwrap_or_default(),
        old_value:    U256::from_str(&old_str).unwrap_or_default(),
        new_value:    U256::from_str(&new_str).unwrap_or_default(),
    })
}