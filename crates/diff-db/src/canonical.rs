use alloy_primitives::{BlockNumber, B256};
use diff_types::{CanonicalBlock, CanonicalStatus};
use crate::{DbError, DbResult, DiffDb};

impl DiffDb {
    /// Insert a new canonical block record.
    /// Call this when processing ChainCommitted for each block.
    pub fn insert_canonical_block(&self, block: &CanonicalBlock) -> DbResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO canonical_blocks
             (block_number, block_hash, parent_hash, canonical_status,
              finalized_hint, derived_checkpoint_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                block.block_number,
                format!("{:?}", block.block_hash),
                format!("{:?}", block.parent_hash),
                block.canonical_status.as_str(),
                block.finalized_hint.map(|b| b as i64),
                block.derived_checkpoint_id,
            ],
        )?;
        Ok(())
    }

    /// Flip a block's status from Active to Reorged.
    /// Called when processing ChainReorged for each old block.
    pub fn mark_reorged(&self, block_hash: &B256) -> DbResult<()> {
        let affected = self.conn.execute(
            "UPDATE canonical_blocks
             SET canonical_status = 'reorged'
             WHERE block_hash = ?1",
            rusqlite::params![format!("{:?}", block_hash)],
        )?;

        if affected == 0 {
            tracing::warn!(
                block_hash = ?block_hash,
                "mark_reorged: block not found in canonical_blocks"
            );
        }
        Ok(())
    }

    /// Get a block by its hash.
    pub fn get_canonical_block(&self, block_hash: &B256) -> DbResult<Option<CanonicalBlock>> {
        let mut stmt = self.conn.prepare(
            "SELECT block_number, block_hash, parent_hash, canonical_status,
                    finalized_hint, derived_checkpoint_id
             FROM canonical_blocks
             WHERE block_hash = ?1",
        )?;

        let result = stmt.query_row(
            rusqlite::params![format!("{:?}", block_hash)],
            row_to_canonical_block,
        );

        match result {
            Ok(block) => Ok(Some(block)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(DbError::Sqlite(e)),
        }
    }

    /// Get all currently active canonical blocks above a given number.
    /// Used by the compaction stage to find unprocessed blocks.
    pub fn get_active_blocks_above(
        &self,
        above: BlockNumber,
    ) -> DbResult<Vec<CanonicalBlock>> {
        let mut stmt = self.conn.prepare(
            "SELECT block_number, block_hash, parent_hash, canonical_status,
                    finalized_hint, derived_checkpoint_id
             FROM canonical_blocks
             WHERE block_number > ?1
               AND canonical_status = 'active'
             ORDER BY block_number ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![above],
            row_to_canonical_block,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }
}

fn row_to_canonical_block(
    row: &rusqlite::Row<'_>,
) -> Result<CanonicalBlock, rusqlite::Error> {
    let block_hash_str: String = row.get(1)?;
    let parent_hash_str: String = row.get(2)?;
    let status_str: String = row.get(3)?;
    let finalized_hint: Option<i64> = row.get(4)?;

    let block_hash = block_hash_str.parse::<B256>()
        .unwrap_or_default();
    let parent_hash = parent_hash_str.parse::<B256>()
        .unwrap_or_default();
    let canonical_status = CanonicalStatus::from_str(&status_str)
        .unwrap_or(CanonicalStatus::Active);

    Ok(CanonicalBlock {
        block_number: row.get(0)?,
        block_hash,
        parent_hash,
        canonical_status,
        finalized_hint: finalized_hint.map(|v| v != 0),
        derived_checkpoint_id: row.get(5)?,
    })
}