use alloy_primitives::BlockNumber;
use diff_types::{RevertOp, RevertTable};
use crate::{DbError, DbResult, DiffDb};

impl DiffDb {
    pub fn insert_revert_op(&self, op: &RevertOp) -> DbResult<()> {
        self.conn.execute(
            "INSERT INTO revert_ops
             (reorg_target_block, op_sequence, table_name,
              primary_key_ref, inverse_payload)
             VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![
                op.reorg_target_block,
                op.op_sequence,
                op.table_name.as_str(),
                op.primary_key_ref,
                op.inverse_payload,
            ],
        )?;
        Ok(())
    }

    /// Load all revert ops for a block in reverse sequence order.
    /// This is the correct order to apply them — last written, first undone.
    pub fn get_revert_ops_for_block(
        &self,
        block: BlockNumber,
    ) -> DbResult<Vec<RevertOp>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, reorg_target_block, op_sequence,
                    table_name, primary_key_ref, inverse_payload
             FROM revert_ops
             WHERE reorg_target_block = ?1
             ORDER BY op_sequence DESC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![block],
            |row| {
                let table_str: String = row.get(3)?;
                Ok(RevertOp {
                    id:                 Some(row.get(0)?),
                    reorg_target_block: row.get(1)?,
                    op_sequence:        row.get(2)?,
                    table_name:         RevertTable::from_str(&table_str)
                                            .unwrap_or(RevertTable::AccountDiffs),
                    primary_key_ref:    row.get(4)?,
                    inverse_payload:    row.get(5)?,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }

    /// Delete all revert ops for a block after they have been applied.
    /// Called after successful reorg revert to keep the table clean.
    pub fn delete_revert_ops_for_block(
        &self,
        block: BlockNumber,
    ) -> DbResult<()> {
        self.conn.execute(
            "DELETE FROM revert_ops WHERE reorg_target_block = ?1",
            rusqlite::params![block],
        )?;
        Ok(())
    }
}