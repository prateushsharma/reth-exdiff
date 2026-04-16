use diff_types::StageCheckpoint;
use crate::{DbError, DbResult, DiffDb};

impl DiffDb {
    /// Write a new checkpoint row.
    /// Never updates — always inserts a new row.
    /// Latest row by id is the current checkpoint.
    pub fn insert_checkpoint(&self, cp: &StageCheckpoint) -> DbResult<i64> {
        self.conn.execute(
            "INSERT INTO stage_checkpoints
             (streaming_cursor, durable_cursor, compacted_until,
              proof_indexed_until, canonical_tip, exex_finished_height)
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                cp.streaming_cursor,
                cp.durable_cursor,
                cp.compacted_until,
                cp.proof_indexed_until,
                cp.canonical_tip,
                cp.exex_finished_height,
            ],
        )?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Load the most recent checkpoint.
    /// Returns genesis checkpoint if table is empty.
    pub fn get_latest_checkpoint(&self) -> DbResult<StageCheckpoint> {
        let mut stmt = self.conn.prepare(
            "SELECT id, streaming_cursor, durable_cursor,
                    compacted_until, proof_indexed_until,
                    canonical_tip, exex_finished_height
             FROM stage_checkpoints
             ORDER BY id DESC
             LIMIT 1",
        )?;

        let result = stmt.query_row([], |row| {
            Ok(StageCheckpoint {
                id:                   Some(row.get(0)?),
                streaming_cursor:     row.get(1)?,
                durable_cursor:       row.get(2)?,
                compacted_until:      row.get(3)?,
                proof_indexed_until:  row.get(4)?,
                canonical_tip:        row.get(5)?,
                exex_finished_height: row.get(6)?,
            })
        });

        match result {
            Ok(cp) => Ok(cp),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                tracing::info!("no checkpoint found, returning genesis");
                Ok(StageCheckpoint::genesis())
            }
            Err(e) => Err(DbError::Sqlite(e)),
        }
    }
}