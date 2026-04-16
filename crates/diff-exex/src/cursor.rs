use alloy_primitives::BlockNumber;
use diff_db::DiffDb;
use diff_types::StageCheckpoint;

/// Manages the four progress cursors for the ExEx layer.
///
/// Cursors in order of trust:
///   streaming  — seen in memory, not yet durable. RESET ON RESTART.
///   durable    — committed to SQLite. SAFE TO RESUME FROM.
///   compacted  — processed by the compaction stage.
///   proof      — proof blobs generated.
///
/// Invariant: proof <= compacted <= durable <= streaming
#[derive(Debug, Clone)]
pub struct ExExCursor {
    /// Highest block seen in current session. In-memory only.
    pub streaming: BlockNumber,
    /// Highest block durably written. Loaded from DB on startup.
    pub durable: BlockNumber,
    /// Highest block compacted. Loaded from DB on startup.
    pub compacted: BlockNumber,
    /// Highest block with proof blobs. Loaded from DB on startup.
    pub proof: BlockNumber,
    /// Current canonical tip.
    pub canonical_tip: BlockNumber,
    /// Last emitted FinishedHeight value.
    pub finished_height: BlockNumber,
}

impl ExExCursor {
    /// Load cursor state from the latest checkpoint in the database.
    /// If no checkpoint exists, returns genesis (all zeros).
    pub fn load_from_db(db: &DiffDb) -> Result<Self, diff_db::DbError> {
        let cp = db.get_latest_checkpoint()?;
        tracing::info!(
            durable = cp.durable_cursor,
            compacted = cp.compacted_until,
            proof = cp.proof_indexed_until,
            "loaded cursor from checkpoint"
        );
        Ok(Self {
            // Streaming always resets to durable on startup.
            // We cannot trust in-memory progress from a previous session.
            streaming:      cp.durable_cursor,
            durable:        cp.durable_cursor,
            compacted:      cp.compacted_until,
            proof:          cp.proof_indexed_until,
            canonical_tip:  cp.canonical_tip,
            finished_height: cp.exex_finished_height,
        })
    }

    /// Advance the streaming cursor. Called after processing a block in memory.
    /// Does NOT write to database.
    pub fn advance_streaming(&mut self, block: BlockNumber) {
        if block > self.streaming {
            self.streaming = block;
        }
    }

    /// Advance the durable cursor and write a checkpoint to the database.
    /// Called only after SQLite writes are confirmed.
    pub fn advance_durable(
        &mut self,
        block: BlockNumber,
        canonical_tip: BlockNumber,
        finished_height: BlockNumber,
        db: &DiffDb,
    ) -> Result<(), diff_db::DbError> {
        self.durable        = block;
        self.canonical_tip  = canonical_tip;
        self.finished_height = finished_height;

        let cp = StageCheckpoint {
            id:                   None,
            streaming_cursor:     self.streaming,
            durable_cursor:       self.durable,
            compacted_until:      self.compacted,
            proof_indexed_until:  self.proof,
            canonical_tip:        self.canonical_tip,
            exex_finished_height: self.finished_height,
        };

        // Panic if invariant is broken — this is a programming error not a runtime error.
        assert!(
            cp.is_consistent(),
            "cursor invariant broken: proof={} compacted={} durable={} streaming={}",
            self.proof, self.compacted, self.durable, self.streaming
        );

        db.insert_checkpoint(&cp)?;
        tracing::debug!(block, "durable cursor advanced and checkpoint written");
        Ok(())
    }

    /// Roll back durable cursor to a given block during reorg.
    /// Writes a new checkpoint reflecting the rollback.
    pub fn rollback_to(
        &mut self,
        block: BlockNumber,
        db: &DiffDb,
    ) -> Result<(), diff_db::DbError> {
        tracing::info!(
            from = self.durable,
            to   = block,
            "rolling back cursor"
        );
        self.streaming      = block;
        self.durable        = block;
        self.canonical_tip  = block;

        let cp = StageCheckpoint {
            id:                   None,
            streaming_cursor:     self.streaming,
            durable_cursor:       self.durable,
            compacted_until:      self.compacted.min(block),
            proof_indexed_until:  self.proof.min(block),
            canonical_tip:        self.canonical_tip,
            exex_finished_height: self.finished_height.min(block),
        };

        self.compacted       = cp.compacted_until;
        self.proof           = cp.proof_indexed_until;
        self.finished_height = cp.exex_finished_height;

        db.insert_checkpoint(&cp)?;
        tracing::debug!(block, "rollback checkpoint written");
        Ok(())
    }
}