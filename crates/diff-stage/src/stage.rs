//! DiffCompactionStage — custom Reth pipeline stage.
//!
//! Reads raw diff rows written by the ExEx and compacts them into
//! secondary indexes (address_block_index, slot_block_index).
//!
//! Correctness invariants this stage upholds:
//!
//! 1. Never compacts beyond what the ExEx has durably written.
//!    (checked via stage_checkpoints.exex_finished_height)
//!
//! 2. execute() is idempotent: re-running over already-compacted blocks
//!    produces the same index state (INSERT OR IGNORE).
//!
//! 3. unwind() deletes all index rows above unwind_to and restores
//!    the checkpoint cursor. After unwind, the index is consistent with
//!    the canonical chain up to unwind_to.
//!
//! 4. Batch size is bounded (MAX_BLOCKS_PER_EXECUTE) so the stage never
//!    holds up the pipeline for unbounded time.

use std::sync::Arc;

use diff_db::DiffDb;
use eyre::Context;
use reth_stages_api::{
    ExecInput, ExecOutput, Stage, StageCheckpoint, StageError, StageId, UnwindInput,
    UnwindOutput,
};
use rusqlite::params;
use tracing::{debug, info, warn};

use crate::index::{
    build_address_index_for_block, build_slot_index_for_block, delete_address_index_above,
    delete_slot_index_above,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum number of blocks to compact in a single execute() call.
///
/// Prevents the stage from blocking the pipeline for too long on initial sync
/// or after a large gap. After processing this many blocks the stage returns
/// progress (done=false) and is called again on the next pipeline cycle.
const MAX_BLOCKS_PER_EXECUTE: u64 = 1_000;

/// The unique identifier for this stage in Reth's pipeline checkpoint map.
///
/// This string is used as the key when reading/writing StageCheckpoint rows
/// in Reth's internal database. It must be unique across all stages in the
/// pipeline. We use a descriptive name that makes it obvious in logs and
/// in the database.
pub const DIFF_COMPACTION_STAGE_ID: StageId = StageId::of("DiffCompaction");

// ---------------------------------------------------------------------------
// Stage struct
// ---------------------------------------------------------------------------

/// Custom Reth pipeline stage that compacts ExEx-written diff rows into
/// query-optimised secondary indexes.
///
/// Owns a reference to the DiffDb (shared with the ExEx via Arc). The ExEx
/// writes raw diffs; this stage reads them and writes index entries.
///
/// The stage does NOT re-derive diffs from block execution. It only indexes
/// what the ExEx already wrote.
pub struct DiffCompactionStage {
    /// Shared access to the SQLite diff database.
    ///
    /// Arc because the ExEx and the stage both need access. In a real Reth
    /// deployment the ExEx runs on a separate tokio task while the stage
    /// runs on the pipeline thread, so Arc<Mutex<DiffDb>> would be needed
    /// for true concurrent access. For now we use Arc and rely on the fact
    /// that Reth's pipeline does not execute stages concurrently with the
    /// ExEx notification loop in a way that would race on the same rows.
    db: Arc<DiffDb>,
}

impl DiffCompactionStage {
    /// Construct a new DiffCompactionStage.
    ///
    /// Runs stage-owned schema migrations (creates address_block_index and
    /// slot_block_index tables if they do not exist).
    ///
    /// # Errors
    /// Returns an error if the schema migration fails (e.g. disk full,
    /// corrupted database).
    pub fn new(db: Arc<DiffDb>) -> eyre::Result<Self> {
        // Run stage-specific migrations on the same connection.
        // DiffDb::open already ran the ExEx migrations. This adds the index
        // tables that only the stage owns.
        crate::schema::run_stage_migrations(db.connection())
            .context("stage schema migration failed")?;

        Ok(Self { db })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Read the ExEx's latest durable cursor from stage_checkpoints.
    ///
    /// This is `exex_finished_height` from the most recent checkpoint row.
    /// The stage will never compact beyond this height.
    ///
    /// Returns 0 if no checkpoint exists yet (ExEx has not processed anything).
    fn exex_durable_height(&self) -> eyre::Result<u64> {
        let checkpoint = self.db.get_latest_checkpoint()
            .context("read latest checkpoint for exex_durable_height")?;
        Ok(checkpoint.exex_finished_height)
    }

    /// Read the stage's own compacted_until cursor from stage_checkpoints.
    ///
    /// Returns 0 if the stage has never run (genesis).
    fn stage_compacted_until(&self) -> eyre::Result<u64> {
        let checkpoint = self.db.get_latest_checkpoint()
            .context("read latest checkpoint for stage_compacted_until")?;
        Ok(checkpoint.compacted_until)
    }

    /// Compact a single block: build address and slot indexes.
    ///
    /// Reads the canonical block hash for this block_number from
    /// canonical_blocks (filtering active status), then builds both indexes.
    ///
    /// Returns Ok(false) if the block is not found in canonical_blocks — this
    /// means the ExEx hasn't written it yet, which should not happen because
    /// we check exex_durable_height before calling this.
    fn compact_block(&self, block_number: u64) -> eyre::Result<bool> {
        // Look up the canonical (active) block hash for this height.
        let conn = self.db.connection();
        let hash_opt: Option<String> = conn
            .query_row(
                "SELECT block_hash FROM canonical_blocks
                 WHERE block_number = ?1 AND canonical_status = 'active'",
                params![block_number as i64],
                |row| row.get(0),
            )
            .optional()
            .context("query canonical_blocks for block_number")?;

        let block_hash = match hash_opt {
            Some(h) => h,
            None => {
                warn!(block_number, "no active canonical block found — skipping compaction");
                return Ok(false);
            }
        };

        // Build both indexes for this block.
        build_address_index_for_block(conn, block_number, &block_hash)
            .with_context(|| format!("build address index for block {block_number}"))?;

        build_slot_index_for_block(conn, block_number, &block_hash)
            .with_context(|| format!("build slot index for block {block_number}"))?;

        Ok(true)
    }

    /// Write a new checkpoint recording the updated compacted_until cursor.
    ///
    /// Reads the current checkpoint, updates compacted_until, and inserts a
    /// new row (checkpoints are append-only — latest row by id wins).
    fn write_compaction_checkpoint(&self, compacted_until: u64) -> eyre::Result<()> {
        let mut ckpt = self.db.get_latest_checkpoint()
            .context("read checkpoint before write")?;

        // Advance the compacted cursor. Never regress it.
        if compacted_until > ckpt.compacted_until {
            ckpt.compacted_until = compacted_until;
        }

        self.db.insert_checkpoint(&ckpt)
            .context("write compaction checkpoint")?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reth Stage trait implementation
// ---------------------------------------------------------------------------

/// The Provider type parameter is Reth's database provider that the pipeline
/// passes to each stage. Our stage does not use Reth's own provider for reads
/// (we read from our own SQLite DiffDb), but we must satisfy the trait bound.
/// We use a generic P with no bounds — if Reth requires specific bounds they
/// can be added later.
impl<Provider> Stage<Provider> for DiffCompactionStage {
    fn id(&self) -> StageId {
        DIFF_COMPACTION_STAGE_ID
    }

    // -----------------------------------------------------------------------
    // execute — forward compaction
    // -----------------------------------------------------------------------
    fn execute(
        &mut self,
        _provider: &Provider,
        input: ExecInput,
    ) -> Result<ExecOutput, StageError> {
        // The pipeline's requested target block (canonical tip or sync target).
        let pipeline_target = input.target();

        // Where this stage last left off.
        let stage_from = input.checkpoint().block_number;

        // How far the ExEx has durably written. We never compact beyond this.
        let exex_ceiling = self
            .exex_durable_height()
            .map_err(|e| StageError::Fatal(e.into()))?;

        // Effective target: min(pipeline_target, exex_ceiling).
        // If the ExEx is lagging, compact only what it has written.
        let effective_target = pipeline_target.min(exex_ceiling);

        if effective_target <= stage_from {
            // Nothing to do: ExEx hasn't written new blocks yet, or we are
            // already caught up.
            debug!(
                stage_from,
                pipeline_target,
                exex_ceiling,
                "compaction stage: nothing to compact yet"
            );
            return Ok(ExecOutput::done(StageCheckpoint::new(stage_from)));
        }

        // Batch: process at most MAX_BLOCKS_PER_EXECUTE blocks this call.
        let batch_end = (stage_from + MAX_BLOCKS_PER_EXECUTE).min(effective_target);

        info!(
            stage_from,
            batch_end,
            effective_target,
            pipeline_target,
            exex_ceiling,
            "compaction stage: starting batch"
        );

        let mut last_compacted = stage_from;

        for block_number in (stage_from + 1)..=batch_end {
            self.compact_block(block_number)
                .map_err(|e| StageError::Fatal(e.into()))?;

            last_compacted = block_number;
        }

        // Persist the new compacted_until cursor.
        self.write_compaction_checkpoint(last_compacted)
            .map_err(|e| StageError::Fatal(e.into()))?;

        let done = last_compacted >= effective_target;

        info!(
            last_compacted,
            effective_target,
            done,
            "compaction stage: batch complete"
        );

        let checkpoint = StageCheckpoint::new(last_compacted);

        if done {
            Ok(ExecOutput::done(checkpoint))
        } else {
            Ok(ExecOutput::progress(checkpoint))
        }
    }

    // -----------------------------------------------------------------------
    // unwind — reverse compaction
    // -----------------------------------------------------------------------
    fn unwind(
        &mut self,
        _provider: &Provider,
        input: UnwindInput,
    ) -> Result<UnwindOutput, StageError> {
        let unwind_to = input.unwind_to;
        let from = input.checkpoint.block_number;

        info!(from, unwind_to, "compaction stage: unwinding");

        // Delete all index rows above unwind_to.
        // This covers both address_block_index and slot_block_index.
        // We do this in a single SQLite transaction for atomicity.
        let conn = self.db.connection();

        conn.execute("BEGIN", [])
            .map_err(|e| StageError::Fatal(eyre::eyre!(e).into()))?;

        let addr_deleted = delete_address_index_above(conn, unwind_to)
            .map_err(|e| {
                let _ = conn.execute("ROLLBACK", []);
                StageError::Fatal(e.into())
            })?;

        let slot_deleted = delete_slot_index_above(conn, unwind_to)
            .map_err(|e| {
                let _ = conn.execute("ROLLBACK", []);
                StageError::Fatal(e.into())
            })?;

        conn.execute("COMMIT", [])
            .map_err(|e| StageError::Fatal(eyre::eyre!(e).into()))?;

        // Update the checkpoint to reflect the unwind.
        let mut ckpt = self.db.get_latest_checkpoint()
            .map_err(|e| StageError::Fatal(e.into()))?;

        ckpt.compacted_until = unwind_to;

        self.db.insert_checkpoint(&ckpt)
            .map_err(|e| StageError::Fatal(e.into()))?;

        info!(
            unwind_to,
            addr_deleted,
            slot_deleted,
            "compaction stage: unwind complete"
        );

        Ok(UnwindOutput {
            checkpoint: StageCheckpoint::new(unwind_to),
        })
    }
}