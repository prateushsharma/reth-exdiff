//! Background compaction loop.
//!
//! Drives DiffCompactionStage::execute() on a timer, advancing the
//! compacted_until cursor to track the ExEx's durable cursor.
//!
//! Also listens for reorg signals from the ExEx and calls
//! DiffCompactionStage::unwind() when the canonical chain shortens.

use std::sync::Arc;
use std::time::Duration;

use diff_db::DiffDb;
use diff_stage::DiffCompactionStage;
use eyre::Context;
use reth_stages_api::{ExecInput, Stage, StageCheckpoint, UnwindInput};
use rusqlite::params;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// How long to sleep between compaction passes when caught up.
const COMPACTION_INTERVAL: Duration = Duration::from_secs(2);

/// How long to sleep when the ExEx has not written any new blocks yet.
const WAIT_FOR_EXEX_INTERVAL: Duration = Duration::from_millis(500);

/// Run the compaction loop indefinitely.
///
/// `db`            — shared DiffDb (also used by the ExEx task)
/// `reorg_signal`  — watch channel receiver; ExEx sends the unwind-to block
///                   number after every reorg so the stage can unwind its
///                   index tables
///
/// This function never returns under normal operation. It exits only if
/// the reorg_signal sender is dropped (node shutdown) or a fatal DB error
/// occurs.
pub async fn run_compaction_loop(
    db: Arc<DiffDb>,
    mut reorg_signal: watch::Receiver<Option<u64>>,
) -> eyre::Result<()> {
    info!("compaction loop starting");

    // Unit provider — DiffCompactionStage ignores the Reth provider.
    let provider = ();

    let mut stage = DiffCompactionStage::new(Arc::clone(&db))
        .context("construct DiffCompactionStage")?;

    loop {
        // ---------------------------------------------------------------
        // Check for reorg signal first.
        //
        // The ExEx writes the unwind-to block number into this channel
        // whenever it processes a ChainReorged or ChainReverted event.
        // We must unwind the stage before doing any forward compaction.
        // ---------------------------------------------------------------
        if reorg_signal.has_changed().unwrap_or(false) {
            let unwind_to_opt = *reorg_signal.borrow_and_update();

            if let Some(unwind_to) = unwind_to_opt {
                let current_compacted = db
                    .get_latest_checkpoint()
                    .context("read checkpoint before unwind")?
                    .compacted_until;

                if unwind_to < current_compacted {
                    info!(
                        current_compacted,
                        unwind_to,
                        "compaction loop: reorg signal received, unwinding stage"
                    );

                    let unwind_input = UnwindInput {
                        checkpoint: StageCheckpoint::new(current_compacted),
                        unwind_to,
                        bad_block: None,
                    };

                    stage
                        .unwind(&provider, unwind_input)
                        .map_err(|e| eyre::eyre!("stage unwind failed: {:?}", e))?;

                    info!(unwind_to, "compaction loop: stage unwind complete");
                } else {
                    debug!(
                        unwind_to,
                        current_compacted,
                        "reorg signal does not require stage unwind (already below target)"
                    );
                }
            }
        }

        // ---------------------------------------------------------------
        // Read the current state: how far has the ExEx written, and how
        // far have we compacted?
        // ---------------------------------------------------------------
        let checkpoint = db
            .get_latest_checkpoint()
            .context("read checkpoint in compaction loop")?;

        let exex_durable   = checkpoint.durable;
        let compacted_until = checkpoint.compacted_until;

        if exex_durable == 0 {
            // ExEx has not processed any blocks yet.
            debug!("compaction loop: waiting for ExEx to write first block");
            tokio::time::sleep(WAIT_FOR_EXEX_INTERVAL).await;
            continue;
        }

        if compacted_until >= exex_durable {
            // Fully caught up with the ExEx.
            debug!(
                compacted_until,
                exex_durable,
                "compaction loop: caught up, sleeping"
            );
            tokio::time::sleep(COMPACTION_INTERVAL).await;
            continue;
        }

        // ---------------------------------------------------------------
        // Run one execute() pass.
        //
        // ExecInput::new(from_checkpoint, target):
        //   from_checkpoint — where the stage last left off
        //   target          — how far to go this pass
        //
        // The stage internally batches at MAX_BLOCKS_PER_EXECUTE=1000
        // so a single execute() call may not reach exex_durable if the
        // gap is large. We loop immediately if it returned progress.
        // ---------------------------------------------------------------
        let input = ExecInput::new(
            StageCheckpoint::new(compacted_until),
            exex_durable,
        );

        let output = stage
            .execute(&provider, input)
            .map_err(|e| eyre::eyre!("stage execute failed: {:?}", e))?;

        let new_checkpoint = output.checkpoint.block_number;

        info!(
            from = compacted_until,
            to   = new_checkpoint,
            done = output.done,
            "compaction loop: execute pass complete"
        );

        if output.done {
            // Fully caught up to exex_durable. Sleep before next cycle.
            tokio::time::sleep(COMPACTION_INTERVAL).await;
        }
        // If !done: loop immediately — there is more work in this batch.
    }
}

/// Read the highest active block number from canonical_blocks.
///
/// Returns 0 if no active blocks exist yet (node just started).
pub fn read_canonical_tip(db: &DiffDb) -> eyre::Result<u64> {
    let conn = db.connection();

    let tip: Option<i64> = conn
        .query_row(
            "SELECT MAX(block_number) FROM canonical_blocks
             WHERE canonical_status = 'active'",
            [],
            |r| r.get(0),
        )
        .optional()
        .context("query canonical tip")?
        .flatten();

    Ok(tip.map(|n| n as u64).unwrap_or(0))
}