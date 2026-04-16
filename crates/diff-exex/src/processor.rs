use alloy_consensus::TxReceipt;
use alloy_eips::eip2718::Encodable2718;
use diff_db::DiffDb;
use diff_types::{CanonicalBlock, CanonicalStatus};
use futures_util::TryStreamExt;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::FullNodeComponents;
use reth_primitives_traits::NodePrimitives;
use tracing::{error, info, warn};
use tokio::sync::mpsc::Receiver;
use reth_exex_types::ExExNotification;
use tokio::sync::watch;

use crate::cursor::ExExCursor;
use crate::extractor::extract_chain_diffs;
use crate::revert::apply_revert;

/// The main ExEx struct.
/// Owns the database handle and cursor state.
/// Created once at startup and drives the notification loop.
pub struct DiffExEx<Node: FullNodeComponents> {
    ctx:    ExExContext<Node>,
    db:     DiffDb,
    cursor: ExExCursor,
     reorg_tx:  Option<watch::Sender<Option<u64>>>,
}

impl<Node: FullNodeComponents> DiffExEx<Node>
where
    Node: FullNodeComponents,
    Node::Types: reth_node_builder::NodeTypes,
    <Node::Types as reth_node_builder::NodeTypes>::Primitives: NodePrimitives,
    <<Node::Types as reth_node_builder::NodeTypes>::Primitives as NodePrimitives>::Receipt:
        TxReceipt + Encodable2718,
{
    /// Create a new DiffExEx, loading cursor from the database.
      pub fn new(ctx: ExExContext<Node>, db: DiffDb) -> Self {
        let checkpoint = db.get_latest_checkpoint()
            .expect("load initial checkpoint");
        let cursor = ExExCursor::load_from_db(checkpoint);
        Self { ctx, db, cursor, reorg_tx: None }
    }

     pub fn new_with_reorg_signal(
        ctx:      ExExContext<Node>,
        db:       DiffDb,
        reorg_tx: watch::Sender<Option<u64>>,
    ) -> Self {
        let checkpoint = db.get_latest_checkpoint()
            .expect("load initial checkpoint");
        let cursor = ExExCursor::load_from_db(checkpoint);
        Self { ctx, db, cursor, reorg_tx: Some(reorg_tx) }
    }

    fn signal_reorg(&self, unwind_to: u64) {
        if let Some(ref tx) = self.reorg_tx {
            // Ignore send error — compaction task may have already exited
            // during shutdown.
            let _ = tx.send(Some(unwind_to));
        }
    }

    /// Run the ExEx loop indefinitely.
    /// Returns only on stream exhaustion or unrecoverable error.
    pub async fn run(mut self) -> eyre::Result<()> {
        while let Some(notification) = self.ctx.notifications.try_next().await? {
            match &notification {
                reth_exex_types::ExExNotification::ChainCommitted { new } => {
                    info!(
                        tip    = new.tip().number(),
                        blocks = new.blocks().len(),
                        "ChainCommitted"
                    );
                    if let Err(e) = self.handle_commit(new) {
                        error!(err = ?e, "failed to handle commit");
                        return Err(e.into());
                    }
                }

                reth_exex_types::ExExNotification::ChainReorged { old, new } => {
                    info!(
                        old_tip = old.tip().number(),
                        new_tip = new.tip().number(),
                        "ChainReorged"
                    );
                    if let Err(e) = self.signal_reorg(reverted_chain_first_block - 1);{
                        error!(err = ?e, "failed to handle reorg");
                        return Err(e.into());
                    }
                }

                reth_exex_types::ExExNotification::ChainReverted { old } => {
                    info!(
                        tip = old.tip().number(),
                        "ChainReverted"
                    );
                    if let Err(e) = self.handle_revert(old) {
                        error!(err = ?e, "failed to handle revert");
                        return Err(e.into());
                    }
                }
            }

            // Emit FinishedHeight after every notification that has a committed chain.
            // This tells reth it is safe to prune state below this height.
            // Rule: only emit after durable write, never before.
            if let Some(committed) = notification.committed_chain() {
                let tip = committed.tip().num_hash();
                self.ctx.events.send(ExExEvent::FinishedHeight(tip))?;
                self.cursor.finished_height = tip.number;
            }
        }

        info!("ExEx notification stream ended");
        Ok(())
    }

    /// Handle ChainCommitted: extract diffs and write to DB.
    fn handle_commit(
        &mut self,
        chain: &std::sync::Arc<reth_execution_types::Chain
            <Node::Types as reth_node_builder::NodeTypes>::Primitives,
        >>,
    ) -> Result<(), diff_db::DbError> {
        let block_diffs = extract_chain_diffs(chain);

        for (block_number, block_hash, diffs) in block_diffs {
            // 1. Write revert ops FIRST — before any other writes.
            //    If we crash after writing diffs but before revert ops,
            //    we cannot undo. Write undo log first.
            for op in &diffs.revert_ops {
                self.db.insert_revert_op(op)?;
            }

            // 2. Write canonical block record.
            self.db.insert_canonical_block(&CanonicalBlock {
                block_number,
                block_hash,
                parent_hash:           Default::default(), // filled below
                canonical_status:      CanonicalStatus::Active,
                finalized_hint:        None,
                derived_checkpoint_id: None,
            })?;

            // 3. Write account diffs.
            for diff in &diffs.account_diffs {
                self.db.insert_account_diff(diff)?;
            }

            // 4. Write storage diffs.
            for diff in &diffs.storage_diffs {
                self.db.insert_storage_diff(diff)?;
            }

            // 5. Write receipt artifacts.
            for artifact in &diffs.receipt_artifacts {
                self.db.insert_receipt_artifact(artifact)?;
            }

            // 6. Advance streaming cursor (in-memory only).
            self.cursor.advance_streaming(block_number);

            // 7. Advance durable cursor and write checkpoint.
            //    Only after ALL writes above are complete.
            self.cursor.advance_durable(
                block_number,
                block_number,
                self.cursor.finished_height,
                &self.db,
            )?;

            info!(
                block_number,
                account_diffs  = diffs.account_diffs.len(),
                storage_diffs  = diffs.storage_diffs.len(),
                receipts       = diffs.receipt_artifacts.len(),
                "block committed to diff-db"
            );
        }

        Ok(())
    }

       pub async fn run_with_receiver(
        db: Arc<DiffDb>,
        mut receiver: Receiver<ExExNotification>,
    ) -> eyre::Result<()> {
        // Load the durable checkpoint to resume from.
        let checkpoint = db.get_latest_checkpoint()
            .context("load checkpoint on startup")?;

        let mut cursor = ExExCursor::load_from_db(checkpoint);

        while let Some(notification) = receiver.recv().await {
            match notification {
                ExExNotification::ChainCommitted { new } => {
                    handle_commit(&db, &mut cursor, &new)
                        .await
                        .context("handle_commit in run_with_receiver")?;
                }
                ExExNotification::ChainReorged { old, new } => {
                    handle_reorg(&db, &mut cursor, &old, &new)
                        .await
                        .context("handle_reorg in run_with_receiver")?;
                }
                ExExNotification::ChainReverted { old } => {
                    handle_revert(&db, &mut cursor, &old)
                        .await
                        .context("handle_revert in run_with_receiver")?;
                }
            }
        }

        // Receiver closed — clean exit.
        Ok(())
    }

    /// Handle ChainReorged: revert old chain, then commit new chain.
    async fn handle_reorg(
    &mut self,
    old_chain: &Chain,
    new_chain: &Chain,
) -> eyre::Result<()> {
    // Step 1: revert all blocks in old_chain, tip-first (highest block first).
    // This applies the revert ops in reverse order so the DB returns to the
    // state it was in before those blocks were committed.
    let mut old_blocks: Vec<u64> = old_chain.blocks().keys().copied().collect();
    old_blocks.sort_unstable_by(|a, b| b.cmp(a)); // descending

    for block_number in &old_blocks {
        apply_revert(&self.db, *block_number)
            .context("apply revert in handle_reorg")?;

        self.db
            .mark_reorged(*block_number)
            .context("mark_reorged in handle_reorg")?;
    }

    // Step 2: roll back the cursor to before the old chain.
    let reorg_base = old_blocks.last().copied().unwrap_or(0).saturating_sub(1);
    self.cursor.rollback_to(&self.db, reorg_base)
        .context("cursor rollback in handle_reorg")?;

    // Step 3: signal the compaction task to unwind its indexes.
    //
    // reorg_base is the last block that is still canonical after the reorg.
    // The compaction task will delete index rows for everything above this.
    //
    // Example: old chain was blocks 3,4,5. reorg_base = 2.
    // Compaction task deletes address_block_index WHERE block_number > 2.
    self.signal_reorg(reorg_base);

    // Step 4: commit the new canonical chain.
    handle_commit_inner(&self.db, &mut self.cursor, new_chain)
        .await
        .context("handle_commit in handle_reorg")?;

    Ok(())
}

    /// Handle ChainReverted: revert old chain, no replacement yet.
    async fn handle_revert(
    &mut self,
    old_chain: &Chain,
) -> eyre::Result<()> {
    // Revert blocks tip-first.
    let mut old_blocks: Vec<u64> = old_chain.blocks().keys().copied().collect();
    old_blocks.sort_unstable_by(|a, b| b.cmp(a)); // descending

    for block_number in &old_blocks {
        apply_revert(&self.db, *block_number)
            .context("apply revert in handle_revert")?;

        self.db
            .mark_reorged(*block_number)
            .context("mark_reorged in handle_revert")?;
    }

    // Roll back cursor.
    let revert_base = old_blocks.last().copied().unwrap_or(0).saturating_sub(1);
    self.cursor.rollback_to(&self.db, revert_base)
        .context("cursor rollback in handle_revert")?;

    // Signal compaction task.
    //
    // revert_base is the last surviving canonical block.
    // Example: reverted blocks 4,5. revert_base = 3.
    // Compaction task deletes index rows WHERE block_number > 3.
    self.signal_reorg(revert_base);

    Ok(())
}
}