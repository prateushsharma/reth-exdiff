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
}

impl<Node> DiffExEx<Node>
where
    Node: FullNodeComponents,
    Node::Types: reth_node_builder::NodeTypes,
    <Node::Types as reth_node_builder::NodeTypes>::Primitives: NodePrimitives,
    <<Node::Types as reth_node_builder::NodeTypes>::Primitives as NodePrimitives>::Receipt:
        TxReceipt + Encodable2718,
{
    /// Create a new DiffExEx, loading cursor from the database.
    pub fn new(ctx: ExExContext<Node>, db: DiffDb) -> Result<Self, diff_db::DbError> {
        let cursor = ExExCursor::load_from_db(&db)?;
        info!(
            durable_cursor = cursor.durable,
            "DiffExEx initialized"
        );
        Ok(Self { ctx, db, cursor })
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
                    if let Err(e) = self.handle_reorg(old, new) {
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
    fn handle_reorg(
        &mut self,
        old: &std::sync::Arc<reth_execution_types::Chain
            <Node::Types as reth_node_builder::NodeTypes>::Primitives,
        >>,
        new: &std::sync::Arc<reth_execution_types::Chain
            <Node::Types as reth_node_builder::NodeTypes>::Primitives,
        >>,
    ) -> Result<(), diff_db::DbError> {
        // Step 1: revert old chain blocks in descending order.
        // We must revert tip first, then walk back to the fork point.
        let mut old_numbers: Vec<u64> = old.blocks().keys().copied().collect();
        old_numbers.sort_unstable_by(|a, b| b.cmp(a)); // descending

        for block_number in old_numbers {
            // Mark block as reorged in canonical_blocks table.
            if let Some(block) = old.blocks().get(&block_number) {
                self.db.mark_reorged(&block.hash())?;
            }
            // Apply revert ops to undo account/storage/receipt diffs.
            apply_revert(&self.db, block_number)?;
        }

        // Roll back cursor to the last common ancestor.
        let fork_block = old.blocks().keys().next().copied().unwrap_or(0);
        let rollback_to = fork_block.saturating_sub(1);
        self.cursor.rollback_to(rollback_to, &self.db)?;

        info!(
            rollback_to,
            new_tip = new.tip().number(),
            "reorg revert complete, committing new chain"
        );

        // Step 2: commit new chain.
        self.handle_commit(new)?;

        Ok(())
    }

    /// Handle ChainReverted: revert old chain, no replacement yet.
    fn handle_revert(
        &mut self,
        old: &std::sync::Arc<reth_execution_types::Chain
            <Node::Types as reth_node_builder::NodeTypes>::Primitives,
        >>,
    ) -> Result<(), diff_db::DbError> {
        let mut old_numbers: Vec<u64> = old.blocks().keys().copied().collect();
        old_numbers.sort_unstable_by(|a, b| b.cmp(a));

        for block_number in old_numbers {
            if let Some(block) = old.blocks().get(&block_number) {
                self.db.mark_reorged(&block.hash())?;
            }
            apply_revert(&self.db, block_number)?;
        }

        let fork_block  = old.blocks().keys().next().copied().unwrap_or(0);
        let rollback_to = fork_block.saturating_sub(1);
        self.cursor.rollback_to(rollback_to, &self.db)?;

        warn!(
            rollback_to,
            "chain reverted with no replacement — waiting for new canonical chain"
        );

        Ok(())
    }
}