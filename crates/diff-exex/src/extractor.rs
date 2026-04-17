use alloy_consensus::TxReceipt;
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{BlockNumber, B256};
use diff_types::{
    AccountDiff, ChangeKind, ReceiptArtifact, RevertOp, RevertTable, StorageDiff,
};
use reth_ethereum_primitives::NodePrimitives;
use reth_execution_types::Chain;
use std::sync::Arc;

/// The full set of derived records for one block.
#[derive(Debug, Default)]
pub struct BlockDiffs {
    pub account_diffs:     Vec<AccountDiff>,
    pub storage_diffs:     Vec<StorageDiff>,
    pub receipt_artifacts: Vec<ReceiptArtifact>,
    /// Revert ops written at commit time, replayed at reorg time.
    /// Stored in ascending op_sequence so they can be applied in reverse.
    pub revert_ops:        Vec<RevertOp>,
}

/// Extract all diffs from a committed chain.
/// Returns one `BlockDiffs` per block in the chain, in ascending block number order.
pub fn extract_chain_diffs<N>(chain: &Arc<Chain<N>>) -> Vec<(BlockNumber, B256, BlockDiffs)>
where
    N: NodePrimitives,
    N::Receipt: TxReceipt + Encodable2718,
{
    let mut results = Vec::new();

    for (block_number, block) in chain.blocks() {
        let block_hash   = block.hash();
        let block_number = *block_number;

        // Get the execution outcome scoped to this specific block.
        // execution_outcome_at_block returns state as it was at the END of this block.
        let outcome = match chain.execution_outcome_at_block(block_number) {
            Some(o) => o,
            None => {
                tracing::warn!(block_number, "no execution outcome for block, skipping");
                continue;
            }
        };

        let mut diffs   = BlockDiffs::default();
        let mut op_seq  = 0i64;

        // ── Account and storage diffs ─────────────────────────────────────
        // bundle.state is HashMap<Address, BundleAccount>
        // Each BundleAccount has:
        //   .info          = Option<AccountInfo>  — state AFTER this block
        //   .original_info = Option<AccountInfo>  — state BEFORE this block
        //   .storage       = HashMap<U256, StorageSlot>
        for (address, bundle_account) in &outcome.bundle.state {
            let old_info = &bundle_account.original_info;
            let new_info = &bundle_account.info;

            // Determine what kind of change this was.
            let change_kind = match (old_info, new_info) {
                (None, Some(_))    => ChangeKind::Created,
                (Some(_), None)    => ChangeKind::Destroyed,
                (Some(o), Some(n)) => {
                    if o.balance != n.balance
                        || o.nonce != n.nonce
                        || o.code_hash != n.code_hash
                    {
                        ChangeKind::Modified
                    } else {
                        ChangeKind::Touched
                    }
                }
                (None, None) => {
                    // Account touched but no state change at all.
                    // Skip — not worth recording.
                    continue;
                }
            };

            let account_diff = AccountDiff {
                block_number,
                block_hash,
                address:       *address,
                old_balance:   old_info.as_ref().map(|i| i.balance),
                new_balance:   new_info.as_ref().map(|i| i.balance),
                old_nonce:     old_info.as_ref().map(|i| i.nonce),
                new_nonce:     new_info.as_ref().map(|i| i.nonce),
                old_code_hash: old_info.as_ref().map(|i| i.code_hash),
                new_code_hash: new_info.as_ref().map(|i| i.code_hash),
                change_kind,
            };

            // Write revert op for this account diff.
            // On reorg we delete the row using (block_hash, address) as key.
            diffs.revert_ops.push(RevertOp {
                id:                 None,
                reorg_target_block: block_number,
                op_sequence:        op_seq,
                table_name:         RevertTable::AccountDiffs,
                primary_key_ref:    serde_json::json!({
                    "block_hash": format!("{:?}", block_hash),
                    "address":    format!("{:?}", address),
                }).to_string(),
                inverse_payload:    String::new(),
            });
            op_seq += 1;

            diffs.account_diffs.push(account_diff);

            // ── Storage diffs ─────────────────────────────────────────────
            for (slot, storage_slot) in &bundle_account.storage {
                let old_val = storage_slot.original_value();
                let new_val = storage_slot.present_value();

                // Skip if value did not actually change.
                if old_val == new_val {
                    continue;
                }

                diffs.revert_ops.push(RevertOp {
                    id:                 None,
                    reorg_target_block: block_number,
                    op_sequence:        op_seq,
                    table_name:         RevertTable::StorageDiffs,
                    primary_key_ref:    serde_json::json!({
                        "block_hash": format!("{:?}", block_hash),
                        "address":    format!("{:?}", address),
                        "slot":       slot.to_string(),
                    }).to_string(),
                    inverse_payload:    String::new(),
                });
                op_seq += 1;

                diffs.storage_diffs.push(StorageDiff {
                    block_number,
                    block_hash,
                    address: *address,
                    slot:     *slot,
                    old_value: old_val,
                    new_value: new_val,
                });
            }
        }

        // ── Receipt artifacts ─────────────────────────────────────────────
        // receipts_by_block returns &[Receipt] indexed by tx position.
        // We need the receipts root from the block header for the proof anchor.
        let receipts     = outcome.receipts_by_block(block_number);
        let receipt_root = block.header().receipts_root();

        for (tx_index, (tx, receipt)) in block
            .body()
            .transactions()
            .zip(receipts.iter())
            .enumerate()
        {
            // RLP-encode the receipt for trie leaf storage.
            let mut receipt_rlp = Vec::new();
            receipt.encode_2718(&mut receipt_rlp);

            let tx_hash = *tx.tx_hash();

            diffs.revert_ops.push(RevertOp {
                id:                 None,
                reorg_target_block: block_number,
                op_sequence:        op_seq,
                table_name:         RevertTable::ReceiptArtifacts,
                primary_key_ref:    serde_json::json!({
                    "block_hash": format!("{:?}", block_hash),
                    "tx_index":   tx_index,
                }).to_string(),
                inverse_payload:    String::new(),
            });
            op_seq += 1;

            diffs.receipt_artifacts.push(ReceiptArtifact {
                block_number,
                block_hash,
                tx_index:            tx_index as u32,
                tx_hash,
                receipt_index:       tx_index as u32,
                receipt_rlp,
                receipt_root_anchor: receipt_root,
                log_bloom:           receipt.bloom(),
                status:              receipt.status(),
                cumulative_gas_used: receipt.cumulative_gas_used(),
            });
        }

        results.push((block_number, block_hash, diffs));
    }

    results
}