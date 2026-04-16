//! Fake ExExNotification builder.
//!
//! Converts FakeBlock descriptors into real Reth ExExNotification values
//! that DiffExEx can process. Uses the minimum real Reth types needed.

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy_consensus::{Header, TxReceipt};
use alloy_primitives::{Address, Bloom, Bytes, B256, U256};
use diff_proof::trie::compute_receipts_root;
use eyre::Context;
use revm_state::{AccountInfo, BundleAccount, BundleState, StorageSlot};
use reth_execution_types::{ExecutionOutcome};
use reth_exex_types::{Chain, ExExNotification};
use reth_primitives::{RecoveredBlock, SealedBlock, Block, BlockBody};
use reth_trie_common::HashedPostState;

use crate::types::{FakeBlock, FakeAccountChange, FakeAccountInfo};

// ---------------------------------------------------------------------------
// Public notification constructors
// ---------------------------------------------------------------------------

/// Build a ChainCommitted notification from a slice of FakeBlocks.
pub fn make_commit_notification(blocks: &[FakeBlock]) -> eyre::Result<ExExNotification> {
    let chain = build_chain(blocks)?;
    Ok(ExExNotification::ChainCommitted {
        new: Arc::new(chain),
    })
}

/// Build a ChainReorged notification.
///
/// `old_blocks` is the chain being replaced (will be reverted).
/// `new_blocks` is the incoming canonical chain (will be committed).
pub fn make_reorg_notification(
    old_blocks: &[FakeBlock],
    new_blocks: &[FakeBlock],
) -> eyre::Result<ExExNotification> {
    let old_chain = build_chain(old_blocks)?;
    let new_chain = build_chain(new_blocks)?;
    Ok(ExExNotification::ChainReorged {
        old: Arc::new(old_chain),
        new: Arc::new(new_chain),
    })
}

/// Build a ChainReverted notification from a slice of FakeBlocks.
pub fn make_revert_notification(blocks: &[FakeBlock]) -> eyre::Result<ExExNotification> {
    let chain = build_chain(blocks)?;
    Ok(ExExNotification::ChainReverted {
        old: Arc::new(chain),
    })
}

// ---------------------------------------------------------------------------
// Chain builder
// ---------------------------------------------------------------------------

/// Build a Chain<EthereumNode> from a slice of FakeBlocks.
///
/// Steps:
/// 1. For each FakeBlock, build a RecoveredBlock (header + empty body + senders)
/// 2. Build a BundleState from all account changes across all blocks
/// 3. Build an ExecutionOutcome from BundleState + per-block receipts
/// 4. Construct Chain::new(blocks, outcome, None)
fn build_chain(blocks: &[FakeBlock]) -> eyre::Result<Chain> {
    if blocks.is_empty() {
        eyre::bail!("cannot build chain from empty block list");
    }

    // --- Step 1: build RecoveredBlocks ---
    let mut recovered_blocks: BTreeMap<u64, RecoveredBlock> = BTreeMap::new();

    for fb in blocks {
        let recovered = build_recovered_block(fb)
            .with_context(|| format!("build recovered block {}", fb.number))?;
        recovered_blocks.insert(fb.number, recovered);
    }

    // --- Step 2: build BundleState ---
    // BundleState covers all blocks in the chain segment.
    // We merge all account changes across all blocks into one BundleState.
    // In a real node the BundleState is the accumulated post-state across
    // the entire chain segment.
    let bundle = build_bundle_state(blocks);

    // --- Step 3: build ExecutionOutcome ---
    // ExecutionOutcome::new(bundle, receipts, first_block, requests)
    // receipts is Vec<Vec<Option<Receipt>>> — outer index is block position
    // in the chain segment (0 = first block), inner index is tx position.
    let first_block = blocks[0].number;

    let receipts_per_block: Vec<Vec<Option<alloy_consensus::Receipt>>> = blocks
        .iter()
        .map(|fb| {
            fb.receipts
                .iter()
                .map(|fr| Some(build_alloy_receipt(fr)))
                .collect()
        })
        .collect();

    let outcome = ExecutionOutcome::new(
        bundle,
        receipts_per_block.into(),
        first_block,
        vec![], // requests (EIP-7685) — empty for tests
    );

    // --- Step 4: build Chain ---
    // Chain::new takes an iterator of RecoveredBlocks plus ExecutionOutcome.
    let chain = Chain::new(
        recovered_blocks.into_values(),
        outcome,
        None, // trie_updates not needed for ExEx processing
    );

    Ok(chain)
}

// ---------------------------------------------------------------------------
// RecoveredBlock builder
// ---------------------------------------------------------------------------

fn build_recovered_block(fb: &FakeBlock) -> eyre::Result<RecoveredBlock> {
    // Build the receipts root for this block's fake receipts.
    // This gets stored in the header and later validated by ProofExtractor.
    let receipts_rlp: Vec<Vec<u8>> = fb.receipts.iter().map(encode_fake_receipt).collect();

    let receipts_root = if receipts_rlp.is_empty() {
        // Empty receipts trie root — the standard empty MPT root.
        // keccak256(RLP([])) = the well-known empty trie root.
        reth_primitives::constants::EMPTY_RECEIPTS
    } else {
        compute_receipts_root(&receipts_rlp)
            .context("compute receipts root for fake block")?
    };

    // Build a minimal header. Fields not set default to zero/empty.
    let header = Header {
        number:          fb.number,
        parent_hash:     fb.parent_hash,
        receipts_root,
        // These are required to be non-zero for some Reth internal checks.
        // Use reasonable defaults.
        gas_limit:       30_000_000,
        base_fee_per_gas: Some(1_000_000_000),
        ..Default::default()
    };

    // Seal the header with our deterministic hash (not keccak of the header RLP,
    // but a test-only hash that matches what FakeBlock.hash was set to).
    // SealedHeader::new(header, hash) sets the hash without recomputing.
    let sealed_header = reth_primitives::SealedHeader::new(header, fb.hash);

    // Empty body — no transactions needed for our diff extraction tests.
    // (DiffExEx reads account changes from BundleState, not from tx bodies.)
    let body = BlockBody::default();

    let sealed_block = SealedBlock::new(
        Block { header: sealed_header.into_inner(), body },
        fb.hash,
    );

    // Senders: one per transaction. Empty body = empty senders.
    let senders: Vec<Address> = fb.receipts.iter().map(|_| Address::ZERO).collect();

    Ok(RecoveredBlock::new_sealed(sealed_block, senders))
}

// ---------------------------------------------------------------------------
// BundleState builder
// ---------------------------------------------------------------------------

fn build_bundle_state(blocks: &[FakeBlock]) -> BundleState {
    use std::collections::HashMap;

    // Merge all changes across all blocks.
    // In a real chain segment BundleState accumulates: original_info is from
    // before the first block in the segment, info is after the last.
    // For simplicity in tests we treat each block's changes as the full delta.
    let mut state: HashMap<Address, BundleAccount> = HashMap::new();

    for fb in blocks {
        for change in &fb.changes {
            let original_info = change.old.as_ref().map(fake_info_to_account_info);
            let current_info  = change.new.as_ref().map(fake_info_to_account_info);

            // Build storage map
            let storage: HashMap<U256, StorageSlot> = change
                .storage
                .iter()
                .map(|(slot, old_val, new_val)| {
                    (
                        *slot,
                        StorageSlot {
                            previous_or_original_value: *old_val,
                            present_value: *new_val,
                        },
                    )
                })
                .collect();

            // If address already in state (multi-block segment), merge:
            // keep original_info from first appearance, update current info.
            state
                .entry(change.address)
                .and_modify(|existing| {
                    existing.info    = current_info.clone();
                    for (slot, sv) in &storage {
                        existing.storage.insert(*slot, sv.clone());
                    }
                })
                .or_insert_with(|| BundleAccount {
                    info:          current_info,
                    original_info,
                    storage,
                    status:        revm_state::AccountStatus::Changed,
                });
        }
    }

    BundleState {
        state,
        contracts: Default::default(),
        reverts:   Default::default(),
        state_size: 0,
        reverts_size: 0,
    }
}

fn fake_info_to_account_info(fi: &crate::types::FakeAccountInfo) -> AccountInfo {
    AccountInfo {
        balance:   fi.balance,
        nonce:     fi.nonce,
        code_hash: fi.code_hash,
        code:      None,
    }
}

// ---------------------------------------------------------------------------
// Receipt builder
// ---------------------------------------------------------------------------

fn build_alloy_receipt(fr: &crate::types::FakeReceipt) -> alloy_consensus::Receipt {
    alloy_consensus::Receipt {
        status: alloy_consensus::Eip658Value::Eip658(fr.status),
        cumulative_gas_used: fr.cumulative_gas_used,
        logs: vec![],
    }
}

/// RLP-encode a FakeReceipt the same way the ExEx does —
/// using Encodable2718 (legacy receipt = plain RLP, no type prefix).
pub fn encode_fake_receipt(fr: &crate::types::FakeReceipt) -> Vec<u8> {
    use alloy_rlp::Encodable;
    let receipt = build_alloy_receipt(fr);
    let mut buf = Vec::new();
    receipt.encode(&mut buf);
    buf
}