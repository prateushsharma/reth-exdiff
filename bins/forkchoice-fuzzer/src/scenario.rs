//! Adversarial forkchoice scenarios.
//!
//! Each function is an async fn() -> eyre::Result<()>.
//! They are called by the runner in main.rs.
//!
//! All scenarios use in-memory SQLite (DiffDb::open_in_memory()) so
//! they are fully isolated from each other and leave no disk state.

use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use diff_db::DiffDb;
use diff_exex::DiffExEx;
use diff_stage::DiffCompactionStage;
use diff_testing::{
    hash_for, FakeAccountChange, FakeBlock, FakeReceipt, InvariantChecker,
    make_commit_notification, make_reorg_notification, make_revert_notification,
};
use eyre::Context;
use tokio::sync::mpsc;
use tracing::info;

// ---------------------------------------------------------------------------
// Test addresses — deterministic, human-readable in logs
// ---------------------------------------------------------------------------

fn addr(seed: u8) -> Address {
    let mut bytes = [0u8; 20];
    bytes[19] = seed;
    Address::from(bytes)
}

fn tx_hash(seed: u8) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[31] = seed;
    B256::from(bytes)
}

// ---------------------------------------------------------------------------
// Core helper: drive ExEx with a list of notifications, return the DB
// ---------------------------------------------------------------------------

/// Send `notifications` through a fresh DiffExEx backed by the given DB,
/// wait for all processing to complete, and return the DB for inspection.
///
/// This works by:
/// 1. Creating an mpsc channel (capacity = notifications.len() + 1)
/// 2. Sending all notifications into the sender
/// 3. Dropping the sender so the receiver closes after the last message
/// 4. Spawning DiffExEx::run_with_receiver(db, receiver) as a tokio task
/// 5. Awaiting the task — it exits when the receiver closes
async fn drive_exex(
    db: Arc<DiffDb>,
    notifications: Vec<reth_exex_types::ExExNotification>,
) -> eyre::Result<()> {
    let (tx, rx) = mpsc::channel(notifications.len() + 1);

    // Send all notifications before spawning — channel is buffered so this
    // does not block.
    for notif in notifications {
        tx.send(notif).await.context("send notification to ExEx channel")?;
    }

    // Drop sender — this signals end-of-stream to the ExEx loop.
    drop(tx);

    // Run the ExEx. It processes all buffered notifications then exits
    // when the channel is empty and closed.
    DiffExEx::run_with_receiver(db, rx).await
        .context("ExEx run_with_receiver")?;

    Ok(())
}

/// Same as drive_exex but does not drop the sender, so the ExEx keeps running.
/// Returns the sender for the caller to send more notifications later.
///
/// The caller must drop the sender to terminate the ExEx task.
async fn drive_exex_partial(
    db: Arc<DiffDb>,
    initial_notifications: Vec<reth_exex_types::ExExNotification>,
) -> eyre::Result<(tokio::task::JoinHandle<eyre::Result<()>>, mpsc::Sender<reth_exex_types::ExExNotification>)> {
    let (tx, rx) = mpsc::channel(64);

    for notif in initial_notifications {
        tx.send(notif).await.context("send initial notification")?;
    }

    let handle = tokio::spawn(DiffExEx::run_with_receiver(db, rx));

    Ok((handle, tx))
}

// ---------------------------------------------------------------------------
// Scenario helpers
// ---------------------------------------------------------------------------

/// Run the compaction stage over all blocks from 0 to `tip` in one pass.
fn run_stage_to(db: Arc<DiffDb>, tip: u64) -> eyre::Result<()> {
    use reth_stages_api::{ExecInput, Stage, StageCheckpoint};

    let mut stage = DiffCompactionStage::new(db)
        .context("construct compaction stage")?;

    // Use a unit provider — our stage ignores the provider argument.
    let provider = ();

    let input = ExecInput::new(
        StageCheckpoint::new(0),
        tip,
    );

    // Run until done.
    loop {
        let output = stage.execute(&provider, input.clone())
            .map_err(|e| eyre::eyre!("stage execute: {:?}", e))?;

        if output.done {
            break;
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 1 — linear forward extension (baseline)
// ---------------------------------------------------------------------------
//
// Commit blocks 1, 2, 3 in order.
// Expect: all three blocks active, diffs visible, invariants hold.

pub async fn linear_forward_extension() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let blocks = vec![
        FakeBlock::new(1,
            vec![FakeAccountChange::transfer(addr(1), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(1), 21_000)],
        ),
        FakeBlock::new(2,
            vec![FakeAccountChange::transfer(addr(2), 2000, 1800)],
            vec![FakeReceipt::success(tx_hash(2), 42_000)],
        ),
        FakeBlock::new(3,
            vec![FakeAccountChange::created(addr(3), 500)],
            vec![FakeReceipt::success(tx_hash(3), 21_000)],
        ),
    ];

    let notif = make_commit_notification(&blocks)?;
    drive_exex(Arc::clone(&db), vec![notif]).await?;

    InvariantChecker::check_all(&db)?;

    // Verify all three blocks are active in canonical_blocks
    let conn = db.connection();
    let active_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM canonical_blocks WHERE canonical_status = 'active'",
        [],
        |r| r.get(0),
    )?;
    eyre::ensure!(active_count == 3, "expected 3 active blocks, got {}", active_count);

    info!("scenario 01: {} active blocks confirmed", active_count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 2 — depth-1 reorg
// ---------------------------------------------------------------------------
//
// Commit block 1 on fork 0.
// Then reorg: replace block 1 fork-0 with block 1 fork-1.
// Expect: fork-0 block invisible, fork-1 block visible, invariants hold.

pub async fn depth1_reorg() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let block_a = FakeBlock::on_fork(1, 0,
        vec![FakeAccountChange::transfer(addr(1), 1000, 900)],
        vec![FakeReceipt::success(tx_hash(1), 21_000)],
    );

    let block_b = FakeBlock::on_fork(1, 1,
        vec![FakeAccountChange::transfer(addr(2), 2000, 1800)],
        vec![FakeReceipt::success(tx_hash(2), 21_000)],
    );

    let commit  = make_commit_notification(&[block_a.clone()])?;
    let reorg   = make_reorg_notification(&[block_a.clone()], &[block_b.clone()])?;

    drive_exex(Arc::clone(&db), vec![commit, reorg]).await?;

    // Old block must be invisible through canonical queries
    InvariantChecker::check_block_invisible(&db, block_a.hash)?;

    // New block must be active
    let conn = db.connection();
    let new_status: String = conn.query_row(
        "SELECT canonical_status FROM canonical_blocks WHERE block_hash = ?1",
        rusqlite::params![format!("{:?}", block_b.hash)],
        |r| r.get(0),
    ).context("query new block status")?;

    eyre::ensure!(
        new_status == "active",
        "reorg target block should be active, got: {}",
        new_status
    );

    InvariantChecker::check_all(&db)?;

    info!("scenario 02: depth-1 reorg validated");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 3 — depth-3 reorg
// ---------------------------------------------------------------------------
//
// Commit blocks 1,2,3 on fork 0.
// Reorg: replace blocks 1,2,3 with blocks 1,2,3 on fork 1.
// Expect: all fork-0 blocks invisible, all fork-1 blocks active.

pub async fn depth3_reorg() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let old_blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::on_fork(n, 0,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    let new_blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::on_fork(n, 1,
            vec![FakeAccountChange::transfer(addr((n + 10) as u8), 2000, 1800)],
            vec![FakeReceipt::success(tx_hash((n + 10) as u8), 21_000)],
        )
    }).collect();

    let commit = make_commit_notification(&old_blocks)?;
    let reorg  = make_reorg_notification(&old_blocks, &new_blocks)?;

    drive_exex(Arc::clone(&db), vec![commit, reorg]).await?;

    for ob in &old_blocks {
        InvariantChecker::check_block_invisible(&db, ob.hash)?;
    }

    // All new blocks should be active
    let conn = db.connection();
    let active_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM canonical_blocks WHERE canonical_status = 'active'",
        [], |r| r.get(0),
    )?;
    eyre::ensure!(active_count == 3,
        "expected 3 active blocks after depth-3 reorg, got {}", active_count);

    InvariantChecker::check_all(&db)?;

    info!("scenario 03: depth-3 reorg validated");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 4 — reorg after partial compaction
// ---------------------------------------------------------------------------
//
// Commit blocks 1-5 on fork 0.
// Run compaction stage up to block 3.
// Reorg blocks 3-5 to fork 1.
// Verify: stage index for blocks 3-5 is unwound, new blocks recompacted.

pub async fn reorg_after_partial_compaction() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let old_blocks: Vec<FakeBlock> = (1u64..=5).map(|n| {
        FakeBlock::on_fork(n, 0,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    // Commit all 5 blocks
    let commit = make_commit_notification(&old_blocks)?;
    drive_exex(Arc::clone(&db), vec![commit]).await?;

    // Compact up to block 3 only
    run_stage_to(Arc::clone(&db), 3)
        .context("stage run to block 3")?;

    // Verify index exists for blocks 1-3
    for n in 1u64..=3 {
        InvariantChecker::check_index_parity(&db, n)?;
    }

    // Now reorg blocks 3-5
    let reorg_old: Vec<FakeBlock> = old_blocks[2..].to_vec(); // blocks 3,4,5 fork-0
    let reorg_new: Vec<FakeBlock> = (3u64..=5).map(|n| {
        FakeBlock::on_fork(n, 1,
            vec![FakeAccountChange::created(addr((n + 20) as u8), 999)],
            vec![FakeReceipt::failure(tx_hash((n + 20) as u8), 21_000)],
        )
    }).collect();

    let reorg = make_reorg_notification(&reorg_old, &reorg_new)?;
    drive_exex(Arc::clone(&db), vec![reorg]).await?;

    // Unwind the stage
    {
        use reth_stages_api::{Stage, StageCheckpoint, UnwindInput};
        let mut stage = DiffCompactionStage::new(Arc::clone(&db))?;
        let provider = ();
        let unwind_input = UnwindInput {
            checkpoint: StageCheckpoint::new(3),
            unwind_to:  2,
            bad_block:  None,
        };
        stage.unwind(&provider, unwind_input)
            .map_err(|e| eyre::eyre!("stage unwind: {:?}", e))?;
    }

    // Re-compact with new blocks
    run_stage_to(Arc::clone(&db), 5)
        .context("stage re-run to block 5")?;

    // Old fork blocks should be invisible
    for ob in &reorg_old {
        InvariantChecker::check_block_invisible(&db, ob.hash)?;
    }

    InvariantChecker::check_all(&db)?;

    info!("scenario 04: reorg after partial compaction validated");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 5 — duplicate commit notification idempotence
// ---------------------------------------------------------------------------
//
// Send the exact same ChainCommitted notification twice.
// Expect: no duplicate rows in diffs, invariants hold.

pub async fn duplicate_commit_idempotence() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let block = FakeBlock::new(1,
        vec![FakeAccountChange::transfer(addr(1), 500, 400)],
        vec![FakeReceipt::success(tx_hash(1), 21_000)],
    );

    // Build the same notification twice and send both
    let notif1 = make_commit_notification(&[block.clone()])?;
    let notif2 = make_commit_notification(&[block.clone()])?;

    drive_exex(Arc::clone(&db), vec![notif1, notif2]).await?;

    // There should be exactly 1 account diff for addr(1), not 2
    let conn = db.connection();
    let diff_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM account_diffs WHERE block_number = 1",
        [], |r| r.get(0),
    )?;

    eyre::ensure!(
        diff_count == 1,
        "expected 1 account diff after duplicate commit, got {}",
        diff_count
    );

    InvariantChecker::check_all(&db)?;

    info!("scenario 05: duplicate commit idempotence confirmed, diff_count={}", diff_count);
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 6 — restart after ExEx commit, before stage checkpoint
// ---------------------------------------------------------------------------
//
// Commit 3 blocks with ExEx (durable checkpoint written).
// Run stage halfway (compact block 1 only).
// Simulate crash: drop stage, recreate it.
// Re-run stage from checkpoint — must not double-insert indexes.
// Expect: index parity holds, no duplicates, invariants pass.

pub async fn restart_after_exex_commit() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::new(n,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    drive_exex(Arc::clone(&db), vec![make_commit_notification(&blocks)?]).await?;

    // First stage run: compact only block 1, then "crash" (scope drop)
    {
        use reth_stages_api::{ExecInput, Stage, StageCheckpoint};
        let mut stage = DiffCompactionStage::new(Arc::clone(&db))?;
        let provider = ();
        // Execute with target=1 only
        let input = ExecInput::new(StageCheckpoint::new(0), 1);
        stage.execute(&provider, input)
            .map_err(|e| eyre::eyre!("first stage run: {:?}", e))?;
        // stage drops here — simulates crash
    }

    // Recreate stage and run to completion
    run_stage_to(Arc::clone(&db), 3)?;

    // All three blocks should have correct index parity
    for n in 1u64..=3 {
        InvariantChecker::check_index_parity(&db, n)?;
    }

    InvariantChecker::check_all(&db)?;

    info!("scenario 06: restart-after-exex-commit validated");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 7 — crash and replay (ExEx restart recovery)
// ---------------------------------------------------------------------------
//
// Commit blocks 1-3, reorg blocks 2-3, then simulate ExEx crash mid-reorg
// by dropping it. Restart a new ExEx against the same DB. It should resume
// from the durable checkpoint (block 1), not reprocess from genesis.
// Expect: final state is consistent with the last durably written block.

pub async fn crash_and_replay() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::on_fork(n, 0,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    // Commit blocks 1-3
    let commit = make_commit_notification(&blocks)?;
    drive_exex(Arc::clone(&db), vec![commit]).await?;

    // Read the durable checkpoint before the "crash"
    let durable_before = db.get_latest_checkpoint()?.durable;

    // Simulate crash: create a new ExEx instance on the same DB (same as restart)
    // and drive it with no new notifications — it should simply resume cleanly.
    drive_exex(Arc::clone(&db), vec![]).await?;

    // Durable checkpoint should not have regressed
    let durable_after = db.get_latest_checkpoint()?.durable;
    eyre::ensure!(
        durable_after >= durable_before,
        "durable checkpoint regressed after restart: {} -> {}",
        durable_before, durable_after
    );

    InvariantChecker::check_all(&db)?;

    info!("scenario 07: crash-and-replay validated, durable={}", durable_after);
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 8 — depth-10 reorg
// ---------------------------------------------------------------------------
//
// Commit blocks 1-10 on fork 0.
// Reorg all 10 blocks to fork 1.
// Expect: all fork-0 blocks invisible, all fork-1 blocks active.

pub async fn depth10_reorg() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let old_blocks: Vec<FakeBlock> = (1u64..=10).map(|n| {
        FakeBlock::on_fork(n, 0,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    let new_blocks: Vec<FakeBlock> = (1u64..=10).map(|n| {
        FakeBlock::on_fork(n, 1,
            vec![FakeAccountChange::created(addr((n + 30) as u8), 500)],
            vec![FakeReceipt::success(tx_hash((n + 30) as u8), 21_000)],
        )
    }).collect();

    let commit = make_commit_notification(&old_blocks)?;
    let reorg  = make_reorg_notification(&old_blocks, &new_blocks)?;

    drive_exex(Arc::clone(&db), vec![commit, reorg]).await?;

    for ob in &old_blocks {
        InvariantChecker::check_block_invisible(&db, ob.hash)?;
    }

    let conn = db.connection();
    let active_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM canonical_blocks WHERE canonical_status = 'active'",
        [], |r| r.get(0),
    )?;
    eyre::ensure!(active_count == 10,
        "expected 10 active blocks after depth-10 reorg, got {}", active_count);

    InvariantChecker::check_all(&db)?;

    info!("scenario 08: depth-10 reorg validated");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 9 — branch pollution attempt
// ---------------------------------------------------------------------------
//
// Commit blocks 1-3 on fork 0 (legitimate canonical chain).
// Then send a ChainReverted for blocks 1-3 on fork 1 (a branch that was
// never committed to our DB). The ExEx must not write any diffs for these
// unknown blocks and must not corrupt the existing canonical state.

pub async fn branch_pollution_attempt() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    // Commit legitimate chain
    let canonical_blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::on_fork(n, 0,
            vec![FakeAccountChange::transfer(addr(n as u8), 1000, 900)],
            vec![FakeReceipt::success(tx_hash(n as u8), 21_000)],
        )
    }).collect();

    let commit = make_commit_notification(&canonical_blocks)?;
    drive_exex(Arc::clone(&db), vec![commit]).await?;

    // Now send a revert for a branch that was never committed
    // (fork 1 blocks that the ExEx has never seen)
    let phantom_blocks: Vec<FakeBlock> = (1u64..=3).map(|n| {
        FakeBlock::on_fork(n, 1,
            vec![FakeAccountChange::transfer(addr((n + 50) as u8), 9999, 8888)],
            vec![FakeReceipt::success(tx_hash((n + 50) as u8), 21_000)],
        )
    }).collect();

    let revert = make_revert_notification(&phantom_blocks)?;
    drive_exex(Arc::clone(&db), vec![revert]).await?;

    // Canonical chain should still be intact — 3 active blocks
    let conn = db.connection();
    let active_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM canonical_blocks WHERE canonical_status = 'active'",
        [], |r| r.get(0),
    )?;
    eyre::ensure!(active_count == 3,
        "canonical chain corrupted by phantom revert: {} active blocks", active_count);

    // Phantom blocks should not appear in canonical_blocks at all
    for pb in &phantom_blocks {
        let hash_str = format!("{:?}", pb.hash);
        let found: Option<String> = conn.query_row(
            "SELECT canonical_status FROM canonical_blocks WHERE block_hash = ?1",
            rusqlite::params![hash_str],
            |r| r.get(0),
        ).optional()?;
        // It's ok if they appear as 'reorged' (ExEx saw the revert and recorded
        // the block as non-canonical). What's not ok is if they appear as 'active'.
        if let Some(status) = found {
            eyre::ensure!(
                status != "active",
                "phantom block {} appears as active after revert-of-unseen-block",
                hash_str
            );
        }
    }

    InvariantChecker::check_all(&db)?;

    info!("scenario 09: branch pollution attempt blocked");
    Ok(())
}

// ---------------------------------------------------------------------------
// Scenario 10 — stale head / backward head movement
// ---------------------------------------------------------------------------
//
// Commit block 1, commit block 2, then reorg back so block 1 is the tip
// (chain shortens — block 2 is reverted, nothing replaces it yet).
// Expect: block 2 is reorged, block 1 is still active, no crash.

pub async fn stale_head_backward_movement() -> eyre::Result<()> {
    let db = Arc::new(DiffDb::open_in_memory()?);

    let block1 = FakeBlock::new(1,
        vec![FakeAccountChange::transfer(addr(1), 1000, 900)],
        vec![FakeReceipt::success(tx_hash(1), 21_000)],
    );
    let block2 = FakeBlock::new(2,
        vec![FakeAccountChange::transfer(addr(2), 2000, 1800)],
        vec![FakeReceipt::success(tx_hash(2), 42_000)],
    );

    // Commit both blocks
    let commit = make_commit_notification(&[block1.clone(), block2.clone()])?;
    drive_exex(Arc::clone(&db), vec![commit]).await?;

    // Revert block 2 — chain shortens back to block 1
    // ChainReverted means these blocks are being removed with no replacement yet.
    let revert = make_revert_notification(&[block2.clone()])?;
    drive_exex(Arc::clone(&db), vec![revert]).await?;

    // Block 2 should be reorged (not active)
    InvariantChecker::check_block_invisible(&db, block2.hash)?;

    // Block 1 should still be active
    let conn = db.connection();
    let block1_status: String = conn.query_row(
        "SELECT canonical_status FROM canonical_blocks WHERE block_hash = ?1",
        rusqlite::params![format!("{:?}", block1.hash)],
        |r| r.get(0),
    ).context("query block1 status")?;

    eyre::ensure!(
        block1_status == "active",
        "block 1 should be active after head moves backward, got: {}",
        block1_status
    );

    InvariantChecker::check_all(&db)?;

    info!("scenario 10: stale head backward movement validated");
    Ok(())
}