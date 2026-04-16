//! Secondary index builders for the compaction stage.
//!
//! These functions read raw diff rows (written by the ExEx) and insert into
//! the stage-owned index tables (written only by the stage).
//!
//! All inserts use INSERT OR IGNORE for idempotence: if the stage crashes
//! mid-block and restarts, re-processing the same block produces the same
//! index rows without error.

use eyre::Context;
use rusqlite::{params, Connection};
use tracing::debug;

// ---------------------------------------------------------------------------
// Address index
// ---------------------------------------------------------------------------

/// Build address_block_index entries for a single block.
///
/// Reads all account_diffs rows for (block_number, block_hash) and inserts
/// one address_block_index row per unique address.
///
/// INSERT OR IGNORE makes this idempotent — safe to call twice for the same
/// block if the stage restarts mid-compaction.
pub fn build_address_index_for_block(
    conn: &Connection,
    block_number: u64,
    block_hash: &str,
) -> eyre::Result<usize> {
    // Read all account diffs for this block.
    let mut stmt = conn
        .prepare_cached(
            "SELECT address, change_kind
             FROM account_diffs
             WHERE block_number = ?1 AND block_hash = ?2",
        )
        .context("prepare account_diffs select")?;

    let rows: Vec<(String, String)> = stmt
        .query_map(params![block_number as i64, block_hash], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .context("query account_diffs")?
        .collect::<Result<_, _>>()
        .context("collect account_diffs rows")?;

    let count = rows.len();

    // Insert into index. INSERT OR IGNORE: duplicate = already indexed = fine.
    let mut insert_stmt = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO address_block_index
                 (address, block_number, block_hash, change_kind)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .context("prepare address_block_index insert")?;

    for (address, change_kind) in &rows {
        insert_stmt
            .execute(params![address, block_number as i64, block_hash, change_kind])
            .context("insert address_block_index row")?;
    }

    debug!(
        block_number,
        block_hash,
        indexed = count,
        "built address index for block"
    );

    Ok(count)
}

/// Delete all address_block_index rows with block_number > unwind_to.
///
/// Called during stage unwind. After this call the index only contains
/// entries for blocks [0, unwind_to] on whatever branch was canonical.
///
/// Note: block_hash is part of the primary key so if a reorged block and the
/// canonical block have the same number but different hashes, only the correct
/// rows are deleted (both are deleted above unwind_to, which is correct —
/// only the new canonical branch will be re-indexed on replay).
pub fn delete_address_index_above(
    conn: &Connection,
    unwind_to: u64,
) -> eyre::Result<usize> {
    let deleted = conn
        .execute(
            "DELETE FROM address_block_index WHERE block_number > ?1",
            params![unwind_to as i64],
        )
        .context("delete address_block_index above unwind target")?;

    debug!(unwind_to, deleted, "unwound address index");
    Ok(deleted)
}

// ---------------------------------------------------------------------------
// Slot index
// ---------------------------------------------------------------------------

/// Build slot_block_index entries for a single block.
///
/// Reads all storage_diffs rows for (block_number, block_hash) and inserts
/// one slot_block_index row per (address, slot) pair.
pub fn build_slot_index_for_block(
    conn: &Connection,
    block_number: u64,
    block_hash: &str,
) -> eyre::Result<usize> {
    let mut stmt = conn
        .prepare_cached(
            "SELECT address, slot
             FROM storage_diffs
             WHERE block_number = ?1 AND block_hash = ?2",
        )
        .context("prepare storage_diffs select")?;

    let rows: Vec<(String, String)> = stmt
        .query_map(params![block_number as i64, block_hash], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .context("query storage_diffs")?
        .collect::<Result<_, _>>()
        .context("collect storage_diffs rows")?;

    let count = rows.len();

    let mut insert_stmt = conn
        .prepare_cached(
            "INSERT OR IGNORE INTO slot_block_index
                 (address, slot, block_number, block_hash)
             VALUES (?1, ?2, ?3, ?4)",
        )
        .context("prepare slot_block_index insert")?;

    for (address, slot) in &rows {
        insert_stmt
            .execute(params![address, slot, block_number as i64, block_hash])
            .context("insert slot_block_index row")?;
    }

    debug!(
        block_number,
        block_hash,
        indexed = count,
        "built slot index for block"
    );

    Ok(count)
}

/// Delete all slot_block_index rows with block_number > unwind_to.
pub fn delete_slot_index_above(
    conn: &Connection,
    unwind_to: u64,
) -> eyre::Result<usize> {
    let deleted = conn
        .execute(
            "DELETE FROM slot_block_index WHERE block_number > ?1",
            params![unwind_to as i64],
        )
        .context("delete slot_block_index above unwind target")?;

    debug!(unwind_to, deleted, "unwound slot index");
    Ok(deleted)
}