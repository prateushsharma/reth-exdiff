//! Schema migrations for stage-owned index tables.
//!
//! These tables are populated by DiffCompactionStage and are never written
//! by the ExEx. They are secondary indexes over the raw diff rows that the
//! ExEx writes into account_diffs and storage_diffs.
//!
//! Invariant: these tables never contain data for blocks above
//! stage_checkpoints.compacted_until. If they do, the stage has a bug.

use eyre::Context;
use rusqlite::Connection;

/// Create the stage-owned index tables if they do not already exist.
///
/// Called once during DiffCompactionStage construction, before any execute()
/// or unwind() call. Safe to call on every restart — all statements are
/// IF NOT EXISTS.
pub fn run_stage_migrations(conn: &Connection) -> eyre::Result<()> {
    conn.execute_batch(
        "
        -- Secondary index: which blocks touched each address?
        -- Populated from account_diffs during compaction.
        -- Primary key includes block_hash so two forks at the same height
        -- get separate rows and unwind can delete the right one.
        CREATE TABLE IF NOT EXISTS address_block_index (
            address      TEXT    NOT NULL,
            block_number INTEGER NOT NULL,
            block_hash   TEXT    NOT NULL,
            change_kind  TEXT    NOT NULL,
            PRIMARY KEY (address, block_number, block_hash)
        );

        -- Fast lookup: all (address, block_number) pairs for a given address.
        -- Used by get_account_diff(address, from_block, to_block).
        CREATE INDEX IF NOT EXISTS idx_abi_address_block
            ON address_block_index (address, block_number);

        -- Secondary index: which blocks touched each (address, slot) pair?
        -- Populated from storage_diffs during compaction.
        CREATE TABLE IF NOT EXISTS slot_block_index (
            address      TEXT    NOT NULL,
            slot         TEXT    NOT NULL,
            block_number INTEGER NOT NULL,
            block_hash   TEXT    NOT NULL,
            PRIMARY KEY (address, slot, block_number, block_hash)
        );

        -- Fast lookup: all blocks for a given (address, slot).
        -- Used by get_storage_diff(address, slot, from_block, to_block).
        CREATE INDEX IF NOT EXISTS idx_sbi_address_slot_block
            ON slot_block_index (address, slot, block_number);
        ",
    )
    .context("failed to run stage schema migrations")?;

    Ok(())
}