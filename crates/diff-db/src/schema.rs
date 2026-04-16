use rusqlite::Connection;
use crate::DbResult;

/// Run all schema migrations.
/// Safe to call on every startup — all statements are idempotent.
pub fn run_migrations(conn: &Connection) -> DbResult<()> {
    conn.execute_batch(MIGRATION_V1)?;
    tracing::debug!("schema migrations applied");
    Ok(())
}

const MIGRATION_V1: &str = "
-- Tracks which blocks are canonical and which got reorged.
-- We never delete rows here. We flip canonical_status on reorg.
-- This gives us a full audit trail of every reorg we witnessed.
CREATE TABLE IF NOT EXISTS canonical_blocks (
    block_number          INTEGER NOT NULL,
    block_hash            TEXT    NOT NULL,
    parent_hash           TEXT    NOT NULL,
    canonical_status      TEXT    NOT NULL DEFAULT 'active',
    finalized_hint        INTEGER,          -- 1/0/NULL
    derived_checkpoint_id INTEGER,
    PRIMARY KEY (block_hash)
);

-- Index for fast lookup by number (used during reorg walks).
CREATE INDEX IF NOT EXISTS idx_canonical_blocks_number
    ON canonical_blocks (block_number);

-- Per-block account state changes.
-- block_hash is part of the PK so reorged blocks coexist with
-- their replacements without collision.
CREATE TABLE IF NOT EXISTS account_diffs (
    block_number   INTEGER NOT NULL,
    block_hash     TEXT    NOT NULL,
    address        TEXT    NOT NULL,
    old_balance    TEXT,
    new_balance    TEXT,
    old_nonce      INTEGER,
    new_nonce      INTEGER,
    old_code_hash  TEXT,
    new_code_hash  TEXT,
    change_kind    TEXT    NOT NULL,
    PRIMARY KEY (block_hash, address)
);

CREATE INDEX IF NOT EXISTS idx_account_diffs_address
    ON account_diffs (address, block_number);

-- Per-block storage slot changes.
CREATE TABLE IF NOT EXISTS storage_diffs (
    block_number INTEGER NOT NULL,
    block_hash   TEXT    NOT NULL,
    address      TEXT    NOT NULL,
    slot         TEXT    NOT NULL,
    old_value    TEXT    NOT NULL,
    new_value    TEXT    NOT NULL,
    PRIMARY KEY (block_hash, address, slot)
);

CREATE INDEX IF NOT EXISTS idx_storage_diffs_address
    ON storage_diffs (address, block_number);

-- Receipt data + everything needed to generate an MPT proof.
CREATE TABLE IF NOT EXISTS receipt_artifacts (
    block_number         INTEGER NOT NULL,
    block_hash           TEXT    NOT NULL,
    tx_index             INTEGER NOT NULL,
    tx_hash              TEXT    NOT NULL,
    receipt_index        INTEGER NOT NULL,
    receipt_rlp          BLOB    NOT NULL,
    receipt_root_anchor  TEXT    NOT NULL,
    log_bloom            BLOB    NOT NULL,
    status               INTEGER NOT NULL,
    cumulative_gas_used  INTEGER NOT NULL,
    PRIMARY KEY (block_hash, tx_index)
);

CREATE INDEX IF NOT EXISTS idx_receipt_artifacts_tx_hash
    ON receipt_artifacts (tx_hash);

-- Undo log. Written at commit time, replayed at reorg time.
-- Apply in descending op_sequence order when reverting a block.
CREATE TABLE IF NOT EXISTS revert_ops (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    reorg_target_block  INTEGER NOT NULL,
    op_sequence         INTEGER NOT NULL,
    table_name          TEXT    NOT NULL,
    primary_key_ref     TEXT    NOT NULL,
    inverse_payload     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_revert_ops_block
    ON revert_ops (reorg_target_block, op_sequence DESC);

-- Four-cursor progress tracking.
-- One row per checkpoint event. Latest row is current state.
CREATE TABLE IF NOT EXISTS stage_checkpoints (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    streaming_cursor      INTEGER NOT NULL,
    durable_cursor        INTEGER NOT NULL,
    compacted_until       INTEGER NOT NULL,
    proof_indexed_until   INTEGER NOT NULL,
    canonical_tip         INTEGER NOT NULL,
    exex_finished_height  INTEGER NOT NULL
);
";