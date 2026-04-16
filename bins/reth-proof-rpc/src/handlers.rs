//! Axum request handlers for the three RPC endpoints.
//!
//! Each handler:
//!   1. Validates the request
//!   2. Spawns a blocking task (SQLite is synchronous)
//!   3. Opens a DiffDb connection inside the blocking task
//!   4. Runs the query
//!   5. Returns the result as JSON or an AppError

use std::path::PathBuf;

use axum::{extract::State, Json};
use diff_db::DiffDb;
use diff_proof::ProofExtractor;
use eyre::Context;
use rusqlite::params;
use tracing::info;

use crate::{
    error::AppError,
    types::{
        AccountDiffRequest, AccountDiffResponse,
        ReceiptProofRequest, ReceiptProofResponse,
        StorageDiffRequest, StorageDiffResponse,
    },
};

// ---------------------------------------------------------------------------
// Shared app state
// ---------------------------------------------------------------------------

/// Axum application state passed to every handler via State<AppState>.
///
/// We store only the DB file path — each handler opens its own connection.
/// This avoids the !Send/!Sync issues with rusqlite::Connection in async
/// contexts.
///
/// If you need higher throughput, replace this with an r2d2 or deadpool
/// connection pool. The handler code does not need to change — just update
/// AppState and how the connection is acquired.
#[derive(Debug, Clone)]
pub struct AppState {
    pub db_path: PathBuf,
}

impl AppState {
    pub fn new(db_path: PathBuf) -> Self {
        Self { db_path }
    }
}

// ---------------------------------------------------------------------------
// POST /receipt_proof
// ---------------------------------------------------------------------------

/// Generate a receipt MPT inclusion proof for a transaction hash.
///
/// Returns 404 if the transaction is not indexed or its block is
/// non-canonical (reorged out).
///
/// Returns 500 if the stored receipts_root does not match the
/// reconstructed trie root (data integrity violation).
pub async fn receipt_proof(
    State(state): State<AppState>,
    Json(req): Json<ReceiptProofRequest>,
) -> Result<Json<ReceiptProofResponse>, AppError> {
    info!(tx_hash = %req.tx_hash, "receipt_proof request");

    let db_path  = state.db_path.clone();
    let tx_hash  = req.tx_hash;

    let proof = tokio::task::spawn_blocking(move || {
        // Open a fresh connection for this request.
        let db = std::sync::Arc::new(
            DiffDb::open(&db_path)
                .context("open db in receipt_proof handler")?,
        );

        let extractor = ProofExtractor::new(db);

        extractor
            .get_receipt_proof_by_tx_hash(tx_hash)
            .context("get_receipt_proof_by_tx_hash")
    })
    .await
    // JoinError means the blocking task panicked.
    .map_err(|e| eyre::eyre!("spawn_blocking panicked: {}", e))?
    // Propagate the inner eyre::Report.
    .map_err(|e: eyre::Report| {
        // Distinguish "not found" from other errors by inspecting the message.
        // A cleaner approach is a typed error in ProofExtractor, but for now
        // a string check is sufficient.
        let msg = format!("{:?}", e);
        if msg.contains("not found") || msg.contains("non-canonical") {
            AppError::not_found(format!("receipt not found for tx {}", tx_hash))
        } else {
            AppError::Internal(e)
        }
    })?;

    Ok(Json(ReceiptProofResponse { proof }))
}

// ---------------------------------------------------------------------------
// POST /account_diff
// ---------------------------------------------------------------------------

/// Return all canonical account state changes for an address in a block range.
///
/// Queries account_diffs joined to canonical_blocks (active only).
/// Results are in ascending block order.
///
/// Returns an empty diffs list (not 404) if the address was never touched
/// in the range — absence of data is a valid answer.
pub async fn account_diff(
    State(state): State<AppState>,
    Json(req): Json<AccountDiffRequest>,
) -> Result<Json<AccountDiffResponse>, AppError> {
    info!(
        address    = %req.address,
        from_block = req.from_block,
        to_block   = req.to_block,
        "account_diff request"
    );

    // Validate range.
    if req.from_block > req.to_block {
        return Err(AppError::bad_request(format!(
            "from_block {} > to_block {}",
            req.from_block, req.to_block
        )));
    }

    let db_path    = state.db_path.clone();
    let address    = req.address;
    let from_block = req.from_block;
    let to_block   = req.to_block;

    let diffs = tokio::task::spawn_blocking(move || {
        let db = DiffDb::open(&db_path)
            .context("open db in account_diff handler")?;

        query_account_diffs(&db, address, from_block, to_block)
    })
    .await
    .map_err(|e| eyre::eyre!("spawn_blocking panicked: {}", e))?
    .map_err(AppError::from)?;

    Ok(Json(AccountDiffResponse {
        address,
        from_block,
        to_block,
        diffs,
    }))
}

// ---------------------------------------------------------------------------
// POST /storage_diff
// ---------------------------------------------------------------------------

/// Return all canonical storage slot changes for an (address, slot) pair
/// in a block range.
pub async fn storage_diff(
    State(state): State<AppState>,
    Json(req): Json<StorageDiffRequest>,
) -> Result<Json<StorageDiffResponse>, AppError> {
    info!(
        address    = %req.address,
        slot       = %req.slot,
        from_block = req.from_block,
        to_block   = req.to_block,
        "storage_diff request"
    );

    if req.from_block > req.to_block {
        return Err(AppError::bad_request(format!(
            "from_block {} > to_block {}",
            req.from_block, req.to_block
        )));
    }

    let db_path    = state.db_path.clone();
    let address    = req.address;
    let slot       = req.slot;
    let from_block = req.from_block;
    let to_block   = req.to_block;

    let diffs = tokio::task::spawn_blocking(move || {
        let db = DiffDb::open(&db_path)
            .context("open db in storage_diff handler")?;

        query_storage_diffs(&db, address, slot, from_block, to_block)
    })
    .await
    .map_err(|e| eyre::eyre!("spawn_blocking panicked: {}", e))?
    .map_err(AppError::from)?;

    Ok(Json(StorageDiffResponse {
        address,
        slot,
        from_block,
        to_block,
        diffs,
    }))
}

// ---------------------------------------------------------------------------
// SQL query helpers
// ---------------------------------------------------------------------------

/// Query account diffs for an address over a canonical block range.
///
/// JOINs canonical_blocks and filters active-only so reorged blocks are
/// never returned. Results ordered by block_number ASC.
///
/// Uses address_block_index for fast address lookup if it has been
/// populated by the compaction stage. Falls back cleanly to a full
/// account_diffs scan if the index is empty — the JOIN-based filter
/// ensures correctness either way.
fn query_account_diffs(
    db:         &DiffDb,
    address:    alloy_primitives::Address,
    from_block: u64,
    to_block:   u64,
) -> eyre::Result<Vec<diff_types::AccountDiff>> {
    let conn        = db.connection();
    let address_str = format!("{:?}", address);

    let mut stmt = conn
        .prepare_cached(
            // We query account_diffs directly with canonical join.
            // address_block_index could narrow the block set first but
            // the canonical join is already selective. For simplicity
            // we skip the two-step index lookup here.
            "SELECT
                ad.block_number,
                ad.block_hash,
                ad.address,
                ad.old_balance,
                ad.new_balance,
                ad.old_nonce,
                ad.new_nonce,
                ad.old_code_hash,
                ad.new_code_hash,
                ad.change_kind
             FROM account_diffs ad
             JOIN canonical_blocks cb
               ON ad.block_number = cb.block_number
              AND ad.block_hash   = cb.block_hash
             WHERE ad.address      = ?1
               AND ad.block_number >= ?2
               AND ad.block_number <= ?3
               AND cb.canonical_status = 'active'
             ORDER BY ad.block_number ASC",
        )
        .context("prepare account_diffs query")?;

    let rows = stmt
        .query_map(
            params![address_str, from_block as i64, to_block as i64],
            |row| {
                Ok(diff_types::AccountDiff {
                    block_number:  row.get::<_, i64>(0)? as u64,
                    block_hash:    row.get::<_, String>(1)?
                                      .parse()
                                      .unwrap_or_default(),
                    address:       row.get::<_, String>(2)?
                                      .parse()
                                      .unwrap_or_default(),
                    old_balance:   row.get::<_, Option<String>>(3)?
                                      .and_then(|s| s.parse().ok()),
                    new_balance:   row.get::<_, Option<String>>(4)?
                                      .and_then(|s| s.parse().ok()),
                    old_nonce:     row.get::<_, Option<i64>>(5)?
                                      .map(|n| n as u64),
                    new_nonce:     row.get::<_, Option<i64>>(6)?
                                      .map(|n| n as u64),
                    old_code_hash: row.get::<_, Option<String>>(7)?
                                      .and_then(|s| s.parse().ok()),
                    new_code_hash: row.get::<_, Option<String>>(8)?
                                      .and_then(|s| s.parse().ok()),
                    change_kind:   row.get::<_, String>(9)?
                                      .parse()
                                      .unwrap_or_default(),
                })
            },
        )
        .context("execute account_diffs query")?
        .collect::<Result<Vec<_>, _>>()
        .context("collect account_diffs rows")?;

    Ok(rows)
}

/// Query storage diffs for an (address, slot) pair over a canonical block range.
fn query_storage_diffs(
    db:         &DiffDb,
    address:    alloy_primitives::Address,
    slot:       alloy_primitives::U256,
    from_block: u64,
    to_block:   u64,
) -> eyre::Result<Vec<diff_types::StorageDiff>> {
    let conn        = db.connection();
    let address_str = format!("{:?}", address);
    let slot_str    = format!("{:?}", slot);

    let mut stmt = conn
        .prepare_cached(
            "SELECT
                sd.block_number,
                sd.block_hash,
                sd.address,
                sd.slot,
                sd.old_value,
                sd.new_value
             FROM storage_diffs sd
             JOIN canonical_blocks cb
               ON sd.block_number = cb.block_number
              AND sd.block_hash   = cb.block_hash
             WHERE sd.address      = ?1
               AND sd.slot         = ?2
               AND sd.block_number >= ?3
               AND sd.block_number <= ?4
               AND cb.canonical_status = 'active'
             ORDER BY sd.block_number ASC",
        )
        .context("prepare storage_diffs query")?;

    let rows = stmt
        .query_map(
            params![address_str, slot_str, from_block as i64, to_block as i64],
            |row| {
                Ok(diff_types::StorageDiff {
                    block_number: row.get::<_, i64>(0)? as u64,
                    block_hash:   row.get::<_, String>(1)?
                                     .parse()
                                     .unwrap_or_default(),
                    address:      row.get::<_, String>(2)?
                                     .parse()
                                     .unwrap_or_default(),
                    slot:         row.get::<_, String>(3)?
                                     .parse()
                                     .unwrap_or_default(),
                    old_value:    row.get::<_, String>(4)?
                                     .parse()
                                     .unwrap_or_default(),
                    new_value:    row.get::<_, String>(5)?
                                     .parse()
                                     .unwrap_or_default(),
                })
            },
        )
        .context("execute storage_diffs query")?
        .collect::<Result<Vec<_>, _>>()
        .context("collect storage_diffs rows")?;

    Ok(rows)
}