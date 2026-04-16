//! Binary: reth-diff-stage
//!
//! Runs a Reth Ethereum node with:
//!   - DiffExEx installed as an in-process execution extension
//!   - DiffCompactionStage driven by a background tokio task
//!
//! The ExEx and the compaction task each open their own DiffDb connection
//! to the same SQLite file (WAL mode makes this safe for concurrent access).
//!
//! The ExEx signals reorgs to the compaction task via a watch channel,
//! so the stage can unwind its index tables before resuming forward
//! compaction on the new canonical branch.

mod compaction;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use diff_db::DiffDb;
use diff_exex::DiffExEx;
use eyre::Context;
use reth_ethereum::EthereumNode;
use reth_node_builder::NodeHandle;
use tokio::sync::watch;
use tracing::info;

// ---------------------------------------------------------------------------
// CLI arguments
// ---------------------------------------------------------------------------

/// Extra CLI arguments for reth-diff-stage.
#[derive(Debug, Clone, Parser)]
pub struct DiffStageArgs {
    /// Path to the SQLite diff database.
    ///
    /// Both the ExEx and the compaction task open separate connections to
    /// this file. SQLite WAL mode (enabled on open) makes concurrent
    /// read/write safe.
    #[arg(long, value_name = "PATH", default_value = "diff.db")]
    pub diff_db_path: PathBuf,

    /// How often the compaction task polls for new blocks to compact,
    /// in milliseconds. Default: 2000ms (2 seconds).
    ///
    /// Lower values make the compacted_until cursor track the ExEx more
    /// closely. Higher values reduce SQLite write pressure during sync.
    #[arg(long, value_name = "MS", default_value = "2000")]
    pub compaction_interval_ms: u64,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("reth_diff_stage=info".parse()?)
                .add_directive("diff_exex=info".parse()?)
                .add_directive("diff_db=info".parse()?)
                .add_directive("diff_stage=info".parse()?)
                .add_directive("reth=warn".parse()?),
        )
        .init();

    info!("reth-diff-stage starting");

    reth_ethereum::cli::Cli::<DiffStageArgs>::parse_args()
        .run(|builder, extra_args| async move {
            let db_path = extra_args.diff_db_path.clone();

            // ------------------------------------------------------------------
            // Reorg signal channel.
            //
            // ExEx sends the unwind-to block number here on every reorg.
            // Compaction task receives and unwinds the stage index.
            //
            // Initial value is None — no reorg has happened yet.
            // ------------------------------------------------------------------
            let (reorg_tx, reorg_rx) = watch::channel::<Option<u64>>(None);

            // ------------------------------------------------------------------
            // Open the ExEx's DiffDb connection.
            //
            // The compaction task opens a separate connection below.
            // Each connection gets its own DiffDb so there is no shared
            // mutable state between the ExEx tokio task and the compaction task.
            // ------------------------------------------------------------------
            let exex_db_path = db_path.clone();
            info!(path = %exex_db_path.display(), "opening ExEx diff database");

            let exex_db = DiffDb::open(&exex_db_path)
                .with_context(|| {
                    format!("open ExEx diff db at {}", exex_db_path.display())
                })?;

            info!("ExEx diff database ready");

            // ------------------------------------------------------------------
            // Open the compaction task's DiffDb connection.
            //
            // Opened before the node launches so any schema errors are caught
            // early, not after the node has started syncing.
            // ------------------------------------------------------------------
            let compaction_db_path = db_path.clone();
            info!(path = %compaction_db_path.display(), "opening compaction diff database");

            let compaction_db = Arc::new(
                DiffDb::open(&compaction_db_path)
                    .with_context(|| {
                        format!("open compaction diff db at {}", compaction_db_path.display())
                    })?
            );

            info!("compaction diff database ready");

            // ------------------------------------------------------------------
            // Spawn the background compaction task.
            //
            // This runs independently of the node's tokio runtime — it is a
            // plain tokio::spawn on the current runtime. It loops until the
            // reorg_rx sender is dropped (node shutdown).
            // ------------------------------------------------------------------
            let compaction_task_handle = tokio::spawn(
                compaction::run_compaction_loop(
                    Arc::clone(&compaction_db),
                    reorg_rx,
                )
            );

            info!("compaction task spawned");

            // ------------------------------------------------------------------
            // Build and launch the node with the ExEx installed.
            //
            // The ExEx receives reorg_tx so it can signal the compaction task
            // after every ChainReorged / ChainReverted event.
            // ------------------------------------------------------------------
            let NodeHandle { node: _, node_exit_future } = builder
                .node(EthereumNode::default())
                .install_exex("diff-exex", move |ctx| {
                    // Each of these is moved into the async block.
                    // exex_db is not Clone — it is owned by this closure.
                    // reorg_tx is Clone (watch sender is Arc-wrapped internally).
                    let reorg_tx_clone = reorg_tx.clone();

                    async move {
                        let exex = DiffExEx::new_with_reorg_signal(
                            ctx,
                            exex_db,
                            reorg_tx_clone,
                        );
                        Ok(exex.run())
                    }
                })
                .launch()
                .await?;

            // ------------------------------------------------------------------
            // Wait for the node to shut down gracefully.
            //
            // When node_exit_future resolves:
            //   - The ExEx loop will exit (ExExContext notification channel closes)
            //   - reorg_tx is dropped when ExEx exits
            //   - reorg_rx in the compaction task sees sender dropped
            //   - compaction loop exits its watch::channel::has_changed() check
            //     on the next iteration and the task completes
            //
            // We wait for the compaction task too so we do not leave it
            // mid-write when the process exits.
            // ------------------------------------------------------------------
            node_exit_future.await?;

            info!("node shut down, waiting for compaction task to finish");

            // Give the compaction task up to 5 seconds to finish its current
            // write before we force-exit.
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                compaction_task_handle,
            )
            .await
            .ok(); // ignore timeout or join error — we are shutting down anyway

            info!("reth-diff-stage shut down cleanly");
            Ok(())
        })
        .await
}