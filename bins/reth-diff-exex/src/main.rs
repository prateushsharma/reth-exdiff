//! Binary: reth-diff-exex
//!
//! Wires the DiffExEx execution extension into a real Reth Ethereum node.
//!
//! Usage:
//!   reth-diff-exex node --diff-db-path /data/diff.db [standard reth flags]
//!
//! This binary is intentionally thin. All real logic lives in:
//!   crates/diff-exex  — notification handling, diff extraction, revert ops
//!   crates/diff-db    — SQLite persistence layer
//!
//! The only jobs of main.rs are:
//!   1. parse the extra --diff-db-path CLI flag alongside Reth's own flags
//!   2. open the DiffDb at that path
//!   3. construct DiffExEx and hand its future to install_exex
//!   4. launch the node

use std::path::PathBuf;

use clap::Parser;
use diff_db::DiffDb;
use diff_exex::DiffExEx;
use eyre::Context;
use reth_ethereum::EthereumNode;
use reth_node_builder::NodeHandle;
use tracing::info;

// ---------------------------------------------------------------------------
// Extra CLI arguments specific to this binary.
//
// Reth's Cli<Ext> type parameter lets you bolt extra clap-parsed fields onto
// the standard Reth CLI without touching Reth internals. These fields show up
// in --help alongside all the standard Reth flags.
// ---------------------------------------------------------------------------

/// Extra CLI arguments for the diff-exex binary.
#[derive(Debug, Clone, Parser)]
pub struct DiffExExArgs {
    /// Path to the SQLite database file where canonical diffs are stored.
    ///
    /// The database is created automatically if it does not exist.
    /// WAL mode and foreign keys are enabled on open.
    ///
    /// Example: --diff-db-path /var/lib/reth-diff/diff.db
    #[arg(long, value_name = "PATH", default_value = "diff.db")]
    pub diff_db_path: PathBuf,
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> eyre::Result<()> {
    // Coloured, human-readable error backtraces via color_eyre.
    // Must be called before any eyre::Report is created.
    color_eyre::install()?;

    // Structured logging via tracing.
    // Reth itself uses tracing throughout, so this picks up Reth's internal
    // spans as well as our own diff-exex spans.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                // Default to info level for our crates, warn for everything else.
                .add_directive("diff_exex=info".parse()?)
                .add_directive("diff_db=info".parse()?)
                .add_directive("reth=warn".parse()?),
        )
        .init();

    info!("reth-diff-exex starting");

    // Parse all CLI flags: standard Reth flags + our DiffExExArgs.
    //
    // reth_ethereum::cli::Cli is a thin wrapper around clap that knows how
    // to set up the Reth builder from the parsed flags. The type parameter
    // DiffExExArgs gets merged into the same clap command so the user sees
    // one unified --help output.
    reth_ethereum::cli::Cli::<DiffExExArgs>::parse_args()
        .run(|builder, extra_args| async move {
            // ------------------------------------------------------------------
            // Open the derived-state database.
            //
            // DiffDb::open runs the schema migration (CREATE TABLE IF NOT EXISTS
            // for all six tables), sets WAL mode, and enables foreign keys.
            // If the file does not exist it is created.
            //
            // We do this inside the .run() closure so that if the path is
            // invalid or the disk is full we get a clean error before the
            // node starts consuming resources.
            // ------------------------------------------------------------------
            let db_path = &extra_args.diff_db_path;
            info!(path = %db_path.display(), "opening diff database");

            let db = DiffDb::open(db_path)
                .with_context(|| format!("failed to open diff db at {}", db_path.display()))?;

            info!("diff database ready");

            // ------------------------------------------------------------------
            // Build the node and install the ExEx.
            //
            // EthereumNode::default() selects the standard Ethereum mainnet
            // node configuration (or whichever chain is specified via --chain).
            //
            // install_exex("diff-exex", factory_closure) registers the ExEx.
            // The factory closure receives an ExExContext<EthereumNode> at
            // node startup and must return a Future that implements the ExEx
            // protocol (i.e. consumes notifications and sends FinishedHeight).
            //
            // DiffExEx::new(ctx, db) constructs the processor.
            // DiffExEx::run() is the async loop that drives it.
            //
            // The closure is async move so it captures db by move (DiffDb is
            // not Clone — it owns a single rusqlite Connection).
            // ------------------------------------------------------------------
            let NodeHandle { node: _, node_exit_future } = builder
                .node(EthereumNode::default())
                .install_exex("diff-exex", move |ctx| {
                    // db is moved into this closure.
                    // ctx is the ExExContext<EthereumNode> for this ExEx instance.
                    async move {
                        let exex = DiffExEx::new(ctx, db);
                        Ok(exex.run())
                    }
                })
                .launch()
                .await?;

            // ------------------------------------------------------------------
            // Wait for the node to shut down.
            //
            // node_exit_future resolves when Reth has finished its graceful
            // shutdown sequence (e.g. SIGINT / SIGTERM received, all stages
            // have checkpointed, network disconnected).
            //
            // We just await it here. If we returned immediately the process
            // would exit and the node would not run.
            // ------------------------------------------------------------------
            node_exit_future.await?;

            info!("reth-diff-exex shut down cleanly");
            Ok(())
        })
        .await
}