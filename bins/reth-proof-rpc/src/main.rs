//! Binary: reth-proof-rpc
//!
//! Standalone HTTP JSON server exposing three read-only endpoints over
//! the DiffDb populated by reth-diff-stage.
//!
//! Endpoints:
//!   POST /receipt_proof  — MPT receipt inclusion proof
//!   POST /account_diff   — canonical account state changes for address+range
//!   POST /storage_diff   — canonical storage changes for address+slot+range
//!
//! Does NOT run a Reth node. Read-only against the SQLite diff database.
//! Safe to run concurrently with reth-diff-stage (WAL mode, separate
//! connection per request).

mod error;
mod handlers;
mod types;

use std::net::SocketAddr;
use std::path::PathBuf;

use axum::{routing::post, Router};
use clap::Parser;
use eyre::Context;
use handlers::AppState;
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::info;

// ---------------------------------------------------------------------------
// CLI arguments
// ---------------------------------------------------------------------------

#[derive(Debug, Parser)]
pub struct RpcArgs {
    /// Path to the SQLite diff database written by reth-diff-stage.
    ///
    /// Must be the same file path used by the running reth-diff-stage
    /// instance. Read-only access; WAL mode allows concurrent reads
    /// alongside reth-diff-stage writes.
    #[arg(long, value_name = "PATH", default_value = "diff.db")]
    pub diff_db_path: PathBuf,

    /// Address and port to bind the HTTP server.
    ///
    /// Examples:
    ///   0.0.0.0:8080   (listen on all interfaces, port 8080)
    ///   127.0.0.1:9000 (localhost only, port 9000)
    #[arg(long, value_name = "ADDR:PORT", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,
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
                .add_directive("reth_proof_rpc=info".parse()?)
                .add_directive("diff_db=info".parse()?)
                .add_directive("diff_proof=info".parse()?),
        )
        .init();

    let args = RpcArgs::parse();

    info!(
        db_path = %args.diff_db_path.display(),
        listen  = %args.listen,
        "reth-proof-rpc starting"
    );

    // Verify the DB file exists and is openable before binding the port.
    // Fail fast: if the DB path is wrong we want an error immediately,
    // not after clients start connecting.
    {
        let _db = diff_db::DiffDb::open(&args.diff_db_path)
            .with_context(|| {
                format!(
                    "cannot open diff database at {} — \
                     is reth-diff-stage running with the same path?",
                    args.diff_db_path.display()
                )
            })?;
        info!("diff database verified");
        // Connection drops here — handlers open their own per-request connections.
    }

    // ------------------------------------------------------------------
    // Build the axum router.
    //
    // AppState carries only the DB path. Each handler opens its own
    // connection via spawn_blocking.
    //
    // Middleware:
    //   TraceLayer  — logs every request with method, path, status, latency
    //   CorsLayer   — allows cross-origin requests (useful for browser-based
    //                 tooling querying the RPC). Restrict origins in prod.
    // ------------------------------------------------------------------
    let state = AppState::new(args.diff_db_path);

    let app = Router::new()
        .route("/receipt_proof", post(handlers::receipt_proof))
        .route("/account_diff",  post(handlers::account_diff))
        .route("/storage_diff",  post(handlers::storage_diff))
        .route("/health",        axum::routing::get(health_check))
        .with_state(state)
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    // ------------------------------------------------------------------
    // Bind and serve.
    //
    // axum::serve uses tokio's TCP listener. It runs until the process
    // receives SIGINT/SIGTERM, at which point tokio's signal handling
    // triggers a graceful shutdown.
    // ------------------------------------------------------------------
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("bind TCP listener on {}", args.listen))?;

    info!(addr = %args.listen, "HTTP server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve error")?;

    info!("reth-proof-rpc shut down cleanly");
    Ok(())
}

// ---------------------------------------------------------------------------
// Health check endpoint
// ---------------------------------------------------------------------------

/// GET /health — returns 200 OK with { "status": "ok" }.
///
/// Used by load balancers and monitoring systems to verify the server
/// is alive and reachable. Does not check DB connectivity.
async fn health_check() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({ "status": "ok" }))
}

// ---------------------------------------------------------------------------
// Graceful shutdown signal handler
// ---------------------------------------------------------------------------

/// Returns a future that resolves when SIGINT or SIGTERM is received.
///
/// axum::serve::with_graceful_shutdown uses this to drain in-flight
/// requests before exiting.
async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install CTRL+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c    => { info!("received SIGINT, shutting down") }
        _ = terminate => { info!("received SIGTERM, shutting down") }
    }
}