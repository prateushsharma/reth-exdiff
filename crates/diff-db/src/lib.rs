use rusqlite::Connection;
use std::path::Path;
use thiserror::Error;

pub mod schema;
pub mod canonical;
pub mod account;
pub mod storage;
pub mod receipt;
pub mod revert;
pub mod checkpoint;

pub use canonical::*;
pub use account::*;
pub use storage::*;
pub use receipt::*;
pub use revert::*;
pub use checkpoint::*;

/// All errors that can come from the database layer.
#[derive(Debug, Error)]
pub enum DbError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid enum value: {0}")]
    InvalidEnum(String),

    #[error("row not found: {0}")]
    NotFound(String),
}

pub type DbResult<T> = Result<T, DbError>;

/// The main database handle.
/// Wraps a SQLite connection and runs migrations on open.
/// All table functions are methods on this struct.
pub struct DiffDb {
    pub(crate) conn: Connection,
}

impl DiffDb {
    /// Open or create the database at the given path.
    /// Runs all schema migrations immediately.
    pub fn open<P: AsRef<Path>>(path: P) -> DbResult<Self> {
        let conn = Connection::open(path)?;

        // Enable WAL mode for better concurrent read performance
        // and crash safety. This is the most important pragma.
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;

        // Foreign keys are off by default in SQLite. Turn them on.
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        // Synchronous = NORMAL is safe with WAL and much faster
        // than FULL. Data is safe after each WAL frame sync.
        conn.execute_batch("PRAGMA synchronous = NORMAL;")?;

        let db = Self { conn };
        schema::run_migrations(&db.conn)?;

        tracing::info!("database opened and migrations applied");
        Ok(db)
    }

    /// Open an in-memory database. Used in tests only.
    pub fn open_in_memory() -> DbResult<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("PRAGMA journal_mode = WAL;")?;
        conn.execute_batch("PRAGMA foreign_keys = ON;")?;
        let db = Self { conn };
        schema::run_migrations(&db.conn)?;
        Ok(db)
    }

    pub fn connection(&self) -> &rusqlite::Connection {
    &self.conn
}
}