//! diff-testing — shared test infrastructure for reth-exdiff.
//!
//! Provides:
//!   - FakeBlock, FakeAccountChange, FakeReceipt, FakeAccountInfo descriptors
//!   - make_commit_notification, make_reorg_notification, make_revert_notification
//!   - hash_for(block_number, fork_id) deterministic test hashes
//!   - InvariantChecker::check_all(db) and per-property checks

pub mod types;
pub mod builder;
pub mod invariants;

pub use types::{FakeBlock, FakeAccountChange, FakeAccountInfo, FakeReceipt, hash_for};
pub use builder::{make_commit_notification, make_reorg_notification, make_revert_notification};
pub use invariants::InvariantChecker;