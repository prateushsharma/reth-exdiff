//! Plain descriptor types for constructing fake ExEx notifications in tests.
//!
//! These have no Reth dependencies — they are just data. The builder in
//! builder.rs converts them into real Reth types.

use alloy_primitives::{Address, B256, U256};

// ---------------------------------------------------------------------------
// Account state descriptor
// ---------------------------------------------------------------------------

/// The state of one account at a point in time.
#[derive(Debug, Clone)]
pub struct FakeAccountInfo {
    pub balance: U256,
    pub nonce:   u64,
    /// keccak256 of the bytecode, or B256::ZERO for EOA
    pub code_hash: B256,
}

impl FakeAccountInfo {
    /// Construct a simple EOA with given balance and nonce.
    pub fn eoa(balance: u128, nonce: u64) -> Self {
        Self {
            balance:   U256::from(balance),
            nonce,
            code_hash: B256::ZERO,
        }
    }

    /// Construct a contract account (non-zero code hash).
    pub fn contract(balance: u128, nonce: u64, code_hash: B256) -> Self {
        Self {
            balance: U256::from(balance),
            nonce,
            code_hash,
        }
    }
}

// ---------------------------------------------------------------------------
// Account change descriptor
// ---------------------------------------------------------------------------

/// One account's state change within a single block.
///
/// `old = None` means the account was created in this block.
/// `new = None` means the account was destroyed in this block.
#[derive(Debug, Clone)]
pub struct FakeAccountChange {
    pub address: Address,
    pub old:     Option<FakeAccountInfo>,
    pub new:     Option<FakeAccountInfo>,
    /// Storage slot changes: (slot, old_value, new_value)
    pub storage: Vec<(U256, U256, U256)>,
}

impl FakeAccountChange {
    /// Simple balance transfer: account existed before and after.
    pub fn transfer(address: Address, old_balance: u128, new_balance: u128) -> Self {
        Self {
            address,
            old: Some(FakeAccountInfo::eoa(old_balance, 0)),
            new: Some(FakeAccountInfo::eoa(new_balance, 0)),
            storage: vec![],
        }
    }

    /// Account created in this block (was not present before).
    pub fn created(address: Address, balance: u128) -> Self {
        Self {
            address,
            old: None,
            new: Some(FakeAccountInfo::eoa(balance, 0)),
            storage: vec![],
        }
    }

    /// Account destroyed in this block (selfdestruct or similar).
    pub fn destroyed(address: Address, old_balance: u128) -> Self {
        Self {
            address,
            old: Some(FakeAccountInfo::eoa(old_balance, 0)),
            new: None,
            storage: vec![],
        }
    }

    /// Storage write alongside an account touch.
    pub fn with_storage(mut self, slot: U256, old_val: U256, new_val: U256) -> Self {
        self.storage.push((slot, old_val, new_val));
        self
    }
}

// ---------------------------------------------------------------------------
// Receipt descriptor
// ---------------------------------------------------------------------------

/// Minimal receipt data for one transaction in a block.
#[derive(Debug, Clone)]
pub struct FakeReceipt {
    /// tx_hash is used as the lookup key in receipt_artifacts
    pub tx_hash:           B256,
    /// true = success, false = revert
    pub status:            bool,
    pub cumulative_gas_used: u64,
}

impl FakeReceipt {
    pub fn success(tx_hash: B256, cumulative_gas: u64) -> Self {
        Self { tx_hash, status: true, cumulative_gas_used: cumulative_gas }
    }

    pub fn failure(tx_hash: B256, cumulative_gas: u64) -> Self {
        Self { tx_hash, status: false, cumulative_gas_used: cumulative_gas }
    }
}

// ---------------------------------------------------------------------------
// Block descriptor
// ---------------------------------------------------------------------------

/// Everything needed to describe one fake block for test notification
/// construction.
#[derive(Debug, Clone)]
pub struct FakeBlock {
    pub number:      u64,
    /// Block hash. Use hash_for(number) helper to get a deterministic value.
    pub hash:        B256,
    pub parent_hash: B256,
    pub changes:     Vec<FakeAccountChange>,
    pub receipts:    Vec<FakeReceipt>,
}

impl FakeBlock {
    pub fn new(number: u64, changes: Vec<FakeAccountChange>, receipts: Vec<FakeReceipt>) -> Self {
        Self {
            number,
            hash:        hash_for(number, 0),
            parent_hash: hash_for(number.saturating_sub(1), 0),
            changes,
            receipts,
        }
    }

    /// Construct a block on an alternate fork (same number, different hash).
    ///
    /// `fork_id` differentiates forks at the same height:
    ///   0 = canonical branch, 1 = first reorg branch, 2 = second, etc.
    pub fn on_fork(
        number:   u64,
        fork_id:  u8,
        changes:  Vec<FakeAccountChange>,
        receipts: Vec<FakeReceipt>,
    ) -> Self {
        Self {
            number,
            hash:        hash_for(number, fork_id),
            parent_hash: hash_for(number.saturating_sub(1), fork_id),
            changes,
            receipts,
        }
    }
}

// ---------------------------------------------------------------------------
// Deterministic hash helper
// ---------------------------------------------------------------------------

/// Generate a deterministic B256 for (block_number, fork_id).
///
/// Different fork_ids produce different hashes at the same block number,
/// simulating two competing branches. The encoding is:
///   bytes [0..8]  = block_number as big-endian u64
///   byte  [8]     = fork_id
///   bytes [9..32] = zeros
///
/// This is not a real hash — it is a unique identifier for testing.
pub fn hash_for(block_number: u64, fork_id: u8) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[0..8].copy_from_slice(&block_number.to_be_bytes());
    bytes[8] = fork_id;
    B256::from(bytes)
}