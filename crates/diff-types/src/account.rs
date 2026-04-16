use alloy_primitives::{Address, BlockNumber, B256, U256};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// What kind of change happened to this account in this block.
/// This is critical for correct unwind behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ChangeKind {
    /// Account did not exist before this block and was created by it.
    /// On unwind: delete the account entirely.
    Created,
    /// Account existed before and was modified (balance, nonce, code).
    /// On unwind: restore old values.
    Modified,
    /// Account was self-destructed in this block.
    /// On unwind: restore the account from old values.
    Destroyed,
    /// Account was touched but no observable state changed.
    /// Still recorded for completeness.
    Touched,
}

impl ChangeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created   => "created",
            Self::Modified  => "modified",
            Self::Destroyed => "destroyed",
            Self::Touched   => "touched",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "created"   => Some(Self::Created),
            "modified"  => Some(Self::Modified),
            "destroyed" => Some(Self::Destroyed),
            "touched"   => Some(Self::Touched),
            _           => None,
        }
    }
}

/// Records how one account changed during one specific block.
/// Keyed by (block_number, block_hash, address) in the database.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AccountDiff {
    /// The block this diff belongs to.
    pub block_number: BlockNumber,
    /// The block hash — needed because two branches can have the
    /// same block_number but different hashes during a reorg.
    pub block_hash: B256,
    /// The account that changed.
    pub address: Address,
    /// Balance before this block executed. None if account did not exist.
    pub old_balance: Option<U256>,
    /// Balance after this block executed. None if account was destroyed.
    pub new_balance: Option<U256>,
    /// Nonce before. None if account did not exist.
    pub old_nonce: Option<u64>,
    /// Nonce after. None if account was destroyed.
    pub new_nonce: Option<u64>,
    /// Code hash before. None if account had no code or did not exist.
    pub old_code_hash: Option<B256>,
    /// Code hash after. None if account has no code or was destroyed.
    pub new_code_hash: Option<B256>,
    /// What kind of change this was.
    pub change_kind: ChangeKind,
}