use alloy_primitives::{BlockNumber, B256};
use diff_types::{AccountDiff, ChangeKind};
use crate::{DbError, DbResult, DiffDb};

impl DiffDb {
    /// Insert one account diff row.
    pub fn insert_account_diff(&self, diff: &AccountDiff) -> DbResult<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO account_diffs
             (block_number, block_hash, address, old_balance, new_balance,
              old_nonce, new_nonce, old_code_hash, new_code_hash, change_kind)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            rusqlite::params![
                diff.block_number,
                format!("{:?}", diff.block_hash),
                format!("{:?}", diff.address),
                diff.old_balance.map(|b| b.to_string()),
                diff.new_balance.map(|b| b.to_string()),
                diff.old_nonce.map(|n| n as i64),
                diff.new_nonce.map(|n| n as i64),
                diff.old_code_hash.map(|h| format!("{:?}", h)),
                diff.new_code_hash.map(|h| format!("{:?}", h)),
                diff.change_kind.as_str(),
            ],
        )?;
        Ok(())
    }

    /// Delete one account diff. Used during reorg revert.
    pub fn delete_account_diff(
        &self,
        block_hash: &B256,
        address: &alloy_primitives::Address,
    ) -> DbResult<()> {
        self.conn.execute(
            "DELETE FROM account_diffs
             WHERE block_hash = ?1 AND address = ?2",
            rusqlite::params![
                format!("{:?}", block_hash),
                format!("{:?}", address),
            ],
        )?;
        Ok(())
    }

    /// Get all account diffs for a specific block hash.
    pub fn get_account_diffs_for_block(
        &self,
        block_hash: &B256,
    ) -> DbResult<Vec<AccountDiff>> {
        let mut stmt = self.conn.prepare(
            "SELECT block_number, block_hash, address, old_balance, new_balance,
                    old_nonce, new_nonce, old_code_hash, new_code_hash, change_kind
             FROM account_diffs
             WHERE block_hash = ?1
             ORDER BY address ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![format!("{:?}", block_hash)],
            row_to_account_diff,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }

    /// Get all account diffs for a specific address across a block range.
    /// Only returns diffs from active canonical blocks.
    pub fn get_account_diffs_for_address(
        &self,
        address: &alloy_primitives::Address,
        from_block: BlockNumber,
        to_block: BlockNumber,
    ) -> DbResult<Vec<AccountDiff>> {
        let mut stmt = self.conn.prepare(
            "SELECT a.block_number, a.block_hash, a.address,
                    a.old_balance, a.new_balance,
                    a.old_nonce, a.new_nonce,
                    a.old_code_hash, a.new_code_hash, a.change_kind
             FROM account_diffs a
             JOIN canonical_blocks c ON a.block_hash = c.block_hash
             WHERE a.address = ?1
               AND a.block_number >= ?2
               AND a.block_number <= ?3
               AND c.canonical_status = 'active'
             ORDER BY a.block_number ASC",
        )?;

        let rows = stmt.query_map(
            rusqlite::params![
                format!("{:?}", address),
                from_block,
                to_block,
            ],
            row_to_account_diff,
        )?;

        rows.collect::<Result<Vec<_>, _>>().map_err(DbError::Sqlite)
    }
}

fn row_to_account_diff(
    row: &rusqlite::Row<'_>,
) -> Result<AccountDiff, rusqlite::Error> {
    use alloy_primitives::{Address, U256, B256};
    use std::str::FromStr;

    let block_hash_str: String    = row.get(1)?;
    let address_str: String       = row.get(2)?;
    let old_balance_str: Option<String> = row.get(3)?;
    let new_balance_str: Option<String> = row.get(4)?;
    let old_nonce: Option<i64>    = row.get(5)?;
    let new_nonce: Option<i64>    = row.get(6)?;
    let old_code_str: Option<String> = row.get(7)?;
    let new_code_str: Option<String> = row.get(8)?;
    let kind_str: String          = row.get(9)?;

    Ok(AccountDiff {
        block_number:  row.get(0)?,
        block_hash:    block_hash_str.parse::<B256>().unwrap_or_default(),
        address:       address_str.parse::<Address>().unwrap_or_default(),
        old_balance:   old_balance_str.and_then(|s| U256::from_str(&s).ok()),
        new_balance:   new_balance_str.and_then(|s| U256::from_str(&s).ok()),
        old_nonce:     old_nonce.map(|n| n as u64),
        new_nonce:     new_nonce.map(|n| n as u64),
        old_code_hash: old_code_str.and_then(|s| s.parse::<B256>().ok()),
        new_code_hash: new_code_str.and_then(|s| s.parse::<B256>().ok()),
        change_kind:   ChangeKind::from_str(&kind_str).unwrap_or(ChangeKind::Touched),
    })
}