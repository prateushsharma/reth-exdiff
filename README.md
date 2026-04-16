# reth-exdiff

**Reorg-safe canonical state-diff and receipt-proof pipeline for Ethereum.**  
Built on Reth. Handles forkchoice-driven canonicality, adversarial reorgs, and crash recovery.  
Not an indexer. A derived-state system.

---

```
                  Consensus Client
                  (forkchoice driver)
                        │
                    Engine API
                        │
                        ▼
    ┌─────────────────────────────────────────────────────┐
    │                    Reth Node                        │
    │                                                     │
    │  Block Execution ──► CanonStateNotification ──► ExEx│
    │        │                                       │    │
    │        │                                       ▼    │
    │        │                              Derived Diff DB
    │        │                                       │    │
    │        ▼                                       ▼    │
    │   Staged Pipeline ◄── DiffCompactionStage ◄───┘    │
    └─────────────────────────────────────────────────────┘
                                  │
                                  ▼
                    reth-proof-rpc (HTTP JSON)
                    ├─ POST /receipt_proof
                    ├─ POST /account_diff
                    └─ POST /storage_diff
```

---

## What this is

Normal indexers say: *"I saw block N, so I stored some data."*

This system says:

> *"I only expose data for the current canonical chain.  
> I unwind when that chain changes.  
> I replay onto the new canonical branch.  
> I prove my output is tied to a specific canonical height.  
> I survive crashes between any two writes."*

That distinction is the entire point.

Post-merge Ethereum has a hard separation between execution and canonicality. The execution client validates and executes blocks. The consensus client decides which executed blocks are actually part of the chain — through the Engine API. Most indexers ignore this boundary. They store data keyed by block number and hope for the best. When a reorg happens they either corrupt their state, miss it entirely, or require a full re-sync.

This system treats reorgs as a **first-class execution mode**, not an exception path. The data model, the cursor design, the write ordering, the checkpoint discipline — every decision was made with the assumption that a reorg will happen, at any depth, at any time, including mid-write.

---

## Architecture

### Two tightly coupled subsystems

**`diff-exex` — streaming derivation**

Runs inside Reth as an Execution Extension. Receives `ExExNotification` events directly from the execution engine. For every canonical chain change it extracts per-block account diffs, storage diffs, receipt artifacts, and writes revert ops before writing anything else. Emits `FinishedHeight` so Reth's pruner stays safe.

Three notification variants handled:

| Variant | Meaning | Action |
|---|---|---|
| `ChainCommitted` | canonical chain extended | extract diffs, write revert ops, advance cursor |
| `ChainReorged` | old branch replaced by new | revert old tip-first, commit new, signal compaction |
| `ChainReverted` | blocks removed, no replacement yet | revert old tip-first, roll back cursor |

**`diff-stage` — compaction and indexing**

A proper `Stage<Provider>` implementation with `execute()` and `unwind()`. Reads raw ExEx output. Builds secondary indexes (`address_block_index`, `slot_block_index`). Generates proof material. Checkpoints progress. Never runs ahead of the ExEx's durable cursor.

The stage is driven by a background task that polls every 2 seconds, respects the `exex_finished_height` ceiling, and receives reorg signals via a `tokio::watch` channel. When a reorg signal arrives it calls `unwind()` before resuming forward execution — the same unwind path that the adversarial test harness stress-tests.

---

## Data model

Six SQLite tables. WAL mode. Foreign keys enforced.

```sql
canonical_blocks     -- block_number, block_hash, canonical_status (active|reorged)
account_diffs        -- per-block account state changes, JOIN canonical_blocks for visibility
storage_diffs        -- per-block storage slot changes
receipt_artifacts    -- RLP-encoded receipts + receipts_root_anchor
revert_ops           -- undo log; written BEFORE diffs so crash recovery is always possible
stage_checkpoints    -- four cursors: streaming, durable, compacted_until, proof_indexed_until
```

Two stage-owned index tables built during compaction:

```sql
address_block_index  -- (address, block_number, block_hash) → O(log n) address lookups
slot_block_index     -- (address, slot, block_number, block_hash) → O(log n) slot lookups
```

**Key invariant:** `block_hash` is part of every primary key. Two competing forks at the same height get separate rows. Reorgs flip `canonical_status` from `active` to `reorged` — they never delete data. Every query through the canonical-join path filters `canonical_status = 'active'`, making reorged data structurally invisible to consumers without destroying the audit trail.

**Write ordering:** Revert ops are written before diffs in a single transaction. If the process crashes after writing revert ops but before writing diffs, the next startup finds a consistent state with nothing to undo. The inverse — crashing after diffs but before revert ops — cannot happen because the write order prohibits it.

---

## Checkpoint discipline

Four cursors. Each owned by a different subsystem. Together they express the full pipeline state.

```
streaming        ← ExEx in-memory position (resets on restart)
durable          ← ExEx durably written to DB (resume point)
compacted_until  ← Stage has indexed up to here
proof_indexed_until ← Proof cache built up to here

Invariant: proof_indexed_until ≤ compacted_until ≤ durable ≤ streaming
```

The stage never compacts beyond `durable`. The proof extractor never generates proofs for blocks above `compacted_until`. On restart the ExEx loads `durable` as its start position. Nothing trusts in-memory progress.

---

## Receipt proof export

Given a `tx_hash`, the system returns a complete Merkle-Patricia Trie inclusion proof:

```json
{
  "proof": {
    "block_hash":      "0x...",
    "block_number":    18431234,
    "receipts_root":   "0x...",
    "receipt_rlp":     "0x...",
    "proof_nodes":     ["0x...", "0x...", "0x..."],
    "tx_index":        4,
    "canonical_anchor": {
      "local_head_number":  18432100,
      "local_head_hash":    "0x...",
      "block_is_canonical": true,
      "finalized_hint":     18431000
    }
  }
}
```

The trie is built using `alloy-trie`'s `HashBuilder` with `ProofRetainer`. Key encoding is `RLP(tx_index)` → nibble-unpacked, leaves fed in ascending order. The computed root is validated against the stored `receipts_root_anchor` before the proof is returned — a mismatch is treated as a fatal data integrity error, not a soft failure.

The `canonical_anchor` is a local attestation: at proof generation time, this node considered the block canonical. If `finalized_hint` covers the proven block it is finalized and cannot be reorged. Not a consensus-layer proof. An honest local claim.

---

## Adversarial test harness

`forkchoice-fuzzer` runs 10 deterministic scenarios. Every scenario uses an in-memory SQLite database, drives a real `DiffExEx` via an `mpsc` channel, and runs `InvariantChecker::check_all` after every operation.

| Scenario | What it stress-tests |
|---|---|
| `01_linear_forward_extension` | baseline happy path |
| `02_depth1_reorg` | single block replacement |
| `03_depth3_reorg` | multi-block revert ordering (tip-first) |
| `04_reorg_after_partial_compaction` | stage unwind + re-compact on new branch |
| `05_duplicate_commit_idempotence` | `INSERT OR IGNORE` guards hold |
| `06_restart_after_exex_commit` | stage resumes from checkpoint, no double-index |
| `07_crash_and_replay` | durable cursor non-regression after ExEx restart |
| `08_depth10_reorg` | revert-op chain stress at depth 10 |
| `09_branch_pollution_attempt` | phantom revert of never-committed blocks |
| `10_stale_head_backward_movement` | chain shortening without replacement |

Three universal invariants checked after every scenario:

1. **No orphan diffs** — every diff row has a canonical_blocks parent. No data written without a registered block.
2. **Checkpoint consistency** — four cursors satisfy `proof ≤ compacted ≤ durable ≤ streaming`.
3. **Root reproducibility** — reconstructing the receipts MPT from stored bytes produces the same root as the stored anchor.

---

## Crate structure

```
reth-exdiff/
├── crates/
│   ├── diff-types/      shared primitive types — zero Reth deps
│   ├── diff-db/         SQLite persistence, six tables, WAL, migrations
│   ├── diff-exex/       ExEx loop, extractor, revert engine, cursor
│   ├── diff-stage/      DiffCompactionStage: execute() + unwind()
│   ├── diff-proof/      MPT trie builder, ProofExtractor, CanonicalAnchor
│   └── diff-testing/    FakeBlock builders, notification factory, InvariantChecker
└── bins/
    ├── reth-diff-exex/  ExEx-only node (no compaction)
    ├── reth-diff-stage/ ExEx + compaction stage in one node process
    ├── reth-proof-rpc/  HTTP JSON query server (read-only)
    └── forkchoice-fuzzer/ adversarial correctness harness
```

`diff-types` has no Reth dependency by design. Every other crate can depend on it without pulling in the full Reth compilation tree. `diff-testing` is the only crate that depends on all others — it is pure test infrastructure and never appears in production deps.

---

## Running it

**Node with ExEx and compaction:**
```bash
cargo build --release -p reth-diff-stage

./target/release/reth-diff-stage node \
  --chain mainnet \
  --diff-db-path /data/diff.db \
  --compaction-interval-ms 2000
```

**Query server (separate process, same DB file):**
```bash
cargo build --release -p reth-proof-rpc

./target/release/reth-proof-rpc \
  --diff-db-path /data/diff.db \
  --listen 0.0.0.0:8080
```

**Adversarial harness (no node needed):**
```bash
cargo build --release -p forkchoice-fuzzer
./target/release/forkchoice-fuzzer
```

---

## Tech stack

| | |
|---|---|
| Node framework | Reth `0a9af79` |
| Alloy | `alloy-primitives 1.5.6`, `alloy-consensus 2.0.0` |
| EVM | `revm 37.0.0`, `revm-state 11.0.0` |
| Storage | `rusqlite 0.31` (bundled), SQLite WAL |
| MPT | `alloy-trie 0.7` |
| HTTP | `axum 0.7`, `tower-http 0.5` |
| Async | `tokio 1` (full) |
| Errors | `eyre 0.6`, `color-eyre 0.6` |
| Logging | `tracing 0.1`, `tracing-subscriber 0.3` |

---

## What this is not

Not a full archive node. Not a state trie implementation. Not a consensus client. Not a general-purpose indexer framework.

It is one thing: a **correctness-first derived-state system** that knows which branch it is on, can prove it, and can undo it.

---

*Built with Reth's ExEx and Stage primitives.*  
*Every design decision documented in source.*

Made with 🩷 by Prateush Sharma 
