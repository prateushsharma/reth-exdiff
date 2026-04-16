fn main() {}
//! forkchoice-fuzzer — adversarial forkchoice scenario harness.
//!
//! Runs 10 deterministic scenarios covering:
//!   - linear forward extension (baseline)
//!   - depth-1, depth-3, depth-10 reorgs
//!   - reorg after partial compaction
//!   - duplicate notification idempotence
//!   - crash-and-restart recovery
//!   - branch pollution attempt
//!   - stale head / backward head movement
//!
//! Each scenario drives a real DiffExEx + DiffDb (in-memory SQLite)
//! through a tokio channel and validates all invariants afterwards.
//!
//! Exit code: 0 if all pass, 1 if any fail.

mod scenarios;

use std::future::Future;
use tracing::{error, info};

// ---------------------------------------------------------------------------
// Result type
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ScenarioResult {
    name:   &'static str,
    passed: bool,
    error:  Option<String>,
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

struct ScenarioRunner {
    results: Vec<ScenarioResult>,
}

impl ScenarioRunner {
    fn new() -> Self {
        Self { results: Vec::new() }
    }

    /// Run one scenario, record its result.
    ///
    /// The scenario is an async fn that returns eyre::Result<()>.
    /// Panics inside the scenario are caught and recorded as failures.
    async fn run<F, Fut>(&mut self, name: &'static str, scenario: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = eyre::Result<()>>,
    {
        info!("--- running scenario: {} ---", name);

        let result = scenario().await;

        match result {
            Ok(()) => {
                info!("PASS: {}", name);
                self.results.push(ScenarioResult {
                    name,
                    passed: true,
                    error: None,
                });
            }
            Err(e) => {
                error!("FAIL: {} — {:?}", name, e);
                self.results.push(ScenarioResult {
                    name,
                    passed: false,
                    error: Some(format!("{:?}", e)),
                });
            }
        }
    }

    /// Print a structured summary table and return whether all passed.
    fn print_summary(&self) -> bool {
        let total  = self.results.len();
        let passed = self.results.iter().filter(|r| r.passed).count();
        let failed = total - passed;

        println!("\n{}", "=".repeat(60));
        println!("  FORKCHOICE FUZZER SUMMARY");
        println!("{}", "=".repeat(60));
        println!("  Total:  {}", total);
        println!("  Passed: {}", passed);
        println!("  Failed: {}", failed);
        println!("{}", "-".repeat(60));

        for r in &self.results {
            let status = if r.passed { "PASS" } else { "FAIL" };
            println!("  [{}]  {}", status, r.name);
            if let Some(ref err) = r.error {
                // Indent the error message for readability
                for line in err.lines().take(5) {
                    println!("         {}", line);
                }
            }
        }

        println!("{}", "=".repeat(60));

        failed == 0
    }
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
                .add_directive("forkchoice_fuzzer=info".parse()?)
                .add_directive("diff_exex=info".parse()?)
                .add_directive("diff_db=info".parse()?)
                .add_directive("diff_stage=info".parse()?),
        )
        .init();

    info!("forkchoice-fuzzer starting");

    let mut runner = ScenarioRunner::new();

    // Run all scenarios in order.
    // Each is async and isolated (fresh in-memory DB per scenario).

    runner.run("01_linear_forward_extension",
        scenarios::linear_forward_extension).await;

    runner.run("02_depth1_reorg",
        scenarios::depth1_reorg).await;

    runner.run("03_depth3_reorg",
        scenarios::depth3_reorg).await;

    runner.run("04_reorg_after_partial_compaction",
        scenarios::reorg_after_partial_compaction).await;

    runner.run("05_duplicate_commit_idempotence",
        scenarios::duplicate_commit_idempotence).await;

    runner.run("06_restart_after_exex_commit",
        scenarios::restart_after_exex_commit).await;

    runner.run("07_crash_and_replay",
        scenarios::crash_and_replay).await;

    runner.run("08_depth10_reorg",
        scenarios::depth10_reorg).await;

    runner.run("09_branch_pollution_attempt",
        scenarios::branch_pollution_attempt).await;

    runner.run("10_stale_head_backward_movement",
        scenarios::stale_head_backward_movement).await;

    let all_passed = runner.print_summary();

    if all_passed {
        info!("all scenarios passed");
        Ok(())
    } else {
        eyre::bail!("one or more scenarios failed — see summary above")
    }
}