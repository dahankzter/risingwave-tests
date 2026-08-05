//! Pipeline lifecycle: running setup SQL, and sealing a finished bulk feed.
//!
//! Sealing is a separate phase, not a row appended to the feed, because `flush` returns before the
//! materialized view has caught up. Measured on the rig with a 200k-row feed: 3917 matches
//! immediately after the final flush, 10624 five seconds later with nothing further inserted. A
//! far-future sentinel delivered inside that window froze the count at 3917 permanently — the
//! watermark discards the rows still in flight rather than matching them, and they never come
//! back. Interposing a flush before the sentinel does not help, because flush is precisely what
//! does not wait. So: settle, seal, settle.

use std::path::Path;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SealConfig {
    /// `table` and `mv` are interpolated into SQL as identifiers, not bound as parameters —
    /// PostgreSQL has no parameter binding for identifiers. They are operator-supplied on the
    /// operator's own cluster, so there is no privilege boundary being crossed here; treat them
    /// as trusted input and do not plumb untrusted values into them.
    pub table: String,
    pub mv: String,
    pub sentinel_partition: i32,
    /// Consecutive unchanged reads before the count is considered settled. This is a heuristic,
    /// not a guarantee — raise it on a slower box or a heavier feed.
    pub stable_polls: u32,
    pub poll: Duration,
    pub max_polls: u32,
}

impl Default for SealConfig {
    fn default() -> Self {
        Self {
            table: "t_perf".into(),
            mv: "mv_perf".into(),
            sentinel_partition: 0,
            stable_polls: 5,
            poll: Duration::from_secs(1),
            max_polls: 600,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SealOutcome {
    pub settled_before: i64,
    pub settled_after: i64,
}

pub async fn run_sql_file(
    client: &tokio_postgres::Client,
    path: &Path,
) -> anyhow::Result<()> {
    let sql = std::fs::read_to_string(path)?;
    // psql meta-commands (\echo) are not SQL; strip them so scenario files run unmodified.
    let cleaned: String = sql
        .lines()
        .filter(|l| !l.trim_start().starts_with('\\'))
        .collect::<Vec<_>>()
        .join("\n");
    client.batch_execute(&cleaned).await?;
    Ok(())
}

async fn count(client: &tokio_postgres::Client, mv: &str) -> anyhow::Result<i64> {
    let row = client.query_one(&format!("select count(*) from {mv}"), &[]).await?;
    Ok(row.get(0))
}

/// Pure stable-count state machine, extracted from `settle` so the settling rule — the guard
/// against the defect where a far-future sentinel delivered before the pipeline drained
/// permanently discarded ~63% of matches — is testable without a database.
///
/// There is no sentinel value for "no observation yet": the first `observe` call is not treated
/// as a change from some prior state, it simply becomes the baseline. A genuinely empty MV (count
/// 0 from the very first poll) settles the same way a nonzero one does — after `stable_polls`
/// consecutive observations equal to that baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settle {
    last: Option<i64>,
    stable: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Poll {
    Settled,
    Continue,
}

impl Settle {
    pub fn new() -> Self {
        Self { last: None, stable: 0 }
    }

    /// Feed one observation. The first call always establishes the baseline (`Continue`,
    /// never `Settled`) — there is nothing to compare it against yet, whether the count is 0 or
    /// not. After that, `stable_polls` consecutive calls with an unchanged `n` settle; any change
    /// resets the streak.
    pub fn observe(&mut self, n: i64, stable_polls: u32) -> Poll {
        if self.last == Some(n) {
            self.stable += 1;
        } else {
            self.last = Some(n);
            self.stable = 0;
        }
        if self.stable >= stable_polls {
            Poll::Settled
        } else {
            Poll::Continue
        }
    }
}

impl Default for Settle {
    fn default() -> Self {
        Self::new()
    }
}

async fn settle(
    client: &tokio_postgres::Client,
    cfg: &SealConfig,
    what: &str,
) -> anyhow::Result<i64> {
    let mut settle = Settle::new();
    let mut polls = 0u32;
    loop {
        let n = count(client, &cfg.mv).await?;
        if settle.observe(n, cfg.stable_polls) == Poll::Settled {
            return Ok(n);
        }
        polls += 1;
        if polls >= cfg.max_polls {
            anyhow::bail!(
                "seal: {what} still moving after {}s (at {n} matches)",
                cfg.max_polls as u64 * cfg.poll.as_secs()
            );
        }
        tokio::time::sleep(cfg.poll).await;
    }
}

pub async fn seal(
    client: &tokio_postgres::Client,
    cfg: &SealConfig,
) -> anyhow::Result<SealOutcome> {
    let settled_before = settle(client, cfg, "feed").await?;

    let max_ts: i32 = client
        .query_one(&format!("select coalesce(max(ts), 0) from {}", cfg.table), &[])
        .await?
        .get(0);

    client
        .batch_execute(&format!(
            "set rw_implicit_flush to true;
             insert into {} (id, ts, kind, amount) values ({}, {}, 'noop', 0);",
            cfg.table,
            cfg.sentinel_partition,
            max_ts as i64 + 1_000_000
        ))
        .await?;

    let settled_after = settle(client, cfg, "seal").await?;
    Ok(SealOutcome { settled_before, settled_after })
}

#[cfg(test)]
mod settle_tests {
    use super::*;

    /// `stable_polls` consecutive unchanged observations after the baseline is required — the
    /// baseline reading itself does not count as a stable observation.
    #[test]
    fn settles_only_after_consecutive_unchanged_observations() {
        let mut s = Settle::new();
        let stable_polls = 3;
        // Baseline: never settled on the first read, no matter what stable_polls is.
        assert_eq!(s.observe(42, stable_polls), Poll::Continue);
        // Two more unchanged reads: still short of the threshold.
        assert_eq!(s.observe(42, stable_polls), Poll::Continue);
        assert_eq!(s.observe(42, stable_polls), Poll::Continue);
        // Third unchanged read after the baseline: threshold reached.
        assert_eq!(s.observe(42, stable_polls), Poll::Settled);
    }

    /// A change at any point resets the streak, even after it had built up close to the
    /// threshold — this is exactly the guard against declaring "settled" while the pipeline is
    /// still catching up.
    #[test]
    fn a_change_resets_the_counter() {
        let mut s = Settle::new();
        let stable_polls = 3;
        assert_eq!(s.observe(10, stable_polls), Poll::Continue); // baseline
        assert_eq!(s.observe(10, stable_polls), Poll::Continue); // stable=1
        assert_eq!(s.observe(10, stable_polls), Poll::Continue); // stable=2
        // Count moves: more matches arrived. Must not settle even though we were one poll away.
        assert_eq!(s.observe(11, stable_polls), Poll::Continue); // new baseline, stable=0
        assert_eq!(s.observe(11, stable_polls), Poll::Continue); // stable=1
        assert_eq!(s.observe(11, stable_polls), Poll::Continue); // stable=2
        assert_eq!(s.observe(11, stable_polls), Poll::Settled); // stable=3
    }

    /// A genuinely empty materialized view — count 0 from the very first poll — must settle
    /// exactly like any other steady count: neither one poll early (0 is not a magic "already
    /// settled" value) nor one poll late (0 is not treated as "no observation yet").
    #[test]
    fn a_count_of_zero_from_the_start_settles_like_any_other_count() {
        let mut zero = Settle::new();
        let mut nonzero = Settle::new();
        let stable_polls = 4;
        let mut zero_results = Vec::new();
        let mut nonzero_results = Vec::new();
        for _ in 0..stable_polls {
            zero_results.push(zero.observe(0, stable_polls));
            nonzero_results.push(nonzero.observe(7, stable_polls));
        }
        // Same shape of results for 0 as for any other steady value.
        assert_eq!(zero_results, nonzero_results);
        // And it does settle, given enough polls.
        assert_eq!(zero.observe(0, stable_polls), Poll::Settled);
    }
}
