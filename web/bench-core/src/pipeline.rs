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

async fn settle(
    client: &tokio_postgres::Client,
    cfg: &SealConfig,
    what: &str,
) -> anyhow::Result<i64> {
    let mut last = -1i64;
    let mut stable = 0u32;
    let mut polls = 0u32;
    loop {
        let n = count(client, &cfg.mv).await?;
        if n == last {
            stable += 1;
            if stable >= cfg.stable_polls {
                return Ok(n);
            }
        } else {
            stable = 0;
        }
        last = n;
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
