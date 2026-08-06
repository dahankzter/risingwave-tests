//! The alert stream reader: a subscription cursor over `t_rt_alerts`, fanned out to the
//! console's broadcast channel.
//!
//! `spawn_reader` must never return early. Every failure — a lost connection, a cursor left
//! dangling by `POST /api/pipeline/rebuild` dropping and recreating `t_rt_alerts`, a row that
//! doesn't decode — is caught, reported as an `Event::Log`, and followed by a retry. A dead
//! reader means a silently empty page, which is worse than a page that shows an error and
//! recovers.
//!
//! The retry/backoff decision ("did this failure move us back to square one?") is pulled out
//! into `Reader`, a tiny state machine with no I/O in it, so that logic is unit-testable without
//! a database. The I/O itself — connect, `create subscription if not exists`, `declare cursor`,
//! `fetch` — lives in free functions around it and is only exercised by hand against a live
//! cluster (see the task report).

use crate::event::Event;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_postgres::Row as PgRow;

const SUBSCRIPTION: &str = "sub_alerts";
const CURSOR: &str = "cur_alerts";
const FETCH_BATCH: i64 = 100;
const EMPTY_FETCH_SLEEP: Duration = Duration::from_millis(200);
/// Backoff after any failure — connect, setup, or fetch. Deliberately longer than the
/// empty-fetch sleep: a failure means something is actually wrong (network down, pipeline being
/// rebuilt), and hammering it every 200ms just spams the log without helping it recover sooner.
const RETRY_SLEEP: Duration = Duration::from_secs(2);

/// Where the reader is relative to having a working cursor. Kept as data, not folded into
/// control flow, specifically so `ready`/`fail`/`phase` are testable without ever opening a
/// connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// No connection, or the last one known to be broken. The next loop iteration must connect
    /// and (re)declare the subscription and cursor before it can fetch anything.
    Disconnected,
    /// Connected, subscription and cursor in place, safe to `fetch` from.
    Ready,
}

/// The reader's state machine, minus all I/O. `spawn_reader`'s loop drives this: it calls
/// `ready()` after a successful (re)connect and `fail()` on any error, and uses `phase()` to
/// decide whether the next iteration needs to (re)connect or can go straight to `fetch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reader {
    phase: Phase,
    /// Consecutive failures since the last successful `ready()`. Not currently used to change
    /// behaviour (the backoff is a flat `RETRY_SLEEP`), but surfaced so a future change — e.g.
    /// exponential backoff, or giving up loudly after N failures — has something to key off
    /// without re-plumbing the loop.
    consecutive_failures: u32,
    /// The last log line published, so an unchanged condition can stay quiet (see `fail`).
    last_report: Option<String>,
    /// Occurrences of the unchanged condition since it was last reported.
    since_report: u32,
}

impl Reader {
    pub fn new() -> Self {
        Self { phase: Phase::Disconnected, consecutive_failures: 0, last_report: None, since_report: 0 }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Call once the connection is open and the subscription + cursor are (re)declared.
    pub fn ready(&mut self) {
        self.phase = Phase::Ready;
        self.consecutive_failures = 0;
        // Forget what was last reported: if the same condition recurs after a healthy stretch,
        // that is news again.
        self.last_report = None;
        self.since_report = 0;
    }

    /// Call on any failure — connect, setup, or fetch. Moves back to `Disconnected` so the next
    /// loop iteration redoes setup from scratch, and returns the `Event::Log` to publish, or
    /// `None` when there is nothing worth saying.
    ///
    /// Two things this is careful about, both learned from watching the page rather than the code:
    ///
    /// * **A missing pipeline is not a failure.** Before anyone presses "rebuild pipeline" there
    ///   is no `t_rt_alerts` to subscribe to, so every attempt fails by construction. Reporting
    ///   that as a warning made a correct, expected state look like a broken engine — and, worse,
    ///   the repeated warning overwrote the "cluster: up" confirmation in the log strip, so the
    ///   one action the operator had just taken appeared to have done nothing. It is reported once,
    ///   as information, in the language of what to do next.
    /// * **Repetition is silence.** A condition that persists is logged on its first occurrence
    ///   and then every `REPEAT_EVERY` attempts, not on all of them.
    pub fn fail(&mut self, context: &str, err: &str) -> Option<Event> {
        /// Re-report an unchanged condition once per this many occurrences, so a persistent
        /// problem stays visible without repainting the strip every retry.
        const REPEAT_EVERY: u32 = 30;

        self.phase = Phase::Disconnected;
        self.consecutive_failures += 1;

        // The pipeline's objects are created by `rebuild pipeline`; until then their absence is
        // the expected state, and RisingWave says so with "not found" / "does not exist".
        //
        // Scoped to the setup phase deliberately: the same message during `fetch` means the
        // cursor died under a running reader — the pipeline-rebuild recovery path — which is a
        // real reconnect and stays a warning.
        let missing_object = err.contains("not found")
            || err.contains("does not exist")
            || err.contains("Catalog error");
        let (level, text) = if missing_object && context == "connect/setup" {
            (
                "info",
                "alert reader: waiting for the pipeline (press \"rebuild pipeline\")".to_string(),
            )
        } else {
            ("warn", format!("alert reader: {context} failed ({err}); reconnecting"))
        };

        // Deduplicate on the MESSAGE, not on the failure count: a condition that changed is news
        // and must be reported immediately, even mid-streak. (Counting instead would have hidden
        // the first "waiting for the pipeline" behind a preceding fetch failure.)
        let changed = self.last_report.as_deref() != Some(text.as_str());
        if changed {
            self.last_report = Some(text.clone());
            self.since_report = 0;
        } else {
            self.since_report += 1;
            if !self.since_report.is_multiple_of(REPEAT_EVERY) {
                return None;
            }
        }
        Some(Event::Log { level: level.to_string(), text })
    }
}

impl Default for Reader {
    fn default() -> Self {
        Self::new()
    }
}

/// `alert_ts - trigger_ingest_ts` in milliseconds. Pulled out because it is the one piece of
/// per-row arithmetic worth testing without a database.
pub fn latency_ms(trigger_ingest_ts: time::OffsetDateTime, alert_ts: time::OffsetDateTime) -> f64 {
    (alert_ts - trigger_ingest_ts).as_seconds_f64() * 1000.0
}

/// Decode one row off `fetch ... from cur_alerts` into an `Event::Alert`. The cursor's result
/// carries the table's columns (`partition_0, ts, chain_len, trigger_ingest_ts, alert_ts`) plus
/// `op` and `rw_timestamp` appended by the subscription; columns are read by name so the extra
/// trailing ones are simply ignored.
fn decode_alert(row: &PgRow) -> anyhow::Result<Event> {
    let partition: i32 = row.try_get("partition_0")?;
    let chain_len: i64 = row.try_get("chain_len")?;
    let trigger_ingest_ts: time::OffsetDateTime = row.try_get("trigger_ingest_ts")?;
    let alert_ts_raw: time::OffsetDateTime = row.try_get("alert_ts")?;
    let fmt = time::format_description::well_known::Rfc3339;
    let alert_ts = alert_ts_raw.format(&fmt)?;
    Ok(Event::Alert {
        partition,
        chain_len,
        latency_ms: latency_ms(trigger_ingest_ts, alert_ts_raw),
        alert_ts,
        ingest_ms: (trigger_ingest_ts.unix_timestamp_nanos() / 1_000_000) as f64,
    })
}

/// Open a dedicated connection. Subscription cursors are session-scoped, so this must not be a
/// connection shared with anything else — another caller resetting the session (or the pool
/// handing the connection back) would pull the cursor out from under this reader.
async fn connect(url: &str) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("bench-web: alert reader connection closed: {e}");
        }
    });
    Ok(client)
}

/// Idempotently (re)establish the subscription and cursor on an already-open connection.
/// `create subscription if not exists` tolerates the subscription already being there; the
/// `close` is best-effort (a fresh connection has no cursor to close, and that's fine — its
/// error is discarded) and exists only to cover redeclaring on a connection that survived a
/// subscription drop.
async fn prepare(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    client
        .batch_execute(&format!(
            "create subscription if not exists {SUBSCRIPTION} from t_rt_alerts \
             with (retention = '1 day')"
        ))
        .await?;
    let _ = client.batch_execute(&format!("close {CURSOR}")).await;
    client
        .batch_execute(&format!("declare {CURSOR} subscription cursor for {SUBSCRIPTION}"))
        .await?;
    Ok(())
}

async fn fetch_batch(client: &tokio_postgres::Client) -> Result<Vec<PgRow>, tokio_postgres::Error> {
    client.query(&format!("fetch {FETCH_BATCH} from {CURSOR}"), &[]).await
}

/// Run one full setup: connect, then declare subscription + cursor. Kept separate from the loop
/// so the loop's error handling is uniform — connect failures and setup failures both just mean
/// "not ready yet, log it, back off, try again".
async fn connect_and_prepare(url: &str) -> anyhow::Result<tokio_postgres::Client> {
    let client = connect(url).await?;
    prepare(&client).await?;
    Ok(client)
}

/// Every cause in an error chain, joined. `tokio_postgres::Error`'s own `Display` is the string
/// `"db error"` — the server's actual complaint ("table or source \"t_rt_alerts\" does not
/// exist") lives one level down in its source. Printing only the outermost error therefore tells
/// the operator nothing AND hides the text the waiting/failure classification keys on, which is
/// exactly how an expected "no pipeline yet" state ended up on screen as an opaque warning.
pub fn full_chain(err: &anyhow::Error) -> String {
    chain_of(err.as_ref())
}

/// The same flattening for any `std::error::Error` — `fetch_batch` returns a bare
/// `tokio_postgres::Error`, which is the very type whose `Display` hides everything.
pub fn chain_of(err: &(dyn std::error::Error + 'static)) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        // Skip a cause that adds nothing (some wrappers repeat their source verbatim).
        if !parts.iter().any(|p| p == &text) {
            parts.push(text);
        }
        source = cause.source();
    }
    parts.join(": ")
}

async fn reader_loop(url: String, tx: broadcast::Sender<Event>) {
    let mut reader = Reader::new();
    let mut client: Option<tokio_postgres::Client> = None;

    loop {
        if reader.phase() == Phase::Disconnected {
            match connect_and_prepare(&url).await {
                Ok(c) => {
                    client = Some(c);
                    reader.ready();
                    let _ = tx.send(Event::Log {
                        level: "info".to_string(),
                        text: "alert reader: subscription and cursor ready".to_string(),
                    });
                }
                Err(e) => {
                    client = None;
                    // A send error here just means no receiver is listening yet; the reader
                    // keeps going regardless — it must never die on account of the channel.
                    if let Some(ev) = reader.fail("connect/setup", &full_chain(&e)) {
                        let _ = tx.send(ev);
                    }
                    tokio::time::sleep(RETRY_SLEEP).await;
                    continue;
                }
            }
        }

        // Reachable only with `client` populated: the branch above either sets it or `continue`s.
        let Some(c) = client.as_ref() else {
            // Structurally unreachable — the branch above either populates `client` or
            // `continue`s — but handled rather than `unwrap`ed/`expect`ed so a future refactor
            // that breaks the invariant degrades to "log and retry" instead of a panic that
            // would take the whole reader down.
            if let Some(ev) = reader.fail("invariant", "ready phase reached with no open connection")
            {
                let _ = tx.send(ev);
            }
            tokio::time::sleep(RETRY_SLEEP).await;
            continue;
        };

        match fetch_batch(c).await {
            Ok(rows) if rows.is_empty() => {
                tokio::time::sleep(EMPTY_FETCH_SLEEP).await;
            }
            Ok(rows) => {
                for row in &rows {
                    match decode_alert(row) {
                        Ok(ev) => {
                            let _ = tx.send(ev);
                        }
                        Err(e) => {
                            // A single malformed row must not take the reader down; log and
                            // move on to the next row in the batch.
                            let _ = tx.send(Event::Log {
                                level: "warn".to_string(),
                                text: format!("alert reader: could not decode a row: {e}"),
                            });
                        }
                    }
                }
            }
            Err(e) => {
                client = None;
                if let Some(ev) = reader.fail("fetch", &chain_of(&e)) {
                    let _ = tx.send(ev);
                }
                tokio::time::sleep(RETRY_SLEEP).await;
            }
        }
    }
}

/// Spawn the reader as a background task. Returns immediately; the task itself never returns —
/// on any error it logs and retries rather than exiting, per the module doc comment.
pub fn spawn_reader(url: String, tx: broadcast::Sender<Event>) -> JoinHandle<()> {
    tokio::spawn(reader_loop(url, tx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_disconnected() {
        let r = Reader::new();
        assert_eq!(r.phase(), Phase::Disconnected);
        assert_eq!(r.consecutive_failures(), 0);
    }

    #[test]
    fn ready_moves_to_ready_and_clears_the_failure_count() {
        let mut r = Reader::new();
        let _ = r.fail("connect/setup", "boom");
        let _ = r.fail("connect/setup", "boom again");
        assert_eq!(r.consecutive_failures(), 2);
        r.ready();
        assert_eq!(r.phase(), Phase::Ready);
        assert_eq!(r.consecutive_failures(), 0);
    }

    #[test]
    fn fail_from_ready_moves_back_to_disconnected() {
        let mut r = Reader::new();
        r.ready();
        assert_eq!(r.phase(), Phase::Ready);
        let ev = r
            .fail("fetch", "cursor \"cur_alerts\" does not exist")
            .expect("the first failure of a condition is always reported");
        assert_eq!(r.phase(), Phase::Disconnected);
        match ev {
            Event::Log { level, text } => {
                assert_eq!(level, "warn");
                assert!(text.contains("fetch"));
                assert!(text.contains("cur_alerts"));
            }
            other => panic!("expected Event::Log, got {other:?}"),
        }
    }

    #[test]
    fn repeated_failures_without_an_intervening_ready_accumulate() {
        let mut r = Reader::new();
        let _ = r.fail("connect/setup", "e1");
        let _ = r.fail("connect/setup", "e2");
        let _ = r.fail("connect/setup", "e3");
        assert_eq!(r.consecutive_failures(), 3);
        assert_eq!(r.phase(), Phase::Disconnected);
    }

    #[test]
    fn this_is_exactly_the_pipeline_rebuild_scenario() {
        // Models the sequence the report's live recovery test drives by hand: reader comes up,
        // serves alerts fine, then `make rt-setup` drops and recreates `t_rt_alerts` underneath
        // it. The next `fetch` errors because the cursor's subscription is gone. The state
        // machine must notice and fall back to `Disconnected` so the loop redoes setup, and it
        // must not lose track of that even across several failed retries before the rebuild
        // finishes.
        let mut r = Reader::new();
        r.ready();
        assert_eq!(r.phase(), Phase::Ready);

        // A fetch that fails because the rebuild dropped the objects underneath a READY reader is
        // a genuine reconnect: warned about, not silently swallowed as "waiting".
        let ev1 = r.fail("fetch", "table or source \"t_rt_alerts\" does not exist");
        assert_eq!(r.phase(), Phase::Disconnected);
        assert!(matches!(ev1, Some(Event::Log { level, .. }) if level == "warn"));

        // Rebuild still in flight: a couple of connect/setup attempts land before the DDL
        // finishes. These are the expected-absence case, so they report as information (and only
        // the first of the streak reports at all).
        let ev2 = r.fail("connect/setup", "subscription \"sub_alerts\" does not exist");
        assert!(
            matches!(&ev2, Some(Event::Log { level, text }) if level == "info" && text.contains("rebuild pipeline")),
            "expected a waiting notice, got {ev2:?}"
        );
        assert!(
            r.fail("connect/setup", "subscription \"sub_alerts\" does not exist").is_none(),
            "a repeat of the same condition must stay quiet"
        );
        assert_eq!(r.phase(), Phase::Disconnected);
        assert_eq!(r.consecutive_failures(), 3);

        // Rebuild finished: setup succeeds, failure count resets, ready to fetch again.
        r.ready();
        assert_eq!(r.phase(), Phase::Ready);
        assert_eq!(r.consecutive_failures(), 0);
    }

    #[test]
    fn latency_ms_is_the_millisecond_gap_between_the_two_timestamps() {
        let trigger = time::OffsetDateTime::UNIX_EPOCH;
        let alert = trigger + time::Duration::milliseconds(6318);
        assert!((latency_ms(trigger, alert) - 6318.0).abs() < 1e-6);
    }

    #[test]
    fn latency_ms_is_zero_for_equal_timestamps() {
        let t = time::OffsetDateTime::UNIX_EPOCH;
        assert_eq!(latency_ms(t, t), 0.0);
    }
}
