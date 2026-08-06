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
}

impl Reader {
    pub fn new() -> Self {
        Self { phase: Phase::Disconnected, consecutive_failures: 0 }
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
    }

    /// Call on any failure — connect, setup, or fetch. Moves back to `Disconnected` so the next
    /// loop iteration redoes setup from scratch, and returns the `Event::Log` to publish.
    /// `context` is a short phase name ("connect", "setup", "fetch") for the log line.
    pub fn fail(&mut self, context: &str, err: &str) -> Event {
        self.phase = Phase::Disconnected;
        self.consecutive_failures += 1;
        Event::Log {
            level: "warn".to_string(),
            text: format!("alert reader: {context} failed ({err}); reconnecting"),
        }
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
                    let ev = reader.fail("connect/setup", &e.to_string());
                    // A send error here just means no receiver is listening yet; the reader
                    // keeps going regardless — it must never die on account of the channel.
                    let _ = tx.send(ev);
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
            let ev = reader.fail("invariant", "ready phase reached with no open connection");
            let _ = tx.send(ev);
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
                let ev = reader.fail("fetch", &e.to_string());
                let _ = tx.send(ev);
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
        r.fail("connect/setup", "boom");
        r.fail("connect/setup", "boom again");
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
        let ev = r.fail("fetch", "cursor \"cur_alerts\" does not exist");
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
        r.fail("connect/setup", "e1");
        r.fail("connect/setup", "e2");
        r.fail("connect/setup", "e3");
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

        let ev1 = r.fail("fetch", "table or source \"t_rt_alerts\" does not exist");
        assert_eq!(r.phase(), Phase::Disconnected);
        assert!(matches!(ev1, Event::Log { .. }));

        // Rebuild still in flight: a couple of connect/setup attempts land before the DDL
        // finishes.
        r.fail("connect/setup", "subscription \"sub_alerts\" does not exist");
        r.fail("connect/setup", "subscription \"sub_alerts\" does not exist");
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
