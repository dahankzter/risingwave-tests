//! Probes reality instead of remembering the last action taken through the API.
//!
//! Before this module existed, `AppState::status` was only ever updated as a side effect of a
//! handler in `api.rs` — `cluster/up` set `cluster: "up"`, `pipeline/rebuild` set
//! `pipeline: "rebuilt"`, and so on. A console started against an already-running cluster (the
//! ordinary case once the demo is set up ahead of time) never took any of those actions, so
//! `/api/status` reported `"unknown"` forever, even with alerts visibly flowing on the same page.
//! This module instead asks: is the container running (`Cluster::is_running`), does the database
//! accept a connection, and does the realtime pipeline actually exist (`t_rt`, `mv_rt`,
//! `t_rt_alerts`, and the `rt_feed` sink, queried from `rw_catalog`). It runs once at startup and
//! then on a fixed tick, publishing the result as `Event::Status` so the UI updates live. The
//! action-side `set_status` calls in `api.rs` are left in place for immediate feedback right after
//! a click; this poller is what keeps the number honest the rest of the time, and is the only
//! thing that can move it off `"unknown"` with no action ever having been taken.

use crate::event::Event;
use crate::state::AppState;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// How often the periodic probe runs. A few seconds is enough that the status strip feels live
/// without hammering podman or opening a fresh database connection too often.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Queries `rw_catalog` for the four objects that make up the realtime pipeline. `Ok(true)` only
/// if all four are present; `Ok(false)` if the connection worked but any are missing (the ordinary
/// "pipeline not built yet" state); `Err` only if the connection or a query itself failed.
///
/// The object names are fixed literals from `setup_realtime.sql`, not request input, so they are
/// interpolated directly rather than bound as parameters — `rw_catalog.rw_tables.name = any($1)`
/// against a `&[&str]` parameter is the more "correct" shape, but a plain `in (...)` avoids
/// depending on tokio-postgres's array-of-text encoding matching what `rw_catalog` expects.
/// The watermark lateness the live table actually declares, in seconds. Read from the catalog
/// rather than remembered from the last rebuild request: the console must report the pipeline it
/// has, not the pipeline someone intended — a dropdown left at 1s while the table still says 5s
/// otherwise misattributes four seconds of every latency it displays.
async fn live_lateness_secs(client: &tokio_postgres::Client) -> Option<u32> {
    let ddl: String = client.query_one("show create table t_rt", &[]).await.ok()?.try_get(1).ok()?;
    // `... WATERMARK FOR ts AS ts - INTERVAL '5' SECOND) APPEND ONLY`
    let after = ddl.split("INTERVAL '").nth(1)?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

async fn pipeline_present(db_url: &str) -> anyhow::Result<bool> {
    let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let tables: i64 = client
        .query_one(
            "select count(*) from rw_catalog.rw_tables where name in ('t_rt', 't_rt_alerts')",
            &[],
        )
        .await?
        .get(0);
    let mv: i64 = client
        .query_one("select count(*) from rw_catalog.rw_materialized_views where name = 'mv_rt'", &[])
        .await?
        .get(0);
    let sink: i64 =
        client.query_one("select count(*) from rw_catalog.rw_sinks where name = 'rt_feed'", &[]).await?.get(0);

    Ok(tables == 2 && mv == 1 && sink == 1)
}

/// One probe pass: check the container, then (only if it's up) the database and the pipeline.
/// Updates `state.status` and publishes the merged result as `Event::Status`. `load` is left
/// untouched — it is not a "does this exist" question, it is this process's own accounting of the
/// one run it might be driving, which the load handlers already keep correct.
pub async fn probe_once(state: &AppState) {
    let cluster = match state.cluster.is_running().await {
        Ok(true) => "up",
        Ok(false) => "down",
        // The probe itself failed (podman not on PATH, unexpected error) — this is the one
        // genuine "we don't know" case; everything else above is a real, checked answer.
        Err(_) => "unknown",
    };

    // Reading the live lateness needs its own connection; only worth opening when the cluster is
    // up and the pipeline is there to describe.
    let mut lateness = None;
    let pipeline = match cluster {
        "up" => match pipeline_present(&state.db_url).await {
            Ok(true) => {
                if let Ok((client, connection)) =
                    tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await
                {
                    tokio::spawn(async move {
                        let _ = connection.await;
                    });
                    lateness = live_lateness_secs(&client).await;
                }
                "present"
            }
            Ok(false) => "absent",
            Err(_) => "unknown",
        },
        // Container confirmed not running: the pipeline cannot be reachable either, and that's a
        // real answer, not an unknown.
        "down" => "absent",
        _ => "unknown",
    };

    state.set_lateness(lateness);
    state.set_status(|s| {
        s.cluster = cluster.to_string();
        s.pipeline = pipeline.to_string();
    });
    let snap = state.status_snapshot();
    state.publish(Event::Status {
        cluster: snap.cluster,
        pipeline: snap.pipeline,
        load: snap.load,
        lateness_secs: lateness,
    });
}

/// Spawns the poller: one `probe_once` immediately (so a page opened right after the server
/// starts already sees real state, not the pre-probe `"unknown"` default), then one every
/// `POLL_INTERVAL` for the lifetime of the process — same "never returns" shape as
/// `stream::spawn_reader` and `ws::spawn_aggregator`.
pub fn spawn_status_poller(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            probe_once(&state).await;
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    })
}
