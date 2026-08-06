//! The control API: cluster up/down/clean, load start/stop/rate, pipeline rebuild, status.
//!
//! One global run, one global cluster (see the plan's Global Constraints): every handler that
//! would otherwise "queue" a second thing instead returns 409. `cluster/clean` is the one
//! destructive endpoint and is gated on an explicit `{"confirm":"clean"}` body — a UI dialog is
//! not enough, because `curl` doesn't see the UI.

use crate::event::Event;
use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bench_core::gen::Config as GenConfig;
use bench_core::run::RunConfig;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/cluster/up", post(cluster_up))
        .route("/api/cluster/down", post(cluster_down))
        .route("/api/cluster/clean", post(cluster_clean))
        .route("/api/load/start", post(load_start))
        .route("/api/load/stop", post(load_stop))
        .route("/api/load/rate", post(load_rate))
        .route("/api/pipeline/rebuild", post(pipeline_rebuild))
}

fn err(code: StatusCode, msg: impl Into<String>) -> Response {
    (code, msg.into()).into_response()
}

// ---- status -------------------------------------------------------------------------------

async fn status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Json(state.status_snapshot())
}

// ---- cluster --------------------------------------------------------------------------------

async fn cluster_up(State(state): State<Arc<AppState>>) -> Response {
    match state.cluster.up().await {
        Ok(()) => {
            state.set_status(|s| s.cluster = "up".to_string());
            state.publish(Event::Log { level: "info".to_string(), text: "cluster: up".to_string() });
            StatusCode::OK.into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn cluster_down(State(state): State<Arc<AppState>>) -> Response {
    match state.cluster.down().await {
        Ok(()) => {
            state.set_status(|s| s.cluster = "down".to_string());
            state.publish(Event::Log { level: "info".to_string(), text: "cluster: down".to_string() });
            StatusCode::OK.into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct CleanRequest {
    #[serde(default)]
    confirm: Option<String>,
}

/// The token check happens here, in the HTTP layer, before `state.cluster.clean()` is ever
/// called — not inside the driver. That keeps the confirmation a property of the endpoint (so it
/// is exercised by `clean_without_the_confirmation_token_is_rejected` and
/// `clean_with_the_wrong_token_is_rejected` with no podman binary present) rather than of the
/// shell-out, and it is still "before doing anything": nothing destructive happens on the
/// rejection paths.
async fn cluster_clean(State(state): State<Arc<AppState>>, Json(body): Json<CleanRequest>) -> Response {
    if body.confirm.as_deref() != Some("clean") {
        return err(
            StatusCode::BAD_REQUEST,
            "cluster/clean requires {\"confirm\":\"clean\"} in the request body",
        );
    }
    match state.cluster.clean().await {
        Ok(()) => {
            state.set_status(|s| {
                s.cluster = "down".to_string();
                s.pipeline = "unknown".to_string();
            });
            state.publish(Event::Log {
                level: "warn".to_string(),
                text: "cluster: cleaned (container and data volume removed)".to_string(),
            });
            StatusCode::OK.into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

// ---- load -----------------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
struct LoadRequest {
    table: String,
    /// "bulk" or "realtime"; anything else is treated as bulk.
    mode: String,
    rows: u64,
    partitions: i32,
    batch: usize,
    hot_count: i32,
    hot_share: f64,
    abandon_prob: f64,
    ties: u32,
    seed: u64,
    rate: f64,
    payload_cols: usize,
    payload_bytes: usize,
}

impl Default for LoadRequest {
    fn default() -> Self {
        let g = GenConfig::default();
        Self {
            table: "t_rt".to_string(),
            mode: "realtime".to_string(),
            rows: g.rows,
            partitions: g.partitions,
            batch: 500,
            hot_count: g.hot_count,
            hot_share: g.hot_share,
            abandon_prob: g.abandon_prob,
            ties: g.ties,
            seed: g.seed,
            rate: 2000.0,
            payload_cols: g.payload_cols,
            payload_bytes: g.payload_bytes,
        }
    }
}

impl LoadRequest {
    fn into_run_config(self, url: String) -> RunConfig {
        let gen = GenConfig {
            rows: self.rows,
            partitions: self.partitions,
            hot_count: self.hot_count,
            hot_share: self.hot_share,
            abandon_prob: self.abandon_prob,
            ties: self.ties,
            seed: self.seed,
            payload_cols: self.payload_cols,
            payload_bytes: self.payload_bytes,
            ..GenConfig::default()
        };
        RunConfig { table: self.table, url, realtime: self.mode == "realtime", batch: self.batch, rate: self.rate, gen }
    }
}

/// Starts the one global load. `RunConfig::validate` (called before anything else touches the
/// generator or a connection) is the single source of truth for "rate <= 0", "batch times columns
/// exceeds the bound-parameter limit", and "ties > 1 in realtime mode" — this handler does not
/// duplicate any of those checks, it just surfaces `validate`'s error as 400.
async fn load_start(State(state): State<Arc<AppState>>, Json(req): Json<LoadRequest>) -> Response {
    let cfg = req.into_run_config(state.db_url.clone());
    if let Err(e) = cfg.validate() {
        return err(StatusCode::BAD_REQUEST, e.to_string());
    }

    let mut guard = state.run.lock().await;
    if let Some(handle) = guard.as_ref() {
        // `done` means the run finished on its own (hit its row target) without anyone calling
        // `/api/load/stop` to clear the slot; that is not "a load is running" and must not block
        // a new one. Anything else — including a run that is still catching up on its last
        // buffered write after `stop()` — counts as running.
        if !handle.progress().borrow().done {
            return err(StatusCode::CONFLICT, "a load is already running");
        }
    }
    if let Some(old) = guard.take() {
        let _ = old.join().await;
    }

    // `cfg.validate()` above already ruled out "the client sent something invalid" — anything
    // `start` fails on now (a bad connection string, an unreachable database, `Direct::connect`
    // erroring) is not the client's fault, so it is a 500, not a 400. Conflating the two would
    // leave a caller unable to tell "fix your request" from "the database is down".
    match bench_core::run::start(cfg).await {
        Ok(handle) => {
            *guard = Some(handle);
            drop(guard);
            state.set_status(|s| s.load = "running".to_string());
            state.publish(Event::Log { level: "info".to_string(), text: "load: started".to_string() });
            StatusCode::OK.into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, format!("load/start: {e}")),
    }
}

async fn load_stop(State(state): State<Arc<AppState>>) -> Response {
    let mut guard = state.run.lock().await;
    let Some(handle) = guard.take() else {
        return err(StatusCode::CONFLICT, "no load is running");
    };
    drop(guard);
    handle.stop();
    let result = handle.join().await;
    state.set_status(|s| s.load = "stopped".to_string());
    match result {
        Ok(()) => {
            state.publish(Event::Log { level: "info".to_string(), text: "load: stopped".to_string() });
            StatusCode::OK.into_response()
        }
        Err(e) => err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(Deserialize)]
struct RateRequest {
    rate: f64,
}

async fn load_rate(State(state): State<Arc<AppState>>, Json(req): Json<RateRequest>) -> Response {
    if req.rate <= 0.0 {
        return err(StatusCode::BAD_REQUEST, "rate must be positive");
    }
    let guard = state.run.lock().await;
    let Some(handle) = guard.as_ref() else {
        return err(StatusCode::CONFLICT, "no load is running");
    };
    handle.set_rate(req.rate);
    StatusCode::OK.into_response()
}

// ---- pipeline ---------------------------------------------------------------------------------

/// Rebuilds the realtime pipeline. Order matters and is not obvious from `setup_realtime.sql`
/// alone: a subscription on `t_rt_alerts` (created by the alert reader — see `stream.rs`) blocks
/// dropping that table, so the setup SQL's `drop table if exists t_rt_alerts` fails with a
/// dependency error unless the subscription is dropped first. This handler therefore, in order:
/// 1. stops any running load (so nothing is writing to `t_rt` mid-rebuild),
/// 2. drops `sub_alerts` explicitly,
/// 3. runs `setup_realtime.sql`,
/// and then returns — it does *not* recreate the subscription itself. `stream.rs`'s reader
/// notices its next `fetch` fails (the subscription is gone), falls back to `Phase::Disconnected`,
/// and re-declares both subscription and cursor on its own retry loop. Recreating it here too
/// would race the reader's retry.
async fn pipeline_rebuild(State(state): State<Arc<AppState>>) -> Response {
    {
        let mut guard = state.run.lock().await;
        if let Some(handle) = guard.take() {
            handle.stop();
            // A failed writer must not block the rebuild — that's the whole point of being able
            // to rebuild — but silently swallowing the error would hide a real problem (e.g. the
            // load died on a connection error moments before the table gets dropped anyway).
            // Surface it as a log line and keep going.
            if let Err(e) = handle.join().await {
                state.publish(Event::Log {
                    level: "warn".to_string(),
                    text: format!("pipeline/rebuild: the stopped load ended in error: {e}"),
                });
            }
            state.set_status(|s| s.load = "stopped".to_string());
        }
    }

    let (client, connection) = match tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await {
        Ok(pair) => pair,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("pipeline/rebuild: connect: {e}")),
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    if let Err(e) = client.batch_execute("drop subscription if exists sub_alerts").await {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("pipeline/rebuild: drop subscription: {e}"));
    }

    if let Err(e) = bench_core::pipeline::run_sql_file(&client, &state.pipeline_sql).await {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("pipeline/rebuild: setup sql: {e}"));
    }

    state.set_status(|s| s.pipeline = "rebuilt".to_string());
    state.publish(Event::Log {
        level: "info".to_string(),
        text: "pipeline: rebuilt; alert reader will reconnect".to_string(),
    });
    StatusCode::OK.into_response()
}
