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
        .route("/api/probe/start", post(probe_start))
        .route("/api/env", get(env_info))
        .route("/api/scenarios", get(scenario_list))
        .route("/api/scenarios/run", post(scenario_run))
        .route("/api/sql/run", post(sql_run))
        .route("/api/catalog", get(catalog))
        .route("/api/pipeline/stats", get(pipeline_stats))
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

    // Refuse to start against a pipeline that isn't there. Without this the run "succeeds": the
    // handle spins, every insert fails on a missing table, rows/s sits at zero, and the only clue
    // on screen is the alert reader's reconnect warning — a state that reads as "the engine is
    // broken" when in fact nobody built the pipeline yet. The status poller keeps this field
    // current, so it is the same answer the header is showing the operator.
    // The poller's vocabulary: present / absent / unknown. Only `absent` is a definite no —
    // `unknown` (probe failed) must not block an operator who knows better.
    if state.status_snapshot().pipeline == "absent" {
        return err(
            StatusCode::CONFLICT,
            "no pipeline: press \"rebuild pipeline\" first (a load against a missing table \
             writes nothing and reports no error of its own)",
        );
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
            // New measurement epoch: percentiles must describe THIS run, not accumulate across
            // runs (a rebuilt pipeline mid-run once put p95 at 26x p50 from stale samples).
            state.clear_last_stats();
            state.publish(Event::StatsReset { epoch_ms: now_ms() });
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
/// 3. runs `setup_realtime.sql`.
///
/// It then returns — it does *not* recreate the subscription itself. `stream.rs`'s reader notices
/// its next `fetch` fails (the subscription is gone), falls back to `Phase::Disconnected`, and
/// re-declares both subscription and cursor on its own retry loop. Recreating it here too would
/// race the reader's retry.
#[derive(Deserialize, Default)]
struct RebuildRequest {
    /// Watermark lateness for the rebuilt pipeline, in seconds. Absent keeps the setup SQL's own
    /// declaration. This is the dial that dominates the latency the console displays — 5s of a 6s
    /// alert is this number — so it is worth being able to change without editing SQL mid-demo.
    lateness_secs: Option<u32>,
}

async fn pipeline_rebuild(
    State(state): State<Arc<AppState>>,
    body: Option<Json<RebuildRequest>>,
) -> Response {
    let req = body.map(|Json(r)| r).unwrap_or_default();
    if let Some(secs) = req.lateness_secs {
        if !(0..=600).contains(&secs) {
            return err(StatusCode::BAD_REQUEST, "lateness_secs must be between 0 and 600");
        }
    }
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

    // `state.pipeline_sql` is only `Some` when `--setup-sql` overrode the default; otherwise use
    // the copy embedded into the binary at compile time (see `embedded.rs`) so this handler works
    // identically regardless of the server's current directory.
    let sql_result = match &state.pipeline_sql {
        Some(path) => bench_core::pipeline::run_sql_file(&client, path).await,
        None => {
            let sql = crate::embedded::setup_sql();
            // Refuse rather than silently keep the file's own lateness: a console that reports 1s
            // while the pipeline runs 5s would misattribute four seconds of every measurement.
            let sql = match req.lateness_secs {
                Some(secs) => match crate::embedded::with_watermark_lateness(&sql, secs) {
                    Some(rewritten) => rewritten,
                    None => {
                        return err(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "could not find the watermark declaration to rewrite; refusing to \
                             rebuild with a lateness the pipeline would not actually have",
                        );
                    }
                },
                None => sql,
            };
            let cleaned = crate::embedded::strip_psql_meta_commands(&sql);
            client.batch_execute(&cleaned).await.map_err(anyhow::Error::from)
        }
    };
    if let Err(e) = sql_result {
        return err(StatusCode::INTERNAL_SERVER_ERROR, format!("pipeline/rebuild: setup sql: {e}"));
    }

    if let Some(secs) = req.lateness_secs {
        // Not recorded as state here: the status poller reads the live table's own declaration, so
        // what the console reports is always what the pipeline has rather than what was asked for.
        state.publish(Event::Log {
            level: "info".to_string(),
            text: format!("pipeline: rebuilt with watermark lateness {secs}s"),
        });
    }
    state.set_status(|s| s.pipeline = "rebuilt".to_string());
    // The rebuild dropped and recreated the pipeline: everything measured before it belongs to a
    // different world. Same epoch roll as load/start.
    state.clear_last_stats();
    state.publish(Event::StatsReset { epoch_ms: now_ms() });
    state.publish(Event::Log {
        level: "info".to_string(),
        text: "pipeline: rebuilt; alert reader will reconnect".to_string(),
    });
    StatusCode::OK.into_response()
}

// ---- probe ------------------------------------------------------------------------------------

#[derive(Deserialize)]
struct ProbeRequest {
    rounds: u32,
}

/// Starts a `latency/probe.sh` run (see `probe.rs`). Same single-run discipline as `load/start`:
/// 409 if a probe is already in flight, rather than queuing a second one. Runs detached — this
/// handler returns as soon as the run is accepted, and results stream back as `Event::Probe`
/// (one per round) and a final `Event::Log` over the WebSocket, not in this response.
async fn probe_start(State(state): State<Arc<AppState>>, Json(req): Json<ProbeRequest>) -> Response {
    if req.rounds == 0 {
        return err(StatusCode::BAD_REQUEST, "rounds must be positive");
    }

    {
        let mut guard = state.probe_running.lock().await;
        if *guard {
            return err(StatusCode::CONFLICT, "a probe is already running");
        }
        *guard = true;
    }

    // SENTINEL=off whenever a load is running — see `probe::sentinel_for`'s doc comment for why:
    // left on, the probe's own watermark-advancing rows release the load's matches early and
    // corrupt whatever the load's own measurement is reporting.
    let load_running = {
        let guard = state.run.lock().await;
        guard.as_ref().map(|handle| !handle.progress().borrow().done).unwrap_or(false)
    };
    let sentinel = crate::probe::sentinel_for(load_running);

    tokio::spawn(crate::probe::run_probe(state.clone(), req.rounds, sentinel));

    StatusCode::OK.into_response()
}

/// What a screenshot of this console was actually measured on. The details tab renders this so a
/// number cannot circulate without its caveats: an emulated or unpinned run is labelled as such
/// right next to the percentiles.
#[derive(Serialize)]
struct EnvInfo {
    image: String,
    host_os: String,
    host_arch: String,
    cores: usize,
    /// The container is always linux/amd64; on any other host arch it runs emulated and every
    /// performance number is a shape-check, not a measurement.
    emulated: bool,
    pinned: bool,
    pin_why: String,
    trusted: bool,
    reasons: Vec<String>,
    /// Watermark lateness in effect, or `null` for the pipeline SQL's own 5s. Most of the latency
    /// the console reports is this number.
    lateness_secs: Option<u32>,
}

async fn env_info(State(state): State<Arc<AppState>>) -> Response {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0);
    let host_arch = std::env::consts::ARCH.to_string();
    let emulated = host_arch != "x86_64";
    let (pinned, pin_why) = pin_status(&state);
    let mut reasons = Vec::new();
    if emulated {
        reasons.push(format!("container is linux/amd64, host is {host_arch}: emulated run"));
    }
    if !pinned {
        reasons.push("CPU unpinned: cluster and bench share cores".to_string());
    }
    if cores < 8 {
        reasons.push(format!("only {cores} cores available"));
    }
    Json(EnvInfo {
        image: state.image.clone(),
        host_os: std::env::consts::OS.to_string(),
        host_arch,
        cores,
        emulated,
        pinned,
        pin_why,
        trusted: reasons.is_empty(),
        reasons,
        lateness_secs: state.lateness_secs(),
    })
    .into_response()
}

/// Pin layout in effect, for `/api/env`. Off by default: every number recorded so far was
/// measured unpinned, so the flag must say which world a screenshot came from.
fn pin_status(state: &AppState) -> (bool, String) {
    match &state.pin_layout {
        Some(l) => (
            l.cluster.is_some(),
            format!("{} — {}", crate::pin::platform_note(), l.why),
        ),
        None => (false, "pinning off (default; --pin enables it)".to_string()),
    }
}

/// Approximate row counts for the pipeline-state panel, from `rw_table_stats` (storage-side key
/// counts — cheap, no scans; approximate by design and labelled as such in the UI).
#[derive(Serialize, Default)]
struct PipelineStats {
    base_rows: Option<i64>,
    matches: Option<i64>,
    alert_rows: Option<i64>,
}

async fn pipeline_stats(State(state): State<Arc<AppState>>) -> Response {
    match fetch_pipeline_stats(&state.db_url).await {
        Ok(stats) => Json(stats).into_response(),
        // A down cluster is the ordinary case for this endpoint, not a 500: the tab shows dashes.
        Err(_) => Json(PipelineStats::default()).into_response(),
    }
}

async fn fetch_pipeline_stats(db_url: &str) -> anyhow::Result<PipelineStats> {
    let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows = client
        .query(
            "select r.name, s.total_key_count \
             from rw_catalog.rw_table_stats s \
             join rw_catalog.rw_relations r on r.id = s.id \
             where r.name in ('t_rt', 'mv_rt', 't_rt_alerts')",
            &[],
        )
        .await?;
    let mut stats = PipelineStats::default();
    for row in rows {
        let name: String = row.get(0);
        let count: i64 = row.get(1);
        match name.as_str() {
            "t_rt" => stats.base_rows = Some(count),
            "mv_rt" => stats.matches = Some(count),
            "t_rt_alerts" => stats.alert_rows = Some(count),
            _ => {}
        }
    }
    Ok(stats)
}

/// Wall clock in unix milliseconds, for the measurement-epoch boundary. Compared against alert
/// ingest stamps, which come from the cluster's `proctime()` — the same clock as long as the
/// cluster and this process share a host, which is the case for every run this console drives.
fn now_ms() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as f64)
        .unwrap_or(0.0)
}

#[derive(Serialize)]
struct ScenarioInfo {
    name: String,
    /// The prose the scenario file itself opens with — see `embedded::leading_comment`.
    description: String,
}

/// The scenarios the console can run: the correctness half of a demo, next to the throughput half.
async fn scenario_list() -> Response {
    let list: Vec<ScenarioInfo> = crate::embedded::scenario_docs()
        .into_iter()
        .map(|(name, description)| ScenarioInfo { name, description })
        .collect();
    Json(list).into_response()
}

#[derive(Deserialize)]
struct ScenarioRequest {
    name: String,
}

/// One step of a scenario's transcript: the expectation the file states, and the result set that
/// followed it. Structured rather than pre-formatted text so the page can render a real table —
/// `1 | NULL | 3` in a monospace block is a transcript, not a result.
#[derive(Serialize, Default)]
struct ScenarioBlock {
    /// The `-- expect: …` line preceding this result, when there was one.
    expect: Option<String>,
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    /// Set when the statement failed; the page shows it in place of a table.
    error: Option<String>,
    /// Set for a statement that succeeded without producing a result set, so the page can say
    /// "ran" rather than leave it out. Scenario transcripts omit these (their `\echo` prose is the
    /// narrative); the playground shows them, because there the statements ARE what the user wrote
    /// and a silently-absent `create table` reads as a statement that never ran.
    status: Option<String>,
    /// A streaming plan, when this block describes one: the operator tree the check's view compiles
    /// to. Captured for every materialized view a check creates, because that tree IS the feature
    /// under discussion — MATCH_RECOGNIZE over a WatermarkSort over a hash exchange.
    plan: Option<Vec<PlanNode>>,
}

/// One operator in a streaming plan. RisingWave prints the tree with `└─` prefixes; the depth is
/// parsed out here so the page can lay it out as a tree instead of re-parsing box-drawing glyphs.
#[derive(Serialize)]
struct PlanNode {
    depth: usize,
    /// Operator name, e.g. `StreamMatchRecognize`.
    op: String,
    /// Everything inside the braces, or empty.
    detail: String,
}

/// Parse `EXPLAIN CREATE MATERIALIZED VIEW`'s output into depth-tagged nodes. Two spaces of
/// indentation per level in RisingWave's renderer; the `└─`/`├─` glyphs mark a child.
fn parse_plan(text: &str) -> Vec<PlanNode> {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|line| {
            let indent = line.chars().take_while(|c| *c == ' ').count();
            let body = line.trim_start().trim_start_matches(['└', '├', '─']).trim();
            if body.is_empty() {
                return None;
            }
            let (op, detail) = match body.split_once('{') {
                Some((op, rest)) => (op.trim().to_owned(), rest.trim_end_matches('}').trim().to_owned()),
                None => (body.to_owned(), String::new()),
            };
            Some(PlanNode { depth: indent / 2, op, detail })
        })
        .collect()
}

/// The view name and inner query of a `create materialized view <name> as <query>` statement, so
/// its plan can be explained while the tables it reads still exist — a check drops everything on
/// its way out, so this cannot be done afterwards.
fn materialized_view_query(stmt: &str) -> Option<(String, String)> {
    let lower = stmt.to_lowercase();
    let head = lower.find("create materialized view")? + "create materialized view".len();
    let rest = &stmt[head..];

    // Find the `AS` keyword as a TOKEN. Substring searches do not work here: `" as "` misses
    // `as\nselect` (these files wrap the line), and any later match swallows the query into the
    // name because MEASURES aliases contain `as` too.
    let mut offset = 0usize;
    let mut name: Option<&str> = None;
    for token in rest.split_whitespace() {
        let at = rest[offset..].find(token)? + offset;
        if token.eq_ignore_ascii_case("as") {
            let n = name?;
            let query = rest[at + token.len()..].trim().trim_end_matches(';').to_owned();
            if n.is_empty() || query.is_empty() {
                return None;
            }
            return Some((n.to_owned(), query));
        }
        name = Some(token);
        offset = at + token.len();
    }
    None
}

#[derive(Serialize)]
struct ScenarioResult {
    name: String,
    blocks: Vec<ScenarioBlock>,
    ok: bool,
}

/// Execute a SQL script statement by statement, collecting each result as a [`ScenarioBlock`].
///
/// Statement-at-a-time rather than `batch_execute` because that discards every result set, and the
/// results are the point — for a check they are what it asserts, and in the playground they are
/// what the user asked for. `\echo` lines become the label of the block that follows, which is how
/// a scenario's stated expectation ends up captioning its own result.
///
/// Also explains every materialized view it creates, while the view's inputs still exist: a check
/// drops its tables on the way out, and a user may too.
/// Whether statements that return no result set get a block of their own.
#[derive(PartialEq, Eq, Clone, Copy)]
enum Ack {
    /// Only results are reported — a curated scenario's `\echo` lines already narrate the DDL.
    ResultsOnly,
    /// Every statement is reported, so hand-written SQL accounts for each line the user typed.
    EveryStatement,
}

async fn run_sql_blocks(
    client: &tokio_postgres::Client,
    sql: &str,
    ack: Ack,
) -> (Vec<ScenarioBlock>, bool) {
    let mut blocks: Vec<ScenarioBlock> = Vec::new();
    let mut ok = true;
    // The `\echo` line most recently seen: it states the expectation for the result that follows.
    let mut pending_expect: Option<String> = None;
    for piece in split_scenario(sql) {
        let stmt = match piece {
            // The files write `\echo 'expect: …'`, so the word is already there — the caption
            // must not read "expect: expect: …".
            Piece::Echo(text) => {
                pending_expect = Some(text);
                continue;
            }
            Piece::Statement(stmt) => stmt,
        };

        if returns_rows(&stmt) {
            // With no `\echo` to caption it, a playground query captions itself with its own text;
            // a scenario deliberately does not, so its transcript never reads "expect select …" as
            // though the statement were the assertion.
            let caption = || match ack {
                Ack::EveryStatement => Some(first_words(&stmt)),
                Ack::ResultsOnly => None,
            };
            match client.query(&stmt, &[]).await {
                Ok(rows) => {
                    let columns = rows
                        .first()
                        .map(|r| r.columns().iter().map(|c| c.name().to_owned()).collect())
                        .unwrap_or_default();
                    blocks.push(ScenarioBlock {
                        expect: pending_expect.take().or_else(caption),
                        columns,
                        rows: rows.iter().map(row_cells).collect(),
                        ..Default::default()
                    });
                }
                Err(e) => {
                    ok = false;
                    blocks.push(ScenarioBlock {
                        expect: pending_expect.take().or_else(caption),
                        error: Some(crate::stream::chain_of(&e)),
                        ..Default::default()
                    });
                    break;
                }
            }
            continue;
        }

        if let Err(e) = client.batch_execute(&stmt).await {
            ok = false;
            blocks.push(ScenarioBlock {
                expect: Some(format!("running: {}", first_words(&stmt))),
                error: Some(crate::stream::chain_of(&e)),
                ..Default::default()
            });
            break;
        }

        if ack == Ack::EveryStatement {
            blocks.push(ScenarioBlock {
                expect: Some(first_words(&stmt)),
                status: Some("ran".to_string()),
                ..Default::default()
            });
        }

        // A view's plan is explained now, while its inputs exist. A failure here is not the
        // statement failing — the view was created successfully — so it is stepped over rather
        // than aborting the run.
        if let Some((name, query)) = materialized_view_query(&stmt) {
            let explain = format!("explain create materialized view _plan_{name} as {query}");
            if let Ok(rows) = client.query(&explain, &[]).await {
                let text = rows
                    .iter()
                    .filter_map(|r| r.try_get::<_, Option<String>>(0).ok().flatten())
                    .collect::<Vec<_>>()
                    .join("\n");
                let nodes = parse_plan(&text);
                if !nodes.is_empty() {
                    blocks.push(ScenarioBlock {
                        expect: Some(format!("plan for {name}")),
                        plan: Some(nodes),
                        ..Default::default()
                    });
                }
            }
        }
    }

    (blocks, ok)
}

/// Run one embedded scenario against the cluster and hand back its transcript.
///
/// The scenario files are psql scripts: they carry `\echo` lines (their expectations, in prose)
/// and a mix of DDL and queries. This runs them statement by statement so a `select`'s rows can be
/// captured — `batch_execute` would run the lot but discard every result — and keeps the `\echo`
/// text inline, which is what makes the transcript readable as "expected X, got Y".
async fn scenario_run(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ScenarioRequest>,
) -> Response {
    let Some(sql) = crate::embedded::scenario_sql(&req.name) else {
        return err(StatusCode::NOT_FOUND, format!("no such scenario: {}", req.name));
    };
    if state.status_snapshot().cluster != "up" {
        return err(StatusCode::CONFLICT, "the cluster is not up");
    }

    let (client, connection) = match tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await
    {
        Ok(pair) => pair,
        Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("connect: {e}")),
    };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let (blocks, ok) = run_sql_blocks(&client, &sql, Ack::ResultsOnly).await;

    state.publish(Event::Log {
        level: if ok { "info".to_string() } else { "error".to_string() },
        text: format!("{}: {}", req.name, if ok { "passed" } else { "failed" }),
    });
    Json(ScenarioResult { name: req.name, blocks, ok }).into_response()
}

enum Piece {
    Echo(String),
    Statement(String),
}

/// Split a psql scenario into `\echo` prose and individual statements. Statement splitting is on
/// `;` at end of line, which is how these files are written throughout — a full SQL parser would
/// be the wrong amount of machinery for a fixed, in-repo set of scripts.
fn split_scenario(sql: &str) -> Vec<Piece> {
    let mut pieces = Vec::new();
    let mut current = String::new();
    for line in sql.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("\\echo") {
            pieces.push(Piece::Echo(rest.trim().trim_matches('\'').to_string()));
            continue;
        }
        if trimmed.starts_with('\\') || trimmed.is_empty() {
            continue;
        }
        // Drop a trailing line comment before looking for the terminator. A full-line comment
        // becomes empty and is skipped, as before; a comment *after* code no longer hides the `;`,
        // which used to glue the statement to the one following it.
        let code = strip_line_comment(line);
        let code_trimmed = code.trim();
        if code_trimmed.is_empty() {
            continue;
        }
        current.push_str(code_trimmed);
        current.push('\n');
        if code_trimmed.ends_with(';') {
            pieces.push(Piece::Statement(std::mem::take(&mut current)));
        }
    }
    if !current.trim().is_empty() {
        pieces.push(Piece::Statement(current));
    }
    pieces
}

/// Cut a trailing `--` line comment, leaving `--` that falls inside a single-quoted literal alone.
/// Quote tracking is deliberately minimal — single quotes only, no dollar quoting — because that is
/// the whole of what these scripts and hand-typed queries use, and a fuller lexer would be a
/// different piece of machinery than a line splitter.
fn strip_line_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_quote = false;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' => in_quote = !in_quote,
            b'-' if !in_quote && bytes.get(i + 1) == Some(&b'-') => return &line[..i],
            _ => {}
        }
        i += 1;
    }
    line
}

/// Whether a statement produces a result set worth capturing.
///
/// The distinction matters because the two paths are different protocol calls: a row-returning
/// statement goes through `query`, anything else through `batch_execute`, which discards results.
/// Sending `show tables` down the second path is not an error — it just silently produces nothing,
/// which is the worst possible outcome for someone using it to find out what exists.
///
/// Classified on the leading keyword only, so a table or column merely *named* like one (`update
/// selections …`) is not misread.
fn returns_rows(stmt: &str) -> bool {
    let Some(first) = stmt.split_whitespace().next() else {
        return false;
    };
    let first = first.trim_start_matches('(').to_lowercase();
    matches!(
        first.as_str(),
        "select" | "show" | "describe" | "desc" | "explain" | "with" | "values" | "table"
    )
}

/// Every cell of a row as display text.
fn row_cells(row: &tokio_postgres::Row) -> Vec<String> {
    (0..row.len()).map(|i| cell_text(row, i)).collect()
}

/// One cell as display text, `NULL` when absent.
fn cell_text(row: &tokio_postgres::Row, i: usize) -> String {
    cell_of(row, i).unwrap_or_else(|| "NULL".to_string())
}

/// One cell, dispatching on the column's Postgres type: a scenario's shape is not known up front,
/// and reading everything as text fails on anything that is not (which rendered every integer
/// measure as a placeholder — hiding exactly the values a check asserts).
///
/// `timestamp` and `timestamptz` are deliberately separate arms: they map to different Rust types
/// (`PrimitiveDateTime` and `OffsetDateTime`), and reading both as the latter fails on the former,
/// which surfaced as `NULL` — a transcript claiming a check produced no timestamp when it did.
fn cell_of(row: &tokio_postgres::Row, i: usize) -> Option<String> {
    use tokio_postgres::types::Type;

    let ty = row.columns()[i].type_();
    Some(match *ty {
        Type::BOOL => row.try_get::<_, Option<bool>>(i).ok()??.to_string(),
        Type::INT2 => row.try_get::<_, Option<i16>>(i).ok()??.to_string(),
        Type::INT4 => row.try_get::<_, Option<i32>>(i).ok()??.to_string(),
        Type::INT8 => row.try_get::<_, Option<i64>>(i).ok()??.to_string(),
        Type::FLOAT4 => row.try_get::<_, Option<f32>>(i).ok()??.to_string(),
        Type::FLOAT8 => row.try_get::<_, Option<f64>>(i).ok()??.to_string(),
        Type::TIMESTAMPTZ => {
            let ts = row.try_get::<_, Option<time::OffsetDateTime>>(i).ok()??;
            fmt_datetime(ts.year(), u8::from(ts.month()), ts.day(), ts.hour(), ts.minute(), ts.second())
        }
        Type::TIMESTAMP => {
            let ts = row.try_get::<_, Option<time::PrimitiveDateTime>>(i).ok()??;
            fmt_datetime(ts.year(), u8::from(ts.month()), ts.day(), ts.hour(), ts.minute(), ts.second())
        }
        _ => row.try_get::<_, Option<String>>(i).ok()??,
    })
}

/// Seconds are enough: transcripts are read for values, not sub-second ordering, and a full
/// RFC3339 stamp per column makes a row unreadable.
fn fmt_datetime(y: i32, mo: u8, d: u8, h: u8, mi: u8, s: u8) -> String {
    format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}")
}

fn first_words(stmt: &str) -> String {
    stmt.split_whitespace().take(4).collect::<Vec<_>>().join(" ")
}

#[derive(Deserialize)]
struct SqlRequest {
    sql: String,
}

#[derive(Serialize)]
struct SqlResult {
    blocks: Vec<ScenarioBlock>,
    ok: bool,
}

/// The playground: run whatever the user typed and hand back every result set.
///
/// This is an arbitrary-SQL endpoint, which is a deliberate choice rather than an oversight. The
/// console already starts and destroys clusters and deletes the data volume, binds loopback by
/// default, and warns loudly when told to bind anything else — a SQL box does not widen that
/// exposure. What it adds is the ability to put RisingWave through its paces beyond the fixed
/// scenarios, which is the point of a bench.
async fn sql_run(State(state): State<Arc<AppState>>, Json(req): Json<SqlRequest>) -> Response {
    if req.sql.trim().is_empty() {
        return err(StatusCode::BAD_REQUEST, "no SQL to run");
    }
    if state.status_snapshot().cluster != "up" {
        return err(StatusCode::CONFLICT, "the cluster is not up");
    }
    let (client, connection) =
        match tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await {
            Ok(pair) => pair,
            Err(e) => return err(StatusCode::INTERNAL_SERVER_ERROR, format!("connect: {e}")),
        };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let (blocks, ok) = run_sql_blocks(&client, &req.sql, Ack::EveryStatement).await;
    Json(SqlResult { blocks, ok }).into_response()
}

#[derive(Serialize)]
struct CatalogEntry {
    name: String,
    kind: &'static str,
}

/// What exists in the database right now, for the playground's browser. Sourced from `rw_catalog`
/// rather than a `show` statement per kind so one request answers the whole question.
async fn catalog(State(state): State<Arc<AppState>>) -> Response {
    let (client, connection) =
        match tokio_postgres::connect(&state.db_url, tokio_postgres::NoTls).await {
            // A down cluster is an ordinary state for this endpoint: an empty list, not a 500.
            Ok(pair) => pair,
            Err(_) => return Json(Vec::<CatalogEntry>::new()).into_response(),
        };
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut out: Vec<CatalogEntry> = Vec::new();
    for (kind, relation) in [
        ("table", "rw_tables"),
        ("materialized view", "rw_materialized_views"),
        ("source", "rw_sources"),
        ("sink", "rw_sinks"),
        // `rw_views` is skipped deliberately: it is dominated by the pg_catalog compatibility
        // views, which are noise for someone writing queries against their own objects.
    ] {
        let sql = format!("select name from rw_catalog.{relation} order by name");
        if let Ok(rows) = client.query(&sql, &[]).await {
            for row in rows {
                if let Ok(name) = row.try_get::<_, String>(0) {
                    // Internal state tables are noise for someone writing queries.
                    if name.starts_with("__internal") {
                        continue;
                    }
                    out.push(CatalogEntry { name, kind });
                }
            }
        }
    }
    Json(out).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_line_comment_still_ends_the_statement() {
        // Curated scenario files never do this, but pasted SQL does constantly. Without the cut,
        // `select 1;` never terminates and swallows the statement after it — which the extended
        // protocol then rejects as multiple statements.
        let pieces = split_scenario("select 1; -- first\nselect 2;\n");
        let stmts: Vec<String> = pieces
            .iter()
            .filter_map(|p| match p {
                Piece::Statement(s) => Some(s.trim().to_string()),
                Piece::Echo(_) => None,
            })
            .collect();
        assert_eq!(stmts, vec!["select 1;", "select 2;"]);
    }

    #[test]
    fn a_double_dash_inside_a_string_literal_is_not_a_comment() {
        let pieces = split_scenario("select 'a -- b';\n");
        let stmts: Vec<String> = pieces
            .iter()
            .filter_map(|p| match p {
                Piece::Statement(s) => Some(s.trim().to_string()),
                Piece::Echo(_) => None,
            })
            .collect();
        assert_eq!(stmts, vec!["select 'a -- b';"]);
    }

    #[test]
    fn row_returning_statements_are_recognised() {
        for stmt in [
            "select 1",
            "  SELECT 1",
            "show tables",
            "SHOW MATERIALIZED VIEWS",
            "describe t_rt",
            "explain create materialized view m as select 1",
            "with x as (select 1) select * from x",
            "values (1), (2)",
        ] {
            assert!(returns_rows(stmt), "{stmt:?} returns rows");
        }
    }

    #[test]
    fn statements_without_a_result_set_are_recognised() {
        for stmt in [
            "create table t (a int)",
            "insert into t values (1)",
            "flush",
            "drop table t",
            "create materialized view m as select * from t",
            // A column or table merely *named* like a keyword must not flip the classification.
            "update selections set a = 1",
        ] {
            assert!(!returns_rows(stmt), "{stmt:?} has no result set");
        }
    }
}
