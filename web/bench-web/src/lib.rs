//! `bench-web`: the demo console for the MATCH_RECOGNIZE bench. A thin `main.rs` does argument
//! parsing and calls `serve`; everything else lives here so it can be exercised by
//! `tests/api.rs` without a database or a `podman` binary (see `router_for_test`).

pub mod api;
pub mod assets;
pub mod embedded;
pub mod event;
pub mod metrics;
pub mod pin;
pub mod podman;
pub mod probe;
pub mod state;
pub mod status;
pub mod stream;
pub mod ws;

use axum::Router;
use podman::{NullCluster, PodmanDriver};
use state::AppState;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

/// Builds the full router over `AppState`, ready to have a listener attached. Merges the control
/// API (`api.rs`) with the `GET /ws` fan-out (`ws.rs`), and falls through to the embedded demo
/// page (`assets.rs`) for everything else — so `/`, `/app.js`, `/style.css`, `/js/*.js`, and
/// `/fonts/*` are all served from the binary, no disk access at runtime.
pub fn app_router(state: Arc<AppState>) -> Router {
    api::router().merge(ws::router()).with_state(state).fallback(assets::static_handler)
}

/// A router wired to a `NullCluster` and no live database, for the rejection-path tests in
/// `tests/api.rs`. Every assertion those tests make is reached before any handler touches the
/// cluster driver or opens a connection, so the placeholder `db_url` is never dialed.
pub fn router_for_test() -> Router {
    let state = Arc::new(AppState::new(
        Arc::new(NullCluster),
        "postgres://unused/unused".to_string(),
        "test-image".to_string(),
        None,
        None,
    ));
    app_router(state)
}

/// Sends `body` as a JSON POST to `uri` against `app` and returns the response's status code.
/// Small enough to inline at each call site, but shared so `tests/api.rs` doesn't hand-roll the
/// `Request`/`oneshot` boilerplate three times.
pub async fn post_json(app: &Router, uri: &str, body: serde_json::Value) -> u16 {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).expect("json body")))
        .expect("well-formed request");
    let res = app.clone().oneshot(req).await.expect("router does not fail to respond");
    res.status().as_u16()
}

/// Everything `main` needs to start the server: where to bind, where the cluster lives, and where
/// the scenario SQL for `pipeline/rebuild` is.
pub struct ServeConfig {
    pub bind: SocketAddr,
    pub db_url: String,
    pub container_name: String,
    pub image: String,
    /// CPU layout to apply, or `None` to leave both sides unpinned (the default).
    pub pin_layout: Option<pin::Layout>,
    /// `None` means use the SQL embedded into the binary at compile time (`embedded::setup_sql`)
    /// — CWD-independent. `Some` is an explicit override, e.g. to iterate on the SQL on disk
    /// without a rebuild.
    pub pipeline_sql: Option<PathBuf>,
}

/// Starts the alert reader, builds the router, and serves it at `cfg.bind` until the process is
/// killed. Binding to anything other than loopback is allowed (`--bind` overrides the default),
/// but `main` is responsible for the startup warning — this function only serves.
pub async fn serve(cfg: ServeConfig) -> anyhow::Result<()> {
    // Process-wide pinning is already done by the time this runs: when `cfg.pin_layout` is set,
    // `main` builds the tokio runtime with an `on_thread_start` hook that pins every worker and
    // blocking-pool thread to the bench cores as it starts (a plain call to `pin::apply_to_self`
    // here would only pin the single thread that called it — `sched_setaffinity` is per-thread,
    // not per-process — which is exactly the bug this hook exists to avoid). So the reader,
    // aggregator and status poller spawned below do land on the bench cores, but because every
    // runtime thread was pinned before it ran any task, not because affinity is inherited from
    // this one.
    let cpuset = cfg.pin_layout.as_ref().and_then(|l| l.cluster.clone());
    let state = Arc::new(AppState::new(
        Arc::new(PodmanDriver::new(cfg.container_name, cfg.image.clone()).with_cpuset(cpuset)),
        cfg.db_url.clone(),
        cfg.image,
        cfg.pin_layout,
        cfg.pipeline_sql,
    ));

    let _reader = stream::spawn_reader(cfg.db_url, state.tx.clone());
    let _aggregator = ws::spawn_aggregator(state.clone());
    let _status_poller = status::spawn_status_poller(state.clone());

    let router = app_router(state);
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    println!("bench-web: listening on http://{}", cfg.bind);
    axum::serve(listener, router).await?;
    Ok(())
}
