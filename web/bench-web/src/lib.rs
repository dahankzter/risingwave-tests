//! `bench-web`: the demo console for the MATCH_RECOGNIZE bench. A thin `main.rs` does argument
//! parsing and calls `serve`; everything else lives here so it can be exercised by
//! `tests/api.rs` without a database or a `podman` binary (see `router_for_test`).

pub mod api;
pub mod event;
pub mod podman;
pub mod state;
pub mod stream;
pub mod ws;

use axum::Router;
use podman::{NullCluster, PodmanDriver};
use state::AppState;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

/// Builds the full router over `AppState`, ready to have a listener attached. Merges the control
/// API (`api.rs`) with the `GET /ws` fan-out (`ws.rs`).
pub fn app_router(state: Arc<AppState>) -> Router {
    api::router().merge(ws::router()).with_state(state)
}

/// A router wired to a `NullCluster` and no live database, for the rejection-path tests in
/// `tests/api.rs`. Every assertion those tests make is reached before any handler touches the
/// cluster driver or opens a connection, so the placeholder `db_url` is never dialed.
pub fn router_for_test() -> Router {
    let state = Arc::new(AppState::new(
        Arc::new(NullCluster),
        "postgres://unused/unused".to_string(),
        PathBuf::from("unused.sql"),
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
    pub pipeline_sql: PathBuf,
}

/// Starts the alert reader, builds the router, and serves it at `cfg.bind` until the process is
/// killed. Binding to anything other than loopback is allowed (`--bind` overrides the default),
/// but `main` is responsible for the startup warning — this function only serves.
pub async fn serve(cfg: ServeConfig) -> anyhow::Result<()> {
    let state = Arc::new(AppState::new(
        Arc::new(PodmanDriver::new(cfg.container_name, cfg.image)),
        cfg.db_url.clone(),
        cfg.pipeline_sql,
    ));

    let _reader = stream::spawn_reader(cfg.db_url, state.tx.clone());
    let _aggregator = ws::spawn_aggregator(state.clone());

    let router = app_router(state);
    let listener = tokio::net::TcpListener::bind(cfg.bind).await?;
    println!("bench-web: listening on http://{}", cfg.bind);
    axum::serve(listener, router).await?;
    Ok(())
}
