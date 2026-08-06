// Uses axum's testing surface: build the router, send requests, assert status codes.
// No database and no podman required for any assertion here.

#[tokio::test]
async fn clean_without_the_confirmation_token_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(&app, "/api/cluster/clean", serde_json::json!({})).await;
    assert_eq!(res, 400, "clean must refuse without an explicit confirmation");
}

#[tokio::test]
async fn clean_with_the_wrong_token_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(&app, "/api/cluster/clean", serde_json::json!({"confirm": "yes"})).await;
    assert_eq!(res, 400);
}

#[tokio::test]
async fn starting_a_load_with_an_invalid_rate_is_rejected() {
    let app = bench_web::router_for_test();
    let res = bench_web::post_json(
        &app,
        "/api/load/start",
        serde_json::json!({"rate": 0, "rows": 100, "partitions": 10}),
    )
    .await;
    assert_eq!(res, 400, "an invalid rate must not reach Pacer::new, which panics on it");
}

/// `router_for_test()` wires a deliberately unreachable `db_url` ("postgres://unused/unused" —
/// "unused" resolves to nothing). A request here passes `RunConfig::validate()` cleanly (sane
/// rate, small rows/partitions, no bound-parameter or ties problem), so if it still came back 400
/// that would mean `/api/load/start` conflates "your request was invalid" with "the database
/// could not be reached" — a client would have no way to tell those apart. This needs no live
/// database: the point is exactly that the connection attempt fails fast (DNS lookup on a
/// nonexistent host), and the failure must not be reported as a client error.
#[tokio::test]
async fn a_valid_load_request_against_an_unreachable_database_is_not_a_400() {
    let app = bench_web::router_for_test();
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        bench_web::post_json(
            &app,
            "/api/load/start",
            serde_json::json!({"rate": 100.0, "rows": 10, "partitions": 5, "batch": 10}),
        ),
    )
    .await
    .expect("the connection attempt to a nonexistent host must fail fast, not hang");
    assert_ne!(res, 400, "an unreachable database is not a client error; got 400");
    assert_eq!(res, 500, "expected 500 for a start() failure, got {res}");
}

#[tokio::test]
async fn static_assets_are_served_with_sane_mime_types() {
    let app = bench_web::router_for_test();

    let get = |uri: &'static str| {
        let app = app.clone();
        async move {
            use axum::body::Body;
            use axum::http::Request;
            use tower::ServiceExt;
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            app.oneshot(req).await.unwrap()
        }
    };

    let res = get("/").await;
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("text/html"), "got {ct}");

    let res = get("/app.js").await;
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("javascript"), "got {ct}");

    let res = get("/style.css").await;
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("css"), "got {ct}");

    let res = get("/fonts/roboto-flex.css").await;
    assert_eq!(res.status(), 200);

    let res = get("/fonts/RobotoFlex-latin.woff2").await;
    assert_eq!(res.status(), 200);
    let ct = res.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(ct.contains("font") || ct.contains("woff"), "got {ct}");

    let res = get("/js/state.js").await;
    assert_eq!(res.status(), 200);
}
