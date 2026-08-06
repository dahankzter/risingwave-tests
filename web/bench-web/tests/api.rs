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
