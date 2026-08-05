//! Live-cluster tests. Ignored by default so `cargo test` stays offline.
//! Run with a cluster up:  cargo test -p bench-core -- --ignored

use bench_core::gen::Kind;
use bench_core::sink::{Direct, Row, Ts};

const URL: &str = "postgres://root@127.0.0.1:4566/dev";

#[tokio::test]
#[ignore]
async fn direct_sink_inserts_rows() {
    let mut d = Direct::connect(URL, "t_direct_test".to_string(), 0).await.unwrap();
    d.client()
        .batch_execute(
            "set rw_implicit_flush to true;
             drop table if exists t_direct_test;
             create table t_direct_test (id int, ts int, kind varchar, amount int) append only;",
        )
        .await
        .unwrap();

    let rows: Vec<Row> = (0..3)
        .map(|i| Row {
            partition: 7,
            ts: Ts::Tick(10 + i),
            kind: Kind::Bet,
            amount: 42,
            payload: vec![],
        })
        .collect();
    d.write_async(&rows).await.unwrap();

    let got: i64 = d
        .client()
        .query_one("select count(*) from t_direct_test", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(got, 3);

    d.client().batch_execute("drop table t_direct_test;").await.unwrap();
}
