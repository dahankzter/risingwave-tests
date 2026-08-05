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

use bench_core::pipeline::{seal, SealConfig};

/// Sealing must wait for the pipeline to drain before advancing the watermark. If it does not,
/// the far-future sentinel discards the rows still in flight and the match count freezes low.
#[tokio::test]
#[ignore]
async fn seal_waits_for_the_pipeline_to_drain() {
    let d = bench_core::sink::Direct::connect(URL, "t_seal_test".to_string(), 0)
        .await
        .unwrap();
    let c = d.client();
    c.batch_execute(
        "set rw_implicit_flush to true;
         drop materialized view if exists mv_seal_test;
         drop table if exists t_seal_test;
         create table t_seal_test (id int, ts int, kind varchar, amount int,
           watermark for ts as ts - 10) append only;
         create materialized view mv_seal_test as
         select * from t_seal_test match_recognize (
           partition by id order by ts
           measures count(*) as chain_len
           one row per match after match skip past last row
           pattern (d b w) within 5000
           define d as d.kind = 'deposit', b as b.kind = 'bet', w as w.kind = 'withdraw');",
    )
    .await
    .unwrap();

    // 100 complete chains, no sentinel.
    let mut sql = String::from("insert into t_seal_test (id, ts, kind, amount) values ");
    for i in 0..100i32 {
        if i > 0 {
            sql.push_str(", ");
        }
        let base = 10 + i * 10;
        sql.push_str(&format!(
            "({i}, {base}, 'deposit', 100), ({i}, {}, 'bet', 10), ({i}, {}, 'withdraw', 90)",
            base + 1,
            base + 2
        ));
    }
    c.batch_execute(&sql).await.unwrap();

    let cfg = SealConfig { table: "t_seal_test".into(), mv: "mv_seal_test".into(), ..Default::default() };
    let outcome = seal(c, &cfg).await.unwrap();

    assert_eq!(outcome.settled_after, 100, "every complete chain must be released by the seal");

    c.batch_execute("drop materialized view mv_seal_test; drop table t_seal_test;")
        .await
        .unwrap();
}
