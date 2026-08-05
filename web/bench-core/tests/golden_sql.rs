use bench_core::gen::{Config, Generator, Kind};
use bench_core::sink::{column_list, EmitSql, Row, Sink, Ts};

/// The column list must be explicit. The realtime table carries a generated `proctime()` column
/// (`ingest_ts`), so a positional INSERT does not line up with the table shape.
#[test]
fn column_list_is_explicit_and_includes_payload() {
    assert_eq!(column_list(0), "(id, ts, kind, amount)");
    assert_eq!(column_list(2), "(id, ts, kind, amount, p0, p1)");
}

#[test]
fn emitted_sql_matches_the_golden_file() {
    let cfg = Config { rows: 20, partitions: 5, seed: 42, ..Config::default() };
    let mut g = Generator::new(cfg.clone()).unwrap();

    let mut out = Vec::new();
    {
        let mut sink = EmitSql::new(&mut out, "t_perf".to_string(), 0);
        let rows: Vec<Row> = (0..cfg.rows)
            .map(|i| {
                let e = g.next_event();
                Row {
                    partition: e.partition,
                    ts: Ts::Tick(10 + i as i64),
                    kind: e.kind,
                    amount: e.amount,
                    payload: vec![],
                }
            })
            .collect();
        sink.write(&rows).unwrap();
        sink.finish().unwrap();
    }

    let actual = String::from_utf8(out).unwrap();
    let expected = include_str!("golden/bulk_seed42.sql");
    assert_eq!(actual, expected, "emitted SQL drifted from the golden file");
}

#[test]
fn kinds_are_quoted_as_sql_string_literals() {
    let mut out = Vec::new();
    {
        let mut sink = EmitSql::new(&mut out, "t".to_string(), 0);
        sink.write(&[Row {
            partition: 1,
            ts: Ts::Tick(10),
            kind: Kind::Withdraw,
            amount: 90,
            payload: vec![],
        }])
        .unwrap();
    }
    let sql = String::from_utf8(out).unwrap();
    assert!(sql.contains("(1, 10, 'withdraw', 90)"), "got: {sql}");
}
