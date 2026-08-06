//! Files embedded into the binary via `rust-embed` so the server is CWD-independent — the same
//! reasoning as `assets.rs`'s embedded `static/`, applied to the two files this crate shells out
//! to or reads from disk: the realtime pipeline's setup SQL, and the latency probe script.
//!
//! Before this module existed, `POST /api/pipeline/rebuild` read `--setup-sql`'s *default*
//! value — `../scenarios/perf/setup_realtime.sql` — which only resolves when the process is
//! launched from `web/`. Launched from the repo root (a perfectly normal way to start the
//! server), the same request 500s with "No such file or directory". Embedding the real content at
//! compile time removes the dependency on where the binary happens to be run from; `--setup-sql`
//! remains available as an explicit override (e.g. to iterate on the SQL without a rebuild).

use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../../scenarios/perf"]
struct PipelineSql;

#[derive(RustEmbed)]
#[folder = "../../latency"]
struct LatencyScripts;

/// The embedded copy of `scenarios/perf/setup_realtime.sql`, used whenever `--setup-sql` is not
/// passed. Panics if the file was somehow not embedded — that would mean the build itself is
/// broken (the file moved or was renamed without updating this module), not a runtime condition
/// any caller could recover from.
pub fn setup_sql() -> String {
    let file = PipelineSql::get("setup_realtime.sql")
        .expect("scenarios/perf/setup_realtime.sql must be embedded at build time");
    String::from_utf8(file.data.into_owned()).expect("setup_realtime.sql is valid UTF-8")
}

/// The embedded copy of `latency/probe.sh`, run by `POST /api/probe/start`. Written out to a
/// temp file per run (see `probe.rs`) rather than executed via `bash -s` from a pipe, because the
/// script itself uses `$$` for per-run-unique probe partitions and reads its own reliably from a
/// real file when diagnosing a failure is easier with a path to point at.
pub fn probe_script() -> Vec<u8> {
    LatencyScripts::get("probe.sh")
        .expect("latency/probe.sh must be embedded at build time")
        .data
        .into_owned()
}

/// Strips psql meta-commands (`\echo` and friends) out of embedded SQL before it is sent over
/// `batch_execute`, which understands SQL only — not psql's own command language. Mirrors
/// `bench_core::pipeline::run_sql_file`'s cleaning step exactly; duplicated here (rather than
/// reused) because that function is file-based and this module's whole point is to avoid the
/// filesystem.
pub fn strip_psql_meta_commands(sql: &str) -> String {
    sql.lines().filter(|l| !l.trim_start().starts_with('\\')).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_sql_is_embedded_and_nonempty() {
        let sql = setup_sql();
        assert!(sql.contains("create table t_rt"), "embedded setup SQL looks wrong: {sql}");
    }

    #[test]
    fn probe_script_is_embedded_and_nonempty() {
        let script = probe_script();
        let text = String::from_utf8(script).expect("probe.sh is valid UTF-8");
        assert!(text.starts_with("#!/usr/bin/env bash"), "embedded probe.sh looks wrong: {text}");
    }

    #[test]
    fn strip_psql_meta_commands_drops_backslash_lines_only() {
        let sql = "select 1;\n\\echo hi\ncreate table t (x int);\n  \\echo indented too";
        let cleaned = strip_psql_meta_commands(sql);
        assert_eq!(cleaned, "select 1;\ncreate table t (x int);");
    }
}
