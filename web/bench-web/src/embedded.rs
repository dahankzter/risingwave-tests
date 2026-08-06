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

/// The semantics scenarios, so the console can run one on demand — the correctness half of a demo,
/// next to the throughput half. Embedded for the same CWD-independence reason as the rest.
#[derive(RustEmbed)]
#[folder = "../../scenarios/semantics"]
struct SemanticsScenarios;

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

/// The semantics scenarios' file names, sorted, without the `.sql`.
pub fn scenario_names() -> Vec<String> {
    let mut names: Vec<String> = SemanticsScenarios::iter()
        .filter_map(|f| f.strip_suffix(".sql").map(str::to_owned))
        .collect();
    names.sort();
    names
}

/// One scenario's SQL. `None` for a name that is not embedded — the caller turns that into a 404
/// rather than trusting a name off the wire to reach the filesystem.
pub fn scenario_sql(name: &str) -> Option<String> {
    let file = SemanticsScenarios::get(&format!("{name}.sql"))?;
    String::from_utf8(file.data.into_owned()).ok()
}

/// Rewrite the realtime pipeline's watermark lateness. The setup SQL keeps a real, runnable
/// `interval '5' second` (so `make rt-setup` and psql work unchanged) and this substitutes the
/// number on the way to the server.
///
/// Returns `None` when the expected declaration is not found, and the caller must then refuse the
/// rebuild: quietly falling back to the file's own 5s while the UI reports 1s would make the
/// console lie about the one number that dominates the latency it displays.
pub fn with_watermark_lateness(sql: &str, seconds: u32) -> Option<String> {
    const NEEDLE: &str = "watermark for ts as ts - interval '5' second";
    if !sql.contains(NEEDLE) {
        return None;
    }
    Some(sql.replace(NEEDLE, &format!("watermark for ts as ts - interval '{seconds}' second")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_only_meta_commands() {
        let cleaned = strip_psql_meta_commands("select 1;\n\\echo hi\nselect 2;\n");
        assert!(cleaned.contains("select 1;") && cleaned.contains("select 2;"));
        assert!(!cleaned.contains("echo"));
    }

    #[test]
    fn the_scenarios_are_embedded() {
        let names = scenario_names();
        assert!(
            names.iter().any(|n| n == "preference_supersession"),
            "expected the supersession scenario, got {names:?}"
        );
        assert!(scenario_sql("preference_supersession").is_some());
        assert!(scenario_sql("../../../etc/passwd").is_none(), "names must not escape the bundle");
    }

    #[test]
    fn the_watermark_lateness_is_rewritten_or_refused() {
        let sql = setup_sql();
        let tightened = with_watermark_lateness(&sql, 1).expect("the declaration must be found");
        assert!(tightened.contains("interval '1' second"));
        assert!(!tightened.contains("interval '5' second"));
        // A pipeline whose declaration changed shape must refuse, not silently keep 5s.
        assert!(with_watermark_lateness("create table t (a int);", 1).is_none());
    }
}
