//! Operator-metrics scrape: the Prometheus endpoint inside the container carries the three
//! `stream_match_recognize_*` counters, one series per actor.
//!
//! **The port is 1260, not 1222.** 1222 is the compute node's metrics port in a multi-node
//! deployment; the `single_node` binary this bench runs serves on 1260. Verified against a live
//! container (1222 accepts nothing; 1260 answers with 36 `stream_match_recognize_*` series) —
//! worth stating because the wrong port fails the same way an image without the counters does. This module fetches the text
//! exposition, sums each counter across actors, and hands the totals to the aggregator, which
//! publishes them as `Event::Metrics` on its 2s tick.
//!
//! The counters are cumulative for the LIFETIME OF THE CLUSTER — they survive dropped and
//! recreated MVs — so the UI labels them "since cluster start", never per-run.
//!
//! The fetch is a raw HTTP/1.0 GET over a `TcpStream`: the endpoint is same-host, plaintext, and
//! tiny, which does not justify an HTTP-client dependency.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct Totals {
    pub matches_emitted: u64,
    pub evicted_rows: u64,
    pub scan_budget_exhausted: u64,
}

/// Fetch and sum. `None` when the endpoint is unreachable (cluster down — a normal state, not an
/// error worth logging every 2 seconds) or when the body carries no MATCH_RECOGNIZE series at all.
///
/// That second case matters: an image built before the operator metrics landed, or a cluster whose
/// MATCH_RECOGNIZE actors have not started, serves a perfectly good `/metrics` with none of these
/// counters in it. Summing that to `Totals::default()` would publish three confident zeros — which
/// is what the console did while ten thousand matches were visibly streaming past in the feed.
/// Reporting nothing keeps the panel's dashes, which say "not measured" rather than "zero".
pub async fn scrape(addr: &str) -> Option<Totals> {
    let body = fetch(addr).await.ok()?;
    let (totals, found) = parse_counted(&body);
    if found == 0 { None } else { Some(totals) }
}

async fn fetch(addr: &str) -> anyhow::Result<String> {
    let connect = TcpStream::connect(addr);
    let mut stream = tokio::time::timeout(Duration::from_secs(1), connect).await??;
    stream
        .write_all(format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .await?;
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf)).await??;
    let text = String::from_utf8(buf)?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or(text);
    Ok(body)
}

/// Sum every sample of each `stream_match_recognize_*` counter across its labels (one series per
/// actor). Prometheus text exposition: `name{labels} value`, comments start with `#`. Values are
/// integer counters but arrive as floats (`42` or `42.0` or scientific for large counts).
pub fn parse(body: &str) -> Totals {
    parse_counted(body).0
}

/// The totals plus how many matching series were seen, so a caller can tell "all three counters
/// are genuinely zero" from "this build has no such counters".
pub fn parse_counted(body: &str) -> (Totals, usize) {
    let mut t = Totals::default();
    let mut found = 0usize;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let target = if line.starts_with("stream_match_recognize_matches_emitted_count") {
            &mut t.matches_emitted
        } else if line.starts_with("stream_match_recognize_evicted_rows_count") {
            &mut t.evicted_rows
        } else if line.starts_with("stream_match_recognize_scan_budget_exhausted_count") {
            &mut t.scan_budget_exhausted
        } else {
            continue;
        };
        // `name{...} value [timestamp]` — the value is the token after the last space-separated
        // split of everything past the label block (or past the name when there are no labels).
        let after_labels = match line.split_once('}') {
            Some((_, rest)) => rest,
            None => line.split_once(' ').map(|(_, r)| r).unwrap_or(""),
        };
        if let Some(v) = after_labels.split_whitespace().next() {
            if let Ok(f) = v.parse::<f64>() {
                if f.is_finite() && f >= 0.0 {
                    *target += f as u64;
                    found += 1;
                }
            }
        }
    }
    (t, found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sums_series_across_actors_and_ignores_the_rest() {
        let body = "\
# HELP stream_match_recognize_matches_emitted_count Matches emitted
# TYPE stream_match_recognize_matches_emitted_count counter
stream_match_recognize_matches_emitted_count{table_id=\"7\",actor_id=\"1\",fragment_id=\"3\"} 10
stream_match_recognize_matches_emitted_count{table_id=\"7\",actor_id=\"2\",fragment_id=\"3\"} 32
stream_match_recognize_evicted_rows_count{table_id=\"7\",actor_id=\"1\",fragment_id=\"3\"} 100.0
stream_match_recognize_scan_budget_exhausted_count{table_id=\"7\",actor_id=\"1\",fragment_id=\"3\"} 0
stream_exchange_frag_send_size{up_fragment_id=\"1\",down_fragment_id=\"2\"} 999999
";
        let t = parse(body);
        assert_eq!(t.matches_emitted, 42);
        assert_eq!(t.evicted_rows, 100);
        assert_eq!(t.scan_budget_exhausted, 0);
    }

    #[test]
    fn a_body_without_these_counters_is_not_three_zeros() {
        // An image predating the operator metrics, or a cluster whose actors have not started,
        // serves a valid /metrics with none of these series. That must read as "not measured".
        let body = "stream_exchange_frag_send_size{a=\"1\"} 42\n";
        assert_eq!(parse_counted(body).1, 0);
        // ... whereas a real zero counter is a measurement, and must be reported as one.
        let real_zero = "stream_match_recognize_matches_emitted_count{a=\"1\"} 0\n";
        let (t, found) = parse_counted(real_zero);
        assert_eq!(found, 1);
        assert_eq!(t.matches_emitted, 0);
    }

    #[test]
    fn tolerates_timestamps_garbage_and_empty_bodies() {
        let body = "\
stream_match_recognize_matches_emitted_count{a=\"1\"} 5 1723000000000
stream_match_recognize_matches_emitted_count{a=\"2\"} not-a-number
stream_match_recognize_evicted_rows_count{a=\"1\"} -3
";
        let t = parse(body);
        assert_eq!(t.matches_emitted, 5);
        assert_eq!(t.evicted_rows, 0);
        assert_eq!(parse(""), Totals::default());
    }
}
