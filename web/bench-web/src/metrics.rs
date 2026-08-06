//! Operator-metrics scrape: the compute node's Prometheus endpoint (port 1222 inside the
//! container, published by `make up`/`compose.yaml`/the podman driver) carries the three
//! `stream_match_recognize_*` counters, one series per actor. This module fetches the text
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
/// error worth logging every 2 seconds) or the body doesn't parse.
pub async fn scrape(addr: &str) -> Option<Totals> {
    let body = fetch(addr).await.ok()?;
    Some(parse(&body))
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
    let mut t = Totals::default();
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
                }
            }
        }
    }
    t
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
