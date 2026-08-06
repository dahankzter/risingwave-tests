//! `GET /ws`: fan-out from `AppState::tx` to the browser, plus the background aggregator that
//! keeps exact percentiles and emits the periodic `Rate`/`Stats`/`Metrics` ticks.
//!
//! Two independent subscribers hang off the same `broadcast::Sender<Event>` (`AppState::tx`) —
//! no second channel:
//!
//! - [`spawn_aggregator`] sees *every* event, in particular every `Event::Alert`, unsampled. It
//!   feeds each alert's latency into a `Latencies` so the percentiles it publishes on the 250ms
//!   tick cover the whole run, and records each alert into `AppState`'s ring buffer for late
//!   joiners. Sampling before measuring would make every number on the page quietly wrong — this
//!   is the one property in this module that must not be gotten backwards.
//! - [`handle_socket`], one per connected client, forwards most events verbatim but thins
//!   `Event::Alert` through a private [`Sampler`] so a browser at the far end of a 2000 rows/s
//!   load isn't asked to render ~500 alerts/s.
//!
//! A client is sent an `Event::Snapshot` first (current status, the last 50 alerts, current
//! stats) so a page opened or refreshed mid-run isn't blank until the next tick, and again on
//! `RecvError::Lagged` so a slow client resyncs instead of the producer stalling for it or the
//! socket being closed out from under it.

use crate::event::Event;
use crate::state::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use bench_core::measure::{Latencies, RateWindow};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Forwarded alerts per second of wall-clock time. Well above what a human eye needs (a browser
/// re-rendering a list faster than ~20/s buys nothing) and well below the ~500/s a 2000 rows/s
/// load produces, so the thinning is easy to see in a live capture.
const DISPLAY_ALERTS_PER_SEC: f64 = 20.0;
/// `Rate`/`Stats` tick period.
const AGG_TICK: Duration = Duration::from_millis(250);
/// `Metrics` tick period. No source exists yet (Task 7 wires `bench-core`'s scan-budget/eviction
/// counters through); the tick fires on schedule but publishes nothing until then, per the plan
/// — fabricating zeros would be worse than staying silent.
const METRICS_TICK: Duration = Duration::from_secs(2);
/// Window `RateWindow` averages `rows_per_sec_in` / `alerts_per_sec_out` over. Short enough that
/// the console's rate numbers track a rate change (e.g. `POST /api/load/rate`) within a couple
/// of seconds rather than smoothing it away.
const RATE_WINDOW: Duration = Duration::from_secs(2);

pub fn router() -> Router<Arc<AppState>> {
    Router::new().route("/ws", get(ws_handler))
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Serialises `ev` and sends it as a text frame. The one place a send error is turned into "stop
/// serving this client" — a closed/broken socket, not a reason to touch shared state.
async fn send_event(socket: &mut WebSocket, ev: &Event) -> bool {
    let text = serde_json::to_string(ev).expect("Event always serialises");
    socket.send(Message::Text(text.into())).await.is_ok()
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    if !send_event(&mut socket, &state.snapshot_event()).await {
        return;
    }

    let mut rx = state.tx.subscribe();
    let mut sampler = Sampler::new(DISPLAY_ALERTS_PER_SEC);
    let start = Instant::now();

    loop {
        match rx.recv().await {
            Ok(Event::Alert { .. }) if !sampler.should_forward(start.elapsed().as_secs_f64()) => {
                // Thinned for display; the aggregator already measured this one.
            }
            Ok(ev) => {
                if !send_event(&mut socket, &ev).await {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {
                // This client fell behind the producer. Rather than close the socket (or block
                // the producer waiting for it), resync it with a fresh snapshot and keep going —
                // the next events it sees will be current even though some were skipped.
                if !send_event(&mut socket, &state.snapshot_event()).await {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// Spawns the background aggregator: the one subscriber that sees every event unsampled, keeps
/// the running `Latencies`, and emits `Rate`/`Stats` on `AGG_TICK` and (once there is a source)
/// `Metrics` on `METRICS_TICK`. Runs for the lifetime of the process, same as `spawn_reader`.
pub fn spawn_aggregator(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(aggregator_loop(state))
}

async fn aggregator_loop(state: Arc<AppState>) {
    let mut rx = state.tx.subscribe();
    let mut latencies = Latencies::new();
    let mut rows_in = RateWindow::new(RATE_WINDOW);
    let mut alerts_out = RateWindow::new(RATE_WINDOW);
    let mut last_rows_sent: u64 = 0;

    let mut agg_tick = tokio::time::interval(AGG_TICK);
    let mut metrics_tick = tokio::time::interval(METRICS_TICK);

    loop {
        tokio::select! {
            ev = rx.recv() => match ev {
                Ok(event @ Event::Alert { latency_ms, .. }) => {
                    // Every alert reaches the percentiles and the ring buffer, unconditionally —
                    // this subscriber is never subject to the display Sampler.
                    latencies.push(latency_ms);
                    alerts_out.record(Instant::now(), 1);
                    state.record_alert(event);
                }
                Ok(_) => {}
                // The aggregator lagging just means some events were skipped this tick; it has
                // no client-facing socket to resync, so it simply keeps accumulating from here.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            },
            _ = agg_tick.tick() => {
                let (rows_sent, rate_requested) = current_progress(&state).await;
                rows_in.record(Instant::now(), rows_sent.saturating_sub(last_rows_sent));
                last_rows_sent = rows_sent;

                let now = Instant::now();
                state.publish(Event::Rate {
                    rows_per_sec_in: rows_in.per_sec(now),
                    rows_per_sec_requested: rate_requested,
                    alerts_per_sec_out: alerts_out.per_sec(now),
                });

                if let Some(p) = latencies.percentiles() {
                    let stats = Event::Stats {
                        n: p.n,
                        min_ms: p.min_ms,
                        p50_ms: p.p50_ms,
                        p95_ms: p.p95_ms,
                        p99_ms: p.p99_ms,
                        max_ms: p.max_ms,
                    };
                    state.set_last_stats(stats.clone());
                    state.publish(stats);
                }
            }
            _ = metrics_tick.tick() => {
                // No source yet — see METRICS_TICK's doc comment. The tick stays wired so
                // Task 7 only has to add the publish, not the scaffolding.
            }
        }
    }
}

/// `(rows_sent, rate_requested)` off the current run's progress, or `(carry-forward, 0.0)` when
/// no load is running — `rows_in`'s delta against the last observed `rows_sent` then correctly
/// reads as zero instead of spuriously spiking once a load starts.
async fn current_progress(state: &AppState) -> (u64, f64) {
    let guard = state.run.lock().await;
    match guard.as_ref() {
        Some(handle) => {
            let p = handle.progress().borrow().clone();
            (p.rows_sent, p.rate_requested)
        }
        None => (0, 0.0),
    }
}

/// Forwards at most `target` items per second of (simulated or real) elapsed time, via a token
/// bucket.
///
/// The burst capacity is one *target-second's* worth of tokens, not a single token. That matters
/// because the real feed is not evenly paced: `stream.rs`'s reader sends a whole fetched batch
/// (up to 100 rows) back-to-back with no per-row delay, then goes quiet until the next fetch. A
/// one-token cap means only the *first* item of every batch can ever be forwarded — the rest of
/// that same batch arrives at ~0 elapsed time and finds an empty bucket — which caps the
/// sustained forwarded rate at the batch frequency (observed ~1-2/s against a 2000 rows/s load),
/// not anywhere near the 20/s target. A bucket that can bank up to a second's worth of tokens
/// during the quiet gap between batches can then spend that bank across the next batch, so the
/// *average* forwarded rate tracks `target` — a token bucket's average throughput is bounded by
/// its refill rate regardless of capacity; capacity only shapes burst behaviour, which is exactly
/// the knob this needed. `sampler_forwards_everything_when_the_rate_is_below_the_target` still
/// holds: a feed slower than `target` never drains the bucket below one token by the next call.
#[derive(Debug)]
pub struct Sampler {
    target_per_sec: f64,
    capacity: f64,
    tokens: f64,
    last_t: Option<f64>,
}

impl Sampler {
    pub fn new(target_per_sec: f64) -> Self {
        let capacity = target_per_sec.max(1.0);
        Self { target_per_sec, capacity, tokens: 1.0, last_t: None }
    }

    /// `t` is seconds since some fixed origin — simulated in tests, `Instant::now()`-derived in
    /// `handle_socket`. Must be non-decreasing across calls (a caller passing real elapsed time
    /// naturally satisfies that).
    pub fn should_forward(&mut self, t: f64) -> bool {
        if let Some(last) = self.last_t {
            let dt = (t - last).max(0.0);
            self.tokens = (self.tokens + dt * self.target_per_sec).min(self.capacity);
        }
        self.last_t = Some(t);
        // A small epsilon absorbs float error from repeated `dt * target_per_sec` additions
        // (e.g. pacing exactly at the target rate can leave `tokens` at 0.999999999996 instead
        // of 1.0) without meaningfully loosening the burst cap.
        if self.tokens >= 1.0 - 1e-9 {
            self.tokens = (self.tokens - 1.0).max(0.0);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// At 2000 rows/s the alert rate is roughly 500/s — far more than a browser should render.
    /// Every alert must still reach the percentiles; only the display feed is thinned.
    #[test]
    fn sampler_thins_to_roughly_the_target_rate() {
        let mut s = Sampler::new(20.0); // 20 forwarded per second
        let mut forwarded = 0;
        // 500 alerts arriving over one simulated second
        for i in 0..500 {
            if s.should_forward(i as f64 / 500.0) {
                forwarded += 1;
            }
        }
        assert!((15..=25).contains(&forwarded), "forwarded {forwarded}, want ~20");
    }

    #[test]
    fn sampler_forwards_everything_when_the_rate_is_below_the_target() {
        let mut s = Sampler::new(20.0);
        let mut forwarded = 0;
        for i in 0..5 {
            if s.should_forward(i as f64 / 5.0) {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 5, "a slow feed must not be thinned at all");
    }

    /// A burst right after a long quiet spell must not be let through unbounded — the bank is
    /// capped at one target-second's worth of tokens (here 20), not the 200 that ten idle
    /// seconds would otherwise accrue, and not the 100 the burst itself contains.
    #[test]
    fn burst_after_a_quiet_period_is_capped_at_one_seconds_worth_of_tokens() {
        let mut s = Sampler::new(20.0);
        assert!(s.should_forward(0.0), "first call always has its single starting token");
        // Ten seconds of silence would, without a burst cap, bank 200 tokens.
        let mut forwarded = 0;
        // A burst of 100 items arriving essentially at once (10s later, ~0 time between them) —
        // this is the real shape of the alert reader's delivery: a whole fetched batch sent
        // back-to-back, then quiet until the next fetch.
        for i in 0..100 {
            if s.should_forward(10.0 + i as f64 * 1e-6) {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 20, "capacity is one target-second's worth of tokens (20 at this target)");
    }

    /// Arrivals paced at exactly the target rate should all get through — the boundary case
    /// between "below target" (forward everything) and "above target" (thin).
    #[test]
    fn rate_exactly_at_the_target_forwards_everything() {
        let mut s = Sampler::new(20.0);
        let mut forwarded = 0;
        // Exactly 20 arrivals spread evenly across one second: one token regenerates just in
        // time for each one.
        for i in 0..20 {
            if s.should_forward(i as f64 / 20.0) {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 20, "arrivals paced at exactly the target must not be thinned");
    }

    /// No calls at all must not panic and must leave a fresh sampler ready to forward its first
    /// item whenever one does arrive.
    #[test]
    fn zero_alerts_is_a_no_op() {
        let mut s = Sampler::new(20.0);
        assert!(s.should_forward(0.0), "an otherwise-idle sampler still forwards the first item");
    }
}
