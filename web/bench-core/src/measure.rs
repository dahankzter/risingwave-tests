//! Latency and throughput summarisation.
//!
//! Pure: no clock of its own beyond the instants callers pass in, and no I/O. The UI's percentiles
//! are computed here rather than by re-querying latency/report.sql on a timer.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Percentiles {
    pub n: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

/// Every latency sample seen this run. The UI's feed is sampled for display, but percentiles
/// must cover all of them — a percentile over a display sample is not a percentile.
///
/// A real consumer must serialise writers behind a `Mutex` regardless (`push` takes `&mut
/// self`), so `percentiles` also takes `&mut self` through that same guard rather than reaching
/// for interior mutability — an `Arc<Mutex<Latencies>>` already gives exclusive access, so a
/// `RefCell` inside it would just add a redundant runtime-borrow-panic path for nothing.
///
/// Sorting is lazy: `push` appends and sets `dirty`; `percentiles` sorts the single vector in
/// place (only when dirty) and clears the flag, so a burst of pushes between two `percentiles()`
/// calls pays for one sort, not one per push, and there is no second copy of the samples sitting
/// around.
#[derive(Debug, Default)]
pub struct Latencies {
    samples: Vec<f64>,
    dirty: bool,
    rejected: usize,
}

impl Latencies {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a latency sample. Non-finite values (NaN, +-inf) are rejected rather than
    /// stored: `sort_by` with `partial_cmp` is not a total order over NaN, so a single bad
    /// sample could leave the vector genuinely unsorted and silently corrupt min, max, and every
    /// percentile. Rejected samples are counted, not just dropped, so the condition is visible.
    pub fn push(&mut self, ms: f64) {
        if !ms.is_finite() {
            self.rejected += 1;
            return;
        }
        self.samples.push(ms);
        self.dirty = true;
    }

    /// Count of samples rejected by `push` for being non-finite.
    pub fn rejected(&self) -> usize {
        self.rejected
    }

    pub fn percentiles(&mut self) -> Option<Percentiles> {
        if self.samples.is_empty() {
            return None;
        }
        if self.dirty {
            self.samples
                .sort_by(|a, b| a.partial_cmp(b).expect("non-finite values are rejected by push"));
            self.dirty = false;
        }
        let v = &self.samples;
        let pick = |q: f64| -> f64 {
            let idx = ((v.len() - 1) as f64 * q).round() as usize;
            v[idx]
        };
        Some(Percentiles {
            n: v.len(),
            min_ms: v[0],
            p50_ms: pick(0.50),
            p95_ms: pick(0.95),
            p99_ms: pick(0.99),
            max_ms: v[v.len() - 1],
        })
    }
}

/// Rows per second over a sliding window.
#[derive(Debug)]
pub struct RateWindow {
    window: Duration,
    samples: VecDeque<(Instant, u64)>,
}

impl RateWindow {
    pub fn new(window: Duration) -> Self {
        Self { window, samples: VecDeque::new() }
    }

    /// Records a sample and prunes anything older than twice the window, so `samples` cannot
    /// grow without bound over a long-running benchmark.
    pub fn record(&mut self, at: Instant, n: u64) {
        self.samples.push_back((at, n));
        let stale_before = at.checked_sub(self.window * 2);
        if let Some(cutoff) = stale_before {
            while let Some((t, _)) = self.samples.front() {
                if *t < cutoff {
                    self.samples.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    /// Rows per second, averaged over the configured window. This is deliberately a
    /// full-window average, not a "divide by elapsed span" rate: at the very start of a run
    /// (elapsed < window) it under-reports and ramps up to the true rate as samples fill the
    /// window. That's the intended reading for a UI counter — dividing by the shorter elapsed
    /// span instead would make the number jump around noisily on the first few ticks (e.g. one
    /// sample recorded 10ms after start would read as an enormous rate). Ramping in is the
    /// less surprising failure mode for a display value.
    pub fn per_sec(&self, now: Instant) -> f64 {
        let cutoff = now.checked_sub(self.window).unwrap_or(now);
        let total: u64 = self
            .samples
            .iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, n)| n)
            .sum();
        total as f64 / self.window.as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn percentiles_over_a_known_distribution() {
        let mut l = Latencies::new();
        for i in 1..=100 {
            l.push(i as f64);
        }
        let p = l.percentiles().unwrap();
        assert_eq!(p.n, 100);
        assert_eq!(p.min_ms, 1.0);
        assert_eq!(p.max_ms, 100.0);
        assert!((p.p50_ms - 50.0).abs() <= 1.0, "p50 was {}", p.p50_ms);
        assert!((p.p95_ms - 95.0).abs() <= 1.0, "p95 was {}", p.p95_ms);
    }

    #[test]
    fn percentiles_are_none_until_there_is_data() {
        assert!(Latencies::new().percentiles().is_none());
    }

    #[test]
    fn a_single_sample_is_every_percentile() {
        let mut l = Latencies::new();
        l.push(7.0);
        let p = l.percentiles().unwrap();
        assert_eq!((p.min_ms, p.p50_ms, p.p99_ms, p.max_ms), (7.0, 7.0, 7.0, 7.0));
    }

    #[test]
    fn rate_window_reports_per_second_over_its_window() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(Duration::from_secs(2));
        w.record(t0, 1000);
        w.record(t0 + Duration::from_secs(1), 1000);
        // 2000 rows across a 2s window
        let r = w.per_sec(t0 + Duration::from_secs(2));
        assert!((r - 1000.0).abs() < 50.0, "rate was {r}");
    }

    #[test]
    fn rate_window_forgets_samples_older_than_the_window() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(Duration::from_secs(2));
        w.record(t0, 10_000);
        // Ten seconds later that sample is long outside the window.
        assert_eq!(w.per_sec(t0 + Duration::from_secs(10)), 0.0);
    }

    #[test]
    fn rate_window_prunes_stale_samples_instead_of_growing_forever() {
        let t0 = Instant::now();
        let mut w = RateWindow::new(Duration::from_millis(10));
        // Each record is far enough past the previous one (> 2x window) that every earlier
        // sample should be pruned on insert, so `samples` never grows past a handful of
        // entries no matter how many records we make.
        for i in 0..10_000u64 {
            w.record(t0 + Duration::from_millis(i * 100), 1);
        }
        assert!(
            w.samples.len() < 10,
            "samples should be pruned, has {} entries",
            w.samples.len()
        );
    }

    #[test]
    fn percentiles_are_exact_after_many_pushes_compared_to_a_naive_sort() {
        // A simple xorshift-ish PRNG so the test has no external dependency and is
        // deterministic across runs.
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % 100_000) as f64 / 100.0
        };

        let mut l = Latencies::new();
        let mut raw = Vec::new();
        for _ in 0..5_000 {
            let v = next();
            l.push(v);
            raw.push(v);
        }
        // Push in a second batch after already having computed percentiles once, to exercise
        // the dirty-flag re-sort path rather than only the first-call path.
        let _ = l.percentiles();
        for _ in 0..5_000 {
            let v = next();
            l.push(v);
            raw.push(v);
        }

        let got = l.percentiles().unwrap();

        raw.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let pick = |q: f64| -> f64 {
            let idx = ((raw.len() - 1) as f64 * q).round() as usize;
            raw[idx]
        };
        assert_eq!(got.n, raw.len());
        assert_eq!(got.min_ms, raw[0]);
        assert_eq!(got.max_ms, raw[raw.len() - 1]);
        assert_eq!(got.p50_ms, pick(0.50));
        assert_eq!(got.p95_ms, pick(0.95));
        assert_eq!(got.p99_ms, pick(0.99));
    }

    #[test]
    fn nan_and_infinity_are_rejected_and_do_not_affect_percentiles() {
        let mut l = Latencies::new();
        for i in 1..=100 {
            l.push(i as f64);
        }
        l.push(f64::NAN);
        l.push(f64::INFINITY);

        let p = l.percentiles().unwrap();
        assert_eq!(p.n, 100, "rejected samples must not be counted");
        assert_eq!(p.min_ms, 1.0);
        assert_eq!(p.max_ms, 100.0);
        assert!((p.p50_ms - 50.0).abs() <= 1.0, "p50 was {}", p.p50_ms);
        assert!((p.p95_ms - 95.0).abs() <= 1.0, "p95 was {}", p.p95_ms);
        assert_eq!(l.rejected(), 2);
    }
}
