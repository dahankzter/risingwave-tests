//! Wall-clock pacing for realtime workloads.
//!
//! Deliberately free of I/O so the scheduling rule is testable in milliseconds rather than by
//! running a two-minute benchmark and squinting at the result.
//!
//! `due` is a pure function of the row index. It must never accumulate, because a schedule that
//! accumulates ignores the time the inserts themselves take: it creeps ahead of the wall clock,
//! and once it leads by more than the table's watermark delay the watermark sits in the future
//! and anything inserted with `now()` is discarded as late.

use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct Pacer {
    /// Origin the current rate is measured from.
    epoch: Instant,
    /// Row index that `epoch` corresponds to.
    epoch_row: u64,
    rate: f64,
}

impl Pacer {
    pub fn new(start: Instant, rate: f64) -> Self {
        assert!(rate > 0.0, "rate must be positive");
        Self { epoch: start, epoch_row: 0, rate }
    }

    pub fn rate(&self) -> f64 {
        self.rate
    }

    /// When row `row_index` should be inserted.
    pub fn due(&self, row_index: u64) -> Instant {
        let ahead = row_index.saturating_sub(self.epoch_row);
        self.epoch + Duration::from_secs_f64(ahead as f64 / self.rate)
    }

    /// Change the rate, rebasing the schedule so rows already emitted are not re-timed.
    /// `at_row` is the next row index to be emitted and `at` is the instant it becomes the new
    /// origin — normally `Instant::now()`.
    pub fn set_rate(&mut self, at_row: u64, at: Instant, rate: f64) {
        assert!(rate > 0.0, "rate must be positive");
        self.epoch = at;
        self.epoch_row = at_row;
        self.rate = rate;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn first_row_is_due_immediately() {
        let start = Instant::now();
        let p = Pacer::new(start, 2000.0);
        assert_eq!(p.due(0), start);
    }

    #[test]
    fn row_n_is_due_at_n_over_rate() {
        let start = Instant::now();
        let p = Pacer::new(start, 2000.0);
        assert_eq!(p.due(2000), start + Duration::from_secs(1));
        assert_eq!(p.due(1000), start + Duration::from_millis(500));
    }

    /// The drift defect: `due` must be a pure function of the row index, never of how many
    /// times it has been called or how long the caller took in between. Interleaving other
    /// work must not move the schedule.
    #[test]
    fn schedule_does_not_drift_with_call_history() {
        let start = Instant::now();
        let p = Pacer::new(start, 1000.0);
        let direct = p.due(5000);
        for i in 0..5000 {
            let _ = p.due(i);
        }
        assert_eq!(p.due(5000), direct);
        assert_eq!(direct, start + Duration::from_secs(5));
    }

    #[test]
    fn set_rate_rebases_from_the_change_point() {
        let start = Instant::now();
        let mut p = Pacer::new(start, 1000.0);
        // One second in, 1000 rows done, double the rate.
        let at = start + Duration::from_secs(1);
        p.set_rate(1000, at, 2000.0);
        assert_eq!(p.rate(), 2000.0);
        // The next 2000 rows now take one second, measured from the change point.
        assert_eq!(p.due(1000), at);
        assert_eq!(p.due(3000), at + Duration::from_secs(1));
    }
}
