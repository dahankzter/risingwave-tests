//! Fraud-shaped workload generation: per partition, a `deposit -> bet{1..n} -> withdraw` chain.
//!
//! A completed chain matches the bench pattern `(d b+ w)`. An abandoned chain runs its bets and
//! then stops, leaving a `d b+` prefix buffered until its WITHIN bound expires — the retained
//! state regime worth measuring. Abandoning at the deposit instead would leave almost nothing
//! behind, which is what the original Python did before it was fixed.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Deposit,
    Bet,
    Withdraw,
    Noop,
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Deposit => "deposit",
            Kind::Bet => "bet",
            Kind::Withdraw => "withdraw",
            Kind::Noop => "noop",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Event {
    pub partition: i32,
    pub kind: Kind,
    pub amount: i32,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rows: u64,
    pub partitions: i32,
    pub hot_count: i32,
    pub hot_share: f64,
    pub bets_min: u32,
    pub bets_max: u32,
    pub abandon_prob: f64,
    pub payload_cols: usize,
    pub payload_bytes: usize,
    pub seed: u64,
    pub ties: u32,
    pub tick_gap: i64,
    pub sentinel_partition: i32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rows: 100_000,
            partitions: 1_000,
            hot_count: 0,
            hot_share: 0.5,
            bets_min: 1,
            bets_max: 4,
            abandon_prob: 0.2,
            payload_cols: 0,
            payload_bytes: 32,
            seed: 42,
            ties: 1,
            tick_gap: 1,
            sentinel_partition: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("--hot-count ({hot_count}) must be < --partitions ({partitions}): no cold partitions would be left")]
    HotCount { hot_count: i32, partitions: i32 },
    #[error("--partitions must be at least 1")]
    Partitions,
    #[error("--ties must be at least 1")]
    Ties,
    #[error("--bets-min ({min}) must be <= --bets-max ({max})")]
    Bets { min: u32, max: u32 },
    #[error("--hot-share and --abandon-prob must be within 0.0..=1.0")]
    Probability,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.partitions < 1 {
            return Err(ConfigError::Partitions);
        }
        if self.hot_count > 0 && self.hot_count >= self.partitions {
            return Err(ConfigError::HotCount {
                hot_count: self.hot_count,
                partitions: self.partitions,
            });
        }
        if self.ties < 1 {
            return Err(ConfigError::Ties);
        }
        if self.bets_min > self.bets_max {
            return Err(ConfigError::Bets { min: self.bets_min, max: self.bets_max });
        }
        if !(0.0..=1.0).contains(&self.hot_share) || !(0.0..=1.0).contains(&self.abandon_prob) {
            return Err(ConfigError::Probability);
        }
        Ok(())
    }
}

/// Mid-chain state for one partition: bets still to emit, and whether it ever completes.
#[derive(Debug, Clone, Copy)]
struct Chain {
    bets_left: u32,
    abandoned: bool,
}

#[derive(Debug)]
pub struct Generator {
    cfg: Config,
    rng: ChaCha8Rng,
    chains: HashMap<i32, Chain>,
}

impl Generator {
    pub fn new(cfg: Config) -> Result<Self, ConfigError> {
        cfg.validate()?;
        let rng = ChaCha8Rng::seed_from_u64(cfg.seed);
        Ok(Self { cfg, rng, chains: HashMap::new() })
    }

    pub fn config(&self) -> &Config {
        &self.cfg
    }

    /// Chains holding a buffered `d b+` prefix with no withdraw yet.
    pub fn open_chains(&self) -> usize {
        self.chains.len()
    }

    fn pick_partition(&mut self) -> i32 {
        if self.cfg.hot_count > 0 && self.rng.gen::<f64>() < self.cfg.hot_share {
            self.rng.gen_range(1..=self.cfg.hot_count)
        } else {
            self.rng.gen_range(self.cfg.hot_count + 1..=self.cfg.partitions)
        }
    }

    pub fn next_event(&mut self) -> Event {
        let partition = self.pick_partition();
        self.event_for(partition)
    }

    fn event_for(&mut self, partition: i32) -> Event {
        match self.chains.get(&partition).copied() {
            None => {
                let bets = self.rng.gen_range(self.cfg.bets_min..=self.cfg.bets_max);
                let abandoned = self.rng.gen::<f64>() < self.cfg.abandon_prob;
                self.chains.insert(partition, Chain { bets_left: bets, abandoned });
                Event { partition, kind: Kind::Deposit, amount: self.rng.gen_range(50..500) }
            }
            Some(c) if c.bets_left > 0 => {
                self.chains.insert(
                    partition,
                    Chain { bets_left: c.bets_left - 1, ..c },
                );
                Event { partition, kind: Kind::Bet, amount: self.rng.gen_range(5..50) }
            }
            Some(c) => {
                self.chains.remove(&partition);
                if c.abandoned {
                    // No withdraw: the prefix stays buffered and this partition starts afresh.
                    self.event_for(partition)
                } else {
                    Event { partition, kind: Kind::Withdraw, amount: self.rng.gen_range(40..450) }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Config {
        Config { partitions: 2, bets_min: 2, bets_max: 2, ..Config::default() }
    }

    /// An abandoned chain runs its bets and then stops. It must never emit a withdraw, and the
    /// partition's next event must open a fresh chain — that buffered `d b+` prefix is the
    /// retained state the WITHIN bound is there to bound.
    #[test]
    fn abandoned_chains_never_withdraw() {
        let mut g = Generator::new(Config { abandon_prob: 1.0, ..cfg() }).unwrap();
        let kinds: Vec<Kind> = (0..40).map(|_| g.next_event().kind).collect();
        assert!(!kinds.contains(&Kind::Withdraw), "abandoned chains must not withdraw");
        assert!(kinds.contains(&Kind::Bet), "an abandoned chain must still run its bets");
        assert!(kinds.iter().filter(|k| **k == Kind::Deposit).count() > 1,
                "a new chain must open after an abandoned one");
    }

    #[test]
    fn completed_chains_are_deposit_bets_withdraw() {
        let mut g = Generator::new(Config { abandon_prob: 0.0, partitions: 1, ..cfg() }).unwrap();
        let kinds: Vec<Kind> = (0..4).map(|_| g.next_event().kind).collect();
        assert_eq!(kinds, vec![Kind::Deposit, Kind::Bet, Kind::Bet, Kind::Withdraw]);
    }

    #[test]
    fn hot_count_must_leave_cold_partitions() {
        let err = Generator::new(Config { partitions: 10, hot_count: 10, ..Config::default() })
            .unwrap_err();
        assert!(matches!(err, ConfigError::HotCount { .. }), "got {err:?}");
    }

    #[test]
    fn hot_partitions_take_their_share() {
        let cfg = Config {
            rows: 10_000, partitions: 1000, hot_count: 10, hot_share: 0.9,
            ..Config::default()
        };
        let mut g = Generator::new(cfg).unwrap();
        let hot = (0..10_000).filter(|_| g.next_event().partition <= 10).count();
        assert!((8500..9500).contains(&hot), "hot share out of range: {hot}");
    }

    #[test]
    fn same_seed_gives_the_same_stream() {
        let mk = || {
            let mut g = Generator::new(cfg()).unwrap();
            (0..200).map(|_| { let e = g.next_event(); (e.partition, e.amount) }).collect::<Vec<_>>()
        };
        assert_eq!(mk(), mk());
    }
}
