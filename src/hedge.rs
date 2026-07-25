use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::Config;

pub struct HedgeGate {
    enabled: bool,
    max_percent: u64,
    primaries: AtomicU64,
    hedges: AtomicU64,
}

impl HedgeGate {
    pub fn new(config: &Config) -> Self {
        Self {
            enabled: config.hedging.enabled,
            max_percent: u64::from(config.hedging.max_percent),
            primaries: AtomicU64::new(0),
            hedges: AtomicU64::new(0),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn record_primary(&self) {
        self.primaries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn try_acquire(&self) -> bool {
        if !self.enabled {
            return false;
        }
        let primaries = self.primaries.load(Ordering::Relaxed);
        let mut hedges = self.hedges.load(Ordering::Relaxed);
        loop {
            let allowed = u128::from(hedges.saturating_add(1)) * 100
                <= u128::from(primaries) * u128::from(self.max_percent);
            if !allowed {
                return false;
            }
            match self.hedges.compare_exchange_weak(
                hedges,
                hedges.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => hedges = actual,
            }
        }
    }

    pub fn counts(&self) -> (u64, u64) {
        (
            self.primaries.load(Ordering::Relaxed),
            self.hedges.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn global_budget_never_exceeds_ten_percent_of_primaries() {
        let gate = HedgeGate::new(&Config::default());
        for _ in 0..9 {
            gate.record_primary();
            assert!(!gate.try_acquire());
        }
        gate.record_primary();
        assert!(gate.try_acquire());
        assert!(!gate.try_acquire());
        for _ in 0..90 {
            gate.record_primary();
        }
        for _ in 0..9 {
            assert!(gate.try_acquire());
        }
        assert!(!gate.try_acquire());
        assert_eq!(gate.counts(), (100, 10));
    }
}
