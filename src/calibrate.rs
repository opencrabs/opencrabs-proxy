//! Per-caller correction for the input estimate.
//!
//! [`crate::limit::estimate_tokens`] charges text at a flat `characters / 4`.
//! Measured against live upstream usage that constant is wrong in both
//! directions: dense JSON and code cost about 2.9 characters per token, plain
//! prose about 5.3. A caller sending agent traffic therefore estimates low, and
//! its true input runs well past the ceiling the estimate reported.
//!
//! No single constant fixes that, because density is a property of what a
//! caller sends. So the factor is measured instead of assumed: every served
//! request carries both the estimate and the upstream's `prompt_tokens`, and
//! the pair gives the density `actual / estimated` for that caller.
//!
//! The factor is clamped to `[1.0, 3.0]`. The lower bound is deliberate: a
//! factor below 1.0 would multiply the estimate down and loosen the gate, so a
//! caller could send sparse traffic to earn a discount and then send one dense
//! body past the ceiling. A correctly configured client compacts at 65% of its
//! window and never needs the looser bound.

use std::collections::HashMap;
use std::sync::Mutex;

/// Observations a caller needs before its own density is trusted.
pub const MIN_SAMPLES: u32 = 20;
/// Smallest estimate that counts as density evidence.
///
/// The upstream's `prompt_tokens` carries a fixed overhead (the system prompt
/// and message envelope, about 24 tokens) that the flat estimate does not
/// model, so a reading is really `density + overhead / estimated`. On a body
/// the estimate puts at 2 tokens that overhead swamps the signal: measured
/// against live traffic, estimates under 50 read 15x on average and up to 22x,
/// while estimates from 200 up read 1.03. Folding those readings in moved a
/// healthy caller's factor from 1.29 to 2.88 in two requests, which made the
/// input gate refuse a 114k-token body as 273k.
///
/// 512 keeps the bias from the overhead under 5% of a 1.2 density factor,
/// which is well inside the noise the factor already tolerates. A caller that
/// only ever sends tiny requests simply stays uncalibrated, which is the
/// pre-calibration behaviour of a 1.0 factor and the safe direction.
pub const MIN_OBSERVATION_ESTIMATE: u64 = 512;
/// Weight of each new observation in the running mean.
const ALPHA: f64 = 0.1;
/// The factor never drops below this, so the gate can only ever tighten.
pub const MIN_FACTOR: f64 = 1.0;
/// Ceiling on the factor, so one pathological request cannot lock a caller out.
pub const MAX_FACTOR: f64 = 3.0;
/// Key for the factor shared by every caller, used until a caller has its own.
pub const GLOBAL: &str = "*";

/// A running mean of one caller's density.
#[derive(Debug, Clone, Copy)]
struct Ema {
    ratio: f64,
    samples: u32,
}

impl Ema {
    /// Fold one `actual / estimated` reading in. The first reading sets the
    /// value outright, so a caller does not have to climb from a default.
    fn observe(&mut self, ratio: f64) {
        self.ratio = if self.samples == 0 {
            ratio
        } else {
            self.ratio * (1.0 - ALPHA) + ratio * ALPHA
        };
        self.samples += 1;
    }
}

impl Default for Ema {
    /// A caller with no observations, which [`Calibration::factor`] treats as
    /// "not yet trusted".
    fn default() -> Self {
        Self {
            ratio: MIN_FACTOR,
            samples: 0,
        }
    }
}

/// Density factors by caller label, plus one shared factor.
#[derive(Debug, Default)]
pub struct Calibration {
    inner: Mutex<HashMap<String, Ema>>,
}

impl Calibration {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one served request into the caller's density and the shared one.
    /// A body with no estimate, or an upstream that reported no usage, tells
    /// us nothing and is skipped rather than counted as a zero. So is a body
    /// whose estimate is too small for the upstream's fixed overhead to be
    /// negligible, see [`MIN_OBSERVATION_ESTIMATE`].
    pub fn observe(&self, label: &str, estimated: u64, actual: u64) {
        if estimated < MIN_OBSERVATION_ESTIMATE || actual == 0 {
            return;
        }
        let ratio = actual as f64 / estimated as f64;
        let mut inner = self.inner.lock().unwrap();
        inner.entry(label.to_string()).or_default().observe(ratio);
        inner.entry(GLOBAL.to_string()).or_default().observe(ratio);
    }

    /// The factor to multiply a caller's estimate by. A caller with too few
    /// observations borrows the shared factor, and until that has enough
    /// observations of its own the answer is `1.0`, which is exactly the
    /// behaviour the proxy had before calibration existed.
    pub fn factor(&self, label: &str) -> f64 {
        let inner = self.inner.lock().unwrap();
        let chosen = inner
            .get(label)
            .filter(|e| e.samples >= MIN_SAMPLES)
            .or_else(|| inner.get(GLOBAL).filter(|e| e.samples >= MIN_SAMPLES));
        match chosen {
            Some(e) => clamp(e.ratio),
            None => 1.0,
        }
    }

    /// Warm the factors from stored history at startup, so a restart does not
    /// cost a caller its calibration. Rows are `(label, mean_ratio, count)`.
    pub fn seed(&self, rows: &[(String, f64, u32)]) {
        let mut inner = self.inner.lock().unwrap();
        for (label, ratio, count) in rows {
            if *count == 0 || !ratio.is_finite() {
                continue;
            }
            inner.insert(
                label.clone(),
                Ema {
                    ratio: *ratio,
                    samples: *count,
                },
            );
        }
    }

    /// Every caller with its factor, for the startup log.
    pub fn snapshot(&self) -> Vec<(String, f64, u32)> {
        let inner = self.inner.lock().unwrap();
        let mut out: Vec<(String, f64, u32)> = inner
            .iter()
            .map(|(k, e)| (k.clone(), clamp(e.ratio), e.samples))
            .collect();
        out.sort_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        out
    }
}

/// Keep a factor inside the band that can only tighten the gate.
fn clamp(ratio: f64) -> f64 {
    if !ratio.is_finite() {
        return MIN_FACTOR;
    }
    ratio.clamp(MIN_FACTOR, MAX_FACTOR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_caller_with_enough_dense_samples_gets_its_own_factor() {
        let c = Calibration::new();
        // 1.37 is the density measured from core-dev-2's live traffic.
        for _ in 0..MIN_SAMPLES {
            c.observe("dense", 100_000, 137_000);
        }
        let f = c.factor("dense");
        assert!((f - 1.37).abs() < 0.01, "got {f}");
    }

    #[test]
    fn one_observation_short_of_the_minimum_is_not_trusted() {
        let c = Calibration::new();
        for _ in 0..(MIN_SAMPLES - 1) {
            c.observe("dense", 100_000, 137_000);
        }
        // The shared factor has the same count, so it is not trusted either.
        assert_eq!(c.factor("dense"), 1.0);
    }

    #[test]
    fn an_unknown_caller_borrows_the_shared_factor() {
        let c = Calibration::new();
        for _ in 0..MIN_SAMPLES {
            c.observe("dense", 100_000, 137_000);
        }
        // A caller the proxy has never seen still benefits from the shared
        // reading, because the density is a property of the model's content.
        assert!((c.factor("brand-new") - 1.37).abs() < 0.01);
    }

    #[test]
    fn a_caller_with_its_own_history_ignores_the_shared_factor() {
        let c = Calibration::new();
        for _ in 0..(MIN_SAMPLES * 2) {
            c.observe("prose", 100_000, 53_000);
        }
        for _ in 0..MIN_SAMPLES {
            c.observe("dense", 100_000, 137_000);
        }
        // Prose reads below 1.0, which the clamp lifts to 1.0 rather than
        // handing prose a discount the gate would then have to trust.
        assert_eq!(c.factor("prose"), MIN_FACTOR);
        assert!((c.factor("dense") - 1.37).abs() < 0.01);
    }

    #[test]
    fn the_factor_never_drops_below_one() {
        let c = Calibration::new();
        // A measured density of 0.5, which would hand the caller a discount
        // if it were applied. The clamp refuses it.
        for _ in 0..MIN_SAMPLES {
            c.observe("prose", 100_000, 50_000);
        }
        assert_eq!(c.factor("prose"), 1.0);
        assert!(
            c.snapshot().iter().all(|(_, f, _)| *f >= MIN_FACTOR),
            "the snapshot clamps too"
        );
    }

    #[test]
    fn the_factor_is_capped() {
        let c = Calibration::new();
        for _ in 0..MIN_SAMPLES {
            c.observe("pathological", 100_000, 900_000);
        }
        assert_eq!(c.factor("pathological"), MAX_FACTOR);
    }

    #[test]
    fn a_body_with_no_estimate_is_skipped_not_counted_as_zero() {
        let c = Calibration::new();
        c.observe("quiet", 0, 137_000);
        c.observe("quiet", 100_000, 0);
        assert!(c.snapshot().is_empty(), "nothing should have been learned");
        assert_eq!(c.factor("quiet"), 1.0);
    }

    #[test]
    fn seeded_history_survives_a_restart() {
        let c = Calibration::new();
        c.seed(&[("dense".to_string(), 1.37, 500)]);
        assert!((c.factor("dense") - 1.37).abs() < 0.001);
    }

    #[test]
    fn a_later_observation_moves_the_factor() {
        let c = Calibration::new();
        for _ in 0..MIN_SAMPLES {
            c.observe("drifting", 100_000, 100_000);
        }
        let before = c.factor("drifting");
        for _ in 0..(MIN_SAMPLES * 5) {
            c.observe("drifting", 100_000, 200_000);
        }
        let after = c.factor("drifting");
        assert!(after > before, "{before} -> {after}");
        assert!(after < 2.0, "it must converge, not jump: {after}");
    }

    #[test]
    fn a_tiny_body_teaches_nothing_about_density() {
        let c = Calibration::new();
        // The upstream charges ~24 prompt tokens for any request, so a body
        // the estimate puts at 2 tokens reads as 12x. That is the overhead,
        // not the density, and it must not be learned.
        for _ in 0..100 {
            c.observe("tiny", 2, 24);
        }
        assert!(
            c.snapshot().is_empty(),
            "a body under the threshold must not be observed at all"
        );
        assert_eq!(c.factor("tiny"), 1.0);
    }

    #[test]
    fn a_body_just_under_the_threshold_is_ignored() {
        let c = Calibration::new();
        c.observe("edge", MIN_OBSERVATION_ESTIMATE - 1, 100_000);
        assert!(c.snapshot().is_empty());
    }

    #[test]
    fn a_body_at_the_threshold_is_observed() {
        let c = Calibration::new();
        c.observe("edge", MIN_OBSERVATION_ESTIMATE, 1_000);
        assert_eq!(c.snapshot().len(), 2, "the caller and the shared factor");
    }

    /// Regression for 22 Oct 2026. Two of the proxy's own `diz OK` probes
    /// (estimated 2, actual 24) moved a healthy caller's factor from 1.29 to
    /// 2.88, and the input gate then refused a 114k-token body as 273k, which
    /// surfaced as a `413 input_too_large` on a session that was nowhere near
    /// the ceiling. A calibrated caller must survive a tiny request.
    #[test]
    fn a_tiny_request_cannot_poison_a_calibrated_caller() {
        let c = Calibration::new();
        c.seed(&[("agent".to_string(), 1.294, 3_605)]);
        let before = c.factor("agent");
        assert!((before - 1.294).abs() < 0.001, "seeded {before}");

        c.observe("agent", 2, 24);
        c.observe("agent", 2, 24);

        let after = c.factor("agent");
        assert!(
            (after - before).abs() < 0.001,
            "tiny requests moved the factor: {before} -> {after}"
        );
    }
}
