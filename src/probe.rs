//! Background upstream health probe.
//!
//! The request table only shows failures that reached the proxy, so it cannot
//! answer the question that matters during an outage: is the upstream itself
//! broken, or is one configured candidate broken while a healthy one sits
//! unused next to it? Both look identical from a caller's side.
//!
//! This module asks the upstream directly, on a timer, once per configured
//! candidate. Each answer is one row in the `probes` table, which gives the
//! alert a per-candidate verdict with a timestamp, and gives `/stats` a state
//! that is measured rather than inferred.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use crate::AppState;

/// Probe every five minutes unless told otherwise. Frequent enough that an
/// alert states a live fact, cheap enough to ignore: one one-token request per
/// candidate per cycle.
pub const DEFAULT_INTERVAL_SECS: u64 = 300;

/// Ceiling on a single probe, so a hung candidate cannot stall the cycle past
/// the next one. Kept below the rotation's per-attempt budget by default, so a
/// probe reports "hung" on the same terms a real request would.
const DEFAULT_PROBE_TIMEOUT_SECS: u64 = 40;

/// How often to probe, from `PROBE_INTERVAL_SECS`. Zero or unparseable values
/// fall back to the default rather than disabling the probe by accident.
pub fn interval_from_env() -> Duration {
    interval_from(std::env::var("PROBE_INTERVAL_SECS").ok().as_deref())
}

/// The parsing behind [`interval_from_env`], split out so it is testable
/// without mutating the process environment. Environment variables are global
/// to the process, so a test that sets one races every other test running in
/// parallel; a pure function cannot race anything.
fn interval_from(raw: Option<&str>) -> Duration {
    let secs = raw
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    Duration::from_secs(secs)
}

/// The per-probe timeout, from `PROBE_TIMEOUT_SECS`.
fn timeout_from_env() -> Duration {
    timeout_from(std::env::var("PROBE_TIMEOUT_SECS").ok().as_deref())
}

/// The parsing behind [`timeout_from_env`]. Pure, for the same reason as
/// [`interval_from`].
fn timeout_from(raw: Option<&str>) -> Duration {
    let secs = raw
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

/// Which models to probe, from `PROBE_MODELS` when set, otherwise every
/// configured rotation candidate, and the allowlist when rotation is off.
///
/// Probing the rotation list is the point: the diagnostic that matters is
/// "candidate A is hung and candidate B answers in two seconds", which is
/// invisible if only one of them is ever exercised. Rotation off means there is
/// no such comparison to make, so the allowlist stands in and the proxy still
/// probes the models it actually serves.
pub fn models_from_env(rotation_names: &[String], allowlist: &[String]) -> Vec<String> {
    models_from(
        std::env::var("PROBE_MODELS").ok().as_deref(),
        rotation_names,
        allowlist,
    )
}

/// The parsing behind [`models_from_env`]. Pure, for the same reason as
/// [`interval_from`]. A blank or all-separator value falls back to the
/// rotation, then to the allowlist, rather than leaving the probe with nothing
/// to check.
fn models_from(raw: Option<&str>, rotation_names: &[String], allowlist: &[String]) -> Vec<String> {
    if let Some(raw) = raw {
        let listed: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !listed.is_empty() {
            return listed;
        }
    }
    if rotation_names.is_empty() {
        return allowlist.to_vec();
    }
    rotation_names.to_vec()
}

/// What one probe found, in the shape the tracker stores.
struct Outcome {
    ok: bool,
    status: Option<u16>,
    latency_ms: u64,
    error: Option<String>,
}

impl Outcome {
    /// True when the upstream refused this candidate at the price our
    /// ceiling bid. This is the one failure that is certain evidence: the
    /// body names a floor above our ceiling, so no request of any size can
    /// be served until it moves. The same 402 status with a different body
    /// code means an empty account, which `proxy::price_refusal` already
    /// excludes, so the probe inherits that discrimination for free.
    fn is_price_refusal(&self) -> bool {
        crate::proxy::price_refusal(self.error.as_deref())
    }
}

/// One minimal non-streaming completion against one model.
///
/// A probe is deliberately the smallest request the upstream will answer: one
/// user message, one token, no stream. It measures reachability and latency,
/// not throughput, and it costs a fraction of a real turn.
async fn probe_once(state: &AppState, model: &str, timeout: Duration) -> Outcome {
    let url = format!(
        "{}/v1/chat/completions",
        state.upstream.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "model": model,
        "messages": [{ "role": "user", "content": "ping" }],
        "max_tokens": 1,
        "stream": false,
    });
    let start = Instant::now();
    let sent = tokio::time::timeout(
        timeout,
        state
            .client
            .post(&url)
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {}", state.keys.current()),
            )
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send(),
    )
    .await;

    let response = match sent {
        Err(_) => {
            return Outcome {
                ok: false,
                status: None,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(format!("no response headers within {}s", timeout.as_secs())),
            }
        }
        Ok(Err(e)) => {
            return Outcome {
                ok: false,
                status: None,
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(e.to_string()),
            }
        }
        Ok(Ok(r)) => r,
    };

    let status = response.status().as_u16();
    // Read the body so the connection is drained and the latency covers the
    // whole exchange, not only the headers.
    let body = match tokio::time::timeout(timeout, response.bytes()).await {
        Err(_) => {
            return Outcome {
                ok: false,
                status: Some(status),
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some("body did not arrive inside the probe budget".to_string()),
            }
        }
        Ok(Err(e)) => {
            return Outcome {
                ok: false,
                status: Some(status),
                latency_ms: start.elapsed().as_millis() as u64,
                error: Some(e.to_string()),
            }
        }
        Ok(Ok(b)) => b,
    };
    let latency_ms = start.elapsed().as_millis() as u64;

    if status >= 400 {
        // The upstream answered, which is the reachability answer, but the
        // body usually names the reason and is worth keeping.
        let snippet: String = String::from_utf8_lossy(&body).chars().take(200).collect();
        return Outcome {
            ok: false,
            status: Some(status),
            latency_ms,
            error: Some(format!("upstream {status}: {snippet}")),
        };
    }

    Outcome {
        ok: true,
        status: Some(status),
        latency_ms,
        error: None,
    }
}

// ── Health verdicts shared with the request path ────────────────────────────

/// The probe's most recent verdict for one model.
#[derive(Clone, Copy)]
struct Verdict {
    ok: bool,
    /// Set on a down verdict whose failure was a price refusal: the only
    /// down a plan may act on. A down verdict without it is weak evidence
    /// (timeout, capacity, transport) and steers nothing.
    priced: bool,
    at: Instant,
}

/// Last probe verdict per model, shared between the probe loop and the request
/// path.
///
/// The request path needs one fact the probe already measures: is this
/// candidate answering right now? Reading it from the database per request
/// would put a query on the hot path, and the probe writes the same answer a
/// few times a minute, so it is kept in memory instead.
pub struct Health {
    verdicts: RwLock<HashMap<String, Verdict>>,
    /// How old a verdict may be and still steer a plan: three probe intervals,
    /// so one missed cycle does not silently disable the skip, and a verdict
    /// never outlives the evidence for it. Zero means the cache steers
    /// nothing, which is the safe default: until the probe has run, the request
    /// path behaves exactly as it did before this cache existed.
    stale_after: RwLock<Duration>,
}

impl Default for Health {
    fn default() -> Self {
        Self::new(Duration::ZERO)
    }
}

impl Health {
    pub fn new(stale_after: Duration) -> Self {
        Self {
            verdicts: RwLock::new(HashMap::new()),
            stale_after: RwLock::new(stale_after),
        }
    }

    /// How long a verdict may steer a plan. Set once at startup, from the probe
    /// interval, so the window tracks the cadence the verdicts actually arrive
    /// at instead of needing a second environment variable to keep in sync.
    pub fn set_stale_after(&self, window: Duration) {
        let mut guard = match self.stale_after.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = window;
    }

    /// Record what one probe just saw. `priced` marks a down verdict as a
    /// price refusal, the one failure that is certain evidence the candidate
    /// cannot serve under the bid in force.
    pub fn record(&self, model: &str, ok: bool, priced: bool) {
        // A poisoned lock means another thread panicked while holding it. This
        // is a cache: recovering it is correct, and dropping the write would
        // leave a stale verdict steering requests.
        let mut guard = match self.verdicts.write() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.insert(
            model.to_string(),
            Verdict {
                ok,
                priced,
                at: Instant::now(),
            },
        );
    }

    /// True when the newest fresh verdict for `model` is a price refusal.
    ///
    /// Deliberately narrower than "the probe saw it down": a probe that timed
    /// out or was refused for capacity is weak evidence, because the probe
    /// waits in the same upstream queue as real traffic and a lane whose
    /// probe dies can still serve a real request. Pruning on weak evidence
    /// collapsed plans onto a price-refusing lane on 2026-10-29, and every
    /// caller ate the resulting 503.
    fn prunable(&self, model: &str) -> bool {
        let window = match self.stale_after.read() {
            Ok(window) => *window,
            Err(poisoned) => *poisoned.into_inner(),
        };
        if window.is_zero() {
            return false;
        }
        let guard = match self.verdicts.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard
            .get(model)
            .is_some_and(|v| !v.ok && v.priced && v.at.elapsed() <= window)
    }
}

/// Drop candidates the probe has measured as unable to serve at the price
/// this proxy bids, unless that would leave the plan with nothing to try.
///
/// Only a price refusal may prune. A probe that timed out or was refused for
/// capacity is weak evidence: the probe is a one-token request waiting in the
/// same upstream queue as real traffic, and a lane whose probe dies can still
/// serve a real request (on 2026-10-29 alicn's probes failed for hours while
/// it carried thousands of real requests). Pruning on weak evidence is how a
/// plan collapses onto a lane that price-refuses instantly, and every caller
/// eats the resulting 503.
///
/// It is a reordering aid and never a gate: when every candidate is priced
/// out the full plan is used and each one still gets its turn, because a
/// fast refusal on every lane still beats an empty plan.
pub fn prune_plan(plan: Vec<String>, health: &Health) -> Vec<String> {
    let kept: Vec<String> = plan
        .iter()
        .filter(|model| !health.prunable(model))
        .cloned()
        .collect();
    if kept.is_empty() {
        return plan;
    }
    kept
}

/// One full cycle: every model, one probe each.
pub async fn run_cycle(state: &AppState, models: &[String], timeout: Duration) {
    for model in models {
        let outcome = probe_once(state, model, timeout).await;
        if !outcome.ok {
            tracing::warn!(
                "probe: `{model}` down ({}ms): {}",
                outcome.latency_ms,
                outcome.error.as_deref().unwrap_or("no detail")
            );
        }
        // Published before the row is written, because the request path reads
        // this and a probe that measured a candidate down should steer the very
        // next request off it.
        state
            .health
            .record(model, outcome.ok, outcome.is_price_refusal());
        state.tracker.record_probe(
            outcome.ok,
            outcome.status,
            Some(outcome.latency_ms),
            model,
            outcome.error.as_deref(),
        );
    }
}

/// Start the probe loop. Returns immediately; the loop runs for the life of
/// the process.
///
/// With no models to probe the loop is not started at all: an empty rotation
/// list would otherwise write a probe row per cycle that says nothing.
pub fn spawn(state: std::sync::Arc<AppState>, models: Vec<String>) {
    if models.is_empty() {
        tracing::warn!("upstream probe disabled: no models to probe");
        return;
    }
    let interval = interval_from_env();
    let timeout = timeout_from_env();
    // A verdict may steer a plan for three intervals: long enough that one
    // missed cycle does not silently disable the skip, short enough that a
    // recovered candidate is not held down by an old measurement.
    state.health.set_stale_after(interval * 3);
    tracing::info!(
        "upstream probe every {}s (timeout {}s, models [{}])",
        interval.as_secs(),
        timeout.as_secs(),
        models.join(",")
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // The first tick fires immediately, which is what we want: the state
        // is known from startup instead of from the first interval.
        loop {
            ticker.tick().await;
            run_cycle(&state, &models, timeout).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rotation() -> Vec<String> {
        vec![
            "combo/cheapest".to_string(),
            "deepseek-v4.1-flash".to_string(),
        ]
    }

    fn allowlist() -> Vec<String> {
        vec!["deepseek-v4.1-flash".to_string()]
    }

    #[test]
    fn probe_models_default_to_the_rotation_candidates() {
        // Unset PROBE_MODELS means the rotation list is what gets probed.
        assert_eq!(models_from(None, &rotation(), &allowlist()), rotation());
    }

    #[test]
    fn probe_models_fall_back_to_the_allowlist_when_rotation_is_off() {
        // Rotation off leaves no candidates to compare, so the probe checks
        // the models the proxy actually serves.
        assert_eq!(models_from(None, &[], &allowlist()), allowlist());
        assert_eq!(models_from(Some("  "), &[], &allowlist()), allowlist());
    }

    #[test]
    fn probe_models_env_overrides_the_rotation_candidates() {
        // Separators and blanks around the ids are trimmed away.
        assert_eq!(
            models_from(Some("a, b ,,c"), &rotation(), &allowlist()),
            vec!["a", "b", "c"]
        );
        // A blank list must not disable the probe: fall back to the rotation.
        assert_eq!(
            models_from(Some("  "), &rotation(), &allowlist()),
            rotation()
        );
        assert_eq!(models_from(Some(""), &rotation(), &allowlist()), rotation());
        assert_eq!(
            models_from(Some(" , ,"), &rotation(), &allowlist()),
            rotation()
        );
    }

    // ── Health verdicts, and the plan they steer ────────────────────────

    fn plan(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_price_refused_candidate_is_pruned() {
        // The upstream named a floor above our ceiling for this candidate, so
        // no request of any size can be served: the plan skips it.
        let health = Health::new(Duration::from_secs(60));
        health.record("a", false, true);
        health.record("b", true, false);
        assert_eq!(
            prune_plan(plan(&["a", "b"]), &health),
            plan(&["b"]),
            "a candidate measured price-refused must not cost a request its wait"
        );
    }

    #[test]
    fn a_timed_out_candidate_stays_in_the_plan() {
        // A probe that timed out is weak evidence: it waited in the same
        // queue as real traffic, and a lane whose probe dies can still serve
        // a real request. Pruning on it is how plans collapsed onto a lane
        // that price-refused instantly on 2026-10-29.
        let health = Health::new(Duration::from_secs(60));
        health.record("a", false, false);
        health.record("b", true, false);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn a_healthy_verdict_keeps_the_candidate() {
        let health = Health::new(Duration::from_secs(60));
        health.record("a", true, false);
        health.record("b", true, false);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn an_unmeasured_candidate_is_kept() {
        // Nothing measured is not evidence of anything, so the plan is
        // untouched until the probe has actually run.
        let health = Health::new(Duration::from_secs(60));
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn a_plan_is_never_pruned_to_nothing() {
        // Every candidate priced out: the full plan is used. An empty plan
        // would be an outage, and a fast refusal on every lane still beats
        // no plan at all.
        let health = Health::new(Duration::from_secs(60));
        health.record("a", false, true);
        health.record("b", false, true);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn the_incident_shape_prunes_only_the_price_dead_lane() {
        // 2026-10-29: one lane price-refusing on every request, two lanes
        // whose probes timed out while they still served real traffic. The
        // prune must remove only the priced lane, leaving the walk a chance
        // to land somewhere that serves.
        let health = Health::new(Duration::from_secs(60));
        health.record("ali", false, true);
        health.record("cb", false, false);
        health.record("alicn", false, false);
        assert_eq!(
            prune_plan(plan(&["ali", "cb", "alicn"]), &health),
            plan(&["cb", "alicn"]),
        );
    }

    #[test]
    fn a_stale_verdict_stops_steering_the_plan() {
        // A measurement from long ago is not evidence about now.
        let health = Health::new(Duration::from_millis(1));
        health.record("a", false, true);
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn a_zero_window_steers_nothing() {
        // The default, before the probe has run. The request path must behave
        // exactly as it did before this cache existed.
        let health = Health::default();
        health.record("a", false, true);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn a_recovered_candidate_returns_to_the_plan() {
        // The newest verdict wins, so a candidate that comes back is used
        // again instead of being held down by its own history.
        let health = Health::new(Duration::from_secs(60));
        health.record("a", false, true);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["b"]));
        health.record("a", true, false);
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["a", "b"]));
    }

    #[test]
    fn a_poisoned_health_lock_still_answers() {
        // This is a cache, so a panic elsewhere must not turn a request into a
        // panic: the data is still there and still usable.
        let health = std::sync::Arc::new(Health::new(Duration::from_secs(60)));
        health.record("a", false, true);
        let clone = std::sync::Arc::clone(&health);
        let _ = std::thread::spawn(move || {
            let _guard = clone.verdicts.write().unwrap();
            panic!("poison the lock");
        })
        .join();
        assert_eq!(prune_plan(plan(&["a", "b"]), &health), plan(&["b"]));
    }

    #[test]
    fn probe_interval_falls_back_rather_than_disabling_itself() {
        // Zero would disable the probe, so it is treated as "unset".
        assert_eq!(
            interval_from(Some("0")),
            Duration::from_secs(DEFAULT_INTERVAL_SECS)
        );
        assert_eq!(
            interval_from(Some("nonsense")),
            Duration::from_secs(DEFAULT_INTERVAL_SECS)
        );
        assert_eq!(
            interval_from(None),
            Duration::from_secs(DEFAULT_INTERVAL_SECS)
        );
        assert_eq!(interval_from(Some("30")), Duration::from_secs(30));
    }

    #[test]
    fn probe_timeout_falls_back_rather_than_disabling_itself() {
        assert_eq!(
            timeout_from(Some("0")),
            Duration::from_secs(DEFAULT_PROBE_TIMEOUT_SECS)
        );
        assert_eq!(
            timeout_from(Some("nope")),
            Duration::from_secs(DEFAULT_PROBE_TIMEOUT_SECS)
        );
        assert_eq!(timeout_from(Some("5")), Duration::from_secs(5));
    }
}
