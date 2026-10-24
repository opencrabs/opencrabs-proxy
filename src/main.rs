//! keyproxy: a key-injecting reverse proxy for an OpenAI-compatible upstream.
//!
//! Clients point their base URL at this proxy and send a per-caller token
//! instead of the upstream key. The proxy holds the real upstream key, which
//! is read from the environment at runtime and never compiled in. A token is
//! refused unless its hash is listed in `TOKENS_FILE`.
//!
//! Subcommands: `keyproxy issue <label>` appends a new caller to `TOKENS_FILE`
//! and prints its token once. `keyproxy revoke <label>` removes a caller; the
//! running proxy applies that after a restart. Only hashes are stored.
//!
//! The install route lets an install id fetch its own token once, so no
//! operator has to issue it by hand. It is mounted only when `INSTALL_PATH`
//! names a path; unset leaves the route off. Usage is tracked per install label.

mod alias;
mod allowlist;
mod audio;
mod calibrate;
mod caps;
mod guard;
mod handlers;
mod install;
mod keys;
mod limit;
mod passthrough;
mod probe;
mod proxy;
mod public_stats;
mod rate;
mod rotation;
mod tokens;
mod tracker;
mod vision;

use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use std::{net::SocketAddr, sync::Arc, sync::RwLock, time::Duration};

/// How far back the startup seed reads density from. A week is long enough to
/// cover a quiet weekend for a low-traffic caller and short enough that a
/// caller whose workload changed is not held to its old density for long.
const CALIBRATION_SEED_DAYS: u32 = 7;

pub struct AppState {
    client: reqwest::Client,
    /// The configured upstream keys, and which one is active.
    keys: keys::KeyRing,
    admin_token: String,
    /// Read on every request. Written only by install issuance, which reloads it.
    tokens: RwLock<tokens::TokenStore>,
    tokens_path: String,
    vision_swap: bool,
    allow: allowlist::Allowlist,
    /// The voice-only second upstream. Unset leaves audio with one leg.
    audio_fallback: Option<audio::AudioFallback>,
    rotation: rotation::Rotation,
    upstream: String,
    rate: rate::RateLimiter<String>,
    /// Requests to the install route per client IP per day.
    install_rate: rate::RateLimiter<String>,
    /// Path the install route is mounted at, from `INSTALL_PATH`. Unset means
    /// the route is not registered at all.
    install_path: Option<String>,
    /// Response header naming the publisher that served a request, from
    /// `PUBLISHER_HEADER`. Unset leaves the column empty: the header name is
    /// deployment state, not something the binary should know.
    publisher_header: Option<String>,
    caps: caps::Caps,
    /// The per-request input ceiling, applied on every route that takes a body.
    limit: limit::InputLimit,
    /// Per-caller density, which turns the flat estimate into a measured one.
    pub calibration: Arc<calibrate::Calibration>,
    pub tracker: tracker::Tracker,
}

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| panic!("{name} must be set at runtime"))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// The single-key fallback handed to [`keys::KeyRing::parse`].
///
/// `UPSTREAM_KEYS` is the ring form, and it is the one the README documents
/// first, so a ring-only deployment must not trip the single-key requirement.
/// `parse` only reads this value when the ring is absent, so an empty string
/// here is correct whenever `UPSTREAM_KEYS` carries the keys. Startup stops
/// only when both forms are absent.
fn single_key_fallback<'a>(ring: Option<&'a str>, single: Option<&'a str>) -> &'a str {
    match (ring, single) {
        (None, None) => panic!("UPSTREAM_KEYS or UPSTREAM_API_KEY must be set at runtime"),
        _ => single.unwrap_or_default(),
    }
}

fn issue_command(label: &str) {
    let path = required("TOKENS_FILE");
    match tokens::issue(&path, label) {
        Ok(token) => {
            println!("{token}");
            eprintln!("Issued `{label}`. Show this token once; the server keeps only its hash.");
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn revoke_command(label: &str) {
    let path = required("TOKENS_FILE");
    match tokens::revoke(&path, label) {
        Ok(()) => eprintln!("Revoked `{label}`. Restart keyproxy so the running proxy drops it."),
        Err(tokens::RevokeError::Unknown) => {
            eprintln!("error: no caller named `{label}` in {path}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("issue") {
        let label = args.get(2).unwrap_or_else(|| {
            eprintln!("usage: keyproxy issue <label>");
            std::process::exit(2);
        });
        issue_command(label);
        return;
    }
    if args.get(1).map(String::as_str) == Some("revoke") {
        let label = args.get(2).unwrap_or_else(|| {
            eprintln!("usage: keyproxy revoke <label>");
            std::process::exit(2);
        });
        revoke_command(label);
        return;
    }
    if args.get(1).map(String::as_str) == Some("export-public-stats") {
        public_stats::export_command(&args[2..]);
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // Every secret and the caller list are required. A missing value stops the
    // process before it listens, so the proxy never runs open.
    // One or more upstream keys. `UPSTREAM_KEYS` is the ring, a comma
    // separated list of `label:key` or bare keys; `UPSTREAM_API_KEY` stays
    // supported as the single-key form so an existing deploy keeps working.
    let upstream_keys = std::env::var("UPSTREAM_KEYS")
        .ok()
        .filter(|s| !s.is_empty());
    let single_key = std::env::var("UPSTREAM_API_KEY")
        .ok()
        .filter(|s| !s.is_empty());
    let min_balance = keys::min_balance_from(std::env::var("KEY_MIN_BALANCE").ok().as_deref());
    let keys = keys::KeyRing::parse(
        upstream_keys.as_deref(),
        single_key_fallback(upstream_keys.as_deref(), single_key.as_deref()),
        min_balance,
    );
    // The balance poller needs a control-plane endpoint, which is deployment
    // state. Unset means no poller: the ring still rotates on upstream
    // refusals, it just loses the pre-emptive signal.
    let balance_url = std::env::var("KEY_BALANCE_URL")
        .ok()
        .filter(|s| !s.is_empty());
    let balance_interval =
        keys::interval_from(std::env::var("KEY_BALANCE_INTERVAL_SECS").ok().as_deref());
    let admin_token = required("ADMIN_TOKEN");
    let tokens_path = required("TOKENS_FILE");
    let tokens = tokens::TokenStore::load(&tokens_path).unwrap_or_else(|e| panic!("{e}"));
    let upstream = required("UPSTREAM_BASE_URL");
    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8787".to_string());
    let rate_max: u32 = std::env::var("RATE_MAX_PER_MIN")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let installs_per_day =
        install::requests_per_day(std::env::var("INSTALL_RATE_PER_DAY").ok().as_deref());
    // The install route is mounted only when a path is configured, so the path
    // is deployment state rather than a constant in the binary.
    let install_path = std::env::var("INSTALL_PATH")
        .ok()
        .filter(|s| !s.is_empty())
        .map(|s| {
            assert!(
                s.starts_with('/'),
                "INSTALL_PATH must start with `/`, got `{s}`"
            );
            s
        });
    // The header naming the publisher is upstream-specific, so it comes from
    // the environment. Unset means the column stays empty.
    let publisher_header = std::env::var("PUBLISHER_HEADER")
        .ok()
        .filter(|s| !s.is_empty());
    let stats_db = std::env::var("STATS_DB").unwrap_or_else(|_| "./keyproxy.db".to_string());
    let vision_swap = env_flag("VISION_SWAP");
    let allowed_raw =
        std::env::var("ALLOWED_MODELS").unwrap_or_else(|_| allowlist::DEFAULT_MODELS.to_string());
    // Three lists that never overlap: a chat id, an image id and an audio id
    // each answer a different kind of request, and an id in two lists would
    // let one kind of request reach a model that cannot serve it. Empty turns
    // that kind off.
    let allow = allowlist::Allowlist::parse(&allowed_raw)
        .with_images(
            &std::env::var("IMAGE_MODELS")
                .unwrap_or_else(|_| allowlist::DEFAULT_IMAGE_MODELS.to_string()),
        )
        .with_audio(
            &std::env::var("AUDIO_MODELS")
                .unwrap_or_else(|_| allowlist::DEFAULT_AUDIO_MODELS.to_string()),
        );
    // Voice-only second leg. All three variables or none: a partial set stops
    // the process rather than promising a fallback that is not there.
    let audio_fallback = audio::AudioFallback::from_env();
    let rotation = rotation::Rotation::parse(std::env::var("ROTATION_MODELS").ok().as_deref());
    let rotation_names = rotation.names();
    let rotation_secs = rotation.attempt_timeout().as_secs();
    // The probe falls back to the rotation candidates, and to the allowlist
    // when rotation is off, so an unconfigured proxy still probes the model it
    // actually serves instead of probing nothing.
    let probe_models = probe::models_from_env(rotation.candidates(), allow.chat_candidates());

    let models_list = allow.names();
    let images_list = allow.image_names();
    let audio_list = allow.audio_names();
    let fallback_note = match &audio_fallback {
        Some(f) => format!("[{}]", f.describe()),
        None => "off".to_string(),
    };
    let caps = caps::Caps::from_env();
    let limit = limit::InputLimit::from_env();
    let input_ceiling = if limit.enabled() {
        format!("{} tokens", limit.max_tokens())
    } else {
        "off".to_string()
    };

    // Built before the state so the calibration can be warmed from the same
    // database the tracker will write to.
    let tracker = tracker::Tracker::new(&stats_db);
    let calibration = Arc::new(calibrate::Calibration::new());
    let history = tracker.density_history(CALIBRATION_SEED_DAYS);
    calibration.seed(&history);
    let calibration_note = format!(
        "{} callers seeded from {CALIBRATION_SEED_DAYS} days",
        history.len()
    );
    for (label, factor, samples) in calibration.snapshot() {
        tracing::info!("density for `{label}`: {factor:.3} over {samples} observations");
    }

    let state = Arc::new(AppState {
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(600))
            .build()
            .expect("build reqwest client"),
        keys,
        admin_token,
        tokens: RwLock::new(tokens),
        tokens_path,
        vision_swap,
        allow,
        audio_fallback,
        rotation,
        upstream: upstream.clone(),
        rate: rate::RateLimiter::new(rate_max, Duration::from_secs(60)),
        install_rate: rate::RateLimiter::new(installs_per_day, Duration::from_secs(86_400)),
        install_path,
        publisher_header,
        caps,
        limit,
        calibration,
        tracker,
    });

    let app = build_router(Arc::clone(&state));
    probe::spawn(Arc::clone(&state), probe_models);
    match balance_url {
        Some(url) => keys::spawn_poller(
            state.keys.clone(),
            state.client.clone(),
            url,
            balance_interval,
            state.tracker.clone_handle(),
        ),
        None => tracing::warn!(
            "KEY_BALANCE_URL is unset, so the balance poller is off and keys are \
             only retired after the upstream refuses them"
        ),
    }

    let install_note = if state.install_path.is_some() {
        format!("enabled, {installs_per_day}/day per ip")
    } else {
        "off".to_string()
    };
    let key_labels = state.keys.labels().join(",");
    let active_key = state.keys.label();
    let listener = tokio::net::TcpListener::bind(&bind)
        .await
        .unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    tracing::info!(
        "keyproxy listening on {bind} (upstream {upstream}, rate {rate_max}/min per caller, vision_swap {vision_swap}, models {models_list}, image models {images_list}, audio models {audio_list}, voice fallback {fallback_note}, stats {stats_db}, caps {:?}, input ceiling {input_ceiling}, calibration {calibration_note}, body limit {} MB, install route {install_note}, rotation [{rotation_names}] at {rotation_secs}s per attempt, keys [{key_labels}] active `{active_key}` floor {min_balance})",
        caps,
        limit::MAX_BODY_BYTES / (1024 * 1024),
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .expect("server error");
}

/// Routes shared by the running server and the handler tests.
fn build_router(state: Arc<AppState>) -> Router {
    let mut router = Router::new()
        .route("/health", get(handlers::health))
        .route("/stats", get(handlers::stats_endpoint))
        .route("/failures", get(handlers::failures_endpoint))
        .route("/upstream-health", get(handlers::upstream_health_endpoint))
        // The path is not a secret: the admin token gates it, so knowing the
        // route grants nothing. It exists so a token can be revoked inside the
        // service namespace instead of by editing the file from the host.
        .route("/admin/revoke", post(handlers::revoke_endpoint))
        .route("/v1/chat/completions", post(handlers::chat_completions))
        .route("/v1/models", get(handlers::list_models));
    // Mounted only when a path is configured, so the install path is deployment
    // state rather than something readable from the repository.
    if let Some(path) = state.install_path.as_deref() {
        router = router.route(path, post(handlers::issue_install));
    }
    router
        .fallback(passthrough::passthrough)
        // Matches `client_max_body_size 25m` on the nginx host in front of the
        // proxy, so a body the edge accepts reaches the handler and leaves a
        // stats row instead of dying at the extractor.
        .layer(DefaultBodyLimit::max(limit::MAX_BODY_BYTES))
        .with_state(state)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => tracing::warn!("SIGTERM handler failed: {e}"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutting down");
}

#[cfg(test)]
mod tests {
    use super::single_key_fallback;

    /// A ring-only deployment is the form the README documents first, and it
    /// started the process with a panic before this: the single-key argument
    /// was read eagerly, so the ring was never consulted.
    #[test]
    fn ring_only_deployment_does_not_require_the_single_key() {
        assert_eq!(single_key_fallback(Some("primary:sk-one"), None), "");
    }

    #[test]
    fn single_key_is_the_fallback_when_no_ring_is_set() {
        assert_eq!(single_key_fallback(None, Some("sk-one")), "sk-one");
    }

    /// Both forms absent still stops startup, which is the guard the eager
    /// read was there for.
    #[test]
    #[should_panic(expected = "UPSTREAM_KEYS or UPSTREAM_API_KEY must be set at runtime")]
    fn both_forms_absent_stops_startup() {
        single_key_fallback(None, None);
    }
}
