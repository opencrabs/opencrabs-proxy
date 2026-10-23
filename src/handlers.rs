//! Route handlers. Every route except `/health` passes the caller gate first,
//! and every request writes one row to the stats tracker, rejected or not.

use axum::{
    body::Bytes,
    extract::{ConnectInfo, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use std::{net::SocketAddr, sync::Arc, time::Instant};

use crate::{
    allowlist::Allowlist, audio, guard, install, limit, proxy, tokens, tracker::RequestRecord,
    vision, AppState,
};

/// Label stored when a request never reached a caller (no or bad token).
const NO_CALLER: &str = "-";

/// Label stored as the key of a request the voice fallback answered. The
/// fallback has its own credential, so the primary key labels would lie.
const AUDIO_FALLBACK_LABEL: &str = "audio-fallback";

/// Why a request never reached a model, as stored in `failure_reason`. Every
/// non-2xx gets a reason so a report can account for all of them instead of
/// leaving a bucket of anonymous errors.
fn rejection_reason(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => "bad_token",
        StatusCode::TOO_MANY_REQUESTS => "rate_limited",
        StatusCode::FORBIDDEN => "forbidden",
        _ => "rejected",
    }
}

pub fn rejected(state: &AppState, ip: String, endpoint: &'static str, status: StatusCode) {
    state.tracker.record(RequestRecord {
        ip,
        token: NO_CALLER.to_string(),
        endpoint,
        status: status.as_u16(),
        failure_reason: Some(rejection_reason(status)),
        ..Default::default()
    });
}

/// True when any message carries an `input_audio` part.
fn carries_audio(body: &serde_json::Value) -> bool {
    body.get("messages")
        .and_then(|m| m.as_array())
        .is_some_and(|messages| {
            messages.iter().any(|message| {
                message
                    .get("content")
                    .and_then(|c| c.as_array())
                    .is_some_and(|parts| parts.iter().any(|p| p.get("input_audio").is_some()))
            })
        })
}

/// The voice-only second leg.
///
/// Called only for a request that carries audio, and only after the primary
/// upstream produced no answer. Text requests never reach it, so a chat prompt
/// cannot be sent to a provider that was configured for voice.
///
/// Tries each fallback model in order on the fallback upstream's own key. The
/// first answer below 400 wins; anything else is logged and the next model is
/// tried. `None` means the primary's own error is the one to relay.
async fn audio_fallback(
    state: &AppState,
    body_json: &mut serde_json::Value,
    start: Instant,
) -> Option<(String, proxy::ForwardResult)> {
    let fallback = state.audio_fallback.as_ref()?;
    // The second upstream reads the audio field as a URL or a data URI while
    // the caller sent bare base64, so the part is converted once here, before
    // any attempt. The primary upstream is not affected: it already answered.
    audio::audio_parts_as_data_uris(body_json);
    let url = fallback.chat_url();
    let policy = proxy::AttemptPolicy::header_only(state.rotation.attempt_timeout());
    for model in &fallback.models {
        body_json["model"] = serde_json::Value::String(model.clone());
        let req = state
            .client
            .post(&url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body_json.to_string());
        match proxy::send_attempt_with(fallback.key(), policy, req).await {
            proxy::Attempt::Ok(upstream) => {
                let forwarded = proxy::forward_response(upstream, start).await;
                if forwarded.status < 400 {
                    return Some((model.clone(), forwarded));
                }
                tracing::warn!("audio fallback: `{model}` upstream {}", forwarded.status);
            }
            proxy::Attempt::ServerError(upstream) => {
                tracing::warn!("audio fallback: `{model}` upstream {}", upstream.status);
            }
            proxy::Attempt::Dead(failure) => {
                tracing::warn!("audio fallback: `{model}` dead: {}", failure.detail);
            }
        }
    }
    None
}

/// Record and answer a request for a model the allowlist does not serve.
pub fn refuse_model(
    state: &AppState,
    ip: String,
    caller: String,
    endpoint: &'static str,
    model: &str,
) -> Response {
    state.tracker.record(RequestRecord {
        ip,
        token: caller,
        endpoint,
        model_requested: model.to_string(),
        model_used: model.to_string(),
        status: StatusCode::FORBIDDEN.as_u16(),
        failure_reason: Some("model_not_allowed"),
        ..Default::default()
    });
    Allowlist::refusal(model)
}

/// Record and answer a request whose estimated input is at or above the ceiling.
/// Runs before the body is parsed for a model, so an oversized body is never
/// parsed and never forwarded.
pub fn refuse_input(
    state: &AppState,
    ip: String,
    caller: String,
    endpoint: &'static str,
    estimate: u64,
    body_bytes: usize,
) -> Response {
    tracing::warn!(
        "refusing an oversized request from `{caller}` on {endpoint}: about {estimate} input tokens in {body_bytes} bytes, ceiling {}",
        state.limit.max_tokens()
    );
    state.tracker.record(RequestRecord {
        ip,
        token: caller,
        endpoint,
        status: StatusCode::PAYLOAD_TOO_LARGE.as_u16(),
        failure_reason: Some("input_too_large"),
        estimated_tokens: Some(estimate),
        ..Default::default()
    });
    limit::refusal(estimate, state.limit.max_tokens())
}

pub async fn chat_completions(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let ip = guard::client_ip(&headers, peer.ip()).to_string();
    let endpoint = "/v1/chat/completions";

    let caller = match guard::gate(&state, &headers) {
        Ok(label) => label,
        Err(rejection) => {
            rejected(&state, ip, endpoint, rejection.status());
            return *rejection;
        }
    };

    // Computed once, before the ceiling check, so the same number both gates
    // the request and lands in its stats row.
    let estimate = limit::estimate_tokens(&body);
    let factor = state.calibration.factor(&caller);
    if state.limit.exceeded_scaled(estimate, factor) {
        let scaled = (estimate as f64 * factor).ceil() as u64;
        return refuse_input(&state, ip, caller, endpoint, scaled, body.len());
    }

    let url = format!("{}/v1/chat/completions", state.upstream);
    // A body that is not JSON has no model, so it cannot pass the allowlist.
    let mut body_json: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return refuse_model(&state, ip, caller, endpoint, ""),
    };

    let model_requested = body_json
        .get("model")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let model_used = if state.vision_swap {
        vision::maybe_swap_vision_model(&mut body_json)
    } else {
        model_requested.clone()
    };
    let vision_swap = model_requested != model_used;

    // An id the client names that this proxy serves under another spelling, but
    // that names the capability of this request, is resolved to the served id
    // rather than refused: a client built against older docs keeps working.
    // An audio request is checked against the audio list and a text request
    // against the chat list. The two never overlap, so a voice note cannot be
    // answered by a text model and a text prompt cannot be answered by a voice
    // one. Resolved on the model that goes upstream, so a vision swap cannot
    // route around the allowlist either.
    let is_audio = carries_audio(&body_json);
    let resolved = if is_audio {
        state.allow.resolve_audio(&model_used)
    } else {
        state.allow.resolve_chat(&model_used)
    };
    let Some(model_used) = resolved else {
        return refuse_model(&state, ip, caller, endpoint, &model_used);
    };
    if model_used != body_json["model"].as_str().unwrap_or_default() {
        tracing::info!(
            "model alias: `{}` is served as `{model_used}`",
            model_requested
        );
        body_json["model"] = serde_json::Value::String(model_used.clone());
    }

    // An audio request is tried on the audio list only: the requested id
    // first, the remaining audio ids after it. The list is non-empty whenever
    // the id resolved, so the plan exists for every audio request that passed
    // the check above.
    let audio_plan = if is_audio {
        state.allow.audio_plan_for(&model_used)
    } else {
        None
    };

    // Whether the caller asked for a stream decides how the request is timed
    // below and whether the upstream is asked for token usage.
    let wants_stream = body_json
        .get("stream")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Token usage on a streaming answer arrives in the final chunk, and only
    // when `stream_options` was sent. A non-streaming answer reports usage on
    // its own, while `stream_options` without `stream` is a 400 on one
    // publisher and a hang on another, so it goes only on a streaming request.
    if wants_stream {
        body_json["stream_options"] = serde_json::json!({"include_usage": true});
    }

    // Rotation: the requested model tries first, then the remaining
    // candidates. A candidate counts as failed on transport death, no headers
    // inside the header budget, no first stream byte inside the first-token
    // budget (small streaming requests only; compaction-scale bodies are
    // exempt), or an upstream 5xx. A 4xx is the upstream speaking for this
    // exact request and is never retried.
    // An audio request is tried on the audio list only: the requested id
    // first, the remaining audio ids after it. The rotation candidates are
    // text models, so they are never part of an audio plan.
    let plan = match &audio_plan {
        Some(plan) => plan.clone(),
        None => state.rotation.plan_for(&model_used),
    };
    let plan_len = plan.len() as u32;
    let first_byte_budget = state.rotation.first_byte_budget(body.len(), wants_stream);
    let start = Instant::now();
    let mut served: Option<String> = None;
    let mut served_index: Option<u32> = None;
    let mut last_error: Option<proxy::UpstreamResponse> = None;
    let mut last_dead: Option<proxy::FailureKind> = None;
    let mut result: Option<proxy::ForwardResult> = None;
    let mut attempts = 0u32;
    // The key that served, and what the upstream said about it, for the
    // request row.
    let mut key_label = state.keys.label();
    let mut upstream_publisher: Option<String> = None;
    let mut ratelimit_remaining: Option<u64> = None;
    // Each key is tried at most once per request.
    let max_key_switches = state.keys.len();
    'plan: for (index, candidate) in plan.iter().enumerate() {
        body_json["model"] = serde_json::Value::String(candidate.clone());
        let policy = match first_byte_budget {
            Some(budget) => proxy::AttemptPolicy::with_first_byte(budget),
            None => proxy::AttemptPolicy::header_only(state.rotation.attempt_timeout()),
        };
        let nth = format!("{}/{}", index + 1, plan.len());
        // A key-level status must not consume a rotation candidate, so the
        // candidate gets its own retry loop.
        let mut key_switches = 0usize;
        'keys: loop {
            attempts += 1;
            let (label, secret) = state.keys.active();
            key_label = label;
            let req = state
                .client
                .post(&url)
                .header(header::CONTENT_TYPE, "application/json")
                .body(body_json.to_string());
            let mut upstream = match proxy::send_attempt_with(&secret, policy, req).await {
                proxy::Attempt::Ok(upstream) => upstream,
                proxy::Attempt::ServerError(upstream) => {
                    tracing::warn!(
                        "rotation: `{candidate}` upstream {} (attempt {nth})",
                        upstream.status
                    );
                    served = Some(candidate.clone());
                    served_index = Some(index as u32);
                    last_error = Some(upstream);
                    break 'keys;
                }
                proxy::Attempt::Dead(failure) => {
                    tracing::warn!(
                        "rotation: `{candidate}` dead (attempt {nth}): {}",
                        failure.detail
                    );
                    last_dead = Some(failure.kind);
                    break 'keys;
                }
            };

            // A key-level status is about the account, not this request, so
            // it is settled before the rotation plan moves on: switch the key
            // and retry the same candidate.
            if proxy::key_level_status(upstream.status) && key_switches < max_key_switches {
                // On a 429 the body is what separates this key's own limit
                // from the platform running out of capacity, so it is read
                // here and put back for the caller either way.
                let body = if upstream.status == 429 {
                    Some(
                        String::from_utf8_lossy(&proxy::drain_body(&mut upstream).await)
                            .to_string(),
                    )
                } else {
                    None
                };
                let cooldown = proxy::rate_limit_cooldown(&upstream.headers);
                if let Some(switch) =
                    state
                        .keys
                        .on_status(upstream.status, body.as_deref(), cooldown)
                {
                    tracing::warn!(
                        "key rotation: `{}` -> `{}` ({}) after upstream {} on attempt {nth}",
                        switch.from,
                        switch.to,
                        switch.reason.as_str(),
                        upstream.status
                    );
                    key_switches += 1;
                    continue 'keys;
                }
            }

            if index > 0 {
                tracing::warn!(
                    "rotation: `{model_used}` failed, served `{candidate}` on retry {nth}"
                );
            }
            served = Some(candidate.clone());
            served_index = Some(index as u32);
            // The publisher header is upstream-specific, so the name comes from
            // the environment. Unset leaves the column empty.
            upstream_publisher = state
                .publisher_header
                .as_deref()
                .and_then(|name| proxy::header_string(&upstream.headers, name));
            ratelimit_remaining = proxy::header_u64(&upstream.headers, "x-ratelimit-remaining");
            result = Some(proxy::forward_response(upstream, start).await);
            break 'plan;
        }
    }
    // Voice-only second leg. Reached only by a request that carries audio,
    // and only when the primary produced no answer: every audio candidate was
    // dead or 5xx, or the primary rejected the request outright. A successful
    // fallback replaces the primary result; a failed one leaves the primary's
    // own error to be relayed.
    let primary_failed = match &result {
        Some(forwarded) => forwarded.status >= 400,
        None => true,
    };
    if is_audio && primary_failed {
        if let Some((model, forwarded)) = audio_fallback(&state, &mut body_json, start).await {
            tracing::warn!("audio fallback: served `{model}` after the primary failed");
            served = Some(model);
            served_index = None;
            key_label = AUDIO_FALLBACK_LABEL.to_string();
            result = Some(forwarded);
        }
    }

    let mut result = match (result, last_error) {
        (Some(result), _) => result,
        // Every candidate answered 5xx: relay the last upstream error body.
        (None, Some(upstream)) => proxy::forward_response(upstream, start).await,
        // Every candidate was dead: no upstream response exists to relay, and
        // the last attempt's failure kind is why the caller gets a 502.
        (None, None) => proxy::ForwardResult::upstream_error(
            start.elapsed().as_millis() as u64,
            last_dead.unwrap_or(proxy::FailureKind::Transport).as_str(),
        ),
    };
    if let Some(model) = &served {
        result = result.with_served_model(model);
    }

    // The reason the caller got what it got, in the form the stats and the
    // alert group by. A relayed upstream error carries no `failure` of its
    // own, so its status supplies the reason.
    let failure_reason = match result.failure {
        Some(reason) => Some(reason),
        None if result.status >= 500 => Some("upstream_5xx"),
        None if result.status >= 400 => Some("upstream_4xx"),
        None => None,
    };
    let status = result.status;
    let latency_ms = result.latency_ms;
    let usage = result.usage.take();

    if let Some(reason) = failure_reason {
        // One greppable line per failed request: the whole rotation decision,
        // not just the per-attempt warnings above it.
        tracing::warn!(
            "chat failed: caller={caller} requested={model_requested} plan_len={plan_len} \
             attempts={attempts} status={status} reason={reason} latency_ms={}",
            start.elapsed().as_millis()
        );
    }

    // Stats carry what actually served, so a rotated request is visible as
    // the model that produced the answer, not the one that was asked.
    let model_served = served.unwrap_or_else(|| model_used.clone());
    let proxy::ForwardResult {
        response,
        stream_metrics,
        ..
    } = result;
    if let Some(metrics_rx) = stream_metrics {
        // Streaming: the row is written once the upstream body has finished,
        // which is also the only point where a body that died after its
        // headers can be told apart from a complete one.
        let tracker = state.tracker.clone_handle();
        let calibration = Arc::clone(&state.calibration);
        tokio::spawn(async move {
            if let Ok(metrics) = metrics_rx.await {
                let truncated = status == 200 && metrics.usage.is_none();
                if truncated {
                    tracing::warn!(
                        "chat truncated: caller={caller} requested={model_requested} \
                         status=200 but the body carried no usage"
                    );
                }
                // The upstream's own count for this body is the ground truth
                // the estimate is corrected against, so feed it back before
                // the record consumes the usage.
                if let Some(u) = metrics.usage.as_ref() {
                    calibration.observe(&caller, estimate, u.prompt_tokens);
                }
                tracker.record(RequestRecord {
                    ip,
                    token: caller,
                    endpoint,
                    model_requested,
                    model_used: model_served,
                    status,
                    latency_ms: metrics.latency_ms,
                    vision_swap,
                    usage: metrics.usage,
                    estimated_tokens: Some(estimate),
                    failure_reason,
                    attempts: Some(attempts),
                    plan_len: Some(plan_len),
                    served_index,
                    truncated,
                    key_label,
                    upstream_publisher,
                    ratelimit_remaining,
                });
            }
        });
    } else {
        let truncated = status == 200 && usage.is_none();
        if truncated {
            tracing::warn!(
                "chat truncated: caller={caller} requested={model_requested} \
                 status=200 but the body carried no usage"
            );
        }
        // Same feedback as the streaming path: the upstream's count corrects
        // the estimate for the next request from this caller.
        if let Some(u) = usage.as_ref() {
            state
                .calibration
                .observe(&caller, estimate, u.prompt_tokens);
        }
        state.tracker.record(RequestRecord {
            ip,
            token: caller,
            endpoint,
            model_requested,
            model_used: model_served,
            status,
            latency_ms,
            vision_swap,
            usage,
            estimated_tokens: Some(estimate),
            failure_reason,
            attempts: Some(attempts),
            plan_len: Some(plan_len),
            served_index,
            truncated,
            key_label,
            upstream_publisher,
            ratelimit_remaining,
        });
    }

    response
}

pub async fn list_models(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ip = guard::client_ip(&headers, peer.ip()).to_string();
    let endpoint = "/v1/models";

    let caller = match guard::gate(&state, &headers) {
        Ok(label) => label,
        Err(rejection) => {
            rejected(&state, ip, endpoint, rejection.status());
            return *rejection;
        }
    };

    let (key_label, upstream_key) = state.keys.active();
    let start = Instant::now();
    let url = format!("{}/v1/models", state.upstream);
    let upstream_err = |latency_ms: u64| {
        state.tracker.record(RequestRecord {
            ip: ip.clone(),
            token: caller.clone(),
            endpoint,
            status: 502,
            latency_ms,
            failure_reason: Some("transport"),
            key_label: key_label.clone(),
            ..Default::default()
        });
        (StatusCode::BAD_GATEWAY, "upstream error").into_response()
    };

    let resp = match state
        .client
        .get(&url)
        .header(header::AUTHORIZATION, format!("Bearer {upstream_key}"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("models upstream error: {e}");
            return upstream_err(start.elapsed().as_millis() as u64);
        }
    };
    let status = resp.status().as_u16();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("models read error: {e}");
            return upstream_err(start.elapsed().as_millis() as u64);
        }
    };
    let latency_ms = start.elapsed().as_millis() as u64;
    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);

    // Only allowlisted models are listed. A payload of another shape passes through untouched.
    let response = match serde_json::from_slice::<serde_json::Value>(&bytes) {
        Ok(mut v) => {
            state.allow.filter_models(&mut v);
            (
                code,
                [(header::CONTENT_TYPE, "application/json")],
                v.to_string(),
            )
                .into_response()
        }
        Err(_) => (code, bytes).into_response(),
    };

    state.tracker.record(RequestRecord {
        ip,
        token: caller,
        endpoint,
        status,
        latency_ms,
        key_label,
        ..Default::default()
    });

    response
}

/// Usage stats. Requires the admin token, which is separate from caller tokens.
pub async fn stats_endpoint(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !guard::admin_ok(&headers, &state.admin_token) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    let keys = state.keys.summary(chrono::Utc::now().timestamp());
    axum::Json(state.tracker.stats_with(state.rotation.summary(), keys)).into_response()
}

/// Upstream probe state on its own, for the alerting scripts. Requires the
/// admin token.
///
/// The alert needs to know whether the upstream itself is reachable before it
/// blames the proxy, and this is the only endpoint that answers that with a
/// measurement instead of a request-derived guess.
pub async fn upstream_health_endpoint(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    if !guard::admin_ok(&headers, &state.admin_token) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    axum::Json(state.tracker.probe_summary()).into_response()
}

#[derive(serde::Deserialize)]
pub struct FailuresQuery {
    /// Window to read, in minutes. Defaults to an hour, capped at a day.
    minutes: Option<u32>,
    /// Maximum rows to return. Defaults to 500, capped at 5000.
    limit: Option<u32>,
}

/// Raw failure rows for the alerting scripts. Requires the admin token.
///
/// The alert needs the individual failures, not only totals: a duration, a
/// per-caller breakdown and a worst-window figure all come from the rows.
pub async fn failures_endpoint(
    State(state): State<Arc<AppState>>,
    Query(params): Query<FailuresQuery>,
    headers: HeaderMap,
) -> Response {
    if !guard::admin_ok(&headers, &state.admin_token) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    let minutes = params.minutes.unwrap_or(60).clamp(1, 24 * 60);
    let limit = params.limit.unwrap_or(500).clamp(1, 5000);
    axum::Json(state.tracker.failures_since(minutes, limit)).into_response()
}

/// Issue an install its own caller token. The route is mounted only when
/// `INSTALL_PATH` names a path, and each client IP gets a daily request cap.
/// Usage is tracked per install label.
pub async fn issue_install(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // A stable label for the stats row. The configured path is deployment
    // state, so it must never reach the database.
    const ENDPOINT: &str = "install";
    // Stands in for an install id the route refused before reading one, so the
    // column stays NOT NULL without inventing a value.
    const UNKNOWN_INSTALL: &str = "-";
    let ip = guard::client_ip(&headers, peer.ip()).to_string();
    if !state.install_rate.allow(ip.clone()) {
        state.tracker.record_install(
            &ip,
            UNKNOWN_INSTALL,
            StatusCode::TOO_MANY_REQUESTS.as_u16(),
            Some("rate_limited"),
        );
        rejected(&state, ip, ENDPOINT, StatusCode::TOO_MANY_REQUESTS);
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "install rate limit exceeded for this address today",
        )
            .into_response();
    }
    let install_id = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("install_id")
                .and_then(|s| s.as_str())
                .map(str::to_string)
        });
    let Some(install_id) = install_id else {
        state.tracker.record_install(
            &ip,
            UNKNOWN_INSTALL,
            StatusCode::BAD_REQUEST.as_u16(),
            Some("bad_body"),
        );
        rejected(&state, ip, ENDPOINT, StatusCode::BAD_REQUEST);
        return (
            StatusCode::BAD_REQUEST,
            "body must be JSON with an install_id",
        )
            .into_response();
    };

    // The write lock covers the check, the write and the reload, so two
    // concurrent requests for one install cannot both mint a token.
    let issued = {
        let mut store = state.tokens.write().expect("tokens lock poisoned");
        install::issue_for(&mut store, &state.tokens_path, &install_id)
    };
    // Bounded before it reaches the database: a refused id can be any length the
    // body allows, and this column is for diagnosis, not storage.
    let recorded_id: String = install_id.chars().take(install::MAX_ID_LEN).collect();
    match issued {
        Ok(token) => {
            tracing::info!("issued a caller token to install `{install_id}`");
            state
                .tracker
                .record_install(&ip, &recorded_id, StatusCode::OK.as_u16(), None);
            axum::Json(serde_json::json!({ "token": token, "label": install_id })).into_response()
        }
        Err(refusal) => {
            let (status, message, reason) = match &refusal {
                install::Refusal::InvalidId => (
                    StatusCode::BAD_REQUEST,
                    "install_id must be 8 to 64 letters, digits, - or _",
                    "invalid_id",
                ),
                install::Refusal::AlreadyIssued => (
                    StatusCode::CONFLICT,
                    "a token was already issued for this install; an operator can revoke it",
                    "already_issued",
                ),
                install::Refusal::Failed(e) => {
                    tracing::error!("install token issuance failed: {e}");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "could not issue a token",
                        "write_failed",
                    )
                }
            };
            state
                .tracker
                .record_install(&ip, &recorded_id, status.as_u16(), Some(reason));
            rejected(&state, ip, ENDPOINT, status);
            (status, message).into_response()
        }
    }
}

#[derive(serde::Deserialize)]
pub struct RevokeRequest {
    label: String,
}

/// Revoke a caller token. Requires the admin token, which is why this path can
/// be a fixed name: knowing it grants nothing without the secret.
///
/// The write happens inside this process, as the service user. Editing the
/// tokens file from the host instead is what broke the `StateDirectory` idmap
/// and crash-looped the unit, so this route exists to make that unnecessary.
/// The store is reloaded while the write lock is held, so the revoked token
/// stops authenticating on its next request instead of at the next restart.
pub async fn revoke_endpoint(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !guard::admin_ok(&headers, &state.admin_token) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    let label = serde_json::from_slice::<RevokeRequest>(&body)
        .map(|r| r.label)
        .unwrap_or_default();
    if label.is_empty() {
        return (StatusCode::BAD_REQUEST, "body must be JSON with a label").into_response();
    }
    // The write lock covers the removal and the reload, so a request racing an
    // install cannot reload a store from a half-written file.
    let outcome = {
        let mut store = state.tokens.write().expect("tokens lock poisoned");
        tokens::revoke_and_reload(&mut store, &state.tokens_path, &label)
    };
    match outcome {
        Ok(()) => {
            tracing::info!("revoked caller token `{label}`");
            axum::Json(serde_json::json!({ "revoked": label })).into_response()
        }
        Err(tokens::RevokeError::Unknown) => {
            (StatusCode::NOT_FOUND, "no caller with that label").into_response()
        }
        Err(tokens::RevokeError::WouldEmpty) => (
            StatusCode::CONFLICT,
            "refusing to revoke the last caller: the proxy will not start on an empty tokens file",
        )
            .into_response(),
        Err(tokens::RevokeError::Failed(e)) => {
            tracing::error!("token revocation failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "could not revoke the token",
            )
                .into_response()
        }
    }
}

pub async fn health() -> &'static str {
    "ok"
}

#[cfg(test)]
pub(crate) mod rotation_tests {
    use super::*;
    use crate::allowlist::Allowlist;
    use crate::{caps, rate, rotation, tokens, tracker};
    use axum::body::Body;
    use axum::extract::ConnectInfo;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tower::ServiceExt;

    /// Scripted upstream: accepts any number of connections. Each request is
    /// drained, counted, and answered with `responder(request_number, bytes)`.
    /// A responder that returns bytes gets its socket closed after the write,
    /// which ends any proxied SSE body; one that returns nothing keeps the
    /// socket open, which is what makes an upstream hang.
    pub(crate) async fn spawn_scripted_upstream(
        responder: impl Fn(usize, &[u8]) -> Vec<u8> + Send + Sync + 'static,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let responder = Arc::new(responder);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let counter = Arc::clone(&counter);
                let responder = Arc::clone(&responder);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65_536];
                    let read = tokio::io::AsyncReadExt::read(&mut sock, &mut buf)
                        .await
                        .unwrap_or(0);
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    let payload = responder(n, &buf[..read]);
                    if !payload.is_empty() {
                        let _ = tokio::io::AsyncWriteExt::write_all(&mut sock, &payload).await;
                        drop(sock);
                    } else {
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                });
            }
        });
        (addr, count)
    }

    pub(crate) struct TestApp {
        pub(crate) state: Arc<AppState>,
        pub(crate) token: String,
        pub(crate) stats_db: std::path::PathBuf,
    }

    async fn test_app(upstream: SocketAddr, models: &[&str], attempt_timeout: Duration) -> TestApp {
        test_app_with_keys(
            upstream,
            models,
            attempt_timeout,
            &[("test", "test-upstream-key")],
        )
        .await
    }

    /// The same app with a key ring of its own, for the key-rotation paths.
    async fn test_app_with_keys(
        upstream: SocketAddr,
        models: &[&str],
        attempt_timeout: Duration,
        keys: &[(&str, &str)],
    ) -> TestApp {
        test_app_with_limit(
            upstream,
            models,
            attempt_timeout,
            keys,
            limit::InputLimit::for_tests(),
        )
        .await
    }

    /// The same app with a voice fallback pointing at a second upstream, so a
    /// test can watch which leg answered.
    async fn test_app_with_fallback(
        upstream: SocketAddr,
        fallback: SocketAddr,
        models: &[&str],
        fallback_models: &[&str],
    ) -> TestApp {
        let mut app = test_app(upstream, models, Duration::from_secs(5)).await;
        let base_url = format!("http://{fallback}");
        let joined = fallback_models.join(",");
        let state = Arc::get_mut(&mut app.state).expect("state is freshly built");
        state.audio_fallback = crate::audio::AudioFallback::from_values(
            Some(&base_url),
            Some("fallback-key"),
            Some(&joined),
        );
        app
    }

    /// The same app with an image list of its own, so a test can watch an id
    /// resolve on an image route.
    pub(crate) async fn test_app_with_images(
        upstream: SocketAddr,
        models: &[&str],
        images: &str,
    ) -> TestApp {
        let mut app = test_app(upstream, models, Duration::from_secs(5)).await;
        let state = Arc::get_mut(&mut app.state).expect("state is freshly built");
        state.allow = Allowlist::parse("m1,m2")
            .with_images(images)
            .with_audio("a1,a2");
        app
    }

    /// The same app with an input ceiling of its own, so a refusal test does not
    /// have to build a 200k-token body to reach the wall.
    async fn test_app_with_limit(
        upstream: SocketAddr,
        models: &[&str],
        attempt_timeout: Duration,
        keys: &[(&str, &str)],
        input_limit: limit::InputLimit,
    ) -> TestApp {
        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "keyproxy-rot-test-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let tokens_path = dir.join("tokens").to_string_lossy().to_string();
        let token = tokens::issue(&tokens_path, "test-caller").unwrap();
        let state = Arc::new(AppState {
            client: reqwest::Client::builder().build().unwrap(),
            keys: crate::keys::KeyRing::for_tests(keys, 0.5),
            admin_token: "test-admin".to_string(),
            tokens: std::sync::RwLock::new(tokens::TokenStore::load(&tokens_path).unwrap()),
            tokens_path,
            vision_swap: false,
            // Chat ids m1/m2, audio ids a1/a2. The two lists never overlap,
            // which is what keeps a voice note off a text model.
            allow: Allowlist::parse("m1,m2").with_audio("a1,a2"),
            audio_fallback: None,
            rotation: rotation::Rotation::for_tests(models, attempt_timeout),
            upstream: format!("http://{upstream}"),
            rate: rate::RateLimiter::new(10_000, Duration::from_secs(60)),
            install_rate: rate::RateLimiter::new(10_000, Duration::from_secs(86_400)),
            install_path: None,
            publisher_header: None,
            caps: caps::Caps {
                daily: 0,
                monthly: 0,
            },
            limit: input_limit,
            calibration: Arc::new(crate::calibrate::Calibration::new()),
            tracker: tracker::Tracker::new(&dir.join("stats.db").to_string_lossy()),
        });
        TestApp {
            state,
            token,
            stats_db: dir.join("stats.db"),
        }
    }

    fn chat_request(token: &str, model: &str, stream: bool) -> Request<Body> {
        let body = serde_json::json!({
            "model": model,
            "stream": stream,
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::from(body))
            .unwrap()
    }

    /// A chat request whose message carries an `input_audio` part.
    fn audio_chat_request(token: &str, model: &str) -> Request<Body> {
        let body = serde_json::json!({
            "model": model,
            "stream": true,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "what do you hear"},
                    {"type": "input_audio", "input_audio": {"data": "T2dnUw==", "format": "ogg"}}
                ]
            }]
        })
        .to_string();
        Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::from(body))
            .unwrap()
    }

    /// The tracker row is written after the proxied body finishes, so poll
    /// the sqlite db until it appears.
    #[derive(Debug)]
    pub(crate) struct TrackedRow {
        /// What the caller asked for, which an alias leaves in its own spelling.
        pub(crate) model_requested: String,
        pub(crate) model_used: String,
        pub(crate) status: u16,
        pub(crate) failure_reason: Option<String>,
        pub(crate) attempts: Option<u32>,
        pub(crate) plan_len: Option<u32>,
        pub(crate) served_index: Option<u32>,
        pub(crate) truncated: bool,
        pub(crate) key_label: String,
        pub(crate) estimated_tokens: Option<u64>,
    }

    async fn tracked_row(db: &std::path::Path) -> TrackedRow {
        tracked_row_for(db, "/v1/chat/completions").await
    }

    /// The latest row for one endpoint. The passthrough writes its own endpoint
    /// label, so a refusal test has to name the route it posted to.
    pub(crate) async fn tracked_row_for(db: &std::path::Path, endpoint: &str) -> TrackedRow {
        for _ in 0..60 {
            if let Ok(conn) = rusqlite::Connection::open(db) {
                let row = conn.query_row(
                    "SELECT COALESCE(model_requested, ''), model_used, status, failure_reason, \
                            attempts, plan_len, served_index, truncated, key_label, \
                            estimated_tokens \
                     FROM requests WHERE endpoint = ?1 \
                     ORDER BY id DESC LIMIT 1",
                    [endpoint],
                    |row| {
                        Ok(TrackedRow {
                            model_requested: row.get(0)?,
                            model_used: row.get(1)?,
                            status: row.get(2)?,
                            failure_reason: row.get(3)?,
                            attempts: row.get::<_, Option<i64>>(4)?.map(|n| n as u32),
                            plan_len: row.get::<_, Option<i64>>(5)?.map(|n| n as u32),
                            served_index: row.get::<_, Option<i64>>(6)?.map(|n| n as u32),
                            truncated: row.get::<_, i64>(7)? != 0,
                            key_label: row.get(8)?,
                            estimated_tokens: row.get::<_, Option<i64>>(9)?.map(|n| n as u64),
                        })
                    },
                );
                if let Ok(answer) = row {
                    return answer;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("no tracker row appeared for {} on {endpoint}", db.display());
    }

    async fn tracked_model_used(db: &std::path::Path) -> (String, u16) {
        let row = tracked_row(db).await;
        (row.model_used, row.status)
    }

    pub(crate) fn sse_ok(model: &str) -> Vec<u8> {
        let head =
            b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n";
        let events = format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"hi from {model}\"}}}}]}}\n\n\
             data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}}}\n\n\
             data: [DONE]\n\n"
        );
        let mut raw = head.to_vec();
        raw.extend_from_slice(events.as_bytes());
        raw
    }

    pub(crate) fn http_error(status: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// A 200 with a JSON body, for the routes that answer with a document.
    pub(crate) fn json_ok(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// The model id a raw request carried, read off its JSON body.
    pub(crate) fn body_model(raw: &[u8]) -> String {
        let text = String::from_utf8_lossy(raw);
        let body = text.split("\r\n\r\n").nth(1).unwrap_or("");
        serde_json::from_str::<serde_json::Value>(body)
            .ok()
            .and_then(|v| v.get("model").and_then(|m| m.as_str().map(str::to_string)))
            .unwrap_or_default()
    }

    /// The request line a raw request carried, so a test can read the path the
    /// upstream was asked for.
    pub(crate) fn request_line(raw: &[u8]) -> String {
        String::from_utf8_lossy(raw)
            .lines()
            .next()
            .unwrap_or("")
            .to_string()
    }

    /// A JSON POST to any `/v1/*` route, for the pass-through tests.
    pub(crate) fn json_post(token: &str, path: &str, model: &str) -> Request<Body> {
        let body = serde_json::json!({"model": model, "prompt": "hi"}).to_string();
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::from(body))
            .unwrap()
    }

    /// A GET of one model id, which the pass-through rewrites in the path.
    pub(crate) fn models_get(token: &str, id: &str) -> Request<Body> {
        Request::builder()
            .method("GET")
            .uri(format!("/v1/models/{id}"))
            .header("authorization", format!("Bearer {token}"))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::empty())
            .unwrap()
    }

    /// The same, with one extra response header, for the rate-limit hint.
    fn http_error_with(status: &str, body: &str, extra: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n{extra}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    /// The upstream key the raw request carried, so a scripted upstream can
    /// answer per key.
    fn bearer_key(raw: &[u8]) -> String {
        let text = String::from_utf8_lossy(raw);
        text.lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("authorization") {
                    Some(value.trim().trim_start_matches("Bearer ").to_string())
                } else {
                    None
                }
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn rotates_to_the_next_candidate_after_an_upstream_5xx() {
        let (upstream, _count) = spawn_scripted_upstream(|n, _| {
            if n == 0 {
                http_error("503 Service Unavailable", "{}")
            } else {
                sse_ok("m2")
            }
        })
        .await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("x-keyproxy-model")
                .and_then(|v| v.to_str().ok()),
            Some("m2")
        );
        let body = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("hi from m2"), "body was: {text}");
        assert!(
            text.contains("\"total_tokens\":3"),
            "usage must be relayed: {text}"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.model_used, "m2");
        assert_eq!(row.status, 200);
        // Rotation engaged, and the row says so: this is what makes a
        // "rotation is working" claim checkable instead of assumed.
        assert_eq!(row.served_index, Some(1));
        assert_eq!(row.attempts, Some(2));
        assert_eq!(row.plan_len, Some(2));
        assert_eq!(row.failure_reason, None);
        assert!(!row.truncated);
    }

    /// `stream_options` is what makes the final stream chunk carry token
    /// usage. Without `stream` one publisher answers 400 and another hangs
    /// until the attempt budget runs out, so it goes upstream only when the
    /// caller asked to stream. Both requests here are answered with SSE; what
    /// is checked is the body that left the proxy.
    #[tokio::test]
    async fn stream_options_goes_upstream_only_on_a_streaming_request() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let (upstream, _count) = spawn_scripted_upstream(move |_, raw| {
            let text = String::from_utf8_lossy(raw).to_string();
            let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
            recorder.lock().unwrap().push(body);
            sse_ok("m1")
        })
        .await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let token = app.token.clone();

        for stream in [false, true] {
            let response = crate::build_router(Arc::clone(&app.state))
                .oneshot(chat_request(&token, "m1", stream))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let bodies = seen.lock().unwrap();
        assert_eq!(bodies.len(), 2, "expected one upstream call per request");
        assert!(
            !bodies[0].contains("stream_options"),
            "a non-streaming request carried stream_options: {}",
            bodies[0]
        );
        assert!(
            bodies[1].contains("stream_options"),
            "a streaming request lost stream_options: {}",
            bodies[1]
        );
    }

    #[test]
    fn detects_audio_parts_and_ignores_text() {
        let with_audio = serde_json::json!({
            "messages": [{"role": "user", "content": [
                {"type": "text", "text": "what is this"},
                {"type": "input_audio", "input_audio": {"data": "AAAA", "format": "ogg"}}
            ]}]
        });
        assert!(carries_audio(&with_audio));

        let text_only = serde_json::json!({
            "messages": [{"role": "user", "content": "hi"}]
        });
        assert!(!carries_audio(&text_only));

        assert!(!carries_audio(&serde_json::json!({})));
    }

    /// Audio is a narrow capability: an audio request is tried on the audio
    /// list only, never on a rotation candidate. The rotation list here is
    /// text models, so a fallback onto one would answer a voice note with a
    /// model that cannot read it.
    #[tokio::test]
    async fn an_audio_request_falls_back_only_within_the_audio_list() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let (upstream, count) = spawn_scripted_upstream(move |n, raw| {
            let text = String::from_utf8_lossy(raw).to_string();
            let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
            let model = serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("model").and_then(|m| m.as_str().map(str::to_string)))
                .unwrap_or_default();
            recorder.lock().unwrap().push(model);
            if n == 0 {
                http_error("503 Service Unavailable", "{}")
            } else {
                sse_ok("a2")
            }
        })
        .await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(audio_chat_request(&token, "a1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["a1".to_string(), "a2".to_string()],
            "an audio request must be tried on the audio list only"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.model_used, "a2");
        assert_eq!(row.attempts, Some(2));
        assert_eq!(row.plan_len, Some(2));
        assert_eq!(row.served_index, Some(1));
    }

    /// When every audio model on the primary upstream fails, the voice
    /// fallback answers. The fallback is a separate upstream with its own key,
    /// and the row says so instead of borrowing a primary key label.
    #[tokio::test]
    async fn a_voice_request_falls_back_to_the_second_upstream() {
        let primary_body = Arc::new(std::sync::Mutex::new(Vec::new()));
        let primary_recorder = Arc::clone(&primary_body);
        let (primary, primary_count) = spawn_scripted_upstream(move |_, bytes| {
            primary_recorder.lock().unwrap().extend_from_slice(bytes);
            http_error("503 Service Unavailable", "{\"error\":\"down\"}")
        })
        .await;
        let fallback_body = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fallback_recorder = Arc::clone(&fallback_body);
        let (fallback, fallback_count) = spawn_scripted_upstream(move |_, bytes| {
            fallback_recorder.lock().unwrap().extend_from_slice(bytes);
            sse_ok("ms/speech-1")
        })
        .await;
        let app = test_app_with_fallback(primary, fallback, &["m1"], &["ms/speech-1"]).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(audio_chat_request(&token, "a1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Both audio ids on the primary are tried first, and only then does
        // the request move to the voice fallback.
        assert_eq!(primary_count.load(Ordering::SeqCst), 2);
        assert_eq!(fallback_count.load(Ordering::SeqCst), 1);

        // The primary upstream gets the caller's own shape, bare base64.
        let sent_primary = String::from_utf8_lossy(&primary_body.lock().unwrap()).to_string();
        assert!(
            sent_primary.contains(r#""data":"T2dnUw==""#),
            "the primary upstream must receive the caller's audio unchanged: {sent_primary}"
        );
        // The fallback upstream reads the field as a URL or data URI, so the
        // part is converted on the way out. Bare base64 comes back 400 there.
        let sent_fallback = String::from_utf8_lossy(&fallback_body.lock().unwrap()).to_string();
        assert!(
            sent_fallback.contains(r#""data":"data:audio/ogg;base64,T2dnUw==""#),
            "the fallback upstream must receive a data URI: {sent_fallback}"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.model_used, "ms/speech-1");
        assert_eq!(row.status, 200);
        assert_eq!(row.key_label, "audio-fallback");
    }

    /// A text request never reaches the voice fallback, even when the primary
    /// fails for it: the fallback upstream is configured for voice only.
    #[tokio::test]
    async fn a_text_request_never_reaches_the_voice_fallback() {
        let (primary, _primary_count) = spawn_scripted_upstream(|_, _| {
            http_error("503 Service Unavailable", "{\"error\":\"down\"}")
        })
        .await;
        let (fallback, fallback_count) =
            spawn_scripted_upstream(|_, _| sse_ok("ms/speech-1")).await;
        let app = test_app_with_fallback(primary, fallback, &["m1"], &["ms/speech-1"]).await;
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            fallback_count.load(Ordering::SeqCst),
            0,
            "a text request must never be sent to the voice fallback"
        );
    }

    /// The split runs both ways: a chat id is not an audio model, and an
    /// audio id is not a chat model. A text prompt naming a voice model is
    /// refused before anything goes upstream.
    #[tokio::test]
    async fn a_chat_id_is_refused_on_an_audio_request() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("a1")).await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(audio_chat_request(&token, "m1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    /// And the reverse: an audio id on a plain text request is refused, so
    /// mimo can only ever answer a request that carries audio.
    #[tokio::test]
    async fn an_audio_id_is_refused_on_a_text_request() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("a1")).await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "a1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }

    /// An id the docs ship for voice, which this proxy does not serve under that
    /// spelling, is served on the head of the voice chain instead of refused.
    /// The upstream sees the served id, and the row keeps the caller's spelling
    /// next to what answered.
    #[tokio::test]
    async fn an_audio_alias_is_served_on_the_voice_chain() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let (upstream, count) = spawn_scripted_upstream(move |_, raw| {
            recorder.lock().unwrap().push(body_model(raw));
            sse_ok("a1")
        })
        .await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(audio_chat_request(&token, "qwen3.8-omni-flash"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["a1".to_string()],
            "a voice alias must reach the upstream as the head of the voice chain"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.model_requested, "qwen3.8-omni-flash");
        assert_eq!(row.model_used, "a1");
        assert_eq!(row.status, 200);
    }

    /// An id that is neither served nor an alias is still refused, so the
    /// alias table does not turn into an open door.
    #[tokio::test]
    async fn an_id_that_is_neither_served_nor_an_alias_is_refused() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "gpt-4", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(count.load(Ordering::SeqCst), 0);

        let row = tracked_row(&db).await;
        assert_eq!(row.model_requested, "gpt-4");
        assert_eq!(row.status, 403);
    }

    #[tokio::test]
    async fn never_retries_a_4xx() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| {
            http_error("400 Bad Request", "{\"error\":{\"message\":\"bad\"}}")
        })
        .await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(
            response
                .headers()
                .get("x-keyproxy-model")
                .and_then(|v| v.to_str().ok()),
            Some("m1")
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "a 4xx must never be retried"
        );

        let (model_used, status) = tracked_model_used(&db).await;
        assert_eq!(model_used, "m1");
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn all_candidates_dead_returns_502_without_a_served_model() {
        // The upstream accepts and never answers: both attempts hit the
        // header budget and the proxy answers for itself.
        let (upstream, count) = spawn_scripted_upstream(|_, _| Vec::new()).await;
        let app = test_app(upstream, &["m1", "m2"], Duration::from_secs(1)).await;
        let db = app.stats_db.clone();

        // Non-streaming body: header budget only, no first-token wait.
        let body = serde_json::json!({
            "model": "m1",
            "messages": [{"role": "user", "content": "hi"}]
        })
        .to_string();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {}", app.token))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::from(body))
            .unwrap();
        let started = std::time::Instant::now();
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(request)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(response.headers().get("x-keyproxy-model"), None);
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "both candidates must be tried"
        );
        // Two 1s header budgets, not the 600s client timeout.
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "dead attempts must be bounded, took {:?}",
            started.elapsed()
        );

        let (model_used, status) = tracked_model_used(&db).await;
        assert_eq!(
            model_used, "m1",
            "nothing served: the requested model stands"
        );
        assert_eq!(status, 502);
    }

    #[tokio::test]
    async fn a_refused_key_is_replaced_and_the_candidate_is_retried_on_it() {
        // The first key is refused (`402`). That is about the account, not the
        // request, so the same candidate must be retried on the next key
        // instead of the caller seeing the error.
        let (upstream, count) = spawn_scripted_upstream(|_, raw| {
            if bearer_key(raw) == "key-a" {
                http_error("402 Payment Required", "{\"error\":\"out of credit\"}")
            } else {
                sse_ok("m1")
            }
        })
        .await;
        let app = test_app_with_keys(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("primary", "key-a"), ("secondary", "key-b")],
        )
        .await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert!(
            String::from_utf8_lossy(&body).contains("hi from m1"),
            "the retry on the second key must reach the caller"
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "one attempt per key: the refused key, then the healthy one"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 200);
        assert_eq!(
            row.key_label, "secondary",
            "the row must name the key that actually served"
        );
        // The candidate list held one model, so this is the key rotation
        // alone: served_index stays 0 and the plan length stays 1.
        assert_eq!(row.served_index, Some(0));
        assert_eq!(row.plan_len, Some(1));
        assert_eq!(row.failure_reason, None);
    }

    #[tokio::test]
    async fn a_capacity_saturated_429_never_burns_a_healthy_key() {
        // The upstream is out of platform capacity. That is not this key's
        // limit, so switching keys would burn a healthy account for nothing.
        let (upstream, count) = spawn_scripted_upstream(|_, _| {
            http_error(
                "429 Too Many Requests",
                "{\"error\":{\"code\":\"capacity_saturated\"}}",
            )
        })
        .await;
        let app = test_app_with_keys(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("primary", "key-a"), ("secondary", "key-b")],
        )
        .await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "capacity saturation must not send the request again"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 429);
        assert_eq!(
            row.key_label, "primary",
            "the key must not have changed for a platform-wide limit"
        );
        assert_eq!(row.attempts, Some(1));
    }

    #[tokio::test]
    async fn a_per_key_rate_limit_moves_to_the_next_key() {
        // This 429 names the key's own window, so the next key is tried. The
        // caller gets an answer and the limited key sits out the reset window.
        let (upstream, count) = spawn_scripted_upstream(|_, raw| {
            if bearer_key(raw) == "key-a" {
                http_error_with(
                    "429 Too Many Requests",
                    "{\"error\":{\"code\":\"rate_limited\"}}",
                    "x-ratelimit-reset: 60\r\n",
                )
            } else {
                sse_ok("m1")
            }
        })
        .await;
        let app = test_app_with_keys(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("primary", "key-a"), ("secondary", "key-b")],
        )
        .await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert!(
            String::from_utf8_lossy(&body).contains("hi from m1"),
            "the retry on the second key must reach the caller"
        );
        assert_eq!(count.load(Ordering::SeqCst), 2);

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 200);
        assert_eq!(row.key_label, "secondary");
    }

    /// The byte offset just past the request head's blank line.
    fn head_end(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
    }

    /// The `content-length` of a request whose head is in `buf`.
    fn declared_body_len(buf: &[u8]) -> usize {
        String::from_utf8_lossy(buf)
            .lines()
            .take_while(|line| !line.is_empty())
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())?
            })
            .unwrap_or(0)
    }

    /// An upstream that reads the whole request before answering, so a test can
    /// post a body larger than a single socket read.
    async fn spawn_draining_upstream(
        responder: impl Fn(usize) -> Vec<u8> + Send + Sync + 'static,
    ) -> (SocketAddr, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&count);
        let responder = Arc::new(responder);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let counter = Arc::clone(&counter);
                let responder = Arc::clone(&responder);
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = vec![0u8; 65_536];
                    let mut want: Option<usize> = None;
                    loop {
                        let n = tokio::io::AsyncReadExt::read(&mut sock, &mut chunk)
                            .await
                            .unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&chunk[..n]);
                        if want.is_none() {
                            if let Some(head) = head_end(&buf) {
                                want = Some(head + declared_body_len(&buf));
                            }
                        }
                        if want.is_some_and(|end| buf.len() >= end) {
                            break;
                        }
                    }
                    let n = counter.fetch_add(1, Ordering::SeqCst);
                    let payload = responder(n);
                    if !payload.is_empty() {
                        let _ = tokio::io::AsyncWriteExt::write_all(&mut sock, &payload).await;
                    }
                    let _ = tokio::io::AsyncWriteExt::shutdown(&mut sock).await;
                });
            }
        });
        (addr, count)
    }

    /// A POST carrying a caller token, for bodies a test builds itself.
    fn post_json(uri: &str, token: &str, body: String) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .extension(ConnectInfo(
                "127.0.0.1:65000".parse::<SocketAddr>().unwrap(),
            ))
            .body(Body::from(body))
            .unwrap()
    }

    /// A chat body carrying one base64 image part of `image_bytes` characters.
    /// The image is the point: it costs about 1,000 tokens however long it is.
    fn chat_body_with_image(model: &str, image_bytes: usize) -> String {
        let image = "A".repeat(image_bytes);
        serde_json::json!({
            "model": model,
            "stream": true,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "what is in this screenshot"},
                    {"type": "image_url", "image_url": {
                        "url": format!("data:image/png;base64,{image}")
                    }}
                ]
            }]
        })
        .to_string()
    }

    /// A tiny ceiling, so a refusal test does not build a 200k-token body.
    fn small_ceiling() -> limit::InputLimit {
        limit::InputLimit::parse(Some("100"))
    }

    #[tokio::test]
    async fn an_oversized_request_is_refused_before_the_upstream() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app_with_limit(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("test", "test-upstream-key")],
            small_ceiling(),
        )
        .await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let body = serde_json::json!({
            "model": "m1",
            "stream": true,
            "messages": [{"role": "user", "content": "x".repeat(4000)}]
        })
        .to_string();
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, body))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["error"]["type"], "input_too_large");
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "an oversized body must never reach the upstream"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 413);
        assert_eq!(row.failure_reason.as_deref(), Some("input_too_large"));
    }

    /// A chat body whose estimate is exactly `tokens`: one text message of
    /// `tokens * 4` characters, since text is charged a quarter of its length.
    fn chat_body_estimating(tokens: u64) -> String {
        serde_json::json!({
            "model": "m1",
            "stream": true,
            "messages": [{"role": "user", "content": "x".repeat((tokens * 4) as usize)}]
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_calibrated_caller_is_refused_below_the_raw_ceiling() {
        // Draining upstream: the bodies here are hundreds of KB, and the
        // scripted one answers after a single 64 KB read, which races the
        // proxy's write and turns a pass into a transport 502.
        let (upstream, count) = spawn_draining_upstream(|_| sse_ok("m1")).await;
        let app = test_app_with_limit(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("test", "test-upstream-key")],
            limit::InputLimit::for_tests(),
        )
        .await;
        // The density measured from core-dev-2's live traffic. Seed it rather
        // than replaying 20 requests to earn it.
        app.state
            .calibration
            .seed(&[("test-caller".to_string(), 1.37, 100)]);
        let token = app.token.clone();
        let db = app.stats_db.clone();

        // 140k * 1.37 = 191,800, still under the 200k ceiling, so it goes.
        let under = chat_body_estimating(140_000);
        assert_eq!(crate::limit::estimate_tokens(under.as_bytes()), 140_000);
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, under))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a calibrated estimate under the ceiling must be served"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);

        // 146k * 1.37 = 200,020, past the ceiling, so it is refused even
        // though the raw estimate of 146k is well under 200k.
        let over = chat_body_estimating(146_000);
        assert_eq!(crate::limit::estimate_tokens(over.as_bytes()), 146_000);
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, over))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "the refused body must not reach the upstream"
        );

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 413);
        // The row records the scaled number, because that is the honest one:
        // the estimate the ceiling was actually applied to. Compared as a
        // range because 146000 * 1.37 is a float.
        let recorded = row.estimated_tokens.unwrap();
        assert!(
            (200_000..=200_100).contains(&recorded),
            "the row should carry the scaled estimate, got {recorded}"
        );
    }

    #[tokio::test]
    async fn an_uncalibrated_caller_gates_on_the_raw_estimate() {
        let (upstream, count) = spawn_draining_upstream(|_| sse_ok("m1")).await;
        let app = test_app_with_limit(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("test", "test-upstream-key")],
            limit::InputLimit::for_tests(),
        )
        .await;
        let token = app.token.clone();

        // No history, so the factor is 1.0 and the same 146k estimate that the
        // calibrated caller above is refused for passes here.
        let body = chat_body_estimating(146_000);
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn an_image_heavy_request_under_the_ceiling_passes() {
        // 800 KB of base64 is about 200k tokens by `bytes / 4` and about 1,000
        // by the per-part estimate, so this is the regression test for the
        // estimate that would have refused a single screenshot.
        let (upstream, count) = spawn_draining_upstream(|_| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let body = chat_body_with_image("m1", 800 * 1024);
        assert!(
            (body.len() as u64).div_ceil(4) >= 200_000,
            "this body must be one the bytes / 4 estimate would have refused"
        );
        let expected_estimate = crate::limit::estimate_tokens(body.as_bytes());

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 200);
        assert_eq!(row.failure_reason, None);
        // The served row carries the estimate the ceiling was measured
        // against, so the density feedback loop has something to correct on
        // the next request from this caller.
        assert_eq!(
            row.estimated_tokens,
            Some(expected_estimate),
            "a served row must record the estimate it was admitted on"
        );
    }

    #[tokio::test]
    async fn an_image_body_above_two_megabytes_reaches_the_handler() {
        // Before the raised body limit this dies at axum's extractor with a
        // plain text 413 and leaves no stats row at all.
        let (upstream, count) = spawn_draining_upstream(|_| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let body = chat_body_with_image("m1", 3 * 1024 * 1024);
        assert!(body.len() > 2 * 1024 * 1024);

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/chat/completions", &token, body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 200);
    }

    #[tokio::test]
    async fn a_normal_request_still_passes_the_size_gate() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&token, "m1", true))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);

        let row = tracked_row(&db).await;
        assert_eq!(row.status, 200);
    }

    #[tokio::test]
    async fn an_oversized_passthrough_request_is_refused() {
        // The ceiling has to hold on the fallback too, or the chat route is
        // simply a decoy and every other path is a way around it.
        let (upstream, count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app_with_limit(
            upstream,
            &["m1"],
            Duration::from_secs(5),
            &[("test", "test-upstream-key")],
            small_ceiling(),
        )
        .await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let body = serde_json::json!({
            "model": "m1",
            "input": "x".repeat(4000)
        })
        .to_string();
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/v1/responses", &token, body))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(count.load(Ordering::SeqCst), 0);

        let row = tracked_row_for(&db, "/v1/* passthrough").await;
        assert_eq!(row.status, 413);
        assert_eq!(row.failure_reason.as_deref(), Some("input_too_large"));
    }

    /// With no path configured the install route is not registered at all, so a
    /// post to the old path falls through to the upstream passthrough.
    #[tokio::test]
    async fn install_route_is_absent_when_no_path_is_configured() {
        let (upstream, _count) = spawn_scripted_upstream(|_, _| sse_ok("m2")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json(
                "/install-test",
                &token,
                serde_json::json!({ "install_id": "abcdefgh" }).to_string(),
            ))
            .await
            .unwrap();
        let body = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert!(
            !text.contains("\"token\""),
            "install route answered with no path configured: {text}"
        );
    }

    /// With a path configured the route issues a token there, and the stats row
    /// carries the stable label `install` rather than the configured path.
    #[tokio::test]
    async fn install_route_is_mounted_at_the_configured_path() {
        let (upstream, _count) = spawn_scripted_upstream(|_, _| sse_ok("m2")).await;
        let mut app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        let db = app.stats_db.clone();
        Arc::get_mut(&mut app.state).unwrap().install_path = Some("/install-test".to_string());
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json(
                "/install-test",
                &token,
                serde_json::json!({ "install_id": "abcdefgh" }).to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("\"token\""), "body was: {text}");
        assert!(text.contains("abcdefgh"), "body was: {text}");

        // A refused request is the one that writes a row, and that row must name
        // the route by its stable label, never by the configured path.
        let refused = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/install-test", &token, "{}".to_string()))
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        let row = tracked_row_for(&db, "install").await;
        assert_eq!(row.status, 400);
    }

    /// One recorded install attempt. `installs` is a table of its own, so the
    /// polling is for the write to land, not for a proxied body to finish.
    #[derive(Debug)]
    pub(crate) struct InstallRow {
        pub(crate) install_id: String,
        pub(crate) status: u16,
        pub(crate) reason: Option<String>,
    }

    async fn install_row_for(db: &std::path::Path, install_id: &str) -> Option<InstallRow> {
        for _ in 0..60 {
            if let Ok(conn) = rusqlite::Connection::open(db) {
                let row = conn.query_row(
                    "SELECT install_id, status, reason FROM installs \
                     WHERE install_id = ?1 ORDER BY id DESC LIMIT 1",
                    [install_id],
                    |row| {
                        Ok(InstallRow {
                            install_id: row.get(0)?,
                            status: row.get(1)?,
                            reason: row.get(2)?,
                        })
                    },
                );
                if let Ok(answer) = row {
                    return Some(answer);
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    async fn install_app() -> TestApp {
        let (upstream, _count) = spawn_scripted_upstream(|_, _| sse_ok("m2")).await;
        let mut app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        Arc::get_mut(&mut app.state).unwrap().install_path = Some("/install-test".to_string());
        app
    }

    async fn post_install(app: &TestApp, install_id: &str) -> StatusCode {
        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json(
                "/install-test",
                &app.token,
                serde_json::json!({ "install_id": install_id }).to_string(),
            ))
            .await
            .unwrap();
        response.status()
    }

    /// A mint leaves a row naming the install. Before this the success path wrote
    /// nothing at all, so the number of keys issued could not be counted.
    #[tokio::test]
    async fn a_mint_is_recorded_with_its_install_id() {
        let app = install_app().await;
        let db = app.stats_db.clone();

        assert_eq!(post_install(&app, "mint-recorded").await, StatusCode::OK);

        let row = install_row_for(&db, "mint-recorded")
            .await
            .expect("a mint must leave an installs row");
        assert_eq!(row.install_id, "mint-recorded");
        assert_eq!(row.status, 200);
        assert_eq!(row.reason, None, "a served mint carries no refusal reason");
    }

    /// A refusal is recorded too, with the reason that tells it apart from a
    /// mint, so the route's whole history is one table.
    #[tokio::test]
    async fn a_refused_mint_is_recorded_with_its_reason() {
        let app = install_app().await;
        let db = app.stats_db.clone();

        assert_eq!(post_install(&app, "already-there").await, StatusCode::OK);
        assert_eq!(
            post_install(&app, "already-there").await,
            StatusCode::CONFLICT
        );

        let row = install_row_for(&db, "already-there")
            .await
            .expect("a refusal must leave an installs row");
        assert_eq!(row.status, 409);
        assert_eq!(row.reason.as_deref(), Some("already_issued"));
    }

    /// The split that separates a key farm from users: minted keys that never
    /// called, against minted keys that called five times or more.
    #[tokio::test]
    async fn install_summary_splits_unused_from_active() {
        let app = install_app().await;
        let db = app.stats_db.clone();

        app.state
            .tracker
            .record_install("10.0.0.1", "never-called", 200, None);
        app.state
            .tracker
            .record_install("10.0.0.2", "busy-caller", 200, None);
        for _ in 0..5 {
            app.state.tracker.record(RequestRecord {
                ip: "10.0.0.2".to_string(),
                token: "busy-caller".to_string(),
                endpoint: "/v1/chat/completions",
                status: 200,
                ..Default::default()
            });
        }

        let conn = rusqlite::Connection::open(&db).unwrap();
        let summary = tracker::InstallSummary::from_db(&conn);
        assert_eq!(summary.minted_24h, 2);
        assert_eq!(summary.minted_total, 2);
        assert_eq!(summary.distinct_ips_24h, 2);
        assert_eq!(summary.refused_24h, 0);
        assert_eq!(summary.unused_24h, 1, "the key that never called");
        assert_eq!(summary.active_24h, 1, "the key that called five times");
    }

    /// The regression the separate table exists to prevent. `/stats` and the
    /// public snapshot count every `requests` row with no endpoint filter, so a
    /// mint row there would publish one extra request and one extra caller per
    /// install.
    #[tokio::test]
    async fn a_mint_does_not_change_request_totals() {
        let app = install_app().await;
        let db = app.stats_db.clone();

        assert_eq!(post_install(&app, "no-inflation").await, StatusCode::OK);

        let conn = rusqlite::Connection::open(&db).unwrap();
        let stats = tracker::Stats::from_db(&conn);
        assert_eq!(stats.total_requests, 0, "a mint is not a request");
        assert_eq!(stats.unique_ips, 0, "a mint is not a caller");
        assert_eq!(stats.installs.minted_total, 1);
    }

    /// The admin route is the only supported way to revoke: it writes as the
    /// service user and reloads the store, so the token dies without a restart.
    #[tokio::test]
    async fn admin_revoke_drops_the_caller_token() {
        let (upstream, _count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;
        // A second caller, so revoking one does not try to empty the file.
        let doomed = tokens::issue(&app.state.tokens_path, "doomed").unwrap();

        let body = serde_json::json!({ "label": "doomed" }).to_string();

        // A caller token is not an admin token.
        let unauth = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/admin/revoke", &app.token, body.clone()))
            .await
            .unwrap();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        // An unknown label is reported, not silently accepted.
        let missing = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json(
                "/admin/revoke",
                "test-admin",
                serde_json::json!({ "label": "nobody" }).to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json("/admin/revoke", "test-admin", body))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let echoed = BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes();
        assert!(String::from_utf8_lossy(&echoed).contains("doomed"));

        // The reload happened in-process: the revoked token is refused now,
        // while the other caller still works.
        let dead = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&doomed, "m1", false))
            .await
            .unwrap();
        assert_eq!(dead.status(), StatusCode::UNAUTHORIZED);

        let alive = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&app.token, "m1", false))
            .await
            .unwrap();
        assert_ne!(alive.status(), StatusCode::UNAUTHORIZED);
    }

    /// Revoking the last entry would leave a file the proxy refuses to start
    /// on, so the route refuses it and the token keeps working.
    #[tokio::test]
    async fn admin_revoke_refuses_to_empty_the_tokens_file() {
        let (upstream, _count) = spawn_scripted_upstream(|_, _| sse_ok("m1")).await;
        let app = test_app(upstream, &["m1"], Duration::from_secs(5)).await;

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(post_json(
                "/admin/revoke",
                "test-admin",
                serde_json::json!({ "label": "test-caller" }).to_string(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);

        let alive = crate::build_router(Arc::clone(&app.state))
            .oneshot(chat_request(&app.token, "m1", false))
            .await
            .unwrap();
        assert_ne!(alive.status(), StatusCode::UNAUTHORIZED);
    }
}
