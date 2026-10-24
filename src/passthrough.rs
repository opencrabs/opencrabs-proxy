//! Pass-through for every other `/v1/*` route (embeddings, responses, and so
//! on). The caller gate and the usage row apply as they do for chat. The
//! request body and response body are not inspected, so no token usage is read.

use axum::{
    body::{Body, Bytes},
    extract::{ConnectInfo, State},
    http::{header, HeaderMap, Method, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use std::{net::SocketAddr, sync::Arc, time::Instant};

use crate::{guard, handlers, limit, tracker::RequestRecord, AppState};

const ENDPOINT: &str = "/v1/* passthrough";

/// The caller's path and query, with the path checked for a dot segment.
///
/// WHATWG URL parsing, which reqwest uses, resolves `.` and `..` segments. A
/// path such as `/v1/../../admin` would therefore leave `/v1/` and reach an
/// arbitrary upstream route with our upstream key attached. `%2e` spells a dot
/// too, so each segment is percent-decoded before it is judged. Returns `None`
/// when any decoded segment is `.` or `..`, or carries a byte that would change
/// how the URL parses (`/`, `\`, `#`, `?`, NUL, or a control byte).
fn safe_path_and_query(uri: &Uri) -> Option<String> {
    let raw = uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or_else(|| uri.path());
    let (path, query) = match raw.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (raw, None),
    };
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        let decoded = percent_decode(segment);
        if decoded == b"." || decoded == b".." {
            return None;
        }
        if decoded
            .iter()
            .any(|&b| matches!(b, b'/' | b'\\' | b'#' | b'?' | 0) || b < 0x20 || b == 0x7f)
        {
            return None;
        }
    }
    Some(match query {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    })
}

/// Percent-decode one path segment. A malformed escape is kept verbatim, so a
/// broken `%` sequence cannot smuggle a byte past the segment check.
fn percent_decode(segment: &str) -> Vec<u8> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                out.push(high << 4 | low);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub async fn passthrough(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(path_and_query) = safe_path_and_query(&uri) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    if !path_and_query.starts_with("/v1/") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let ip = guard::client_ip(&headers, peer.ip()).to_string();

    let caller = match guard::gate(&state, &headers) {
        Ok(label) => label,
        Err(rejection) => {
            handlers::rejected(&state, ip, ENDPOINT, rejection.status());
            return *rejection;
        }
    };

    // Computed once, so the same number both gates the request and lands in
    // its stats row.
    let estimate = limit::estimate_tokens(&body);
    let factor = state.calibration.factor(&caller);
    if state.limit.exceeded_scaled(estimate, factor) {
        let scaled = (estimate as f64 * factor).ceil() as u64;
        return handlers::refuse_input(&state, ip, caller, ENDPOINT, scaled, body.len());
    }

    // Only served model ids may be named, in the path or in the JSON body. An
    // id the client names that this proxy serves under another spelling, but
    // that names the capability of the route, is resolved to the served id
    // instead of refused. Image routes take the image list, audio routes the
    // audio list, every other route the chat list. The path and the body are
    // rewritten only when the id resolved elsewhere, so a request naming a
    // served id is forwarded byte for byte.
    let mut path_and_query = path_and_query;
    let mut body = body;
    if let Some(id) = uri.path().strip_prefix("/v1/models/") {
        let Some(served) = state.allow.resolve_for_path(uri.path(), id) else {
            return handlers::refuse_model(&state, ip, caller, ENDPOINT, id);
        };
        if served != id {
            tracing::info!("model alias: `/v1/models/{id}` is served as `{served}`");
            path_and_query = match path_and_query.split_once('?') {
                Some((_, query)) => format!("/v1/models/{served}?{query}"),
                None => format!("/v1/models/{served}"),
            };
        }
    }
    let parsed = serde_json::from_slice::<serde_json::Value>(&body).ok();
    let named = parsed
        .as_ref()
        .and_then(|v| v.get("model").and_then(|m| m.as_str().map(str::to_string)));
    // An image route must name an image id: a body with no model is refused,
    // so it cannot reach upstream on a default model.
    if uri.path().starts_with("/v1/images/") && named.is_none() {
        return handlers::refuse_model(&state, ip, caller, ENDPOINT, "");
    }
    if let Some(model) = named {
        let Some(served) = state.allow.resolve_for_path(uri.path(), &model) else {
            return handlers::refuse_model(&state, ip, caller, ENDPOINT, &model);
        };
        if served != model {
            tracing::info!("model alias: `{model}` is served as `{served}`");
            let mut v = parsed.unwrap_or_else(|| serde_json::json!({}));
            v["model"] = serde_json::Value::String(served);
            match serde_json::to_vec(&v) {
                Ok(bytes) => body = Bytes::from(bytes),
                Err(e) => {
                    tracing::warn!("model alias: `{model}` could not be rewritten: {e}");
                    return handlers::refuse_model(&state, ip, caller, ENDPOINT, &model);
                }
            }
        }
    }

    let Ok(upstream_method) = reqwest::Method::from_bytes(method.as_str().as_bytes()) else {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    };
    let url = format!("{}{}", state.upstream, path_and_query);

    let (key_label, upstream_key) = state.keys.active();
    let mut req = state
        .client
        .request(upstream_method, &url)
        .header(header::AUTHORIZATION, format!("Bearer {upstream_key}"))
        .body(body);
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(value) = headers.get(&name) {
            req = req.header(name, value.clone());
        }
    }
    // Image, audio and every other route reach the same upstream, so the same
    // price ceiling applies: a caller must not be able to route around it by
    // naming a path the chat handler does not serve.
    req = state.ceiling.apply(req);

    let start = Instant::now();
    let resp = match req.send().await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("passthrough upstream error: {e}");
            state.tracker.record(RequestRecord {
                ip,
                token: caller,
                endpoint: ENDPOINT,
                status: 502,
                latency_ms: start.elapsed().as_millis() as u64,
                failure_reason: Some("transport"),
                key_label: key_label.clone(),
                ..Default::default()
            });
            return (StatusCode::BAD_GATEWAY, "upstream error").into_response();
        }
    };

    let status = resp.status();
    let latency_ms = start.elapsed().as_millis() as u64;
    state.tracker.record(RequestRecord {
        ip,
        token: caller,
        endpoint: ENDPOINT,
        status: status.as_u16(),
        latency_ms,
        estimated_tokens: Some(estimate),
        key_label,
        ..Default::default()
    });

    // Headers are recorded above; the body streams back as the upstream sends it.
    let mut out = Response::builder().status(status);
    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        out = out.header(header::CONTENT_TYPE, ct.clone());
    }
    match out.body(Body::from_stream(resp.bytes_stream())) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("passthrough response build error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::rotation_tests::{
        body_model, json_ok, json_post, models_get, request_line, spawn_scripted_upstream,
        test_app_with_images, tracked_row_for,
    };
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn path(raw: &str) -> Option<String> {
        safe_path_and_query(&raw.parse::<Uri>().unwrap())
    }

    #[test]
    fn dot_segments_are_refused() {
        assert_eq!(path("/v1/../../admin"), None);
        assert_eq!(path("/v1/../admin"), None);
        assert_eq!(path("/v1/./admin"), None);
        assert_eq!(path("/v1/%2e%2e/%2e%2e/admin"), None);
        assert_eq!(path("/v1/%2E%2E/admin"), None);
        assert_eq!(path("/v1/%2e/admin"), None);
    }

    #[test]
    fn encoded_separators_and_controls_are_refused() {
        assert_eq!(path("/v1/%2fadmin"), None);
        assert_eq!(path("/v1/%5c.."), None);
        assert_eq!(path("/v1/a%00b"), None);
    }

    #[test]
    fn normal_paths_pass_through_unchanged() {
        assert_eq!(
            path("/v1/chat/completions").as_deref(),
            Some("/v1/chat/completions")
        );
        assert_eq!(
            path("/v1/images/generations").as_deref(),
            Some("/v1/images/generations")
        );
        assert_eq!(
            path("/v1/models/deepseek-v4.1-flash").as_deref(),
            Some("/v1/models/deepseek-v4.1-flash")
        );
    }

    #[test]
    fn query_is_preserved() {
        assert_eq!(
            path("/v1/embeddings?x=1&y=..%2f").as_deref(),
            Some("/v1/embeddings?x=1&y=..%2f")
        );
    }

    /// An image request naming the id the docs ship reaches the upstream as the
    /// image id this proxy serves, so a client built against older docs keeps
    /// generating images.
    #[tokio::test]
    async fn an_image_alias_is_forwarded_as_the_served_id() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let (upstream, count) = spawn_scripted_upstream(move |_, raw| {
            recorder.lock().unwrap().push(body_model(raw));
            json_ok("{\"data\":[{\"url\":\"http://upstream/1.png\"}]}")
        })
        .await;
        let app = test_app_with_images(upstream, &["m1"], "gemini-2.5-flash-image").await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(json_post(
                &token,
                "/v1/images/generations",
                "gemini-3.1-flash-image",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["gemini-2.5-flash-image".to_string()],
            "an image alias must reach the upstream as the served image id"
        );

        let row = tracked_row_for(&db, ENDPOINT).await;
        assert_eq!(row.status, 200);
    }

    /// A models path naming an id this proxy does not serve is refused, so the id
    /// in the path is checked before anything goes upstream.
    #[tokio::test]
    async fn a_models_path_naming_an_unsupported_id_is_refused() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| json_ok("{}")).await;
        let app = test_app_with_images(upstream, &["m1"], "gemini-2.5-flash-image").await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(models_get(&token, "gpt-4"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(count.load(Ordering::SeqCst), 0);

        let row = tracked_row_for(&db, ENDPOINT).await;
        assert_eq!(row.model_requested, "gpt-4");
        assert_eq!(row.status, 403);
    }

    /// An id this proxy serves on a models path is forwarded as it was named:
    /// the path is rewritten only when the id resolved elsewhere.
    #[tokio::test]
    async fn a_served_id_on_a_models_path_is_forwarded_unchanged() {
        let seen: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let (upstream, count) = spawn_scripted_upstream(move |_, raw| {
            recorder.lock().unwrap().push(request_line(raw));
            json_ok("{\"id\":\"m1\"}")
        })
        .await;
        let app = test_app_with_images(upstream, &["m1"], "gemini-2.5-flash-image").await;
        let token = app.token.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(models_get(&token, "m1"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let line = seen.lock().unwrap()[0].clone();
        assert!(
            line.contains("/v1/models/m1"),
            "a served id must be forwarded on the path the caller named: {line}"
        );
    }

    /// An id that is neither served nor an alias is refused before anything
    /// goes upstream, on a pass-through route as much as on chat.
    #[tokio::test]
    async fn an_id_that_is_neither_served_nor_an_alias_is_refused_on_a_route() {
        let (upstream, count) = spawn_scripted_upstream(|_, _| json_ok("{}")).await;
        let app = test_app_with_images(upstream, &["m1"], "gemini-2.5-flash-image").await;
        let token = app.token.clone();
        let db = app.stats_db.clone();

        let response = crate::build_router(Arc::clone(&app.state))
            .oneshot(json_post(&token, "/v1/images/generations", "gpt-4"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(count.load(Ordering::SeqCst), 0);

        let row = tracked_row_for(&db, ENDPOINT).await;
        assert_eq!(row.model_requested, "gpt-4");
        assert_eq!(row.status, 403);
    }
}
