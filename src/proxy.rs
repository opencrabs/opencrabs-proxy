use axum::{
    body::Body,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

use crate::tracker::TokenUsage;

/// Metrics returned asynchronously after a streaming response completes.
pub struct StreamMetrics {
    pub usage: Option<TokenUsage>,
    pub latency_ms: u64,
}

pub struct ForwardResult {
    pub response: Response,
    pub status: u16,
    pub latency_ms: u64,
    pub usage: Option<TokenUsage>,
    /// For streaming responses: a receiver that yields `StreamMetrics` once
    /// the full body has been streamed. Non-streaming responses set this to
    /// `None` (usage is populated directly in `usage` above).
    pub stream_metrics: Option<oneshot::Receiver<StreamMetrics>>,
    /// Why this request failed, when it did. `None` on a served request.
    pub failure: Option<&'static str>,
}

impl ForwardResult {
    /// The answer for an upstream that never produced a usable response at
    /// all. `reason` is the [`FailureKind`] id that caused it.
    pub fn upstream_error(latency_ms: u64, reason: &'static str) -> Self {
        Self {
            response: (StatusCode::BAD_GATEWAY, "upstream error").into_response(),
            status: 502,
            latency_ms,
            usage: None,
            stream_metrics: None,
            failure: Some(reason),
        }
    }

    /// Every candidate refused at the configured price ceiling.
    ///
    /// Not a 402 to the caller: a 402 reads as "payment required", and a
    /// client that sees it may treat its own token as bad. The honest answer
    /// is that the upstream has nothing under our ceiling right now, which is
    /// a capacity-shaped problem, so it goes out as a 503 with a reason the
    /// stats and the alert group can count.
    pub fn price_ceiling_refusal(latency_ms: u64) -> Self {
        Self {
            response: (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(serde_json::json!({
                    "error": {
                        "code": "price_ceiling",
                        "message": "no upstream provider is available under the configured price ceiling",
                    }
                })),
            )
                .into_response(),
            status: 503,
            latency_ms,
            usage: None,
            stream_metrics: None,
            failure: Some("price_ceiling"),
        }
    }

    /// Label the response with the model id that actually produced it.
    pub fn with_served_model(mut self, model: &str) -> Self {
        if let Ok(value) = header::HeaderValue::from_str(model) {
            self.response
                .headers_mut()
                .insert(header::HeaderName::from_static("x-keyproxy-model"), value);
        }
        self
    }
}

/// Why one upstream attempt produced nothing usable.
///
/// The string form is what lands in the stats database and in journald, so a
/// report can group failures by cause instead of lumping every 502 together:
/// "the upstream hung for 40s" and "the upstream refused in 19ms" are the same
/// status code and completely different problems.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FailureKind {
    /// The upstream accepted the request but sent no response headers inside
    /// the per-attempt budget.
    HeaderTimeout,
    /// Headers arrived, then the stream produced no byte inside the
    /// first-token budget.
    FirstByteTimeout,
    /// The connection failed, or the body stream errored.
    Transport,
    /// The upstream closed the stream before it sent anything.
    StreamClosed,
    /// The body died partway through, after bytes had already reached the
    /// caller.
    StreamError,
}

impl FailureKind {
    /// The id stored in `requests.failure_reason`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HeaderTimeout => "header_timeout",
            Self::FirstByteTimeout => "first_byte_timeout",
            Self::Transport => "transport",
            Self::StreamClosed => "stream_closed",
            Self::StreamError => "stream_error",
        }
    }
}

/// A dead attempt: the machine-readable kind plus the human detail that goes
/// to the log line.
#[derive(Clone, Debug)]
pub struct AttemptFailure {
    pub kind: FailureKind,
    pub detail: String,
}

/// One upstream attempt that was actually sent.
pub enum Attempt {
    /// A response a caller can use: any status below 500. A 4xx is the
    /// upstream speaking for this exact request, so it is never retried.
    Ok(UpstreamResponse),
    /// The upstream answered with a server error. The response is kept so a
    /// rotation loop can relay the body when no other candidate succeeds.
    ServerError(UpstreamResponse),
    /// No response at all: transport failure, no headers inside the header
    /// budget, or no first stream byte inside the first-token budget.
    Dead(AttemptFailure),
}

/// Budgets for one upstream attempt.
#[derive(Clone, Copy)]
pub struct AttemptPolicy {
    /// Deadline for response headers. Applies to every request.
    pub header_timeout: Duration,
    /// Deadline from send to the first SSE body byte. Only set for small
    /// streaming requests: a silent upstream that accepted the request but
    /// never produces a token is a dead attempt worth rotating off. Never set
    /// for compaction-scale bodies, whose first token legitimately takes
    /// minutes, or for non-streaming requests, whose first byte is the whole
    /// completion.
    pub first_byte_timeout: Option<Duration>,
}

impl AttemptPolicy {
    pub fn header_only(header_timeout: Duration) -> Self {
        Self {
            header_timeout,
            first_byte_timeout: None,
        }
    }

    /// One budget covering send through the first stream byte.
    pub fn with_first_byte(budget: Duration) -> Self {
        Self {
            header_timeout: budget,
            first_byte_timeout: Some(budget),
        }
    }
}

/// An upstream response with its body as a stream, so an attempt that already
/// consumed the first bytes (the first-token check) can still hand the client
/// a complete body starting from those bytes.
pub struct UpstreamResponse {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    pub body: BodyStream,
}

pub type BodyStream =
    std::pin::Pin<Box<dyn futures_util::Stream<Item = reqwest::Result<bytes::Bytes>> + Send>>;

impl UpstreamResponse {
    pub fn from_response(r: reqwest::Response) -> Self {
        Self {
            status: r.status().as_u16(),
            headers: r.headers().clone(),
            body: Box::pin(r.bytes_stream()),
        }
    }

    pub fn is_stream(&self) -> bool {
        self.headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|ct| ct.contains("text/event-stream"))
            .unwrap_or(false)
    }
}

/// Send one upstream attempt, injecting the real key and bounding it with the
/// policy budgets. A streaming body keeps flowing after its first byte for as
/// long as the reqwest client's own total timeout allows, so generation is
/// never cut mid-stream.
pub async fn send_attempt_with(
    upstream_key: &str,
    policy: AttemptPolicy,
    req: reqwest::RequestBuilder,
) -> Attempt {
    let start = Instant::now();
    let req = req.header(header::AUTHORIZATION, format!("Bearer {upstream_key}"));
    let sent = match tokio::time::timeout(policy.header_timeout, req.send()).await {
        Err(_) => {
            return Attempt::Dead(AttemptFailure {
                kind: FailureKind::HeaderTimeout,
                detail: format!(
                    "no response headers within {}s",
                    policy.header_timeout.as_secs()
                ),
            })
        }
        Ok(Err(e)) => {
            return Attempt::Dead(AttemptFailure {
                kind: FailureKind::Transport,
                detail: e.to_string(),
            })
        }
        Ok(Ok(r)) => r,
    };

    let status = sent.status().as_u16();
    if status >= 500 {
        // Error bodies are small and immediate; no first-token budget applies.
        return Attempt::ServerError(UpstreamResponse::from_response(sent));
    }

    let mut upstream = UpstreamResponse::from_response(sent);
    if let Some(budget) = policy.first_byte_timeout {
        if upstream.is_stream() {
            let remaining = budget.saturating_sub(start.elapsed());
            let first = match tokio::time::timeout(remaining, upstream.body.as_mut().next()).await {
                Err(_) => {
                    return Attempt::Dead(AttemptFailure {
                        kind: FailureKind::FirstByteTimeout,
                        detail: format!("no first stream byte within {}s", budget.as_secs()),
                    })
                }
                Ok(None) => {
                    return Attempt::Dead(AttemptFailure {
                        kind: FailureKind::StreamClosed,
                        detail: "stream closed before any byte".to_string(),
                    })
                }
                Ok(Some(Err(e))) => {
                    return Attempt::Dead(AttemptFailure {
                        kind: FailureKind::Transport,
                        detail: e.to_string(),
                    })
                }
                Ok(Some(Ok(first))) => first,
            };
            // Put the consumed byte back at the head of the body.
            upstream.body = Box::pin(
                futures_util::stream::once(async move { Ok::<_, reqwest::Error>(first) })
                    .chain(upstream.body),
            );
        }
    }
    Attempt::Ok(upstream)
}

/// Upstream statuses that describe the KEY rather than this request. A 400 or
/// a 404 is the upstream answering this exact request and must never be
/// retried; a 401, 402 or 429 is about the account, so another key is worth a
/// try before the caller sees the error.
///
/// A 402 is not always about the key. When it carries [`PRICE_REFUSAL_CODE`]
/// it is about the price ceiling this attempt sent, and switching keys cannot
/// help: see [`price_refusal`].
pub fn key_level_status(status: u16) -> bool {
    matches!(status, 401 | 402 | 429)
}

/// The upstream's code for "no provider is willing to serve at the price you
/// bid". It arrives as a 402, which is also the status for an empty account,
/// so the code is what separates them.
pub const PRICE_REFUSAL_CODE: &str = "no_provider_under_bid";

/// True when a 402 body says this candidate has no supply at our ceiling.
///
/// A price refusal is a fact about the model's current supply, not about the
/// key: the account is fine and the next candidate may well be cheap enough.
/// Retiring the key on one would drain a healthy ring over a temporary price
/// spike, which is how a cost guard becomes an outage.
pub fn price_refusal(body: Option<&str>) -> bool {
    body.is_some_and(|b| b.contains(PRICE_REFUSAL_CODE))
}

/// Read a small error body into memory so it can be classified, and put it
/// back so the same response can still be forwarded to the caller.
///
/// Only ever used on key-level 4xx responses, whose bodies are small JSON. A
/// streaming success body is never drained this way.
pub async fn drain_body(upstream: &mut UpstreamResponse) -> Vec<u8> {
    let mut buf = Vec::new();
    while let Some(item) = upstream.body.as_mut().next().await {
        match item {
            Ok(chunk) => buf.extend_from_slice(&chunk),
            Err(e) => {
                tracing::warn!("error body read failed: {e}");
                break;
            }
        }
    }
    let replay = buf.clone();
    upstream.body = Box::pin(futures_util::stream::once(async move {
        Ok::<_, reqwest::Error>(bytes::Bytes::from(replay))
    }));
    buf
}

/// A header value as an owned string, for the fields the stats keep.
pub fn header_string(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// A header value as a number, ignoring anything that does not parse.
pub fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
}

/// How long to stay off a key that reported its own rate limit, read from
/// `x-ratelimit-reset`.
///
/// This upstream sends an absolute Unix timestamp. A relative number of
/// seconds is accepted too, so a change of shape does not silently become a
/// zero-length cooldown. Capped at five minutes: a longer window than that is
/// a platform problem rather than a per-key one.
pub fn rate_limit_cooldown(headers: &HeaderMap) -> Option<Duration> {
    let raw = header_u64(headers, "x-ratelimit-reset")?;
    let secs = if raw > 1_000_000_000 {
        (raw as i64 - chrono::Utc::now().timestamp()).max(1) as u64
    } else {
        raw.max(1)
    };
    Some(Duration::from_secs(secs.min(300)))
}

/// Convert a usable upstream response into the client response, streaming or
/// buffered. `start` is the send of the FIRST attempt, so recorded latency
/// covers the whole request, rotation included.
pub async fn forward_response(upstream: UpstreamResponse, start: Instant) -> ForwardResult {
    let latency_ms = start.elapsed().as_millis() as u64;
    if upstream.is_stream() {
        forward_streaming(upstream, start).await
    } else {
        forward_buffered(upstream, latency_ms).await
    }
}

/// Non-streaming: buffer the entire body, extract usage, forward as one chunk.
async fn forward_buffered(upstream: UpstreamResponse, latency_ms: u64) -> ForwardResult {
    let status = upstream.status;
    let mut body = upstream.body;
    let mut buf = Vec::new();
    while let Some(item) = body.as_mut().next().await {
        match item {
            Ok(chunk) => buf.extend_from_slice(&chunk),
            Err(e) => {
                tracing::warn!("upstream body read error: {e}");
                return ForwardResult::upstream_error(
                    latency_ms,
                    FailureKind::StreamError.as_str(),
                );
            }
        }
    }
    let usage = extract_usage(&buf);

    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(code);
    for name in [header::CONTENT_TYPE, header::CACHE_CONTROL] {
        if let Some(v) = upstream.headers.get(&name) {
            builder = builder.header(name, v.clone());
        }
    }
    let response = builder.body(Body::from(buf)).unwrap_or_else(|_| {
        (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
    });
    ForwardResult {
        response,
        status,
        latency_ms,
        usage,
        stream_metrics: None,
        failure: None,
    }
}

/// Streaming SSE: forward each chunk immediately while extracting token usage
/// from the final SSE event. Returns a oneshot receiver that fires once the
/// stream ends (with or without usage data).
async fn forward_streaming(upstream: UpstreamResponse, start: Instant) -> ForwardResult {
    let status = upstream.status;
    let (tx, rx) = mpsc::unbounded_channel::<Result<axum::body::Bytes, axum::Error>>();
    let (metrics_tx, metrics_rx) = oneshot::channel::<StreamMetrics>();

    // Spawn a task that reads the upstream stream, forwards each chunk via the
    // channel, and extracts SSE usage on the fly.
    let mut stream = upstream.body;
    tokio::spawn(async move {
        let mut extractor = SseUsageExtractor::new();

        while let Some(item) = stream.next().await {
            match item {
                Ok(chunk) => {
                    extractor.process_chunk(&chunk);
                    if tx.send(Ok(chunk)).is_err() {
                        // Client disconnected; stop reading upstream.
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("upstream stream error: {e}");
                    let _ = tx.send(Err(axum::Error::new(e)));
                    break;
                }
            }
        }
        // Channel dropped = Body stream ends.

        let latency_ms = start.elapsed().as_millis() as u64;
        let _ = metrics_tx.send(StreamMetrics {
            usage: extractor.take_usage(),
            latency_ms,
        });
    });

    let code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    let mut builder = Response::builder().status(code);
    for name in [header::CONTENT_TYPE, header::CACHE_CONTROL] {
        if let Some(v) = upstream.headers.get(&name) {
            builder = builder.header(name, v.clone());
        }
    }
    let response = builder
        .body(Body::from_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(rx),
        ))
        .unwrap_or_else(|_| {
            (StatusCode::INTERNAL_SERVER_ERROR, "response build error").into_response()
        });

    ForwardResult {
        response,
        status,
        // Will be updated when the stream completes (via metrics_rx).
        latency_ms: 0,
        usage: None,
        stream_metrics: Some(metrics_rx),
        failure: None,
    }
}

// ── SSE usage extraction (streaming) ────────────────────────────────────────

/// Processes SSE chunks incrementally, extracting `"usage"` from `data:`
/// events as they stream past. Designed for zero-copy streaming: each chunk is
/// scanned and forwarded immediately; no full-body buffering.
///
/// A stream can report usage more than once. The upstream sends a detailed
/// record first and the gateway appends its billing record (which carries
/// `cost` and only the token totals) in a later chunk, so the first record is
/// not the last word: scanning continues until a record with a cost is seen.
struct SseUsageExtractor {
    /// Incomplete line left over from the previous chunk.
    leftover: Vec<u8>,
    /// Accumulates the current SSE event (text between `\n\n` boundaries).
    event_buffer: Vec<u8>,
    /// The usage seen so far, merged across every record in the stream.
    usage: Option<TokenUsage>,
    /// Set once a record carrying `cost` has been folded in. Until then a
    /// later record can still supply it, so scanning must go on.
    done: bool,
    /// Hard cap on event buffer size to prevent memory leaks (1 MB).
    max_event_buffer: usize,
}

impl SseUsageExtractor {
    fn new() -> Self {
        Self {
            leftover: Vec::new(),
            event_buffer: Vec::new(),
            usage: None,
            done: false,
            max_event_buffer: 1_048_576, // 1 MB
        }
    }

    fn process_chunk(&mut self, chunk: &[u8]) {
        if self.done {
            return;
        }

        // Combine leftover from previous chunk with new data.
        let combined = if self.leftover.is_empty() {
            chunk.to_vec()
        } else {
            let mut buf = std::mem::take(&mut self.leftover);
            buf.extend_from_slice(chunk);
            buf
        };

        // Split on newlines; last element may be incomplete.
        let mut lines: Vec<&[u8]> = Vec::new();
        let mut last_end = 0;
        for (i, byte) in combined.iter().enumerate() {
            if *byte == b'\n' {
                lines.push(&combined[last_end..i]);
                last_end = i + 1;
            }
        }
        // Anything after the last newline is incomplete; save as leftover.
        if last_end < combined.len() {
            self.leftover = combined[last_end..].to_vec();
        }

        for line in &lines {
            // Skip empty lines (they're SSE event boundaries).
            if line.is_empty() {
                self.event_buffer.clear();
                continue;
            }

            self.event_buffer.extend_from_slice(line);
            self.event_buffer.push(b'\n');

            // Cap the event buffer to prevent unbounded growth.
            if self.event_buffer.len() > self.max_event_buffer {
                self.event_buffer.clear();
                continue;
            }

            // Check if the line starts with "data: " and contains "usage".
            if let Some(data) = strip_prefix(line, b"data: ") {
                let data = trim_whitespace(data);
                if data == b"[DONE]" {
                    continue;
                }
                if let Ok(text) = std::str::from_utf8(data) {
                    if text.contains("\"usage\"") {
                        if let Ok(chunk) = serde_json::from_str::<serde_json::Value>(text) {
                            if let Some(usage_v) = chunk.get("usage") {
                                if let Some(usage) = parse_usage_value(usage_v) {
                                    self.usage = merge_usage(self.usage.take(), usage);
                                    // Only a record that carries a cost is
                                    // final. The billing record arrives in a
                                    // later chunk, so a token-only record
                                    // leaves the scan running.
                                    if self.usage.as_ref().is_some_and(|u| u.cost.is_some()) {
                                        self.done = true;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fn take_usage(self) -> Option<TokenUsage> {
        self.usage
    }
}

fn strip_prefix<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    if line.len() >= prefix.len() && &line[..prefix.len()] == prefix {
        Some(&line[prefix.len()..])
    } else {
        None
    }
}

fn trim_whitespace(data: &[u8]) -> &[u8] {
    let start = data
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(data.len());
    let end = data
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map(|i| i + 1)
        .unwrap_or(0);
    if start >= end {
        b""
    } else {
        &data[start..end]
    }
}

// ── Usage extraction (non-streaming / complete body) ────────────────────────

/// Extract token usage from a response body.
/// Tries JSON first (non-streaming), then SSE (streaming).
pub fn extract_usage(body: &[u8]) -> Option<TokenUsage> {
    extract_usage_json(body).or_else(|| extract_usage_sse(body))
}

/// Non-streaming: top-level `"usage"` object.
fn extract_usage_json(body: &[u8]) -> Option<TokenUsage> {
    let v: serde_json::Value = serde_json::from_slice(body).ok()?;
    parse_usage_value(v.get("usage")?)
}

/// Streaming SSE: scan `data: {...}` lines for chunks containing `"usage"`,
/// merging every record found. The billing record with `cost` arrives after
/// the model's own token record, so the scan cannot stop at the first one.
fn extract_usage_sse(body: &[u8]) -> Option<TokenUsage> {
    let text = std::str::from_utf8(body).ok()?;
    let mut found: Option<TokenUsage> = None;
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };
        let data = data.trim();
        if data == "[DONE]" {
            continue;
        }
        if let Ok(chunk) = serde_json::from_str::<serde_json::Value>(data) {
            if let Some(usage) = chunk.get("usage").and_then(parse_usage_value) {
                found = merge_usage(found, usage);
                if found.as_ref().is_some_and(|u| u.cost.is_some()) {
                    return found;
                }
            }
        }
    }
    found
}

/// Fold a newly seen usage record into the one already kept.
///
/// The detailed record wins on tokens, because it carries the breakdown
/// (`cached_tokens`, `reasoning_tokens`) the billing record omits, while `cost`
/// is adopted from whichever record reports it. A later record therefore never
/// overwrites the counts and never loses the price.
fn merge_usage(stored: Option<TokenUsage>, fresh: TokenUsage) -> Option<TokenUsage> {
    match stored {
        None => Some(fresh),
        Some(mut kept) => {
            if kept.cost.is_none() {
                kept.cost = fresh.cost;
            }
            Some(kept)
        }
    }
}

pub fn parse_usage_value(v: &serde_json::Value) -> Option<TokenUsage> {
    let cached_tokens = v
        .get("prompt_tokens_details")
        .and_then(|d| d.get("cached_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let reasoning_tokens = v
        .get("completion_tokens_details")
        .and_then(|d| d.get("reasoning_tokens"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    // `usage.cost` is what the upstream billed for this exact request, in
    // USD. It is optional on the API and absent on upstreams that do not
    // price per request, so a missing or malformed value is `None`, never a
    // zero: zero would read as a free request in every average downstream.
    let cost = v
        .get("cost")
        .and_then(|c| c.as_f64())
        .filter(|c| c.is_finite() && *c >= 0.0);
    Some(TokenUsage {
        prompt_tokens: v.get("prompt_tokens")?.as_u64()?,
        completion_tokens: v.get("completion_tokens")?.as_u64()?,
        total_tokens: v.get("total_tokens")?.as_u64()?,
        cached_tokens,
        reasoning_tokens,
        cost,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Attempt classification against a raw TCP upstream ───────────────

    use std::net::SocketAddr;

    /// Bind 127.0.0.1:0, accept one connection, drain the request head, then
    /// run `respond`: empty vec = accept and stay silent (a hang), otherwise
    /// the raw HTTP response bytes. The socket is then held open so hangs stay
    /// hangs for the duration of a test.
    async fn spawn_upstream(respond: impl FnOnce() -> Vec<u8> + Send + 'static) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 8192];
            let _ = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await;
            let payload = respond();
            if !payload.is_empty() {
                let _ = tokio::io::AsyncWriteExt::write_all(&mut sock, &payload).await;
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        });
        addr
    }

    /// Bind 127.0.0.1:0, accept one connection, hand the raw request head back
    /// on a channel, then answer 200. Used to prove what actually goes on the
    /// wire, which is the only place a header can be shown to be sent.
    async fn spawn_upstream_capturing_head() -> (SocketAddr, tokio::sync::oneshot::Receiver<String>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 8192];
            let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf)
                .await
                .unwrap_or(0);
            let _ = tx.send(String::from_utf8_lossy(&buf[..n]).to_string());
            let _ = tokio::io::AsyncWriteExt::write_all(
                &mut sock,
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
            )
            .await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        (addr, rx)
    }

    #[tokio::test]
    async fn every_upstream_attempt_carries_the_price_ceiling() {
        let (addr, head_rx) = spawn_upstream_capturing_head().await;
        let ceiling = crate::price::PriceCeiling::parse(Some("0.003"), Some("0.012"));
        let policy = AttemptPolicy::header_only(Duration::from_secs(5));
        let req = ceiling.apply(post(&client(), addr));
        let attempt =
            tokio::time::timeout(Duration::from_secs(10), send_attempt_with("k", policy, req))
                .await
                .unwrap();
        assert!(matches!(attempt, Attempt::Ok(_)));
        let head = head_rx.await.unwrap().to_lowercase();
        assert!(
            head.contains("x-max-input-price: 0.003"),
            "input ceiling header missing from the wire head: {head}"
        );
        assert!(
            head.contains("x-max-output-price: 0.012"),
            "output ceiling header missing from the wire head: {head}"
        );
    }

    #[tokio::test]
    async fn a_disabled_ceiling_puts_no_price_header_on_the_wire() {
        let (addr, head_rx) = spawn_upstream_capturing_head().await;
        let ceiling = crate::price::PriceCeiling::parse(Some("0"), Some("0"));
        let policy = AttemptPolicy::header_only(Duration::from_secs(5));
        let req = ceiling.apply(post(&client(), addr));
        let attempt =
            tokio::time::timeout(Duration::from_secs(10), send_attempt_with("k", policy, req))
                .await
                .unwrap();
        assert!(matches!(attempt, Attempt::Ok(_)));
        let head = head_rx.await.unwrap().to_lowercase();
        assert!(
            !head.contains("x-max-input-price") && !head.contains("x-max-output-price"),
            "a disabled ceiling must send neither header: {head}"
        );
    }

    fn client() -> reqwest::Client {
        // No total timeout: the attempt budgets must be what fires.
        reqwest::Client::builder().build().unwrap()
    }

    fn post(client: &reqwest::Client, addr: SocketAddr) -> reqwest::RequestBuilder {
        client
            .post(format!("http://{addr}/v1/chat/completions"))
            .body("{}".to_string())
    }

    fn sse_headers() -> Vec<u8> {
        b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\nconnection: close\r\n\r\n".to_vec()
    }

    #[tokio::test]
    async fn hung_upstream_is_dead_at_the_header_budget() {
        let addr = spawn_upstream(Vec::new).await;
        let policy = AttemptPolicy::header_only(Duration::from_secs(1));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .expect("attempt must be bounded by the header budget");
        match attempt {
            Attempt::Dead(failure) => {
                assert_eq!(failure.kind, FailureKind::HeaderTimeout);
                assert!(failure.detail.contains("no response headers"));
                assert_eq!(failure.kind.as_str(), "header_timeout");
            }
            _ => panic!("expected Dead, got another variant"),
        }
    }

    #[tokio::test]
    async fn sse_silence_after_headers_is_dead_at_the_first_byte_budget() {
        let addr = spawn_upstream(sse_headers).await;
        let policy = AttemptPolicy::with_first_byte(Duration::from_secs(1));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .expect("attempt must be bounded by the first-byte budget");
        match attempt {
            Attempt::Dead(failure) => {
                assert_eq!(failure.kind, FailureKind::FirstByteTimeout);
                assert!(failure.detail.contains("no first stream byte"));
            }
            _ => panic!("expected Dead, got another variant"),
        }
    }

    #[test]
    fn failure_kind_ids_are_stable() {
        // These strings are what lands in the stats database, so they are a
        // contract with every report that reads it.
        assert_eq!(FailureKind::HeaderTimeout.as_str(), "header_timeout");
        assert_eq!(FailureKind::FirstByteTimeout.as_str(), "first_byte_timeout");
        assert_eq!(FailureKind::Transport.as_str(), "transport");
        assert_eq!(FailureKind::StreamClosed.as_str(), "stream_closed");
        assert_eq!(FailureKind::StreamError.as_str(), "stream_error");
    }

    #[tokio::test]
    async fn first_stream_byte_completes_and_is_spliced_back() {
        let addr = spawn_upstream(|| {
            let mut raw = sse_headers();
            raw.extend_from_slice(b": ping\n\n");
            raw
        })
        .await;
        let policy = AttemptPolicy::with_first_byte(Duration::from_secs(5));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .unwrap();
        let Attempt::Ok(mut up) = attempt else {
            panic!("expected Ok after a timely first byte")
        };
        // The spliced body must start with the byte the check consumed.
        let first = up.body.as_mut().next().await.unwrap().unwrap();
        assert!(first.windows(6).any(|w| w == b": ping"));
    }

    #[tokio::test]
    async fn sse_silence_without_a_first_byte_policy_is_ok() {
        // Compaction-scale and non-streaming requests carry no first-token
        // budget: headers arriving is enough to hand the body over.
        let addr = spawn_upstream(sse_headers).await;
        let policy = AttemptPolicy::header_only(Duration::from_secs(1));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .unwrap();
        assert!(matches!(attempt, Attempt::Ok(_)));
    }

    #[tokio::test]
    async fn upstream_5xx_is_a_server_error_kept_for_relay() {
        let addr = spawn_upstream(|| {
            b"HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}".to_vec()
        })
        .await;
        let policy = AttemptPolicy::with_first_byte(Duration::from_secs(5));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .unwrap();
        let Attempt::ServerError(up) = attempt else {
            panic!("expected ServerError for a 503")
        };
        assert_eq!(up.status, 503);
    }

    #[tokio::test]
    async fn upstream_4xx_is_ok_and_never_rotated() {
        let addr = spawn_upstream(|| {
            b"HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}".to_vec()
        })
        .await;
        let policy = AttemptPolicy::with_first_byte(Duration::from_secs(5));
        let fut = send_attempt_with("k", policy, post(&client(), addr));
        let attempt = tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .unwrap();
        let Attempt::Ok(up) = attempt else {
            panic!("a 4xx is the upstream speaking for this request and must not be retried")
        };
        assert_eq!(up.status, 400);
    }

    // ── Non-streaming (buffered) extraction ─────────────────────────────

    #[test]
    fn extract_usage_from_json_body() {
        let body = serde_json::json!({
            "id": "chatcmpl-123",
            "choices": [],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 20,
                "total_tokens": 30,
                "prompt_tokens_details": {"cached_tokens": 5},
                "completion_tokens_details": {"reasoning_tokens": 8}
            }
        });
        let usage = extract_usage(body.to_string().as_bytes()).unwrap();
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 20);
        assert_eq!(usage.total_tokens, 30);
        assert_eq!(usage.cached_tokens, 5);
        assert_eq!(usage.reasoning_tokens, 8);
    }

    #[test]
    fn extract_usage_from_sse_body() {
        let body = b"data: {\"id\":\"1\",\"choices\":[]}\n\ndata: {\"id\":\"2\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":15,\"total_tokens\":20,\"prompt_tokens_details\":{\"cached_tokens\":3},\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n";
        let usage = extract_usage(body).unwrap();
        assert_eq!(usage.prompt_tokens, 5);
        assert_eq!(usage.completion_tokens, 15);
        assert_eq!(usage.total_tokens, 20);
        assert_eq!(usage.cached_tokens, 3);
        assert_eq!(usage.reasoning_tokens, 7);
    }

    #[test]
    fn extract_usage_returns_none_when_missing() {
        let body = b"data: {\"id\":\"1\",\"choices\":[]}\n\ndata: [DONE]\n";
        assert!(extract_usage(body).is_none());
    }

    #[test]
    fn extract_usage_returns_none_for_empty() {
        assert!(extract_usage(b"").is_none());
    }

    // ── Streaming SSE extraction ────────────────────────────────────────

    #[test]
    fn sse_extractor_single_chunk() {
        let mut ext = SseUsageExtractor::new();
        let chunk = b"data: {\"id\":\"1\",\"choices\":[]}\n\ndata: {\"id\":\"2\",\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":200,\"total_tokens\":300,\"prompt_tokens_details\":{\"cached_tokens\":50},\"completion_tokens_details\":{\"reasoning_tokens\":25}}}\n\ndata: [DONE]\n";
        ext.process_chunk(chunk);
        let usage = ext.take_usage().unwrap();
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.completion_tokens, 200);
        assert_eq!(usage.total_tokens, 300);
        assert_eq!(usage.cached_tokens, 50);
        assert_eq!(usage.reasoning_tokens, 25);
    }

    #[test]
    fn sse_extractor_multi_chunk() {
        let mut ext = SseUsageExtractor::new();
        // Chunk 1: partial SSE event
        ext.process_chunk(b"data: {\"id\":\"1\",\"choices\":[]}\n\n");
        assert!(ext.usage.is_none());
        // Chunk 2: the usage event
        ext.process_chunk(b"data: {\"id\":\"2\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n");
        let usage = ext.take_usage().unwrap();
        assert_eq!(usage.prompt_tokens, 1);
        assert_eq!(usage.completion_tokens, 2);
        assert_eq!(usage.total_tokens, 3);
    }

    #[test]
    fn sse_extractor_usage_split_across_chunks() {
        let mut ext = SseUsageExtractor::new();
        // Split the usage data: line across two chunks
        ext.process_chunk(b"data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prom");
        assert!(ext.usage.is_none());
        ext.process_chunk(b"pt_tokens\":42,\"completion_tokens\":58,\"total_tokens\":100}}\n\n");
        let usage = ext.take_usage().unwrap();
        assert_eq!(usage.prompt_tokens, 42);
        assert_eq!(usage.completion_tokens, 58);
        assert_eq!(usage.total_tokens, 100);
    }

    #[test]
    fn sse_extractor_no_usage_returns_none() {
        let mut ext = SseUsageExtractor::new();
        ext.process_chunk(b"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n");
        assert!(ext.take_usage().is_none());
    }

    #[test]
    fn sse_extractor_empty_chunk() {
        let mut ext = SseUsageExtractor::new();
        ext.process_chunk(b"");
        assert!(ext.take_usage().is_none());
    }

    #[test]
    fn sse_extractor_keeps_the_first_records_tokens() {
        let mut ext = SseUsageExtractor::new();
        ext.process_chunk(b"data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n");
        assert_eq!(ext.usage.as_ref().unwrap().prompt_tokens, 1);
        // A later record without a cost cannot be the billing one, so the
        // counts stay as the detailed record reported them.
        ext.process_chunk(b"data: {\"usage\":{\"prompt_tokens\":999,\"completion_tokens\":999,\"total_tokens\":1998}}\n\n");
        assert_eq!(ext.usage.as_ref().unwrap().prompt_tokens, 1);
    }

    #[test]
    fn sse_extractor_adopts_the_cost_from_a_later_billing_record() {
        // The shape the live upstream actually sends: the model's detailed
        // record first, then the gateway's billing record, which carries the
        // price and only the totals. Dropping the second record is what left
        // every streaming request with a null cost.
        let mut ext = SseUsageExtractor::new();
        ext.process_chunk(
            b"data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prompt_tokens\":23,\"completion_tokens\":5,\"total_tokens\":28,\"prompt_tokens_details\":{\"cached_tokens\":11}}}\n\n",
        );
        assert!(ext.usage.as_ref().unwrap().cost.is_none());
        ext.process_chunk(
            b"data: {\"id\":\"2\",\"choices\":[],\"usage\":{\"prompt_tokens\":23,\"completion_tokens\":5,\"total_tokens\":28,\"cost\":0.00001}}\n\n",
        );
        let usage = ext.take_usage().unwrap();
        assert_eq!(usage.cost, Some(0.00001));
        // Tokens and their breakdown still come from the detailed record.
        assert_eq!(usage.prompt_tokens, 23);
        assert_eq!(usage.total_tokens, 28);
        assert_eq!(usage.cached_tokens, 11);
    }

    #[test]
    fn extract_usage_from_sse_body_takes_the_cost_from_the_billing_chunk() {
        let body = b"data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10,\"prompt_tokens_details\":{\"cached_tokens\":2}}}\n\ndata: {\"id\":\"2\",\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3,\"total_tokens\":10,\"cost\":0.00002}}\n\ndata: [DONE]\n";
        let usage = extract_usage(body).unwrap();
        assert_eq!(usage.cost, Some(0.00002));
        assert_eq!(usage.prompt_tokens, 7);
        assert_eq!(usage.total_tokens, 10);
        assert_eq!(usage.cached_tokens, 2);
    }

    #[test]
    fn sse_extractor_event_buffer_cap() {
        let mut ext = SseUsageExtractor::new();
        ext.max_event_buffer = 100; // Artificially low cap for testing.
                                    // Feed a huge data line that exceeds the cap.
        let huge_line = format!("data: {}\n\n", "x".repeat(200));
        ext.process_chunk(huge_line.as_bytes());
        // Buffer should have been cleared; no usage extracted.
        assert!(ext.usage.is_none());
    }

    // ── Utility functions ───────────────────────────────────────────────

    #[test]
    fn strip_prefix_works() {
        assert_eq!(
            strip_prefix(b"data: hello", b"data: "),
            Some(b"hello".as_slice())
        );
        assert_eq!(strip_prefix(b"nope", b"data: "), None);
        assert_eq!(strip_prefix(b"dat", b"data: "), None);
    }

    #[test]
    fn trim_whitespace_works() {
        assert_eq!(trim_whitespace(b"  hello  "), b"hello");
        assert_eq!(trim_whitespace(b"no-spaces"), b"no-spaces");
        assert_eq!(trim_whitespace(b"   "), b"");
    }
}
