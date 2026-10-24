/// Per-request record written to SQLite after each proxied call.
///
/// The first eight fields describe what the caller asked for and what it got.
/// The last five describe *how* the rotation went, which is what turns a
/// `502` in the stats into a diagnosis: a request that burned three attempts
/// and died on a header timeout reads nothing like one the upstream refused
/// in 19ms, even though both land as a 5xx.
#[derive(Default)]
pub struct RequestRecord {
    pub ip: String,
    /// Label of the caller whose token made the request, or `-` when none
    /// was accepted.
    pub token: String,
    pub endpoint: &'static str,
    pub model_requested: String,
    pub model_used: String,
    pub status: u16,
    pub latency_ms: u64,
    pub usage: Option<TokenUsage>,
    /// The proxy's own estimate of this request's input tokens, computed
    /// before the body was forwarded. `usage.prompt_tokens` is what the
    /// upstream charged for the same body, so the pair gives the density
    /// `actual / estimated` that [`crate::calibrate`] learns per caller.
    /// `None` on routes that carry no body to estimate.
    pub estimated_tokens: Option<u64>,
    /// Machine-readable id for why the request failed, or `None` when it was
    /// served. See [`crate::proxy::FailureKind`]: `upstream_5xx`,
    /// `upstream_4xx`, `header_timeout`, `first_byte_timeout`, `transport`,
    /// `stream_closed`, `stream_error`.
    pub failure_reason: Option<&'static str>,
    /// Upstream attempts actually sent. `None` on routes that never reach a
    /// model (`/v1/models`, installs, refusals).
    pub attempts: Option<u32>,
    /// Size of the rotation plan built for this request. A plan of 1 means
    /// rotation is silently a no-op, so this is the field that proves whether
    /// the configured candidate list is doing anything.
    pub plan_len: Option<u32>,
    /// Index of the plan entry that produced the response, `None` when no
    /// candidate answered. `0` is the first candidate, so any value above 0
    /// is direct evidence that rotation engaged and saved the request.
    pub served_index: Option<u32>,
    /// Label of the upstream key that served this request. Empty on routes
    /// that never reach the upstream.
    pub key_label: String,
    /// Publisher that served it, read from the header named by
    /// `PUBLISHER_HEADER`. Empty when that header is not configured.
    pub upstream_publisher: Option<String>,
    /// Requests left in the active key's rate-limit window, read from
    /// `x-ratelimit-remaining`.
    pub ratelimit_remaining: Option<u64>,
    /// A chat request that answered 200 but carried no token usage. The proxy
    /// always asks the upstream for usage, so a 200 chat completion without it
    /// is a body that died after its headers: the caller sees a short answer
    /// and no error.
    pub truncated: bool,
}

/// Token usage extracted from the upstream response body.
#[derive(Debug, Clone)]
pub struct TokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cached_tokens: u64,
    pub reasoning_tokens: u64,

    /// What the upstream said this request cost, in USD, from `usage.cost`.
    /// `None` when the response carried no usage or the upstream does not
    /// price per request, which is not the same as zero and must not be
    /// stored as zero.
    pub cost: Option<f64>,
}
