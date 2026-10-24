# keyproxy

A key-injecting proxy for an OpenAI-compatible upstream. Clients get a base URL
and a caller token. The upstream API key stays on the server, in `/etc/keyproxy.env`,
and is never compiled into the binary or sent to clients.

## Client contract

Any OpenAI-compatible client works unchanged. Point it at the proxy:

- Base URL: `https://inference.opencrabs.com/v1`
- API key: the caller token issued to you

Supported routes:

- `POST /v1/chat/completions` (streaming and non-streaming, with token usage tracked)
- `GET /v1/models` (non-chat models are filtered out)
- `POST /v1/images/generations`, when `IMAGE_MODELS` is set. The body names one of those ids, written exactly as the upstream lists it (`provider/model`), and the proxy sends it on unchanged. An id older docs published for image generation is served on the id set here: see [Ids older clients name](#ids-older-clients-name). Responses stream back as the upstream sends them
- A chat request that carries an `input_audio` part is served only by `AUDIO_MODELS`, never by the chat list and never by a rotation candidate. A chat id is refused on such a request, and an audio id is refused on a request without audio, so a text prompt cannot be answered by a voice model and a voice note cannot be answered by a text one. When every audio id on the primary upstream fails, the voice-only `AUDIO_FALLBACK_*` upstream answers, if it is configured. An id older docs published for voice is served on the head of the chain: see [Ids older clients name](#ids-older-clients-name).
- Any other `/v1/*` route (for example `/v1/responses`) is forwarded as-is. Usage is not read from those.

A request without a valid token gets `401`. Unknown paths outside `/v1/` get `404`.

### Ids older clients name

The published OpenCrabs config and docs named `gemini-3.1-flash-image` for image
generation and `qwen3.8-omni-flash` for voice. Neither is an id the upstream behind
this proxy serves, so a client built against those docs would be refused. Each one
is served on the id this proxy carries for the capability the route names:

| A client names | Served as | On |
|---|---|---|
| `gemini-3.1-flash-image` | the `IMAGE_MODELS` id, `gemini-2.5-flash-image` by default | `/v1/images/*` |
| `qwen3.8-omni-flash` | the head of the voice chain, `mimo/mimo-v2.6-flash` by default | a chat request carrying `input_audio`, and `/v1/audio/*` |

Two rules hold:

- An alias resolves only inside the capability it names, and only when this proxy
  serves something for that capability. A voice id never resolves onto an image id,
  and an empty `IMAGE_MODELS` keeps image generation off whatever the caller names.
- An alias is accepted input, never advertised. `/v1/models` lists the served id, so
  a picker fed from it still shows one id per capability.

An id that is neither served nor an alias is refused with `403` before any upstream
call. The table lives in `src/alias.rs`.

## Access model

- Every caller has its own token. The tokens file holds only SHA-256 hashes.
- Issue a token: `TOKENS_FILE=... keyproxy issue <label>`. The token is printed once.
- Rate limits are per caller (`RATE_MAX_PER_MIN`).
- The client IP comes from the last `X-Forwarded-For` entry, which the trusted nginx hop adds. Client-supplied entries are ignored.
- `/stats` needs `ADMIN_TOKEN`, not a caller token. It shows requests and tokens per caller label.

## Configuration

Set these in the environment. Required values stop the process at startup if missing.

| Variable | Required | Meaning |
|---|---|---|
| `UPSTREAM_API_KEY` | no | Single upstream key, server-side only. Still supported: a one-key deploy needs nothing else |
| `UPSTREAM_KEYS` | no | The key ring: comma list of `label:key`, or bare keys which are numbered `key1`, `key2`. Takes precedence over `UPSTREAM_API_KEY`. One of the two is required |
| `KEY_MIN_BALANCE` | no | Default 0.50. A key at or below this balance stops being chosen |
| `KEY_BALANCE_URL` | no | Balance endpoint for the key ring. Unset turns the poller off, so keys are only retired after the upstream refuses them. Needs a browser `User-Agent`: some control planes refuse the default one with 403 |
| `KEY_BALANCE_INTERVAL_SECS` | no | Default 300. How often each key's balance is read. Zero falls back to the default rather than disabling the poller |
| `UPSTREAM_BASE_URL` | yes | Upstream origin with no `/v1` and no trailing slash |
| `ADMIN_TOKEN` | yes | Bearer token for `/stats` |
| `TOKENS_FILE` | yes | Issued caller token hashes |
| `BIND_ADDR` | no | Default `127.0.0.1:8787` |
| `RATE_MAX_PER_MIN` | no | Default 60 per caller |
| `ALLOWED_MODELS` | no | Comma list of chat model ids this proxy serves. Default `deepseek-v4.1-flash`, one id. A client picks that id once and the proxy chooses the publisher behind it, so prefixed routing ids (`ali/...`, `cb/...`) belong in `ROTATION_MODELS`, never here: a picker fed from `/v1/models` must not offer a second spelling of the same model. Any other id is refused before the upstream call, and `/v1/models` lists only these |
| `IMAGE_MODELS` | no | Comma list of image generation ids, served only on `/v1/images/*`. Default `gemini-2.5-flash-image`, one id, written in the exact spelling the upstream lists it: the proxy never adds or strips a prefix. Empty turns image generation off. An id may not also appear in `ALLOWED_MODELS` |
| `AUDIO_MODELS` | no | Comma list of audio input ids, served only to a request that carries an `input_audio` part, or on a `/v1/audio/*` route. Default `mimo/mimo-v2.6-flash,ocg/mimo-v2.6-flash`. The first id is tried first and the rest are fallbacks in order, so a caller names one voice id and the chain stays inside the proxy. An id here is never a chat model, so a text prompt cannot be answered by a voice model, and a chat id is refused on an audio request. An id may not also appear in `ALLOWED_MODELS` or `IMAGE_MODELS`. The order is walked as written and is never changed for you: in the default chain the first id is refused by the upstream with `403` because it is not in the catalog that upstream publishes, so every voice request spends one attempt before reaching the id that answers. Drop the refused id here to get that attempt back. A chat id is never a voice fallback, because answering text is not evidence that it answers audio |
| `AUDIO_FALLBACK_BASE_URL` | no | Base URL of a second upstream used for voice only, after every `AUDIO_MODELS` id on the primary upstream fails. Never consulted for a text request |
| `AUDIO_FALLBACK_API_KEY` | no | Key for the voice fallback upstream |
| `AUDIO_FALLBACK_MODELS` | no | Comma list of ids tried in order on the voice fallback upstream. The three `AUDIO_FALLBACK_*` variables are set together or not at all: a partial set stops the process at startup rather than leaving voice without the second leg it was promised. The audio part is sent to this upstream as a data URI (`data:audio/<format>;base64,<data>`), because a provider that reads that field as a URL refuses bare base64; the primary upstream still gets the caller's own bytes |
| `STATS_DB` | no | SQLite path for usage rows |
| `MAX_INPUT_PRICE` | no | Default `0.003`. Ceiling on the input price per million tokens, in USD, sent on every upstream attempt as `x-max-input-price`. `0` sends no header. Unset or unparsable falls back to the default, so a typo tightens the ceiling rather than turning it off. See [Price ceiling](#price-ceiling) |
| `MAX_OUTPUT_PRICE` | no | Default `0.012`. The same ceiling for output tokens, sent as `x-max-output-price` |
| `ROTATION_MODELS` | no | Comma list of chat model ids to try in order when one fails. Unset means rotation is off: one attempt on the model the caller named. It is never derived from `ALLOWED_MODELS`, because the allowlist is what a caller may name and rotation is what is safe to serve a text request on, so an audio-only or image id must never become a fallback |
| `ROTATION_ATTEMPT_TIMEOUT_SECS` | no | Default 15. Budget for response headers, and for the first SSE byte on small streaming requests |
| `ROTATION_TTFT_MAX_BODY_BYTES` | no | Default 524288. Request bodies at or above this size are compaction-scale and are exempt from the first-token budget |
| `PROBE_INTERVAL_SECS` | no | Default 300. How often the upstream health probe runs. A value of zero falls back to the default rather than disabling the probe |
| `PROBE_TIMEOUT_SECS` | no | Default 40. Ceiling on a single probe request |
| `PROBE_MODELS` | no | Comma list to probe. Defaults to every `ROTATION_MODELS` candidate, and to `ALLOWED_MODELS` when rotation is off |
| `MAX_INPUT_TOKENS` | no | Default 200000. A request whose calibrated estimated input reaches this is refused with `413` before any upstream call. `0` disables it. Global: one value for every caller |
| `INSTALL_PATH` | no | Path the install route is mounted at. Unset means the route is not registered at all. Must start with `/` |
| `INSTALL_RATE_PER_DAY` | no | Default 5000. Install-route requests per client IP per day |
| `PUBLISHER_HEADER` | no | Response header naming the publisher that served a request, recorded in `upstream_publisher`. Unset leaves that column `NULL` |

`.env.example` lists them all.

### The input ceiling

`MAX_INPUT_TOKENS` is a guardrail against traffic that cannot be ours. The
endpoint serves OpenCrabs clients, and a client's own auto-compaction targets 65%
of its context window, so on the built-in 200k window it compacts at 130k and
never approaches the ceiling. A request that reaches it is either misconfigured or
not from OpenCrabs at all, and either way it is not worth a key's balance.

The estimate is per part, mirroring the client's own accounting:

| Part | Charge |
|---|---|
| text | characters / 4 |
| `image_url` | 1,000 tokens |
| `input_audio` | 1,000 tokens |
| `tool_calls`, `tools` | characters / 4 of the serialized JSON |
| no `messages` array, or not JSON | bytes / 4 |

Bytes alone would be wrong. A 1 MB PNG is about 1.35 MB of base64, roughly 340k
by `bytes / 4`, while the client charges 1,000 tokens for it, so a single
screenshot would trip the ceiling on its own.

A refused request is one row with `failure_reason = "input_too_large"`, so an
audit can separate it from a rate limit. The ceiling is checked on
`/v1/chat/completions` and on the `/v1/*` passthrough, so no other path routes
around it.

The body limit is 25 MB, matching `client_max_body_size 25m` on the nginx host in
front of the proxy. axum's own default is 2 MB, which is lower, so without that
raise the largest bodies would die at the extractor with a plain text `413` and
leave no stats row at all.

Measured on 2026-10-15, four days of traffic: this machine peaked at 135,067
input tokens with nothing at or above 200k, while one other caller peaked at
334,842 with 2,598 requests above 200k. That caller pins no context window, so it
inherits `DEFAULT_CONTEXT_WINDOW = 1_000_000` and compacts at 650k instead of
130k. Those requests are refused once this ships, which is the intended outcome:
the fix is a `context_window = 200000` pin on the caller's provider.

### Calibration

`characters / 4` is a constant, and density is not. Agent traffic (JSON, code,
tool schemas) runs about 2.9 characters per token, so a flat constant estimates
it low and its true input sails past the ceiling the estimate reported. Prose
runs about 5.3, where the constant estimates high. Measured on 2026-10-15, two
bodies that both estimated to exactly 190,000 came back from the upstream as
259,549 (dense) and 142,521 (prose).

The request carries no token count of its own, so the proxy cannot read the
number it needs. It only arrives afterwards, in the upstream's `usage` object on
the response. So the proxy measures each caller instead of assuming a constant:

| Step | Behaviour |
|---|---|
| Feedback | Every served response with a `usage` block yields `prompt_tokens / estimated_tokens` for that caller |
| Running mean | New readings fold into an EMA with `ALPHA = 0.1`. The first reading sets the value outright, so no caller climbs from a default |
| Trust threshold | A caller needs `MIN_SAMPLES = 20` observations before its own factor is used |
| Cold-start fallback | Below 20 samples the caller borrows the factor shared by all callers (`*`); if that also has fewer than 20 the factor is `1.0`, exactly the pre-calibration behaviour |
| Clamp | The factor is clamped to `[1.0, 3.0]`. The lower bound is deliberate: a factor below 1.0 would multiply the estimate down and loosen the gate, so a caller could send sparse traffic to earn a discount and then send one dense body past the ceiling |
| Persistence | At boot the factors are seeded from 7 days of stored rows, so a restart does not cost a caller its calibration |
| What is recorded | The stats row carries the scaled estimate, the number the ceiling was actually applied to, not the raw one |

The gate applies the factor on `/v1/chat/completions` and on the `/v1/*`
passthrough, so no path routes around it. The ceiling value itself stays global:
one `MAX_INPUT_TOKENS` for every caller, and only the density correction is per
caller. There is no knob for the clamp or the sample threshold.

## Endpoints

| Route | Auth | Purpose |
|---|---|---|
| `GET /health` | none | Liveness. Answers 200 whenever the process is up |
| `GET /stats` | `ADMIN_TOKEN` | Usage totals, windowed counters, per-hour buckets, rotation config, the key ring with each key's balance and state, upstream probe state, current outage, install issuance. `total_cost_usd` sums every recorded `cost` and `cost_usd_24h` does the same over a day, which is the number that shows a price change while it is happening |
| `GET /failures?minutes=60&limit=500` | `ADMIN_TOKEN` | Raw failure rows in the window: reason, status, caller, model, attempt count, truncation. `minutes` caps at 1440 and `limit` at 5000 |
| `GET /upstream-health` | `ADMIN_TOKEN` | The upstream probe state on its own, with per-candidate detail |

## Failure tracking

Every request row carries how it went, not only what it answered:

| Column | Meaning |
|---|---|
| `failure_reason` | Why it failed, or `NULL` when it was served. See the ids below |
| `attempts` | Upstream attempts actually sent |
| `plan_len` | Size of the rotation plan built for this request. `1` means rotation was a no-op |
| `served_index` | Which plan entry answered. Anything above `0` is proof rotation engaged and saved the request |
| `truncated` | A chat response that answered 200 but carried no token usage, so the caller got a short answer with no error |
| `key_label` | Label of the upstream key that carried the request, `NULL` on routes that never reach the upstream |
| `upstream_publisher` | Publisher that served it, read from the response header named by `PUBLISHER_HEADER`. `NULL` when that variable is unset |
| `ratelimit_remaining` | Requests left in the serving key's window, read from `x-ratelimit-remaining` |
| `cost` | What the upstream reported the request cost, in USD, from `usage.cost`. `NULL` when the response carried no usage, which is why the `/stats` total is a floor and not a full accounting |

The `failure_reason` ids are a contract with the alerting scripts:

| id | Cause |
|---|---|
| `header_timeout` | No response headers inside `ROTATION_ATTEMPT_TIMEOUT_SECS` |
| `first_byte_timeout` | Headers arrived, then no SSE byte inside the first-token budget |
| `transport` | Connection failed, or the body stream errored |
| `stream_closed` | The upstream closed the stream before sending anything |
| `stream_error` | The body died partway through a non-streaming read |
| `empty_body` | Every candidate answered `200` with no content and no usage. Nothing reached the caller, so the request failed as an error instead of a silent empty answer |
| `upstream_5xx` | Every candidate answered 5xx and the last error body was relayed |
| `upstream_4xx` | The upstream refused this exact request |
| `model_not_allowed` | The allowlist does not serve the requested model |
| `input_too_large` | Estimated input reached `MAX_INPUT_TOKENS`. Refused with `413` before any upstream call |
| `bad_token`, `rate_limited`, `forbidden`, `rejected` | The request never reached a model |

A request that burned three attempts and died on `header_timeout` reads
completely differently from one the upstream refused in 19ms, even though both
land as a 5xx. That difference is the point of the column.

`truncated` and `empty_body` describe the same symptom from opposite sides.
`truncated` marks a response that was committed and then turned out to carry no
usage, so the caller received a short answer and no error. `empty_body` marks
one that was caught before anything was committed, so the caller received an
error and the attempt was rotated away.

## Upstream health probe

The request table only shows failures that reached the proxy, so it cannot say
whether the upstream is broken or one configured candidate is broken while a
healthy one sits unused beside it. From a caller's side those are identical.

The probe asks the upstream directly: one one-token non-streaming completion
per configured candidate, every `PROBE_INTERVAL_SECS`, each result a row in the
`probes` table.

The newest verdict per candidate is also kept in memory and read on the request
path, so a request skips a candidate the probe just measured down instead of
paying its per-attempt budget before the rotation can move past it. The skip is
a reordering aid and never a gate: it expires after three probe intervals, so a
recovered candidate returns to the plan on its own, and it never empties a plan,
so when every candidate is down each one still gets its turn. `/upstream-health`
and the `upstream` block of `/stats` report per candidate whether it is up, when
that state started, how many consecutive probes failed, and the last error. The
summary collapses to the worst
candidate: `state` is `down` when any candidate is down, because one dead alias
is enough to hang every caller whose request lands on it.


## Rotation on upstream failure

Chat requests can survive a dying model. The proxy tries the requested model
first, then the remaining candidates from `ROTATION_MODELS`, at most three
attempts. Rotation is off unless `ROTATION_MODELS` is set, so a proxy with the
variable unset serves one attempt on the model the caller named and never
substitutes another. A candidate counts as failed when the connection dies,
when no response headers arrive inside `ROTATION_ATTEMPT_TIMEOUT_SECS`, when a
small streaming request produces no first SSE byte inside that same budget, or
when the upstream answers 5xx, or when it answers `200` with a body that
carries no answer. A 4xx is never retried: it is the upstream speaking for that
exact request. When every candidate fails with 5xx, the last upstream error
body is relayed; when every candidate is dead, the proxy answers 502 itself.

A `200` is not an answer, which is why the body is inspected before the
response is committed. The upstream can hold a request for half a minute and
then send a well-formed envelope with an empty `content` and no tokens, which
the caller reads as a truncated reply and cannot tell apart from a real one.
The attempt is read far enough to tell the two apart while no byte has reached
the caller yet: a non-streaming body is buffered (it is read whole anyway), and
a streaming body is watched until its first content-bearing event. The bytes
read while proving that go back at the head of the stream, so a healthy answer
still arrives from its first byte and streaming latency is unchanged. The watch
is bounded by the same first-token budget the attempt already used, so it adds
no timeout. A body this cannot parse is treated as an answer: rotating on a
guess would throw away a good response and pay for the prompt twice. A choice
the provider refused on policy grounds (`finish_reason: content_filter`) is an
answer too, since resending the same prompt would collect the same refusal.

Every served response carries an `x-keyproxy-model` header naming the model
that actually produced it, and the stats row records that id as `model_used`,
so a rotated request is visible as the model that answered.

Two cases get no first-token trigger. Non-streaming requests, because their
whole body IS the completion, so nothing shorter than the client's own timeout
is safe. And compaction-scale requests: a body at or above
`ROTATION_TTFT_MAX_BODY_BYTES` (about 130k tokens of JSON history) is a manual
or auto compact, whose first token legitimately takes minutes. Rotating on it
would be a false positive that restarts the same huge prompt on another model.

## Price ceiling

The upstream prices the same model differently depending on which provider it
picks, and the spread is large: under two percent of the official rate on a
normal day, and half the official rate on the day this guard was written. With
no bid attached the choice is the upstream's, so the expensive tier is a thing
that happens to you rather than a thing you agree to.

Every upstream attempt therefore carries a bid: `x-max-input-price` and
`x-max-output-price`, USD per million tokens, from `MAX_INPUT_PRICE` and
`MAX_OUTPUT_PRICE`. The defaults are `0.003` and `0.012`, which is two percent
of the official $0.15 / $0.60 and roughly twenty times the cheapest tier
observed, so ordinary traffic is unaffected.

A bid is a ceiling, not a discount. A request served under it is billed exactly
what it would have been billed without it, so the header costs nothing while
cheap capacity exists. What it changes is the failure direction. When no
provider is available under the bid the upstream answers `402` with the code
`no_provider_under_bid`, and the proxy treats that as a rotation signal: it
moves to the next `ROTATION_MODELS` candidate rather than retiring the key,
because a price refusal says nothing about the key's health. Only when every
candidate refuses does the caller see the `402`.

`0` on either variable sends no header for that side, the same convention the
input ceiling uses. A partial ceiling is allowed.

### Watching what a request actually cost

Every recorded request stores the amount the upstream reported in `usage.cost`,
and `/stats` totals it as `total_cost_usd` and `cost_usd_24h`. That total is the
guard's other half: the bid stops the expensive tier being chosen, and the total
is what shows a price change while it is still happening rather than as a
smaller balance six hours later.

The upstream bills a minimum per request, measured here at `$0.00001`: a 39
token request and a 1,029 token request both billed exactly that, while a 20,029
token request billed `$0.000027`. On a request that small, dividing the charge by
the token count reports a rate far above any sane ceiling, so the "cost above
ceiling" warning skips requests at or below the floor's worth of tokens
(`MIN_CHARGE_USD / ceiling`). Above that point the comparison is real, and a
genuine climb to an expensive tier still warns.

## Upstream key ring

The proxy can hold several upstream keys and pick between them, so a drained or
rate-limited account does not take the service down. Configure them as
`UPSTREAM_KEYS=primary:sk-...,secondary:sk-...`. `UPSTREAM_API_KEY` alone keeps
working and is a ring of one, which makes every rule below a no-op.

Selection is sticky: the active key stays active while it is available, so
normal traffic does not bounce between accounts. A key becomes unavailable when
it is retired (the upstream refused it), when its balance is at or below
`KEY_MIN_BALANCE`, or while it is cooling down after its own rate limit. When
the active key is unavailable the next available one is taken, wrapping, and if
nothing is available the ring still serves on the richest balance rather than
hard-failing the caller.

Keys move on three upstream statuses:

| Status | What it means | What the ring does |
|---|---|---|
| `401`, `402` | Bad key, or out of credit | Retires the key for the life of the process and moves on |
| `429` naming this key's own limit | The account's window is full | Cools the key down for the window in `x-ratelimit-reset` (capped at five minutes) and moves on |
| `429` naming platform capacity (`capacity_saturated`, `no_capacity`) | The platform is out of room, not the account | Nothing. Switching keys would burn a healthy account for a problem it did not cause |

A key-level status is about the account, not the request, so the candidate that
hit it is retried on the new key. The caller only sees the error when no key in
the ring can serve it, and a key is tried at most once per request.

A background poller reads each key's balance from `KEY_BALANCE_URL` every
`KEY_BALANCE_INTERVAL_SECS` and writes a row per key to the `key_balances`
table, so balance history is queryable rather than only current. `/stats` under
`keys` reports the active key, the floor, and per key: balance, account, email,
requests served, whether it is drained or rate-limited, and the age of its last
balance reading.

## Install issuance

Every attempt at the install route is recorded in its own `installs` table:
mints and refusals alike, with the caller address, the install id, the status
and, on a refusal, the reason (`invalid_id`, `already_issued`, `rate_limited`,
`write_failed`, `bad_body`).

The table is separate from `requests` on purpose. `/stats` and the public
snapshot both count every `requests` row with no endpoint filter, so a mint row
there would publish one extra request and one extra caller per install.

`/stats` under `installs` reports `minted_24h`, `minted_total`, `refused_24h`,
`distinct_ips_24h`, `unused_24h` and `active_24h`. The last two are the pair that
separates a key farm from real use: `unused_24h` counts keys minted in the day
that never made a request, `active_24h` counts keys minted in the day that made
five or more. A wave of keys that never calls is a farm; a wave that calls
hundreds of times is people.

## Public usage snapshot

`opencrabs.com/stats/` shows how much traffic the proxy serves. It reads one
static file, never the proxy itself:

```
keyproxy export-public-stats --db /var/lib/keyproxy/stats.db --out /srv/opencrabs/stats/public.json
```

The output is an allowlist: a fixed struct filled from aggregate SQL, with no
database row ever serialized. Provider prefixes, caller IPs, token labels,
upstream publishers and failure detail never leave the box; model ids are
reduced to a capability bucket. The leak test in `src/public_stats.rs` greps the
serialized output for the names it must never contain, so a field added without
thinking fails the suite.

`deploy/keyproxy-public-stats.timer` runs the command every five minutes and
`deploy/keyproxy-public-stats.service` carries the hardening: read one file,
write one directory, no network. Install both with the rest of the deploy:

```
install -m 0644 deploy/keyproxy-public-stats.service deploy/keyproxy-public-stats.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now keyproxy-public-stats.timer
```

The dashboard container mounts `/srv/opencrabs/stats` read-only and serves it
under `/stats-data/`. Nothing in the container writes to that path.

## Build and test

```
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
```

`cargo test` does not rebuild the `keyproxy` binary. Run `cargo build --locked` before a live test.

## Deploy

Deployment needs an explicit go for each host. The steps:

1. `cargo build --release --locked`, then copy the binary to `/usr/local/bin/keyproxy`.
2. Create `/etc/keyproxy.env` (root-only, `chmod 600`) with the values above.
3. Install `deploy/keyproxy.service` as the systemd unit, then `systemctl enable --now keyproxy`.
4. Install `deploy/nginx-inference.conf` for `inference.opencrabs.com`, then run certbot for that host.
