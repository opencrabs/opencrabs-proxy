mod record;
mod stats;

pub use record::{RequestRecord, TokenUsage};
pub use stats::Stats;

use serde::Serialize;
use std::sync::{Arc, Mutex};

/// A row of the probe tail: `(ts, ok, error, latency_ms, model)`.
type ProbeTailRow = (String, bool, Option<String>, Option<i64>, String);

#[derive(Clone)]
pub struct Tracker {
    inner: Arc<TrackerInner>,
}

struct TrackerInner {
    db: Mutex<rusqlite::Connection>,
}

/// One failed or suspect request, as returned by `/failures`.
#[derive(Serialize)]
pub struct FailureRow {
    pub ts: String,
    pub token: String,
    pub model_requested: String,
    pub model_used: String,
    pub status: u16,
    pub latency_ms: u64,
    pub failure_reason: Option<String>,
    pub attempts: Option<u32>,
    pub plan_len: Option<u32>,
    pub served_index: Option<u32>,
    pub truncated: bool,
}

/// One model's probe state.
#[derive(Serialize, Default, Clone)]
pub struct ProbeModel {
    pub model: String,
    /// `up` or `down`.
    pub state: String,
    /// Consecutive same-result probes at the tail for this model.
    pub consecutive_failures: u64,
    /// When this model's current state started.
    pub since: Option<String>,
    pub last_error: Option<String>,
    pub last_latency_ms: Option<u64>,
}

/// Upstream probe state, as returned by `/stats` and `/upstream-health`.
///
/// A probe cycle covers every configured candidate, so the top-level fields
/// describe the worst of them: `state` is `down` when any candidate failed its
/// last probe. That is the field the alert reads, because one dead alias is
/// enough to hang every caller whose request lands on it.
#[derive(Serialize, Default)]
pub struct ProbeSummary {
    /// `up`, `down`, or `unknown` before the first probe has run.
    pub state: String,
    /// When the current state started, from the trailing run of same-result
    /// probes. `None` when nothing has been probed yet.
    pub since: Option<String>,
    /// Consecutive failing probes at the tail, zero when the last one passed.
    pub consecutive_failures: u64,
    pub last_error: Option<String>,
    pub last_latency_ms: Option<u64>,
    pub last_checked: Option<String>,
    /// The model the top-level fields describe: the failing one when any
    /// candidate is down, otherwise the newest probe.
    pub model: Option<String>,
    /// Per-candidate detail, so an alert can name the culprit instead of
    /// reporting a single collapsed verdict.
    pub models: Vec<ProbeModel>,
    /// Probes run and passed in the last 24 hours, so the report can state an
    /// uptime share instead of only a current state.
    pub probes_24h: u64,
    pub ok_24h: u64,
}

/// The current or most recent run of failures, from the requests table.
#[derive(Serialize, Default)]
pub struct Outage {
    /// The newest failure is inside [`ONGOING_WITHIN_MINUTES`], so the run is
    /// probably still going. The probe state is the authoritative signal; this
    /// is the request-side view of the same event.
    pub ongoing: bool,
    pub since: Option<String>,
    pub minutes: u64,
    pub failures: u64,
    pub last_failure: Option<String>,
}

/// A gap of more than this many minutes splits one outage run from the next.
pub const OUTAGE_GAP_MINUTES: u64 = 5;

/// A failure newer than this many minutes means the run is still going.
pub const ONGOING_WITHIN_MINUTES: u64 = 10;

/// How many trailing probes are read to find when the current state started.
const PROBE_RUN_SCAN: u32 = 500;

/// Add a column to `requests`, tolerating a database that already has it.
/// `CREATE TABLE IF NOT EXISTS` never alters an existing table, so every
/// column added after the first release needs this to reach live databases.
fn add_column(conn: &rusqlite::Connection, ddl: &str) {
    match conn.execute(ddl, []) {
        Ok(_) => {}
        Err(e) if e.to_string().contains("duplicate column name") => {}
        Err(e) => panic!("{ddl}: {e}"),
    }
}

impl Tracker {
    pub fn new(path: &str) -> Self {
        let conn = rusqlite::Connection::open(path).expect("open stats db");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS requests (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                ts                TEXT    NOT NULL DEFAULT (datetime('now')),
                ip                TEXT    NOT NULL,
                endpoint          TEXT    NOT NULL,
                model_requested   TEXT,
                model_used        TEXT,
                status            INTEGER NOT NULL,
                latency_ms        INTEGER,
                vision_swap       INTEGER NOT NULL DEFAULT 0,
                prompt_tokens     INTEGER,
                completion_tokens INTEGER,
                total_tokens      INTEGER,
                cached_tokens     INTEGER,
                reasoning_tokens  INTEGER,
                estimated_tokens  INTEGER,
                token_label       TEXT,
                failure_reason    TEXT,
                attempts          INTEGER,
                plan_len          INTEGER,
                served_index      INTEGER,
                truncated         INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_req_ts      ON requests(ts);
            CREATE INDEX IF NOT EXISTS idx_req_model   ON requests(model_used);
            CREATE INDEX IF NOT EXISTS idx_req_ip      ON requests(ip);

            CREATE TABLE IF NOT EXISTS probes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         TEXT    NOT NULL DEFAULT (datetime('now')),
                ok         INTEGER NOT NULL,
                status     INTEGER,
                latency_ms INTEGER,
                model      TEXT,
                error      TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_probe_ts ON probes(ts);

            CREATE TABLE IF NOT EXISTS key_balances (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                ts      TEXT    NOT NULL DEFAULT (datetime('now')),
                label   TEXT    NOT NULL,
                account TEXT,
                email   TEXT,
                balance REAL    NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_keybal_ts ON key_balances(ts);",
        )
        .expect("create stats tables");

        // Databases created before each of these columns lack it.
        //
        // Indexes on these columns are created afterwards, never in the batch
        // above: on a database that predates the column, `CREATE INDEX` on a
        // missing column is a hard error and the proxy would refuse to start
        // against its own stats db.
        add_column(&conn, "ALTER TABLE requests ADD COLUMN token_label TEXT");
        add_column(&conn, "ALTER TABLE requests ADD COLUMN failure_reason TEXT");
        add_column(&conn, "ALTER TABLE requests ADD COLUMN attempts INTEGER");
        add_column(&conn, "ALTER TABLE requests ADD COLUMN plan_len INTEGER");
        add_column(
            &conn,
            "ALTER TABLE requests ADD COLUMN served_index INTEGER",
        );
        add_column(
            &conn,
            "ALTER TABLE requests ADD COLUMN truncated INTEGER NOT NULL DEFAULT 0",
        );
        add_column(&conn, "ALTER TABLE requests ADD COLUMN key_label TEXT");
        add_column(
            &conn,
            "ALTER TABLE requests ADD COLUMN upstream_publisher TEXT",
        );
        add_column(
            &conn,
            "ALTER TABLE requests ADD COLUMN ratelimit_remaining INTEGER",
        );
        add_column(
            &conn,
            "ALTER TABLE requests ADD COLUMN estimated_tokens INTEGER",
        );
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_req_token   ON requests(token_label);
             CREATE INDEX IF NOT EXISTS idx_req_failure ON requests(failure_reason);",
        )
        .expect("create column indexes");

        tracing::info!("stats db: {path}");
        Self {
            inner: Arc::new(TrackerInner {
                db: Mutex::new(conn),
            }),
        }
    }

    /// Return a cheap clone that shares the same database handle.
    /// Safe to move into `tokio::spawn` without holding `AppState`.
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn record(&self, rec: RequestRecord) {
        let db = self.inner.db.lock().unwrap();
        let (pt, ct, tt, cached, reasoning) = match &rec.usage {
            Some(u) => (
                Some(u.prompt_tokens as i64),
                Some(u.completion_tokens as i64),
                Some(u.total_tokens as i64),
                Some(u.cached_tokens as i64),
                Some(u.reasoning_tokens as i64),
            ),
            None => (None, None, None, None, None),
        };
        if let Err(e) = db.execute(
            "INSERT INTO requests \
             (ip, endpoint, model_requested, model_used, status, latency_ms, vision_swap, \
              prompt_tokens, completion_tokens, total_tokens, cached_tokens, reasoning_tokens, \
              token_label, failure_reason, attempts, plan_len, served_index, truncated, \
              key_label, upstream_publisher, ratelimit_remaining, estimated_tokens) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, \
                     ?19, ?20, ?21, ?22)",
            rusqlite::params![
                rec.ip,
                rec.endpoint,
                rec.model_requested,
                rec.model_used,
                rec.status,
                rec.latency_ms,
                rec.vision_swap as i32,
                pt,
                ct,
                tt,
                cached,
                reasoning,
                rec.token,
                rec.failure_reason,
                rec.attempts.map(|n| n as i64),
                rec.plan_len.map(|n| n as i64),
                rec.served_index.map(|n| n as i64),
                rec.truncated as i32,
                rec.key_label,
                rec.upstream_publisher,
                rec.ratelimit_remaining.map(|n| n as i64),
                rec.estimated_tokens.map(|n| n as i64),
            ],
        ) {
            tracing::warn!("stats record error: {e}");
        }
    }

    /// One balance reading for one upstream key, as the control plane
    /// reported it. Append-only, so the history shows when a key crossed the
    /// floor rather than only that it is below it now.
    pub fn record_key_balance(
        &self,
        label: &str,
        account: Option<&str>,
        email: Option<&str>,
        balance: f64,
    ) {
        let db = self.inner.db.lock().unwrap();
        if let Err(e) = db.execute(
            "INSERT INTO key_balances (label, account, email, balance) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![label, account, email, balance],
        ) {
            tracing::warn!("key balance record error: {e}");
        }
    }

    /// Tokens the caller has used since the start of the current UTC day or
    /// month. `since` is a SQLite modifier such as `start of day`.
    pub fn tokens_used_since(&self, label: &str, since: &str) -> u64 {
        let db = self.inner.db.lock().unwrap();
        let sql = format!(
            "SELECT COALESCE(SUM(total_tokens), 0) FROM requests \
             WHERE token_label = ?1 AND ts >= datetime('now', '{since}')"
        );
        match db.query_row(&sql, [label], |r| r.get::<_, i64>(0)) {
            Ok(n) => n.max(0) as u64,
            Err(e) => {
                // Failing open here would hide a cap, so refuse the request.
                tracing::error!("cap lookup failed for {label}: {e}");
                u64::MAX
            }
        }
    }

    /// Mean density per caller over the last `days`, for warming
    /// [`crate::calibrate::Calibration`] at startup.
    ///
    /// Rows are `(label, mean_ratio, samples)`, and only requests that carry
    /// both an estimate and an upstream `prompt_tokens` count. The ratio is
    /// the mean of `actual / estimated` rather than a ratio of sums: one huge
    /// request must not outweigh a thousand ordinary ones.
    ///
    /// Estimates below [`crate::calibrate::MIN_OBSERVATION_ESTIMATE`] are left
    /// out for the same reason the live calibration ignores them: on a tiny
    /// body the upstream's fixed overhead dominates the ratio and the reading
    /// says nothing about density.
    pub fn density_history(&self, days: u32) -> Vec<(String, f64, u32)> {
        let db = self.inner.db.lock().unwrap();
        let sql = format!(
            "SELECT token_label, AVG(CAST(prompt_tokens AS REAL) / estimated_tokens), COUNT(*) \
             FROM requests \
             WHERE token_label IS NOT NULL AND token_label != '' \
               AND estimated_tokens >= {min} AND prompt_tokens > 0 \
               AND ts >= datetime('now', '-{days} days') \
             GROUP BY token_label \
             ORDER BY COUNT(*) DESC",
            min = crate::calibrate::MIN_OBSERVATION_ESTIMATE
        );
        let mut stmt = match db.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("density history prepare failed: {e}");
                return Vec::new();
            }
        };
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, f64>(1)?,
                r.get::<_, i64>(2)? as u32,
            ))
        });
        match rows {
            Ok(rows) => rows.filter_map(Result::ok).collect(),
            Err(e) => {
                tracing::error!("density history query failed: {e}");
                Vec::new()
            }
        }
    }

    /// Requests that failed or answered without usage in the last `minutes`.
    ///
    /// "Failed" is any status at or above 400, plus a 200 chat response that
    /// carried no usage, which is the silent truncation the caller cannot see.
    pub fn failures_since(&self, minutes: u32, limit: u32) -> Vec<FailureRow> {
        let db = self.inner.db.lock().unwrap();
        let mut out = Vec::new();
        let Ok(mut stmt) = db.prepare(
            "SELECT ts, COALESCE(token_label, '-'), COALESCE(model_requested, ''), \
                    COALESCE(model_used, ''), status, COALESCE(latency_ms, 0), failure_reason, \
                    attempts, plan_len, served_index, truncated \
             FROM requests \
             WHERE ts >= datetime('now', ?1) AND (status >= 400 OR truncated = 1) \
             ORDER BY id DESC LIMIT ?2",
        ) else {
            return out;
        };
        let rows = stmt.query_map(
            rusqlite::params![format!("-{minutes} minutes"), limit as i64],
            |r| {
                Ok(FailureRow {
                    ts: r.get(0)?,
                    token: r.get(1)?,
                    model_requested: r.get(2)?,
                    model_used: r.get(3)?,
                    status: r.get(4)?,
                    latency_ms: r.get(5)?,
                    failure_reason: r.get(6)?,
                    attempts: r.get::<_, Option<i64>>(7)?.map(|n| n as u32),
                    plan_len: r.get::<_, Option<i64>>(8)?.map(|n| n as u32),
                    served_index: r.get::<_, Option<i64>>(9)?.map(|n| n as u32),
                    truncated: r.get::<_, i64>(10)? != 0,
                })
            },
        );
        if let Ok(rows) = rows {
            out.extend(rows.flatten());
        }
        out
    }

    /// Persist one upstream probe result.
    pub fn record_probe(
        &self,
        ok: bool,
        status: Option<u16>,
        latency_ms: Option<u64>,
        model: &str,
        error: Option<&str>,
    ) {
        let db = self.inner.db.lock().unwrap();
        if let Err(e) = db.execute(
            "INSERT INTO probes (ok, status, latency_ms, model, error) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                ok as i32,
                status.map(|s| s as i64),
                latency_ms.map(|n| n as i64),
                model,
                error,
            ],
        ) {
            tracing::warn!("probe record error: {e}");
        }
    }

    /// Current upstream state from the probe history.
    pub fn probe_summary(&self) -> ProbeSummary {
        let db = self.inner.db.lock().unwrap();
        let mut summary = ProbeSummary {
            state: "unknown".to_string(),
            ..Default::default()
        };

        let (probes_24h, ok_24h) = db
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(ok), 0) FROM probes \
                 WHERE ts >= datetime('now', '-1 day')",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
            )
            .unwrap_or((0, 0));
        summary.probes_24h = probes_24h.max(0) as u64;
        summary.ok_24h = ok_24h.max(0) as u64;

        // Newest first. Grouped per model below, because a cycle probes every
        // candidate and a flat walk would see the results interleave.
        let mut tail: Vec<ProbeTailRow> = Vec::new();
        if let Ok(mut stmt) = db.prepare(
            "SELECT ts, ok, error, latency_ms, COALESCE(model, '') FROM probes \
             ORDER BY id DESC LIMIT ?1",
        ) {
            if let Ok(rows) = stmt.query_map([PROBE_RUN_SCAN as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)? != 0,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<i64>>(3)?,
                    r.get::<_, String>(4)?,
                ))
            }) {
                tail.extend(rows.flatten());
            }
        }
        if tail.is_empty() {
            return summary;
        }

        // One entry per model, in the order its newest probe appears.
        let mut order: Vec<String> = Vec::new();
        let mut per_model: std::collections::HashMap<String, ProbeModel> =
            std::collections::HashMap::new();
        for (ts, ok, error, latency, model) in &tail {
            if per_model.contains_key(model) {
                continue;
            }
            // Walk back through this model's own history only: that is its
            // current run, uninterrupted by the other candidates' probes.
            let mut since = ts.clone();
            let mut run = 0u64;
            for (ts_here, ok_here, _, _, model_here) in &tail {
                if model_here != model {
                    // Another candidate's probe: not part of this run.
                    continue;
                }
                if *ok_here != *ok {
                    // This model changed state here, so the run ends.
                    break;
                }
                run += 1;
                since = ts_here.clone();
            }
            order.push(model.clone());
            per_model.insert(
                model.clone(),
                ProbeModel {
                    model: model.clone(),
                    state: if *ok { "up" } else { "down" }.to_string(),
                    consecutive_failures: if *ok { 0 } else { run },
                    since: Some(since),
                    last_error: error.clone(),
                    last_latency_ms: latency.map(|n| n.max(0) as u64),
                },
            );
        }

        // The summary describes the worst candidate: a dead one is the whole
        // problem even when every other candidate is healthy.
        let worst = order
            .iter()
            .filter_map(|m| per_model.get(m))
            .find(|m| m.state == "down");
        let newest = order
            .first()
            .and_then(|m| per_model.get(m))
            .expect("the newest model is always present");

        summary.state = if worst.is_some() { "down" } else { "up" }.to_string();
        summary.last_checked = tail.first().map(|(ts, ..)| ts.clone());
        let described = worst.unwrap_or(newest);
        summary.model = Some(described.model.clone());
        summary.last_error = described.last_error.clone();
        summary.last_latency_ms = described.last_latency_ms;
        summary.since = described.since.clone();
        summary.consecutive_failures = described.consecutive_failures;
        summary.models = order
            .into_iter()
            .filter_map(|m| per_model.remove(&m))
            .collect();
        summary
    }

    /// The current or most recent run of failures, with a gap of more than
    /// [`OUTAGE_GAP_MINUTES`] splitting one run from the next.
    pub fn outage(&self) -> Outage {
        let db = self.inner.db.lock().unwrap();
        let mut out = Outage::default();
        let mut stamps: Vec<String> = Vec::new();
        if let Ok(mut stmt) = db.prepare(
            "SELECT ts FROM requests WHERE status >= 500 AND ts >= datetime('now', '-1 day') \
             ORDER BY id DESC LIMIT 5000",
        ) {
            if let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(0)) {
                stamps.extend(rows.flatten());
            }
        }
        let Some(newest) = stamps.first().cloned() else {
            return out;
        };
        out.last_failure = Some(newest.clone());

        // Timestamps are `YYYY-MM-DD HH:MM:SS` in UTC, so a plain string
        // compare is a valid ordering and the gap is read from the minute
        // field by way of `strftime` in SQL. Parse in Rust instead, which
        // keeps this independent of the connection's date functions.
        let parse = |s: &str| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok();
        let mut since = newest.clone();
        let mut failures = 1u64;
        let mut previous = parse(&newest);
        for ts in stamps.iter().skip(1) {
            let Some(current) = parse(ts) else { break };
            let Some(prev) = previous else { break };
            let gap = (prev - current).num_minutes();
            if gap > OUTAGE_GAP_MINUTES as i64 {
                break;
            }
            since = ts.clone();
            failures += 1;
            previous = Some(current);
        }
        out.since = Some(since.clone());
        out.failures = failures;
        if let (Some(start), Some(end)) = (parse(&since), parse(&newest)) {
            out.minutes = (end - start).num_minutes().max(0) as u64;
        }
        // A request that answered after the newest failure ends the run: the
        // proxy is serving again, so this is history rather than a live
        // outage. Without this check the run stays "ongoing" for
        // ONGOING_WITHIN_MINUTES after recovery, which reports an outage that
        // is not happening.
        let answered_since = db
            .query_row(
                "SELECT ts FROM requests WHERE status < 500 AND ts >= datetime('now', '-1 day') \
                 ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
            .is_some_and(|ts| ts > newest);
        out.ongoing = !answered_since
            && parse(&newest)
                .map(|t| {
                    (chrono::Utc::now().naive_utc() - t).num_minutes()
                        <= ONGOING_WITHIN_MINUTES as i64
                })
                .unwrap_or(false);
        out
    }

    pub fn stats(&self) -> Stats {
        let db = self.inner.db.lock().unwrap();
        Stats::from_db(&db)
    }

    /// `/stats` needs the tracker's own derived blocks, which the connection
    /// alone cannot produce.
    ///
    /// The derived blocks each take the connection lock, so this must not be
    /// holding it when it calls them. `std::sync::Mutex` is not reentrant and
    /// the second lock would deadlock the request instead of answering it.
    pub fn stats_with(
        &self,
        rotation: crate::rotation::RotationSummary,
        keys: crate::keys::KeysSummary,
    ) -> Stats {
        let mut stats = {
            let db = self.inner.db.lock().unwrap();
            Stats::from_db(&db)
        };
        stats.rotation = rotation;
        stats.keys = keys;
        stats.upstream = self.probe_summary();
        stats.outage = self.outage();
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(token: &str, status: u16) -> RequestRecord {
        RequestRecord {
            ip: "203.0.113.9".to_string(),
            token: token.to_string(),
            endpoint: "/v1/models",
            status,
            latency_ms: 1,
            ..Default::default()
        }
    }

    #[test]
    fn usage_sums_only_the_callers_rows_in_window() {
        let t = Tracker::new(":memory:");
        let with_tokens = |token: &str, n: u64| RequestRecord {
            usage: Some(TokenUsage {
                prompt_tokens: n,
                completion_tokens: 0,
                total_tokens: n,
                cached_tokens: 0,
                reasoning_tokens: 0,
            }),
            ..row(token, 200)
        };
        t.record(with_tokens("alice", 30));
        t.record(with_tokens("alice", 12));
        t.record(with_tokens("bob", 1000));
        assert_eq!(t.tokens_used_since("alice", "start of day"), 42);
        assert_eq!(t.tokens_used_since("alice", "start of month"), 42);
        assert_eq!(t.tokens_used_since("carol", "start of day"), 0);
    }

    #[test]
    fn usage_excludes_rows_older_than_the_window() {
        let t = Tracker::new(":memory:");
        t.inner
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO requests (ts, ip, endpoint, status, total_tokens, token_label) \
             VALUES ('2000-01-01 00:00:00', '-', '/v1/models', 200, 999, 'alice')",
                [],
            )
            .unwrap();
        assert_eq!(t.tokens_used_since("alice", "start of day"), 0);
        assert_eq!(t.tokens_used_since("alice", "start of month"), 0);
    }

    #[test]
    fn stats_group_requests_by_caller_label() {
        let t = Tracker::new(":memory:");
        t.record(row("alice", 200));
        t.record(row("alice", 200));
        t.record(row("-", 401));
        let s = t.stats();
        assert_eq!(s.requests_by_token.get("alice"), Some(&2));
        assert_eq!(s.requests_by_token.get("-"), Some(&1));
        assert_eq!(s.errors, 1);
    }

    #[test]
    fn probes_table_round_trips() {
        let t = Tracker::new(":memory:");
        t.record_probe(true, Some(200), Some(1_900), "deepseek-v4.1-flash", None);
        t.record_probe(
            false,
            None,
            Some(40_000),
            "combo/cheapest",
            Some("no response headers within 40s"),
        );
        let s = t.probe_summary();
        assert_eq!(s.state, "down");
        assert_eq!(s.consecutive_failures, 1);
        assert_eq!(s.last_latency_ms, Some(40_000));
        assert_eq!(s.model.as_deref(), Some("combo/cheapest"));
        assert_eq!(s.probes_24h, 2);
        assert_eq!(s.ok_24h, 1);
        assert!(s.since.is_some());
    }

    #[test]
    fn probe_summary_is_unknown_until_the_first_probe() {
        let t = Tracker::new(":memory:");
        let s = t.probe_summary();
        assert_eq!(s.state, "unknown");
        assert_eq!(s.consecutive_failures, 0);
        assert_eq!(s.last_checked, None);
    }

    #[test]
    fn probe_summary_counts_a_run_of_failures() {
        let t = Tracker::new(":memory:");
        t.record_probe(true, Some(200), Some(100), "m", None);
        for _ in 0..3 {
            t.record_probe(false, None, Some(40_000), "m", Some("timeout"));
        }
        let s = t.probe_summary();
        assert_eq!(s.state, "down");
        assert_eq!(s.consecutive_failures, 3);
    }

    #[test]
    fn failures_since_returns_errors_and_truncations_only() {
        let t = Tracker::new(":memory:");
        t.record(row("alice", 200));
        t.record(RequestRecord {
            endpoint: "/v1/chat/completions",
            status: 200,
            truncated: true,
            ..row("alice", 200)
        });
        t.record(RequestRecord {
            endpoint: "/v1/chat/completions",
            status: 502,
            failure_reason: Some("header_timeout"),
            attempts: Some(1),
            plan_len: Some(1),
            ..row("bob", 502)
        });
        let rows = t.failures_since(60, 100);
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|r| r.truncated));
        let failed = rows
            .iter()
            .find(|r| r.status == 502)
            .expect("the 502 must be listed");
        assert_eq!(failed.failure_reason.as_deref(), Some("header_timeout"));
        assert_eq!(failed.attempts, Some(1));
        assert_eq!(failed.plan_len, Some(1));
        assert_eq!(failed.token, "bob");
    }

    #[test]
    fn outage_is_absent_without_failures() {
        let t = Tracker::new(":memory:");
        t.record(row("alice", 200));
        let o = t.outage();
        assert!(!o.ongoing);
        assert_eq!(o.failures, 0);
        assert_eq!(o.since, None);
    }

    #[test]
    fn outage_groups_a_contiguous_run_and_ignores_the_older_one() {
        let t = Tracker::new(":memory:");
        // Stamps come from the database clock, which is the same clock the
        // one-day scan window is measured against.
        let stamp = |minutes_ago: i64| -> String {
            t.inner
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT datetime('now', ?1)",
                    [format!("-{minutes_ago} minutes")],
                    |r| r.get(0),
                )
                .unwrap()
        };
        let insert = |ts: &str| {
            t.inner
                .db
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO requests (ts, ip, endpoint, status) \
                     VALUES (?1, '-', '/v1/chat/completions', 502)",
                    [ts],
                )
                .unwrap();
        };
        // A run an hour back, a 48 minute gap, then the current run.
        insert(&stamp(60));
        insert(&stamp(58));
        insert(&stamp(10));
        insert(&stamp(8));
        insert(&stamp(6));
        let o = t.outage();
        assert_eq!(o.failures, 3, "only the contiguous tail counts");
        assert_eq!(o.since.as_deref(), Some(stamp(10).as_str()));
        assert_eq!(o.last_failure.as_deref(), Some(stamp(6).as_str()));
        assert_eq!(o.minutes, 4);
        assert!(o.ongoing, "the newest failure is six minutes old");
    }

    #[test]
    fn outage_stops_being_ongoing_once_a_request_answers() {
        let t = Tracker::new(":memory:");
        let stamp = |minutes_ago: i64| -> String {
            t.inner
                .db
                .lock()
                .unwrap()
                .query_row(
                    "SELECT datetime('now', ?1)",
                    [format!("-{minutes_ago} minutes")],
                    |r| r.get(0),
                )
                .unwrap()
        };
        let insert = |ts: &str, status: u16| {
            t.inner
                .db
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO requests (ts, ip, endpoint, status) \
                     VALUES (?1, '-', '/v1/chat/completions', ?2)",
                    rusqlite::params![ts, status],
                )
                .unwrap();
        };
        // A failure five minutes ago with nothing after it is live.
        insert(&stamp(5), 502);
        assert!(t.outage().ongoing, "nothing has answered since the failure");

        // A success one minute ago ends the run. The failure is still the
        // most recent one, so it is reported, but it is no longer ongoing.
        insert(&stamp(1), 200);
        let o = t.outage();
        assert!(!o.ongoing, "a later success ends the run");
        assert_eq!(o.failures, 1);
        assert!(o.last_failure.is_some());
    }

    #[test]
    fn outage_ignores_failures_older_than_the_scan_window() {
        let t = Tracker::new(":memory:");
        t.inner
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO requests (ts, ip, endpoint, status) \
                 VALUES (datetime('now', '-2 days'), '-', '/v1/chat/completions', 502)",
                [],
            )
            .unwrap();
        let o = t.outage();
        assert_eq!(o.failures, 0);
        assert_eq!(o.since, None);
        assert!(!o.ongoing);
    }

    #[test]
    fn a_pre_existing_database_gains_the_new_columns() {
        // A database with the original schema only, as a live server would
        // have before this change.
        let dir = std::env::temp_dir().join(format!("keyproxy-mig-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("old.db");
        let _ = std::fs::remove_file(&path);
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL DEFAULT (datetime('now')),
                    ip TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    model_requested TEXT,
                    model_used TEXT,
                    status INTEGER NOT NULL,
                    latency_ms INTEGER,
                    vision_swap INTEGER NOT NULL DEFAULT 0,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    cached_tokens INTEGER,
                    reasoning_tokens INTEGER
                );
                INSERT INTO requests (ip, endpoint, status) VALUES ('-', '/v1/models', 200);",
            )
            .unwrap();
        }
        let t = Tracker::new(&path.to_string_lossy());
        // The old row survives, and the new columns accept a write.
        assert_eq!(t.stats().total_requests, 1);
        t.record(RequestRecord {
            endpoint: "/v1/chat/completions",
            status: 502,
            failure_reason: Some("transport"),
            attempts: Some(2),
            plan_len: Some(3),
            served_index: None,
            ..row("-", 502)
        });
        let rows = t.failures_since(60, 10);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].failure_reason.as_deref(), Some("transport"));
        assert_eq!(rows[0].attempts, Some(2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn density_history_averages_only_rows_with_both_numbers() {
        let t = Tracker::new(":memory:");
        let sample = |token: &str, est: Option<u64>, actual: Option<u64>| RequestRecord {
            estimated_tokens: est,
            usage: actual.map(|n| TokenUsage {
                prompt_tokens: n,
                completion_tokens: 0,
                total_tokens: n,
                cached_tokens: 0,
                reasoning_tokens: 0,
            }),
            ..row(token, 200)
        };
        // Two dense readings for alice: 1.5 and 1.3, mean 1.4.
        t.record(sample("alice", Some(100_000), Some(150_000)));
        t.record(sample("alice", Some(100_000), Some(130_000)));
        // Rows with only one of the two numbers teach nothing and are excluded,
        // so they cannot drag a mean toward zero.
        t.record(sample("alice", None, Some(999_999)));
        t.record(sample("alice", Some(999_999), None));
        t.record(sample("bob", Some(10_000), Some(29_000)));

        let history = t.density_history(7);
        let alice = history.iter().find(|(l, _, _)| l == "alice").unwrap();
        assert_eq!(alice.2, 2, "only the two complete rows count");
        assert!((alice.1 - 1.4).abs() < 0.001, "got {}", alice.1);
        let bob = history.iter().find(|(l, _, _)| l == "bob").unwrap();
        assert_eq!(bob.2, 1);
        assert!((bob.1 - 2.9).abs() < 0.001);
    }

    #[test]
    fn density_history_excludes_rows_older_than_the_window() {
        let t = Tracker::new(":memory:");
        t.inner
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO requests (ts, ip, endpoint, status, token_label, \
                 estimated_tokens, prompt_tokens) \
                 VALUES (datetime('now', '-30 days'), '-', '/v1/chat/completions', 200, \
                 'old', 1000, 999999)",
                [],
            )
            .unwrap();
        assert!(
            t.density_history(7).is_empty(),
            "a month-old row must not seed the factor"
        );
    }

    #[test]
    fn density_history_excludes_bodies_too_small_to_measure() {
        let t = Tracker::new(":memory:");
        // A tiny body reads as 12x because the upstream charges a fixed
        // overhead, and seeding it would start the caller above its real
        // density on every restart.
        t.inner
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO requests (ts, ip, endpoint, status, token_label, \
                 estimated_tokens, prompt_tokens) \
                 VALUES (datetime('now', '-1 hours'), '-', '/v1/chat/completions', 200, \
                 'tiny', 2, 24)",
                [],
            )
            .unwrap();
        assert!(
            t.density_history(7).is_empty(),
            "a body under the threshold must not seed the factor"
        );
    }

    #[test]
    fn density_history_ignores_a_tiny_row_beside_real_ones() {
        let t = Tracker::new(":memory:");
        let db = t.inner.db.lock().unwrap();
        // Two honest readings of 1.2 and one tiny one that reads as 12x.
        for (est, actual) in [(10_000u64, 12_000u64), (20_000, 24_000), (2, 24)] {
            db.execute(
                "INSERT INTO requests (ts, ip, endpoint, status, token_label, \
                 estimated_tokens, prompt_tokens) \
                 VALUES (datetime('now', '-1 hours'), '-', '/v1/chat/completions', 200, \
                 'mixed', ?1, ?2)",
                [est.to_string(), actual.to_string()],
            )
            .unwrap();
        }
        drop(db);
        let history = t.density_history(7);
        let mixed = history.iter().find(|(l, _, _)| l == "mixed").unwrap();
        assert_eq!(mixed.2, 2, "the tiny row is excluded");
        assert!((mixed.1 - 1.2).abs() < 0.001, "got {}", mixed.1);
    }

    #[test]
    fn the_estimate_column_exists_and_round_trips() {
        let t = Tracker::new(":memory:");
        let columns: Vec<String> = {
            let db = t.inner.db.lock().unwrap();
            let mut stmt = db.prepare("PRAGMA table_info(requests)").unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(1)).unwrap();
            rows.map(Result::unwrap).collect()
        };
        assert!(
            columns.iter().any(|c| c == "estimated_tokens"),
            "estimated_tokens missing from {columns:?}"
        );

        t.record(RequestRecord {
            estimated_tokens: Some(48_000),
            ..row("alice", 200)
        });
        // A request with no body to estimate leaves the column null, so it
        // never drags a density average toward zero.
        t.record(row("alice", 200));

        let read = |token: &str| -> Option<i64> {
            let db = t.inner.db.lock().unwrap();
            db.query_row(
                "SELECT estimated_tokens FROM requests WHERE token_label = ?1 \
                 ORDER BY id LIMIT 1",
                [token],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(read("alice"), Some(48_000));
        let nulls: i64 = {
            let db = t.inner.db.lock().unwrap();
            db.query_row(
                "SELECT COUNT(*) FROM requests WHERE estimated_tokens IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(nulls, 1, "the bodyless row must stay null");
    }
}
