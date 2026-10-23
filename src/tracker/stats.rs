use crate::keys::KeysSummary;
use crate::rotation::RotationSummary;
use crate::tracker::{Outage, ProbeSummary};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Serialize)]
pub struct Stats {
    pub total_requests: u64,
    pub requests_by_model: HashMap<String, u64>,
    pub requests_by_endpoint: HashMap<String, u64>,
    pub unique_ips: u64,
    pub vision_swaps: u64,
    pub errors: u64,
    pub avg_latency_ms: f64,
    pub total_prompt_tokens: u64,
    pub total_completion_tokens: u64,
    pub total_tokens: u64,
    pub tokens_by_model: HashMap<String, u64>,
    pub total_cached_tokens: u64,
    pub total_reasoning_tokens: u64,
    pub requests_by_token: HashMap<String, u64>,
    pub tokens_by_token: HashMap<String, u64>,
    pub hourly: Vec<HourlyCount>,
    /// Requests, errors, latency and truncations over short windows. The
    /// alert reads these instead of differencing cumulative counters against
    /// a state file, so a missed or duplicated cron run cannot skew it.
    pub recent: Recent,
    /// Why requests failed in the last day, keyed by the machine-readable
    /// reason the proxy recorded. This is the table that separates "the
    /// upstream hung" from "the upstream refused".
    pub failure_reasons_24h: HashMap<String, u64>,
    /// Whether rotation is doing anything, and whether it has saved requests.
    pub rotation_24h: RotationUse,
    /// The configured rotation: names, plan length, per-attempt timeout.
    pub rotation: RotationSummary,
    /// The upstream keys: which one is active, and every key's balance.
    pub keys: KeysSummary,
    /// Upstream reachability from the background probe.
    pub upstream: ProbeSummary,
    /// The current or most recent run of 5xx, with its duration.
    pub outage: Outage,
    /// Issuance counts. A mint writes no row to `requests`, so this is the only
    /// place the number of issued keys is visible.
    pub installs: InstallSummary,
}

/// What the install route did, in counts.
///
/// `requests` cannot answer any of this: a minted key that never called appears
/// nowhere in that table. `unused_24h` is the abuse signal, because a wave of
/// keys issued and never used is a farm, while a wave that calls hundreds of
/// times is people.
#[derive(Serialize, Default)]
pub struct InstallSummary {
    pub minted_24h: u64,
    pub minted_total: u64,
    pub refused_24h: u64,
    pub distinct_ips_24h: u64,
    pub unused_24h: u64,
    pub active_24h: u64,
}

impl InstallSummary {
    pub fn from_db(db: &rusqlite::Connection) -> Self {
        let count = |sql: &str| -> u64 {
            db.query_row(sql, [], |r| r.get::<_, i64>(0))
                .unwrap_or(0)
                .max(0) as u64
        };
        Self {
            minted_24h: count(
                "SELECT COUNT(*) FROM installs \
                 WHERE status = 200 AND ts >= datetime('now', '-1 day')",
            ),
            minted_total: count("SELECT COUNT(*) FROM installs WHERE status = 200"),
            refused_24h: count(
                "SELECT COUNT(*) FROM installs \
                 WHERE status <> 200 AND ts >= datetime('now', '-1 day')",
            ),
            distinct_ips_24h: count(
                "SELECT COUNT(DISTINCT ip) FROM installs \
                 WHERE status = 200 AND ts >= datetime('now', '-1 day')",
            ),
            unused_24h: count(
                "SELECT COUNT(*) FROM installs i \
                 WHERE i.status = 200 AND i.ts >= datetime('now', '-1 day') \
                   AND NOT EXISTS (SELECT 1 FROM requests r WHERE r.token_label = i.install_id)",
            ),
            active_24h: count(
                "SELECT COUNT(*) FROM installs i \
                 WHERE i.status = 200 AND i.ts >= datetime('now', '-1 day') \
                   AND (SELECT COUNT(*) FROM requests r WHERE r.token_label = i.install_id) >= 5",
            ),
        }
    }
}

#[derive(Serialize)]
pub struct HourlyCount {
    pub hour: String,
    pub count: u64,
}

/// Counters for one time window. `p50_ms` and friends are zero when the
/// window holds no timed requests.
#[derive(Serialize, Default)]
pub struct Window {
    pub requests: u64,
    pub errors: u64,
    pub error_rate: f64,
    pub truncated: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub p99_ms: u64,
}

#[derive(Serialize, Default)]
pub struct Recent {
    pub m15: Window,
    pub m60: Window,
    pub h24: Window,
}

/// How the rotation plan behaved over the last day.
#[derive(Serialize, Default)]
pub struct RotationUse {
    /// Chat requests that carried a rotation plan.
    pub chat_requests: u64,
    /// Of those, the ones a candidate other than the first one served. Any
    /// number above zero is proof that rotation engaged.
    pub rotated: u64,
    /// Of those, the ones whose plan held a single candidate, which makes
    /// rotation a no-op however it is configured.
    pub single_plan: u64,
}

impl Stats {
    pub fn from_db(db: &rusqlite::Connection) -> Self {
        let total: u64 = db
            .query_row("SELECT COUNT(*) FROM requests", [], |r| r.get(0))
            .unwrap_or(0);

        let requests_by_model = Self::group_by(
            db,
            "SELECT COALESCE(model_used, 'unknown'), COUNT(*) FROM requests GROUP BY model_used",
        );
        let requests_by_endpoint = Self::group_by(
            db,
            "SELECT endpoint, COUNT(*) FROM requests GROUP BY endpoint",
        );

        let unique_ips: u64 = db
            .query_row("SELECT COUNT(DISTINCT ip) FROM requests", [], |r| r.get(0))
            .unwrap_or(0);

        let vision_swaps: u64 = db
            .query_row(
                "SELECT COUNT(*) FROM requests WHERE vision_swap = 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let errors: u64 = db
            .query_row(
                "SELECT COUNT(*) FROM requests WHERE status >= 400",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let avg_latency_ms: f64 = db
            .query_row(
                "SELECT COALESCE(AVG(latency_ms), 0.0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0.0);

        let total_prompt_tokens: u64 = db
            .query_row(
                "SELECT COALESCE(SUM(prompt_tokens), 0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let total_completion_tokens: u64 = db
            .query_row(
                "SELECT COALESCE(SUM(completion_tokens), 0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let total_tokens: u64 = db
            .query_row(
                "SELECT COALESCE(SUM(total_tokens), 0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let tokens_by_model = Self::group_by(
            db,
            "SELECT COALESCE(model_used, 'unknown'), COALESCE(SUM(total_tokens), 0) \
             FROM requests GROUP BY model_used",
        );

        let total_cached_tokens: u64 = db
            .query_row(
                "SELECT COALESCE(SUM(cached_tokens), 0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let total_reasoning_tokens: u64 = db
            .query_row(
                "SELECT COALESCE(SUM(reasoning_tokens), 0) FROM requests",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);

        let requests_by_token = Self::group_by(
            db,
            "SELECT COALESCE(token_label, '-'), COUNT(*) FROM requests GROUP BY token_label",
        );
        let tokens_by_token = Self::group_by(
            db,
            "SELECT COALESCE(token_label, '-'), COALESCE(SUM(total_tokens), 0) \
             FROM requests GROUP BY token_label",
        );

        let hourly = Self::hourly(db);

        Stats {
            total_requests: total,
            requests_by_model,
            requests_by_endpoint,
            unique_ips,
            vision_swaps,
            errors,
            avg_latency_ms,
            total_prompt_tokens,
            total_completion_tokens,
            total_tokens,
            tokens_by_model,
            total_cached_tokens,
            total_reasoning_tokens,
            requests_by_token,
            tokens_by_token,
            hourly,
            recent: Recent {
                m15: Self::window(db, 15),
                m60: Self::window(db, 60),
                h24: Self::window(db, 24 * 60),
            },
            failure_reasons_24h: Self::failure_reasons(db),
            rotation_24h: Self::rotation_use(db),
            rotation: RotationSummary::default(),
            keys: crate::keys::KeysSummary::default(),
            upstream: ProbeSummary::default(),
            outage: Outage::default(),
            installs: InstallSummary::from_db(db),
        }
    }

    /// One window of request counts plus latency percentiles.
    fn window(db: &rusqlite::Connection, minutes: u32) -> Window {
        let since = format!("-{minutes} minutes");
        let (requests, errors, truncated) = db
            .query_row(
                "SELECT COUNT(*), COALESCE(SUM(status >= 400), 0), COALESCE(SUM(truncated), 0) \
                 FROM requests WHERE ts >= datetime('now', ?1)",
                [&since],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                },
            )
            .unwrap_or((0, 0, 0));
        let requests = requests.max(0) as u64;
        let errors = errors.max(0) as u64;
        Window {
            requests,
            errors,
            error_rate: if requests == 0 {
                0.0
            } else {
                errors as f64 / requests as f64
            },
            truncated: truncated.max(0) as u64,
            p50_ms: Self::percentile(db, &since, 0.50),
            p95_ms: Self::percentile(db, &since, 0.95),
            p99_ms: Self::percentile(db, &since, 0.99),
        }
    }

    /// Nearest-rank percentile of the latency in one window. Reads one row at
    /// the right offset rather than pulling the window into memory.
    fn percentile(db: &rusqlite::Connection, since: &str, p: f64) -> u64 {
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM requests WHERE ts >= datetime('now', ?1) \
                 AND latency_ms IS NOT NULL",
                [since],
                |r| r.get(0),
            )
            .unwrap_or(0);
        if count <= 0 {
            return 0;
        }
        let offset = ((count as f64 - 1.0) * p).round().max(0.0) as i64;
        db.query_row(
            "SELECT latency_ms FROM requests WHERE ts >= datetime('now', ?1) \
             AND latency_ms IS NOT NULL ORDER BY latency_ms LIMIT 1 OFFSET ?2",
            rusqlite::params![since, offset],
            |r| r.get::<_, i64>(0),
        )
        .unwrap_or(0)
        .max(0) as u64
    }

    fn failure_reasons(db: &rusqlite::Connection) -> HashMap<String, u64> {
        Self::group_by(
            db,
            "SELECT failure_reason, COUNT(*) FROM requests \
             WHERE failure_reason IS NOT NULL AND ts >= datetime('now', '-1 day') \
             GROUP BY failure_reason",
        )
    }

    fn rotation_use(db: &rusqlite::Connection) -> RotationUse {
        let row = db.query_row(
            "SELECT COUNT(*), \
                    COALESCE(SUM(served_index > 0), 0), \
                    COALESCE(SUM(plan_len <= 1), 0) \
             FROM requests \
             WHERE endpoint = '/v1/chat/completions' AND plan_len IS NOT NULL \
               AND ts >= datetime('now', '-1 day')",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            },
        );
        match row {
            Ok((chat, rotated, single)) => RotationUse {
                chat_requests: chat.max(0) as u64,
                rotated: rotated.max(0) as u64,
                single_plan: single.max(0) as u64,
            },
            Err(_) => RotationUse::default(),
        }
    }

    fn group_by(db: &rusqlite::Connection, sql: &str) -> HashMap<String, u64> {
        let mut map = HashMap::new();
        if let Ok(mut stmt) = db.prepare(sql) {
            if let Ok(rows) =
                stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))
            {
                for row in rows.flatten() {
                    map.insert(row.0, row.1);
                }
            }
        }
        map
    }

    fn hourly(db: &rusqlite::Connection) -> Vec<HourlyCount> {
        let mut out = Vec::new();
        if let Ok(mut stmt) = db.prepare(
            "SELECT strftime('%Y-%m-%dT%H:00', ts) AS hour, COUNT(*) AS cnt \
             FROM requests GROUP BY hour ORDER BY hour",
        ) {
            if let Ok(rows) = stmt.query_map([], |r| {
                Ok(HourlyCount {
                    hour: r.get::<_, String>(0)?,
                    count: r.get::<_, u64>(1)?,
                })
            }) {
                for row in rows.flatten() {
                    out.push(row);
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use crate::tracker::{RequestRecord, Tracker};

    /// A chat row. Every field the windowed queries read is explicit.
    fn row(
        status: u16,
        latency_ms: u64,
        truncated: bool,
        failure_reason: Option<&'static str>,
        served_index: Option<u32>,
        plan_len: Option<u32>,
    ) -> RequestRecord {
        RequestRecord {
            ip: "203.0.113.9".to_string(),
            token: "alice".to_string(),
            endpoint: "/v1/chat/completions",
            status,
            latency_ms,
            truncated,
            failure_reason,
            served_index,
            plan_len,
            attempts: plan_len,
            ..Default::default()
        }
    }

    #[test]
    fn windows_count_requests_errors_and_truncations() {
        let t = Tracker::new(":memory:");
        // Written now, so all three windows see the same four rows.
        t.record(row(200, 100, false, None, Some(0), Some(1)));
        t.record(row(200, 200, false, None, Some(0), Some(1)));
        t.record(row(200, 300, true, None, Some(0), Some(1)));
        t.record(row(
            502,
            40_000,
            false,
            Some("header_timeout"),
            None,
            Some(1),
        ));
        let s = t.stats();
        for w in [&s.recent.m15, &s.recent.m60, &s.recent.h24] {
            assert_eq!(w.requests, 4);
            assert_eq!(w.errors, 1);
            assert_eq!(w.truncated, 1);
            assert!((w.error_rate - 0.25).abs() < 1e-9, "got {}", w.error_rate);
        }
        // Nearest rank with rounding: round((4 - 1) * 0.50) = 2, the third of
        // four latencies. p99 lands on the last one.
        assert_eq!(s.recent.m60.p50_ms, 300);
        assert_eq!(s.recent.m60.p99_ms, 40_000);
    }

    #[test]
    fn failure_reasons_are_grouped_by_count() {
        let t = Tracker::new(":memory:");
        for _ in 0..3 {
            t.record(row(
                502,
                40_000,
                false,
                Some("header_timeout"),
                None,
                Some(1),
            ));
        }
        t.record(row(502, 20, false, Some("upstream_5xx"), None, Some(1)));
        // A success must not appear in the breakdown.
        t.record(row(200, 100, false, None, Some(0), Some(1)));
        let s = t.stats();
        assert_eq!(s.failure_reasons_24h.get("header_timeout"), Some(&3));
        assert_eq!(s.failure_reasons_24h.get("upstream_5xx"), Some(&1));
        assert_eq!(s.failure_reasons_24h.len(), 2);
    }

    #[test]
    fn rotation_use_separates_rotated_from_single_plan() {
        let t = Tracker::new(":memory:");
        // Served by the second candidate: rotation engaged.
        t.record(row(200, 900, false, None, Some(1), Some(3)));
        // Served by the first candidate of a three-candidate plan.
        t.record(row(200, 900, false, None, Some(0), Some(3)));
        // A one-candidate plan can never rotate.
        t.record(row(
            502,
            40_000,
            false,
            Some("header_timeout"),
            None,
            Some(1),
        ));
        // A non-chat route carries no plan and must not be counted.
        t.record(RequestRecord {
            endpoint: "/v1/models",
            ..row(200, 10, false, None, None, None)
        });
        let s = t.stats();
        assert_eq!(s.rotation_24h.chat_requests, 3);
        assert_eq!(s.rotation_24h.rotated, 1);
        assert_eq!(s.rotation_24h.single_plan, 1);
    }

    #[test]
    fn an_empty_database_reports_zeros_not_errors() {
        let t = Tracker::new(":memory:");
        let s = t.stats();
        assert_eq!(s.recent.m15.requests, 0);
        assert_eq!(s.recent.m15.error_rate, 0.0);
        assert_eq!(s.recent.h24.p99_ms, 0);
        assert_eq!(s.rotation_24h.chat_requests, 0);
        assert_eq!(s.failure_reasons_24h.len(), 0);
        assert_eq!(s.upstream.state, "");
    }

    #[test]
    fn rotation_summary_is_filled_by_stats_with() {
        let t = Tracker::new(":memory:");
        let summary =
            crate::rotation::Rotation::for_tests(&["m1", "m2"], std::time::Duration::from_secs(40))
                .summary();
        let s = t.stats_with(summary, crate::keys::KeysSummary::default());
        assert_eq!(s.rotation.plan_len, 2);
        assert!(s.rotation.engaging);
        assert_eq!(s.rotation.attempt_timeout_secs, 40);
    }
}
