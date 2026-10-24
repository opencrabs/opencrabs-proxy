//! Sanitized public usage snapshot.
//!
//! This module exists for one consumer: a public dashboard that shows how much
//! traffic the proxy serves without showing who serves it.
//!
//! It is an allowlist, not a blocklist. [`PublicStats`] is a fixed struct built
//! from aggregate SQL, and no database row is ever serialized. The columns that
//! must never leave the box are:
//!
//! - `model_used` and `model_requested`, which carry a provider prefix such as
//!   `cb/` or `ali/`. Model ids are reduced to a capability bucket instead.
//! - `ip` and `token_label`, which identify callers.
//! - `key_label` and `upstream_publisher`, which identify the upstream.
//! - `failure_reason`, `status`, `attempts`, `plan_len` and `served_index`,
//!   which describe how the private routing behaved.
//!
//! Anything added here has to be aggregate, and the leak test in this module
//! greps the serialized output for the names it must never contain.

use chrono::{SecondsFormat, Utc};
use rusqlite::{Connection, OpenFlags};
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::Duration;

/// Bumped when a field is renamed or removed, so the page can refuse a shape it
/// does not understand instead of rendering blanks.
/// Bumped to 2 when the distinct-caller counts were added. The page reads the
/// version so a stale snapshot cannot be rendered as zeros.
pub const SCHEMA: u32 = 2;

/// Days of per-day rows carried in the snapshot. Weekly and monthly rollups are
/// computed in the browser from this one series, so there is one source of
/// truth and the file stays small.
pub const DAILY_DAYS: u32 = 90;

/// The four capability buckets. A request belongs to exactly one.
pub const CAPABILITIES: [&str; 4] = ["chat", "image", "audio", "other"];

#[derive(Serialize, Debug)]
pub struct PublicStats {
    pub schema: u32,
    pub generated_at: String,
    /// `YYYY-MM-DD` of the oldest recorded request, `None` on an empty
    /// database. The page shows it so a young history cannot read as a
    /// month-long outage.
    pub history_since: Option<String>,
    pub totals: Totals,
    pub windows: Windows,
    pub daily: Vec<DailyPoint>,
    pub hourly: Vec<HourlyPoint>,
    pub capabilities: Vec<CapabilityCount>,
}

#[derive(Serialize, Debug, Default)]
pub struct Totals {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    /// Share of input tokens served from the upstream cache, 0 to 100.
    pub cache_hit_pct: f64,
    pub reasoning_tokens: u64,
    pub median_latency_ms: u64,
    /// Distinct caller IPs. A count only: the addresses themselves never leave
    /// the database. This is reach, not a headcount, because a rotating mobile
    /// address counts again and a shared office address counts once.
    pub unique_ips: u64,
    /// Distinct client labels. Also a count only. The closer measure of "how
    /// many API clients": one client on several addresses counts once.
    pub unique_clients: u64,
}

#[derive(Serialize, Debug, Default)]
pub struct Window {
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub cache_hit_pct: f64,
    pub median_latency_ms: u64,
    /// Distinct caller IPs inside this window, count only.
    pub unique_ips: u64,
    pub unique_clients: u64,
}

/// Trailing windows, computed in SQL so the page does not have to reconcile
/// day boundaries against its own clock.
#[derive(Serialize, Debug, Default)]
pub struct Windows {
    pub h24: Window,
    pub d7: Window,
    pub d30: Window,
}

#[derive(Serialize, Debug)]
pub struct DailyPoint {
    pub date: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub median_latency_ms: u64,
    /// Distinct caller IPs that day, count only.
    pub unique_ips: u64,
    /// Distinct client labels that day, count only.
    pub unique_clients: u64,
}

#[derive(Serialize, Debug)]
pub struct HourlyPoint {
    /// Hour of day in UTC, 0 to 23.
    pub hour: u32,
    pub requests: u64,
}

#[derive(Serialize, Debug)]
pub struct CapabilityCount {
    pub kind: &'static str,
    pub requests: u64,
}

/// Open the stats database read-only and build a snapshot.
///
/// Read-only matters twice: the proxy keeps the database in rollback-journal
/// mode, so a writer can hold a lock, and this job must never be able to
/// change a row even if it is pointed at the wrong file.
/// Build a snapshot from the database at `db` and write it to `out`.
///
/// The write is atomic: the snapshot goes to a sibling temp file which is then
/// renamed over the target, so a reader never observes a half-written file and
/// a failed run leaves the previous snapshot in place. The parent directory is
/// created when missing, so the timer works on a fresh box.
pub fn export(db_path: &str, out_path: &str) -> std::io::Result<PublicStats> {
    let stats = from_path(db_path).map_err(|e| std::io::Error::other(format!("{db_path}: {e}")))?;
    let body = serde_json::to_vec_pretty(&stats).map_err(std::io::Error::other)?;

    let out = std::path::Path::new(out_path);
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Same directory as the target, so the rename stays on one filesystem.
    let tmp = out.with_extension("json.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&body)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    // Create honours the umask; the file has to stay world readable so nginx
    // can serve it from a container that runs as a different user.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o644))?;
    }
    std::fs::rename(&tmp, out)?;
    Ok(stats)
}

/// `keyproxy export-public-stats --db <path> --out <path>`
///
/// Exits non-zero on a missing flag or an unreadable database, so the systemd
/// unit reports a failure instead of silently leaving a stale snapshot.
pub fn export_command(args: &[String]) {
    let mut db = None;
    let mut out = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--db" => db = it.next().cloned(),
            "--out" => out = it.next().cloned(),
            "-h" | "--help" => {
                usage();
                return;
            }
            other => {
                eprintln!("error: unexpected argument `{other}`");
                usage();
                std::process::exit(2);
            }
        }
    }
    let (Some(db), Some(out)) = (db, out) else {
        usage();
        std::process::exit(2);
    };
    match export(&db, &out) {
        Ok(s) => eprintln!(
            "wrote {out}: {} requests, {} daily points, {} bytes",
            s.totals.requests,
            s.daily.len(),
            std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0)
        ),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

fn usage() {
    eprintln!("usage: keyproxy export-public-stats --db <stats.db> --out <public.json>");
}

pub fn from_path(path: &str) -> rusqlite::Result<PublicStats> {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    db.busy_timeout(Duration::from_secs(5))?;
    Ok(build(&db))
}

pub fn build(db: &Connection) -> PublicStats {
    PublicStats {
        schema: SCHEMA,
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        history_since: db
            .query_row("SELECT date(MIN(ts)) FROM requests", [], |r| {
                r.get::<_, Option<String>>(0)
            })
            .ok()
            .flatten(),
        totals: totals(db),
        windows: Windows {
            h24: window(db, "-1 day"),
            d7: window(db, "-7 days"),
            d30: window(db, "-30 days"),
        },
        daily: daily(db),
        hourly: hourly(db),
        capabilities: capabilities(db),
    }
}

fn totals(db: &Connection) -> Totals {
    let (requests, input, output, cached, reasoning, ips, clients) = db
        .query_row(
            "SELECT COUNT(*), \
                    COALESCE(SUM(prompt_tokens), 0), \
                    COALESCE(SUM(completion_tokens), 0), \
                    COALESCE(SUM(cached_tokens), 0), \
                    COALESCE(SUM(reasoning_tokens), 0), \
                    COUNT(DISTINCT ip), \
                    COUNT(DISTINCT token_label) \
             FROM requests",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                ))
            },
        )
        .unwrap_or((0, 0, 0, 0, 0, 0, 0));
    Totals {
        requests: count(requests),
        input_tokens: count(input),
        output_tokens: count(output),
        cached_tokens: count(cached),
        cache_hit_pct: pct(cached, input),
        reasoning_tokens: count(reasoning),
        median_latency_ms: median_latency(db, None),
        unique_ips: count(ips),
        unique_clients: count(clients),
    }
}

fn window(db: &Connection, since: &str) -> Window {
    let (requests, input, output, cached, ips, clients) = db
        .query_row(
            "SELECT COUNT(*), \
                    COALESCE(SUM(prompt_tokens), 0), \
                    COALESCE(SUM(completion_tokens), 0), \
                    COALESCE(SUM(cached_tokens), 0), \
                    COUNT(DISTINCT ip), \
                    COUNT(DISTINCT token_label) \
             FROM requests WHERE ts >= datetime('now', ?1)",
            [since],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            },
        )
        .unwrap_or((0, 0, 0, 0, 0, 0));
    Window {
        requests: count(requests),
        input_tokens: count(input),
        output_tokens: count(output),
        cached_tokens: count(cached),
        cache_hit_pct: pct(cached, input),
        median_latency_ms: median_latency(db, Some(since)),
        unique_ips: count(ips),
        unique_clients: count(clients),
    }
}

/// Per-day requests, tokens and median latency for the last [`DAILY_DAYS`].
///
/// The median comes from one window-function query rather than a loop of
/// nearest-rank reads: ninety separate scans of a table that only grows is the
/// wrong shape for a job that runs every five minutes.
fn daily(db: &Connection) -> Vec<DailyPoint> {
    let since = format!("-{DAILY_DAYS} days");
    let sql = "WITH d AS ( \
                   SELECT date(ts) AS day, COUNT(*) AS requests, \
                          COALESCE(SUM(prompt_tokens), 0) AS input_tokens, \
                          COALESCE(SUM(completion_tokens), 0) AS output_tokens, \
                          COALESCE(SUM(cached_tokens), 0) AS cached_tokens, \
                          COUNT(DISTINCT ip) AS unique_ips, \
                          COUNT(DISTINCT token_label) AS unique_clients \
                   FROM requests WHERE ts >= datetime('now', ?1) GROUP BY day \
               ), \
               l AS ( \
                   SELECT day, latency_ms FROM ( \
                       SELECT date(ts) AS day, latency_ms, \
                              ROW_NUMBER() OVER (PARTITION BY date(ts) ORDER BY latency_ms) AS rn, \
                              COUNT(*) OVER (PARTITION BY date(ts)) AS cnt \
                       FROM requests \
                       WHERE ts >= datetime('now', ?1) AND latency_ms IS NOT NULL \
                   ) WHERE rn = CAST(ROUND((cnt - 1) * 0.5) AS INTEGER) + 1 \
               ) \
               SELECT d.day, d.requests, d.input_tokens, d.output_tokens, d.cached_tokens, \
                      COALESCE(l.latency_ms, 0), d.unique_ips, d.unique_clients \
               FROM d LEFT JOIN l ON l.day = d.day ORDER BY d.day";
    let mut out = Vec::new();
    if let Ok(mut stmt) = db.prepare(sql) {
        if let Ok(rows) = stmt.query_map([&since], |r| {
            Ok(DailyPoint {
                date: r.get::<_, String>(0)?,
                requests: count(r.get::<_, i64>(1)?),
                input_tokens: count(r.get::<_, i64>(2)?),
                output_tokens: count(r.get::<_, i64>(3)?),
                cached_tokens: count(r.get::<_, i64>(4)?),
                median_latency_ms: count(r.get::<_, i64>(5)?),
                unique_ips: count(r.get::<_, i64>(6)?),
                unique_clients: count(r.get::<_, i64>(7)?),
            })
        }) {
            out.extend(rows.flatten());
        }
    }
    out
}

/// Requests per hour of day, always 24 entries so the chart has a fixed axis
/// instead of bars that shift position as the day fills in.
fn hourly(db: &Connection) -> Vec<HourlyPoint> {
    let mut buckets = [0u64; 24];
    if let Ok(mut stmt) = db.prepare(
        "SELECT CAST(strftime('%H', ts) AS INTEGER) AS h, COUNT(*) \
         FROM requests GROUP BY h",
    ) {
        if let Ok(rows) = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))) {
            for (hour, n) in rows.flatten() {
                if (0..24).contains(&hour) {
                    buckets[hour as usize] = count(n);
                }
            }
        }
    }
    (0..24u32)
        .map(|hour| HourlyPoint {
            hour,
            requests: buckets[hour as usize],
        })
        .collect()
}

/// Request counts per capability bucket, always all four in a fixed order.
fn capabilities(db: &Connection) -> Vec<CapabilityCount> {
    let mut totals: BTreeMap<&'static str, u64> = CAPABILITIES.iter().map(|k| (*k, 0u64)).collect();
    if let Ok(mut stmt) = db.prepare(
        "SELECT endpoint, model_used, COUNT(*) FROM requests GROUP BY endpoint, model_used",
    ) {
        if let Ok(rows) = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, i64>(2)?,
            ))
        }) {
            for (endpoint, model, n) in rows.flatten() {
                let kind = capability(&endpoint, model.as_deref());
                if let Some(slot) = totals.get_mut(kind) {
                    *slot += count(n);
                }
            }
        }
    }
    CAPABILITIES
        .iter()
        .map(|kind| CapabilityCount {
            kind,
            requests: totals.get(kind).copied().unwrap_or(0),
        })
        .collect()
}

/// Reduce a model id and route to one bucket.
///
/// Model ids are matched on capability keywords only, never echoed, so a
/// provider prefix in the id cannot reach the output. Audio is checked before
/// chat because the voice models are served on the chat route.
pub fn capability(endpoint: &str, model: Option<&str>) -> &'static str {
    let ep = endpoint.to_ascii_lowercase();
    let m = model.unwrap_or_default().to_ascii_lowercase();
    let image = m.contains("image")
        || m.contains("imagen")
        || m.contains("dall-e")
        || m.contains("stable-diffusion")
        || ep.starts_with("/v1/images");
    if image {
        return "image";
    }
    let audio = m.contains("omni")
        || m.contains("audio")
        || m.contains("tts")
        || m.contains("stt")
        || m.contains("whisper")
        || m.contains("transcribe")
        || m.contains("asr")
        || m.contains("paraformer")
        || m.contains("sensevoice")
        || m.contains("funasr")
        || ep.starts_with("/v1/audio");
    if audio {
        return "audio";
    }
    if ep.starts_with("/v1/chat") {
        return "chat";
    }
    "other"
}

/// Nearest-rank median, matching the convention in `tracker::stats`.
///
/// Reads one row at the right offset instead of pulling the window into
/// memory, so the cost does not grow with the table.
fn median_latency(db: &Connection, since: Option<&str>) -> u64 {
    let (count_sql, row_sql) = match since {
        Some(_) => (
            "SELECT COUNT(*) FROM requests \
             WHERE latency_ms IS NOT NULL AND ts >= datetime('now', ?1)",
            "SELECT latency_ms FROM requests \
             WHERE latency_ms IS NOT NULL AND ts >= datetime('now', ?1) \
             ORDER BY latency_ms LIMIT 1 OFFSET ?2",
        ),
        None => (
            "SELECT COUNT(*) FROM requests WHERE latency_ms IS NOT NULL",
            "SELECT latency_ms FROM requests WHERE latency_ms IS NOT NULL \
             ORDER BY latency_ms LIMIT 1 OFFSET ?1",
        ),
    };
    let count: i64 = match since {
        Some(s) => db.query_row(count_sql, [s], |r| r.get(0)),
        None => db.query_row(count_sql, [], |r| r.get(0)),
    }
    .unwrap_or(0);
    if count <= 0 {
        return 0;
    }
    let offset = (((count as f64) - 1.0) * 0.5).round().max(0.0) as i64;
    let lat: i64 = match since {
        Some(s) => db.query_row(row_sql, rusqlite::params![s, offset], |r| r.get(0)),
        None => db.query_row(row_sql, [offset], |r| r.get(0)),
    }
    .unwrap_or(0);
    lat.max(0) as u64
}

fn count(n: i64) -> u64 {
    n.max(0) as u64
}

fn pct(part: i64, whole: i64) -> f64 {
    if whole <= 0 {
        return 0.0;
    }
    let v = (part as f64 / whole as f64) * 100.0;
    (v * 100.0).round() / 100.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// The columns the aggregator reads, in the shape the proxy creates them.
    /// Kept here rather than borrowing `Tracker` so a test can write rows at
    /// arbitrary timestamps, which `Tracker::record` cannot do.
    fn fixture() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE requests (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                ts                   TEXT    NOT NULL,
                ip                   TEXT    NOT NULL,
                endpoint             TEXT    NOT NULL,
                model_requested      TEXT,
                model_used           TEXT,
                status               INTEGER NOT NULL,
                latency_ms           INTEGER,
                prompt_tokens        INTEGER,
                completion_tokens    INTEGER,
                total_tokens         INTEGER,
                cached_tokens        INTEGER,
                reasoning_tokens     INTEGER,
                estimated_tokens     INTEGER,
                token_label          TEXT,
                failure_reason       TEXT,
                attempts             INTEGER,
                plan_len             INTEGER,
                served_index         INTEGER,
                truncated            INTEGER NOT NULL DEFAULT 0,
                key_label            TEXT,
                upstream_publisher   TEXT,
                ratelimit_remaining  INTEGER
            );",
        )
        .unwrap();
        db
    }

    /// A synthetic request row: how far back it sits plus its latency and token split.
    struct Row<'a> {
        offset: &'a str,
        model: &'a str,
        latency_ms: i64,
        prompt: i64,
        completion: i64,
        cached: i64,
    }

    /// Insert one synthetic chat-completions row described by `row`.
    fn insert(db: &Connection, row: Row<'_>) {
        insert_caller(db, row, "198.51.100.7", "alice");
    }

    /// Same row with a chosen caller identity, so the distinct counts have
    /// something to tell apart. Documentation-range addresses (RFC 5737) and
    /// invented labels, never anything out of a real database.
    fn insert_caller(db: &Connection, row: Row<'_>, ip: &str, label: &str) {
        db.execute(
            "INSERT INTO requests \
             (ts, ip, endpoint, model_requested, model_used, status, latency_ms, \
              prompt_tokens, completion_tokens, total_tokens, cached_tokens, \
              reasoning_tokens, token_label, attempts, plan_len, served_index, \
              key_label, upstream_publisher, truncated) \
             VALUES (datetime('now', ?1), ?2, '/v1/chat/completions', ?3, ?3, 200, ?4, \
                     ?5, ?6, ?5 + ?6, ?7, 0, ?8, 1, 1, 0, 'key-a', 'pub-x', 0)",
            rusqlite::params![
                row.offset,
                ip,
                row.model,
                row.latency_ms,
                row.prompt,
                row.completion,
                row.cached,
                label
            ],
        )
        .unwrap();
    }

    fn four_rows() -> Connection {
        let db = fixture();
        insert(
            &db,
            Row {
                offset: "-10 days",
                model: "chat-model",
                latency_ms: 100,
                prompt: 1_000,
                completion: 400,
                cached: 800,
            },
        );
        insert(
            &db,
            Row {
                offset: "-25 hours",
                model: "chat-model",
                latency_ms: 300,
                prompt: 2_000,
                completion: 100,
                cached: 1_500,
            },
        );
        insert(
            &db,
            Row {
                offset: "-25 hours",
                model: "chat-model",
                latency_ms: 200,
                prompt: 2_000,
                completion: 100,
                cached: 1_000,
            },
        );
        insert(
            &db,
            Row {
                offset: "+0 seconds",
                model: "chat-model",
                latency_ms: 50,
                prompt: 500,
                completion: 500,
                cached: 500,
            },
        );
        db
    }

    fn every_key(v: &Value, out: &mut Vec<String>) {
        match v {
            Value::Object(map) => {
                for (k, inner) in map {
                    out.push(k.clone());
                    every_key(inner, out);
                }
            }
            Value::Array(items) => {
                for inner in items {
                    every_key(inner, out);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn capability_buckets_by_model_keyword_and_route() {
        let chat = "/v1/chat/completions";
        assert_eq!(capability(chat, Some("cb/deepseek-v4.1-flash")), "chat");
        assert_eq!(capability(chat, Some("deepseek-v4.1-flash")), "chat");
        assert_eq!(capability(chat, Some("qwen3.8-omni-flash")), "audio");
        assert_eq!(capability(chat, Some("gpt-4o-mini-tts")), "audio");
        assert_eq!(capability(chat, Some("whisper-large-v3")), "audio");
        assert_eq!(capability(chat, Some("gemini-3.1-flash-image")), "image");
        assert_eq!(capability(chat, Some("imagen-4.0-generate-001")), "image");
        // A vision chat model is still a chat request: it reads an image and
        // answers in text.
        assert_eq!(capability(chat, Some("mimo/mimo-v2.6-flash")), "chat");
        assert_eq!(
            capability("/v1/models", Some("deepseek-v4.1-flash")),
            "other"
        );
        assert_eq!(capability("/v1/* passthrough", None), "other");
        assert_eq!(capability("install", None), "other");
        assert_eq!(capability("/v1/images/generations", None), "image");
        assert_eq!(capability("/v1/audio/transcriptions", None), "audio");
    }

    #[test]
    fn totals_windows_and_daily_roll_ups() {
        let db = four_rows();
        let s = build(&db);

        // Lifetime: four rows, tokens summed, cache share of input.
        assert_eq!(s.totals.requests, 4);
        assert_eq!(s.totals.input_tokens, 5_500);
        assert_eq!(s.totals.output_tokens, 1_100);
        assert_eq!(s.totals.cached_tokens, 3_800);
        assert_eq!(s.totals.cache_hit_pct, 69.09);
        // Sorted latencies [50, 100, 200, 300]: nearest rank for n=4 is the
        // third one.
        assert_eq!(s.totals.median_latency_ms, 200);

        // The 24 hour window holds only the row written at `now`, because the
        // other two are 25 hours old by construction.
        assert_eq!(s.windows.h24.requests, 1);
        assert_eq!(s.windows.d7.requests, 3);
        assert_eq!(s.windows.d30.requests, 4);

        // Three calendar days: -10 days, yesterday, today.
        assert_eq!(s.daily.len(), 3);
        let yesterday = &s.daily[1];
        assert_eq!(yesterday.requests, 2);
        assert_eq!(yesterday.input_tokens, 4_000);
        assert_eq!(yesterday.output_tokens, 200);
        assert_eq!(yesterday.cached_tokens, 2_500);
        assert_eq!(yesterday.median_latency_ms, 300);
        assert_eq!(s.daily[2].requests, 1);
        assert_eq!(s.daily[2].median_latency_ms, 50);
        assert_eq!(s.daily[0].median_latency_ms, 100);
    }

    #[test]
    fn distinct_callers_dedupe_by_ip_and_by_label() {
        let db = fixture();
        let row = |offset: &'static str| Row {
            offset,
            model: "chat-model",
            latency_ms: 100,
            prompt: 100,
            completion: 10,
            cached: 0,
        };
        // Two addresses and two labels across four rows: alice shows up twice on
        // each address, bob shares the first. The counts must come out as 2 and
        // 2, not 4 and 4.
        insert_caller(&db, row("+0 seconds"), "203.0.113.1", "alice");
        insert_caller(&db, row("+0 seconds"), "203.0.113.1", "bob");
        insert_caller(&db, row("+0 seconds"), "203.0.113.2", "alice");
        insert_caller(&db, row("+0 seconds"), "203.0.113.2", "alice");
        // One more outside every rolling window but inside the daily series.
        insert_caller(&db, row("-10 days"), "203.0.113.9", "carol");

        let s = build(&db);
        assert_eq!(s.totals.requests, 5);
        assert_eq!(s.totals.unique_ips, 3);
        assert_eq!(s.totals.unique_clients, 3);

        assert_eq!(s.windows.h24.requests, 4);
        assert_eq!(s.windows.h24.unique_ips, 2);
        assert_eq!(s.windows.h24.unique_clients, 2);
        assert_eq!(s.windows.d7.unique_ips, 2);
        assert_eq!(s.windows.d30.unique_ips, 3);

        let today = s.daily.last().unwrap();
        assert_eq!(today.unique_ips, 2);
        assert_eq!(today.unique_clients, 2);
        let old = s.daily.first().unwrap();
        assert_eq!(old.unique_ips, 1);
        assert_eq!(old.unique_clients, 1);
    }

    #[test]
    fn hourly_always_carries_twenty_four_buckets() {
        let db = four_rows();
        let s = build(&db);
        assert_eq!(s.hourly.len(), 24);
        let total: u64 = s.hourly.iter().map(|h| h.requests).sum();
        assert_eq!(total, 4);
        assert_eq!(s.hourly[0].hour, 0);
        assert_eq!(s.hourly[23].hour, 23);
    }

    #[test]
    fn capabilities_always_carry_four_buckets_in_order() {
        let db = four_rows();
        let s = build(&db);
        let kinds: Vec<&str> = s.capabilities.iter().map(|c| c.kind).collect();
        assert_eq!(kinds, CAPABILITIES.to_vec());
        assert_eq!(s.capabilities[0].requests, 4);
        assert_eq!(s.capabilities[1].requests, 0);
        assert_eq!(s.capabilities[2].requests, 0);
        assert_eq!(s.capabilities[3].requests, 0);
    }

    #[test]
    fn an_empty_database_reports_zeros_not_errors() {
        let db = fixture();
        let s = build(&db);
        assert_eq!(s.schema, SCHEMA);
        assert_eq!(s.totals.requests, 0);
        assert_eq!(s.totals.cache_hit_pct, 0.0);
        assert_eq!(s.totals.median_latency_ms, 0);
        assert_eq!(s.history_since, None);
        assert!(s.daily.is_empty());
        assert_eq!(s.hourly.len(), 24);
        assert!(s.hourly.iter().all(|h| h.requests == 0));
        assert_eq!(s.capabilities.len(), 4);
    }

    #[test]
    fn serialized_json_uses_only_allowlisted_keys() {
        let db = four_rows();
        let json = serde_json::to_value(build(&db)).unwrap();
        let mut keys = Vec::new();
        every_key(&json, &mut keys);

        let allowed = [
            "schema",
            "generated_at",
            "history_since",
            "totals",
            "windows",
            "h24",
            "d7",
            "d30",
            "daily",
            "hourly",
            "capabilities",
            "requests",
            "input_tokens",
            "output_tokens",
            "cached_tokens",
            "cache_hit_pct",
            "reasoning_tokens",
            "median_latency_ms",
            "date",
            "hour",
            "kind",
            "unique_ips",
            "unique_clients",
        ];
        for key in &keys {
            assert!(
                allowed.contains(&key.as_str()),
                "`{key}` is not on the public allowlist: {}",
                json
            );
        }
        assert!(!keys.is_empty(), "the walk found no keys at all");
    }

    /// Strings that must never appear anywhere in a public snapshot. Each one
    /// is a real value the private deployment uses: provider prefixes, an
    /// upstream publisher, key labels, caller labels and failure reasons.
    const FORBIDDEN: &[&str] = &[
        "cb/",
        "ali/",
        "alicn",
        "combo/",
        "plain/",
        "mimo/",
        "ocg/",
        "qwencloud",
        "codebuddy",
        "CodeBuddy",
        "Alibaba",
        "OpenCrabs",
        "key-a",
        "key-b",
        "alice",
        "bob",
        "198.51.100.7",
        "203.0.113.9",
        "upstream_5xx",
        "header_timeout",
        "first_byte_timeout",
        "transport",
        "stream_closed",
        "stream_error",
        "deepseek",
        "gemini",
        "claude",
        "whisper",
        "imagen",
    ];

    /// Scan a serialized snapshot for every forbidden string.
    ///
    /// This is the guard the page depends on: the struct is an allowlist, so
    /// this only has to catch a field someone added without thinking about
    /// what it carries.
    fn leaks(json: &str) -> Vec<&'static str> {
        FORBIDDEN
            .iter()
            .copied()
            .filter(|needle| json.contains(needle))
            .collect()
    }

    /// A database where every forbidden value sits in a column the snapshot
    /// reads near: model ids, key labels, publishers, caller labels, failure
    /// reasons and IPs.
    fn hostile_fixture() -> Connection {
        let db = fixture();
        let rows = [
            (
                "cb/deepseek-v4.1-flash",
                "key-a",
                "CodeBuddy",
                "alice",
                None,
            ),
            ("ali/deepseek-v4.1-flash", "key-b", "Alibaba", "bob", None),
            ("combo/cheapest", "key-a", "OpenCrabs", "alice", None),
            ("plain/deepseek-v4.1-flash", "key-b", "Alibaba", "bob", None),
            (
                "mimo/mimo-v2.6-flash",
                "key-a",
                "qwencloud",
                "alice",
                Some("header_timeout"),
            ),
            (
                "alicn/deepseek-v4.1-flash",
                "key-b",
                "codebuddy",
                "bob",
                Some("upstream_5xx"),
            ),
            (
                "ocg/gemini-3.1-flash-image",
                "key-a",
                "OpenCrabs",
                "alice",
                Some("first_byte_timeout"),
            ),
        ];
        for (model, key, publisher, caller, reason) in rows {
            db.execute(
                "INSERT INTO requests \
                 (ts, ip, endpoint, model_requested, model_used, status, latency_ms, \
                  prompt_tokens, completion_tokens, total_tokens, cached_tokens, \
                  reasoning_tokens, token_label, failure_reason, attempts, plan_len, \
                  served_index, key_label, upstream_publisher, truncated) \
                 VALUES (datetime('now'), '198.51.100.7', '/v1/chat/completions', ?1, ?1, \
                         502, 40000, 1000, 100, 1100, 500, 0, ?2, ?3, 2, 2, 1, ?4, ?5, 0)",
                rusqlite::params![model, caller, reason, key, publisher],
            )
            .unwrap();
        }
        // A caller-facing IP that must not survive either.
        db.execute(
            "INSERT INTO requests (ts, ip, endpoint, status, latency_ms, token_label) \
             VALUES (datetime('now'), '203.0.113.9', '/v1/models', 200, 12, 'alice')",
            [],
        )
        .unwrap();
        db
    }

    #[test]
    fn serialized_json_leaks_no_provider_caller_or_upstream_identity() {
        let json = serde_json::to_string(&build(&hostile_fixture())).unwrap();
        let found = leaks(&json);
        assert!(
            found.is_empty(),
            "public snapshot leaked {found:?} from the fixture: {json}"
        );
        // The rows are still counted, so the test is not passing because the
        // snapshot came out empty.
        let s = build(&hostile_fixture());
        assert_eq!(s.totals.requests, 8);
        // Six chat rows, one image row (`ocg/gemini-3.1-flash-image`), and the
        // `/v1/models` row lands in `other`.
        assert_eq!(s.capabilities[0].requests, 6);
        assert_eq!(s.capabilities[1].requests, 1);
        assert_eq!(s.capabilities[3].requests, 1);
    }

    #[test]
    fn the_leak_scanner_catches_an_injected_provider_name() {
        // A guard nobody has seen fail is a guard nobody knows works. Inject a
        // provider prefix into an otherwise clean snapshot and require the
        // scanner to name it.
        let clean = serde_json::to_string(&build(&four_rows())).unwrap();
        assert!(leaks(&clean).is_empty(), "the clean snapshot is not clean");

        // Anchored on the live schema number rather than a literal: a hardcoded
        // 1 stopped matching the moment the schema was bumped, so the injection
        // silently did nothing and this test proved nothing.
        let anchor = format!("\"schema\":{SCHEMA}");
        assert!(
            clean.contains(&anchor),
            "the serialized snapshot does not carry {anchor}: {clean}"
        );
        let injected = clean.replace(
            &anchor,
            &format!("{anchor},\"model\":\"cb/deepseek-v4.1-flash\""),
        );
        let found = leaks(&injected);
        // Both the provider prefix and the vendor name are caught. Pinning the
        // whole list would make this brittle against a new needle, so only the
        // two the injection carries are required.
        assert!(
            found.contains(&"cb/"),
            "missed the provider prefix: {found:?}"
        );
        assert!(
            found.contains(&"deepseek"),
            "missed the vendor name: {found:?}"
        );
    }

    #[test]
    fn generated_at_is_utc_rfc3339() {
        let s = build(&fixture());
        assert!(
            s.generated_at.ends_with('Z'),
            "expected a UTC timestamp, got {}",
            s.generated_at
        );
        assert!(
            chrono::DateTime::parse_from_rfc3339(&s.generated_at).is_ok(),
            "expected RFC3339, got {}",
            s.generated_at
        );
    }
}
