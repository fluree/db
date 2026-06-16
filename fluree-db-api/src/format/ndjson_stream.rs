//! NDJSON record protocol for the streaming query endpoint.
//!
//! The streaming endpoint emits newline-delimited JSON — one self-describing
//! record per line — so a client can consume results incrementally and a
//! long-running query can keep bytes flowing past proxy idle timeouts.
//!
//! Record types (the `type` field discriminates):
//! - `{"type":"head","vars":[...]}` — emitted first, before any row pull, so the
//!   client learns the column order immediately and the connection's idle clock
//!   starts fresh.
//! - `{"type":"row","row":{...}}` — one per result row. The `row` body is written
//!   by the per-format row emitters (e.g. [`super::sparql::stream_ndjson_rows`]),
//!   not here.
//! - `{"type":"heartbeat","t_ms":N,"fuel":F?}` — emitted on a wall-clock timer
//!   when no row has flowed recently; carries elapsed time and (when fuel is
//!   tracked) the live running fuel total as a progress signal.
//! - `{"type":"end","rows":N,...}` — success terminator.
//! - `{"type":"error","error":{...},"rows":N}` — failure terminator.
//!
//! Exactly one terminal record (`end` or `error`) is emitted, and never a row
//! after it. The client treats the *absence* of any terminal record as a
//! truncated/dropped stream (failure) — this is the only way to distinguish a
//! server-side completion from a connection that died mid-stream after the
//! `200 OK` was already committed.

use serde_json::json;

/// Content type for NDJSON streaming query responses.
pub const NDJSON_CONTENT_TYPE: &str = "application/x-ndjson";

/// `head` record: the ordered output column names. Written once, before the
/// first row is pulled, to flush an immediate first byte.
pub fn head_record(vars: &[String]) -> String {
    let mut line = json!({ "type": "head", "vars": vars }).to_string();
    line.push('\n');
    line
}

/// `heartbeat` record: keeps the connection alive during long stalls (e.g. a
/// blocking ORDER BY/GROUP BY drain). `fuel` is omitted when fuel tracking is
/// disabled; when present it climbs as scans charge, signalling progress even
/// though no rows have been emitted yet.
pub fn heartbeat_record(elapsed_ms: u64, fuel: Option<f64>) -> String {
    let mut value = json!({ "type": "heartbeat", "t_ms": elapsed_ms });
    if let Some(fuel) = fuel {
        value["fuel"] = json!(fuel);
    }
    let mut line = value.to_string();
    line.push('\n');
    line
}

/// `end` record: success terminator carrying the final row count and, when
/// tracked, the same `t`/`fuel`/`time` metadata the buffered endpoint reports.
pub fn end_record(rows: u64, t: Option<i64>, fuel: Option<f64>, time: Option<&str>) -> String {
    let mut value = json!({ "type": "end", "rows": rows });
    if let Some(t) = t {
        value["t"] = json!(t);
    }
    if let Some(fuel) = fuel {
        value["fuel"] = json!(fuel);
    }
    if let Some(time) = time {
        value["time"] = json!(time);
    }
    let mut line = value.to_string();
    line.push('\n');
    line
}

/// `error` record: failure terminator. Carries the rows already emitted so the
/// client knows how far the (now-aborted) stream got. Emitted *instead of*
/// `end` — never both, never a row after it.
pub fn error_record(code: &str, message: &str, rows: u64) -> String {
    let mut line = json!({
        "type": "error",
        "error": { "code": code, "message": message },
        "rows": rows,
    })
    .to_string();
    line.push('\n');
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn parse(line: &str) -> Value {
        assert!(line.ends_with('\n'), "record must be newline-terminated");
        assert_eq!(line.matches('\n').count(), 1, "record must be a single line");
        serde_json::from_str(line.trim_end()).expect("record must be valid JSON")
    }

    #[test]
    fn head_carries_var_order() {
        let v = parse(&head_record(&["s".into(), "p".into(), "o".into()]));
        assert_eq!(v["type"], "head");
        assert_eq!(v["vars"], json!(["s", "p", "o"]));
    }

    #[test]
    fn heartbeat_omits_fuel_when_untracked() {
        let v = parse(&heartbeat_record(14982, None));
        assert_eq!(v["type"], "heartbeat");
        assert_eq!(v["t_ms"], 14982);
        assert!(v.get("fuel").is_none());

        let v = parse(&heartbeat_record(14982, Some(84.213)));
        assert_eq!(v["fuel"], 84.213);
    }

    #[test]
    fn end_includes_only_tracked_fields() {
        let v = parse(&end_record(50213, None, None, None));
        assert_eq!(v["type"], "end");
        assert_eq!(v["rows"], 50213);
        assert!(v.get("t").is_none());
        assert!(v.get("fuel").is_none());

        let v = parse(&end_record(3, Some(42), Some(1.01), Some("12.34ms")));
        assert_eq!(v["t"], 42);
        assert_eq!(v["fuel"], 1.01);
        assert_eq!(v["time"], "12.34ms");
    }

    #[test]
    fn error_reports_code_and_partial_rows() {
        let v = parse(&error_record("fuel_exhausted", "fuel limit exceeded", 50213));
        assert_eq!(v["type"], "error");
        assert_eq!(v["error"]["code"], "fuel_exhausted");
        assert_eq!(v["error"]["message"], "fuel limit exceeded");
        assert_eq!(v["rows"], 50213);
    }
}
