//! The virtual-pathway span allowlist and its aggregation into [`Counters`].
//!
//! Every name here was verified against a `debug_span!`/`instrument` callsite in
//! the engine (file cited below, re-checked at test time by
//! `allowlist_literals_exist_at_engine_callsites` so a silent rename can't zero
//! a counter); they are stable string literals, so the allowlist is safe to pin
//! as the schema of the pathway counters:
//!
//! | span | site | numeric fields |
//! |---|---|---|
//! | `r2rml.scan_table`   | `fluree-db-api/src/graph_source/r2rml.rs` (scan setup: loadTable + planning) | `projection_len` |
//! | `r2rml.count_manifest` | `fluree-db-api/src/graph_source/r2rml.rs` (COUNT(*) manifest-only read; `fired` = answered vs declined-to-scan) | — |
//! | `r2rml.load_table`   | `fluree-db-api/src/graph_source/r2rml.rs` (cold REST/OAuth catalog load) | — |
//! | `iceberg.scan_plan`  | `fluree-db-iceberg/src/scan/send_planner.rs` (manifest read + file pruning) | `files_selected`, `files_pruned`, `estimated_row_count` |
//! | `iceberg.parquet_read` | `fluree-db-api/src/graph_source/r2rml.rs` (per-file decode, in spawned tasks) | `file_size` |
//! | `iceberg.oauth_token`  | `fluree-db-iceberg/src/auth/oauth2.rs` (OAuth token mint) | — |
//! | `iceberg.read_footer` / `iceberg.plan_columns` / `iceberg.fetch_bytes` / `iceberg.decode` | `fluree-db-iceberg/src/io/send_parquet.rs` (per-file cost decomposition inside `iceberg.parquet_read`) | — |
//!
//! Engine-stage spans in `fluree-db-query` (`operator_open`, `reasoning_prep`)
//! were considered but left out: they are generic to every query (native and
//! virtual alike) and add no native-vs-virtual signal, and there is no stable
//! `query_run`/`query_plan` span literal to cite. Keeping the allowlist to
//! virtual-only spans keeps the counter schema minimal and stable.

use fluree_bench_support::tracing::SpanRecord;

use crate::schema::{Counters, SpanAgg};

/// Spans captured for the pathway counters. Passed to `BenchSpanCapture::layer`
/// and `BenchSpanLayer::filter` so nothing else is captured.
pub const SPAN_ALLOWLIST: &[&str] = &[
    "r2rml.scan_table",
    "r2rml.count_manifest",
    "r2rml.load_table",
    "iceberg.parquet_read",
    "iceberg.scan_plan",
    "iceberg.oauth_token",
    // PR-2 phase-1 (measurement-only): per-file cost decomposition sub-spans
    // nested inside `iceberg.parquet_read` (small-file path,
    // `send_parquet.rs::read_task_small_file`). `mean = total_us / n` per name
    // splits the ~200ms per-file wall into footer / plan / fetch / decode; the
    // residual `parquet_read - sum(children)` is spawn+channel scheduling.
    "iceberg.read_footer",
    "iceberg.plan_columns",
    "iceberg.fetch_bytes",
    "iceberg.decode",
];

/// Span groups where AT LEAST ONE member MUST fire on any non-trivial virtual
/// query. A virtual execution where a whole group is absent either didn't hit
/// the R2RML engine or ran with tracing mis-installed — that's what
/// `spans_missing` flags (as the group's names joined with `|`).
///
/// A group (rather than a single must-fire span) because a bare `COUNT(*)`
/// answered by the manifest shortcut legitimately never scans: it emits
/// `r2rml.count_manifest` and NO `r2rml.scan_table`. Either span proves the
/// R2RML engine ran.
///
/// Deliberately excludes the data-/cache-/plan-dependent spans:
/// - `iceberg.scan_plan` — fires only when the planner takes the
///   pruning/pushdown branch (2 of 16 smoke queries; finding F7 in
///   `04-findings-register`), so treating it as must-fire false-flagged most
///   virtual queries. Re-add if the engine ever emits it unconditionally
///   (with `files_pruned=0`), per the ROADMAP harness follow-up.
/// - `iceberg.parquet_read` — a metadata-only COUNT can answer from row-count
///   stats without reading any Parquet.
/// - `r2rml.load_table` / `iceberg.oauth_token` — fire only on a cold catalog /
///   OAuth miss; a warm cross-query cache skips them.
pub const EXPECTED_ANY_FOR_VIRTUAL: &[&[&str]] = &[&["r2rml.scan_table", "r2rml.count_manifest"]];

/// Fold a rep's captured spans into per-span timing aggregates plus the summed
/// numeric fields the Iceberg planner/reader record.
pub fn aggregate(records: &[SpanRecord]) -> Counters {
    let mut counters = Counters::default();
    for record in records {
        if !SPAN_ALLOWLIST.contains(&record.name) {
            continue;
        }
        let agg = counters
            .spans
            .entry(record.name.to_string())
            .or_insert_with(SpanAgg::default);
        agg.n += 1;
        agg.total_us += record.elapsed_us;
        agg.max_us = agg.max_us.max(record.elapsed_us);

        counters.files_selected += field_u64(record, "files_selected");
        counters.files_pruned += field_u64(record, "files_pruned");
        counters.estimated_row_count += field_u64(record, "estimated_row_count");
        counters.file_size += field_u64(record, "file_size");
    }
    counters
}

/// Read a numeric span field as a non-negative `u64` (0 if absent/negative).
fn field_u64(record: &SpanRecord, key: &str) -> u64 {
    record
        .fields
        .get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
        .map(|n| u64::try_from(n).unwrap_or(0))
        .unwrap_or(0)
}

/// Expected-for-virtual span groups where no member fired, each rendered as
/// the group's names joined with `|`. Empty for native targets.
pub fn spans_missing(counters: &Counters, is_virtual: bool) -> Vec<String> {
    if !is_virtual {
        return Vec::new();
    }
    EXPECTED_ANY_FOR_VIRTUAL
        .iter()
        .filter(|group| {
            group
                .iter()
                .all(|name| counters.spans.get(*name).is_none_or(|a| a.n == 0))
        })
        .map(|group| group.join("|"))
        .collect()
}

/// Convenience accessor: number of spans of `name` seen (0 if none).
pub fn span_count(counters: &Counters, name: &str) -> u64 {
    counters.spans.get(name).map_or(0, |a| a.n)
}

/// Convenience accessor: total microseconds spent in spans of `name`.
pub fn span_total_us(counters: &Counters, name: &str) -> u64 {
    counters.spans.get(name).map_or(0, |a| a.total_us)
}

/// Aggregation stats keyed on nothing but the sink layout — no runtime span
/// capture, so it's a plain fixture test.
#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use serde_json::{Map, Number, Value};

    fn rec(name: &'static str, elapsed_us: u64, fields: &[(&str, i64)]) -> SpanRecord {
        let mut map = Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), Value::Number(Number::from(*v)));
        }
        SpanRecord {
            name,
            parent: None,
            elapsed_us,
            fields: map,
        }
    }

    #[test]
    fn aggregates_counts_timing_and_numeric_fields() {
        let records = vec![
            rec("r2rml.scan_table", 1000, &[]),
            rec(
                "iceberg.scan_plan",
                500,
                &[
                    ("files_selected", 3),
                    ("files_pruned", 7),
                    ("estimated_row_count", 1200),
                ],
            ),
            rec("iceberg.parquet_read", 800, &[("file_size", 4096)]),
            rec("iceberg.parquet_read", 900, &[("file_size", 8192)]),
            // Not on the allowlist — ignored.
            rec("some.other.span", 5, &[]),
        ];
        let c = aggregate(&records);
        assert_eq!(span_count(&c, "iceberg.parquet_read"), 2);
        assert_eq!(span_total_us(&c, "iceberg.parquet_read"), 1700);
        assert_eq!(c.spans.get("iceberg.parquet_read").unwrap().max_us, 900);
        assert_eq!(c.files_selected, 3);
        assert_eq!(c.files_pruned, 7);
        assert_eq!(c.estimated_row_count, 1200);
        assert_eq!(c.file_size, 12288);
        assert!(!c.spans.contains_key("some.other.span"));
    }

    #[test]
    fn spans_missing_flags_absent_expected_spans_for_virtual_only() {
        // Nothing fired: the must-fire group is flagged (joined with `|`).
        let c = aggregate(&[]);
        assert_eq!(
            spans_missing(&c, true),
            vec!["r2rml.scan_table|r2rml.count_manifest".to_string()]
        );
        // Native target: never flagged.
        assert!(spans_missing(&c, false).is_empty());
        // scan_table fired: nothing missing — in particular the conditional
        // pruning-branch span (iceberg.scan_plan) must NOT be false-flagged on
        // a query whose plan never takes the pushdown branch (finding F7).
        let fired = aggregate(&[rec("r2rml.scan_table", 10, &[])]);
        assert!(spans_missing(&fired, true).is_empty());
        // A COUNT(*) answered by the manifest shortcut never scans: the
        // count_manifest span alone must satisfy the group (no false flag on
        // the very pathway the shortcut creates).
        let counted = aggregate(&[rec("r2rml.count_manifest", 10, &[])]);
        assert!(spans_missing(&counted, true).is_empty());
    }

    /// Rename guard: every allowlisted span literal must still exist at its
    /// cited engine callsite. A renamed engine span would otherwise silently
    /// zero its counter (the layer filter just stops matching). Reads the
    /// workspace sources relative to this crate, so it runs anywhere the repo
    /// checkout does.
    #[test]
    fn allowlist_literals_exist_at_engine_callsites() {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let sites: &[(&str, &str)] = &[
            (
                "r2rml.scan_table",
                "fluree-db-api/src/graph_source/r2rml.rs",
            ),
            (
                "r2rml.count_manifest",
                "fluree-db-api/src/graph_source/r2rml.rs",
            ),
            (
                "r2rml.load_table",
                "fluree-db-api/src/graph_source/r2rml.rs",
            ),
            (
                "iceberg.parquet_read",
                "fluree-db-api/src/graph_source/r2rml.rs",
            ),
            (
                "iceberg.scan_plan",
                "fluree-db-iceberg/src/scan/send_planner.rs",
            ),
            (
                "iceberg.oauth_token",
                "fluree-db-iceberg/src/auth/oauth2.rs",
            ),
            (
                "iceberg.read_footer",
                "fluree-db-iceberg/src/io/send_parquet.rs",
            ),
            (
                "iceberg.plan_columns",
                "fluree-db-iceberg/src/io/send_parquet.rs",
            ),
            (
                "iceberg.fetch_bytes",
                "fluree-db-iceberg/src/io/send_parquet.rs",
            ),
            ("iceberg.decode", "fluree-db-iceberg/src/io/send_parquet.rs"),
        ];
        // The table above must stay in lockstep with SPAN_ALLOWLIST.
        for name in SPAN_ALLOWLIST {
            assert!(
                sites.iter().any(|(n, _)| n == name),
                "span '{name}' is allowlisted but has no cited callsite in this test"
            );
        }
        for (name, file) in sites {
            let path = workspace.join(file);
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("reading engine source {}: {e}", path.display()));
            assert!(
                source.contains(&format!("\"{name}\"")),
                "span literal \"{name}\" not found in {file} — if the engine span was \
                 renamed or moved, update SPAN_ALLOWLIST and this table so the \
                 pathway counters keep measuring what they claim to"
            );
        }
    }
}
