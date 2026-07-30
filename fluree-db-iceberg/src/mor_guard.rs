//! Fail-closed guard for Iceberg merge-on-read (MoR) delete files.
//!
//! Fluree's virtual Iceberg read path *recognizes* position/equality delete
//! files (content=1 manifests) but does **not** apply them: it drops the delete
//! manifests and reads only the live data files (see
//! [`crate::scan::planner`] / [`crate::stats`]). On a table maintained with
//! merge-on-read semantics that silently returns deleted rows as if live and
//! over-counts `COUNT(*)` / row totals by the delete cardinality — a silent
//! wrong answer, the worst failure class for a database. Copy-on-write deletes
//! (which rewrite data files and mark old manifest entries `DELETED`) are
//! handled correctly and are unaffected by this guard.
//!
//! Until delete application is implemented, this module **fails closed**: any
//! read whose snapshot carries delete files is refused with an actionable typed
//! error ([`IcebergError::MergeOnReadDeletes`]). Two independent signals are
//! checked so a snapshot cannot slip through:
//!
//! 1. the snapshot summary counters (`total-delete-files` /
//!    `total-position-deletes` / `total-equality-deletes`) — a zero-I/O check,
//!    the summary is already in memory ([`ensure_no_summary_deletes`]); and
//! 2. the manifest list — a delete manifest (`content=1`) present even when the
//!    summary omits/under-counts it ([`ensure_no_delete_manifests`]).
//!
//! The refusal is disabled by the [`ALLOW_MOR_DELETES_ENV`] escape hatch
//! (default off). When set truthy the old skip-and-proceed behavior is restored
//! and a once-per-table warning is logged that results may include deleted rows.
//!
//! See audit finding F-AUD-1 (`audit-2026-07/00-MASTER-AUDIT.md`,
//! `V1-mor-verification.md`) for the verified gap and exposure bounding.

use std::collections::HashSet;
use std::sync::Mutex;

use crate::error::{IcebergError, Result};
use crate::manifest::ManifestListEntry;
use crate::metadata::Snapshot;

/// Environment switch that disables the fail-closed MoR delete guard.
///
/// Default (unset or falsy): the guard is **active** — reads over snapshots that
/// carry delete files are refused. Truthy (`1` / `true` / `yes` / `on`,
/// case-insensitive): the guard is disabled, the old behavior (delete files
/// ignored) is restored, and a once-per-table warning is logged.
pub const ALLOW_MOR_DELETES_ENV: &str = "FLUREE_ICEBERG_ALLOW_MOR_DELETES";

/// Whether the operator has opted OUT of the fail-closed guard via
/// [`ALLOW_MOR_DELETES_ENV`]. Read fresh on each call — this is only consulted
/// on the (cold) metadata/scan-plan path, never per row.
pub fn mor_deletes_allowed() -> bool {
    env_truthy(std::env::var(ALLOW_MOR_DELETES_ENV).ok().as_deref())
}

/// Truthy semantics for the opt-out switch, matching the repo's `env_flag`
/// default-off flags (`1`/`true`/`yes`/`on`, case-insensitive; anything else,
/// including absent, is off). Split out from [`mor_deletes_allowed`] so it can be
/// unit-tested without mutating the shared process environment.
fn env_truthy(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

/// The three snapshot-summary counters that report merge-on-read delete files.
/// The Iceberg spec types `summary` as `map<string,string>` with these as
/// string-encoded integers, so a conforming writer never emits a non-integer.
const SUMMARY_DELETE_KEYS: [&str; 3] = [
    "total-delete-files",
    "total-position-deletes",
    "total-equality-deletes",
];

/// Scan the summary delete counters with **fail-closed** semantics, returning a
/// human-readable evidence fragment for every counter that indicates — or might
/// indicate — deletes. A counter contributes evidence when it is *present* and
/// either fails to parse as `i64` or parses to a non-zero value (a negative is
/// treated as deletes-present, not clamped to zero). An *absent* counter
/// contributes nothing: the manifest-list check ([`ensure_no_delete_manifests`])
/// is the backstop for writers that omit the counters. An empty result means the
/// summary is clean.
///
/// The distinction between "absent" and "present-but-unparseable" has to be drawn
/// here, on the raw `summary` map: the typed accessors on [`Snapshot`] collapse
/// both to `None`, which would let a garbled counter read as zero (fail-open) —
/// the exact hole this guard closes.
fn summary_delete_evidence(snapshot: &Snapshot) -> Vec<String> {
    let mut evidence = Vec::new();
    for key in SUMMARY_DELETE_KEYS {
        let Some(raw) = snapshot.summary.get(key) else {
            continue; // absent → manifest-list backstop covers it
        };
        match raw.parse::<i64>() {
            Ok(0) => {}
            Ok(n) if n < 0 => evidence.push(format!("{key}={n} (negative)")),
            Ok(n) => evidence.push(format!("{key}={n}")),
            Err(_) => evidence.push(format!("{key}={raw:?} (unparseable)")),
        }
    }
    evidence
}

/// Whether the snapshot summary indicates (or, fail-closed, might indicate)
/// merge-on-read delete files: any present counter that is non-zero, negative, or
/// unparseable. Used to caveat a read that proceeds under the override
/// ([`ALLOW_MOR_DELETES_ENV`]) — its row/COUNT totals are then upper bounds.
pub fn summary_indicates_deletes(snapshot: &Snapshot) -> bool {
    !summary_delete_evidence(snapshot).is_empty()
}

/// Fail closed if the snapshot **summary** reports any merge-on-read delete
/// files. Zero extra I/O: `snapshot.summary` is already loaded. A *present*
/// counter that is non-zero, negative, or unparseable refuses; only an *absent*
/// counter proceeds (the manifest-list check in [`ensure_no_delete_manifests`] is
/// the backstop for writers that omit the counters). Failing closed on a
/// malformed value has ~zero false-positive risk against a spec-conforming writer
/// (the counters are always string-encoded integers).
///
/// `allowed` is the operator opt-out ([`mor_deletes_allowed`]); pass it in so
/// callers read the env once and this stays pure/testable.
pub fn ensure_no_summary_deletes(snapshot: &Snapshot, table: &str, allowed: bool) -> Result<()> {
    let evidence = summary_delete_evidence(snapshot);
    if evidence.is_empty() {
        return Ok(());
    }
    decide(
        table,
        &format!(
            "snapshot summary reports merge-on-read delete files ({})",
            evidence.join(", ")
        ),
        allowed,
    )
}

/// Fail closed if the manifest list carried any delete manifests (`content=1`).
/// This is the backstop for snapshots whose summary omits or under-counts the
/// delete counters — the manifest list is read on the scan/stats path anyway, so
/// counting `is_deletes()` entries is free.
///
/// `delete_manifests` is the number of delete manifests observed (`0` proceeds).
pub fn ensure_no_delete_manifests(
    delete_manifests: usize,
    table: &str,
    allowed: bool,
) -> Result<()> {
    if delete_manifests == 0 {
        return Ok(());
    }
    decide(
        table,
        &format!("manifest list contains {delete_manifests} merge-on-read delete manifest(s)"),
        allowed,
    )
}

/// Read the override switch once and apply the zero-I/O summary-counter guard
/// (before the manifest-list read). Returns the override flag to thread into the
/// post-read [`ensure_manifests_scannable`] check. Both scan planners call this
/// pair so the fail-closed guard cannot drift between them (audit F-AUD-1).
pub fn ensure_summary_scannable(snapshot: &Snapshot, table: &str) -> Result<bool> {
    let allowed = mor_deletes_allowed();
    ensure_no_summary_deletes(snapshot, table, allowed)?;
    Ok(allowed)
}

/// Belt-and-suspenders guard over an already-parsed manifest list: refuse if any
/// `content=1` delete manifest is present (covers a snapshot whose summary omits
/// the counters). `allowed` is the override returned by [`ensure_summary_scannable`].
/// Shared by both scan planners so the count-and-check cannot drift.
pub fn ensure_manifests_scannable(
    entries: &[ManifestListEntry],
    table: &str,
    allowed: bool,
) -> Result<()> {
    let delete_manifests = entries.iter().filter(|e| e.is_deletes()).count();
    ensure_no_delete_manifests(delete_manifests, table, allowed)
}

/// Shared decision: refuse (fail-closed) unless the operator opted out, in which
/// case warn once per table and proceed.
fn decide(table: &str, evidence: &str, allowed: bool) -> Result<()> {
    if allowed {
        warn_once(table);
        return Ok(());
    }
    Err(IcebergError::MergeOnReadDeletes(format!(
        "Refusing to read Iceberg table `{table}`: {evidence}. Merge-on-read \
         delete files are recognized but NOT applied yet, so query results would \
         include deleted rows and COUNT/row totals would be over-counted. Set \
         {ALLOW_MOR_DELETES_ENV}=1 to read anyway (results may include deleted rows)."
    )))
}

/// Emit the "deletes ignored" warning at most once per table for the life of the
/// process, so the opt-out does not spam the log on every query.
fn warn_once(table: &str) {
    static WARNED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
    let mut guard = WARNED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let seen = guard.get_or_insert_with(HashSet::new);
    if seen.insert(table.to_string()) {
        tracing::warn!(
            table = %table,
            env = %ALLOW_MOR_DELETES_ENV,
            "Iceberg merge-on-read delete files are being IGNORED (guard disabled via \
             {ALLOW_MOR_DELETES_ENV}); query results may include deleted rows and row \
             counts may be over-counted"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn snapshot_with(summary: &[(&str, &str)]) -> Snapshot {
        Snapshot {
            snapshot_id: 1,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1000,
            manifest_list: Some("s3://b/t/metadata/snap.avro".to_string()),
            manifests: None,
            summary: summary
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<HashMap<_, _>>(),
            schema_id: Some(0),
        }
    }

    #[test]
    fn summary_position_deletes_refused_when_guard_active() {
        // total-position-deletes > 0 → refuse (guard active, allowed=false).
        let snap = snapshot_with(&[
            ("total-delete-files", "1"),
            ("total-position-deletes", "42"),
            ("total-equality-deletes", "0"),
        ]);
        let err = ensure_no_summary_deletes(&snap, "dw.fact_order_line", false).unwrap_err();
        assert!(matches!(err, IcebergError::MergeOnReadDeletes(_)));
    }

    #[test]
    fn summary_equality_deletes_refused_when_guard_active() {
        // Equality deletes alone (CDC/Flink shape) must also refuse.
        let snap = snapshot_with(&[("total-equality-deletes", "3")]);
        assert!(ensure_no_summary_deletes(&snap, "t", false).is_err());
    }

    #[test]
    fn summary_zero_or_absent_proceeds() {
        // All-zero counters proceed (the deployed Snowflake v2 append shape).
        let zeroed = snapshot_with(&[
            ("total-delete-files", "0"),
            ("total-position-deletes", "0"),
            ("total-equality-deletes", "0"),
        ]);
        assert!(ensure_no_summary_deletes(&zeroed, "t", false).is_ok());
        // Absent counters (summary omits them) also proceed at this check — the
        // manifest-list backstop covers the omit case.
        let absent = snapshot_with(&[("total-records", "1000"), ("operation", "append")]);
        assert!(ensure_no_summary_deletes(&absent, "t", false).is_ok());
    }

    #[test]
    fn summary_deletes_allowed_under_override() {
        let snap = snapshot_with(&[("total-position-deletes", "5")]);
        assert!(
            ensure_no_summary_deletes(&snap, "t", true).is_ok(),
            "override must let the read proceed"
        );
    }

    #[test]
    fn delete_manifests_refused_then_allowed_under_override() {
        assert!(ensure_no_delete_manifests(0, "t", false).is_ok());
        assert!(ensure_no_delete_manifests(2, "t", false).is_err());
        assert!(ensure_no_delete_manifests(2, "t", true).is_ok());
    }

    #[test]
    fn summary_scannable_returns_override_and_refuses_deletes() {
        // The shared planner helper: a delete-free snapshot proceeds and reports
        // the override flag (false in the test env); a delete-bearing one refuses.
        let clean = snapshot_with(&[("total-records", "10")]);
        assert!(
            !ensure_summary_scannable(&clean, "t").unwrap(),
            "override is off in the test env"
        );
        let dirty = snapshot_with(&[("total-position-deletes", "1")]);
        assert!(matches!(
            ensure_summary_scannable(&dirty, "t"),
            Err(IcebergError::MergeOnReadDeletes(_))
        ));
    }

    #[test]
    fn manifests_scannable_refuses_a_delete_manifest() {
        use crate::manifest::ManifestContent;
        fn entry(content: ManifestContent) -> ManifestListEntry {
            ManifestListEntry {
                manifest_path: "m.avro".to_string(),
                manifest_length: 0,
                partition_spec_id: 0,
                content,
                sequence_number: 0,
                min_sequence_number: 0,
                added_snapshot_id: 0,
                added_data_files_count: 0,
                existing_data_files_count: 0,
                deleted_data_files_count: 0,
                added_rows_count: 0,
                existing_rows_count: 0,
                deleted_rows_count: 0,
                partitions: vec![],
            }
        }
        // Only data manifests → proceeds.
        assert!(ensure_manifests_scannable(&[entry(ManifestContent::Data)], "t", false).is_ok());
        // A content=1 delete manifest → refuse (guard active); proceed under override.
        let with_delete = [
            entry(ManifestContent::Data),
            entry(ManifestContent::Deletes),
        ];
        assert!(ensure_manifests_scannable(&with_delete, "t", false).is_err());
        assert!(ensure_manifests_scannable(&with_delete, "t", true).is_ok());
    }

    #[test]
    fn summary_unparseable_counter_refuses() {
        // W1: a present-but-unparseable counter must FAIL CLOSED (read as
        // deletes-present), not silently read as zero via `.parse().ok()`.
        let snap = snapshot_with(&[("total-delete-files", "garbage")]);
        let err = ensure_no_summary_deletes(&snap, "t", false).unwrap_err();
        assert!(matches!(err, IcebergError::MergeOnReadDeletes(_)));
        assert!(
            err.to_string().contains("garbage"),
            "evidence names the malformed value: {err}"
        );
    }

    #[test]
    fn summary_negative_counter_refuses() {
        // W1: a negative counter is nonsensical for a count; fail closed rather
        // than clamp-to-zero-and-proceed.
        let snap = snapshot_with(&[("total-position-deletes", "-1")]);
        assert!(ensure_no_summary_deletes(&snap, "t", false).is_err());
    }

    #[test]
    fn summary_absent_counters_proceed() {
        // W1: absent counters still proceed here — the manifest-list backstop
        // covers omission — and are not flagged by `summary_indicates_deletes`.
        let snap = snapshot_with(&[("total-records", "1000"), ("operation", "append")]);
        assert!(ensure_no_summary_deletes(&snap, "t", false).is_ok());
        assert!(!summary_indicates_deletes(&snap));
    }

    #[test]
    fn summary_indicates_deletes_classifies_counters() {
        // Non-zero / unparseable ⇒ deletes; zero / absent ⇒ clean.
        assert!(summary_indicates_deletes(&snapshot_with(&[(
            "total-delete-files",
            "1"
        )])));
        assert!(summary_indicates_deletes(&snapshot_with(&[(
            "total-equality-deletes",
            "garbage"
        )])));
        assert!(!summary_indicates_deletes(&snapshot_with(&[(
            "total-delete-files",
            "0"
        )])));
        assert!(!summary_indicates_deletes(&snapshot_with(&[(
            "total-records",
            "5"
        )])));
    }

    #[test]
    fn error_message_is_actionable() {
        // W2: the refusal must name the identifier the REAL call sites pass, the
        // override switch, and the risk. The scan planners pass `metadata.location`
        // (`scan/planner.rs`, `scan/send_planner.rs`); the preview passes
        // "qualified (location)" (`fluree-db-api` `iceberg_catalog.rs`). Assert
        // both shapes so the test tracks what the sites actually emit, not a bare
        // literal no site produces.
        let snap = snapshot_with(&[("total-delete-files", "9")]);

        // Planner shape: storage location only.
        let location = "s3://bucket/dw/dim_customer";
        let msg = ensure_no_summary_deletes(&snap, location, false)
            .unwrap_err()
            .to_string();
        assert!(msg.contains(location), "names the storage location: {msg}");
        assert!(
            msg.contains(ALLOW_MOR_DELETES_ENV),
            "names the override switch: {msg}"
        );
        assert!(msg.contains("deleted rows"), "explains the risk: {msg}");

        // Preview shape: catalog-qualified name AND storage location, so an
        // operator grepping logs from either path finds a common identifier.
        let preview_id = format!("dw.dim_customer ({location})");
        let msg = ensure_no_summary_deletes(&snap, &preview_id, false)
            .unwrap_err()
            .to_string();
        assert!(
            msg.contains("dw.dim_customer"),
            "names the qualified table: {msg}"
        );
        assert!(msg.contains(location), "names the storage location: {msg}");
    }

    #[test]
    fn env_override_parsing() {
        // Pure parsing test — never touches the shared process env, so it can
        // never race the fixture tests that call `mor_deletes_allowed()`.
        assert!(!env_truthy(None), "absent defaults to guard-active");
        for truthy in ["1", "true", "TRUE", "yes", "On", " on "] {
            assert!(
                env_truthy(Some(truthy)),
                "{truthy:?} should disable the guard"
            );
        }
        for falsy in ["0", "false", "off", "no", "", "maybe"] {
            assert!(
                !env_truthy(Some(falsy)),
                "{falsy:?} should keep the guard active"
            );
        }
    }
}
