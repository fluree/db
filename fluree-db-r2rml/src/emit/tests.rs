//! Tests for the deterministic emitter.
//!
//! Unit tests (subject-key selection, FK heuristic cases, determinism, wire
//! shape) run without the `turtle` feature. The round-trip, enterprise
//! structural-match, and hex-guard tests are gated on `turtle` because they
//! compile the emitted Turtle back through `R2rmlLoader`.

#![cfg(test)]

use std::collections::{BTreeMap, BTreeSet};

use fluree_db_tabular::FieldType;

use crate::emit::diagnostic::DiagCode;
use crate::emit::fixtures::enterprise_dw_tables;
use crate::emit::input::{EmitColumn, EmitColumnStats, EmitTableSchema, TypedBound};
use crate::emit::{emit_r2rml, EmitOptions, EmitOutput, SubjectStrategy, TableKey, TableOverride};

// =============================================================================
// Synthetic-input helpers (for focused FK-heuristic unit tests)
// =============================================================================

/// An integer key column with an explicit `[min, max]` range.
fn ik(field_id: i32, name: &str, min: i64, max: i64, required: bool) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: "long".to_string(),
        field_type: FieldType::Int64,
        required,
        nested: false,
        doc: None,
        stats: EmitColumnStats {
            null_fraction: if required { Some(0.0) } else { None },
            min: Some(TypedBound::Int(min)),
            max: Some(TypedBound::Int(max)),
        },
    }
}

/// A bounds-free scalar column.
fn sc(field_id: i32, name: &str, ft: FieldType) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: "x".to_string(),
        field_type: ft,
        required: false,
        nested: false,
        doc: None,
        stats: EmitColumnStats::default(),
    }
}

/// The scale-0 decimal type of a Snowflake `NUMBER(38,0)` surrogate key — the
/// live shape that arrives as Iceberg `decimal(38,0)`, NOT `long`.
const DEC38_0: FieldType = FieldType::Decimal {
    precision: 38,
    scale: 0,
};

/// A `decimal(38,0)` key column with an explicit `[min, max]` range.
fn dk(field_id: i32, name: &str, min: i64, max: i64, required: bool) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: "decimal(38,0)".to_string(),
        field_type: DEC38_0,
        required,
        nested: false,
        doc: None,
        stats: EmitColumnStats {
            null_fraction: if required { Some(0.0) } else { None },
            min: Some(TypedBound::Int(min)),
            max: Some(TypedBound::Int(max)),
        },
    }
}

/// A `decimal(38,0)` key column with NO min/max bounds — the common Snowflake
/// case where the manifest supplies no integer bounds. `required` still drives
/// null-freeness (a `required` no-bounds key is a valid, indexable subject PK).
fn dk_nobounds(field_id: i32, name: &str, required: bool) -> EmitColumn {
    EmitColumn {
        field_id,
        name: name.to_string(),
        iceberg_type: "decimal(38,0)".to_string(),
        field_type: DEC38_0,
        required,
        nested: false,
        doc: None,
        stats: EmitColumnStats {
            null_fraction: if required { Some(0.0) } else { None },
            min: None,
            max: None,
        },
    }
}

/// `EmitOptions::default()` with the strict `Identifier` subject strategy (the
/// pre-change subject-key behavior).
fn strict_opts() -> EmitOptions {
    EmitOptions {
        subject_strategy: SubjectStrategy::Identifier,
        ..EmitOptions::default()
    }
}

fn tbl(name: &str, identifier_field_ids: Vec<i32>, columns: Vec<EmitColumn>) -> EmitTableSchema {
    EmitTableSchema {
        namespace: "DW".to_string(),
        name: name.to_string(),
        columns,
        identifier_field_ids,
    }
}

/// The resolved FK edges in an emit output, as `(childTable, child, parentTable, parent)`.
fn resolved_fks(out: &EmitOutput) -> BTreeSet<(String, String, String, String)> {
    let mut set = BTreeSet::new();
    for tm in &out.structured.table_mappings {
        for col in &tm.columns {
            if let Some(fk) = &col.foreign_key {
                set.insert((
                    tm.table_name.clone(),
                    fk.child_column.clone(),
                    fk.target_table.clone(),
                    fk.parent_column.clone(),
                ));
            }
        }
    }
    set
}

/// The `(table, column)` pairs carrying a given diagnostic code.
fn diag_cols(out: &EmitOutput, code: DiagCode) -> BTreeSet<(String, String)> {
    out.diagnostics
        .iter()
        .filter(|d| d.code == code)
        .filter_map(|d| Some((d.table.clone()?, d.column.clone()?)))
        .collect()
}

// =============================================================================
// Subject-key selection
// =============================================================================

#[test]
fn subject_key_from_identifier_field_ids() {
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            sc(2, "NAME", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
    let tm = &out.structured.table_mappings[0];
    assert!(
        tm.subject_template.ends_with("/{WIDGET_KEY}"),
        "{}",
        tm.subject_template
    );
    // A clean identifier hint has no NoSafeSubjectKey, but — since uniqueness is
    // unverifiable metadata-only — it earns a SubjectKeyUnverified on the key.
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())])
    );
    // The subject-key column is retained as a literal marked isSubjectId.
    let pk = tm
        .columns
        .iter()
        .find(|c| c.column_name == "WIDGET_KEY")
        .unwrap();
    assert!(pk.is_subject_id);
}

#[test]
fn subject_key_name_fallback_is_unverified() {
    // No identifier_field_ids, but a required WIDGET_KEY matching <STEM>_KEY.
    let t = tbl(
        "DIM_WIDGET",
        vec![],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            sc(2, "NAME", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
    assert!(out.structured.table_mappings[0]
        .subject_template
        .ends_with("/{WIDGET_KEY}"));
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())])
    );
}

#[test]
fn subject_key_fallback_rejects_nullable_key_under_strict_strategy() {
    // Under the strict `Identifier` strategy, a name-matching key that is nullable
    // fails the gate → NoSafeSubjectKey, no subject. (Under the default `Auto`
    // strategy it is emitted anyway; see `auto_name_fallback_emits_unverified_subject`.)
    let t = tbl(
        "DIM_WIDGET",
        vec![],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false),
            sc(2, "NAME", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &strict_opts());
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    assert!(!diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn identifier_field_ids_nullable_column_adopted_under_auto_not_indexed() {
    // identifier_field_ids points at a NULLABLE column (not required, no proven
    // null_fraction==0) — the Snowflake-managed-Iceberg case. Under the default
    // `Auto` strategy the emitter ADOPTS it as the subject (downgrade
    // NoSafeSubjectKey → SubjectKeyUnverified) so the table stays browsable
    // instead of emitting an empty subject that later 500s at scan time ("Subject
    // map must have rr:template, rr:column, or rr:constant"). But it is NOT
    // indexed as an FK parent: a nullable parent key would silently drop child
    // rows at join time.
    let parent = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false), // nullable identifier
            sc(2, "NAME", FieldType::String),
        ],
    );
    // A child that WOULD join to WIDGET_KEY if the nullable identifier were
    // (wrongly) indexed as a PK.
    let child = tbl(
        "FACT_USE",
        vec![1],
        vec![
            ik(1, "USE_KEY", 1, 100, true),
            ik(2, "WIDGET_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());

    let widget = out.structured.table_mapping("DW.DIM_WIDGET").unwrap();
    assert!(
        widget.subject_template.ends_with("/{WIDGET_KEY}"),
        "a nullable identifier must still be adopted as a subject under Auto: {}",
        widget.subject_template
    );
    // Downgraded to SubjectKeyUnverified — NOT NoSafeSubjectKey.
    assert!(diag_cols(&out, DiagCode::SubjectKeyUnverified)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));
    assert!(!diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));

    // Adopted for browsability but NOT indexed as a PK → the child's WIDGET_KEY
    // resolves to no parent.
    assert!(
        !resolved_fks(&out)
            .iter()
            .any(|(ct, cc, _, _)| ct == "DW.FACT_USE" && cc == "WIDGET_KEY"),
        "a nullable identifier must not be a valid FK parent"
    );
}

#[test]
fn identifier_field_ids_nullable_column_rejected_under_strict() {
    // Under the strict `Identifier` strategy the pre-change behavior holds: a
    // nullable declared identifier fails the non-null gate → NoSafeSubjectKey, no
    // subject. (Under `Auto` it is adopted-but-not-indexed; see
    // `identifier_field_ids_nullable_column_adopted_under_auto_not_indexed`.)
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false), // nullable identifier
            sc(2, "NAME", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &strict_opts());
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));
}

#[test]
fn identifier_with_unknown_null_fraction_from_partial_stats_is_not_safe() {
    // Partial-coverage Tier-B stats leave null_fraction == None (unknown). An
    // identifier column that is neither `required` nor proven null-free
    // (null_fraction == 0) must NOT be treated as a safe subject key — is_non_null
    // is false — so the partial-coverage fix (#5) and the non-null gate (#4)
    // compose correctly at the emitter level. The shared fixtures only ever set
    // null_fraction from `required`, so this builds the column explicitly.
    let key = |null_fraction: Option<f64>| EmitColumn {
        field_id: 1,
        name: "WIDGET_KEY".to_string(),
        iceberg_type: "long".to_string(),
        field_type: FieldType::Int64,
        required: false, // not declared NOT NULL; safety must come from stats
        nested: false,
        doc: None,
        stats: EmitColumnStats {
            null_fraction,
            min: Some(TypedBound::Int(1)),
            max: Some(TypedBound::Int(100)),
        },
    };

    // Unknown coverage → not a safe subject. Observed under the strict
    // `Identifier` strategy, where the failed non-null gate still yields no
    // subject + NoSafeSubjectKey. (Under `Auto` the same is_non_null==false
    // column is instead adopted-but-not-indexed — see
    // `identifier_field_ids_nullable_column_adopted_under_auto_not_indexed`. Both
    // observe the same gate: is_non_null must be false for unknown null_fraction.)
    let out = emit_r2rml(
        &[tbl("DIM_WIDGET", vec![1], vec![key(None)])],
        &strict_opts(),
    );
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));

    // Full coverage proving null_fraction == 0 → the SAME column is safe (only
    // unverified for uniqueness).
    let out2 = emit_r2rml(
        &[tbl("DIM_WIDGET", vec![1], vec![key(Some(0.0))])],
        &EmitOptions::default(),
    );
    assert!(out2.structured.table_mappings[0]
        .subject_template
        .ends_with("/{WIDGET_KEY}"));
    assert_eq!(
        diag_cols(&out2, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())])
    );
    assert!(diag_cols(&out2, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn no_safe_subject_key_emits_no_subject_under_strict_strategy() {
    // Under the strict `Identifier` strategy a keyless table emits no subject and
    // never invents a surrogate row id. (Under `Auto` it synthesizes a composite;
    // see `auto_keyless_table_synthesizes_composite_subject`.)
    let t = tbl(
        "WEIRD",
        vec![],
        vec![sc(1, "A", FieldType::String), sc(2, "B", FieldType::Int64)],
    );
    let out = emit_r2rml(&[t], &strict_opts());
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    let codes: Vec<_> = out.diagnostics.iter().map(|d| d.code).collect();
    assert!(codes.contains(&DiagCode::NoSafeSubjectKey));
}

#[test]
fn composite_subject_key_uses_multi_placeholder_template() {
    let t = tbl(
        "BRIDGE",
        vec![1, 2],
        vec![
            ik(1, "LEFT_KEY", 1, 100, true),
            ik(2, "RIGHT_KEY", 1, 100, true),
            sc(3, "V", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
    assert!(out.structured.table_mappings[0]
        .subject_template
        .ends_with("/{LEFT_KEY}/{RIGHT_KEY}"));
}

// =============================================================================
// Subject-key strategy — Auto fallback (always emit) vs Identifier (strict)
// =============================================================================

#[test]
fn auto_name_fallback_emits_unverified_subject_not_indexed() {
    // The live Snowflake case: no identifier_field_ids, and a WIDGET_KEY that is
    // required=false with unknown null_fraction (NOT provably non-null). Under the
    // default Auto strategy the emitter USES it — SubjectKeyUnverified, downgraded
    // from NoSafeSubjectKey — so the table is saveable, but does NOT index it as
    // an FK parent: same policy as a nullable declared identifier (a nullable /
    // uniqueness-unverifiable parent key silently changes join cardinality). A
    // caller that knows better asserts the PK via the per-table override.
    let parent = tbl(
        "DIM_WIDGET",
        vec![], // no identifier_field_ids
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false), // not provably non-null
            sc(2, "NAME", FieldType::String),
        ],
    );
    let child = tbl(
        "FACT_USE",
        vec![1],
        vec![
            ik(1, "USE_KEY", 1, 100, true),
            ik(2, "WIDGET_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());

    let widget = out.structured.table_mapping("DW.DIM_WIDGET").unwrap();
    assert!(
        widget.subject_template.ends_with("/{WIDGET_KEY}"),
        "Auto must emit a subject on the unverified name key, got {}",
        widget.subject_template
    );
    assert!(diag_cols(&out, DiagCode::SubjectKeyUnverified)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
    // NOT join-compatible: the nullable fallback key must not become a live FK
    // parent, so the child's WIDGET_KEY stays literal and is surfaced.
    assert!(
        !resolved_fks(&out)
            .iter()
            .any(|(ct, cc, pt, _)| ct == "DW.FACT_USE"
                && cc == "WIDGET_KEY"
                && pt == "DW.DIM_WIDGET"),
        "a nullable name-fallback key must not be an FK parent"
    );
    assert!(diag_cols(&out, DiagCode::UnresolvedFkCandidate)
        .contains(&("DW.FACT_USE".to_string(), "WIDGET_KEY".to_string())));
}

#[test]
fn auto_keyless_table_synthesizes_composite_subject() {
    // Auto default + a table with NO key-like column and NO provably-non-null
    // column → a deterministic composite subject over ALL flat columns
    // (SubjectKeySynthesized), never a fabricated rownum — and the diagnostic
    // must DISCLOSE that rows with a NULL in any templated column are dropped
    // (template expansion emits no subject for them).
    let weird = tbl(
        "WEIRD",
        vec![],
        vec![sc(1, "A", FieldType::String), sc(2, "B", FieldType::Int64)],
    );
    let out = emit_r2rml(&[weird], &EmitOptions::default());
    let tm = &out.structured.table_mappings[0];
    assert!(
        tm.subject_template.ends_with("/{A}/{B}"),
        "keyless table must get a composite subject, got {}",
        tm.subject_template
    );
    let synth = out
        .diagnostics
        .iter()
        .find(|d| {
            d.code == DiagCode::SubjectKeySynthesized && d.table.as_deref() == Some("DW.WEIRD")
        })
        .expect("keyless table must earn SubjectKeySynthesized");
    assert!(
        synth.message.contains("WILL NOT APPEAR"),
        "an all-nullable synthesized subject must disclose the NULL row drop, got: {}",
        synth.message
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn auto_keyless_synthesis_prefers_provably_non_null_columns() {
    // Keyless DIM_LOOKUP(CODE NOT NULL, LABEL nullable, NOTE nullable): the
    // synthesized composite must template over the provably-non-null subset
    // ONLY — templating over LABEL/NOTE would silently drop every row where
    // either is NULL (exactly the rows keyless tables tend to have). The
    // trade (rows identical on CODE collapse) is disclosed instead.
    let lookup = tbl(
        "LOOKUP",
        vec![],
        vec![
            ik(1, "CODE", 1, 999, true), // required → provably non-null
            sc(2, "LABEL", FieldType::String),
            sc(3, "NOTE", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[lookup], &EmitOptions::default());
    let tm = &out.structured.table_mappings[0];
    assert!(
        tm.subject_template.ends_with("/{CODE}"),
        "synthesis must use the non-null subset, got {}",
        tm.subject_template
    );
    assert!(
        !tm.subject_template.contains("{LABEL}") && !tm.subject_template.contains("{NOTE}"),
        "nullable columns must not enter the synthesized template, got {}",
        tm.subject_template
    );
    let synth = out
        .diagnostics
        .iter()
        .find(|d| {
            d.code == DiagCode::SubjectKeySynthesized && d.table.as_deref() == Some("DW.LOOKUP")
        })
        .expect("keyless table must earn SubjectKeySynthesized");
    assert!(
        synth.message.contains("provably-non-null") && synth.message.contains("no row is dropped"),
        "diagnostic must state the non-null-subset choice, got: {}",
        synth.message
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn auto_synthesized_composite_is_not_an_fk_parent() {
    // A synthesized composite must NOT be indexed as an FK parent (it is not a
    // unique key) — even when it happens to be a single column.
    let weird = tbl("WEIRD", vec![], vec![ik(1, "B", 1, 100, false)]);
    let child = tbl(
        "FACT_C",
        vec![1],
        vec![ik(1, "C_KEY", 1, 100, true), ik(2, "B", 1, 100, false)],
    );
    let out = emit_r2rml(&[weird, child], &EmitOptions::default());
    // WEIRD is saveable (got a subject)...
    assert!(!out
        .structured
        .table_mapping("DW.WEIRD")
        .unwrap()
        .subject_template
        .is_empty());
    // ...but is never an FK parent for the child's B column.
    assert!(!resolved_fks(&out)
        .iter()
        .any(|(ct, cc, _, _)| ct == "DW.FACT_C" && cc == "B"));
}

#[test]
fn composite_override_subject_key_renders() {
    // A per-table override may now carry a COMPOSITE (multi-column) subject key —
    // the only prior blocker was the override being a single `Option<String>`.
    let t = tbl(
        "BRIDGE",
        vec![1],
        vec![
            ik(1, "LEFT_KEY", 1, 100, true),
            ik(2, "RIGHT_KEY", 1, 100, true),
            sc(3, "V", FieldType::String),
        ],
    );
    let opts = opts_with_override(
        "DW",
        "BRIDGE",
        TableOverride {
            primary_key: Some(vec!["LEFT_KEY".to_string(), "RIGHT_KEY".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);
    let tm = &out.structured.table_mappings[0];
    assert!(
        tm.subject_template.ends_with("/{LEFT_KEY}/{RIGHT_KEY}"),
        "composite override must render both placeholders, got {}",
        tm.subject_template
    );
    // Both key columns are marked isSubjectId; each earns a SubjectKeyUnverified.
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([
            ("DW.BRIDGE".to_string(), "LEFT_KEY".to_string()),
            ("DW.BRIDGE".to_string(), "RIGHT_KEY".to_string()),
        ])
    );
    let subj_id_cols: BTreeSet<&str> = tm
        .columns
        .iter()
        .filter(|c| c.is_subject_id)
        .map(|c| c.column_name.as_str())
        .collect();
    assert_eq!(subj_id_cols, BTreeSet::from(["LEFT_KEY", "RIGHT_KEY"]));
}

#[test]
fn composite_override_key_is_not_indexed_as_single_pk() {
    // A composite override subject key must NOT be indexed as a single-column FK
    // parent — a child matching one member must not resolve to this table.
    let bridge = tbl(
        "BRIDGE",
        vec![1],
        vec![
            ik(1, "LEFT_KEY", 1, 100, true),
            ik(2, "RIGHT_KEY", 1, 100, true),
        ],
    );
    let child = tbl(
        "FACT_C",
        vec![1],
        vec![
            ik(1, "C_KEY", 1, 100, true),
            ik(2, "LEFT_KEY", 1, 100, false),
        ],
    );
    let opts = opts_with_override(
        "DW",
        "BRIDGE",
        TableOverride {
            primary_key: Some(vec!["LEFT_KEY".to_string(), "RIGHT_KEY".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[bridge, child], &opts);
    assert!(!resolved_fks(&out)
        .iter()
        .any(|(ct, cc, _, _)| ct == "DW.FACT_C" && cc == "LEFT_KEY"));
}

#[test]
fn per_table_identifier_strategy_overrides_global_auto() {
    // Global strategy is Auto, but a per-table subject_strategy=Identifier forces
    // strict behavior for that table: a nullable name key → NoSafeSubjectKey.
    let t = tbl(
        "DIM_WIDGET",
        vec![],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false), // not provably non-null
            sc(2, "NAME", FieldType::String),
        ],
    );
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: None,
            class_name: None,
            subject_strategy: Some(SubjectStrategy::Identifier),
        },
    );
    let out = emit_r2rml(&[t], &opts);
    assert!(
        out.structured.table_mappings[0].subject_template.is_empty(),
        "per-table Identifier override must restore strict (no subject)"
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())));
}

// =============================================================================
// FK heuristic — focused cases on synthetic inputs
// =============================================================================

#[test]
fn fk_exact_name_match_resolves() {
    let parent = tbl(
        "DIM_PARENT",
        vec![1],
        vec![ik(1, "PARENT_KEY", 1, 100, true)],
    );
    let child = tbl(
        "FACT_CHILD",
        vec![1],
        vec![
            ik(1, "CHILD_KEY", 1, 100, true),
            ik(2, "PARENT_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());
    assert!(resolved_fks(&out).contains(&(
        "DW.FACT_CHILD".to_string(),
        "PARENT_KEY".to_string(),
        "DW.DIM_PARENT".to_string(),
        "PARENT_KEY".to_string(),
    )));
}

#[test]
fn fk_unambiguous_suffix_match_resolves() {
    let parent = tbl("DIM_NODE", vec![1], vec![ik(1, "NODE_KEY", 1, 100, true)]);
    let child = tbl(
        "FACT_EDGE",
        vec![1],
        vec![
            ik(1, "EDGE_KEY", 1, 100, true),
            ik(2, "SOURCE_NODE_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());
    assert!(resolved_fks(&out).contains(&(
        "DW.FACT_EDGE".to_string(),
        "SOURCE_NODE_KEY".to_string(),
        "DW.DIM_NODE".to_string(),
        "NODE_KEY".to_string(),
    )));
}

#[test]
fn fk_ambiguous_multiple_parents_not_fabricated() {
    // Two parents share the PK name FOO_KEY → a child FOO_KEY is ambiguous.
    let a = tbl("DIM_A", vec![1], vec![ik(1, "FOO_KEY", 1, 100, true)]);
    let b = tbl("DIM_B", vec![1], vec![ik(1, "FOO_KEY", 1, 100, true)]);
    let child = tbl(
        "FACT_C",
        vec![1],
        vec![
            ik(1, "C_KEY", 1, 100, true),
            ik(2, "FOO_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[a, b, child], &EmitOptions::default());
    // No join fabricated for the ambiguous column.
    assert!(!resolved_fks(&out)
        .iter()
        .any(|(ct, cc, _, _)| ct == "DW.FACT_C" && cc == "FOO_KEY"));
    assert!(diag_cols(&out, DiagCode::AmbiguousFk)
        .contains(&("DW.FACT_C".to_string(), "FOO_KEY".to_string())));
}

#[test]
fn fk_role_renamed_key_is_unresolved_not_fabricated() {
    // MANAGER_KEY never matches EMPLOYEE_KEY by name → unresolved, no join.
    let emp = tbl(
        "DIM_EMPLOYEE",
        vec![1],
        vec![
            ik(1, "EMPLOYEE_KEY", 1, 100, true),
            ik(2, "MANAGER_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[emp], &EmitOptions::default());
    assert!(
        resolved_fks(&out).is_empty(),
        "role-renamed FK must not resolve"
    );
    assert!(diag_cols(&out, DiagCode::UnresolvedFkCandidate)
        .contains(&("DW.DIM_EMPLOYEE".to_string(), "MANAGER_KEY".to_string())));
}

#[test]
fn fk_self_join_resolves_when_name_aligned() {
    // A name-aligned self reference (PARENT_NODE_KEY → NODE_KEY) resolves.
    let node = tbl(
        "DIM_NODE",
        vec![1],
        vec![
            ik(1, "NODE_KEY", 1, 100, true),
            ik(2, "PARENT_NODE_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[node], &EmitOptions::default());
    assert!(resolved_fks(&out).contains(&(
        "DW.DIM_NODE".to_string(),
        "PARENT_NODE_KEY".to_string(),
        "DW.DIM_NODE".to_string(),
        "NODE_KEY".to_string(),
    )));
}

#[test]
fn fk_join_locals_disambiguate_across_joins_not_just_literals() {
    // Two parents whose PKs strip+camel to the SAME local: GEOGRAPHY_KEY and
    // GEOGRAPHY_ID both → `geography`. A child referencing both must emit two
    // DISTINCT predicate IRIs (the second `Ref`-suffixed), never one merged
    // predicate that would collapse two relationships.
    let geo_a = tbl(
        "DIM_GEO_A",
        vec![1],
        vec![ik(1, "GEOGRAPHY_KEY", 1, 100, true)],
    );
    let geo_b = tbl(
        "DIM_GEO_B",
        vec![1],
        vec![ik(1, "GEOGRAPHY_ID", 1, 100, true)],
    );
    let child = tbl(
        "FACT_C",
        vec![1],
        vec![
            ik(1, "C_KEY", 1, 100, true),
            ik(2, "GEOGRAPHY_KEY", 1, 100, false),
            ik(3, "GEOGRAPHY_ID", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[geo_a, geo_b, child], &EmitOptions::default());

    // Both FKs resolve to their distinct parents.
    let fks = resolved_fks(&out);
    assert!(fks.contains(&(
        "DW.FACT_C".to_string(),
        "GEOGRAPHY_KEY".to_string(),
        "DW.DIM_GEO_A".to_string(),
        "GEOGRAPHY_KEY".to_string(),
    )));
    assert!(fks.contains(&(
        "DW.FACT_C".to_string(),
        "GEOGRAPHY_ID".to_string(),
        "DW.DIM_GEO_B".to_string(),
        "GEOGRAPHY_ID".to_string(),
    )));

    // The two join predicate IRIs are DISTINCT.
    let child_tm = out.structured.table_mapping("DW.FACT_C").unwrap();
    let join_preds: Vec<&str> = child_tm
        .columns
        .iter()
        .filter(|c| c.foreign_key.is_some())
        .map(|c| c.predicate_iri.as_str())
        .collect();
    assert_eq!(join_preds.len(), 2);
    assert_ne!(
        join_preds[0], join_preds[1],
        "colliding FK locals must not merge to one predicate IRI"
    );
}

#[test]
fn fk_single_parent_vetoed_when_bounds_refute_containment() {
    // Range-containment is a disambiguator, not a hard gate — but when BOTH
    // sides supply complete [min,max] bounds and the child range is NOT
    // contained, the join is provably contradicted: veto it (kept literal,
    // surfaced as UnresolvedFkCandidate) rather than fabricating a dangling
    // relationship. Child [1,500] vs parent [1000,2000] is disjoint.
    let parent = tbl(
        "DIM_PARENT",
        vec![1],
        vec![ik(1, "PARENT_KEY", 1000, 2000, true)],
    );
    let child = tbl(
        "FACT_CHILD",
        vec![1],
        vec![
            ik(1, "CHILD_KEY", 1, 100, true),
            ik(2, "PARENT_KEY", 1, 500, false),
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());
    assert!(
        resolved_fks(&out).is_empty(),
        "a single name+type parent with refuting bounds must NOT resolve"
    );
    let veto = out
        .diagnostics
        .iter()
        .find(|d| {
            d.code == DiagCode::UnresolvedFkCandidate
                && d.table.as_deref() == Some("DW.FACT_CHILD")
                && d.column.as_deref() == Some("PARENT_KEY")
        })
        .expect("range veto must surface an UnresolvedFkCandidate diagnostic");
    assert!(
        veto.message.contains("not contained"),
        "veto diagnostic must explain the range refutation, got: {}",
        veto.message
    );
}

#[test]
fn fk_single_parent_resolves_when_bounds_cannot_refute() {
    // One side missing bounds ⇒ containment can be neither confirmed NOR
    // refuted ⇒ the single name+type match still joins (the Snowflake
    // no-bounds case the disambiguator-not-gate design exists for).
    let parent = tbl(
        "DIM_PARENT",
        vec![1],
        vec![dk_nobounds(1, "PARENT_KEY", true)], // parent supplies no bounds
    );
    let child = tbl(
        "FACT_CHILD",
        vec![1],
        vec![
            dk(1, "CHILD_KEY", 1, 100, true),
            dk(2, "PARENT_KEY", 1, 500, false), // child has bounds
        ],
    );
    let out = emit_r2rml(&[parent, child], &EmitOptions::default());
    assert!(
        resolved_fks(&out).contains(&(
            "DW.FACT_CHILD".to_string(),
            "PARENT_KEY".to_string(),
            "DW.DIM_PARENT".to_string(),
            "PARENT_KEY".to_string(),
        )),
        "absent bounds must never block a single name+type match"
    );
}

#[test]
fn fk_range_disambiguates_between_same_named_parents() {
    // TWO parents share the PK name PARENT_KEY (name+type ambiguous). Range
    // containment is used ONLY to disambiguate: the child range ⊆ exactly one
    // parent → that parent is chosen; the other is excluded.
    let in_range = tbl("DIM_A", vec![1], vec![ik(1, "PARENT_KEY", 1, 100, true)]);
    let out_of_range = tbl(
        "DIM_B",
        vec![1],
        vec![ik(1, "PARENT_KEY", 1000, 2000, true)],
    );
    let child = tbl(
        "FACT_CHILD",
        vec![1],
        vec![
            ik(1, "CHILD_KEY", 1, 100_000, true),
            ik(2, "PARENT_KEY", 1, 100, false), // ⊆ DIM_A only
        ],
    );
    let out = emit_r2rml(&[in_range, out_of_range, child], &EmitOptions::default());
    assert!(
        resolved_fks(&out).contains(&(
            "DW.FACT_CHILD".to_string(),
            "PARENT_KEY".to_string(),
            "DW.DIM_A".to_string(),
            "PARENT_KEY".to_string(),
        )),
        "range-containment must disambiguate to the in-range parent"
    );
    // Never to the out-of-range parent.
    assert!(!resolved_fks(&out).contains(&(
        "DW.FACT_CHILD".to_string(),
        "PARENT_KEY".to_string(),
        "DW.DIM_B".to_string(),
        "PARENT_KEY".to_string(),
    )));
}

#[test]
fn fk_child_fact_to_hub_emitted_with_advisory() {
    let hub = tbl(
        "FACT_ORDER",
        vec![1],
        vec![ik(1, "ORDER_KEY", 1, 100, true)],
    );
    let line = tbl(
        "FACT_ORDER_LINE",
        vec![1],
        vec![
            ik(1, "ORDER_LINE_KEY", 1, 100, true),
            ik(2, "ORDER_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[hub, line], &EmitOptions::default());
    assert!(resolved_fks(&out).contains(&(
        "DW.FACT_ORDER_LINE".to_string(),
        "ORDER_KEY".to_string(),
        "DW.FACT_ORDER".to_string(),
        "ORDER_KEY".to_string(),
    )));
    assert!(diag_cols(&out, DiagCode::FactHubJoinAdvisory)
        .contains(&("DW.FACT_ORDER_LINE".to_string(), "ORDER_KEY".to_string())));
}

#[test]
fn non_pk_fact_link_is_not_fabricated() {
    // A fact key column whose name matches no PK (only a non-PK measure) stays
    // literal — non-PK targets are never join targets.
    let a = tbl(
        "FACT_A",
        vec![1],
        vec![
            ik(1, "A_KEY", 1, 100, true),
            sc(2, "WIDGET_COUNT", FieldType::Int64),
        ],
    );
    let b = tbl(
        "FACT_B",
        vec![1],
        vec![
            ik(1, "B_KEY", 1, 100, true),
            ik(2, "WIDGET_KEY", 1, 100, false),
        ],
    );
    let out = emit_r2rml(&[a, b], &EmitOptions::default());
    assert!(resolved_fks(&out).is_empty());
    // WIDGET_KEY is key-like but unmatched → surfaced, never fabricated.
    assert!(diag_cols(&out, DiagCode::UnresolvedFkCandidate)
        .contains(&("DW.FACT_B".to_string(), "WIDGET_KEY".to_string())));
}

#[test]
fn nested_column_is_skipped() {
    let mut nested = sc(2, "PAYLOAD", FieldType::String);
    nested.nested = true;
    let t = tbl(
        "DIM_THING",
        vec![1],
        vec![ik(1, "THING_KEY", 1, 100, true), nested],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
    let tm = &out.structured.table_mappings[0];
    assert!(tm.columns.iter().all(|c| c.column_name != "PAYLOAD"));
    assert!(diag_cols(&out, DiagCode::NestedColumnSkipped)
        .contains(&("DW.DIM_THING".to_string(), "PAYLOAD".to_string())));
}

// =============================================================================
// FK heuristic — decimal keys, bounds-free joins, non-key-type skip
// =============================================================================

#[test]
fn fk_decimal_scale0_child_resolves_to_decimal_parent() {
    // Snowflake NUMBER(38,0) surrogate keys arrive as decimal(38,0), NOT long.
    // FACT_ORDER.CUSTOMER_KEY (decimal(38,0)) → DIM_CUSTOMER.CUSTOMER_KEY must
    // resolve — the integer-only candidacy gate used to drop it silently.
    let dim = tbl(
        "DIM_CUSTOMER",
        vec![1],
        vec![dk(1, "CUSTOMER_KEY", 1, 100_000, true)],
    );
    let fact = tbl(
        "FACT_ORDER",
        vec![1],
        vec![
            dk(1, "ORDER_KEY", 1, 500_000, true),
            dk(2, "CUSTOMER_KEY", 1, 100_000, false),
        ],
    );
    let out = emit_r2rml(&[dim, fact], &EmitOptions::default());
    assert!(
        resolved_fks(&out).contains(&(
            "DW.FACT_ORDER".to_string(),
            "CUSTOMER_KEY".to_string(),
            "DW.DIM_CUSTOMER".to_string(),
            "CUSTOMER_KEY".to_string(),
        )),
        "a decimal(38,0) child key must resolve to a decimal(38,0) parent PK"
    );
}

#[test]
fn fk_exact_name_type_resolves_with_no_bounds_present() {
    // Neither side supplies min/max bounds (the common Snowflake case). A single
    // name+type match must still join — on name∧type ALONE, no range needed.
    let dim = tbl(
        "DIM_CUSTOMER",
        vec![1],
        vec![dk_nobounds(1, "CUSTOMER_KEY", true)],
    );
    let fact = tbl(
        "FACT_ORDER",
        vec![1],
        vec![
            dk_nobounds(1, "ORDER_KEY", true),
            dk_nobounds(2, "CUSTOMER_KEY", false),
        ],
    );
    let out = emit_r2rml(&[dim, fact], &EmitOptions::default());
    assert!(
        resolved_fks(&out).contains(&(
            "DW.FACT_ORDER".to_string(),
            "CUSTOMER_KEY".to_string(),
            "DW.DIM_CUSTOMER".to_string(),
            "CUSTOMER_KEY".to_string(),
        )),
        "an exact name+type match must resolve with no bounds present"
    );
}

#[test]
fn fk_key_named_non_key_type_emits_diagnostic() {
    // A `*_KEY`-named column whose TYPE is not key-like (here a VARCHAR) is
    // excluded from FK inference — but with a visible NonKeyTypeSkipped diagnostic
    // instead of a silent drop, and never a fabricated join.
    let dim = tbl(
        "DIM_CUSTOMER",
        vec![1],
        vec![ik(1, "CUSTOMER_KEY", 1, 100, true)],
    );
    let fact = tbl(
        "FACT_ORDER",
        vec![1],
        vec![
            ik(1, "ORDER_KEY", 1, 500, true),
            sc(2, "CUSTOMER_KEY", FieldType::String), // key-named, non-key type
        ],
    );
    let out = emit_r2rml(&[dim, fact], &EmitOptions::default());
    assert!(diag_cols(&out, DiagCode::NonKeyTypeSkipped)
        .contains(&("DW.FACT_ORDER".to_string(), "CUSTOMER_KEY".to_string())));
    assert!(!resolved_fks(&out)
        .iter()
        .any(|(ct, cc, _, _)| ct == "DW.FACT_ORDER" && cc == "CUSTOMER_KEY"));

    // A SCALED decimal `*_KEY` (money-like, scale > 0) is likewise non-key-typed.
    let mut scaled = dk(3, "AMOUNT_KEY", 1, 100, false);
    scaled.field_type = FieldType::Decimal {
        precision: 18,
        scale: 2,
    };
    scaled.iceberg_type = "decimal(18,2)".to_string();
    let t = tbl(
        "FACT_X",
        vec![1],
        vec![ik(1, "X_KEY", 1, 100, true), scaled],
    );
    let out2 = emit_r2rml(&[t], &EmitOptions::default());
    assert!(diag_cols(&out2, DiagCode::NonKeyTypeSkipped)
        .contains(&("DW.FACT_X".to_string(), "AMOUNT_KEY".to_string())));
}

// =============================================================================
// Datatype map (via a full emit) + determinism
// =============================================================================

#[test]
fn every_non_string_column_carries_a_datatype() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    for tm in &out.structured.table_mappings {
        for col in &tm.columns {
            if col.foreign_key.is_some() {
                continue; // join POMs carry no datatype
            }
            // The only untyped literals are strings.
            if col.datatype.is_none() {
                // Confirm it really is a string column in the source schema.
                // (COUNTRY, names, ids, ... — all String.)
                assert!(
                    is_string_column(&tm.table_name, &col.column_name),
                    "non-string column {}.{} is missing a datatype",
                    tm.table_name,
                    col.column_name
                );
            }
        }
    }
}

/// The known string columns (used to justify a missing datatype).
fn is_string_column(table: &str, column: &str) -> bool {
    let tables = enterprise_dw_tables();
    tables
        .iter()
        .find(|t| t.qualified_name() == table)
        .and_then(|t| t.columns.iter().find(|c| c.name == column))
        .map(|c| c.field_type == FieldType::String)
        .unwrap_or(false)
}

#[test]
fn emit_is_byte_deterministic() {
    let tables = enterprise_dw_tables();
    let opts = EmitOptions::default();
    let a = emit_r2rml(&tables, &opts);
    let b = emit_r2rml(&tables, &opts);
    assert_eq!(a.turtle, b.turtle, "two emit runs must be byte-identical");
}

// =============================================================================
// Wire shape: `structured` is solo's camelCase StructuredR2rmlMapping
// =============================================================================

#[test]
fn structured_serializes_to_solo_camelcase_shape() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    let v = serde_json::to_value(&out.structured).unwrap();
    let obj = v
        .as_object()
        .expect("structured must be a JSON object, not an array");

    assert!(obj.contains_key("baseNamespace"));
    assert!(obj.contains_key("prefixes"));
    let table_mappings = obj
        .get("tableMappings")
        .and_then(|t| t.as_array())
        .expect("tableMappings array");
    assert_eq!(table_mappings.len(), 16);

    let first = table_mappings[0].as_object().unwrap();
    for key in ["tableName", "classIri", "subjectTemplate", "columns"] {
        assert!(first.contains_key(key), "tableMapping missing {key}");
    }

    // Find a foreign-key column somewhere and check its camelCase shape.
    let fk = table_mappings
        .iter()
        .flat_map(|t| t["columns"].as_array().unwrap())
        .find_map(|c| c.get("foreignKey").filter(|f| !f.is_null()))
        .expect("at least one foreignKey column");
    let fk = fk.as_object().unwrap();
    for key in ["targetTable", "childColumn", "parentColumn"] {
        assert!(fk.contains_key(key), "foreignKey missing {key}");
    }

    // A literal column entry has the camelCase scalar fields.
    let col = table_mappings[0]["columns"][0].as_object().unwrap();
    for key in ["columnName", "predicateIri", "isSubjectId", "isIri"] {
        assert!(col.contains_key(key), "column missing {key}");
    }
}

// =============================================================================
// Enterprise structural match (the full 16-table graph)
// =============================================================================

/// The 29 deterministically-resolvable FK edges of `enterprise.ttl`.
const EXPECTED_RESOLVED: &[(&str, &str, &str, &str)] = &[
    // dimension → dimension (7)
    (
        "DW.DIM_SUPPLIER",
        "GEOGRAPHY_KEY",
        "DW.DIM_GEOGRAPHY",
        "GEOGRAPHY_KEY",
    ),
    (
        "DW.DIM_ACCOUNT",
        "GEOGRAPHY_KEY",
        "DW.DIM_GEOGRAPHY",
        "GEOGRAPHY_KEY",
    ),
    ("DW.DIM_EMPLOYEE", "STORE_KEY", "DW.DIM_STORE", "STORE_KEY"),
    (
        "DW.DIM_STORE",
        "GEOGRAPHY_KEY",
        "DW.DIM_GEOGRAPHY",
        "GEOGRAPHY_KEY",
    ),
    (
        "DW.DIM_CUSTOMER",
        "GEOGRAPHY_KEY",
        "DW.DIM_GEOGRAPHY",
        "GEOGRAPHY_KEY",
    ),
    (
        "DW.DIM_CUSTOMER",
        "ACCOUNT_KEY",
        "DW.DIM_ACCOUNT",
        "ACCOUNT_KEY",
    ),
    (
        "DW.DIM_PRODUCT",
        "SUPPLIER_KEY",
        "DW.DIM_SUPPLIER",
        "SUPPLIER_KEY",
    ),
    // fact → dimension / hub (22)
    (
        "DW.FACT_ORDER",
        "CUSTOMER_KEY",
        "DW.DIM_CUSTOMER",
        "CUSTOMER_KEY",
    ),
    (
        "DW.FACT_ORDER",
        "ACCOUNT_KEY",
        "DW.DIM_ACCOUNT",
        "ACCOUNT_KEY",
    ),
    ("DW.FACT_ORDER", "STORE_KEY", "DW.DIM_STORE", "STORE_KEY"),
    ("DW.FACT_ORDER", "ORDER_DATE_KEY", "DW.DIM_DATE", "DATE_KEY"),
    (
        "DW.FACT_ORDER_LINE",
        "ORDER_KEY",
        "DW.FACT_ORDER",
        "ORDER_KEY",
    ),
    (
        "DW.FACT_ORDER_LINE",
        "PRODUCT_KEY",
        "DW.DIM_PRODUCT",
        "PRODUCT_KEY",
    ),
    (
        "DW.FACT_INVENTORY_SNAPSHOT",
        "PRODUCT_KEY",
        "DW.DIM_PRODUCT",
        "PRODUCT_KEY",
    ),
    (
        "DW.FACT_INVENTORY_SNAPSHOT",
        "STORE_KEY",
        "DW.DIM_STORE",
        "STORE_KEY",
    ),
    (
        "DW.FACT_INVENTORY_SNAPSHOT",
        "SNAPSHOT_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
    (
        "DW.FACT_SHIPMENT",
        "ORDER_KEY",
        "DW.FACT_ORDER",
        "ORDER_KEY",
    ),
    (
        "DW.FACT_SHIPMENT",
        "DEST_GEOGRAPHY_KEY",
        "DW.DIM_GEOGRAPHY",
        "GEOGRAPHY_KEY",
    ),
    (
        "DW.FACT_SHIPMENT",
        "SHIP_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
    ("DW.FACT_PAYMENT", "ORDER_KEY", "DW.FACT_ORDER", "ORDER_KEY"),
    (
        "DW.FACT_PAYMENT",
        "CUSTOMER_KEY",
        "DW.DIM_CUSTOMER",
        "CUSTOMER_KEY",
    ),
    (
        "DW.FACT_PAYMENT",
        "PAYMENT_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
    (
        "DW.FACT_GL_JOURNAL",
        "POSTING_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
    (
        "DW.FACT_WEB_EVENT",
        "CUSTOMER_KEY",
        "DW.DIM_CUSTOMER",
        "CUSTOMER_KEY",
    ),
    (
        "DW.FACT_WEB_EVENT",
        "PRODUCT_KEY",
        "DW.DIM_PRODUCT",
        "PRODUCT_KEY",
    ),
    (
        "DW.FACT_WEB_EVENT",
        "EVENT_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
    (
        "DW.FACT_SUPPORT_TICKET",
        "CUSTOMER_KEY",
        "DW.DIM_CUSTOMER",
        "CUSTOMER_KEY",
    ),
    (
        "DW.FACT_SUPPORT_TICKET",
        "PRODUCT_KEY",
        "DW.DIM_PRODUCT",
        "PRODUCT_KEY",
    ),
    (
        "DW.FACT_SUPPORT_TICKET",
        "OPEN_DATE_KEY",
        "DW.DIM_DATE",
        "DATE_KEY",
    ),
];

/// The 4 role-renamed employee FKs `enterprise.ttl` has but that are
/// unresolvable from metadata (name never matches `EMPLOYEE_KEY`).
const EXPECTED_UNRESOLVED_ROLE_RENAMED: &[(&str, &str)] = &[
    ("DW.DIM_EMPLOYEE", "MANAGER_KEY"),
    ("DW.DIM_STORE", "REGION_MANAGER_KEY"),
    ("DW.FACT_ORDER", "SALES_REP_KEY"),
    ("DW.FACT_SUPPORT_TICKET", "AGENT_KEY"),
];

/// The 3 child-fact → `FACT_ORDER` hub joins (emitted, but perf-advisory).
const EXPECTED_HUB: &[(&str, &str)] = &[
    ("DW.FACT_ORDER_LINE", "ORDER_KEY"),
    ("DW.FACT_SHIPMENT", "ORDER_KEY"),
    ("DW.FACT_PAYMENT", "ORDER_KEY"),
];

/// The surrogate subject key per table.
const EXPECTED_SUBJECT_KEYS: &[(&str, &str)] = &[
    ("DW.DIM_DATE", "DATE_KEY"),
    ("DW.DIM_GEOGRAPHY", "GEOGRAPHY_KEY"),
    ("DW.DIM_SUPPLIER", "SUPPLIER_KEY"),
    ("DW.DIM_ACCOUNT", "ACCOUNT_KEY"),
    ("DW.DIM_EMPLOYEE", "EMPLOYEE_KEY"),
    ("DW.DIM_STORE", "STORE_KEY"),
    ("DW.DIM_CUSTOMER", "CUSTOMER_KEY"),
    ("DW.DIM_PRODUCT", "PRODUCT_KEY"),
    ("DW.FACT_ORDER", "ORDER_KEY"),
    ("DW.FACT_ORDER_LINE", "ORDER_LINE_KEY"),
    ("DW.FACT_INVENTORY_SNAPSHOT", "INVENTORY_KEY"),
    ("DW.FACT_SHIPMENT", "SHIPMENT_KEY"),
    ("DW.FACT_PAYMENT", "PAYMENT_KEY"),
    ("DW.FACT_GL_JOURNAL", "JOURNAL_KEY"),
    ("DW.FACT_WEB_EVENT", "EVENT_KEY"),
    ("DW.FACT_SUPPORT_TICKET", "TICKET_KEY"),
];

#[test]
fn enterprise_fk_graph_matches_29_of_33() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());

    let got = resolved_fks(&out);
    let expected: BTreeSet<_> = EXPECTED_RESOLVED
        .iter()
        .map(|(ct, cc, pt, pc)| {
            (
                ct.to_string(),
                cc.to_string(),
                pt.to_string(),
                pc.to_string(),
            )
        })
        .collect();

    assert_eq!(got.len(), 29, "expected exactly 29 resolved FKs");
    assert_eq!(
        got, expected,
        "resolved FK graph must match enterprise.ttl's resolvable joins"
    );

    // The 4 role-renamed FKs are surfaced as UnresolvedFkCandidate, not fabricated.
    let unresolved = diag_cols(&out, DiagCode::UnresolvedFkCandidate);
    for (t, c) in EXPECTED_UNRESOLVED_ROLE_RENAMED {
        assert!(
            unresolved.contains(&(t.to_string(), c.to_string())),
            "{t}.{c} must be UnresolvedFkCandidate"
        );
        // ...and must NOT appear as a resolved join.
        assert!(
            !got.iter().any(|(ct, cc, _, _)| ct == t && cc == c),
            "{t}.{c} must not be fabricated as a join"
        );
    }

    // The 3 hub joins carry a FactHubJoinAdvisory.
    let hub = diag_cols(&out, DiagCode::FactHubJoinAdvisory);
    let expected_hub: BTreeSet<_> = EXPECTED_HUB
        .iter()
        .map(|(t, c)| (t.to_string(), c.to_string()))
        .collect();
    assert_eq!(
        hub, expected_hub,
        "hub advisories must be exactly the 3 child-fact→FACT_ORDER joins"
    );
}

#[test]
fn enterprise_subject_keys_match() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    for (table, key) in EXPECTED_SUBJECT_KEYS {
        let tm = out
            .structured
            .table_mapping(table)
            .unwrap_or_else(|| panic!("missing table {table}"));
        assert!(
            tm.subject_template.contains(&format!("{{{key}}}")),
            "table {table} subject template {} must key on {key}",
            tm.subject_template
        );
        // Exactly one class per table.
        assert!(!tm.class_iri.is_empty());
    }
}

#[test]
fn enterprise_datatypes_match_reference() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    // (table, column, expected datatype) covering every non-string datatype used
    // in enterprise.ttl, plus a string (untyped) control.
    let checks: &[(&str, &str, Option<&str>)] = &[
        ("DW.DIM_DATE", "DATE_KEY", Some("xsd:integer")),
        ("DW.DIM_DATE", "DATE", Some("xsd:date")),
        ("DW.DIM_DATE", "IS_WEEKEND", Some("xsd:boolean")),
        ("DW.DIM_GEOGRAPHY", "LATITUDE", Some("xsd:double")),
        ("DW.FACT_GL_JOURNAL", "DEBIT_AMOUNT", Some("xsd:decimal")),
        ("DW.FACT_WEB_EVENT", "EVENT_TS", Some("xsd:dateTime")),
        ("DW.DIM_GEOGRAPHY", "COUNTRY", None), // string → plain literal
    ];
    let mut by_table: BTreeMap<&str, BTreeMap<String, Option<String>>> = BTreeMap::new();
    for tm in &out.structured.table_mappings {
        let entry = by_table.entry(tm.table_name.as_str()).or_default();
        for col in &tm.columns {
            if col.foreign_key.is_none() {
                entry.insert(col.column_name.clone(), col.datatype.clone());
            }
        }
    }
    for (table, column, expected) in checks {
        let got = by_table
            .get(table)
            .and_then(|m| m.get(*column))
            .unwrap_or_else(|| panic!("missing {table}.{column}"));
        assert_eq!(
            got.as_deref(),
            *expected,
            "{table}.{column} datatype mismatch"
        );
    }
}

#[test]
fn session_id_is_the_only_extra_unresolved_candidate() {
    // Documented deviation: SESSION_ID is an integer *_ID with no matching PK,
    // so the spec's *_KEY/*_ID rule flags it. It is NOT one of the 33 enterprise
    // FKs, so "29/33 resolved + 4 role-renamed" still holds.
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    let unresolved = diag_cols(&out, DiagCode::UnresolvedFkCandidate);
    let mut expected: BTreeSet<(String, String)> = EXPECTED_UNRESOLVED_ROLE_RENAMED
        .iter()
        .map(|(t, c)| (t.to_string(), c.to_string()))
        .collect();
    expected.insert(("DW.FACT_WEB_EVENT".to_string(), "SESSION_ID".to_string()));
    assert_eq!(unresolved, expected);
}

// =============================================================================
// Per-table overrides (primary_key / class_name)
// =============================================================================

/// `EmitOptions::default()` plus a single `{namespace,name}` override entry.
fn opts_with_override(namespace: &str, name: &str, ov: TableOverride) -> EmitOptions {
    let mut opts = EmitOptions::default();
    opts.per_table_overrides
        .insert(TableKey::new(namespace, name), ov);
    opts
}

#[test]
fn override_primary_key_replaces_identifier_field_ids() {
    // identifier_field_ids=[1] would key on WIDGET_KEY with no unverified diag;
    // the override REPLACES it with ALT_KEY and always earns SubjectKeyUnverified.
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            ik(2, "ALT_KEY", 1, 100, true),
            sc(3, "NAME", FieldType::String),
        ],
    );
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: Some(vec!["ALT_KEY".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);
    let tm = &out.structured.table_mappings[0];

    // Subject keys on the override column, not the identifier_field_ids column.
    assert!(
        tm.subject_template.ends_with("/{ALT_KEY}"),
        "subject template {} must key on the override column",
        tm.subject_template
    );
    assert!(!tm.subject_template.contains("{WIDGET_KEY}"));
    // The override column is the isSubjectId literal.
    let pk = tm
        .columns
        .iter()
        .find(|c| c.column_name == "ALT_KEY")
        .unwrap();
    assert!(pk.is_subject_id);
    assert!(tm
        .columns
        .iter()
        .find(|c| c.column_name == "WIDGET_KEY")
        .is_some_and(|c| !c.is_subject_id));

    // An override PK ALWAYS attaches SubjectKeyUnverified, and never NoSafeSubjectKey.
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "ALT_KEY".to_string())])
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn override_primary_key_failing_null_gate_yields_no_safe_subject_key() {
    // The override column exists but is nullable (required=false, null_fraction
    // unknown) → fails the required / null_fraction==0 gate → NoSafeSubjectKey,
    // no subject, and NOT SubjectKeyUnverified.
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            ik(2, "NULLABLE_COL", 1, 100, false),
        ],
    );
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: Some(vec!["NULLABLE_COL".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);
    let tm = &out.structured.table_mappings[0];

    assert!(
        tm.subject_template.is_empty(),
        "a failing override gate must emit no subject"
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "NULLABLE_COL".to_string())));
    assert!(diag_cols(&out, DiagCode::SubjectKeyUnverified).is_empty());
}

#[test]
fn override_primary_key_gate_passes_via_null_fraction_zero() {
    // required=false but null_fraction==0.0 (stats-proven null-free) passes the gate.
    let mut col = ik(2, "PROVEN_COL", 1, 100, false);
    col.stats.null_fraction = Some(0.0);
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![ik(1, "WIDGET_KEY", 1, 100, true), col],
    );
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: Some(vec!["PROVEN_COL".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);
    assert!(out.structured.table_mappings[0]
        .subject_template
        .ends_with("/{PROVEN_COL}"));
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "PROVEN_COL".to_string())])
    );
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn override_primary_key_on_missing_column_is_clean_diagnostic_not_panic() {
    // A nonexistent override column must produce a clean NoSafeSubjectKey (no
    // subject), never a panic.
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![ik(1, "WIDGET_KEY", 1, 100, true)],
    );
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: Some(vec!["DOES_NOT_EXIST".to_string()]),
            class_name: None,
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey)
        .contains(&("DW.DIM_WIDGET".to_string(), "DOES_NOT_EXIST".to_string())));
    // No fabricated subject key column exists in the output.
    assert!(out.structured.table_mappings[0]
        .columns
        .iter()
        .all(|c| !c.is_subject_id));
}

#[test]
fn override_class_name_changes_class_and_slug_only() {
    // DIM_WIDGET derives class "Widget" / slug "widget"; the override forces
    // "Gadget" / "gadget" and changes NOTHING else (columns, subject key column).
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            sc(2, "NAME", FieldType::String),
        ],
    );
    let default_out = emit_r2rml(std::slice::from_ref(&t), &EmitOptions::default());
    let opts = opts_with_override(
        "DW",
        "DIM_WIDGET",
        TableOverride {
            primary_key: None,
            class_name: Some("Gadget".to_string()),
            subject_strategy: None,
        },
    );
    let out = emit_r2rml(&[t], &opts);

    let default_tm = &default_out.structured.table_mappings[0];
    let tm = &out.structured.table_mappings[0];

    // rr:class + subject slug reflect the override.
    assert!(default_tm.class_iri.ends_with("Widget"));
    assert!(tm.class_iri.ends_with("Gadget"), "{}", tm.class_iri);
    assert!(tm.subject_template.contains("/gadget/{WIDGET_KEY}"));
    // ONLY the slug portion of the template changed (subject key column intact).
    assert_eq!(
        default_tm.subject_template.replace("/widget/", "/gadget/"),
        tm.subject_template
    );
    // Predicate-object mappings (predicates derive from column names, not the
    // class) are byte-identical, and the class-name override changes no
    // diagnostics (both runs carry the identifier_field_ids SubjectKeyUnverified).
    assert_eq!(default_tm.columns, tm.columns);
    assert_eq!(out.diagnostics, default_out.diagnostics);
    assert_eq!(
        diag_cols(&out, DiagCode::SubjectKeyUnverified),
        BTreeSet::from([("DW.DIM_WIDGET".to_string(), "WIDGET_KEY".to_string())])
    );
}

#[test]
fn empty_and_unmatched_overrides_are_byte_identical_to_pre_change_golden() {
    // The golden was captured from the pre-override emitter over the 16-table
    // fixture; empty overrides must reproduce it byte-for-byte (no-op safety).
    const GOLDEN: &str = include_str!("testdata/enterprise_default.ttl");
    let default_out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    assert_eq!(
        default_out.turtle, GOLDEN,
        "default (empty-overrides) output must be byte-identical to the pre-change emitter"
    );

    // An override keyed on a table NOT in the input is inert — same bytes.
    let opts = opts_with_override(
        "DW",
        "NOT_A_REAL_TABLE",
        TableOverride {
            primary_key: Some(vec!["X".to_string()]),
            class_name: Some("Y".to_string()),
            subject_strategy: None,
        },
    );
    let unmatched_out = emit_r2rml(&enterprise_dw_tables(), &opts);
    assert_eq!(
        unmatched_out.turtle, GOLDEN,
        "an override on a table not in the request must be a no-op"
    );
    assert_eq!(unmatched_out.diagnostics, default_out.diagnostics);
}

// =============================================================================
// Round-trip through the real loader (turtle feature)
// =============================================================================

#[cfg(feature = "turtle")]
#[test]
fn all_16_tables_round_trip_through_loader() {
    let out = emit_r2rml(&enterprise_dw_tables(), &EmitOptions::default());
    crate::emit::roundtrip_check(&out).expect("round-trip must reproduce the emitted IR");

    // And the compiled mapping has exactly 16 TriplesMaps.
    let compiled = crate::loader::R2rmlLoader::from_turtle(&out.turtle)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(compiled.len(), 16);
}

#[cfg(feature = "turtle")]
#[test]
fn single_table_round_trips() {
    // The simplest increment: DIM_DATE (no FKs) renders and round-trips.
    let tables = enterprise_dw_tables();
    let dim_date = tables.into_iter().find(|t| t.name == "DIM_DATE").unwrap();
    let out = emit_r2rml(&[dim_date], &EmitOptions::default());
    crate::emit::roundtrip_check(&out).expect("DIM_DATE must round-trip");
    assert!(out.turtle.contains("rr:tableName \"DW.DIM_DATE\""));
    assert!(out.turtle.contains("{DATE_KEY}"));
}

#[cfg(feature = "turtle")]
#[test]
fn adversarial_names_escape_and_do_not_inject_predicate_object_maps() {
    // A column name carrying Turtle-significant characters (`"`, `\`, newline)
    // plus a crafted `] ; rr:predicate ... ; rr:column "` injection payload.
    // Unescaped, the bare `"` closes `rr:column` and the tail becomes extra
    // predicate-object maps; escaped, the document still parses and gains none.
    let evil = "E\"VIL\\\n ] ; rr:predicate <http://x/inject> ; rr:column \"X";
    let t = EmitTableSchema {
        namespace: "DW".to_string(),
        name: "DIM_WIDGET".to_string(),
        identifier_field_ids: vec![1],
        columns: vec![
            ik(1, "WIDGET_KEY", 1, 100, true),
            EmitColumn {
                field_id: 2,
                name: evil.to_string(),
                iceberg_type: "string".to_string(),
                field_type: FieldType::String,
                required: false,
                nested: false,
                doc: None,
                stats: EmitColumnStats::default(),
            },
        ],
    };
    let out = emit_r2rml(&[t], &EmitOptions::default());

    // (a) The emitted Turtle compiles through the real loader and the IR
    // reconstructs — proving the escaping is valid and lossless.
    crate::emit::roundtrip_check(&out).expect("adversarial names must round-trip");

    // (b) Exactly ONE TriplesMap, with exactly TWO predicate-object maps (the
    // subject-key literal + the evil column's literal). The crafted name
    // injected no extra map.
    let compiled = crate::loader::R2rmlLoader::from_turtle(&out.turtle)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(compiled.len(), 1, "no extra TriplesMap may be injected");
    let maps = compiled.find_maps_for_table("DW.DIM_WIDGET");
    assert_eq!(maps.len(), 1);
    assert_eq!(
        maps[0].predicate_object_maps.len(),
        2,
        "the crafted column name must not inject extra predicate-object maps"
    );

    // (c) The evil name survives verbatim in the IR (rendered escaped, parsed
    // back intact) — no lossy mangling.
    let tm = &out.structured.table_mappings[0];
    assert!(tm.columns.iter().any(|c| c.column_name == evil));
}

#[cfg(feature = "turtle")]
#[test]
fn hostile_base_namespace_is_escaped_in_header_and_cannot_inject() {
    // `base_namespace` reaches the `@prefix v: <…>` header, which bypasses
    // curie(). A crafted newline + `@prefix`/triple must not inject real Turtle.
    let hostile =
        "http://ns.example/x\n@prefix evil: <http://evil/> .\n<#Inj> a <http://evil/T> .\n#";
    let opts = EmitOptions {
        base_namespace: hostile.to_string(),
        ..EmitOptions::default()
    };
    let t = tbl(
        "DIM_WIDGET",
        vec![1],
        vec![ik(1, "WIDGET_KEY", 1, 100, true)],
    );
    let out = emit_r2rml(&[t], &opts);

    // Still compiles and round-trips to exactly ONE TriplesMap — the injected
    // statements did not parse as real triples.
    crate::emit::roundtrip_check(&out).expect("hostile base_namespace must still round-trip");
    let compiled = crate::loader::R2rmlLoader::from_turtle(&out.turtle)
        .unwrap()
        .compile()
        .unwrap();
    assert_eq!(compiled.len(), 1, "no injected TriplesMap");

    // The injection vector is a RAW newline that ends the @prefix line and starts
    // a new directive/triple. Escaping turns every base newline into a \uXXXX
    // (header IRI) or \n (rr:template) escape — neither a real 0x0A — so the
    // crafted payload can never reach statement position.
    assert!(
        !out.turtle.contains("\n@prefix evil"),
        "raw-newline prefix injection: {}",
        out.turtle
    );
    assert!(
        !out.turtle.contains("\n<#Inj>"),
        "raw-newline triple injection: {}",
        out.turtle
    );
}

// =============================================================================
// hex-not-base64 regression guard (rule 2)
// =============================================================================

#[test]
fn bytes_datatype_is_hexbinary_coupled_to_materializer_output() {
    use std::sync::Arc;

    use fluree_db_tabular::{BatchSchema, Column, ColumnBatch, FieldInfo};

    use crate::emit::naming::xsd_datatype;
    use crate::mapping::ObjectMap;
    use crate::materialize::materialize_object_from_batch;

    // The emitter's choice for bytes.
    assert_eq!(xsd_datatype(FieldType::Bytes, true), Some("xsd:hexBinary"));

    // The materializer's ACTUAL lexical output for a bytes column. If anyone
    // "fixes" term.rs::base64_encode into real base64, this becomes "3q2+7w=="
    // and the assertion fails — loudly coupling the datatype choice to reality.
    let schema = Arc::new(BatchSchema::new(vec![FieldInfo {
        name: "PAYLOAD".to_string(),
        field_type: FieldType::Bytes,
        nullable: true,
        field_id: 1,
    }]));
    let batch = ColumnBatch::new(
        schema,
        vec![Column::Bytes(vec![Some(vec![0xde, 0xad, 0xbe, 0xef])])],
    )
    .unwrap();
    let om = ObjectMap::column_typed("PAYLOAD", "http://www.w3.org/2001/XMLSchema#hexBinary");
    let term = materialize_object_from_batch(&om, &batch, 0)
        .unwrap()
        .unwrap();
    match term {
        crate::materialize::RdfTerm::Literal { value, .. } => {
            assert_eq!(value, "deadbeef", "bytes must materialize as lowercase hex");
        }
        other => panic!("expected literal, got {other:?}"),
    }
}
