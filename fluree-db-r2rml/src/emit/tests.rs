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
use crate::emit::{emit_r2rml, EmitOptions, EmitOutput};

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
    // No NoSafeSubjectKey / SubjectKeyUnverified for a clean identifier hint.
    assert!(diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
    assert!(diag_cols(&out, DiagCode::SubjectKeyUnverified).is_empty());
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
fn subject_key_fallback_rejects_nullable_key() {
    // A name-matching key that is nullable fails the gate → NoSafeSubjectKey.
    let t = tbl(
        "DIM_WIDGET",
        vec![],
        vec![
            ik(1, "WIDGET_KEY", 1, 100, false),
            sc(2, "NAME", FieldType::String),
        ],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
    assert!(out.structured.table_mappings[0].subject_template.is_empty());
    assert!(!diag_cols(&out, DiagCode::NoSafeSubjectKey).is_empty());
}

#[test]
fn no_safe_subject_key_emits_no_subject_and_never_invents_one() {
    let t = tbl(
        "WEIRD",
        vec![],
        vec![sc(1, "A", FieldType::String), sc(2, "B", FieldType::Int64)],
    );
    let out = emit_r2rml(&[t], &EmitOptions::default());
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
fn fk_range_out_of_bounds_rejected() {
    // Name + type match, but the child range ⊄ parent range → no join.
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
        !resolved_fks(&out)
            .iter()
            .any(|(ct, cc, _, _)| ct == "DW.FACT_CHILD" && cc == "PARENT_KEY"),
        "range-incompatible FK must not resolve"
    );
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
