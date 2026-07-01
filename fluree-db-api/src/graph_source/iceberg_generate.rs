//! End-to-end deterministic R2RML generation (PR-1 item (c), the capstone).
//!
//! This module glues the two halves already on the branch: track (a)'s
//! metadata-only [`preview_iceberg_table`] API (the INPUT) and the deterministic
//! [`emit_r2rml`] engine (the ENGINE). For each requested table it fetches the
//! Tier-A schema + Tier-B stats preview, maps that preview onto the emitter's
//! self-contained input model ([`EmitTableSchema`] / [`EmitColumn`] /
//! [`EmitColumnStats`]) — closing the `TODO(track-a)` stand-in in
//! `fluree-db-r2rml`'s `emit::input` — runs the emitter with the caller's
//! `per_table_overrides`, and returns the authoritative
//! [`StructuredR2rmlMapping`] IR + Turtle + diagnostics + the pinned snapshot.
//!
//! **Metadata-only**: preview reads REST `loadTable` + Avro manifests, never a
//! Parquet/data file; generation adds no I/O beyond those previews. No graph
//! source is created — this is the read-semantics surface solo's PR-2 lambda
//! calls to obtain a mapping for review before saving.
//!
//! The fetch (async, network) is deliberately separable from the map+emit core
//! ([`assemble_generate_response`], pure) so the mapping/emission is unit-tested
//! offline against synthesized [`TablePreview`] fixtures.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use fluree_db_iceberg::FieldType;
use fluree_db_r2rml::emit::{
    emit_r2rml, EmitColumn, EmitColumnStats, EmitOptions, EmitOutput, EmitTableSchema, TableKey,
    TypedBound,
};

use crate::graph_source::config::IcebergConnectionConfig;
use crate::graph_source::iceberg_catalog::{
    preview_iceberg_table, ColumnInfo, ColumnStats, SnapshotRef, StatsTier, TableIdentifier,
    TablePreview,
};
use crate::Result;

// Re-export the emitter's wire types that appear in this module's public API, so
// callers (and the HTTP route) can name them straight off `fluree_db_api`.
pub use fluree_db_r2rml::emit::{Diagnostic, StructuredR2rmlMapping, TableOverride};

// =============================================================================
// Request / response
// =============================================================================

/// The emit knobs a generate call may tune. Every field is a pure, deterministic
/// switch — identical knobs + identical (pinned) metadata yield byte-identical
/// output. The subject/vocab IRI bases are NOT here: they are derived from
/// [`GenerateR2rmlRequest::base_namespace`] (see `emit::naming::subject_base`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GenerateOptions {
    /// Emit `xsd:integer` for `Int32`/`Int64` (the `enterprise.ttl` convention);
    /// when `false`, use `xsd:int` / `xsd:long`. Defaults to `true`.
    #[serde(default = "default_true")]
    pub xsd_long_as_integer: bool,
    /// Emit `rr:parentTriplesMap` joins for deterministically-resolved FKs.
    /// Defaults to `true`.
    #[serde(default = "default_true")]
    pub emit_fk_joins: bool,
    /// Keep resolved-FK key columns as literal predicate-object maps too
    /// (pushdown-friendly). Defaults to `true`.
    #[serde(default = "default_true")]
    pub keep_fk_keys_as_literals: bool,
}

fn default_true() -> bool {
    true
}

impl Default for GenerateOptions {
    fn default() -> Self {
        Self {
            xsd_long_as_integer: true,
            emit_fk_joins: true,
            keep_fk_keys_as_literals: true,
        }
    }
}

/// A deterministic R2RML generation request over one pinned snapshot.
///
/// Constructed by the caller (solo's lambda / the HTTP route) rather than
/// deserialized directly: it holds a live [`IcebergConnectionConfig`] and a
/// `HashMap` keyed by [`TableIdentifier`] (JSON object keys must be strings), so
/// the wire form (a flat connection + an overrides list) is adapted by the route.
#[derive(Debug, Clone)]
pub struct GenerateR2rmlRequest {
    /// The (possibly unsaved) Iceberg connection to preview against.
    pub connection: IcebergConnectionConfig,
    /// The tables to map, in output order (multi-table per Dataset).
    pub tables: Vec<TableIdentifier>,
    /// The SINGLE base namespace all IRIs derive from (== the emitted
    /// `StructuredR2rmlMapping.baseNamespace`).
    pub base_namespace: String,
    /// Per-table subject-key / class-name overrides, keyed by table identity.
    pub per_table_overrides: HashMap<TableIdentifier, TableOverride>,
    /// The emit knobs.
    pub options: GenerateOptions,
    /// RESERVED for PR-4 (target-model IRI rewrite): accepted and ignored here.
    pub target_model_ledger_id: Option<String>,
}

/// The deterministic generation result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateR2rmlResponse {
    /// R2RML Turtle, rendered from `structured`; compiles through `R2rmlLoader`.
    pub turtle: String,
    /// The AUTHORITATIVE wire IR (solo's camelCase `StructuredR2rmlMapping`) — the
    /// object PR-2 persists, PR-3 reviews, PR-4 rewrites. NOT `Vec<TriplesMap>`.
    pub structured: StructuredR2rmlMapping,
    /// Every decision the emitter could not make from metadata alone.
    pub diagnostics: Vec<Diagnostic>,
    /// A REPRESENTATIVE snapshot: the first requested table's current snapshot at
    /// generation time. Iceberg snapshots are per-table (there is no catalog-wide
    /// snapshot), so this is NOT a true multi-table pin — it is threaded through as a
    /// single coherent `snapshotId` (solo persists one per Dataset), not a guarantee
    /// that every mapped table was read at this snapshot.
    pub snapshot_id: SnapshotRef,
}

// =============================================================================
// Preview → emitter-input mapping (the closed `TODO(track-a)`) — pure
// =============================================================================

/// Map one column's JSON `min`/`max` bound to a comparable [`TypedBound`].
///
/// FK candidacy is integer-only (surrogate `*_KEY` spaces), so only JSON integer
/// bounds become `TypedBound::Int`; every non-integer bound (float, string date,
/// absent, or out-of-`i64`-range) becomes `None` — an unbounded column the
/// range-containment FK check simply cannot confirm a join for.
fn json_to_typed_bound(value: Option<&serde_json::Value>) -> Option<TypedBound> {
    value.and_then(serde_json::Value::as_i64).map(TypedBound::Int)
}

/// Map Tier-B [`ColumnStats`] onto the emitter's [`EmitColumnStats`].
///
/// Only the three signals the deterministic heuristic consumes are carried:
/// `null_fraction` (the NOT-NULL gate) and typed integer `min`/`max` (FK
/// range-containment). NDV is deliberately absent (unavailable metadata-only).
fn map_stats(stats: &ColumnStats) -> EmitColumnStats {
    EmitColumnStats {
        null_fraction: stats.null_fraction,
        min: json_to_typed_bound(stats.min.as_ref()),
        max: json_to_typed_bound(stats.max.as_ref()),
    }
}

/// Map a preview [`ColumnInfo`] onto an emitter [`EmitColumn`].
///
/// A column the emitter must pass over — an actual nested struct/list/map, or one
/// whose Iceberg type did not parse to a scalar `FieldType` — is marked
/// `nested = true` (the only skip lever the emitter exposes: both Phase 1 and
/// Phase 2 `continue` past a nested column). For such a skipped column the
/// concrete `field_type` is never read, so a `String` placeholder is harmless.
fn map_column(col: &ColumnInfo) -> EmitColumn {
    let skip = col.nested || col.field_type.is_none();
    EmitColumn {
        field_id: col.field_id,
        name: col.name.clone(),
        iceberg_type: col.iceberg_type.clone(),
        field_type: col.field_type.unwrap_or(FieldType::String),
        required: col.required,
        nested: skip,
        doc: col.doc.clone(),
        stats: col.stats.as_ref().map(map_stats).unwrap_or_default(),
    }
}

/// Map a full [`TablePreview`] (schema + stats) onto one emitter input unit.
///
/// The `{namespace, name}` identity comes from the request's [`TableIdentifier`]
/// (the preview's `schema.table` is only the pre-joined `"NS.NAME"` string);
/// columns are carried in `field_id` order (the emitter's deterministic emission
/// order), and `identifier_field_ids` (Iceberg's PK hint) passes straight through.
fn preview_to_emit_schema(table: &TableIdentifier, preview: &TablePreview) -> EmitTableSchema {
    EmitTableSchema {
        namespace: table.namespace.clone(),
        name: table.name.clone(),
        columns: preview.schema.columns.iter().map(map_column).collect(),
        identifier_field_ids: preview.schema.identifier_field_ids.clone(),
    }
}

/// Build the emitter [`EmitOptions`] from the request. The caller's overrides —
/// keyed by [`TableIdentifier`] — are re-keyed onto the emitter's [`TableKey`]
/// (the same `{namespace, name}` identity the emitter iterates); the base
/// namespace + emit knobs come from the request, the remaining IRI-base defaults
/// from [`EmitOptions::new`].
fn build_emit_options(req: &GenerateR2rmlRequest) -> EmitOptions {
    let per_table_overrides = req
        .per_table_overrides
        .iter()
        .map(|(id, ov)| (TableKey::new(id.namespace.clone(), id.name.clone()), ov.clone()))
        .collect();

    EmitOptions {
        xsd_long_as_integer: req.options.xsd_long_as_integer,
        emit_fk_joins: req.options.emit_fk_joins,
        keep_fk_keys_as_literals: req.options.keep_fk_keys_as_literals,
        per_table_overrides,
        ..EmitOptions::new(&req.base_namespace)
    }
}

/// Map every fetched preview onto emitter input and run the deterministic
/// emitter. Pure — no I/O — so it is exercised offline over synthetic previews.
fn emit_from_previews(
    previews: &[(TableIdentifier, TablePreview)],
    req: &GenerateR2rmlRequest,
) -> EmitOutput {
    let tables: Vec<EmitTableSchema> = previews
        .iter()
        .map(|(id, preview)| preview_to_emit_schema(id, preview))
        .collect();
    emit_r2rml(&tables, &build_emit_options(req))
}

/// Assemble the response from the fetched previews: pin the snapshot, run the
/// map+emit core, and package `{turtle, structured, diagnostics, snapshot_id}`.
/// Pure (no network) so the whole non-fetch path is unit-tested offline.
///
/// **Snapshot pinning.** Iceberg snapshots are per-table (there is no
/// catalog-wide snapshot), so the returned `snapshot_id` is the first requested
/// table's current snapshot — captured once and threaded through as the coherent
/// pin for the whole generate (solo persists a single `snapshotId` per Dataset).
fn assemble_generate_response(
    previews: &[(TableIdentifier, TablePreview)],
    req: &GenerateR2rmlRequest,
) -> Result<GenerateR2rmlResponse> {
    let snapshot_id = previews
        .first()
        .map(|(_, preview)| preview.schema.snapshot.clone())
        .ok_or_else(|| crate::ApiError::config("generate_r2rml requires at least one table"))?;

    let output = emit_from_previews(previews, req);

    Ok(GenerateR2rmlResponse {
        turtle: output.turtle,
        structured: output.structured,
        diagnostics: output.diagnostics,
        snapshot_id,
    })
}

// =============================================================================
// Fluree surface
// =============================================================================

impl crate::Fluree {
    /// Deterministically generate an R2RML mapping over a set of Iceberg tables.
    ///
    /// For each requested table it fetches the Tier-A+B [`preview_iceberg_table`]
    /// at the table's current snapshot, maps the preview onto the emitter's input
    /// model, runs the deterministic emitter with the request's
    /// `per_table_overrides`, and returns the authoritative
    /// [`StructuredR2rmlMapping`] + Turtle + diagnostics + the pinned snapshot.
    ///
    /// **Metadata-only and side-effect-free**: no Parquet scan, no graph source
    /// created. `target_model_ledger_id` is RESERVED for PR-4 and ignored here.
    pub async fn generate_r2rml(
        &self,
        req: GenerateR2rmlRequest,
    ) -> Result<GenerateR2rmlResponse> {
        if req.tables.is_empty() {
            return Err(crate::ApiError::config(
                "generate_r2rml requires at least one table",
            ));
        }

        // Fetch each requested table's Tier-A+B preview (metadata-only). Pinning:
        // the response snapshot is taken from the first table (see
        // `assemble_generate_response`); each preview reads its table's current
        // snapshot at fetch time.
        let mut previews: Vec<(TableIdentifier, TablePreview)> =
            Vec::with_capacity(req.tables.len());
        for table in &req.tables {
            let preview =
                preview_iceberg_table(req.connection.clone(), table.clone(), StatsTier::Stats)
                    .await?;
            previews.push((table.clone(), preview));
        }

        assemble_generate_response(&previews, &req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::graph_source::iceberg_catalog::{StatsCompleteness, TableSchema};

    // ---- synthetic preview fixtures (reuse track (a)'s shapes) ----

    fn snapshot() -> SnapshotRef {
        SnapshotRef {
            id: 4242,
            timestamp_ms: 1_700_000_000_000,
            schema_id: Some(0),
        }
    }

    /// An integer column carrying `[min, max]` integer bounds (a PK/FK surrogate).
    fn int_col(field_id: i32, name: &str, required: bool, min: i64, max: i64) -> ColumnInfo {
        ColumnInfo {
            field_id,
            name: name.to_string(),
            iceberg_type: "long".to_string(),
            field_type: Some(FieldType::Int64),
            xsd_type: Some("xsd:integer".to_string()),
            required,
            nested: false,
            doc: None,
            stats: Some(ColumnStats {
                null_count: None,
                value_count: None,
                null_fraction: if required { Some(0.0) } else { None },
                nan_count: None,
                min: Some(serde_json::json!(min)),
                max: Some(serde_json::json!(max)),
                on_disk_bytes: None,
                distinct_count: None,
            }),
        }
    }

    /// A scalar column of `field_type`, no stats.
    fn scalar_col(field_id: i32, name: &str, field_type: FieldType) -> ColumnInfo {
        ColumnInfo {
            field_id,
            name: name.to_string(),
            iceberg_type: "x".to_string(),
            field_type: Some(field_type),
            xsd_type: None,
            required: false,
            nested: false,
            doc: None,
            stats: None,
        }
    }

    /// A nested (struct/list/map) column: `field_type = None`, `nested = true`.
    fn nested_col(field_id: i32, name: &str) -> ColumnInfo {
        ColumnInfo {
            field_id,
            name: name.to_string(),
            iceberg_type: "struct".to_string(),
            field_type: None,
            xsd_type: None,
            required: false,
            nested: true,
            doc: None,
            stats: None,
        }
    }

    fn preview(ns: &str, name: &str, ident: Vec<i32>, columns: Vec<ColumnInfo>) -> (TableIdentifier, TablePreview) {
        let schema = TableSchema {
            table: format!("{ns}.{name}"),
            table_uuid: None,
            format_version: 2,
            current_schema_id: 0,
            snapshot: snapshot(),
            row_count: Some(1000),
            data_file_count: Some(4),
            total_bytes: Some(4096),
            identifier_field_ids: ident,
            partition_spec: Vec::new(),
            sort_order: Vec::new(),
            properties: HashMap::new(),
            columns,
            snapshot_log: Vec::new(),
        };
        (
            TableIdentifier::new(ns, name),
            TablePreview {
                schema,
                stats_completeness: StatsCompleteness {
                    tier: "stats".to_string(),
                    manifests_read: 1,
                    had_column_bounds: true,
                },
                warnings: Vec::new(),
            },
        )
    }

    fn base_req(
        tables: Vec<TableIdentifier>,
        overrides: HashMap<TableIdentifier, TableOverride>,
    ) -> GenerateR2rmlRequest {
        GenerateR2rmlRequest {
            // A dummy connection — the offline core never touches it.
            connection: IcebergConnectionConfig::direct("s3://unused/warehouse/ns/table"),
            tables,
            base_namespace: "http://ns.fluree.dev/edw#".to_string(),
            per_table_overrides: overrides,
            options: GenerateOptions::default(),
            target_model_ledger_id: None,
        }
    }

    // ---- min/max JSON → TypedBound ----

    #[test]
    fn json_bounds_map_integers_only() {
        assert_eq!(
            json_to_typed_bound(Some(&serde_json::json!(42))),
            Some(TypedBound::Int(42))
        );
        assert_eq!(
            json_to_typed_bound(Some(&serde_json::json!(-7))),
            Some(TypedBound::Int(-7))
        );
        // Non-integer bounds (float, string date) are not FK-usable → None.
        assert_eq!(json_to_typed_bound(Some(&serde_json::json!(3.5))), None);
        assert_eq!(
            json_to_typed_bound(Some(&serde_json::json!("2021-01-01"))),
            None
        );
        assert_eq!(json_to_typed_bound(None), None);
    }

    // ---- preview → EmitTableSchema mapping ----

    #[test]
    fn mapping_preserves_order_types_required_nested_skip_and_stats() {
        let (id, pv) = preview(
            "DW",
            "DIM_WIDGET",
            vec![1],
            vec![
                int_col(1, "WIDGET_KEY", true, 1, 100),
                scalar_col(2, "NAME", FieldType::String),
                nested_col(3, "META"),
                scalar_col(4, "BAD_TYPE", FieldType::String), // will be forced field_type=None below
                int_col(5, "OTHER_KEY", false, 3, 40),
            ],
        );
        // Force a non-nested column whose Iceberg type did not parse (field_type
        // None) — it must still pass through and be skipped like a nested column.
        let mut pv = pv;
        pv.schema.columns[3].field_type = None;
        pv.schema.columns[3].nested = false;

        let emit = preview_to_emit_schema(&id, &pv);

        assert_eq!(emit.namespace, "DW");
        assert_eq!(emit.name, "DIM_WIDGET");
        assert_eq!(emit.identifier_field_ids, vec![1]);

        // field_id order preserved 1:1.
        let ids: Vec<i32> = emit.columns.iter().map(|c| c.field_id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);

        // Types + required carried.
        assert_eq!(emit.columns[0].field_type, FieldType::Int64);
        assert!(emit.columns[0].required);
        assert_eq!(emit.columns[1].field_type, FieldType::String);
        assert!(!emit.columns[1].required);

        // Nested struct AND unparsed-type column both become skip (nested=true);
        // the unparsed column gets a harmless String placeholder type.
        assert!(emit.columns[2].nested, "struct column must be skipped");
        assert!(
            emit.columns[3].nested,
            "field_type=None column must be skipped"
        );
        assert_eq!(emit.columns[3].field_type, FieldType::String);

        // Stats: integer bounds → TypedBound::Int; null_fraction carried.
        assert_eq!(emit.columns[0].stats.min, Some(TypedBound::Int(1)));
        assert_eq!(emit.columns[0].stats.max, Some(TypedBound::Int(100)));
        assert_eq!(emit.columns[0].stats.null_fraction, Some(0.0));
        assert_eq!(emit.columns[4].stats.min, Some(TypedBound::Int(3)));
        assert_eq!(emit.columns[4].stats.max, Some(TypedBound::Int(40)));
        // A stats-free column maps to default (bounds-free) stats.
        assert_eq!(emit.columns[1].stats.min, None);
        assert_eq!(emit.columns[1].stats.null_fraction, None);
    }

    #[test]
    fn snapshot_is_pinned_from_first_table() {
        let previews = vec![
            preview("DW", "DIM_A", vec![1], vec![int_col(1, "A_KEY", true, 1, 100)]),
            preview("DW", "DIM_B", vec![1], vec![int_col(1, "B_KEY", true, 1, 100)]),
        ];
        let ids: Vec<TableIdentifier> = previews.iter().map(|(id, _)| id.clone()).collect();
        let resp = assemble_generate_response(&previews, &base_req(ids, HashMap::new())).unwrap();
        assert_eq!(resp.snapshot_id.id, 4242);
        assert_eq!(resp.snapshot_id.timestamp_ms, 1_700_000_000_000);
    }

    // ---- overrides flow through ----

    #[test]
    fn per_table_override_changes_subject_key_and_flags_unverified() {
        // identifier_field_ids=[1] (WIDGET_KEY); override REPLACES it with ALT_KEY.
        let (id, pv) = preview(
            "DW",
            "DIM_WIDGET",
            vec![1],
            vec![
                int_col(1, "WIDGET_KEY", true, 1, 100),
                int_col(2, "ALT_KEY", true, 1, 100),
                scalar_col(3, "NAME", FieldType::String),
            ],
        );
        let mut overrides = HashMap::new();
        overrides.insert(
            id.clone(),
            TableOverride {
                primary_key: Some("ALT_KEY".to_string()),
                class_name: None,
            },
        );
        let req = base_req(vec![id.clone()], overrides);
        let resp = assemble_generate_response(&[(id, pv)], &req).unwrap();

        let tm = &resp.structured.table_mappings[0];
        assert!(
            tm.subject_template.ends_with("/{ALT_KEY}"),
            "subject must key on the override column, got {}",
            tm.subject_template
        );
        assert!(!tm.subject_template.contains("{WIDGET_KEY}"));

        // An override PK ALWAYS earns SubjectKeyUnverified (uniqueness is
        // unprovable metadata-only), on the override column.
        use fluree_db_r2rml::emit::DiagCode;
        let unverified: Vec<&Diagnostic> = resp
            .diagnostics
            .iter()
            .filter(|d| d.code == DiagCode::SubjectKeyUnverified)
            .collect();
        assert_eq!(unverified.len(), 1);
        assert_eq!(unverified[0].column.as_deref(), Some("ALT_KEY"));
        assert_eq!(unverified[0].table.as_deref(), Some("DW.DIM_WIDGET"));
    }

    // ---- generated turtle round-trips + FK resolves ----

    #[test]
    fn generated_turtle_round_trips_through_loader_and_resolves_fk() {
        use fluree_db_r2rml::R2rmlLoader;

        // DIM_GEOGRAPHY (PK GEOGRAPHY_KEY) + DIM_SUPPLIER (FK GEOGRAPHY_KEY) — the
        // FK must resolve by name ∧ type ∧ range-containment.
        let previews = vec![
            preview(
                "DW",
                "DIM_GEOGRAPHY",
                vec![1],
                vec![
                    int_col(1, "GEOGRAPHY_KEY", true, 1, 100_000),
                    scalar_col(2, "COUNTRY", FieldType::String),
                ],
            ),
            preview(
                "DW",
                "DIM_SUPPLIER",
                vec![1],
                vec![
                    int_col(1, "SUPPLIER_KEY", true, 1, 100_000),
                    scalar_col(2, "SUPPLIER_NAME", FieldType::String),
                    int_col(3, "GEOGRAPHY_KEY", false, 1, 100_000),
                ],
            ),
        ];
        let ids: Vec<TableIdentifier> = previews.iter().map(|(id, _)| id.clone()).collect();
        let resp = assemble_generate_response(&previews, &base_req(ids, HashMap::new())).unwrap();

        // The wire IR carries both tables.
        assert_eq!(resp.structured.table_mappings.len(), 2);

        // The FK resolved as a join (DIM_SUPPLIER.GEOGRAPHY_KEY → DIM_GEOGRAPHY).
        let supplier = resp
            .structured
            .table_mapping("DW.DIM_SUPPLIER")
            .expect("supplier mapping");
        let fk = supplier
            .columns
            .iter()
            .find_map(|c| c.foreign_key.as_ref())
            .expect("GEOGRAPHY_KEY must resolve to a join");
        assert_eq!(fk.target_table, "DW.DIM_GEOGRAPHY");
        assert_eq!(fk.child_column, "GEOGRAPHY_KEY");
        assert_eq!(fk.parent_column, "GEOGRAPHY_KEY");

        // The rendered Turtle compiles through the real loader to the same table
        // count (the internal round-trip artifact — never on the wire).
        let compiled = R2rmlLoader::from_turtle(&resp.turtle)
            .expect("emitted turtle must parse")
            .compile()
            .expect("emitted turtle must compile");
        assert_eq!(compiled.len(), resp.structured.table_mappings.len());
    }

    // ---- wire shape: camelCase StructuredR2rmlMapping, not Vec<TriplesMap> ----

    #[test]
    fn response_structured_is_camelcase_structured_mapping() {
        let previews = vec![preview(
            "DW",
            "DIM_DATE",
            vec![1],
            vec![int_col(1, "DATE_KEY", true, 1, 100)],
        )];
        let ids: Vec<TableIdentifier> = previews.iter().map(|(id, _)| id.clone()).collect();
        let resp = assemble_generate_response(&previews, &base_req(ids, HashMap::new())).unwrap();

        let json = serde_json::to_value(&resp).unwrap();
        // `structured` is solo's camelCase IR (an object), not a `Vec<TriplesMap>`.
        assert!(json["structured"]["baseNamespace"].is_string());
        assert!(json["structured"]["tableMappings"].is_array());
        assert!(!json["structured"].is_array());
        // The pinned snapshot rides along.
        assert_eq!(json["snapshot_id"]["id"], serde_json::json!(4242));
        assert!(json["turtle"].is_string());
    }

    // ---- empty-tables guard ----

    #[test]
    fn empty_tables_is_a_clean_error() {
        let err = assemble_generate_response(&[], &base_req(Vec::new(), HashMap::new()))
            .expect_err("no tables must error, not panic");
        assert!(err.to_string().contains("at least one table"));
    }
}
