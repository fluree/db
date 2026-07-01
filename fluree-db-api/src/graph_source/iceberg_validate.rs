//! R2RML **validate / dry-run**: compile an R2RML Turtle mapping through the real
//! loader and cross-check its table / column / join references — plus subject-key
//! **nullability** — against the live Iceberg schema + stats (track (a)'s preview).
//!
//! This is item **(d)** of PR-1. It is strictly **read-only**: it creates no graph
//! source, writes nothing to CAS, and publishes nothing. It returns structured
//! [`Diagnostic`]s (reusing the emitter's wire contract) describing every defect it
//! found, so an operator can fix a hand-written or generated mapping before saving.
//!
//! # What is checked
//!
//! For a mapping that compiles, each [`TriplesMap`] is cross-checked against the
//! metadata preview of its logical table (`tier = Stats`):
//!
//! * **table existence** — the logical table must resolve in the catalog, else
//!   [`DiagCode::TableNotFound`].
//! * **column existence + casing** — every referenced column (POM `rr:column`s and
//!   object templates, subject `template_columns`, and join `child`/`parent`
//!   columns) must exist with **exact** casing: missing ⇒ [`DiagCode::ColumnNotFound`];
//!   present but case-differing ⇒ [`DiagCode::CasingMismatch`] (Iceberg field names
//!   are case-sensitive).
//! * **join key type compatibility** — a join's child-column `field_type` must equal
//!   the parent table's parent-column `field_type`, else [`DiagCode::JoinTypeMismatch`].
//! * **subject-key nullability** — every subject-key column must be `required` OR
//!   have `null_fraction == 0` per stats, else [`DiagCode::NoSafeSubjectKey`] (a
//!   nullable key silently drops rows). **Uniqueness is deliberately NOT checked**
//!   — NDV requires column profiling and is deferred to PR-5.
//!
//! # Testability
//!
//! The catalog preview is separated from the pure cross-check: the async
//! [`crate::Fluree::validate_r2rml`] resolves each table to a [`TableSchema`] via
//! the preview, then the pure [`cross_check_mapping`] does all diagnostic logic over
//! an injected `schemas` map. Unit tests drive `cross_check_mapping` directly with
//! synthesized schemas/stats — no network, no catalog.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use fluree_db_r2rml::emit::{DiagCode, Diagnostic, Severity};
use fluree_db_r2rml::loader::R2rmlLoader;
use fluree_db_r2rml::mapping::{CompiledR2rmlMapping, ObjectMap, TriplesMap};

use super::config::IcebergConnectionConfig;
use super::iceberg_catalog::{
    preview_iceberg_table, ColumnInfo, StatsTier, TableIdentifier, TableSchema,
};
use crate::Result;

/// The structured result of a `validate_r2rml` dry-run.
///
/// `compiled_ok == false` is the authoritative signal that the Turtle failed to
/// compile; in that case `diagnostics` carries a single error diagnostic with the
/// compile message and the cross-check is skipped (`triples_map_count == 0`,
/// `table_names` empty).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateR2rmlResponse {
    /// Whether the mapping compiled through `R2rmlLoader::from_turtle().compile()`.
    pub compiled_ok: bool,
    /// Number of `rr:TriplesMap` definitions in the mapping (0 if it did not compile).
    pub triples_map_count: usize,
    /// Distinct logical table names referenced by the mapping (sorted).
    pub table_names: Vec<String>,
    /// Structured diagnostics — every cross-check defect, plus any compile error.
    pub diagnostics: Vec<Diagnostic>,
}

/// Split an R2RML logical table name (`"NAMESPACE.NAME"`, dot-normalized) into a
/// catalog [`TableIdentifier`]. Namespaces may themselves contain dots, so the
/// **last** dot separates the namespace from the table name (mirrors the catalog's
/// own `split_qualified_table`). A name with no dot becomes an empty-namespace
/// identifier, which the catalog will reject — surfacing as `TableNotFound`.
fn table_identifier_from_name(name: &str) -> TableIdentifier {
    match name.rsplit_once('.') {
        Some((namespace, table)) => TableIdentifier::new(namespace, table),
        None => TableIdentifier::new("", name),
    }
}

/// Diagnostic for a mapping that failed to compile. Uses the dedicated
/// [`DiagCode::CompileError`] (with no table — the mapping resolved zero tables) so a
/// compile failure never collides on the wire with a `TableNotFound` cross-check
/// diagnostic; `compiled_ok == false` remains the authoritative machine signal.
fn compile_error_diagnostic(message: &str) -> Diagnostic {
    Diagnostic {
        severity: Severity::Error,
        code: DiagCode::CompileError,
        table: None,
        column: None,
        message: format!("R2RML mapping failed to compile: {message}"),
    }
}

/// How a referenced column resolves against a live schema.
enum ColumnResolution<'a> {
    /// Exact byte-for-byte name match.
    Exact(&'a ColumnInfo),
    /// Matches only case-insensitively (the schema spells it differently).
    Casing(&'a ColumnInfo),
    /// No column matches, even case-insensitively.
    Missing,
}

fn resolve_column<'a>(schema: &'a TableSchema, name: &str) -> ColumnResolution<'a> {
    if let Some(ci) = schema.columns.iter().find(|c| c.name == name) {
        ColumnResolution::Exact(ci)
    } else if let Some(ci) = schema
        .columns
        .iter()
        .find(|c| c.name.eq_ignore_ascii_case(name))
    {
        ColumnResolution::Casing(ci)
    } else {
        ColumnResolution::Missing
    }
}

/// Resolve a referenced column, pushing a [`DiagCode::CasingMismatch`] (warning) or
/// [`DiagCode::ColumnNotFound`] (error) as appropriate. Returns the resolved
/// [`ColumnInfo`] (exact or case-corrected) for downstream type / nullability
/// checks, or `None` when the column does not exist at all.
fn check_column<'a>(
    schema: &'a TableSchema,
    table: &str,
    name: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<&'a ColumnInfo> {
    match resolve_column(schema, name) {
        ColumnResolution::Exact(ci) => Some(ci),
        ColumnResolution::Casing(ci) => {
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::CasingMismatch,
                table.to_string(),
                Some(name.to_string()),
                format!(
                    "Column '{name}' does not match the live schema's casing; the schema spells it \
                     '{}'. Iceberg field names are case-sensitive — fix the casing to match.",
                    ci.name
                ),
            ));
            Some(ci)
        }
        ColumnResolution::Missing => {
            diagnostics.push(Diagnostic::new(
                Severity::Error,
                DiagCode::ColumnNotFound,
                table.to_string(),
                Some(name.to_string()),
                format!(
                    "Column '{name}' referenced by the mapping does not exist in table '{table}'."
                ),
            ));
            None
        }
    }
}

/// Pure cross-check of a compiled mapping against resolved live schemas.
///
/// `schemas` maps each **R2RML logical table name** to its [`TableSchema`] (only for
/// tables the preview resolved). A mapping table absent from `schemas` yields
/// [`DiagCode::TableNotFound`]; `load_errors` (table name → preview error string,
/// empty in unit tests) enriches that message when the absence was a preview
/// failure rather than a genuine miss.
///
/// This function performs **no I/O** — it is the whole diagnostic surface and is
/// driven directly by unit tests with synthesized schemas.
fn cross_check_mapping(
    compiled: &CompiledR2rmlMapping,
    schemas: &HashMap<String, TableSchema>,
    load_errors: &HashMap<String, String>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    // Deterministic order: iterate TriplesMaps sorted by IRI so diagnostics are stable.
    let mut tms: Vec<&TriplesMap> = compiled.triples_maps.values().collect();
    tms.sort_by(|a, b| a.iri.cmp(&b.iri));

    for tm in tms {
        let Some(table) = tm.table_name() else {
            continue; // logical table is always a table name today; skip defensively.
        };

        let Some(schema) = schemas.get(table) else {
            let detail = load_errors
                .get(table)
                .map(|e| format!(" (catalog preview failed: {e})"))
                .unwrap_or_default();
            diagnostics.push(Diagnostic::new(
                Severity::Error,
                DiagCode::TableNotFound,
                table.to_string(),
                None,
                format!(
                    "Table '{table}' referenced by TriplesMap <{}> was not found in the live \
                     catalog schema{detail}.",
                    tm.iri
                ),
            ));
            continue; // no schema → can't check columns/joins/keys for this table.
        };

        // --- Existence + casing for every column referenced in THIS table ---
        // `referenced_columns()` = subject template columns + POM predicate columns +
        // POM object columns (for a RefObjectMap, the join *child* columns). The
        // `rr:column` subject form is not covered by it, so add it explicitly. Each
        // distinct name is resolved exactly once to avoid duplicate diagnostics.
        let mut referenced: BTreeSet<&str> = tm.referenced_columns().into_iter().collect();
        if let Some(col) = &tm.subject_map.column {
            referenced.insert(col.as_str());
        }
        let mut resolved: HashMap<&str, Option<&ColumnInfo>> = HashMap::new();
        for &name in &referenced {
            let ci = check_column(schema, table, name, &mut diagnostics);
            resolved.insert(name, ci);
        }

        // --- Join key type compatibility (child field_type vs parent field_type) ---
        for pom in &tm.predicate_object_maps {
            let ObjectMap::RefObjectMap(rom) = &pom.object_map else {
                continue;
            };

            let parent_tm = compiled.get(&rom.parent_triples_map);
            let parent_table = parent_tm.and_then(|p| p.table_name());
            let parent_schema = parent_table.and_then(|t| schemas.get(t));

            if parent_tm.is_none() {
                // Dangling parentTriplesMap — there is no parent table to resolve.
                diagnostics.push(Diagnostic {
                    severity: Severity::Error,
                    code: DiagCode::TableNotFound,
                    table: None,
                    column: None,
                    message: format!(
                        "TriplesMap <{}> joins to parent TriplesMap <{}>, which is not defined in \
                         the mapping.",
                        tm.iri, rom.parent_triples_map
                    ),
                });
            }

            for jc in &rom.join_conditions {
                // Child column was already resolved above (it is in referenced_columns()).
                let child_ci = resolved.get(jc.child_column.as_str()).copied().flatten();

                // Parent column is resolved against the PARENT table's schema. When
                // the parent table/schema is absent its own TableNotFound already
                // fired (parent is iterated as its own TriplesMap), so skip here.
                let parent_ci = match (parent_table, parent_schema) {
                    (Some(pt), Some(ps)) => {
                        check_column(ps, pt, &jc.parent_column, &mut diagnostics)
                    }
                    _ => None,
                };

                if let (Some(cc), Some(pc)) = (child_ci, parent_ci) {
                    // Compare parsed field types; skip when either is unknown
                    // (nested / unparseable) — cannot prove a mismatch.
                    if let (Some(ct), Some(pt_ty)) = (&cc.field_type, &pc.field_type) {
                        if ct != pt_ty {
                            diagnostics.push(Diagnostic::new(
                                Severity::Error,
                                DiagCode::JoinTypeMismatch,
                                table.to_string(),
                                Some(jc.child_column.clone()),
                                format!(
                                    "Join key type mismatch: child column '{}.{}' is {:?} but parent \
                                     column '{}.{}' is {:?}; the join will never match.",
                                    table,
                                    jc.child_column,
                                    ct,
                                    parent_table.unwrap_or_default(),
                                    jc.parent_column,
                                    pt_ty
                                ),
                            ));
                        }
                    }
                }
            }
        }

        // --- Subject-key nullability (NOT uniqueness — NDV deferred to PR-5) ---
        let mut subject_key_cols: Vec<&str> = tm
            .subject_map
            .template_columns
            .iter()
            .map(String::as_str)
            .collect();
        if let Some(col) = &tm.subject_map.column {
            subject_key_cols.push(col.as_str());
        }
        subject_key_cols.sort_unstable();
        subject_key_cols.dedup();

        for name in subject_key_cols {
            // Use the already-resolved column (exact or case-corrected). If it could
            // not be resolved at all, ColumnNotFound already fired — skip.
            let Some(Some(ci)) = resolved.get(name) else {
                continue;
            };
            // `null_fraction` is null_count/value_count over integer counts, so it is
            // exactly 0.0 iff there are no nulls; `<= 0.0` is an exact, clippy-clean
            // "no nulls" test.
            let null_fraction = ci.stats.as_ref().and_then(|s| s.null_fraction);
            let null_free = ci.required || matches!(null_fraction, Some(f) if f <= 0.0);
            if !null_free {
                let detail = match null_fraction {
                    Some(f) => format!("stats report null_fraction = {f}"),
                    None => "the column is not `required` and null-freeness could not be verified \
                             from stats"
                        .to_string(),
                };
                diagnostics.push(Diagnostic::new(
                    Severity::Warning,
                    DiagCode::NoSafeSubjectKey,
                    table.to_string(),
                    Some(name.to_string()),
                    format!(
                        "Subject-key column '{name}' is nullable ({detail}); rows with a NULL value \
                         here produce no subject and are silently dropped. Uniqueness is NOT checked \
                         here (NDV is deferred to PR-5)."
                    ),
                ));
            }
        }
    }

    diagnostics
}

/// Compile the Turtle for validation, or produce the `compiled_ok = false` response
/// carrying the compile error. Factored out (no `self` / no catalog) so the
/// compile-failure branch is unit-testable offline.
fn compile_for_validate(
    turtle: &str,
) -> std::result::Result<CompiledR2rmlMapping, ValidateR2rmlResponse> {
    match R2rmlLoader::from_turtle(turtle).and_then(R2rmlLoader::compile) {
        Ok(compiled) => Ok(compiled),
        Err(e) => Err(ValidateR2rmlResponse {
            compiled_ok: false,
            triples_map_count: 0,
            table_names: Vec::new(),
            diagnostics: vec![compile_error_diagnostic(&e.to_string())],
        }),
    }
}

impl crate::Fluree {
    /// **Validate / dry-run** an R2RML mapping against a live Iceberg catalog,
    /// creating **no** graph source (no CAS write, no publish).
    ///
    /// Compiles `turtle` and, on success, cross-checks every TriplesMap's table,
    /// columns, joins, and subject-key nullability against the metadata preview
    /// (`tier = Stats`) of each referenced table, returning structured
    /// [`Diagnostic`]s. On a compile failure it returns immediately with
    /// `compiled_ok = false` and a single error diagnostic.
    ///
    /// `snapshot` is an optional Iceberg snapshot id the caller intends to validate
    /// against. The metadata preview resolves each table's **current** snapshot, so
    /// this is recorded (traced) rather than enforced; historical-snapshot preview
    /// is a preview-layer capability this read-only lane does not add.
    ///
    /// Catalog preview failures (unreachable catalog, Direct mode, a genuinely
    /// missing table) are surfaced as [`DiagCode::TableNotFound`] diagnostics rather
    /// than hard errors — a dry-run reports problems, it does not throw on them.
    pub async fn validate_r2rml(
        &self,
        conn: IcebergConnectionConfig,
        turtle: String,
        snapshot: Option<i64>,
    ) -> Result<ValidateR2rmlResponse> {
        let compiled = match compile_for_validate(&turtle) {
            Ok(compiled) => compiled,
            Err(response) => return Ok(response),
        };

        // Distinct logical tables referenced by the mapping (sorted, deterministic).
        let mut table_names_set: BTreeSet<String> = BTreeSet::new();
        for tm in compiled.triples_maps.values() {
            if let Some(t) = tm.table_name() {
                table_names_set.insert(t.to_string());
            }
        }

        // Resolve each table to its live schema via the preview (Tier-B stats, so
        // subject-key null_fraction is available). Preview failures are recorded and
        // become TableNotFound diagnostics in the pure cross-check below.
        let mut schemas: HashMap<String, TableSchema> = HashMap::new();
        let mut load_errors: HashMap<String, String> = HashMap::new();
        for name in &table_names_set {
            let table_id = table_identifier_from_name(name);
            tracing::debug!(
                table = %name,
                requested_snapshot = ?snapshot,
                "validate_r2rml: previewing table current snapshot for cross-check"
            );
            match preview_iceberg_table(conn.clone(), table_id, StatsTier::Stats).await {
                Ok(preview) => {
                    schemas.insert(name.clone(), preview.schema);
                }
                Err(e) => {
                    tracing::debug!(table = %name, error = %e, "validate_r2rml: preview failed");
                    load_errors.insert(name.clone(), e.to_string());
                }
            }
        }

        let diagnostics = cross_check_mapping(&compiled, &schemas, &load_errors);

        Ok(ValidateR2rmlResponse {
            compiled_ok: true,
            triples_map_count: compiled.len(),
            table_names: table_names_set.into_iter().collect(),
            diagnostics,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fluree_db_iceberg::FieldType;
    use fluree_db_r2rml::mapping::{
        JoinCondition, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap, SubjectMap,
        TriplesMap,
    };

    use super::super::iceberg_catalog::{ColumnStats, SnapshotRef};

    // ----- synthesized-schema builders (stand in for the live preview) -----

    fn snapshot_ref() -> SnapshotRef {
        SnapshotRef {
            id: 1,
            timestamp_ms: 0,
            schema_id: Some(0),
        }
    }

    /// A column with a given type, requiredness, and (optional) null_fraction stat.
    fn col(name: &str, ft: FieldType, required: bool, null_fraction: Option<f64>) -> ColumnInfo {
        ColumnInfo {
            field_id: 0,
            name: name.to_string(),
            iceberg_type: "n/a".to_string(),
            field_type: Some(ft),
            xsd_type: None,
            required,
            nested: false,
            doc: None,
            stats: null_fraction.map(|nf| ColumnStats {
                null_fraction: Some(nf),
                ..ColumnStats::default()
            }),
        }
    }

    fn schema(table: &str, columns: Vec<ColumnInfo>) -> TableSchema {
        TableSchema {
            table: table.to_string(),
            table_uuid: None,
            format_version: 2,
            current_schema_id: 0,
            snapshot: snapshot_ref(),
            row_count: None,
            data_file_count: None,
            total_bytes: None,
            identifier_field_ids: Vec::new(),
            partition_spec: Vec::new(),
            sort_order: Vec::new(),
            properties: std::collections::HashMap::new(),
            columns,
            snapshot_log: Vec::new(),
        }
    }

    fn no_errors() -> HashMap<String, String> {
        HashMap::new()
    }

    /// `DIM_STORE` with a required subject key `STORE_KEY` (long) + a `NAME` (string).
    fn dim_store_map() -> TriplesMap {
        let mut tm = TriplesMap::new("<#DimStore>", "DW.DIM_STORE");
        tm.subject_map =
            SubjectMap::template("http://ex/store/{STORE_KEY}").with_class("http://ex/Store");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/name"),
            object_map: ObjectMap::column("NAME"),
        }];
        tm
    }

    fn dim_store_schema() -> TableSchema {
        schema(
            "DW.DIM_STORE",
            vec![
                col("STORE_KEY", FieldType::Int64, true, Some(0.0)),
                col("NAME", FieldType::String, false, Some(0.1)),
            ],
        )
    }

    fn has_code(diags: &[Diagnostic], code: DiagCode) -> bool {
        diags.iter().any(|d| d.code == code)
    }

    fn has_error(diags: &[Diagnostic]) -> bool {
        diags.iter().any(|d| d.severity == Severity::Error)
    }

    // ----------------------------- tests -----------------------------

    #[test]
    fn correct_mapping_validates_clean() {
        let compiled = CompiledR2rmlMapping::new(vec![dim_store_map()]);
        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), dim_store_schema());

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(
            diags.is_empty(),
            "a correct mapping must produce no diagnostics, got: {diags:?}"
        );
        assert!(!has_error(&diags));
    }

    #[test]
    fn miscased_column_flags_casing_mismatch_not_column_not_found() {
        // Subject key spelled `store_key`; schema has `STORE_KEY`.
        let mut tm = TriplesMap::new("<#DimStore>", "DW.DIM_STORE");
        tm.subject_map = SubjectMap::template("http://ex/store/{store_key}");
        let compiled = CompiledR2rmlMapping::new(vec![tm]);

        let mut schemas = HashMap::new();
        schemas.insert(
            "DW.DIM_STORE".to_string(),
            schema(
                "DW.DIM_STORE",
                vec![col("STORE_KEY", FieldType::Int64, true, Some(0.0))],
            ),
        );

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(has_code(&diags, DiagCode::CasingMismatch), "{diags:?}");
        assert!(!has_code(&diags, DiagCode::ColumnNotFound), "{diags:?}");
        // Case-corrected key is required → no nullable-subject-key warning.
        assert!(!has_code(&diags, DiagCode::NoSafeSubjectKey), "{diags:?}");
    }

    #[test]
    fn nonexistent_column_flags_column_not_found() {
        // Object column `BOGUS` does not exist on the table.
        let mut tm = TriplesMap::new("<#DimStore>", "DW.DIM_STORE");
        tm.subject_map = SubjectMap::template("http://ex/store/{STORE_KEY}");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/bogus"),
            object_map: ObjectMap::column("BOGUS"),
        }];
        let compiled = CompiledR2rmlMapping::new(vec![tm]);

        let mut schemas = HashMap::new();
        schemas.insert(
            "DW.DIM_STORE".to_string(),
            schema(
                "DW.DIM_STORE",
                vec![col("STORE_KEY", FieldType::Int64, true, Some(0.0))],
            ),
        );

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(has_code(&diags, DiagCode::ColumnNotFound), "{diags:?}");
        assert!(diags.iter().any(|d| d.column.as_deref() == Some("BOGUS")));
    }

    #[test]
    fn missing_table_flags_table_not_found() {
        // Schema map is empty → the referenced table cannot be resolved.
        let compiled = CompiledR2rmlMapping::new(vec![dim_store_map()]);
        let diags = cross_check_mapping(&compiled, &HashMap::new(), &no_errors());
        assert!(has_code(&diags, DiagCode::TableNotFound), "{diags:?}");
    }

    #[test]
    fn join_parent_column_absent_flags_column_not_found() {
        // FACT_SALES joins DIM_STORE on STORE_KEY→STORE_KEY, but the parent schema
        // lacks STORE_KEY entirely.
        let compiled = CompiledR2rmlMapping::new(vec![fact_sales_map(), dim_store_map()]);

        let mut schemas = HashMap::new();
        schemas.insert("DW.FACT_SALES".to_string(), fact_sales_schema());
        // Parent DIM_STORE exists but WITHOUT the STORE_KEY column.
        schemas.insert(
            "DW.DIM_STORE".to_string(),
            schema(
                "DW.DIM_STORE",
                vec![col("NAME", FieldType::String, false, Some(0.0))],
            ),
        );

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        // Parent column STORE_KEY missing on DIM_STORE → ColumnNotFound on the parent.
        assert!(has_code(&diags, DiagCode::ColumnNotFound), "{diags:?}");
        assert!(diags
            .iter()
            .any(|d| d.table.as_deref() == Some("DW.DIM_STORE")
                && d.column.as_deref() == Some("STORE_KEY")));
    }

    #[test]
    fn join_parent_table_absent_flags_table_not_found() {
        // Parent DIM_STORE not resolved at all (absent from schemas).
        let compiled = CompiledR2rmlMapping::new(vec![fact_sales_map(), dim_store_map()]);
        let mut schemas = HashMap::new();
        schemas.insert("DW.FACT_SALES".to_string(), fact_sales_schema());

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(has_code(&diags, DiagCode::TableNotFound), "{diags:?}");
        assert!(diags
            .iter()
            .any(|d| d.table.as_deref() == Some("DW.DIM_STORE")));
    }

    #[test]
    fn join_type_mismatch_flags_join_type_mismatch() {
        // Child STORE_KEY is a String, parent STORE_KEY is a Long → mismatch.
        let compiled = CompiledR2rmlMapping::new(vec![fact_sales_map(), dim_store_map()]);

        let mut schemas = HashMap::new();
        schemas.insert(
            "DW.FACT_SALES".to_string(),
            schema(
                "DW.FACT_SALES",
                vec![
                    col("SALE_KEY", FieldType::Int64, true, Some(0.0)),
                    col("STORE_KEY", FieldType::String, false, Some(0.0)), // wrong type
                ],
            ),
        );
        schemas.insert("DW.DIM_STORE".to_string(), dim_store_schema()); // STORE_KEY is Int64

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(has_code(&diags, DiagCode::JoinTypeMismatch), "{diags:?}");
    }

    #[test]
    fn nullable_subject_key_flags_no_safe_subject_key_but_not_uniqueness() {
        // Subject key STORE_KEY is NOT required and stats show null_fraction > 0.
        let mut tm = TriplesMap::new("<#DimStore>", "DW.DIM_STORE");
        tm.subject_map = SubjectMap::template("http://ex/store/{STORE_KEY}");
        let compiled = CompiledR2rmlMapping::new(vec![tm]);

        let mut schemas = HashMap::new();
        schemas.insert(
            "DW.DIM_STORE".to_string(),
            schema(
                "DW.DIM_STORE",
                vec![col("STORE_KEY", FieldType::Int64, false, Some(0.25))],
            ),
        );

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(
            has_code(&diags, DiagCode::NoSafeSubjectKey),
            "nullable subject key must be flagged: {diags:?}"
        );
        // Uniqueness (NDV) is deliberately NOT checked in PR-1.
        assert!(
            !has_code(&diags, DiagCode::SubjectKeyUnverified),
            "uniqueness must NOT be flagged (NDV deferred to PR-5): {diags:?}"
        );
    }

    #[test]
    fn nullable_subject_key_not_flagged_when_null_fraction_zero() {
        // Not required, but stats prove null_fraction == 0 → safe, no warning.
        let mut tm = TriplesMap::new("<#DimStore>", "DW.DIM_STORE");
        tm.subject_map = SubjectMap::template("http://ex/store/{STORE_KEY}");
        let compiled = CompiledR2rmlMapping::new(vec![tm]);

        let mut schemas = HashMap::new();
        schemas.insert(
            "DW.DIM_STORE".to_string(),
            schema(
                "DW.DIM_STORE",
                vec![col("STORE_KEY", FieldType::Int64, false, Some(0.0))],
            ),
        );

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(!has_code(&diags, DiagCode::NoSafeSubjectKey), "{diags:?}");
    }

    #[test]
    fn preview_failure_enriches_table_not_found_message() {
        let compiled = CompiledR2rmlMapping::new(vec![dim_store_map()]);
        let mut load_errors = HashMap::new();
        load_errors.insert(
            "DW.DIM_STORE".to_string(),
            "Direct catalog mode cannot be used".to_string(),
        );

        let diags = cross_check_mapping(&compiled, &HashMap::new(), &load_errors);
        assert!(has_code(&diags, DiagCode::TableNotFound));
        assert!(diags
            .iter()
            .any(|d| d.message.contains("Direct catalog mode")));
    }

    #[test]
    fn turtle_that_fails_to_compile_yields_compiled_ok_false_and_error() {
        // Unterminated string literal → a hard Turtle parse error.
        let bad = "@prefix ex: <http://ex/> .\nex:a ex:b \"oops .";
        let response = compile_for_validate(bad).expect_err("malformed Turtle must not compile");

        assert!(!response.compiled_ok);
        assert_eq!(response.triples_map_count, 0);
        assert!(response.table_names.is_empty());
        assert!(
            has_error(&response.diagnostics),
            "{:?}",
            response.diagnostics
        );
        // The compile-fail diagnostic carries the dedicated CompileError code, never
        // TableNotFound — so on the wire it can't be confused with a dangling table
        // reference produced by the cross-check.
        assert!(
            has_code(&response.diagnostics, DiagCode::CompileError),
            "{:?}",
            response.diagnostics
        );
        assert!(!has_code(&response.diagnostics, DiagCode::TableNotFound));
        assert!(response.diagnostics[0]
            .message
            .contains("failed to compile"));
    }

    #[test]
    fn table_identifier_split_uses_last_dot() {
        assert_eq!(
            table_identifier_from_name("DW.DIM_STORE"),
            TableIdentifier::new("DW", "DIM_STORE")
        );
        assert_eq!(
            table_identifier_from_name("db.schema.events"),
            TableIdentifier::new("db.schema", "events")
        );
    }

    // A FACT table that joins DIM_STORE on STORE_KEY → STORE_KEY.
    fn fact_sales_map() -> TriplesMap {
        let mut tm = TriplesMap::new("<#FactSales>", "DW.FACT_SALES");
        tm.subject_map = SubjectMap::template("http://ex/sale/{SALE_KEY}");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex/store"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap {
                parent_triples_map: "<#DimStore>".to_string(),
                join_conditions: vec![JoinCondition {
                    child_column: "STORE_KEY".to_string(),
                    parent_column: "STORE_KEY".to_string(),
                }],
            }),
        }];
        tm
    }

    fn fact_sales_schema() -> TableSchema {
        schema(
            "DW.FACT_SALES",
            vec![
                col("SALE_KEY", FieldType::Int64, true, Some(0.0)),
                col("STORE_KEY", FieldType::Int64, false, Some(0.0)),
            ],
        )
    }
}
