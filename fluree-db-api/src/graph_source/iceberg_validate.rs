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
//! * **`rr:sqlQuery` row filters** — an `rr:sqlQuery` map is checked against its
//!   query's `FROM` table, and the query by the check registration refuses it
//!   with and a read refuses its rows with
//!   ([`check_query_map`](super::row_filter::check_query_map)): the restricted
//!   subset, a select list holding every column the map reads, and each column
//!   named exactly and each literal typed against its live column. Its refusal
//!   is an error carrying registration's message, [`DiagCode::ColumnNotFound`]
//!   for a column the table lacks and [`DiagCode::CompileError`] otherwise, so
//!   the dry-run never calls clean a mapping registration refuses.
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

use fluree_db_iceberg::metadata::Schema;
use fluree_db_iceberg::FieldType;
use fluree_db_r2rml::emit::{DiagCode, Diagnostic, Severity};
use fluree_db_r2rml::loader::R2rmlLoader;
use fluree_db_r2rml::mapping::{
    CompiledR2rmlMapping, KeyColumn, ObjectMap, RestrictedSelect, SubjectKeyProof, TableKeys,
    TriplesMap,
};

use super::row_filter::{check_query_map, query_maps};

use super::config::IcebergConnectionConfig;
use super::iceberg_catalog::{
    preview_iceberg_table_and_read_schema_in, table_schema_from_metadata, ColumnInfo, StatsTier,
    TableIdentifier, TableSchema,
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

/// A live column, as far as a mapping is checked against it.
#[derive(Debug, Clone)]
pub(crate) struct LiveColumn {
    pub(crate) name: String,
    /// `None` for a type that cannot be compared (nested, unreadable).
    pub(crate) field_type: Option<FieldType>,
    pub(crate) required: bool,
    pub(crate) null_fraction: Option<f64>,
}

impl From<&ColumnInfo> for LiveColumn {
    fn from(ci: &ColumnInfo) -> Self {
        Self {
            name: ci.name.clone(),
            field_type: ci.field_type,
            required: ci.required,
            null_fraction: ci.stats.as_ref().and_then(|s| s.null_fraction),
        }
    }
}

/// What differs between the table formats a mapping is checked against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Wording {
    /// What a name that matches only ignoring case means for a query.
    pub(crate) casing: &'static str,
    /// Introduces the reason a table's schema could not be had.
    pub(crate) unread: &'static str,
}

const ICEBERG_WORDING: Wording = Wording {
    casing: "Iceberg field names are case-sensitive — fix the casing to match.",
    unread: "catalog preview failed",
};

/// Which table an `rr:sqlQuery` triples map is checked against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QueryTables {
    /// The query's alias, as a table name: a source that refuses queries
    /// reports the alias as a table it cannot find.
    #[cfg_attr(not(any(feature = "delta", feature = "sql")), allow(dead_code))]
    Alias,
    /// The `FROM` table of a query in the restricted subset an Iceberg source
    /// evaluates (see [`super::row_filter`]); a query outside it is an error.
    FromTable,
}

/// The table `tm` is checked against; `None` for a query outside the subset.
fn source_table(tm: &TriplesMap, queries: QueryTables) -> Option<String> {
    match (queries, tm.sql_query()) {
        (QueryTables::FromTable, Some(sql)) => RestrictedSelect::parse(sql).ok().map(|s| s.table),
        _ => tm.table_name().map(str::to_string),
    }
}

/// How a referenced column resolves against a live schema.
enum ColumnResolution<'a> {
    /// Exact byte-for-byte name match.
    Exact(&'a LiveColumn),
    /// Matches only case-insensitively (the schema spells it differently).
    Casing(&'a LiveColumn),
    /// No column matches, even case-insensitively.
    Missing,
}

fn resolve_column<'a>(schema: &'a [LiveColumn], name: &str) -> ColumnResolution<'a> {
    if let Some(ci) = schema.iter().find(|c| c.name == name) {
        ColumnResolution::Exact(ci)
    } else if let Some(ci) = schema.iter().find(|c| c.name.eq_ignore_ascii_case(name)) {
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
    schema: &'a [LiveColumn],
    table: &str,
    name: &str,
    wording: Wording,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<&'a LiveColumn> {
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
                     '{}'. {}",
                    ci.name, wording.casing
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
///
/// `read_schemas` maps a table to the schema an unpinned read of it reads, the
/// one a row filter is checked against (absent for a table that did not load).
fn cross_check_mapping(
    compiled: &CompiledR2rmlMapping,
    schemas: &HashMap<String, TableSchema>,
    read_schemas: &HashMap<String, Schema>,
    load_errors: &HashMap<String, String>,
) -> Vec<Diagnostic> {
    let live = schemas
        .iter()
        .map(|(name, schema)| {
            let columns = schema.columns.iter().map(LiveColumn::from).collect();
            (name.clone(), columns)
        })
        .collect();
    let mut diagnostics = cross_check_live(
        compiled,
        &live,
        load_errors,
        ICEBERG_WORDING,
        QueryTables::FromTable,
    );
    diagnostics.extend(query_map_diagnostics(compiled, read_schemas));
    diagnostics
}

/// Each `rr:sqlQuery` map's refusal by [`check_query_map`], as an error: the
/// check registration refuses the mapping with and a read refuses its rows
/// with, over the schema an unpinned read of the query's `FROM` table reads
/// (only the checks that need no schema when the table did not load; its
/// `TableNotFound` is already reported). The message is the one registration
/// gives.
fn query_map_diagnostics(
    compiled: &CompiledR2rmlMapping,
    read_schemas: &HashMap<String, Schema>,
) -> Vec<Diagnostic> {
    query_maps(compiled)
        .into_iter()
        .filter_map(|tm| {
            let table = source_table(tm, QueryTables::FromTable);
            let schema = table.as_deref().and_then(|t| read_schemas.get(t));
            let refusal = check_query_map(compiled, tm, schema).err()?;
            let code = match refusal.missing_column {
                Some(_) => DiagCode::ColumnNotFound,
                None => DiagCode::CompileError,
            };
            Some(Diagnostic {
                severity: Severity::Error,
                code,
                table,
                column: refusal.missing_column,
                message: refusal.message,
            })
        })
        .collect()
}

/// [`cross_check_mapping`] over any table format's columns.
pub(crate) fn cross_check_live(
    compiled: &CompiledR2rmlMapping,
    schemas: &HashMap<String, Vec<LiveColumn>>,
    load_errors: &HashMap<String, String>,
    wording: Wording,
    queries: QueryTables,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();

    // Deterministic order: iterate TriplesMaps sorted by IRI so diagnostics are stable.
    let mut tms: Vec<&TriplesMap> = compiled.triples_maps.values().collect();
    tms.sort_by(|a, b| a.iri.cmp(&b.iri));

    for tm in tms {
        // An `rr:sqlQuery` map's term maps are checked against its FROM table
        // here; the query itself is checked by `query_map_diagnostics`, and
        // one outside the subset names no table.
        let Some(table) = source_table(tm, queries) else {
            continue;
        };
        let table = table.as_str();

        let Some(schema) = schemas.get(table) else {
            let detail = load_errors
                .get(table)
                .map(|e| format!(" ({}: {e})", wording.unread))
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
        let mut resolved: HashMap<&str, Option<&LiveColumn>> = HashMap::new();
        for &name in &referenced {
            let ci = check_column(schema, table, name, wording, &mut diagnostics);
            resolved.insert(name, ci);
        }

        // --- Join key type compatibility (child field_type vs parent field_type) ---
        for pom in &tm.predicate_object_maps {
            let ObjectMap::RefObjectMap(rom) = &pom.object_map else {
                continue;
            };

            let parent_tm = compiled.get(&rom.parent_triples_map);
            let parent_table = parent_tm.and_then(|p| source_table(p, queries));
            let parent_schema = parent_table.as_deref().and_then(|t| schemas.get(t));

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
                let parent_ci = match (parent_table.as_deref(), parent_schema) {
                    (Some(pt), Some(ps)) => {
                        check_column(ps, pt, &jc.parent_column, wording, &mut diagnostics)
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
                                    parent_table.as_deref().unwrap_or_default(),
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
            let null_fraction = ci.null_fraction;
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
                         here."
                    ),
                ));
            }
        }
    }

    diagnostics.extend(declared_key_diagnostics(compiled, schemas, queries));
    diagnostics
}

/// Every declared key column (`f:key`) checked against its table's schema as
/// an Iceberg query checks it before refusing: a column the table lacks, or
/// one spelled with other casing, is [`DiagCode::ColumnNotFound`] /
/// [`DiagCode::CasingMismatch`]; one whose values cannot identify a row
/// (floating-point, nested, unreadable) is [`DiagCode::NonKeyTypeSkipped`].
/// A key on an `rr:sqlQuery` logical table is checked against the query's
/// `FROM` table, and a column its select list leaves out is
/// [`DiagCode::ColumnNotFound`] too. All errors, once per logical table and
/// column however many maps read it. A table with no schema already has its
/// [`DiagCode::TableNotFound`].
fn declared_key_diagnostics(
    compiled: &CompiledR2rmlMapping,
    schemas: &HashMap<String, Vec<LiveColumn>>,
    queries: QueryTables,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    // Logical table name (a table, or a query's alias) -> the table its rows
    // come from, and the query's select list when it has one.
    let mut tables: std::collections::BTreeMap<&str, (String, Option<Vec<String>>)> =
        std::collections::BTreeMap::new();
    for tm in compiled.triples_maps.values() {
        let Some(logical) = tm.table_name() else {
            continue;
        };
        let Some(table) = source_table(tm, queries) else {
            continue;
        };
        let selected = match (queries, tm.sql_query()) {
            (QueryTables::FromTable, Some(sql)) => {
                RestrictedSelect::parse(sql).ok().and_then(|s| s.columns)
            }
            _ => None,
        };
        tables.entry(logical).or_insert((table, selected));
    }
    for (logical, (table, selected)) in tables {
        let Some(schema) = schemas.get(&table) else {
            continue;
        };
        let on = if logical == table {
            format!("'{table}'")
        } else {
            format!("the rr:sqlQuery over '{table}'")
        };
        let table = table.as_str();
        let keys = compiled.declared_keys(logical);
        let columns: BTreeSet<&str> = keys.iter().flatten().map(String::as_str).collect();
        for name in columns {
            let unselected = selected
                .as_ref()
                .is_some_and(|selected| !selected.iter().any(|c| c == name));
            let (code, message) = match resolve_column(schema, name) {
                ColumnResolution::Exact(_) | ColumnResolution::Casing(_) if unselected => (
                    DiagCode::ColumnNotFound,
                    format!(
                        "f:key on {on} names column '{name}', which the query does not \
                         select."
                    ),
                ),
                ColumnResolution::Missing => (
                    DiagCode::ColumnNotFound,
                    format!(
                        "f:key on {on} names column '{name}', which the table does not \
                         have."
                    ),
                ),
                ColumnResolution::Casing(ci) => (
                    DiagCode::CasingMismatch,
                    format!(
                        "f:key on {on} names column '{name}', which the schema spells \
                         '{}'; a key column must match exactly.",
                        ci.name
                    ),
                ),
                ColumnResolution::Exact(ci)
                    if fluree_db_r2rml::mapping::usable_in_key(ci.field_type.as_ref()) =>
                {
                    continue
                }
                ColumnResolution::Exact(ci) => (
                    DiagCode::NonKeyTypeSkipped,
                    match &ci.field_type {
                        Some(ty) => format!(
                            "f:key on {on} names column '{name}' of type {ty:?}; a \
                             floating-point value cannot identify a row (NaN is unequal to \
                             itself)."
                        ),
                        None => format!(
                            "f:key on {on} names column '{name}', whose type is nested \
                             or not one values are read from."
                        ),
                    },
                ),
            };
            diagnostics.push(Diagnostic::new(
                Severity::Error,
                code,
                table.to_string(),
                Some(name.to_string()),
                message,
            ));
        }
    }
    diagnostics
}

/// Where a source lets an operator state a table's key besides `f:key`, for
/// the remedy a [`DiagCode::KeylessMap`] advisory gives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeyRemedy {
    /// `f:key`, or the table's identifier fields (Iceberg).
    DeclareOrIdentifierFields,
    /// `f:key`: the source reports no key of its own (Delta), or only the one
    /// its registration probe verified (SQL).
    Declare,
}

/// What a query over a map no key proves subject-unique does, and costs.
const DEDUPLICATES: &str = "queries over it remove repeated solutions as they run. That is \
     correct, but a COUNT, a same-subject star or an ORDER BY ... LIMIT over it reads more of \
     the table, and the solutions it has seen are held against the query's memory budget";

/// An advisory ([`DiagCode::KeylessMap`]) for each map whose subjects no
/// known key of its logical table proves unique
/// ([`TriplesMap::subject_unique_under`]): queries over it take the
/// deduplicating lanes. It names the map and, where a key could prove it, the
/// columns and how to declare them.
///
/// `keys` holds each logical table's keys (by [`TriplesMap::table_name`]) as
/// a query unites them: the mapping's `f:key` with what the source reports.
/// A map whose table is absent is skipped: its keys could not be learned,
/// and the caller reports why elsewhere. One advisory per map, in IRI order.
pub(crate) fn keyless_map_diagnostics(
    compiled: &CompiledR2rmlMapping,
    keys: &HashMap<String, TableKeys>,
    queries: QueryTables,
    remedy: KeyRemedy,
) -> Vec<Diagnostic> {
    let mut tms: Vec<&TriplesMap> = compiled.triples_maps.values().collect();
    tms.sort_by(|a, b| a.iri.cmp(&b.iri));
    let mut diagnostics = Vec::new();
    for tm in tms {
        let Some(table_keys) = tm.table_name().and_then(|t| keys.get(t)) else {
            continue;
        };
        if tm.subject_unique_under(table_keys) {
            continue;
        }
        // The table the other diagnostics name: an Iceberg query's FROM
        // table; none for a query alias, which names nothing an operator
        // wrote.
        let table = source_table(tm, queries)
            .filter(|t| !fluree_db_r2rml::mapping::LogicalTable::is_sql_query_alias(t));
        let on = match (&table, tm.sql_query()) {
            (Some(t), Some(_)) => format!("its rr:sqlQuery logical table (over '{t}')"),
            (Some(t), None) => format!("its logical table '{t}'"),
            (None, _) => "its rr:sqlQuery logical table".to_string(),
        };
        let message = match tm.subject_key_proof() {
            SubjectKeyProof::KeyWithin(columns) => {
                let listed = columns.join(", ");
                let known = if table_keys.is_empty() {
                    "it has no known key".to_string()
                } else {
                    let known: Vec<String> = table_keys
                        .iter()
                        .map(|key| format!("({})", key.join(", ")))
                        .collect();
                    format!("its known keys are {}", known.join(", "))
                };
                let declaration: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
                let (identify, them) = match columns.as_slice() {
                    [one] => (
                        format!("{one} identifies"),
                        "it the table's identifier field",
                    ),
                    _ => (
                        format!("({listed}) together identify"),
                        "them the table's identifier fields",
                    ),
                };
                let or_fields = match remedy {
                    KeyRemedy::DeclareOrIdentifierFields => format!(", or make {them}"),
                    KeyRemedy::Declare => String::new(),
                };
                format!(
                    "TriplesMap <{}>: no key of {on} lies within its subject's columns \
                     ({listed}), and {known}, so {DEDUPLICATES}. If {identify} a row, declare \
                     `f:key ( {} )` on its logical table{or_fields}.",
                    tm.iri,
                    declaration.join(" ")
                )
            }
            SubjectKeyProof::RunTogether => format!(
                "TriplesMap <{}>: its subject template can run two values together (no \
                 character the IRI escape always encodes, such as '/', separates two of its \
                 placeholders), so no key of {on} can prove its subjects unique, and \
                 {DEDUPLICATES}. Separate each pair of placeholders with '/' and declare a key \
                 over its columns to prove it.",
                tm.iri
            ),
            SubjectKeyProof::NotFromColumns => format!(
                "TriplesMap <{}>: its subject is not built from the row's columns (a constant, \
                 or a term with no column or template), so no key of {on} can prove its \
                 subjects unique, and {DEDUPLICATES}.",
                tm.iri
            ),
        };
        diagnostics.push(Diagnostic {
            severity: Severity::Advisory,
            code: DiagCode::KeylessMap,
            table,
            column: None,
            message,
        });
    }
    diagnostics
}

/// Each logical table's keys as an Iceberg query would learn them, from the
/// metadata preview a dry-run reads: the mapping's `f:key` united with the
/// identifier fields of the table a map reads ([`identifier_fields_key`]),
/// counted only where the logical table's query selects them. A table the
/// preview did not resolve is left out (it has its `TableNotFound`).
///
/// [`identifier_fields_key`]: super::r2rml::identifier_fields_key
pub(crate) fn preview_table_keys(
    compiled: &CompiledR2rmlMapping,
    schemas: &HashMap<String, TableSchema>,
) -> HashMap<String, TableKeys> {
    let mut keys = HashMap::new();
    for tm in compiled.triples_maps.values() {
        let Some(logical) = tm.table_name() else {
            continue;
        };
        let Some(schema) = source_table(tm, QueryTables::FromTable).and_then(|t| schemas.get(&t))
        else {
            continue;
        };
        let selected = tm
            .sql_query()
            .and_then(|sql| RestrictedSelect::parse(sql).ok())
            .and_then(|select| select.columns);
        let selects = |name: &str| {
            selected
                .as_ref()
                .is_none_or(|s| s.iter().any(|c| c == name))
        };
        let column = |name: &str| match schema.columns.iter().find(|c| c.name == name) {
            Some(c) if selects(name) => KeyColumn::Typed(c.field_type),
            _ => KeyColumn::Absent,
        };
        let names: Option<Vec<String>> = schema
            .identifier_field_ids
            .iter()
            .map(|id| {
                schema
                    .columns
                    .iter()
                    .find(|c| c.field_id == *id)
                    .map(|c| c.name.clone())
            })
            .collect();
        let reported = super::r2rml::identifier_fields_key(schema.format_version, names, column);
        keys.insert(
            logical.to_string(),
            compiled.declared_keys(logical).with_reported(reported),
        );
    }
    keys
}

/// Each logical table's keys from the mapping's `f:key` alone: what a
/// source that reports no key of its own (Delta) queries under.
pub(crate) fn declared_table_keys(compiled: &CompiledR2rmlMapping) -> HashMap<String, TableKeys> {
    compiled
        .triples_maps
        .values()
        .filter_map(|tm| tm.table_name())
        .map(|table| (table.to_string(), compiled.declared_keys(table)))
        .collect()
}

/// [`keyless_map_diagnostics`] over a generated mapping's Turtle, with the
/// keys `keys` finds for it. A generator writes no `f:key`, so this tells
/// the operator, before the mapping is saved, which maps its queries will
/// deduplicate. Turtle that does not compile gets none (the generator's
/// output always does).
pub(crate) fn keyless_generated_maps(
    turtle: &str,
    keys: impl FnOnce(&CompiledR2rmlMapping) -> HashMap<String, TableKeys>,
    queries: QueryTables,
    remedy: KeyRemedy,
) -> Vec<Diagnostic> {
    match R2rmlLoader::from_turtle(turtle).and_then(R2rmlLoader::compile) {
        Ok(compiled) => keyless_map_diagnostics(&compiled, &keys(&compiled), queries, remedy),
        Err(e) => {
            tracing::debug!(error = %e, "generated mapping did not compile; no key advisories");
            Vec::new()
        }
    }
}

/// Compile the Turtle for validation, or produce the `compiled_ok = false` response
/// carrying the compile error. Factored out (no `self` / no catalog) so the
/// compile-failure branch is unit-testable offline.
pub(crate) fn compile_for_validate(
    turtle: &str,
) -> std::result::Result<CompiledR2rmlMapping, ValidateR2rmlResponse> {
    match R2rmlLoader::from_turtle(turtle).and_then(R2rmlLoader::compile) {
        // Registration refuses a mapping that defines nothing, so the dry-run
        // must not call it clean.
        Ok(compiled) if compiled.is_empty() => Err(ValidateR2rmlResponse {
            compiled_ok: false,
            triples_map_count: 0,
            table_names: Vec::new(),
            diagnostics: vec![compile_error_diagnostic(
                crate::graph_source::r2rml::EMPTY_MAPPING,
            )],
        }),
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
        // Resolve any SecretRef auth up front. A missing resolver is a host
        // wiring fault (not a mapping problem), so it fails closed here rather
        // than degrading to a per-table TableNotFound diagnostic.
        let conn = self.hydrate_conn(conn).await?;

        let compiled = match compile_for_validate(&turtle) {
            Ok(compiled) => compiled,
            Err(response) => return Ok(response),
        };

        let table_names_set = validated_tables(&compiled);

        // Resolve each table to its live schema via the preview (Tier-B stats, so
        // subject-key null_fraction is available). Preview failures are recorded and
        // become TableNotFound diagnostics in the pure cross-check below.
        let mut schemas: HashMap<String, TableSchema> = HashMap::new();
        let mut read_schemas: HashMap<String, Schema> = HashMap::new();
        let mut load_errors: HashMap<String, String> = HashMap::new();
        for name in &table_names_set {
            let table_id = table_identifier_from_name(name);
            tracing::debug!(
                table = %name,
                requested_snapshot = ?snapshot,
                "validate_r2rml: previewing table current snapshot for cross-check"
            );
            // Under this instance's identity context, as every catalog read is.
            let preview = preview_iceberg_table_and_read_schema_in(
                conn.clone(),
                table_id,
                StatsTier::Stats,
                self.identity_context(),
                None,
            );
            match preview.await {
                Ok((preview, read_schema)) => {
                    schemas.insert(name.clone(), preview.schema);
                    read_schemas.extend(read_schema.map(|schema| (name.clone(), schema)));
                }
                Err(e) => {
                    tracing::debug!(table = %name, error = %e, "validate_r2rml: preview failed");
                    load_errors.insert(name.clone(), e.to_string());
                }
            }
        }

        Ok(validated(
            &compiled,
            table_names_set,
            &schemas,
            &read_schemas,
            &load_errors,
        ))
    }

    /// [`Self::validate_r2rml`] over local table directories (by table name)
    /// instead of a catalog, so a test can validate a mapping against the
    /// same table it registers and reads. Reads only under
    /// `FLUREE_ICEBERG_LOCAL_ROOTS`.
    #[doc(hidden)]
    pub async fn validate_r2rml_over_local_tables_for_test(
        &self,
        tables: HashMap<String, String>,
        turtle: String,
    ) -> Result<ValidateR2rmlResponse> {
        let compiled = match compile_for_validate(&turtle) {
            Ok(compiled) => compiled,
            Err(response) => return Ok(response),
        };
        let table_names_set = validated_tables(&compiled);
        let mut schemas: HashMap<String, TableSchema> = HashMap::new();
        let mut read_schemas: HashMap<String, Schema> = HashMap::new();
        let mut load_errors: HashMap<String, String> = HashMap::new();
        for name in &table_names_set {
            let loaded = match super::ephemeral::load_local(&tables, name).await {
                Ok((_, metadata)) => {
                    table_schema_from_metadata(&table_identifier_from_name(name), &metadata)
                        .map(|schema| (schema, metadata))
                }
                Err(e) => Err(crate::ApiError::config(e.to_string())),
            };
            match loaded {
                Ok((schema, metadata)) => {
                    schemas.insert(name.clone(), schema);
                    read_schemas.extend(
                        super::r2rml::unpinned_read_schema(&metadata)
                            .map(|schema| (name.clone(), schema.into_owned())),
                    );
                }
                Err(e) => {
                    load_errors.insert(name.clone(), e.to_string());
                }
            }
        }
        Ok(validated(
            &compiled,
            table_names_set,
            &schemas,
            &read_schemas,
            &load_errors,
        ))
    }
}

/// The distinct tables a mapping reads (sorted): an `rr:sqlQuery` map's table
/// is its query's FROM table.
fn validated_tables(compiled: &CompiledR2rmlMapping) -> BTreeSet<String> {
    compiled
        .triples_maps
        .values()
        .filter_map(|tm| source_table(tm, QueryTables::FromTable))
        .collect()
}

/// The dry-run's response for a compiled mapping, given the tables it reads
/// and the schemas of those that loaded: the cross-check's diagnostics, and an
/// advisory for each map whose subjects no known key proves unique.
fn validated(
    compiled: &CompiledR2rmlMapping,
    table_names: BTreeSet<String>,
    schemas: &HashMap<String, TableSchema>,
    read_schemas: &HashMap<String, Schema>,
    load_errors: &HashMap<String, String>,
) -> ValidateR2rmlResponse {
    let mut diagnostics = cross_check_mapping(compiled, schemas, read_schemas, load_errors);
    diagnostics.extend(keyless_map_diagnostics(
        compiled,
        &preview_table_keys(compiled, schemas),
        QueryTables::FromTable,
        KeyRemedy::DeclareOrIdentifierFields,
    ));
    ValidateR2rmlResponse {
        compiled_ok: true,
        triples_map_count: compiled.len(),
        table_names: table_names.into_iter().collect(),
        diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fluree_db_iceberg::FieldType;

    /// [`super::cross_check_mapping`] with each table's read schema made from
    /// its synthesized preview columns, as a loaded table's would be.
    fn cross_check_mapping(
        compiled: &CompiledR2rmlMapping,
        schemas: &HashMap<String, TableSchema>,
        load_errors: &HashMap<String, String>,
    ) -> Vec<Diagnostic> {
        let read_schemas = schemas
            .iter()
            .map(|(name, table)| (name.clone(), read_schema(table)))
            .collect();
        super::cross_check_mapping(compiled, schemas, &read_schemas, load_errors)
    }

    fn read_schema(table: &TableSchema) -> Schema {
        let fields: Vec<_> = table
            .columns
            .iter()
            .map(|c| {
                serde_json::json!({
                    "id": c.field_id, "name": c.name, "required": c.required,
                    "type": c.iceberg_type,
                })
            })
            .collect();
        serde_json::from_value(serde_json::json!({
            "type": "struct", "schema-id": 0, "fields": fields
        }))
        .expect("schema")
    }
    use fluree_db_r2rml::mapping::{
        GraphMap, JoinCondition, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap,
        SubjectMap, TriplesMap,
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
            iceberg_type: super::super::iceberg_catalog::field_type_to_iceberg_string(ft),
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

    /// An `rr:sqlQuery` map is checked against its query's FROM table, columns
    /// the query names included, never reported as a missing table named by
    /// its alias. A query outside the restricted subset is an error.
    #[test]
    fn a_query_map_is_checked_against_its_from_table() {
        let mut tm = dim_store_map();
        tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(
            "SELECT * FROM DW.DIM_STORE WHERE NAME IS NOT NULL",
        );
        let compiled = CompiledR2rmlMapping::new(vec![tm.clone()]);
        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), dim_store_schema());
        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(diags.is_empty(), "{diags:?}");

        tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(
            "SELECT * FROM DW.DIM_STORE WHERE REGION = 'west'",
        );
        let diags = cross_check_mapping(
            &CompiledR2rmlMapping::new(vec![tm.clone()]),
            &schemas,
            &no_errors(),
        );
        assert!(
            diags.iter().any(
                |d| d.code == DiagCode::ColumnNotFound && d.column.as_deref() == Some("REGION")
            ),
            "{diags:?}"
        );
        assert!(!has_code(&diags, DiagCode::TableNotFound), "{diags:?}");

        tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(
            "SELECT * FROM DW.DIM_STORE WHERE NAME = 'a' OR NAME = 'b'",
        );
        let diags =
            cross_check_mapping(&CompiledR2rmlMapping::new(vec![tm]), &schemas, &no_errors());
        assert!(
            diags.iter().any(|d| d.code == DiagCode::CompileError
                && d.severity == Severity::Error
                && d.message.contains("OR is not supported")),
            "{diags:?}"
        );
    }

    /// The dry-run calls no `rr:sqlQuery` map clean that registration refuses
    /// or a read fails on: a select list leaving out a column the map reads, a
    /// literal that does not type against its column, a miscased column in
    /// the query (a read matches it exactly). Each is an error carrying the
    /// message registration gives (`check_query_map`).
    #[test]
    fn a_query_map_registration_refuses_is_an_error() {
        let mut tm = dim_store_map();
        let table_schema = schema(
            "DW.DIM_STORE",
            vec![
                col("STORE_KEY", FieldType::Int64, true, Some(0.0)),
                col("NAME", FieldType::String, false, Some(0.1)),
                col("OPENED", FieldType::Date, false, None),
                col("FLOORS", FieldType::Int32, false, None),
                col("SCORE", FieldType::Float64, false, None),
            ],
        );
        let schemas = HashMap::from([("DW.DIM_STORE".to_string(), table_schema.clone())]);
        let cases = [
            (
                "SELECT STORE_KEY FROM DW.DIM_STORE",
                DiagCode::CompileError,
                None,
                "selects STORE_KEY, but the map reads NAME",
            ),
            (
                "SELECT * FROM DW.DIM_STORE WHERE OPENED > '2024-01-01'",
                DiagCode::CompileError,
                None,
                "column 'OPENED' is date",
            ),
            (
                "SELECT * FROM DW.DIM_STORE WHERE FLOORS > 0.5",
                DiagCode::CompileError,
                None,
                "compare it with an integer, not 0.5",
            ),
            (
                "SELECT * FROM DW.DIM_STORE WHERE SCORE > 0.5",
                DiagCode::CompileError,
                None,
                "no exact binary value",
            ),
            (
                "SELECT * FROM DW.DIM_STORE WHERE floors = 1",
                DiagCode::ColumnNotFound,
                Some("floors"),
                "did you mean \"FLOORS\"",
            ),
        ];
        for (sql, code, column, needle) in cases {
            tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(sql);
            let compiled = CompiledR2rmlMapping::new(vec![tm.clone()]);
            let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
            let refusal =
                check_query_map(&compiled, &tm, Some(&read_schema(&table_schema))).expect_err(sql);
            let found = diags
                .iter()
                .find(|d| d.severity == Severity::Error && d.message == refusal.message)
                .unwrap_or_else(|| panic!("{sql}: no error carries {refusal:?}: {diags:?}"));
            assert!(found.message.contains(needle), "{sql}: {found:?}");
            assert!(found.message.contains("<#DimStore>"), "{sql}: {found:?}");
            assert_eq!(found.code, code, "{sql}");
            assert_eq!(found.column.as_deref(), column, "{sql}");
            assert_eq!(found.table.as_deref(), Some("DW.DIM_STORE"), "{sql}");
        }

        // A table that did not load still gets the checks that need no schema.
        tm.logical_table =
            fluree_db_r2rml::mapping::LogicalTable::sql_query("SELECT STORE_KEY FROM DW.DIM_STORE");
        let diags = cross_check_mapping(
            &CompiledR2rmlMapping::new(vec![tm]),
            &HashMap::new(),
            &no_errors(),
        );
        assert!(has_code(&diags, DiagCode::TableNotFound), "{diags:?}");
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("but the map reads NAME")),
            "{diags:?}"
        );
    }

    #[test]
    fn a_key_on_a_query_is_checked_against_its_table_and_select_list() {
        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), dim_store_schema());
        let on_query = |sql: &str, key: &[&str]| {
            let mut tm = dim_store_map().with_declared_key(key.iter().copied());
            tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(sql);
            CompiledR2rmlMapping::new(vec![tm])
        };
        for sql in [
            "SELECT * FROM DW.DIM_STORE WHERE NAME IS NOT NULL",
            "SELECT STORE_KEY, NAME FROM DW.DIM_STORE",
        ] {
            let diags = cross_check_mapping(&on_query(sql, &["STORE_KEY"]), &schemas, &no_errors());
            assert!(diags.is_empty(), "{sql}: {diags:?}");
        }
        let diags = cross_check_mapping(
            &on_query("SELECT NAME FROM DW.DIM_STORE", &["STORE_KEY"]),
            &schemas,
            &no_errors(),
        );
        let key: Vec<&Diagnostic> = diags
            .iter()
            .filter(|d| d.column.as_deref() == Some("STORE_KEY"))
            .collect();
        assert_eq!(key.len(), 1, "{diags:?}");
        assert_eq!(key[0].code, DiagCode::ColumnNotFound);
        assert_eq!(key[0].table.as_deref(), Some("DW.DIM_STORE"));
        assert!(
            key[0]
                .message
                .contains("the rr:sqlQuery over 'DW.DIM_STORE'")
                && key[0].message.contains("does not select"),
            "{:?}",
            key[0]
        );
        let diags = cross_check_mapping(
            &on_query("SELECT * FROM DW.DIM_STORE", &["STORE_ID"]),
            &schemas,
            &no_errors(),
        );
        assert!(
            diags.iter().any(|d| d.code == DiagCode::ColumnNotFound
                && d.column.as_deref() == Some("STORE_ID")
                && d.message.contains("which the table does not have")),
            "a key over a column the FROM table lacks: {diags:?}"
        );
    }

    #[test]
    fn declared_key_columns_are_checked_like_a_query_checks_them() {
        // Two maps over one table, each declaring keys: one diagnostic per
        // table and column, whichever map declared it.
        let mut schema = dim_store_schema();
        schema
            .columns
            .push(col("RATING", FieldType::Float64, false, Some(0.0)));
        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), schema);
        let keyed = |iri: &str, key: &[&str]| {
            let mut tm = dim_store_map().with_declared_key(key.iter().copied());
            tm.iri = iri.to_string();
            tm
        };
        let compiled = CompiledR2rmlMapping::new(vec![
            keyed("<#A>", &["STORE_KEY"]),
            keyed("<#B>", &["STORE_KEY", "NAME"]),
        ]);
        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(diags.is_empty(), "usable key columns are clean: {diags:?}");

        let compiled = CompiledR2rmlMapping::new(vec![
            keyed("<#A>", &["STORE_ID"]),
            keyed("<#B>", &["store_key", "RATING"]),
            keyed("<#C>", &["STORE_ID"]),
        ]);
        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        let by_column = |name: &str| -> Vec<&Diagnostic> {
            diags
                .iter()
                .filter(|d| d.column.as_deref() == Some(name))
                .collect()
        };
        let missing = by_column("STORE_ID");
        assert_eq!(missing.len(), 1, "{diags:?}");
        assert_eq!(missing[0].code, DiagCode::ColumnNotFound);
        let casing = by_column("store_key");
        assert_eq!(casing.len(), 1, "{diags:?}");
        assert_eq!(casing[0].code, DiagCode::CasingMismatch);
        let float = by_column("RATING");
        assert_eq!(float.len(), 1, "{diags:?}");
        assert_eq!(float[0].code, DiagCode::NonKeyTypeSkipped);
        for d in missing.iter().chain(&casing).chain(&float) {
            assert_eq!(d.severity, Severity::Error, "{d:?}");
            assert!(d.message.contains("f:key on 'DW.DIM_STORE'"), "{d:?}");
        }
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
    fn graph_map_column_absent_flags_column_not_found() {
        // The graph template names `REGION`, which the table lacks. Unchecked,
        // every row would route to the default graph with nothing to say so.
        let mut tm = dim_store_map();
        tm.subject_map.graph_map = Some(GraphMap::template("http://ex/g/{REGION}"));
        let compiled = CompiledR2rmlMapping::new(vec![tm]);

        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), dim_store_schema());

        let diags = cross_check_mapping(&compiled, &schemas, &no_errors());
        assert!(has_code(&diags, DiagCode::ColumnNotFound), "{diags:?}");
        assert!(diags.iter().any(|d| d.column.as_deref() == Some("REGION")));
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

    /// A mapping that parses but defines no rr:TriplesMap (here the `rr:`
    /// prefix without its `#`) is the one registration refuses, so the dry-run
    /// reports it as a compile error rather than validating it clean.
    #[test]
    fn a_mapping_that_defines_no_triples_map_does_not_validate() {
        let empty = "@prefix rr: <http://www.w3.org/ns/r2rml> .\n\
                     <http://ex/m#M> a rr:TriplesMap ;\n\
                     rr:logicalTable [ rr:tableName \"ns.t\" ] .";
        let response = compile_for_validate(empty)
            .expect_err("a mapping that defines nothing must not validate");
        assert!(!response.compiled_ok);
        assert_eq!(response.triples_map_count, 0);
        assert!(
            has_code(&response.diagnostics, DiagCode::CompileError)
                && response
                    .diagnostics
                    .iter()
                    .any(|d| d.message.contains("defines no rr:TriplesMap")),
            "{:?}",
            response.diagnostics
        );
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

    // ----- keyless maps (`keylessMap`) -----

    /// `DW.DIM_STORE` with `STORE_KEY` as field 1 and `NAME` as field 2, and
    /// `identifier` as its identifier field ids, at `format_version`.
    fn identified_dim_store(identifier: Vec<i32>, format_version: i32) -> TableSchema {
        let mut s = dim_store_schema();
        for (i, c) in s.columns.iter_mut().enumerate() {
            c.field_id = i32::try_from(i).unwrap() + 1;
        }
        s.identifier_field_ids = identifier;
        s.format_version = format_version;
        s
    }

    /// The `keylessMap` advisories a dry-run gives `maps` over `table`.
    fn keyless(maps: Vec<TriplesMap>, table: TableSchema) -> Vec<Diagnostic> {
        let compiled = CompiledR2rmlMapping::new(maps);
        let mut schemas = HashMap::new();
        schemas.insert("DW.DIM_STORE".to_string(), table);
        keyless_map_diagnostics(
            &compiled,
            &preview_table_keys(&compiled, &schemas),
            QueryTables::FromTable,
            KeyRemedy::DeclareOrIdentifierFields,
        )
    }

    /// A map no key proves subject-unique is named, with the `f:key` that
    /// would prove it, as an advisory, never an error.
    #[test]
    fn a_keyless_map_is_advised_with_its_remedy() {
        let diags = keyless(vec![dim_store_map()], identified_dim_store(vec![], 2));
        assert_eq!(diags.len(), 1, "{diags:?}");
        let d = &diags[0];
        assert_eq!(
            (d.severity, d.code, d.table.as_deref()),
            (
                Severity::Advisory,
                DiagCode::KeylessMap,
                Some("DW.DIM_STORE")
            )
        );
        for part in [
            "TriplesMap <<#DimStore>>",
            "(STORE_KEY)",
            "it has no known key",
            "remove repeated solutions",
            r#"If STORE_KEY identifies a row, declare `f:key ( "STORE_KEY" )` on its logical table, or make it the table's identifier field."#,
        ] {
            assert!(d.message.contains(part), "{part}: {}", d.message);
        }
        assert!(!has_error(&diags));
    }

    /// A key from any route that covers the subject silences it: a declared
    /// one, or the table's identifier fields while the format honors them.
    #[test]
    fn a_map_with_key_evidence_is_not_advised() {
        let declared = dim_store_map().with_declared_key(["STORE_KEY"]);
        assert!(keyless(vec![declared], identified_dim_store(vec![], 2)).is_empty());
        assert!(keyless(vec![dim_store_map()], identified_dim_store(vec![1], 2)).is_empty());
        assert!(keyless(vec![dim_store_map()], identified_dim_store(vec![1], 3)).is_empty());
        // A version 4 reader ignores identifier fields.
        assert_eq!(
            keyless(vec![dim_store_map()], identified_dim_store(vec![1], 4)).len(),
            1
        );
        // A template covering the key beside another column is unique too.
        let mut wide = dim_store_map();
        wide.subject_map.template = Some("http://ex/store/{STORE_KEY}/{NAME}".to_string());
        assert!(keyless(vec![wide], identified_dim_store(vec![1], 2)).is_empty());
    }

    /// A key the subject does not cover leaves the map advised, naming the
    /// keys it has; a template that runs values together says so.
    #[test]
    fn a_key_the_subject_cannot_use_is_named() {
        let diags = keyless(vec![dim_store_map()], identified_dim_store(vec![2], 2));
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(
            diags[0].message.contains("its known keys are (NAME)"),
            "{}",
            diags[0].message
        );

        let mut merged = dim_store_map().with_declared_key(["STORE_KEY", "NAME"]);
        merged.subject_map.template = Some("http://ex/store/{STORE_KEY}-{NAME}".to_string());
        let diags = keyless(vec![merged], identified_dim_store(vec![], 2));
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(
            diags[0].message.contains("can run two values together"),
            "{}",
            diags[0].message
        );
    }

    /// Over a filtered logical table, the identifier fields count only when
    /// the query selects them.
    #[test]
    fn identifier_fields_count_where_the_query_selects_them() {
        let over = |sql: &str| {
            let mut tm = dim_store_map();
            tm.logical_table = fluree_db_r2rml::mapping::LogicalTable::sql_query(sql);
            keyless(vec![tm], identified_dim_store(vec![1], 2))
        };
        assert!(over("SELECT * FROM DW.DIM_STORE WHERE NAME IS NOT NULL").is_empty());
        assert!(over("SELECT STORE_KEY, NAME FROM DW.DIM_STORE WHERE NAME IS NOT NULL").is_empty());
        let diags = over("SELECT NAME FROM DW.DIM_STORE WHERE NAME IS NOT NULL");
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(
            diags[0]
                .message
                .contains("its rr:sqlQuery logical table (over 'DW.DIM_STORE')"),
            "{}",
            diags[0].message
        );
    }

    /// A table the preview did not resolve gets no advisory: its keys are
    /// unknown, and it has its `TableNotFound`.
    #[test]
    fn an_unresolved_table_is_not_advised() {
        let compiled = CompiledR2rmlMapping::new(vec![dim_store_map()]);
        let keys = preview_table_keys(&compiled, &HashMap::new());
        assert!(keys.is_empty());
        assert!(keyless_map_diagnostics(
            &compiled,
            &keys,
            QueryTables::FromTable,
            KeyRemedy::DeclareOrIdentifierFields
        )
        .is_empty());
    }

    /// Delta's remedy is the declaration alone, and a generated mapping (no
    /// `f:key`) is advised before it is saved.
    #[test]
    fn a_generated_mapping_without_keys_is_advised() {
        let turtle = r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            <http://ex/mapping#Orders> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "sales.orders" ] ;
                rr:subjectMap [ rr:template "http://ex/order/{order_id}" ] ;
                rr:predicateObjectMap [ rr:predicate <http://ex/total> ;
                                        rr:objectMap [ rr:column "total" ] ] .
        "#;
        let diags = keyless_generated_maps(
            turtle,
            declared_table_keys,
            QueryTables::Alias,
            KeyRemedy::Declare,
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(
            diags[0]
                .message
                .ends_with(r#"declare `f:key ( "order_id" )` on its logical table."#),
            "{}",
            diags[0].message
        );
        let keyed = turtle.replace(
            r#"rr:tableName "sales.orders" ]"#,
            r#"rr:tableName "sales.orders" ; <https://ns.flur.ee/db#key> ( "order_id" ) ]"#,
        );
        assert!(keyless_generated_maps(
            &keyed,
            declared_table_keys,
            QueryTables::Alias,
            KeyRemedy::Declare
        )
        .is_empty());
    }
}
