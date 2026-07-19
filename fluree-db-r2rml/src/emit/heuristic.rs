//! The deterministic PK-selection + FK-inference heuristic.
//!
//! Two phases, mirroring the spec:
//!
//! - **Phase 1** builds one [`TableMapping`] per table — subject key, class,
//!   subject template, and a literal predicate-object mapping for every scalar
//!   column (nested columns skipped) — and indexes every single-column PK.
//! - **Phase 2** infers foreign keys from the complete PK index using
//!   name ∧ type ∧ range-containment, emits join mappings ONLY when exactly one
//!   parent survives, and records a diagnostic for every case it refuses to
//!   resolve. It never fabricates a join.
//!
//! The output IR encodes no answers the fixtures fed in — every FK is derived
//! here from schema + stats alone.

use std::collections::HashSet;

use fluree_db_tabular::FieldType;

use crate::emit::diagnostic::{DiagCode, Diagnostic, Severity};
use crate::emit::input::{EmitColumn, EmitTableSchema, TypedBound};
use crate::emit::ir::{
    ColumnMapping, ForeignKey, PrefixDecl, StructuredR2rmlMapping, TableMapping,
};
use crate::emit::naming;
use crate::emit::{EmitOptions, SubjectStrategy};
use crate::vocab::R2RML;

/// The XSD namespace IRI (for the `xsd:` prefix declaration).
pub(crate) const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";

/// A single-column PK, indexed in Phase 1 and consulted in Phase 2.
struct PkEntry {
    /// Fully-qualified parent table name (`"DW.DIM_GEOGRAPHY"`).
    table_name: String,
    /// The parent key column name (`"GEOGRAPHY_KEY"`).
    pk_column: String,
    /// The parent key column type (for the type-match gate).
    field_type: FieldType,
    /// Typed lower/upper bounds (for range-containment), if known.
    min: Option<TypedBound>,
    max: Option<TypedBound>,
    /// Whether the parent is a fact table (for the child-fact→hub advisory).
    is_fact: bool,
}

/// Per-table Phase-1 intermediate carried into Phase 2.
struct TableDraft {
    class_iri: String,
    subject_template: String,
    /// Literal mappings for every scalar column, in `field_id` order.
    literals: Vec<ColumnMapping>,
    /// The set of camelCase predicate local-names used by literals (for join
    /// predicate collision avoidance).
    literal_locals: HashSet<String>,
    /// The subject-key column names (excluded from the FK pass).
    subject_key_columns: HashSet<String>,
}

/// Build the authoritative [`StructuredR2rmlMapping`] plus diagnostics from the
/// table inputs. Pure and deterministic.
pub fn build_mapping(
    tables: &[EmitTableSchema],
    opts: &EmitOptions,
) -> (StructuredR2rmlMapping, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let mut pk_index: Vec<PkEntry> = Vec::new();
    let mut drafts: Vec<TableDraft> = Vec::with_capacity(tables.len());

    // -- Phase 1: per-table subject + literals; build the PK index. --
    for table in tables {
        let draft = build_table_draft(table, opts, &mut pk_index, &mut diagnostics);
        drafts.push(draft);
    }

    // -- Phase 2: FK inference against the complete PK index. --
    let mut table_mappings = Vec::with_capacity(tables.len());
    for (table, draft) in tables.iter().zip(drafts) {
        let (joins, resolved_fk_cols) = if opts.emit_fk_joins {
            infer_foreign_keys(table, &draft, &pk_index, opts, &mut diagnostics)
        } else {
            (Vec::new(), HashSet::new())
        };

        // Assemble columns: literals (optionally dropping resolved-FK keys) then
        // joins. The subject-key literal is always retained.
        let mut columns: Vec<ColumnMapping> = draft
            .literals
            .into_iter()
            .filter(|lit| {
                opts.keep_fk_keys_as_literals
                    || lit.is_subject_id
                    || !resolved_fk_cols.contains(&lit.column_name)
            })
            .collect();
        columns.extend(joins);

        table_mappings.push(TableMapping {
            table_name: table.qualified_name(),
            class_iri: draft.class_iri,
            subject_template: draft.subject_template,
            columns,
        });
    }

    let mapping = StructuredR2rmlMapping {
        base_namespace: opts.base_namespace.clone(),
        prefixes: vec![
            PrefixDecl {
                prefix: "rr".to_string(),
                namespace: R2RML::NS.to_string(),
            },
            PrefixDecl {
                prefix: "xsd".to_string(),
                namespace: XSD_NS.to_string(),
            },
            PrefixDecl {
                prefix: opts.vocab_prefix.clone(),
                namespace: opts.base_namespace.clone(),
            },
        ],
        table_mappings,
    };

    (mapping, diagnostics)
}

/// Phase 1 for a single table.
fn build_table_draft(
    table: &EmitTableSchema,
    opts: &EmitOptions,
    pk_index: &mut Vec<PkEntry>,
    diagnostics: &mut Vec<Diagnostic>,
) -> TableDraft {
    let stem = table.stem();
    let table_override = opts.per_table_overrides.get(&table.key());

    // Class name + subject slug. A per-table `class_name` override replaces the
    // stem-derived pair: the override is used verbatim as the `rr:class`
    // ClassName, and the subject slug is its kebab-case rendering (the same case
    // rule `class_slug` applies to a stem). An absent/`None` override reproduces
    // the stem-derived defaults byte-for-byte.
    let (class_local_name, subject_slug) =
        match table_override.and_then(|o| o.class_name.as_deref()) {
            Some(class_name) => (class_name.to_string(), naming::kebab_case(class_name)),
            None => (naming::class_local_name(stem), naming::class_slug(stem)),
        };
    let class_iri = format!("{}{}", opts.base_namespace, class_local_name);

    // Subject key selection. A per-table `primary_key` override REPLACES
    // `identifier_field_ids` as the subject key (validated + always unverified).
    // The effective strategy is the per-table override's, else the global.
    let strategy = table_override
        .and_then(|o| o.subject_strategy)
        .unwrap_or(opts.subject_strategy);
    let subject_key = select_subject_key(
        table,
        table_override.and_then(|o| o.primary_key.as_deref()),
        strategy,
        diagnostics,
    );
    let subject_key_columns: HashSet<String> = subject_key.columns.iter().cloned().collect();

    let subject_template = if subject_key.columns.is_empty() {
        String::new()
    } else {
        let placeholders: String = subject_key
            .columns
            .iter()
            .map(|c| format!("{{{c}}}"))
            .collect::<Vec<_>>()
            .join("/");
        format!(
            "{}{}/{}",
            naming::subject_base(&opts.base_namespace),
            subject_slug,
            placeholders
        )
    };

    // Index a single-column, join-eligible PK for the FK pass. A synthesized
    // composite fallback (`index_as_pk == false`) is never a unique key, so it
    // must not become an FK parent even when it happens to be one column.
    if subject_key.columns.len() == 1 && subject_key.index_as_pk {
        let pk_name = &subject_key.columns[0];
        if let Some(col) = table.columns.iter().find(|c| &c.name == pk_name) {
            pk_index.push(PkEntry {
                table_name: table.qualified_name(),
                pk_column: pk_name.clone(),
                field_type: col.field_type,
                min: col.stats.min,
                max: col.stats.max,
                is_fact: table.is_fact(),
            });
        }
    }

    // Literal predicate-object mappings for every scalar column.
    let mut literals = Vec::new();
    let mut literal_locals = HashSet::new();
    for col in &table.columns {
        if col.nested {
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::NestedColumnSkipped,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "column '{}' is a nested struct/list/map; R2RML addresses flat columns only",
                    col.name
                ),
            ));
            continue;
        }

        let local = naming::camel_case(&col.name);
        let predicate_iri = format!("{}{}", opts.base_namespace, local);
        let datatype = naming::xsd_datatype(col.field_type, opts.xsd_long_as_integer)
            .map(std::string::ToString::to_string);
        let is_subject_id = subject_key_columns.contains(&col.name);
        literal_locals.insert(local);
        literals.push(ColumnMapping::literal(
            col.name.clone(),
            predicate_iri,
            datatype,
            is_subject_id,
        ));
    }

    TableDraft {
        class_iri,
        subject_template,
        literals,
        literal_locals,
        subject_key_columns,
    }
}

/// Choose the subject key.
///
/// Precedence: a per-table `primary_key` override → Iceberg `identifier_field_ids`
/// → a `<STEM>_KEY`/`<STEM>_ID` name fallback → (strategy-dependent) either a
/// synthesized composite subject or no subject.
///
/// The override and identifier paths are validated STRICTLY under both
/// strategies (a missing / nullable column earns `NoSafeSubjectKey` and no
/// subject): an explicit override is the caller's assertion, and a nullable
/// Iceberg identifier is a schema contradiction that must not be silently
/// indexed as an FK parent. The [`SubjectStrategy`] governs ONLY the
/// name-fallback / keyless tail:
///
/// - [`SubjectStrategy::Auto`] (default): a name-fallback column that is not
///   *provably* non-null is USED anyway — downgraded from `NoSafeSubjectKey` to
///   a non-blocking `SubjectKeyUnverified`, but NOT indexed as an FK parent
///   (same policy as a nullable declared identifier: a nullable / uniqueness-
///   unverifiable parent key would silently change join cardinality) — and a
///   table with no key-like column at all gets a deterministic COMPOSITE
///   subject (`SubjectKeySynthesized`) so it stays saveable.
/// - [`SubjectStrategy::Identifier`] (strict, the pre-change behavior): a
///   nullable name key or a keyless table yields `NoSafeSubjectKey`, no subject.
///
/// Never fabricates a surrogate row id (Iceberg exposes no stable logical row id).
fn select_subject_key(
    table: &EmitTableSchema,
    override_primary_key: Option<&[String]>,
    strategy: SubjectStrategy,
    diagnostics: &mut Vec<Diagnostic>,
) -> SubjectKey {
    // -- Per-table `primary_key` override: replaces identifier_field_ids. --
    if let Some(pk_cols) = override_primary_key {
        return select_override_subject_key(table, pk_cols, diagnostics);
    }

    // -- Iceberg identifier_field_ids (nullable identifier handled per strategy). --
    if !table.identifier_field_ids.is_empty() {
        return select_identifier_subject_key(table, strategy, diagnostics);
    }

    // -- Name fallback: a `<STEM>_KEY` / `<STEM>_ID` column. --
    let marker_stem = naming::strip_table_marker(table.stem());
    let candidates = [format!("{marker_stem}_KEY"), format!("{marker_stem}_ID")];
    if let Some(col) = table
        .columns
        .iter()
        .find(|c| candidates.iter().any(|cand| cand == &c.name))
    {
        // A proven-non-null name key is safe under both strategies (only its
        // uniqueness is unverifiable).
        if col.is_non_null() {
            push_subject_key_unverified(
                table,
                col,
                "chosen by name+required fallback",
                diagnostics,
            );
            return SubjectKey::single(col.name.clone());
        }
        // Not provably non-null: the strategy decides.
        return match strategy {
            SubjectStrategy::Auto => {
                // Use it anyway — DOWNGRADE NoSafeSubjectKey → SubjectKeyUnverified —
                // but do NOT index it as an FK parent, mirroring the nullable-
                // identifier policy: saveability only needs a subject, while an
                // FK parent needs a provably-non-null key (a nullable / dup-able
                // parent key silently changes join cardinality with no diagnostic).
                // A caller that KNOWS the column is a real PK asserts it via the
                // per-table `primary_key` override, which does index.
                diagnostics.push(Diagnostic::new(
                    Severity::Warning,
                    DiagCode::SubjectKeyUnverified,
                    table.qualified_name(),
                    Some(col.name.clone()),
                    format!(
                        "subject key '{}' chosen by name fallback; NOT provably non-null \
                         (no `required`, no null_fraction==0) and uniqueness is unverifiable \
                         metadata-only — emitted so the table is saveable, but NOT indexed \
                         as an FK parent (assert it with a per-table primary_key override \
                         to enable joins)",
                        col.name
                    ),
                ));
                SubjectKey {
                    columns: vec![col.name.clone()],
                    index_as_pk: false,
                }
            }
            SubjectStrategy::Identifier => {
                diagnostics.push(Diagnostic::new(
                    Severity::Error,
                    DiagCode::NoSafeSubjectKey,
                    table.qualified_name(),
                    Some(col.name.clone()),
                    format!(
                        "candidate subject key '{}' is nullable; no safe subject key",
                        col.name
                    ),
                ));
                SubjectKey::none()
            }
        };
    }

    // -- No key-like column at all: the strategy decides. --
    match strategy {
        SubjectStrategy::Auto => synthesize_composite_subject_key(table, diagnostics),
        SubjectStrategy::Identifier => {
            diagnostics.push(Diagnostic::new(
                Severity::Error,
                DiagCode::NoSafeSubjectKey,
                table.qualified_name(),
                None,
                "no identifier_field_ids and no required <STEM>_KEY/<STEM>_ID column; \
                 emitting no subject (never inventing a surrogate row id)"
                    .to_string(),
            ));
            SubjectKey::none()
        }
    }
}

/// Per-table `primary_key` override handling (one or more columns), validated
/// strictly under both strategies. A single column is a simple key (indexed as
/// an FK parent); multiple columns are a composite subject template.
fn select_override_subject_key(
    table: &EmitTableSchema,
    pk_cols: &[String],
    diagnostics: &mut Vec<Diagnostic>,
) -> SubjectKey {
    if pk_cols.is_empty() {
        diagnostics.push(Diagnostic::new(
            Severity::Error,
            DiagCode::NoSafeSubjectKey,
            table.qualified_name(),
            None,
            "per-table primary_key override is an empty column list; no safe subject key"
                .to_string(),
        ));
        return SubjectKey::none();
    }
    let mut columns = Vec::with_capacity(pk_cols.len());
    for pk_name in pk_cols {
        let col = match table.columns.iter().find(|c| &c.name == pk_name) {
            Some(col) => col,
            None => {
                diagnostics.push(Diagnostic::new(
                    Severity::Error,
                    DiagCode::NoSafeSubjectKey,
                    table.qualified_name(),
                    Some(pk_name.clone()),
                    format!(
                        "per-table primary_key override '{pk_name}' is not a column of the \
                         table; no safe subject key"
                    ),
                ));
                return SubjectKey::none();
            }
        };
        if !col.is_non_null() {
            diagnostics.push(Diagnostic::new(
                Severity::Error,
                DiagCode::NoSafeSubjectKey,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "per-table primary_key override '{}' is nullable (fails required / \
                     null_fraction==0); no safe subject key",
                    col.name
                ),
            ));
            return SubjectKey::none();
        }
        push_subject_key_unverified(table, col, "set by per-table override", diagnostics);
        columns.push(col.name.clone());
    }
    // A single-column override is a simple key (indexed as an FK parent); a
    // composite (len > 1) is a subject template that is never indexed.
    let index_as_pk = columns.len() == 1;
    SubjectKey {
        columns,
        index_as_pk,
    }
}

/// Iceberg `identifier_field_ids` subject-key handling.
///
/// Iceberg requires identifier fields to be `required`, but a non-conforming
/// writer (e.g. Snowflake-managed Iceberg) can populate `identifier_field_ids`
/// without marking the column `required`. A nullable declared identifier is
/// resolved per [`SubjectStrategy`], mirroring the name-fallback path:
/// - [`SubjectStrategy::Auto`]: ADOPT it as the subject (downgrade
///   `NoSafeSubjectKey` → `SubjectKeyUnverified`) so the table stays browsable,
///   but do NOT index it as an FK parent — a nullable parent key would silently
///   drop child rows at join time, and rows with a NULL key are unaddressable.
/// - [`SubjectStrategy::Identifier`] (strict): emit `NoSafeSubjectKey`, no
///   subject.
///
/// A declared identifier proven non-null is always adopted (uniqueness alone is
/// unverifiable metadata-only, so it earns `SubjectKeyUnverified`). Malformed
/// metadata (a field id with no matching column) is always `NoSafeSubjectKey`.
fn select_identifier_subject_key(
    table: &EmitTableSchema,
    strategy: SubjectStrategy,
    diagnostics: &mut Vec<Diagnostic>,
) -> SubjectKey {
    let mut columns = Vec::new();
    // Every declared identifier column proven non-null? Only then may a
    // single-column key be indexed as an FK parent.
    let mut all_non_null = true;
    for &fid in &table.identifier_field_ids {
        let col = match table.column_by_field_id(fid) {
            Some(col) => col,
            None => {
                diagnostics.push(Diagnostic::new(
                    Severity::Error,
                    DiagCode::NoSafeSubjectKey,
                    table.qualified_name(),
                    None,
                    format!("identifier_field_ids references unknown field id {fid}"),
                ));
                return SubjectKey::none();
            }
        };
        if col.is_non_null() {
            // Uniqueness is still unverifiable metadata-only (NDV deferred).
            push_subject_key_unverified(
                table,
                col,
                "from Iceberg identifier_field_ids",
                diagnostics,
            );
        } else {
            match strategy {
                SubjectStrategy::Auto => {
                    // Adopt for browsability, but not as an FK parent: a nullable
                    // declared identifier is the Snowflake-managed-Iceberg case
                    // that previously produced an empty subject template and then
                    // 500'd at scan time ("Subject map must have rr:template,
                    // rr:column, or rr:constant").
                    all_non_null = false;
                    diagnostics.push(Diagnostic::new(
                        Severity::Warning,
                        DiagCode::SubjectKeyUnverified,
                        table.qualified_name(),
                        Some(col.name.clone()),
                        format!(
                            "subject key '{}' from Iceberg identifier_field_ids is NOT provably \
                             non-null (a non-conforming writer set identifier_field_ids without \
                             marking the column `required`); adopted so the table stays browsable, \
                             but NOT indexed as an FK parent — rows with a NULL key are \
                             unaddressable",
                            col.name
                        ),
                    ));
                }
                SubjectStrategy::Identifier => {
                    diagnostics.push(Diagnostic::new(
                        Severity::Error,
                        DiagCode::NoSafeSubjectKey,
                        table.qualified_name(),
                        Some(col.name.clone()),
                        format!(
                            "identifier_field_ids column '{}' is nullable (fails required / \
                             null_fraction==0); no safe subject key",
                            col.name
                        ),
                    ));
                    return SubjectKey::none();
                }
            }
        }
        columns.push(col.name.clone());
    }
    // A nullable-adopted (Auto) key is never an FK parent; a composite (len > 1)
    // is never a unique key.
    let index_as_pk = columns.len() == 1 && all_non_null;
    SubjectKey {
        columns,
        index_as_pk,
    }
}

/// Synthesize a deterministic COMPOSITE subject over the row's columns when no
/// key-like column exists (Auto strategy only). The column set is the
/// PROVABLY-NON-NULL non-nested columns in `field_id` order when at least one
/// exists, else every non-nested column — the closest stable approximation of
/// row identity without a hash (R2RML templates cannot hash).
///
/// The non-null subset is preferred because template expansion produces NO
/// subject for a row with a NULL in ANY referenced column — templating over a
/// nullable column silently drops every row where it is NULL, which is exactly
/// the population keyless tables tend to have. Restricting to non-null columns
/// trades that invisible row loss for a visible one: rows identical across the
/// (narrower) subset collapse into one subject with merged values. Both hazards
/// are disclosed in the `SubjectKeySynthesized` diagnostic. The subject is
/// NEVER indexed as an FK parent (it is not a unique key).
fn synthesize_composite_subject_key(
    table: &EmitTableSchema,
    diagnostics: &mut Vec<Diagnostic>,
) -> SubjectKey {
    let flat: Vec<&EmitColumn> = table.columns.iter().filter(|c| !c.nested).collect();
    if flat.is_empty() {
        // Degenerate: nothing flat to key on (all columns nested).
        diagnostics.push(Diagnostic::new(
            Severity::Error,
            DiagCode::NoSafeSubjectKey,
            table.qualified_name(),
            None,
            "no key-like column and no flat columns to synthesize a composite subject from; \
             emitting no subject"
                .to_string(),
        ));
        return SubjectKey::none();
    }
    let non_null: Vec<String> = flat
        .iter()
        .filter(|c| c.is_non_null())
        .map(|c| c.name.clone())
        .collect();
    let (columns, message) = if non_null.is_empty() {
        // No provably-non-null column: template over everything and DISCLOSE
        // the null-drop — this is the only synthesized shape that loses rows.
        let all: Vec<String> = flat.iter().map(|c| c.name.clone()).collect();
        let msg = format!(
            "no key-like column and no provably-non-null column; synthesized a deterministic \
             composite subject over all {} flat columns so the table is saveable — the subject \
             is neither unique nor an FK parent, rows identical across all columns collapse, \
             and ROWS WITH A NULL IN ANY COLUMN WILL NOT APPEAR in the virtual dataset \
             (R2RML template expansion emits no subject for them); set a per-table \
             primary_key/subject_key override to control this",
            all.len()
        );
        (all, msg)
    } else {
        let msg = format!(
            "no key-like column; synthesized a deterministic composite subject over the {} \
             provably-non-null flat column(s) (of {} flat) so the table is saveable and no \
             row is dropped for a NULL — the subject is neither unique nor an FK parent, and \
             rows identical across those column(s) collapse into one subject with merged \
             values; set a per-table primary_key/subject_key override to control this",
            non_null.len(),
            flat.len()
        );
        (non_null, msg)
    };
    diagnostics.push(Diagnostic::new(
        Severity::Warning,
        DiagCode::SubjectKeySynthesized,
        table.qualified_name(),
        None,
        message,
    ));
    SubjectKey {
        columns,
        index_as_pk: false,
    }
}

/// Emit the standard `SubjectKeyUnverified` warning (uniqueness is unprovable
/// metadata-only — NDV deferred) for a chosen key column.
fn push_subject_key_unverified(
    table: &EmitTableSchema,
    col: &EmitColumn,
    origin: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    diagnostics.push(Diagnostic::new(
        Severity::Warning,
        DiagCode::SubjectKeyUnverified,
        table.qualified_name(),
        Some(col.name.clone()),
        format!(
            "subject key '{}' {origin}; uniqueness is unverifiable metadata-only (NDV deferred)",
            col.name
        ),
    ));
}

/// Selected subject key (empty `columns` ⇒ no safe subject key).
struct SubjectKey {
    columns: Vec<String>,
    /// Whether this key may be indexed as a single-column FK parent PK: true for
    /// a genuine single-column key (identifier / single override / name
    /// fallback), false for a synthesized composite (not a unique key).
    index_as_pk: bool,
}

impl SubjectKey {
    /// A single-column genuine key — indexed as an FK parent.
    fn single(column: String) -> Self {
        Self {
            columns: vec![column],
            index_as_pk: true,
        }
    }

    /// No safe subject key — the table emits no subject.
    fn none() -> Self {
        Self {
            columns: Vec::new(),
            index_as_pk: false,
        }
    }
}

/// Phase 2 for a single table: returns the join mappings and the set of child
/// columns that were resolved to a join.
fn infer_foreign_keys(
    table: &EmitTableSchema,
    draft: &TableDraft,
    pk_index: &[PkEntry],
    opts: &EmitOptions,
    diagnostics: &mut Vec<Diagnostic>,
) -> (Vec<ColumnMapping>, HashSet<String>) {
    let mut joins = Vec::new();
    let mut resolved = HashSet::new();
    // Predicate locals already emitted by joins in THIS table, so a later join
    // whose local collides with an earlier one is disambiguated (not merged).
    let mut emitted_join_locals: HashSet<String> = HashSet::new();

    for col in &table.columns {
        // FK candidacy: non-nested, non-subject-key columns only.
        if col.nested || draft.subject_key_columns.contains(&col.name) {
            continue;
        }
        // A key-NAMED column whose TYPE is not key-like (not an integer or
        // scale-0 decimal) would be silently dropped before the name match —
        // surface it so the miss is explainable, then skip it.
        if !col.is_key_type() {
            if col.is_key_like() {
                diagnostics.push(Diagnostic::new(
                    Severity::Warning,
                    DiagCode::NonKeyTypeSkipped,
                    table.qualified_name(),
                    Some(col.name.clone()),
                    format!(
                        "'{}' is named like a key but its type ('{}') is not key-like (not an \
                         integer or scale-0 decimal); excluded from FK inference",
                        col.name, col.iceberg_type
                    ),
                ));
            }
            continue;
        }

        // Parents matching on NAME ∧ TYPE. A single match joins on name∧type
        // ALONE when bounds are absent (Snowflake often supplies no integer
        // bounds) but is VETOED when both sides carry complete bounds and
        // containment provably fails; multiple matches are DISAMBIGUATED by
        // range-containment (never a hard gate).
        let name_type = candidate_parents(table, col, pk_index);
        let mut range_vetoed: Option<&PkEntry> = None;
        let chosen: Option<&PkEntry> = match name_type.as_slice() {
            [] => None,
            [only] => {
                if range_refuted(col.stats.min, col.stats.max, only.min, only.max) {
                    range_vetoed = Some(only);
                    None
                } else {
                    Some(only)
                }
            }
            many => {
                let contained: Vec<&PkEntry> = many
                    .iter()
                    .copied()
                    .filter(|pk| range_contained(col.stats.min, col.stats.max, pk.min, pk.max))
                    .collect();
                // Range disambiguates only when it narrows to EXACTLY one parent.
                match contained.as_slice() {
                    [one] => Some(*one),
                    _ => None,
                }
            }
        };

        if let Some(parent) = chosen {
            let fk = ForeignKey {
                target_table: parent.table_name.clone(),
                child_column: col.name.clone(),
                parent_column: parent.pk_column.clone(),
            };
            let predicate_iri = join_predicate(&col.name, draft, &mut emitted_join_locals, opts);
            joins.push(ColumnMapping::join(col.name.clone(), predicate_iri, fk));
            resolved.insert(col.name.clone());

            // Child-fact → hub advisory: both sides fact, joining on the parent's
            // PK (always true here — we only ever join to a PK).
            if table.is_fact() && parent.is_fact {
                diagnostics.push(Diagnostic::new(
                    Severity::Advisory,
                    DiagCode::FactHubJoinAdvisory,
                    table.qualified_name(),
                    Some(col.name.clone()),
                    format!(
                        "child-fact→hub join '{}' → {}.{} is a bounded PK point-lookup; \
                         emitted, but flagged as a perf advisory",
                        col.name, parent.table_name, parent.pk_column
                    ),
                ));
            }
        } else if let Some(parent) = range_vetoed {
            // A single name∧type parent whose bounds CONTRADICT the child's:
            // both sides supplied complete [min,max] bounds and the child range
            // is not contained — joining would fabricate a provably-dangling
            // relationship, so keep the column literal and say why.
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::UnresolvedFkCandidate,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "'{}' matches {}.{} by name∧type but its value range {} is not contained \
                     in the parent's {}; kept literal, no join fabricated",
                    col.name,
                    parent.table_name,
                    parent.pk_column,
                    fmt_range(col.stats.min, col.stats.max),
                    fmt_range(parent.min, parent.max),
                ),
            ));
        } else if name_type.len() >= 2 {
            // Multiple name∧type parents that range-containment could not narrow
            // to one — ambiguous, never fabricated.
            let parents: Vec<String> = name_type
                .iter()
                .map(|p| format!("{}.{}", p.table_name, p.pk_column))
                .collect();
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::AmbiguousFk,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "'{}' matches multiple candidate parents ({}) and range-containment could \
                     not disambiguate; kept literal, no join fabricated",
                    col.name,
                    parents.join(", ")
                ),
            ));
        } else if col.is_key_like() {
            // No name∧type match at all for a key-named column.
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::UnresolvedFkCandidate,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "'{}' looks like a key but matches no known PK by name∧type; kept literal, \
                     no join fabricated",
                    col.name
                ),
            ));
        }
        // A non-key-like key-typed column with no match is an ordinary numeric
        // measure — no diagnostic.
    }

    (joins, resolved)
}

/// Collect the parents matching `col` on NAME ∧ TYPE.
///
/// Range-containment is deliberately NOT applied here: it is a DISAMBIGUATOR the
/// caller uses only when more than one parent survives. Requiring it as a hard
/// gate blocks correct joins whenever Snowflake supplies no integer min/max
/// bounds, so an exact name+type match on a single parent joins on its own —
/// unless both sides carry complete bounds that REFUTE containment
/// ([`range_refuted`]), in which case the caller vetoes the join.
fn candidate_parents<'a>(
    table: &EmitTableSchema,
    col: &EmitColumn,
    pk_index: &'a [PkEntry],
) -> Vec<&'a PkEntry> {
    pk_index
        .iter()
        .filter(|pk| {
            // (1) Name: exact, or an unambiguous role-prefixed `_<PK>` suffix.
            let name_match =
                col.name == pk.pk_column || col.name.ends_with(&format!("_{}", pk.pk_column));
            if !name_match {
                return false;
            }
            // A PK never joins to its own row via its own name in its own table.
            if pk.table_name == table.qualified_name() && pk.pk_column == col.name {
                return false;
            }
            // (2) Type-match (integers exact; scale-0 decimals mutually).
            key_types_match(col.field_type, pk.field_type)
        })
        .collect()
}

/// Whether a child column and a parent PK have compatible KEY types.
///
/// Integers match integers of the same width (preserving the pre-change exact
/// `FieldType` equality); any two scale-0 decimals match regardless of precision
/// (`decimal(38,0)` ↔ `decimal(18,0)`, i.e. Snowflake `NUMBER(n,0)` surrogate
/// keys); integers and decimals never cross-match (conservative — avoids
/// fabricating joins across a mixed-type schema).
fn key_types_match(child: FieldType, parent: FieldType) -> bool {
    match (child, parent) {
        (FieldType::Decimal { scale: cs, .. }, FieldType::Decimal { scale: ps, .. }) => {
            cs == 0 && ps == 0
        }
        (c, p) => c == p,
    }
}

/// True iff BOTH sides carry complete `[min,max]` bounds AND the child range is
/// NOT contained in the parent's — i.e. the join is provably contradicted by
/// the stats. Any missing bound ⇒ cannot refute ⇒ `false` (absence of bounds
/// never blocks a single name∧type match; that is the Snowflake no-bounds case
/// the disambiguator-not-gate design exists for).
fn range_refuted(
    child_min: Option<TypedBound>,
    child_max: Option<TypedBound>,
    parent_min: Option<TypedBound>,
    parent_max: Option<TypedBound>,
) -> bool {
    match (child_min, child_max, parent_min, parent_max) {
        (Some(cmin), Some(cmax), Some(pmin), Some(pmax)) => !(pmin <= cmin && cmax <= pmax),
        _ => false,
    }
}

/// Render an optional `[min,max]` bound pair for a diagnostic message.
fn fmt_range(min: Option<TypedBound>, max: Option<TypedBound>) -> String {
    let f = |b: Option<TypedBound>| match b {
        Some(TypedBound::Int(v)) => v.to_string(),
        None => "?".to_string(),
    };
    format!("[{},{}]", f(min), f(max))
}

/// True iff `child [min,max] ⊆ parent [min,max]`. Any missing bound ⇒ cannot
/// confirm ⇒ `false` (never fabricate an unconfirmed join).
fn range_contained(
    child_min: Option<TypedBound>,
    child_max: Option<TypedBound>,
    parent_min: Option<TypedBound>,
    parent_max: Option<TypedBound>,
) -> bool {
    match (child_min, child_max, parent_min, parent_max) {
        (Some(cmin), Some(cmax), Some(pmin), Some(pmax)) => pmin <= cmin && cmax <= pmax,
        _ => false,
    }
}

/// Derive the join predicate IRI for a resolved FK on `child_column`.
///
/// Uses `camelCase(strip_key_suffix(child))` (readable: `geography`,
/// `destGeography`), disambiguating with a `Ref` suffix when that local would
/// collide with EITHER an existing literal predicate (e.g. `orderDate` literal
/// vs. `ORDER_DATE_KEY` join → `orderDateRef`) OR a local already emitted by an
/// earlier join in the same table (e.g. `GEOGRAPHY_KEY` and `GEOGRAPHY_ID` both
/// strip+camel to `geography` → the second becomes `geographyRef`). Without the
/// second check, two FKs to different parents would emit the same predicate IRI
/// and silently merge two distinct relationships. Further collisions append an
/// increasing counter so the local always stays unique.
fn join_predicate(
    child_column: &str,
    draft: &TableDraft,
    emitted_join_locals: &mut HashSet<String>,
    opts: &EmitOptions,
) -> String {
    let base_local = naming::camel_case(naming::strip_key_suffix(child_column));
    let mut local = base_local.clone();
    let mut suffix = 1u32;
    while draft.literal_locals.contains(&local) || emitted_join_locals.contains(&local) {
        local = if suffix == 1 {
            format!("{base_local}Ref")
        } else {
            format!("{base_local}Ref{suffix}")
        };
        suffix += 1;
    }
    emitted_join_locals.insert(local.clone());
    format!("{}{}", opts.base_namespace, local)
}
