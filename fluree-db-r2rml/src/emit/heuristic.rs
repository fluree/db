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
use crate::emit::EmitOptions;
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
    let subject_key = select_subject_key(
        table,
        table_override.and_then(|o| o.primary_key.as_deref()),
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

    // Index a single-column PK for the FK pass.
    if subject_key.columns.len() == 1 {
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
/// A per-table `primary_key` override (`override_primary_key`), when present,
/// REPLACES `identifier_field_ids`: the named column must exist and pass the
/// `required` / null-free gate (else `NoSafeSubjectKey`, no subject), and — because
/// uniqueness is unprovable metadata-only — it ALWAYS earns a `SubjectKeyUnverified`
/// diagnostic. Never fabricate a surrogate.
///
/// Otherwise: prefer `identifier_field_ids`; else a `required` / null-free
/// `<STEM>_KEY` / `<STEM>_ID` fallback (`SubjectKeyUnverified`); else
/// `NoSafeSubjectKey` (no subject; never invent a surrogate row id).
fn select_subject_key(
    table: &EmitTableSchema,
    override_primary_key: Option<&str>,
    diagnostics: &mut Vec<Diagnostic>,
) -> SubjectKey {
    // -- Per-table `primary_key` override: replaces identifier_field_ids. --
    if let Some(pk_name) = override_primary_key {
        let col = match table.columns.iter().find(|c| c.name == pk_name) {
            Some(col) => col,
            None => {
                diagnostics.push(Diagnostic::new(
                    Severity::Error,
                    DiagCode::NoSafeSubjectKey,
                    table.qualified_name(),
                    Some(pk_name.to_string()),
                    format!(
                        "per-table primary_key override '{pk_name}' is not a column of the \
                         table; no safe subject key"
                    ),
                ));
                return SubjectKey {
                    columns: Vec::new(),
                };
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
            return SubjectKey {
                columns: Vec::new(),
            };
        }
        diagnostics.push(Diagnostic::new(
            Severity::Warning,
            DiagCode::SubjectKeyUnverified,
            table.qualified_name(),
            Some(col.name.clone()),
            format!(
                "subject key '{}' set by per-table override; uniqueness is unverifiable \
                 metadata-only (NDV deferred)",
                col.name
            ),
        ));
        return SubjectKey {
            columns: vec![col.name.clone()],
        };
    }

    if !table.identifier_field_ids.is_empty() {
        let mut columns = Vec::new();
        for &fid in &table.identifier_field_ids {
            match table.column_by_field_id(fid) {
                Some(col) => columns.push(col.name.clone()),
                None => {
                    diagnostics.push(Diagnostic::new(
                        Severity::Error,
                        DiagCode::NoSafeSubjectKey,
                        table.qualified_name(),
                        None,
                        format!("identifier_field_ids references unknown field id {fid}"),
                    ));
                    return SubjectKey {
                        columns: Vec::new(),
                    };
                }
            }
        }
        return SubjectKey { columns };
    }

    // Fallback: a required / null-free column matching <STEM>_KEY or <STEM>_ID.
    let marker_stem = naming::strip_table_marker(table.stem());
    let candidates = [format!("{marker_stem}_KEY"), format!("{marker_stem}_ID")];
    if let Some(col) = table
        .columns
        .iter()
        .find(|c| candidates.iter().any(|cand| cand == &c.name))
    {
        if col.is_non_null() {
            diagnostics.push(Diagnostic::new(
                Severity::Warning,
                DiagCode::SubjectKeyUnverified,
                table.qualified_name(),
                Some(col.name.clone()),
                format!(
                    "subject key '{}' chosen by name+required fallback; uniqueness is \
                     unverifiable metadata-only (NDV deferred)",
                    col.name
                ),
            ));
            return SubjectKey {
                columns: vec![col.name.clone()],
            };
        }
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
        return SubjectKey {
            columns: Vec::new(),
        };
    }

    diagnostics.push(Diagnostic::new(
        Severity::Error,
        DiagCode::NoSafeSubjectKey,
        table.qualified_name(),
        None,
        "no identifier_field_ids and no required <STEM>_KEY/<STEM>_ID column; \
         emitting no subject (never inventing a surrogate row id)"
            .to_string(),
    ));
    SubjectKey {
        columns: Vec::new(),
    }
}

/// Selected subject key (empty `columns` ⇒ no safe subject key).
struct SubjectKey {
    columns: Vec<String>,
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

    for col in &table.columns {
        // FK candidacy: non-nested, integer-typed, non-subject-key columns only.
        if col.nested || !col.is_integer() || draft.subject_key_columns.contains(&col.name) {
            continue;
        }

        let survivors = candidate_parents(table, col, pk_index);

        match survivors.as_slice() {
            [parent] => {
                let fk = ForeignKey {
                    target_table: parent.table_name.clone(),
                    child_column: col.name.clone(),
                    parent_column: parent.pk_column.clone(),
                };
                let predicate_iri = join_predicate(&col.name, draft, opts);
                joins.push(ColumnMapping::join(col.name.clone(), predicate_iri, fk));
                resolved.insert(col.name.clone());

                // Child-fact → hub advisory: both sides fact, joining on the
                // parent's PK (always true here — we only ever join to a PK).
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
            }
            [] => {
                if col.is_key_like() {
                    diagnostics.push(Diagnostic::new(
                        Severity::Warning,
                        DiagCode::UnresolvedFkCandidate,
                        table.qualified_name(),
                        Some(col.name.clone()),
                        format!(
                            "'{}' looks like a key but matches no known PK by name∧type∧range; \
                             kept literal, no join fabricated",
                            col.name
                        ),
                    ));
                }
                // Non-key-like integer: an ordinary measure — no diagnostic.
            }
            _ => {
                let parents: Vec<String> = survivors
                    .iter()
                    .map(|p| format!("{}.{}", p.table_name, p.pk_column))
                    .collect();
                diagnostics.push(Diagnostic::new(
                    Severity::Warning,
                    DiagCode::AmbiguousFk,
                    table.qualified_name(),
                    Some(col.name.clone()),
                    format!(
                        "'{}' matches multiple candidate parents ({}); kept literal, no join \
                         fabricated",
                        col.name,
                        parents.join(", ")
                    ),
                ));
            }
        }
    }

    (joins, resolved)
}

/// Collect the parents surviving name ∧ type ∧ range-containment for `col`.
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
            // (2) Type-match.
            if col.field_type != pk.field_type {
                return false;
            }
            // (3) Range-containment: child [min,max] ⊆ parent [min,max].
            range_contained(col.stats.min, col.stats.max, pk.min, pk.max)
        })
        .collect()
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
/// `destGeography`), appending `Ref` only when that local would collide with an
/// existing literal predicate in the same table (e.g. `orderDate` literal vs.
/// `ORDER_DATE_KEY` join → `orderDateRef`). Predicate IRIs are not compared by
/// any structural test; this only keeps the emitted document unambiguous.
fn join_predicate(child_column: &str, draft: &TableDraft, opts: &EmitOptions) -> String {
    let mut local = naming::camel_case(naming::strip_key_suffix(child_column));
    if draft.literal_locals.contains(&local) {
        local.push_str("Ref");
    }
    format!("{}{}", opts.base_namespace, local)
}
