//! Deterministic Iceberg-metadata → R2RML emitter (greenfield).
//!
//! Given per-table schema + metadata-stats ([`EmitTableSchema`]), this module
//! builds solo's authoritative [`StructuredR2rmlMapping`] IR, renders Turtle
//! from it, and (behind the `turtle` feature) round-trips that Turtle back
//! through [`crate::loader::R2rmlLoader`] to prove the emitted mapping compiles
//! and its join graph reconnects.
//!
//! Pipeline: [`emit_r2rml`] → `heuristic::build_mapping` (PK/FK inference,
//! producing the IR + diagnostics) → `render::render_turtle`. The compiled
//! `Vec<TriplesMap>` produced during the round-trip check is INTERNAL-only and
//! never leaves the emitter — the wire artifact is the camelCase
//! [`StructuredR2rmlMapping`].
//!
//! Additive and self-contained: it adds no dependency (std + serde +
//! `fluree-db-tabular`, all already present) and touches no existing code path,
//! so it is always compiled. Only the round-trip verification and the
//! enterprise-structural-match tests require the `turtle` feature.

pub mod diagnostic;
pub mod input;
pub mod ir;

mod heuristic;
mod naming;
mod render;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod tests;

pub use diagnostic::{DiagCode, Diagnostic, Severity};
pub use input::{EmitColumn, EmitColumnStats, EmitTableSchema, TypedBound};
pub use ir::{ColumnMapping, ForeignKey, PrefixDecl, StructuredR2rmlMapping, TableMapping};

/// Emitter configuration. Every field is a pure knob — identical options plus
/// identical inputs yield byte-identical output.
#[derive(Debug, Clone)]
pub struct EmitOptions {
    /// The single base namespace all vocab IRIs derive from (== solo's
    /// `StructuredR2rmlMapping.baseNamespace`). The subject-IRI base is derived
    /// from it (trailing `#`→`/`, else append `/`).
    pub base_namespace: String,
    /// The `@base` for the mapping document's TriplesMap node IRIs (`<#Foo>`).
    pub map_document_base: String,
    /// The prefix label bound to `base_namespace` in the rendered Turtle.
    pub vocab_prefix: String,
    /// Emit `xsd:integer` for `Int32`/`Int64` (matches `enterprise.ttl`); when
    /// `false`, use `xsd:int` / `xsd:long`.
    pub xsd_long_as_integer: bool,
    /// Emit `rr:parentTriplesMap` joins for resolved FKs (Phase 2).
    pub emit_fk_joins: bool,
    /// Keep resolved-FK key columns as literal predicate-object maps too
    /// (pushdown-friendly).
    pub keep_fk_keys_as_literals: bool,
}

impl EmitOptions {
    /// Options with the given base namespace and reference-matching defaults.
    pub fn new(base_namespace: impl Into<String>) -> Self {
        Self {
            base_namespace: base_namespace.into(),
            ..Self::default()
        }
    }
}

impl Default for EmitOptions {
    fn default() -> Self {
        Self {
            base_namespace: "http://ns.fluree.dev/edw#".to_string(),
            map_document_base: "http://mapping.fluree.dev/r2rml".to_string(),
            vocab_prefix: "v".to_string(),
            xsd_long_as_integer: true,
            emit_fk_joins: true,
            keep_fk_keys_as_literals: true,
        }
    }
}

/// The emitter's output: the authoritative structured IR, the rendered Turtle,
/// and the diagnostics.
#[derive(Debug, Clone)]
pub struct EmitOutput {
    /// The authoritative wire IR (camelCase `StructuredR2rmlMapping`).
    pub structured: StructuredR2rmlMapping,
    /// Turtle rendered from `structured`; compiles through `R2rmlLoader`.
    pub turtle: String,
    /// Diagnostics for every decision the emitter could not make from metadata.
    pub diagnostics: Vec<Diagnostic>,
}

/// Run the deterministic emitter over a set of tables.
pub fn emit_r2rml(tables: &[EmitTableSchema], opts: &EmitOptions) -> EmitOutput {
    let (structured, diagnostics) = heuristic::build_mapping(tables, opts);
    let turtle = render::render_turtle(&structured, opts);
    EmitOutput {
        structured,
        turtle,
        diagnostics,
    }
}

/// Internal round-trip check: compile the emitted Turtle through the real
/// loader and assert the compiled `Vec<TriplesMap>` reproduces the emitted IR's
/// table count, subject templates, and FK join graph.
///
/// The compiled mapping is INTERNAL-only — this is a verification aid, not a
/// wire artifact. Returns `Ok(())` on a faithful round-trip, or `Err` with the
/// first discrepancy found.
#[cfg(feature = "turtle")]
pub fn roundtrip_check(output: &EmitOutput) -> Result<(), String> {
    use std::collections::BTreeSet;

    use crate::loader::R2rmlLoader;

    let compiled = R2rmlLoader::from_turtle(&output.turtle)
        .map_err(|e| format!("from_turtle failed: {e}"))?
        .compile()
        .map_err(|e| format!("compile failed: {e}"))?;

    let structured = &output.structured;

    if compiled.len() != structured.table_mappings.len() {
        return Err(format!(
            "table count mismatch: compiled {} vs structured {}",
            compiled.len(),
            structured.table_mappings.len()
        ));
    }

    // FK tuples across the whole document: (childTable, childCol, parentTable, parentCol).
    let mut structured_fks: BTreeSet<(String, String, String, String)> = BTreeSet::new();

    for tm in &structured.table_mappings {
        let maps = compiled.find_maps_for_table(&tm.table_name);
        let compiled_tm = match maps.as_slice() {
            [single] => *single,
            [] => return Err(format!("compiled mapping missing table {}", tm.table_name)),
            _ => {
                return Err(format!(
                    "compiled mapping has {} TriplesMaps for table {}",
                    maps.len(),
                    tm.table_name
                ))
            }
        };

        let expected_template = if tm.subject_template.is_empty() {
            None
        } else {
            Some(tm.subject_template.clone())
        };
        if compiled_tm.subject_map.template != expected_template {
            return Err(format!(
                "subject template mismatch for {}: compiled {:?} vs structured {:?}",
                tm.table_name, compiled_tm.subject_map.template, expected_template
            ));
        }

        for col in &tm.columns {
            if let Some(fk) = &col.foreign_key {
                structured_fks.insert((
                    tm.table_name.clone(),
                    fk.child_column.clone(),
                    fk.target_table.clone(),
                    fk.parent_column.clone(),
                ));
            }
        }
    }

    // Reconstruct the FK graph from the compiled maps, resolving each
    // parentTriplesMap IRI back to its logical table name.
    let mut compiled_fks: BTreeSet<(String, String, String, String)> = BTreeSet::new();
    for tm in &structured.table_mappings {
        let compiled_tm = compiled
            .find_maps_for_table(&tm.table_name)
            .into_iter()
            .next()
            .ok_or_else(|| format!("compiled mapping missing table {}", tm.table_name))?;
        for pom in &compiled_tm.predicate_object_maps {
            if let Some(rom) = pom.object_map.as_ref() {
                let parent_tm = compiled.get(&rom.parent_triples_map).ok_or_else(|| {
                    format!(
                        "parentTriplesMap {} did not resolve to a compiled TriplesMap",
                        rom.parent_triples_map
                    )
                })?;
                let parent_table = parent_tm
                    .table_name()
                    .ok_or_else(|| {
                        format!(
                            "parent TriplesMap {} has no table name",
                            rom.parent_triples_map
                        )
                    })?
                    .to_string();
                for jc in &rom.join_conditions {
                    compiled_fks.insert((
                        compiled_tm.table_name().unwrap_or_default().to_string(),
                        jc.child_column.clone(),
                        parent_table.clone(),
                        jc.parent_column.clone(),
                    ));
                }
            }
        }
    }

    if structured_fks != compiled_fks {
        let missing: Vec<_> = structured_fks.difference(&compiled_fks).collect();
        let extra: Vec<_> = compiled_fks.difference(&structured_fks).collect();
        return Err(format!(
            "FK graph mismatch: in-IR-not-compiled {missing:?}; in-compiled-not-IR {extra:?}"
        ));
    }

    Ok(())
}
