//! Materialize an R2RML / Iceberg graph source into a native Fluree ledger.
//!
//! The query path reads a graph source on the fly via `CONSTRUCT`-style
//! template expansion. Native Fluree features (BM25 full-text search, vector /
//! RAG, reasoning) operate only on facts committed to a *native* ledger, so to
//! make those work over an external Iceberg table we **materialize** it: read
//! the source rows, expand the R2RML subject / predicate / object maps into RDF
//! terms exactly as the query path does, group them per subject, and `upsert`
//! them into a target ledger.
//!
//! Refreshes are incremental when safe. The caller passes the last materialized
//! snapshot id as `from_snapshot_id`; if the source's `(from, to]` window is
//! append/compaction-only (see
//! [`window_is_incremental_safe`](fluree_db_iceberg::metadata::TableMetadata::window_is_incremental_safe)),
//! only the *added* rows are read and upserted. The returned `to_snapshot_id` is
//! the new watermark for the caller to persist — watermark storage is left to
//! the caller (the tracking worker) so this stays free of storage assumptions.
//!
//! This module is only available with the `iceberg` feature.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value as JsonValue};

use fluree_db_iceberg::io::ColumnBatch;
use fluree_db_query::r2rml::R2rmlProvider;
use fluree_db_r2rml::mapping::TriplesMap;
use fluree_db_r2rml::materialize::{
    materialize_object_from_batch, materialize_subject_from_batch, RdfTerm,
};
use fluree_vocab::UnresolvedDatatypeConstraint;

use crate::graph_source::FlureeR2rmlProvider;
use crate::{ApiError, Fluree, Result};

/// Subject-IRI prefix for the per-source materialization watermark stored in the
/// target ledger. The full subject is `{PREFIX}{source_graph_source_id}` so one
/// target ledger can track several sources independently.
const WATERMARK_SUBJECT_PREFIX: &str = "urn:fluree:materialize-state:";
/// Predicate holding the last materialized source snapshot id (stored as a
/// string to preserve full i64 precision for 19-digit snapshot ids).
const WATERMARK_SNAPSHOT_PRED: &str = "urn:fluree:materialize#lastSnapshotId";
/// Predicate recording which source the watermark belongs to (informational).
const WATERMARK_SOURCE_PRED: &str = "urn:fluree:materialize#source";

/// Outcome of one materialization pass.
#[derive(Debug, Clone)]
pub struct MaterializeResult {
    /// The source snapshot that was materialized — persisted as the watermark in
    /// the target ledger for the next incremental refresh. `None` when the source
    /// table has no snapshots yet (nothing was materialized).
    pub to_snapshot_id: Option<i64>,
    /// The watermark this pass started from (the previously-materialized source
    /// snapshot id), or `None` for an initial / forced-full materialization.
    pub from_snapshot_id: Option<i64>,
    /// Whether an incremental (added-files-only) scan was used. `false` means a
    /// full re-read (initial run, expired/branched history, or a window
    /// containing a genuine `overwrite`/`delete`).
    pub incremental: bool,
    /// Whether anything was committed (data and/or an advanced watermark). A
    /// no-delta poll (`from == to`) returns `false` — no commit churn.
    pub committed: bool,
    /// Number of source rows read across all batches.
    pub rows_read: usize,
    /// Number of distinct subject nodes upserted.
    pub subjects_upserted: usize,
}

impl Fluree {
    /// Materialize an R2RML / Iceberg graph source into a native ledger.
    ///
    /// Resolves the starting point from the **watermark persisted in the target
    /// ledger** (the last materialized source snapshot id) — unless `force_full`
    /// is set, which ignores it and re-reads the whole live table. Reads the
    /// source (incrementally when the `(from, to]` window is append/compaction
    /// only, else full), expands each row through the source's compiled R2RML
    /// mapping into RDF terms, and `upsert`s one JSON-LD node per subject into
    /// `target_ledger_id` (created if absent). The new watermark is written in
    /// the **same** `upsert` commit, so it advances atomically with the data and
    /// the operation is idempotent. Callers track nothing; a bare re-run resumes
    /// incrementally. This makes the call safe to invoke on a timer (the
    /// tracking worker does exactly that).
    ///
    /// A no-delta poll (`from == to`) commits nothing and returns
    /// `committed = false`.
    ///
    /// **Deletion safety.** An added-files scan captures inserts (and in-place
    /// updates land via `upsert`), but it cannot see row *deletions*. The window
    /// check therefore forces a full re-read whenever an `overwrite`/`delete`
    /// snapshot appears, so deletes are never silently missed — while routine
    /// appends and periodic compaction stay on the cheap incremental path. A
    /// full re-read still only `upsert`s present rows; reconciling subjects that
    /// vanished entirely (set-difference retraction) is a separate follow-up.
    pub async fn materialize_r2rml_graph_source(
        &self,
        source_graph_source_id: &str,
        target_ledger_id: &str,
        force_full: bool,
    ) -> Result<MaterializeResult> {
        let provider = FlureeR2rmlProvider::new(self);

        // 1. Compiled R2RML mapping (subject / predicate / object maps).
        let mapping = provider
            .compiled_mapping(source_graph_source_id, None)
            .await?;
        if mapping.triples_maps.is_empty() {
            return Err(ApiError::Config(format!(
                "Graph source '{source_graph_source_id}' has no R2RML triples maps"
            )));
        }

        // 2. Resolve the starting watermark from the target ledger (unless the
        //    caller forces a full re-read).
        let from_snapshot_id = if force_full {
            None
        } else {
            self.materialize_watermark(target_ledger_id, source_graph_source_id)
                .await?
        };

        // 3. Read source rows (incremental when safe) per logical table and
        //    aggregate all triples per subject IRI into one JSON-LD node. The
        //    BTreeMap keeps subjects in a stable order (deterministic txn).
        let mut subjects: BTreeMap<String, SubjectNode> = BTreeMap::new();
        let mut rows_read = 0usize;
        let mut to_snapshot_id: Option<i64> = None;
        // The pass is incremental only if *every* table's scan was incremental
        // (and we resolved a watermark to begin with).
        let mut incremental_all = from_snapshot_id.is_some();
        let mut any_table = false;

        for tm in mapping.triples_maps.values() {
            let Some(table_name) = tm.table_name() else {
                // Subject-only / constant-IRI maps with no logical table.
                continue;
            };
            any_table = true;

            let (to_id, incremental, batches) = provider
                .scan_for_materialize(source_graph_source_id, table_name, &[], from_snapshot_id)
                .await?;
            to_snapshot_id = to_id;
            incremental_all = incremental_all && incremental;

            for batch in &batches {
                for row in 0..batch.num_rows {
                    rows_read += 1;
                    materialize_row_into(tm, batch, row, &mut subjects)?;
                }
            }
        }

        let incremental = any_table && incremental_all;
        let subjects_upserted = subjects.len();

        // 4. Decide whether anything needs committing. Nothing changed when the
        //    source has no snapshot, or the watermark already equals `to` and no
        //    rows were produced (a no-delta poll) — skip the commit entirely.
        let watermark_unchanged = to_snapshot_id.is_none() || to_snapshot_id == from_snapshot_id;
        if subjects.is_empty() && watermark_unchanged {
            return Ok(MaterializeResult {
                to_snapshot_id,
                from_snapshot_id,
                incremental,
                committed: false,
                rows_read,
                subjects_upserted: 0,
            });
        }

        // 5. Build the JSON-LD insert array: one node per subject, plus the
        //    advanced watermark node (committed atomically with the data).
        let mut nodes: Vec<JsonValue> =
            subjects.into_values().map(SubjectNode::into_json).collect();
        if let Some(to) = to_snapshot_id {
            nodes.push(watermark_node(source_graph_source_id, to));
        }
        let doc = JsonValue::Array(nodes);

        // 6. Load-or-create the target ledger and upsert.
        let ledger = if self.ledger_exists(target_ledger_id).await? {
            self.ledger(target_ledger_id).await?
        } else {
            self.create_ledger(target_ledger_id).await?
        };
        self.upsert(ledger, &doc).await?;

        Ok(MaterializeResult {
            to_snapshot_id,
            from_snapshot_id,
            incremental,
            committed: true,
            rows_read,
            subjects_upserted,
        })
    }

    /// Read the materialization watermark (last materialized source snapshot id)
    /// for `source_graph_source_id` from the target ledger. Returns `None` if the
    /// target ledger does not exist yet or carries no watermark for that source.
    pub async fn materialize_watermark(
        &self,
        target_ledger_id: &str,
        source_graph_source_id: &str,
    ) -> Result<Option<i64>> {
        if !self.ledger_exists(target_ledger_id).await? {
            return Ok(None);
        }
        let db = self.db(target_ledger_id).await?;

        let subject = format!("{WATERMARK_SUBJECT_PREFIX}{source_graph_source_id}");
        let mut where_obj = Map::new();
        where_obj.insert("@id".to_string(), JsonValue::String(subject));
        where_obj.insert(
            WATERMARK_SNAPSHOT_PRED.to_string(),
            JsonValue::String("?v".to_string()),
        );
        let query = json!({ "select": ["?v"], "where": JsonValue::Object(where_obj) });

        let result = self.query(&db, &query).await?;
        let json = result
            .to_jsonld(&db.snapshot)
            .map_err(|e| ApiError::Internal(format!("Failed to format watermark query: {e}")))?;
        Ok(extract_first_i64(&json))
    }
}

/// Build the watermark JSON-LD node (`@id` + last snapshot id + source). Stored
/// as a string to preserve full i64 precision; `upsert` retracts-then-asserts so
/// the watermark advances cleanly in place.
fn watermark_node(source_graph_source_id: &str, to_snapshot_id: i64) -> JsonValue {
    let mut node = Map::new();
    node.insert(
        "@id".to_string(),
        JsonValue::String(format!(
            "{WATERMARK_SUBJECT_PREFIX}{source_graph_source_id}"
        )),
    );
    node.insert(
        WATERMARK_SNAPSHOT_PRED.to_string(),
        JsonValue::String(to_snapshot_id.to_string()),
    );
    node.insert(
        WATERMARK_SOURCE_PRED.to_string(),
        JsonValue::String(source_graph_source_id.to_string()),
    );
    JsonValue::Object(node)
}

/// Find the first value anywhere in a JSON-LD query result that parses as an
/// i64 (the watermark query selects exactly one string value, so this is
/// unambiguous). Tolerant of the formatter's row nesting.
fn extract_first_i64(v: &JsonValue) -> Option<i64> {
    match v {
        JsonValue::Array(a) => a.iter().find_map(extract_first_i64),
        JsonValue::Object(o) => o.values().find_map(extract_first_i64),
        JsonValue::String(s) => s.parse::<i64>().ok(),
        JsonValue::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// Accumulates all triples for a single subject IRI before emitting one JSON-LD
/// node. Predicates collect multiple object values (combined into an array on
/// output) and `@type` classes are de-duplicated.
struct SubjectNode {
    id: String,
    types: Vec<String>,
    /// predicate IRI -> object values (`@id` refs, value objects, or scalars).
    predicates: BTreeMap<String, Vec<JsonValue>>,
}

impl SubjectNode {
    fn new(id: String) -> Self {
        Self {
            id,
            types: Vec::new(),
            predicates: BTreeMap::new(),
        }
    }

    fn add_type(&mut self, class: &str) {
        if !self.types.iter().any(|c| c == class) {
            self.types.push(class.to_string());
        }
    }

    fn add_object(&mut self, predicate: &str, value: JsonValue) {
        let entry = self.predicates.entry(predicate.to_string()).or_default();
        if !entry.iter().any(|v| v == &value) {
            entry.push(value);
        }
    }

    fn into_json(self) -> JsonValue {
        let mut node = Map::new();
        node.insert("@id".to_string(), JsonValue::String(self.id));
        if !self.types.is_empty() {
            node.insert(
                "@type".to_string(),
                JsonValue::Array(self.types.into_iter().map(JsonValue::String).collect()),
            );
        }
        for (pred, mut vals) in self.predicates {
            let v = if vals.len() == 1 {
                vals.pop().expect("len == 1")
            } else {
                JsonValue::Array(vals)
            };
            node.insert(pred, v);
        }
        JsonValue::Object(node)
    }
}

/// Expand one source row through a triples map and merge its triples into the
/// per-subject accumulator. Reuses the exact `term.rs` materializers so the
/// subject IRIs match the query path byte-for-byte.
fn materialize_row_into(
    tm: &TriplesMap,
    batch: &ColumnBatch,
    row: usize,
    subjects: &mut BTreeMap<String, SubjectNode>,
) -> Result<()> {
    let subject_term = materialize_subject_from_batch(&tm.subject_map, batch, row)
        .map_err(|e| ApiError::Internal(format!("R2RML subject materialization failed: {e}")))?;
    let subject_iri = match subject_term {
        Some(RdfTerm::Iri(iri)) => iri,
        // Null subject column (skip), blank-node subjects (no stable identity to
        // upsert), or a literal (term.rs already rejects this) -> skip the row.
        Some(RdfTerm::BlankNode(_) | RdfTerm::Literal { .. }) | None => return Ok(()),
    };

    let node = subjects
        .entry(subject_iri.clone())
        .or_insert_with(|| SubjectNode::new(subject_iri));

    // rr:class -> @type
    for class in &tm.subject_map.classes {
        node.add_type(class);
    }

    // predicate-object maps (constant predicates only; RefObjectMap joins are
    // resolved at query time, not yet during materialization).
    for pom in &tm.predicate_object_maps {
        let Some(predicate) = pom.predicate_map.as_constant() else {
            continue;
        };
        let obj = materialize_object_from_batch(&pom.object_map, batch, row)
            .map_err(|e| ApiError::Internal(format!("R2RML object materialization failed: {e}")))?;
        if let Some(term) = obj {
            node.add_object(predicate, rdf_term_to_jsonld(term));
        }
    }

    Ok(())
}

/// Convert an [`RdfTerm`] into its JSON-LD object representation.
fn rdf_term_to_jsonld(term: RdfTerm) -> JsonValue {
    match term {
        RdfTerm::Iri(iri) | RdfTerm::BlankNode(iri) => {
            let mut m = Map::new();
            m.insert("@id".to_string(), JsonValue::String(iri));
            JsonValue::Object(m)
        }
        RdfTerm::Literal { value, dtc } => match dtc {
            // Plain literal (xsd:string) -> bare JSON string, matching the
            // query path's default datatype.
            None => JsonValue::String(value),
            Some(UnresolvedDatatypeConstraint::LangTag(lang)) => {
                let mut m = Map::new();
                m.insert("@value".to_string(), JsonValue::String(value));
                m.insert("@language".to_string(), JsonValue::String(lang.to_string()));
                JsonValue::Object(m)
            }
            Some(UnresolvedDatatypeConstraint::Explicit(dt)) => {
                let mut m = Map::new();
                m.insert("@value".to_string(), JsonValue::String(value));
                m.insert("@type".to_string(), JsonValue::String(dt.to_string()));
                JsonValue::Object(m)
            }
        },
    }
}
