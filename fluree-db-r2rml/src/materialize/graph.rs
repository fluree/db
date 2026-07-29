//! Whole-graph triple enumeration for bulk materialization.
//!
//! This module composes the per-term materialization primitives in
//! [`super::term`] into a WHOLE-GRAPH enumerator: given a compiled R2RML
//! mapping and the tabular rows of each logical table, it emits every
//! `(subject, predicate, object)` triple the mapping produces — exactly the
//! triples the query engine would return for an unconstrained crawl, but driven
//! directly off the row batches with no query plan, operator, or fuel tracker.
//!
//! It exists to feed the bulk twin builder (`fluree materialize`): the emitted
//! triples stream into an ingestion sink (the native import pipeline) instead of
//! being shaped into query bindings.
//!
//! # What one row produces
//!
//! For each row of a [`TriplesMap`]'s logical table:
//! 1. The subject term (`rr:subjectMap`). A NULL in any subject-template column
//!    means the row produces **no triples at all** (R2RML semantics).
//! 2. One `rdf:type` triple per `rr:class`.
//! 3. For each `rr:predicateObjectMap`: the predicate IRI, then either a direct
//!    object (column / template / constant — a NULL object column yields no
//!    triple) or, for a `RefObjectMap`, the FK edge to the parent subject.
//!
//! # RefObjectMap (foreign-key) edges — lookup-free, dims-first
//!
//! Because a full-table materialization scans BOTH sides, FK edges are resolved
//! without any per-row parent re-scan. Parent triples maps are processed before
//! their children ([`dependency_order`]); while a parent is scanned, its join
//! key → subject term is recorded in a [`ParentIndexSet`]. When a child row
//! carries a foreign key, the edge is emitted only if that key is present in the
//! parent index — a **dangling** or NULL FK produces no triple, which is R2RML
//! inner-join semantics (and matches what the virtual query path serves).
//!
//! Cyclic and self-referential parents (which cannot be indexed lazily in a
//! single pass) are fully pre-indexed before the emit pass.

use std::collections::{HashMap, HashSet, VecDeque};

use fluree_db_tabular::ColumnBatch;
use fluree_vocab::UnresolvedDatatypeConstraint;

use super::term::{
    get_join_key_from_batch, materialize_object_from_batch, materialize_predicate_from_batch,
    materialize_subject_from_batch, RdfTerm,
};
use crate::error::{R2rmlError, R2rmlResult};
use crate::mapping::{CompiledR2rmlMapping, ObjectMap, RefObjectMap, TriplesMap};

/// Sink for materialized triples. The subject is always an IRI or blank node;
/// the object may be any RDF term. Implementations turn triples into whatever
/// the consumer needs — canonical N-Triples for a parity diff, or encoded
/// flakes for the ingestion pipeline.
///
/// `observe` returns a result so an encoding sink can surface a hard failure
/// (a corrupt value/datatype pair) rather than silently dropping it.
pub trait TripleObserver {
    /// Consume one `(subject, predicate, object)` triple.
    fn observe(&mut self, subject: &RdfTerm, predicate: &str, object: &RdfTerm) -> R2rmlResult<()>;
}

/// Counts collected during enumeration — the raw material for the build-time
/// parity gate. `ref_edges` is keyed by `(child TriplesMap IRI, predicate IRI)`
/// so the builder can cross-check the FK-edge count per relationship against the
/// virtual query path.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MaterializeStats {
    /// Rows that produced a subject (i.e. non-null subject key).
    pub subjects: u64,
    /// `rdf:type` triples emitted.
    pub type_triples: u64,
    /// Direct data triples (column / template / constant objects).
    pub data_triples: u64,
    /// Foreign-key edge triples emitted (matched parent).
    pub ref_triples: u64,
    /// Foreign keys present but with no matching parent (dropped — dangling).
    pub ref_dangling: u64,
    /// Object columns/templates that were NULL, producing no triple.
    pub null_objects: u64,
    /// Total triples emitted, per TriplesMap IRI.
    pub per_tm: HashMap<String, u64>,
    /// FK-edge count per `(child TriplesMap IRI, predicate IRI)` — parity key.
    pub ref_edges: HashMap<(String, String), u64>,
}

impl MaterializeStats {
    /// Total triples emitted across all TriplesMaps.
    pub fn total_triples(&self) -> u64 {
        self.type_triples + self.data_triples + self.ref_triples
    }
}

// ---------------------------------------------------------------------------
// N-Triples rendering (parity-diff observer)
// ---------------------------------------------------------------------------

/// A [`TripleObserver`] that renders each triple to a canonical N-Triples line.
/// The collected lines, when sorted and de-duplicated, form a diffable
/// serialization of the materialized graph — the basis for the twin-vs-virtual
/// full-triple parity check.
#[derive(Debug, Default)]
pub struct NTriplesCollector {
    /// Rendered `S P O .` lines, in emission order.
    pub lines: Vec<String>,
}

impl TripleObserver for NTriplesCollector {
    fn observe(&mut self, subject: &RdfTerm, predicate: &str, object: &RdfTerm) -> R2rmlResult<()> {
        self.lines.push(format!(
            "{} <{}> {} .",
            render_term(subject),
            predicate,
            render_term(object)
        ));
        Ok(())
    }
}

impl NTriplesCollector {
    /// The lines as a sorted, de-duplicated set — the canonical form for a set
    /// comparison against another graph's N-Triples (RDF graphs are sets).
    pub fn sorted_unique(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .lines
            .iter()
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        v.sort();
        v
    }
}

/// Render an [`RdfTerm`] as an N-Triples term (`<iri>`, `_:blank`, or a quoted
/// literal with optional `^^<datatype>` / `@lang`). A plain literal (no datatype
/// constraint) renders without an explicit `xsd:string` type, the common
/// N-Triples convention; a diff against another serializer normalizes both sides.
pub fn render_term(term: &RdfTerm) -> String {
    match term {
        RdfTerm::Iri(iri) => format!("<{iri}>"),
        RdfTerm::BlankNode(id) => {
            // Accept ids that already carry the `_:` prefix or bare labels.
            let label = id.strip_prefix("_:").unwrap_or(id);
            format!("_:{label}")
        }
        RdfTerm::Literal { value, dtc } => render_literal(value, dtc.as_ref()),
    }
}

fn render_literal(value: &str, dtc: Option<&UnresolvedDatatypeConstraint>) -> String {
    let escaped = escape_literal(value);
    match dtc {
        None => format!("\"{escaped}\""),
        Some(UnresolvedDatatypeConstraint::LangTag(tag)) => format!("\"{escaped}\"@{tag}"),
        Some(c @ UnresolvedDatatypeConstraint::Explicit(_)) => {
            format!("\"{escaped}\"^^<{}>", c.datatype_iri())
        }
    }
}

/// Escape a literal lexical form per N-Triples (backslash, quote, and the
/// control characters that have short escapes).
fn escape_literal(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Parent (foreign-key) index
// ---------------------------------------------------------------------------

/// Canonicalize a [`RefObjectMap`]'s join into positionally-aligned parent and
/// child column lists, ordered deterministically by parent column name so the
/// parent-side index and the child-side lookup key are built the same way.
///
/// Returns an error for a join with no conditions (a cross-product ref), which
/// the bulk materializer refuses rather than explode.
fn canonical_join(rom: &RefObjectMap) -> R2rmlResult<(Vec<String>, Vec<String>)> {
    if rom.join_conditions.is_empty() {
        return Err(R2rmlError::Materialization(format!(
            "RefObjectMap referencing parent '{}' has no join conditions; \
             cross-product joins are not supported by the bulk materializer",
            rom.parent_triples_map
        )));
    }
    let mut pairs: Vec<(&str, &str)> = rom
        .join_conditions
        .iter()
        .map(|jc| (jc.parent_column.as_str(), jc.child_column.as_str()))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    let parent_cols = pairs.iter().map(|(p, _)| (*p).to_string()).collect();
    let child_cols = pairs.iter().map(|(_, c)| (*c).to_string()).collect();
    Ok((parent_cols, child_cols))
}

/// A parent's join-key tuple → its materialized subject term, for one canonical
/// parent-column set.
type KeyToSubject = HashMap<Vec<String>, RdfTerm>;

/// All of one parent TriplesMap's indexes, keyed by canonical parent-column list
/// (a parent may be joined on more than one column set).
type ParentColIndex = HashMap<Vec<String>, KeyToSubject>;

/// Parent-subject indexes for every TriplesMap referenced as a foreign-key
/// target. Built by scanning parent tables once; consulted by child rows.
///
/// Layout: `parent TM IRI → (canonical parent columns → (key tuple → parent
/// subject term))`. A parent may be referenced via more than one column set, so
/// the middle level is keyed by the canonical column list.
#[derive(Debug, Default)]
pub struct ParentIndexSet {
    /// Which canonical parent-column sets each parent TM must be indexed on.
    needed: HashMap<String, HashSet<Vec<String>>>,
    /// The built indexes.
    index: HashMap<String, ParentColIndex>,
}

impl ParentIndexSet {
    /// Plan the parent indexes required by `mapping`. Computes, for every
    /// `RefObjectMap`, the parent TM and the canonical parent-column set it
    /// joins on. Errors if a `RefObjectMap` names a parent not in the mapping
    /// (fail-closed: an unresolvable FK is a broken mapping, not a silent drop).
    pub fn new(mapping: &CompiledR2rmlMapping) -> R2rmlResult<Self> {
        let mut needed: HashMap<String, HashSet<Vec<String>>> = HashMap::new();
        for tm in mapping.triples_maps.values() {
            for pom in &tm.predicate_object_maps {
                if let ObjectMap::RefObjectMap(rom) = &pom.object_map {
                    if !mapping.triples_maps.contains_key(&rom.parent_triples_map) {
                        return Err(R2rmlError::Materialization(format!(
                            "RefObjectMap in '{}' references unknown parent TriplesMap '{}'",
                            tm.iri, rom.parent_triples_map
                        )));
                    }
                    let (parent_cols, _child_cols) = canonical_join(rom)?;
                    needed
                        .entry(rom.parent_triples_map.clone())
                        .or_default()
                        .insert(parent_cols);
                }
            }
        }
        let index = needed.keys().map(|p| (p.clone(), HashMap::new())).collect();
        Ok(Self { needed, index })
    }

    /// Whether `tm_iri` is referenced as a foreign-key parent (and therefore
    /// must be indexed while it is scanned).
    pub fn is_parent(&self, tm_iri: &str) -> bool {
        self.needed.contains_key(tm_iri)
    }

    /// A sibling index set with the SAME required parent-column plan (`needed`) but
    /// an EMPTY built index. Lets one parent table be pre-indexed in isolation —
    /// used by the concurrent Pass-1 pre-index, where each table scans into its own
    /// partial index and the partials are merged back with [`merge_from`].
    pub fn split_empty(&self) -> Self {
        Self {
            needed: self.needed.clone(),
            index: HashMap::new(),
        }
    }

    /// Fold another index set's built indexes into this one (union per parent, per
    /// canonical column set, per key). Pre-indexed parent tables have disjoint
    /// parent IRIs, but the merge is a full union so it stays correct regardless.
    pub fn merge_from(&mut self, other: Self) {
        for (parent, col_index) in other.index {
            let entry = self.index.entry(parent).or_default();
            for (cols, key_to_subject) in col_index {
                entry.entry(cols).or_default().extend(key_to_subject);
            }
        }
    }

    /// Record a parent TriplesMap's rows from one batch into the index. A no-op
    /// when `tm` is not a foreign-key parent. Rows with a null subject or a null
    /// join key are skipped (they can never satisfy a join).
    pub fn index_batch(&mut self, tm: &TriplesMap, batch: &ColumnBatch) -> R2rmlResult<()> {
        let col_sets: Vec<Vec<String>> = match self.needed.get(&tm.iri) {
            Some(sets) => sets.iter().cloned().collect(),
            None => return Ok(()),
        };
        let entry = self.index.entry(tm.iri.clone()).or_default();
        for row in 0..batch.num_rows {
            let subject = match materialize_subject_from_batch(&tm.subject_map, batch, row)? {
                Some(s) => s,
                None => continue,
            };
            for cols in &col_sets {
                if let Some(key) = get_join_key_from_batch(cols, batch, row) {
                    entry
                        .entry(cols.clone())
                        .or_default()
                        .insert(key, subject.clone());
                }
            }
        }
        Ok(())
    }

    /// Look up a parent subject term by parent IRI, canonical parent columns,
    /// and the child's join-key values.
    fn lookup(&self, parent_iri: &str, cols: &[String], key: &[String]) -> Option<&RdfTerm> {
        self.index.get(parent_iri)?.get(cols)?.get(key)
    }

    /// The union of parent-column names `tm_iri` must be indexed on — the extra
    /// columns a scan of this parent must project (beyond the TriplesMap's own
    /// referenced columns) so the foreign-key join keys can be read. Empty when
    /// `tm_iri` is not a foreign-key parent.
    pub fn needed_parent_columns(&self, tm_iri: &str) -> Vec<String> {
        let mut cols: Vec<String> = match self.needed.get(tm_iri) {
            Some(sets) => sets.iter().flatten().cloned().collect(),
            None => return Vec::new(),
        };
        cols.sort();
        cols.dedup();
        cols
    }
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

/// Emit every triple produced by one TriplesMap's batch of rows. Parent indexes
/// for any `RefObjectMap` targets must already be populated (guaranteed by
/// [`enumerate_from_batches`] / the dims-first driver).
pub fn emit_batch(
    tm: &TriplesMap,
    batch: &ColumnBatch,
    parents: &ParentIndexSet,
    observer: &mut dyn TripleObserver,
    stats: &mut MaterializeStats,
) -> R2rmlResult<()> {
    // Precompute canonical joins for the RefObjectMap POMs so we don't rebuild
    // them per row on a large fact table.
    let ref_joins: Vec<Option<(Vec<String>, Vec<String>)>> = tm
        .predicate_object_maps
        .iter()
        .map(|pom| match &pom.object_map {
            ObjectMap::RefObjectMap(rom) => canonical_join(rom).map(Some),
            _ => Ok(None),
        })
        .collect::<R2rmlResult<Vec<_>>>()?;

    for row in 0..batch.num_rows {
        let subject = match materialize_subject_from_batch(&tm.subject_map, batch, row)? {
            Some(s) => s,
            None => continue, // null subject key → no triples for this row
        };
        stats.subjects += 1;

        // rdf:type triples, one per declared class.
        for class in tm.classes() {
            let object = RdfTerm::iri(class.clone());
            observer.observe(&subject, fluree_vocab::rdf::TYPE, &object)?;
            stats.type_triples += 1;
            *stats.per_tm.entry(tm.iri.clone()).or_default() += 1;
        }

        // Predicate-object maps.
        for (i, pom) in tm.predicate_object_maps.iter().enumerate() {
            let predicate = match materialize_predicate_from_batch(&pom.predicate_map, batch, row)?
            {
                Some(p) => p,
                None => continue, // null templated/column predicate → no triple
            };

            match &pom.object_map {
                ObjectMap::RefObjectMap(rom) => {
                    let (parent_cols, child_cols) = ref_joins[i]
                        .as_ref()
                        .expect("ref POM has a precomputed canonical join");
                    let child_key = match get_join_key_from_batch(child_cols, batch, row) {
                        Some(k) => k,
                        None => continue, // null FK → no triple
                    };
                    match parents.lookup(&rom.parent_triples_map, parent_cols, &child_key) {
                        Some(parent_subject) => {
                            observer.observe(&subject, &predicate, parent_subject)?;
                            stats.ref_triples += 1;
                            *stats
                                .ref_edges
                                .entry((tm.iri.clone(), predicate.clone()))
                                .or_default() += 1;
                            *stats.per_tm.entry(tm.iri.clone()).or_default() += 1;
                        }
                        None => stats.ref_dangling += 1, // dangling/absent parent → no triple
                    }
                }
                other => match materialize_object_from_batch(other, batch, row)? {
                    Some(object) => {
                        observer.observe(&subject, &predicate, &object)?;
                        stats.data_triples += 1;
                        *stats.per_tm.entry(tm.iri.clone()).or_default() += 1;
                    }
                    None => stats.null_objects += 1, // null object column → no triple
                },
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

/// Compute a dims-first emission order: every parent TriplesMap precedes the
/// children that reference it. Returns `(order, cyclic)` where `order` is the
/// topological order of the acyclic part and `cyclic` holds any TriplesMaps
/// caught in a foreign-key cycle (mutually-referencing tables), which the driver
/// must pre-index. Self-referential foreign keys are NOT treated as cycles here
/// (they leave a single node acyclic); the driver pre-indexes them separately.
pub fn dependency_order(mapping: &CompiledR2rmlMapping) -> (Vec<String>, Vec<String>) {
    let mut iris: Vec<String> = mapping.triples_maps.keys().cloned().collect();
    iris.sort();

    // Edge parent → child. `parents_of[c]` = the distinct parents c depends on.
    let mut parents_of: HashMap<String, HashSet<String>> =
        iris.iter().map(|i| (i.clone(), HashSet::new())).collect();
    let mut children_of: HashMap<String, HashSet<String>> =
        iris.iter().map(|i| (i.clone(), HashSet::new())).collect();

    for tm in mapping.triples_maps.values() {
        for pom in &tm.predicate_object_maps {
            if let ObjectMap::RefObjectMap(rom) = &pom.object_map {
                let parent = &rom.parent_triples_map;
                // Ignore self-edges (handled by pre-indexing) and edges to
                // unknown parents (rejected earlier by ParentIndexSet::new).
                if parent != &tm.iri
                    && mapping.triples_maps.contains_key(parent)
                    && parents_of.get_mut(&tm.iri).unwrap().insert(parent.clone())
                {
                    children_of.get_mut(parent).unwrap().insert(tm.iri.clone());
                }
            }
        }
    }

    // Kahn's algorithm; process the ready set in sorted order for determinism.
    let mut indeg: HashMap<String, usize> = iris
        .iter()
        .map(|i| (i.clone(), parents_of[i].len()))
        .collect();
    let mut queue: VecDeque<String> = iris.iter().filter(|i| indeg[*i] == 0).cloned().collect();
    let mut order: Vec<String> = Vec::with_capacity(iris.len());
    while let Some(n) = queue.pop_front() {
        order.push(n.clone());
        let mut newly: Vec<String> = Vec::new();
        for c in &children_of[&n] {
            let d = indeg.get_mut(c).unwrap();
            *d -= 1;
            if *d == 0 {
                newly.push(c.clone());
            }
        }
        newly.sort();
        for c in newly {
            queue.push_back(c);
        }
    }

    let placed: HashSet<&String> = order.iter().collect();
    let mut cyclic: Vec<String> = iris
        .iter()
        .filter(|i| !placed.contains(i))
        .cloned()
        .collect();
    cyclic.sort();
    (order, cyclic)
}

/// TriplesMaps that reference themselves via a foreign key — they must be fully
/// indexed before emission, since a self-edge cannot be resolved lazily in a
/// single scan pass.
fn self_referential(mapping: &CompiledR2rmlMapping) -> HashSet<String> {
    mapping
        .triples_maps
        .values()
        .filter(|tm| {
            tm.predicate_object_maps.iter().any(|pom| {
                matches!(&pom.object_map, ObjectMap::RefObjectMap(rom) if rom.parent_triples_map == tm.iri)
            })
        })
        .map(|tm| tm.iri.clone())
        .collect()
}

/// The execution plan for enumerating a mapping: the dims-first emission order
/// (every parent before its children, with any cyclic remainder appended) and
/// the set of parents that must be FULLY pre-indexed before the emit pass
/// (cyclic tables and self-referential ones, which cannot be indexed lazily in
/// a single forward pass). Shared by the in-memory driver here and the
/// streaming (provider-backed) driver so both order and pre-index identically.
#[derive(Debug, Clone)]
pub struct MaterializationPlan {
    /// TriplesMap IRIs in emission order.
    pub emit_order: Vec<String>,
    /// TriplesMap IRIs to pre-index before emission.
    pub preindex: HashSet<String>,
}

/// Compute the [`MaterializationPlan`] for a mapping.
pub fn plan(mapping: &CompiledR2rmlMapping) -> MaterializationPlan {
    let (order, cyclic) = dependency_order(mapping);
    let mut preindex: HashSet<String> = cyclic.iter().cloned().collect();
    preindex.extend(self_referential(mapping));
    let emit_order = order.into_iter().chain(cyclic).collect();
    MaterializationPlan {
        emit_order,
        preindex,
    }
}

// ---------------------------------------------------------------------------
// In-memory driver (tests + reference for the streaming driver)
// ---------------------------------------------------------------------------

/// Enumerate every triple of a mapping from in-memory batches keyed by
/// TriplesMap IRI. This is the reference driver used by tests and the shape the
/// streaming (provider-backed) builder mirrors: same dims-first ordering, same
/// per-batch [`emit_batch`] / [`ParentIndexSet::index_batch`] calls; only the
/// source of the batches differs (here a map, there a scan stream).
///
/// A TriplesMap with no batches in `batches` contributes nothing (an empty
/// table), which is correct.
pub fn enumerate_from_batches(
    mapping: &CompiledR2rmlMapping,
    batches: &HashMap<String, Vec<ColumnBatch>>,
    observer: &mut dyn TripleObserver,
) -> R2rmlResult<MaterializeStats> {
    let mut parents = ParentIndexSet::new(mapping)?;
    let materialization = plan(mapping);

    // Pre-index the parents that cannot be indexed lazily (cyclic / self-ref).
    for tm_iri in &materialization.preindex {
        if let (Some(tm), Some(tm_batches)) =
            (mapping.triples_maps.get(tm_iri), batches.get(tm_iri))
        {
            for batch in tm_batches {
                parents.index_batch(tm, batch)?;
            }
        }
    }

    // Emit in dims-first order. Lazily index a parent during its own emit pass
    // unless it was pre-indexed above.
    let mut stats = MaterializeStats::default();
    for tm_iri in &materialization.emit_order {
        let tm = match mapping.triples_maps.get(tm_iri) {
            Some(tm) => tm,
            None => continue,
        };
        if let Some(tm_batches) = batches.get(tm_iri) {
            for batch in tm_batches {
                if !materialization.preindex.contains(tm_iri) && parents.is_parent(tm_iri) {
                    parents.index_batch(tm, batch)?;
                }
                emit_batch(tm, batch, &parents, observer, &mut stats)?;
            }
        }
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{
        JoinCondition, ObjectMap, PredicateMap, PredicateObjectMap, RefObjectMap, SubjectMap,
        TriplesMap,
    };
    use fluree_db_tabular::{BatchSchema, Column, FieldInfo, FieldType};
    use std::sync::Arc;

    fn field(name: &str, ty: FieldType, nullable: bool, id: i32) -> FieldInfo {
        FieldInfo {
            name: name.to_string(),
            field_type: ty,
            nullable,
            field_id: id,
        }
    }

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    /// Run the enumerator over the fixture and return the sorted-unique triple
    /// set plus the stats.
    fn run(
        mapping: &CompiledR2rmlMapping,
        batches: &HashMap<String, Vec<ColumnBatch>>,
    ) -> (Vec<String>, MaterializeStats) {
        let mut collector = NTriplesCollector::default();
        let stats = enumerate_from_batches(mapping, batches, &mut collector).unwrap();
        (collector.sorted_unique(), stats)
    }

    // ------- dims table (parent) -------
    fn dim_customer_tm() -> TriplesMap {
        let mut tm = TriplesMap::new("<#Customer>", "dw.customer");
        tm.subject_map = SubjectMap::template("http://ex.org/customer/{c_key}")
            .with_class("http://ex.org/Customer")
            .with_class("http://ex.org/Party"); // multi-class
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex.org/name"),
            object_map: ObjectMap::column("name"),
        }];
        tm
    }

    fn customer_batch() -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![
            field("c_key", FieldType::Int64, false, 1),
            field("name", FieldType::String, true, 2),
        ]));
        // c_key=10 name=Acme; c_key=20 name=NULL (name triple dropped)
        ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(10), Some(20)]),
                Column::String(vec![s("Acme"), None]),
            ],
        )
        .unwrap()
    }

    // ------- fact table (child) with FK to Customer -------
    fn order_tm() -> TriplesMap {
        let mut tm = TriplesMap::new("<#Order>", "dw.orders");
        tm.subject_map =
            SubjectMap::template("http://ex.org/order/{o_key}").with_class("http://ex.org/Order");
        tm.predicate_object_maps = vec![
            // typed literal
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/amount"),
                object_map: ObjectMap::column_typed(
                    "amount",
                    "http://www.w3.org/2001/XMLSchema#decimal",
                ),
            },
            // FK edge to Customer
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/placedBy"),
                object_map: ObjectMap::RefObjectMap(RefObjectMap::new(
                    "<#Customer>",
                    "cust_key",
                    "c_key",
                )),
            },
        ];
        tm
    }

    fn order_batch() -> ColumnBatch {
        let schema = Arc::new(BatchSchema::new(vec![
            field("o_key", FieldType::Int64, false, 1),
            field("amount", FieldType::String, true, 2),
            field("cust_key", FieldType::Int64, true, 3),
        ]));
        // o1 → cust 10 (matches Acme); o2 → cust 99 (dangling → no edge);
        // o3 → cust NULL (no edge).
        ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(1), Some(2), Some(3)]),
                Column::String(vec![s("9.99"), s("5.00"), s("1.00")]),
                Column::Int64(vec![Some(10), Some(99), None]),
            ],
        )
        .unwrap()
    }

    fn star_mapping() -> CompiledR2rmlMapping {
        CompiledR2rmlMapping::new(vec![dim_customer_tm(), order_tm()])
    }

    fn star_batches() -> HashMap<String, Vec<ColumnBatch>> {
        let mut m = HashMap::new();
        m.insert("<#Customer>".to_string(), vec![customer_batch()]);
        m.insert("<#Order>".to_string(), vec![order_batch()]);
        m
    }

    #[test]
    fn emits_expected_star_schema_triples() {
        let (triples, stats) = run(&star_mapping(), &star_batches());

        let expected = vec![
            // Customer 10 (multi-class) + name
            "<http://ex.org/customer/10> <http://ex.org/name> \"Acme\" .",
            "<http://ex.org/customer/10> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Customer> .",
            "<http://ex.org/customer/10> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Party> .",
            // Customer 20 (name NULL → only the two type triples)
            "<http://ex.org/customer/20> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Customer> .",
            "<http://ex.org/customer/20> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Party> .",
            // Order 1: type, amount (decimal), placedBy → customer 10
            "<http://ex.org/order/1> <http://ex.org/amount> \"9.99\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            "<http://ex.org/order/1> <http://ex.org/placedBy> <http://ex.org/customer/10> .",
            "<http://ex.org/order/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Order> .",
            // Order 2: type + amount, but FK 99 is dangling → NO placedBy edge
            "<http://ex.org/order/2> <http://ex.org/amount> \"5.00\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            "<http://ex.org/order/2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Order> .",
            // Order 3: type + amount, FK NULL → NO placedBy edge
            "<http://ex.org/order/3> <http://ex.org/amount> \"1.00\"^^<http://www.w3.org/2001/XMLSchema#decimal> .",
            "<http://ex.org/order/3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex.org/Order> .",
        ];
        let mut expected: Vec<String> = expected.into_iter().map(String::from).collect();
        expected.sort();

        assert_eq!(triples, expected, "materialized triple set mismatch");

        // Stats cross-check.
        assert_eq!(
            stats.type_triples, 7,
            "2 customers×2 classes + 3 orders×1 class"
        );
        assert_eq!(stats.data_triples, 1 /*Acme name*/ + 3 /*amounts*/);
        assert_eq!(stats.ref_triples, 1, "only order 1 matches a customer");
        // order 2 (cust 99) is a present-but-unmatched FK = dangling; order 3's
        // FK is NULL (no reference intended), skipped like a null object — not
        // counted as dangling. Both correctly produce no placedBy edge.
        assert_eq!(
            stats.ref_dangling, 1,
            "order 2 (99) only; order 3 FK is null"
        );
        assert_eq!(stats.null_objects, 1, "customer 20 name is null");
        assert_eq!(
            *stats
                .ref_edges
                .get(&("<#Order>".to_string(), "http://ex.org/placedBy".to_string()))
                .unwrap(),
            1
        );
    }

    #[test]
    fn dependency_order_is_dims_first() {
        let mapping = star_mapping();
        let (order, cyclic) = dependency_order(&mapping);
        assert!(cyclic.is_empty());
        let cust = order.iter().position(|i| i == "<#Customer>").unwrap();
        let ord = order.iter().position(|i| i == "<#Order>").unwrap();
        assert!(
            cust < ord,
            "parent Customer must precede child Order: {order:?}"
        );
    }

    #[test]
    fn split_empty_and_merge_from_union_parent_indexes() {
        // The concurrent Pass-1 (O5) indexes each parent table into its own partial
        // index (`split_empty`) and merges them back (`merge_from`). Two partials
        // each holding a disjoint slice of the same parent must union on merge.
        let mapping = star_mapping();
        let base = ParentIndexSet::new(&mapping).unwrap();
        let cust = dim_customer_tm();

        let mut p1 = base.split_empty();
        p1.index_batch(&cust, &customer_batch()).unwrap(); // keys 10, 20

        let schema = Arc::new(BatchSchema::new(vec![
            field("c_key", FieldType::Int64, false, 1),
            field("name", FieldType::String, true, 2),
        ]));
        let batch2 = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(30)]),
                Column::String(vec![s("Globex")]),
            ],
        )
        .unwrap();
        let mut p2 = base.split_empty();
        p2.index_batch(&cust, &batch2).unwrap(); // key 30

        let mut merged = base; // empty index; needed carries the plan
        merged.merge_from(p1);
        merged.merge_from(p2);

        let cols = vec!["c_key".to_string()];
        assert!(merged.is_parent("<#Customer>"));
        assert!(
            merged
                .lookup("<#Customer>", &cols, &["10".to_string()])
                .is_some(),
            "key 10 from partial 1 must survive the merge"
        );
        assert!(
            merged
                .lookup("<#Customer>", &cols, &["30".to_string()])
                .is_some(),
            "key 30 from partial 2 must survive the merge"
        );
        assert!(
            merged
                .lookup("<#Customer>", &cols, &["99".to_string()])
                .is_none(),
            "an unindexed key must not resolve"
        );
    }

    #[test]
    fn fk_resolves_regardless_of_batch_map_iteration() {
        // Run many times: the HashMap batch iteration order varies, but dims-first
        // ordering must still index Customer before Order emits, so the edge is
        // always present.
        for _ in 0..50 {
            let (triples, stats) = run(&star_mapping(), &star_batches());
            assert_eq!(stats.ref_triples, 1);
            assert!(triples.contains(
                &"<http://ex.org/order/1> <http://ex.org/placedBy> <http://ex.org/customer/10> ."
                    .to_string()
            ));
        }
    }

    #[test]
    fn composite_key_fk_matches_on_both_columns() {
        // Parent keyed by (region, code); child references both.
        let mut parent = TriplesMap::new("<#Site>", "dw.site");
        parent.subject_map = SubjectMap::template("http://ex.org/site/{region}-{code}")
            .with_class("http://ex.org/Site");
        let mut child = TriplesMap::new("<#Visit>", "dw.visit");
        child.subject_map =
            SubjectMap::template("http://ex.org/visit/{v}").with_class("http://ex.org/Visit");
        child.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex.org/atSite"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::with_conditions(
                "<#Site>",
                vec![
                    JoinCondition::new("v_region", "region"),
                    JoinCondition::new("v_code", "code"),
                ],
            )),
        }];
        let mapping = CompiledR2rmlMapping::new(vec![parent, child]);

        let site_schema = Arc::new(BatchSchema::new(vec![
            field("region", FieldType::String, false, 1),
            field("code", FieldType::String, false, 2),
        ]));
        let site_batch = ColumnBatch::new(
            site_schema,
            vec![
                Column::String(vec![s("EU"), s("US")]),
                Column::String(vec![s("A1"), s("A1")]),
            ],
        )
        .unwrap();

        let visit_schema = Arc::new(BatchSchema::new(vec![
            field("v", FieldType::Int64, false, 1),
            field("v_region", FieldType::String, true, 2),
            field("v_code", FieldType::String, true, 3),
        ]));
        // visit1 → (EU,A1) match; visit2 → (EU,A9) no match (code differs).
        let visit_batch = ColumnBatch::new(
            visit_schema,
            vec![
                Column::Int64(vec![Some(1), Some(2)]),
                Column::String(vec![s("EU"), s("EU")]),
                Column::String(vec![s("A1"), s("A9")]),
            ],
        )
        .unwrap();

        let mut batches = HashMap::new();
        batches.insert("<#Site>".to_string(), vec![site_batch]);
        batches.insert("<#Visit>".to_string(), vec![visit_batch]);

        let (triples, stats) = run(&mapping, &batches);
        assert_eq!(stats.ref_triples, 1, "only (EU,A1) matches");
        assert!(triples.contains(
            &"<http://ex.org/visit/1> <http://ex.org/atSite> <http://ex.org/site/EU-A1> ."
                .to_string()
        ));
        assert!(!triples
            .iter()
            .any(|t| t.contains("visit/2") && t.contains("atSite")));
    }

    #[test]
    fn self_referential_fk_is_preindexed_and_resolves() {
        // Employee → manager (self reference). Manager row may appear in a later
        // batch than the employee referencing it; pre-indexing must still resolve.
        let mut emp = TriplesMap::new("<#Emp>", "dw.emp");
        emp.subject_map =
            SubjectMap::template("http://ex.org/emp/{id}").with_class("http://ex.org/Emp");
        emp.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex.org/manager"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new("<#Emp>", "mgr_id", "id")),
        }];
        let mapping = CompiledR2rmlMapping::new(vec![emp]);

        let schema = Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, false, 1),
            field("mgr_id", FieldType::Int64, true, 2),
        ]));
        // emp1 → mgr 2 (defined in the SAME batch, later row); emp2 → no manager.
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(1), Some(2)]),
                Column::Int64(vec![Some(2), None]),
            ],
        )
        .unwrap();
        let mut batches = HashMap::new();
        batches.insert("<#Emp>".to_string(), vec![batch]);

        let (triples, stats) = run(&mapping, &batches);
        assert_eq!(stats.ref_triples, 1);
        assert!(triples.contains(
            &"<http://ex.org/emp/1> <http://ex.org/manager> <http://ex.org/emp/2> .".to_string()
        ));
    }

    #[test]
    fn null_subject_key_drops_entire_row() {
        let mut tm = TriplesMap::new("<#T>", "dw.t");
        tm.subject_map = SubjectMap::template("http://ex.org/t/{id}").with_class("http://ex.org/T");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex.org/p"),
            object_map: ObjectMap::column("v"),
        }];
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        let schema = Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, true, 1),
            field("v", FieldType::String, true, 2),
        ]));
        // Row 0: id NULL → whole row dropped even though v is present.
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![None, Some(7)]),
                Column::String(vec![s("x"), s("y")]),
            ],
        )
        .unwrap();
        let mut batches = HashMap::new();
        batches.insert("<#T>".to_string(), vec![batch]);
        let (triples, stats) = run(&mapping, &batches);
        assert_eq!(stats.subjects, 1, "only row 1 produced a subject");
        assert_eq!(triples.len(), 2, "row 1: type + p; row 0 fully dropped");
        assert!(triples.iter().all(|t| t.contains("/t/7")));
    }

    #[test]
    fn lang_and_constant_and_template_objects_render() {
        let mut tm = TriplesMap::new("<#T>", "dw.t");
        tm.subject_map = SubjectMap::template("http://ex.org/t/{id}");
        tm.predicate_object_maps = vec![
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/label"),
                object_map: ObjectMap::Column {
                    column: "label".to_string(),
                    datatype: None,
                    language: Some("en".to_string()),
                    term_type: crate::mapping::TermType::Literal,
                },
            },
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/kind"),
                object_map: ObjectMap::constant_iri("http://ex.org/Widget"),
            },
            PredicateObjectMap {
                predicate_map: PredicateMap::constant("http://ex.org/homepage"),
                object_map: ObjectMap::template("http://ex.org/page/{id}", vec!["id".to_string()]),
            },
        ];
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        let schema = Arc::new(BatchSchema::new(vec![
            field("id", FieldType::Int64, false, 1),
            field("label", FieldType::String, true, 2),
        ]));
        let batch = ColumnBatch::new(
            schema,
            vec![
                Column::Int64(vec![Some(1)]),
                Column::String(vec![s("Hello")]),
            ],
        )
        .unwrap();
        let mut batches = HashMap::new();
        batches.insert("<#T>".to_string(), vec![batch]);
        let (triples, _stats) = run(&mapping, &batches);
        assert!(triples
            .contains(&"<http://ex.org/t/1> <http://ex.org/label> \"Hello\"@en .".to_string()));
        assert!(triples.contains(
            &"<http://ex.org/t/1> <http://ex.org/kind> <http://ex.org/Widget> .".to_string()
        ));
        assert!(triples.contains(
            &"<http://ex.org/t/1> <http://ex.org/homepage> <http://ex.org/page/1> .".to_string()
        ));
    }

    #[test]
    fn unknown_parent_ref_is_rejected() {
        let mut tm = TriplesMap::new("<#Child>", "dw.child");
        tm.subject_map = SubjectMap::template("http://ex.org/c/{id}");
        tm.predicate_object_maps = vec![PredicateObjectMap {
            predicate_map: PredicateMap::constant("http://ex.org/ref"),
            object_map: ObjectMap::RefObjectMap(RefObjectMap::new("<#Missing>", "fk", "id")),
        }];
        let mapping = CompiledR2rmlMapping::new(vec![tm]);
        let err = ParentIndexSet::new(&mapping).unwrap_err();
        assert!(format!("{err}").contains("unknown parent"), "got: {err}");
    }

    #[test]
    fn empty_join_ref_is_rejected() {
        let rom = RefObjectMap::with_conditions("<#P>", vec![]);
        let err = canonical_join(&rom).unwrap_err();
        assert!(
            format!("{err}").contains("no join conditions"),
            "got: {err}"
        );
    }
}
