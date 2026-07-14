//! Typed Cypher result cells for value-typed transports (Bolt).
//!
//! The JSON transport flattens everything to strings/numbers
//! ([`super::cypher`]); PackStream carries real graph and temporal values.
//! This module walks the same columns/rows as the JSON formatter but keeps
//! cells **typed**: node refs hydrate into [`CypherNode`] (labels +
//! properties fetched per subject at format time), relationship values keep
//! their endpoints and annotation properties, and temporal literals carry
//! epoch components instead of ISO strings. Naming (labels, property keys,
//! relationship types) reuses the engine's Cypher rule
//! ([`fluree_db_query::eval::cypher_name_from_iri`]) so `labels(n)` and a
//! returned node never disagree.
//!
//! Hydration reads raw SPOT state (snapshot + overlay at the view's `t`),
//! which bypasses the scan operators' per-flake policy filtering — so when
//! the view carries a policy, every fetched subject's flakes run through
//! the same enforcer the scan path uses
//! (`QueryPolicyEnforcer::filter_flakes_for_graph`, with the class cache
//! populated for `f:onClass` targeting) before any property is rendered.

use std::collections::HashMap;

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value as JsonValue;

use super::iri::IriCompactor;
use super::{FormatError, Result};
use crate::query::QueryResult;
use crate::view::GraphDb;
use fluree_db_core::{FlakeValue, IndexType, RangeMatch, RangeOptions, RangeTest, Sid};
use fluree_db_query::binding::Binding;
use fluree_db_query::eval::cypher_name_from_iri;

/// Predicates under the Fluree system namespace (`db:reifies*`, the
/// `db:Node` existence marker's class triples, ...) are internal wiring,
/// never user properties.
const FLUREE_SYSTEM_NS: &str = "https://ns.flur.ee/";

/// One result cell, typed for transports with richer value models than JSON.
#[derive(Debug, Clone, PartialEq)]
pub enum CypherCell {
    /// Plain scalar/string/bool/@json — RDF-faithful JSON as produced by
    /// the shared per-binding formatter.
    Value(JsonValue),
    /// `xsd:decimal` exact lexical form (PackStream has no decimal type;
    /// the transport decides the degradation).
    Decimal(String),
    /// Arbitrary-precision integer that may exceed i64.
    BigInt(String),
    Temporal(CypherTemporal),
    List(Vec<CypherCell>),
    Map(Vec<(String, CypherCell)>),
    Node(Box<CypherNode>),
    Relationship(Box<CypherRelationship>),
    Path(Box<CypherPath>),
}

/// A hydrated node: durable identity (IRI), Cypher labels, and properties.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherNode {
    /// Full IRI — the durable identity (`elementId` on Bolt).
    pub iri: std::sync::Arc<str>,
    /// `rdf:type` classes as Cypher label names (the `db:Node` existence
    /// marker is hidden, matching `labels()`).
    pub labels: Vec<std::sync::Arc<str>>,
    pub properties: Vec<(std::sync::Arc<str>, CypherCell)>,
}

/// A relationship value: endpoints, type, and annotation properties.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherRelationship {
    pub start_iri: std::sync::Arc<str>,
    pub end_iri: std::sync::Arc<str>,
    /// Cypher relationship type (predicate IRI local name).
    pub type_name: std::sync::Arc<str>,
    /// The reifier subject's IRI when the edge is reified — the durable
    /// relationship identity where one exists.
    pub reifier_iri: Option<std::sync::Arc<str>>,
    pub properties: Vec<(std::sync::Arc<str>, CypherCell)>,
}

/// A path as Bolt models it: unique node and relationship lists plus the
/// alternating (relationship, node) index sequence describing the walk
/// from `nodes[0]`. Relationship indices are 1-based and negated when the
/// hop traverses the edge end→start; node indices are 0-based.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherPath {
    pub nodes: Vec<CypherNode>,
    pub rels: Vec<CypherRelationship>,
    pub indices: Vec<i64>,
}

/// A temporal literal with epoch components. `iso` keeps the original
/// lexical form for transports (or clients) that prefer strings.
#[derive(Debug, Clone, PartialEq)]
pub enum CypherTemporal {
    /// `xsd:date` — days since 1970-01-01.
    Date { days: i64, iso: String },
    /// `xsd:dateTime` — UTC epoch seconds + subsecond nanos; the original
    /// offset in seconds when the lexical form carried one.
    DateTime {
        epoch_seconds: i64,
        nanos: u32,
        tz_offset_seconds: Option<i32>,
        iso: String,
    },
    /// `xsd:time` — nanoseconds since midnight; offset as for DateTime.
    Time {
        nanos_since_midnight: i64,
        tz_offset_seconds: Option<i32>,
        iso: String,
    },
}

/// The typed counterpart of [`super::cypher::table`]: same column
/// selection, same rows, typed cells. Async because node/relationship
/// cells fetch their properties from the view.
pub(crate) async fn typed_table(
    result: &QueryResult,
    compactor: &IriCompactor,
    view: &GraphDb,
) -> Result<(Vec<String>, Vec<Vec<CypherCell>>)> {
    let col_vars = super::cypher::column_vars(result);
    let columns: Vec<String> = col_vars
        .iter()
        .map(|&v| result.vars.name(v).to_string())
        .collect();

    let mut hydrator = NodeHydrator::new(view, compactor, result.binary_graph.as_ref());

    // Prefetch pass: the engine has already produced the subject list, so
    // the remaining work is a bulk read, not N point probes. Subjects that
    // arrive as `EncodedSid` carry their raw s_id, which the overlay-free
    // batched lane consumes directly.
    let mut wanted: Vec<WantedSubject> = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            for &var_id in &col_vars {
                if let Some(b) = batch.get(row_idx, var_id) {
                    collect_subject_sids(result.binary_graph.as_ref(), b, &mut wanted)?;
                }
            }
        }
    }
    hydrator.prefetch(wanted).await?;

    let mut rows = Vec::new();
    for batch in &result.batches {
        for row_idx in 0..batch.len() {
            let mut row = Vec::with_capacity(col_vars.len());
            for &var_id in &col_vars {
                let cell = match batch.get(row_idx, var_id) {
                    Some(b) => binding_cell(result, b, &mut hydrator).await?,
                    None => CypherCell::Value(JsonValue::Null),
                };
                row.push(cell);
            }
            rows.push(row);
        }
    }
    Ok((columns, rows))
}

/// One subject the prefetch pass will hydrate. The raw `s_id` is carried
/// when the binding had one (`EncodedSid`) — it selects the batched
/// sorted-crawl lane, which never needs the dictionary again.
struct WantedSubject {
    sid: Sid,
    s_id: Option<u64>,
}

/// Collect every subject this binding will hydrate when rendered: node
/// refs, relationship reifiers (annotation properties), and path nodes.
/// Mirrors the dispatch in [`binding_cell`]. Encoded subject refs
/// (`EncodedSid`, the shape bare node vars carry off the binary scan)
/// materialize here — skipping them silently empties the prefetch and
/// degrades every row to a serial fetch.
fn collect_subject_sids(
    gv: Option<&fluree_db_binary_index::BinaryGraphView>,
    binding: &Binding,
    out: &mut Vec<WantedSubject>,
) -> Result<()> {
    if binding.is_encoded() {
        if let Some(s_id) = binding.encoded_s_id() {
            let materialized = super::materialize::materialize_with_graph(gv, binding)?;
            if let Binding::Sid { sid, .. } = materialized {
                out.push(WantedSubject {
                    sid,
                    s_id: Some(s_id),
                });
            }
        }
        return Ok(());
    }
    match binding {
        Binding::Sid { sid, .. } => out.push(WantedSubject {
            sid: sid.clone(),
            s_id: None,
        }),
        Binding::IriMatch { primary_sid, .. } => out.push(WantedSubject {
            sid: primary_sid.clone(),
            s_id: None,
        }),
        Binding::Rel(rel) => {
            if let Some(reifier) = &rel.reifier {
                out.push(WantedSubject {
                    sid: reifier.clone(),
                    s_id: None,
                });
            }
        }
        Binding::Path { nodes, .. } => out.extend(nodes.iter().map(|sid| WantedSubject {
            sid: sid.clone(),
            s_id: None,
        })),
        Binding::List(values) | Binding::Grouped(values) => {
            for v in values {
                collect_subject_sids(gv, v, out)?;
            }
        }
        Binding::Map(entries) => {
            for (_, v) in entries {
                collect_subject_sids(gv, v, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Hydrate a flat list of subjects into [`CypherNode`]s against `view`
/// (prefetched with bounded concurrency). Used by the write-RETURN path,
/// where the created-entity Sids are known without a query result.
pub(crate) async fn hydrate_nodes(
    view: &GraphDb,
    compactor: &IriCompactor,
    sids: &[Sid],
) -> Result<Vec<CypherNode>> {
    let mut hydrator = NodeHydrator::new(view, compactor, None);
    hydrator
        .prefetch(
            sids.iter()
                .map(|sid| WantedSubject {
                    sid: sid.clone(),
                    s_id: None,
                })
                .collect(),
        )
        .await?;
    let mut nodes = Vec::with_capacity(sids.len());
    for sid in sids {
        nodes.push(hydrator.node(sid).await?);
    }
    Ok(nodes)
}

fn binding_cell<'a>(
    result: &'a QueryResult,
    binding: &'a Binding,
    hydrator: &'a mut NodeHydrator<'_>,
) -> BoxFuture<'a, Result<CypherCell>> {
    async move {
        if binding.is_encoded() {
            let materialized = super::materialize::materialize_binding(result, binding)?;
            return binding_cell_owned(result, materialized, hydrator).await;
        }
        match binding {
            Binding::Unbound | Binding::Poisoned => Ok(CypherCell::Value(JsonValue::Null)),
            Binding::Sid { sid, .. } => hydrator.subject_cell(sid).await,
            Binding::IriMatch { primary_sid, .. } => hydrator.subject_cell(primary_sid).await,
            Binding::Rel(rel) => {
                let start_iri = hydrator.compactor.decode_sid_shared(&rel.start)?;
                let end_iri = hydrator.compactor.decode_sid_shared(&rel.end)?;
                let type_iri = hydrator.compactor.decode_sid(&rel.predicate)?;
                let (reifier_iri, properties) = match &rel.reifier {
                    Some(reifier) => (
                        Some(hydrator.compactor.decode_sid_shared(reifier)?),
                        hydrator.annotation_properties(reifier).await?,
                    ),
                    None => (None, Vec::new()),
                };
                Ok(CypherCell::Relationship(Box::new(CypherRelationship {
                    start_iri,
                    end_iri,
                    type_name: cypher_name_from_iri(&type_iri).into(),
                    reifier_iri,
                    properties,
                })))
            }
            Binding::Path { nodes, edges } => Ok(CypherCell::Path(Box::new(
                hydrator.path(nodes, edges).await?,
            ))),
            Binding::List(values) | Binding::Grouped(values) => {
                let mut cells = Vec::with_capacity(values.len());
                for v in values {
                    cells.push(binding_cell(result, v, hydrator).await?);
                }
                Ok(CypherCell::List(cells))
            }
            Binding::Map(entries) => {
                let mut cells = Vec::with_capacity(entries.len());
                for (k, v) in entries {
                    cells.push((k.to_string(), binding_cell(result, v, hydrator).await?));
                }
                Ok(CypherCell::Map(cells))
            }
            Binding::Lit { val, .. } => match val {
                FlakeValue::Decimal(d) => Ok(CypherCell::Decimal(d.to_plain_string())),
                FlakeValue::BigInt(n) => Ok(CypherCell::BigInt(n.to_string())),
                FlakeValue::Date(d) => Ok(CypherCell::Temporal(date_cell(d))),
                FlakeValue::DateTime(dt) => Ok(CypherCell::Temporal(datetime_cell(dt))),
                FlakeValue::Time(t) => Ok(CypherCell::Temporal(time_cell(t))),
                _ => Ok(CypherCell::Value(
                    super::jsonld::format_binding_with_result(result, binding, hydrator.compactor)?,
                )),
            },
            _ => Ok(CypherCell::Value(
                super::jsonld::format_binding_with_result(result, binding, hydrator.compactor)?,
            )),
        }
    }
    .boxed()
}

/// Owned-binding variant for post-materialization recursion.
async fn binding_cell_owned(
    result: &QueryResult,
    binding: Binding,
    hydrator: &mut NodeHydrator<'_>,
) -> Result<CypherCell> {
    binding_cell(result, &binding, hydrator).await
}

fn date_cell(d: &fluree_db_core::temporal::Date) -> CypherTemporal {
    CypherTemporal::Date {
        days: d.days_since_epoch() as i64,
        iso: d.original().to_string(),
    }
}

fn datetime_cell(dt: &fluree_db_core::temporal::DateTime) -> CypherTemporal {
    let micros = dt.epoch_micros();
    CypherTemporal::DateTime {
        epoch_seconds: micros.div_euclid(1_000_000),
        nanos: (micros.rem_euclid(1_000_000) * 1_000) as u32,
        tz_offset_seconds: dt.tz_offset().map(|o| o.local_minus_utc()),
        iso: dt.original().to_string(),
    }
}

fn time_cell(t: &fluree_db_core::temporal::Time) -> CypherTemporal {
    let whole_minutes_secs = (t.hours() as f64) * 3600.0 + (t.minutes() as f64) * 60.0;
    let nanos = (whole_minutes_secs + t.seconds()) * 1_000_000_000.0;
    CypherTemporal::Time {
        nanos_since_midnight: nanos.round() as i64,
        tz_offset_seconds: t.tz_offset().map(|o| o.local_minus_utc()),
        iso: t.original().to_string(),
    }
}

/// The subjects the overlay contributes flakes for, per graph — the
/// per-subject gate for the batched crawl lane (untouched subjects read
/// base truth; touched ones need the merge-correct per-subject path).
///
/// Derived from one full overlay SPOT walk and cached process-wide keyed
/// on `(content_version, g_id)` — the contract `content_version` exists
/// for. Returns `None` when the overlay can't be safely summarized (no
/// version stamp): callers must treat every subject as dirty.
fn overlay_dirty_subjects(
    overlay: &dyn fluree_db_core::OverlayProvider,
    g_id: u16,
) -> Option<Arc<std::collections::HashSet<Sid>>> {
    use std::sync::OnceLock;
    type DirtyCache =
        parking_lot::Mutex<lru::LruCache<(u64, u16), Arc<std::collections::HashSet<Sid>>>>;
    static CACHE: OnceLock<DirtyCache> = OnceLock::new();

    if overlay.is_effectively_empty() {
        static EMPTY: OnceLock<Arc<std::collections::HashSet<Sid>>> = OnceLock::new();
        return Some(Arc::clone(
            EMPTY.get_or_init(|| Arc::new(std::collections::HashSet::new())),
        ));
    }
    let version = overlay.content_version()?;
    let cache = CACHE.get_or_init(|| {
        parking_lot::Mutex::new(lru::LruCache::new(
            std::num::NonZeroUsize::new(8).expect("nonzero"),
        ))
    });
    if let Some(hit) = cache.lock().get(&(version, g_id)) {
        return Some(Arc::clone(hit));
    }
    let mut subjects = std::collections::HashSet::new();
    // Collect with no to_t cap: a superset stays conservative (a subject
    // whose overlay flakes are all beyond the view's `t` just falls back).
    overlay.for_each_overlay_flake(
        g_id,
        IndexType::Spot,
        None,
        None,
        true,
        i64::MAX,
        &mut |flake| {
            subjects.insert(flake.s.clone());
        },
    );
    let subjects = Arc::new(subjects);
    cache.lock().put((version, g_id), Arc::clone(&subjects));
    Some(subjects)
}

/// Fetches and caches node hydrations for one table walk.
struct NodeHydrator<'a> {
    view: &'a GraphDb,
    compactor: &'a IriCompactor,
    /// Binary graph of the result, when it has one — carries the store the
    /// batched sorted-crawl lane reads. Forced to `None` under a non-root
    /// policy: the batched lane builds nodes from raw index rows, bypassing
    /// the flake-level policy filter.
    gv: Option<&'a fluree_db_binary_index::BinaryGraphView>,
    /// The view's policy enforcer: hydration fetches read raw SPOT state,
    /// so per-flake view policy is applied here (never `None` when the
    /// view has a non-root policy).
    enforcer: Option<Arc<fluree_db_query::policy::QueryPolicyEnforcer>>,
    rdf_type: Option<Sid>,
    node_marker: Option<Sid>,
    /// The `f:reifies{Subject,Predicate,Object}` predicate Sids (when the
    /// dictionary knows them): a subject carrying these is an edge
    /// annotation and renders as a Relationship, never a Node.
    reifies_subject: Option<Sid>,
    reifies_predicate: Option<Sid>,
    reifies_object: Option<Sid>,
    cache: HashMap<Sid, CypherNode>,
    /// Rendered top-level cells per subject (Node, or Relationship for
    /// reifier subjects); what [`Self::subject_cell`] serves.
    cell_cache: HashMap<Sid, CypherCell>,
    /// Raw subject flakes, shared by node hydration, annotation
    /// properties, and path nodes; populated in bulk by [`Self::prefetch`].
    flake_cache: HashMap<Sid, Arc<Vec<fluree_db_core::Flake>>>,
    /// Predicate Sid → Cypher property key (None = system predicate,
    /// hidden). Predicates repeat across every node; decoding each flake's
    /// predicate IRI per occurrence is pure waste.
    key_names: HashMap<Sid, Option<Arc<str>>>,
    /// Class Sid → Cypher label (None = the hidden `db:Node` marker).
    label_names: HashMap<Sid, Option<Arc<str>>>,
    /// p_id → Cypher property key for the batched lane (None = system
    /// predicate, hidden).
    key_names_by_pid: HashMap<u32, Option<Arc<str>>>,
}

/// Concurrent subject fetches in flight during [`NodeHydrator::prefetch`].
const PREFETCH_CONCURRENCY: usize = 16;

impl<'a> NodeHydrator<'a> {
    fn new(
        view: &'a GraphDb,
        compactor: &'a IriCompactor,
        gv: Option<&'a fluree_db_binary_index::BinaryGraphView>,
    ) -> Self {
        let enforcer = view
            .policy_enforcer()
            .filter(|e| !e.is_root())
            .map(Arc::clone);
        Self {
            view,
            compactor,
            // Under policy every subject takes the fallback lane, where
            // `apply_view_policy` filters the fetched flakes per subject.
            gv: if enforcer.is_none() { gv } else { None },
            enforcer,
            rdf_type: view.snapshot.encode_iri(fluree_vocab::rdf::TYPE),
            node_marker: view.snapshot.encode_iri(fluree_vocab::fluree::NODE),
            reifies_subject: view
                .snapshot
                .encode_iri(fluree_vocab::reifies_iris::SUBJECT),
            reifies_predicate: view
                .snapshot
                .encode_iri(fluree_vocab::reifies_iris::PREDICATE),
            reifies_object: view.snapshot.encode_iri(fluree_vocab::reifies_iris::OBJECT),
            cell_cache: HashMap::new(),
            cache: HashMap::new(),
            flake_cache: HashMap::new(),
            key_names: HashMap::new(),
            label_names: HashMap::new(),
            key_names_by_pid: HashMap::new(),
        }
    }

    /// Run one subject batch's raw-SPOT fetch through the view policy — the
    /// same two-step enforcement the scan path applies
    /// (`BinaryScanOperator::filter_flakes_by_policy`): populate the class
    /// cache for `f:onClass` targeting, then per-flake filtering with
    /// `f:query` support against the view's own snapshot/overlay/t.
    async fn apply_view_policy(
        &self,
        subjects: &[Sid],
        flakes: Vec<fluree_db_core::Flake>,
    ) -> Result<Vec<fluree_db_core::Flake>> {
        let Some(enforcer) = self.enforcer.as_ref() else {
            return Ok(flakes);
        };
        if flakes.is_empty() {
            return Ok(flakes);
        }
        let db = self.view.as_graph_db_ref();
        enforcer
            .populate_class_cache_for_graph(db, subjects)
            .await
            .map_err(|e| FormatError::InvalidBinding(format!("policy class lookup failed: {e}")))?;
        let tracker = fluree_db_core::Tracker::disabled();
        enforcer
            .filter_flakes_for_graph(db.snapshot, db.overlay, db.t, &tracker, flakes)
            .await
            .map_err(|e| FormatError::InvalidBinding(format!("policy filtering failed: {e}")))
    }

    /// Memoized predicate Sid → property key (`None` hides system predicates).
    fn key_name(&mut self, pred: &Sid) -> Result<Option<Arc<str>>> {
        if let Some(hit) = self.key_names.get(pred) {
            return Ok(hit.clone());
        }
        let p_iri = self.compactor.decode_sid(pred)?;
        let name = if p_iri.starts_with(FLUREE_SYSTEM_NS) {
            None
        } else {
            Some(Arc::from(cypher_name_from_iri(&p_iri).as_str()))
        };
        self.key_names.insert(pred.clone(), name.clone());
        Ok(name)
    }

    /// Memoized class Sid → label (`None` hides the `db:Node` marker).
    fn label_name(&mut self, class_sid: &Sid) -> Result<Option<Arc<str>>> {
        if let Some(hit) = self.label_names.get(class_sid) {
            return Ok(hit.clone());
        }
        let name = if Some(class_sid) == self.node_marker.as_ref() {
            None
        } else {
            let class_iri = self.compactor.decode_sid(class_sid)?;
            Some(Arc::from(cypher_name_from_iri(&class_iri).as_str()))
        };
        self.label_names.insert(class_sid.clone(), name.clone());
        Ok(name)
    }

    /// Bulk-fetch every not-yet-cached subject the table will render.
    ///
    /// Two lanes:
    /// - **Batched sorted crawl** — subjects that arrived with a raw `s_id`
    ///   (`EncodedSid` off the binary scan), when the overlay is certainly
    ///   empty: one gap-aware sorted SPOT sweep
    ///   ([`batched_lookup_subject_properties`]) decodes each touched
    ///   leaflet once and never re-enters the dictionary for ref-valued
    ///   rows (relationships are skipped by `o_type` before any decode).
    /// - **Per-subject fallback** — everything else (no s_id, or live
    ///   novelty): CPU-bound point reads spawned as chunked tasks across
    ///   runtime workers, in subject order.
    async fn prefetch(&mut self, wanted: Vec<WantedSubject>) -> Result<()> {
        let mut batched: Vec<(u64, Sid)> = Vec::new();
        let mut fallback: Vec<Sid> = Vec::new();
        // Per-subject lane choice: base truth (the batched sweep) is only
        // complete for subjects the overlay doesn't touch. `None` means the
        // overlay can't be summarized — every subject is treated as dirty.
        let dirty = if self.gv.is_some() {
            overlay_dirty_subjects(&*self.view.overlay, self.view.graph_id)
        } else {
            None
        };
        let mut seen: std::collections::HashSet<Sid> = std::collections::HashSet::new();
        for subject in wanted {
            if self.cell_cache.contains_key(&subject.sid)
                || self.flake_cache.contains_key(&subject.sid)
                || !seen.insert(subject.sid.clone())
            {
                continue;
            }
            let clean = matches!(&dirty, Some(dirty) if !dirty.contains(&subject.sid));
            if !clean {
                fallback.push(subject.sid);
                continue;
            }
            // Under live novelty the executor emits materialized `Sid`
            // bindings (no raw s_id); a clean subject still belongs on the
            // batched lane — one reverse dict lookup is far cheaper than a
            // fallback point read. Novelty-only subjects (no persisted
            // s_id) genuinely need the merge path.
            let s_id = subject.s_id.or_else(|| {
                self.gv.and_then(|gv| {
                    gv.store()
                        .find_subject_id_by_parts(subject.sid.namespace_code, &subject.sid.name)
                        .ok()
                        .flatten()
                })
            });
            match s_id {
                Some(s_id) => batched.push((s_id, subject.sid)),
                None => fallback.push(subject.sid),
            }
        }

        tracing::debug!(
            batched = batched.len(),
            fallback = fallback.len(),
            "typed-table hydration lanes"
        );
        if !batched.is_empty() {
            self.crawl_batched(batched)?;
        }
        self.prefetch_fallback(fallback).await
    }

    /// The batched lane: sorted SPOT sweep over raw s_ids, building nodes
    /// straight from `(p_id, o_type, o_key)` rows. Overlay-free only — the
    /// caller gates on `OverlayProvider::is_effectively_empty`.
    fn crawl_batched(&mut self, mut subjects: Vec<(u64, Sid)>) -> Result<()> {
        let gv = self.gv.expect("batch lane gated on gv");
        let store = gv.clone_store();
        let g_id = gv.g_id();
        subjects.sort_unstable_by_key(|(s_id, _)| *s_id);
        let s_ids: Vec<u64> = subjects.iter().map(|(s_id, _)| *s_id).collect();

        let mut rows_by_subject =
            fluree_db_binary_index::read::batched_lookup::batched_lookup_subject_properties(
                &store,
                g_id,
                &s_ids,
                self.view.t,
            )
            .map_err(|e| {
                FormatError::InvalidBinding(format!("batched subject crawl failed: {e}"))
            })?;

        let reifies_pids: Option<(u32, u32, u32)> = match (
            self.reifies_subject.as_ref(),
            self.reifies_predicate.as_ref(),
            self.reifies_object.as_ref(),
        ) {
            (Some(rs), Some(rp), Some(ro)) => match (
                store.sid_to_p_id(rs),
                store.sid_to_p_id(rp),
                store.sid_to_p_id(ro),
            ) {
                (Some(a), Some(b), Some(c)) => Some((a, b, c)),
                _ => None,
            },
            _ => None,
        };
        for (s_id, sid) in subjects {
            let rows = rows_by_subject.remove(&s_id).unwrap_or_default();
            let cell = if let Some(rel) =
                self.relationship_from_rows(&sid, &rows, store.as_ref(), g_id, reifies_pids)?
            {
                CypherCell::Relationship(Box::new(rel))
            } else {
                let node = self.node_from_rows(&sid, &rows, store.as_ref(), g_id)?;
                self.cache.insert(sid.clone(), node.clone());
                CypherCell::Node(Box::new(node))
            };
            self.cell_cache.insert(sid, cell);
        }
        Ok(())
    }

    /// Detect and render a reifier subject from batched-crawl rows: present
    /// `f:reifies{Subject,Predicate,Object}` rows with node-ref objects make
    /// it a Relationship. Returns `None` for ordinary nodes.
    fn relationship_from_rows(
        &mut self,
        sid: &Sid,
        rows: &[(u32, u16, u64)],
        store: &fluree_db_binary_index::BinaryIndexStore,
        g_id: u16,
        reifies_pids: Option<(u32, u32, u32)>,
    ) -> Result<Option<CypherRelationship>> {
        let Some((rs_pid, rp_pid, ro_pid)) = reifies_pids else {
            return Ok(None);
        };
        let mut start = None;
        let mut pred = None;
        let mut end = None;
        for &(p_id, o_type, o_key) in rows {
            if p_id != rs_pid && p_id != rp_pid && p_id != ro_pid {
                continue;
            }
            if !fluree_db_core::o_type::OType::from_u16(o_type).is_node_ref() {
                continue;
            }
            let decoded = store
                .decode_value_v3(o_type, o_key, p_id, g_id)
                .map_err(|e| FormatError::InvalidBinding(format!("decode reifies ref: {e}")))?;
            let FlakeValue::Ref(target) = decoded else {
                continue;
            };
            if p_id == rs_pid {
                start = Some(target);
            } else if p_id == rp_pid {
                pred = Some(target);
            } else {
                end = Some(target);
            }
        }
        let (Some(start), Some(pred), Some(end)) = (start, pred, end) else {
            return Ok(None);
        };
        // Annotation (user) properties: the scalar rows minus bookkeeping.
        let node = self.node_from_rows(sid, rows, store, g_id)?;
        let type_iri = self.compactor.decode_sid(&pred)?;
        Ok(Some(CypherRelationship {
            start_iri: self.compactor.decode_sid_shared(&start)?,
            end_iri: self.compactor.decode_sid_shared(&end)?,
            type_name: cypher_name_from_iri(&type_iri).into(),
            reifier_iri: Some(node.iri),
            properties: node.properties,
        }))
    }

    /// Build a node from batched-crawl rows. Ref-valued rows are skipped by
    /// `o_type` **before** decoding — that skip (no dict/arena touch per
    /// edge target) is the point of the batched lane.
    fn node_from_rows(
        &mut self,
        sid: &Sid,
        rows: &[(u32, u16, u64)],
        store: &fluree_db_binary_index::BinaryIndexStore,
        g_id: u16,
    ) -> Result<CypherNode> {
        let iri = self.compactor.decode_sid_shared(sid)?;
        let rdf_type_p_id = self
            .rdf_type
            .as_ref()
            .and_then(|sid| store.sid_to_p_id(sid));
        let mut labels = Vec::new();
        let mut props: Vec<(Arc<str>, Vec<CypherCell>)> = Vec::new();
        for &(p_id, o_type, o_key) in rows {
            if Some(p_id) == rdf_type_p_id {
                let decoded = store
                    .decode_value_v3(o_type, o_key, p_id, g_id)
                    .map_err(|e| FormatError::InvalidBinding(format!("decode class ref: {e}")))?;
                if let FlakeValue::Ref(class_sid) = decoded {
                    if let Some(label) = self.label_name(&class_sid)? {
                        labels.push(label);
                    }
                }
                continue;
            }
            if fluree_db_core::o_type::OType::from_u16(o_type).is_node_ref() {
                continue;
            }
            let Some(key) = self.key_name_by_pid(p_id, store)? else {
                continue;
            };
            let decoded = store
                .decode_value_v3(o_type, o_key, p_id, g_id)
                .map_err(|e| FormatError::InvalidBinding(format!("decode property: {e}")))?;
            let cell = self.flake_value_cell(&decoded)?;
            match props.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref()) {
                Some((_, cells)) => cells.push(cell),
                None => props.push((key, vec![cell])),
            }
        }
        Ok(CypherNode {
            iri,
            labels,
            properties: props
                .into_iter()
                .map(|(k, mut cells)| {
                    let cell = if cells.len() == 1 {
                        cells.pop().expect("one cell")
                    } else {
                        CypherCell::List(cells)
                    };
                    (k, cell)
                })
                .collect(),
        })
    }

    /// Memoized p_id → property key (`None` hides system predicates).
    fn key_name_by_pid(
        &mut self,
        p_id: u32,
        store: &fluree_db_binary_index::BinaryIndexStore,
    ) -> Result<Option<Arc<str>>> {
        if let Some(hit) = self.key_names_by_pid.get(&p_id) {
            return Ok(hit.clone());
        }
        let name = match store.resolve_predicate_iri(p_id) {
            Some(iri) if iri.starts_with(FLUREE_SYSTEM_NS) => None,
            Some(iri) => Some(Arc::from(cypher_name_from_iri(iri).as_str())),
            None => None,
        };
        self.key_names_by_pid.insert(p_id, name.clone());
        Ok(name)
    }

    /// The fallback lane: per-subject point reads, spawned as chunked
    /// tasks (CPU-bound work — cursor setup, leaflet decode, dict
    /// resolves — needs parallel workers, not cooperative concurrency).
    async fn prefetch_fallback(&mut self, mut sids: Vec<Sid>) -> Result<()> {
        sids.sort_unstable_by(|a, b| {
            (a.namespace_code, a.name.as_ref()).cmp(&(b.namespace_code, b.name.as_ref()))
        });
        sids.dedup();
        if sids.is_empty() {
            return Ok(());
        }

        let workers = std::thread::available_parallelism()
            .map(std::num::NonZero::get)
            .unwrap_or(4)
            .min(PREFETCH_CONCURRENCY)
            .min(sids.len());
        let chunk_size = sids.len().div_ceil(workers);
        let mut handles = Vec::with_capacity(workers);
        for chunk in sids.chunks(chunk_size) {
            let chunk = chunk.to_vec();
            let view = self.view.clone();
            handles.push(tokio::spawn(async move {
                let db = view.as_graph_db_ref();
                let mut out = Vec::with_capacity(chunk.len());
                for sid in chunk {
                    let flakes = db
                        .range_with_opts(
                            IndexType::Spot,
                            RangeTest::Eq,
                            RangeMatch::subject(sid.clone()),
                            RangeOptions::default(),
                        )
                        .await
                        .map_err(|e| {
                            FormatError::InvalidBinding(format!("node property fetch failed: {e}"))
                        })?;
                    out.push((sid, flakes));
                }
                Ok::<_, FormatError>(out)
            }));
        }
        let mut fetched: Vec<(Sid, Vec<fluree_db_core::Flake>)> = Vec::new();
        for handle in handles {
            fetched.extend(handle.await.map_err(|e| {
                FormatError::InvalidBinding(format!("node property fetch task failed: {e}"))
            })??);
        }
        if self.enforcer.is_none() {
            for (sid, flakes) in fetched {
                self.flake_cache.insert(sid, Arc::new(flakes));
            }
            return Ok(());
        }

        // Policy runs over the whole batch (one class-cache populate),
        // then the flakes regroup by subject — including empty groups, so
        // fully-filtered subjects cache as property-less instead of
        // refetching on the row walk.
        let subjects: Vec<Sid> = fetched.iter().map(|(s, _)| s.clone()).collect();
        let all: Vec<fluree_db_core::Flake> =
            fetched.into_iter().flat_map(|(_, flakes)| flakes).collect();
        let visible = self.apply_view_policy(&subjects, all).await?;
        let mut grouped: HashMap<Sid, Vec<fluree_db_core::Flake>> =
            subjects.into_iter().map(|sid| (sid, Vec::new())).collect();
        for flake in visible {
            if let Some(group) = grouped.get_mut(&flake.s) {
                group.push(flake);
            }
        }
        for (sid, flakes) in grouped {
            self.flake_cache.insert(sid, Arc::new(flakes));
        }
        Ok(())
    }
    /// A subject's flakes, from the prefetch cache when warm; the fallback
    /// single fetch covers subjects the prefetch pass couldn't see
    /// (encoded bindings that materialized during the row walk).
    async fn subject_flakes(&mut self, sid: &Sid) -> Result<Arc<Vec<fluree_db_core::Flake>>> {
        if let Some(hit) = self.flake_cache.get(sid) {
            return Ok(Arc::clone(hit));
        }
        let flakes = self
            .view
            .as_graph_db_ref()
            .range_with_opts(
                IndexType::Spot,
                RangeTest::Eq,
                RangeMatch::subject(sid.clone()),
                RangeOptions::default(),
            )
            .await
            .map_err(|e| FormatError::InvalidBinding(format!("node property fetch failed: {e}")))?;
        let flakes = self
            .apply_view_policy(std::slice::from_ref(sid), flakes)
            .await?;
        let flakes = Arc::new(flakes);
        self.flake_cache.insert(sid.clone(), Arc::clone(&flakes));
        Ok(flakes)
    }

    /// The rendered cell for a top-level subject binding: a Relationship
    /// when the subject is an edge annotation (bound relationship vars
    /// resolve to the reifier subject via `coalesce(ann, …)`), a Node
    /// otherwise.
    async fn subject_cell(&mut self, sid: &Sid) -> Result<CypherCell> {
        if let Some(hit) = self.cell_cache.get(sid) {
            return Ok(hit.clone());
        }
        let flakes = self.subject_flakes(sid).await?;
        let mut start = None;
        let mut pred = None;
        let mut end = None;
        for flake in flakes.iter().filter(|f| f.op) {
            if Some(&flake.p) == self.reifies_subject.as_ref() {
                if let FlakeValue::Ref(s) = &flake.o {
                    start = Some(s.clone());
                }
            } else if Some(&flake.p) == self.reifies_predicate.as_ref() {
                if let FlakeValue::Ref(p) = &flake.o {
                    pred = Some(p.clone());
                }
            } else if Some(&flake.p) == self.reifies_object.as_ref() {
                if let FlakeValue::Ref(o) = &flake.o {
                    end = Some(o.clone());
                }
            }
        }
        let cell = match (start, pred, end) {
            // A reified node→node edge. (Literal-object annotations keep
            // the node rendering — Bolt relationships need node endpoints.)
            (Some(start), Some(pred), Some(end)) => {
                let start_iri = self.compactor.decode_sid_shared(&start)?;
                let end_iri = self.compactor.decode_sid_shared(&end)?;
                let type_iri = self.compactor.decode_sid(&pred)?;
                let properties = self.annotation_properties(sid).await?;
                CypherCell::Relationship(Box::new(CypherRelationship {
                    start_iri,
                    end_iri,
                    type_name: cypher_name_from_iri(&type_iri).into(),
                    reifier_iri: Some(self.compactor.decode_sid_shared(sid)?),
                    properties,
                }))
            }
            _ => CypherCell::Node(Box::new(self.node(sid).await?)),
        };
        self.cell_cache.insert(sid.clone(), cell.clone());
        Ok(cell)
    }

    async fn node(&mut self, sid: &Sid) -> Result<CypherNode> {
        if let Some(hit) = self.cache.get(sid) {
            return Ok(hit.clone());
        }
        let iri = self.compactor.decode_sid_shared(sid)?;
        let mut labels = Vec::new();
        let mut props: Vec<(Arc<str>, Vec<CypherCell>)> = Vec::new();
        let flakes = self.subject_flakes(sid).await?;
        for flake in flakes.iter() {
            if !flake.op {
                continue;
            }
            if Some(&flake.p) == self.rdf_type.as_ref() {
                if let FlakeValue::Ref(class_sid) = &flake.o {
                    if let Some(label) = self.label_name(class_sid)? {
                        labels.push(label);
                    }
                }
                continue;
            }
            // Ref-valued predicates are relationships, not node properties —
            // a Neo4j Node carries scalars only; edges surface by binding a
            // relationship or path var. Inlining them also made node emit
            // scale with out-degree: every target ref forces a dict/arena
            // IRI materialization, where literal objects are already inline
            // in the leaflet.
            if matches!(flake.o, FlakeValue::Ref(_)) {
                continue;
            }
            let Some(key) = self.key_name(&flake.p)? else {
                continue;
            };
            let cell = self.flake_value_cell(&flake.o)?;
            match props.iter_mut().find(|(k, _)| k.as_ref() == key.as_ref()) {
                Some((_, cells)) => cells.push(cell),
                None => props.push((key, vec![cell])),
            }
        }
        let node = CypherNode {
            iri,
            labels,
            properties: props
                .into_iter()
                .map(|(k, mut cells)| {
                    let cell = if cells.len() == 1 {
                        cells.pop().expect("one cell")
                    } else {
                        CypherCell::List(cells)
                    };
                    (k, cell)
                })
                .collect(),
        };
        self.cache.insert(sid.clone(), node.clone());
        Ok(node)
    }

    /// Properties of a reifier (annotation) subject: user annotation keys
    /// only — the `db:reifies*` bookkeeping and `rdf:type` are skipped.
    async fn annotation_properties(&mut self, sid: &Sid) -> Result<Vec<(Arc<str>, CypherCell)>> {
        let mut props: Vec<(Arc<str>, CypherCell)> = Vec::new();
        let flakes = self.subject_flakes(sid).await?;
        for flake in flakes.iter() {
            if !flake.op
                || Some(&flake.p) == self.rdf_type.as_ref()
                || matches!(flake.o, FlakeValue::Ref(_))
            {
                continue;
            }
            let Some(key) = self.key_name(&flake.p)? else {
                continue;
            };
            props.push((key, self.flake_value_cell(&flake.o)?));
        }
        Ok(props)
    }

    async fn path(&mut self, nodes: &[Sid], edges: &[(Sid, Sid, Sid)]) -> Result<CypherPath> {
        let mut path_nodes: Vec<CypherNode> = Vec::new();
        let mut node_index: HashMap<Arc<str>, usize> = HashMap::new();
        let mut index_of_node = |n: CypherNode, path_nodes: &mut Vec<CypherNode>| -> usize {
            if let Some(&i) = node_index.get(&n.iri) {
                return i;
            }
            let i = path_nodes.len();
            node_index.insert(n.iri.clone(), i);
            path_nodes.push(n);
            i
        };

        let mut rels: Vec<CypherRelationship> = Vec::new();
        let mut indices = Vec::new();
        if nodes.is_empty() {
            return Ok(CypherPath {
                nodes: path_nodes,
                rels,
                indices,
            });
        }
        let first = self.node(&nodes[0]).await?;
        index_of_node(first, &mut path_nodes);

        for (hop, (s, p, o)) in edges.iter().enumerate() {
            let Some(walk_from) = nodes.get(hop) else {
                break;
            };
            let forward = s == walk_from;
            let start_iri = self.compactor.decode_sid_shared(s)?;
            let end_iri = self.compactor.decode_sid_shared(o)?;
            let type_iri = self.compactor.decode_sid(p)?;
            let rel = CypherRelationship {
                start_iri,
                end_iri,
                type_name: cypher_name_from_iri(&type_iri).into(),
                reifier_iri: None,
                properties: Vec::new(),
            };
            let rel_pos = match rels.iter().position(|r| r == &rel) {
                Some(i) => i,
                None => {
                    rels.push(rel);
                    rels.len() - 1
                }
            };
            let rel_index = (rel_pos + 1) as i64;
            indices.push(if forward { rel_index } else { -rel_index });

            if let Some(next_sid) = nodes.get(hop + 1) {
                let next = self.node(next_sid).await?;
                let node_pos = index_of_node(next, &mut path_nodes);
                indices.push(node_pos as i64);
            }
        }
        Ok(CypherPath {
            nodes: path_nodes,
            rels,
            indices,
        })
    }

    fn flake_value_cell(&self, value: &FlakeValue) -> Result<CypherCell> {
        Ok(match value {
            FlakeValue::Ref(sid) => {
                CypherCell::Value(JsonValue::String(self.compactor.decode_sid(sid)?))
            }
            FlakeValue::String(s) => CypherCell::Value(JsonValue::String(s.to_string())),
            FlakeValue::Long(n) => CypherCell::Value(serde_json::json!(n)),
            FlakeValue::Double(d) => CypherCell::Value(if d.is_finite() {
                serde_json::json!(d)
            } else {
                JsonValue::String(d.to_string())
            }),
            FlakeValue::Boolean(b) => CypherCell::Value(serde_json::json!(b)),
            FlakeValue::Decimal(d) => CypherCell::Decimal(d.to_plain_string()),
            FlakeValue::BigInt(n) => CypherCell::BigInt(n.to_string()),
            FlakeValue::Date(d) => CypherCell::Temporal(date_cell(d)),
            FlakeValue::DateTime(dt) => CypherCell::Temporal(datetime_cell(dt)),
            FlakeValue::Time(t) => CypherCell::Temporal(time_cell(t)),
            FlakeValue::Json(s) => CypherCell::Value(
                serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string())),
            ),
            FlakeValue::Vector(v) => CypherCell::Value(JsonValue::Array(
                v.iter().map(|f| serde_json::json!(f)).collect(),
            )),
            FlakeValue::Null => CypherCell::Value(JsonValue::Null),
            other => CypherCell::Value(JsonValue::String(other.to_string())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_query::binding::RelValue;

    fn sid(name: &str) -> Sid {
        Sid::new(100, name)
    }

    #[test]
    fn collector_finds_every_hydration_subject() {
        let rel = Binding::Rel(Box::new(RelValue {
            start: sid("a"),
            predicate: sid("knows"),
            end: sid("b"),
            reifier: Some(sid("ann1")),
        }));
        let nested = Binding::List(vec![
            Binding::Sid {
                sid: sid("n1"),
                t: None,
                op: None,
            },
            Binding::Map(vec![(
                Arc::from("k"),
                Binding::IriMatch {
                    iri: Arc::from("http://x/n2"),
                    primary_sid: sid("n2"),
                    ledger_alias: Arc::from("l"),
                },
            )]),
        ]);
        let path = Binding::Path {
            nodes: vec![sid("p1"), sid("p2")],
            edges: vec![(sid("p1"), sid("knows"), sid("p2"))],
        };

        let mut out = Vec::new();
        collect_subject_sids(None, &rel, &mut out).unwrap();
        collect_subject_sids(None, &nested, &mut out).unwrap();
        collect_subject_sids(None, &path, &mut out).unwrap();

        let names: Vec<&str> = out.iter().map(|s| s.sid.name.as_ref()).collect();
        assert_eq!(names, vec!["ann1", "n1", "n2", "p1", "p2"]);
    }
}
