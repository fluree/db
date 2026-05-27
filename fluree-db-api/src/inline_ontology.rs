//! Inline ontology (RDFS / OWL) axioms parsing for queries.
//!
//! Parses a JSON-LD document carrying schema axioms
//! (rdfs:subClassOf, rdfs:subPropertyOf, owl:inverseOf,
//! owl:equivalentClass / equivalentProperty, owl:imports,
//! rdf:type for owl:Class / owl:TransitiveProperty / …) into a
//! [`SchemaBundleFlakes`] suitable for layering onto
//! `ReasoningConfig.schema_bundle` at query preparation time.
//!
//! Mirrors [`crate::inline_shapes::parse_inline_shapes_to_bundle`]
//! but runs against the live snapshot's namespace registry instead
//! of a staged-tx registry, because reasoning happens at query
//! time. The registry is cloned for the parse so any ns-code
//! allocations stay scoped to the request — the on-disk dictionary
//! is untouched.

use fluree_db_core::{Flake, LedgerSnapshot};
use fluree_db_query::schema_bundle::SchemaBundleFlakes;
use fluree_db_transact::flake_sink::FlakeSink;
use fluree_db_transact::namespace::NamespaceRegistry;
use fluree_graph_ir::GraphSink;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Parse a JSON-LD ontology document into a `SchemaBundleFlakes`.
///
/// Encoding runs against a `NamespaceRegistry` seeded from
/// `snapshot`. IRIs the snapshot already knows reuse the existing
/// codes (so reasoning joins line up); axioms naming previously
/// unseen IRIs allocate fresh in-request codes that are discarded
/// when the request returns.
///
/// Returns `Ok(None)` for a no-triple document so callers can
/// skip overlay construction. Parse / event errors return
/// [`ApiError::config`] — the inline ontology is admin-supplied
/// for *this query*, so silently dropping it is the wrong
/// failure mode.
pub(crate) fn parse_inline_ontology_to_bundle(
    ontology_json: &JsonValue,
    snapshot: &LedgerSnapshot,
) -> crate::Result<Option<Arc<SchemaBundleFlakes>>> {
    let mut ns_registry = NamespaceRegistry::from_db(snapshot);
    let txn_id = format!("inline-ontology-{}", snapshot.ledger_id);
    let mut sink = FlakeSink::new(&mut ns_registry, 0, txn_id);

    register_jsonld_prefixes(ontology_json, &mut sink);

    let expanded = fluree_graph_json_ld::expand(ontology_json).map_err(|e| {
        crate::ApiError::config(format!("inline ontology JSON-LD expand error: {e}"))
    })?;
    fluree_graph_json_ld::adapter::to_graph_events(&expanded, &mut sink).map_err(|e| {
        crate::ApiError::config(format!(
            "inline ontology JSON-LD event conversion error: {e}"
        ))
    })?;

    let flakes: Vec<Flake> = sink
        .finish()
        .map_err(|e| crate::ApiError::config(format!("inline ontology flake build failed: {e}")))?;
    if flakes.is_empty() {
        return Ok(None);
    }
    let bundle = SchemaBundleFlakes::from_collected_schema_triples(flakes).map_err(|e| {
        crate::ApiError::config(format!("inline ontology bundle construction failed: {e}"))
    })?;
    Ok(Some(Arc::new(bundle)))
}

/// Build a single `SchemaBundleFlakes` by concatenating the flakes
/// from two bundles. `from_collected_schema_triples` dedupes,
/// sorts each index ordering, and recomputes the epoch — so an
/// inline ontology that repeats axioms already in the configured
/// bundle costs nothing extra at reasoning time.
pub(crate) fn merge_bundles(
    a: Arc<SchemaBundleFlakes>,
    b: Arc<SchemaBundleFlakes>,
) -> crate::Result<Arc<SchemaBundleFlakes>> {
    let mut combined = a.flakes_for_merge();
    combined.extend(b.flakes_for_merge());
    let bundle = SchemaBundleFlakes::from_collected_schema_triples(combined).map_err(|e| {
        crate::ApiError::config(format!("inline ontology bundle merge failed: {e}"))
    })?;
    Ok(Arc::new(bundle))
}

/// Register JSON-LD `@context` prefix mappings into the sink's
/// namespace allocator before `expand()` runs. Same shape as the
/// helper in [`crate::inline_shapes`]; kept local so each inline
/// surface stays self-contained.
fn register_jsonld_prefixes(doc: &JsonValue, sink: &mut FlakeSink<'_>) {
    fn visit_context(ctx: &JsonValue, sink: &mut FlakeSink<'_>) {
        match ctx {
            JsonValue::Object(obj) => {
                for (key, val) in obj {
                    if key.starts_with('@') {
                        continue;
                    }
                    if let Some(iri) = val.as_str() {
                        sink.on_prefix(key, iri);
                    } else if let Some(obj_val) = val.as_object() {
                        if let Some(id) = obj_val.get("@id").and_then(|v| v.as_str()) {
                            sink.on_prefix(key, id);
                        }
                    }
                }
            }
            JsonValue::Array(arr) => {
                for item in arr {
                    visit_context(item, sink);
                }
            }
            _ => {}
        }
    }

    if let Some(ctx) = doc.get("@context") {
        visit_context(ctx, sink);
    }
}
