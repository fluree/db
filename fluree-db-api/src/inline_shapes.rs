//! Inline SHACL shapes parsing.
//!
//! Parses a JSON-LD document carrying SHACL shape definitions
//! (`sh:NodeShape`, `sh:targetClass`, `sh:property`, …) into a
//! `SchemaBundleFlakes` suitable for [`fluree_db_query::schema_bundle::SchemaBundleOverlay`].
//!
//! Reuses `FlakeSink` — the same direct-to-flakes sink that powers
//! Turtle insert. Encoding happens against the data ledger's
//! *staged* `NamespaceRegistry`, matching the term context used for
//! cross-ledger shapes. Allocating fresh ns codes for inline shape
//! vocabulary is acceptable: the user explicitly asked the engine
//! to honor those IRIs for this transaction, which is functionally
//! equivalent to staging the shapes directly.
//!
//! Inline shape flakes do *not* persist into the ledger — they live
//! only on the in-memory overlay attached to this transaction's
//! SHACL validation pass.

use fluree_db_core::Flake;
use fluree_db_query::schema_bundle::SchemaBundleFlakes;
use fluree_db_transact::flake_sink::FlakeSink;
use fluree_db_transact::namespace::NamespaceRegistry;
use fluree_db_transact::TransactError;
use fluree_graph_ir::GraphSink;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// Parse a JSON-LD inline shapes document into a
/// `SchemaBundleFlakes`. Encoding runs against `staged_ns`, which
/// the staging pipeline has already updated with any namespaces
/// the in-flight transaction introduced.
///
/// Returns `Ok(None)` for empty / no-triple documents so the caller
/// can skip overlay construction entirely. Returns an error if the
/// JSON-LD is malformed or contains unsupported constructs — the
/// transaction is rejected rather than silently dropping shapes.
pub(crate) fn parse_inline_shapes_to_bundle(
    shapes_json: &JsonValue,
    staged_ns: &mut NamespaceRegistry,
    t: i64,
    ledger_id: &str,
) -> Result<Option<Arc<SchemaBundleFlakes>>, TransactError> {
    // FlakeSink wants an owned txn_id for blank-node skolemization;
    // the inline-shapes context is transient so a synthetic id is
    // fine. Any blank-node shapes parsed here get unique sids.
    let txn_id = format!("inline-shapes-{ledger_id}-{t}");

    let mut sink = FlakeSink::new(staged_ns, t, txn_id);

    // Pre-register @context prefixes so IRIs the parser produces
    // align with the user's intended namespace boundaries.
    register_jsonld_prefixes(shapes_json, &mut sink);

    let expanded = fluree_graph_json_ld::expand(shapes_json)
        .map_err(|e| TransactError::Parse(format!("inline shapes JSON-LD expand error: {e}")))?;
    fluree_graph_json_ld::adapter::to_graph_events(&expanded, &mut sink).map_err(|e| {
        TransactError::Parse(format!("inline shapes JSON-LD event conversion error: {e}"))
    })?;

    let flakes: Vec<Flake> = sink.finish()?;
    if flakes.is_empty() {
        return Ok(None);
    }

    let bundle = SchemaBundleFlakes::from_collected_schema_triples(flakes).map_err(|e| {
        TransactError::Parse(format!("inline shapes bundle construction failed: {e}"))
    })?;
    Ok(Some(Arc::new(bundle)))
}

/// Register JSON-LD `@context` prefix mappings into the sink's
/// namespace allocator before `expand()` runs. Mirrors the helper
/// in `fluree-db-transact/src/import.rs` so the trie ends up with
/// the same prefix codes whether shapes arrive via inline-opts or
/// via a normal staged transaction.
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
