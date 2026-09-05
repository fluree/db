//! Overlay content versions as cross-query cache keys.
//!
//! Two process-global caches key on `OverlayProvider::content_version`: the
//! overlay→V3 translation cache the binary scan lane shares across query
//! executions, and the OWL2-RL materialization cache. Both used to key on the
//! overlay *epoch*, which is only unique within one overlay's lineage: a
//! composed overlay (reasoning over novelty) that cannot vouch for a content
//! version is silently never cached, and a staged preview's novelty reports
//! the very epoch the committed novelty reports once those flakes commit.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_overlay_content_version

#![cfg(feature = "native")]

use std::sync::Mutex;

use fluree_db_api::{
    CommitOpts, Fluree, FlureeBuilder, GraphDb, IndexConfig, QueryInput, ReindexOptions, TxnOpts,
};
use serde_json::{json, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// `cache_hit` values recorded on `overlay_translate` spans since the last
/// drain, in recording order.
static TRANSLATE_HITS: Mutex<Vec<bool>> = Mutex::new(Vec::new());
/// The probe is process-global, so tests run one at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct CacheHit(Option<bool>);

impl Visit for CacheHit {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == "cache_hit" {
            self.0 = Some(value);
        }
    }
    fn record_debug(&mut self, _: &Field, _: &dyn std::fmt::Debug) {}
}

struct TranslateProbe;

impl<S> Layer<S> for TranslateProbe
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &Id, _ctx: Context<'_, S>) {
        if attrs.metadata().name() != "overlay_translate" {
            return;
        }
        let mut visit = CacheHit(None);
        attrs.record(&mut visit);
        if let Some(hit) = visit.0 {
            TRANSLATE_HITS.lock().expect("probe lock").push(hit);
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.name() != "overlay_translate" {
            return;
        }
        let mut visit = CacheHit(None);
        values.record(&mut visit);
        if let Some(hit) = visit.0 {
            TRANSLATE_HITS.lock().expect("probe lock").push(hit);
        }
    }
}

fn install_probe() {
    let _ = tracing_subscriber::registry()
        .with(TranslateProbe)
        .try_init();
}

fn drain_hits() -> Vec<bool> {
    std::mem::take(&mut *TRANSLATE_HITS.lock().expect("probe lock"))
}

fn ctx() -> Value {
    json!({
        "ex": "http://example.org/ns/",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    })
}

/// Background indexing stays out of the way: a reindex would swap the
/// snapshot and store under a view mid-test.
fn quiet_index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 30,
        reindex_max_bytes: 1 << 31,
    }
}

async fn quiet_insert(
    fluree: &Fluree,
    ledger: fluree_db_api::LedgerState,
    data: &Value,
) -> fluree_db_api::LedgerState {
    fluree
        .insert_with_opts(
            ledger,
            data,
            TxnOpts::default(),
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect("commit")
        .ledger
}

async fn new_fluree() -> (Fluree, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    (fluree, dir)
}

/// An ontology (`ex:Dog ⊑ ex:Animal`) plus `n` dogs, indexed and reloaded so
/// the returned state carries a binary range provider, then one more dog
/// committed on top so the novelty is non-empty.
async fn indexed_ledger(fluree: &Fluree, ledger_id: &str, n: usize) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    let mut nodes = vec![json!({
        "@id": "ex:Dog",
        "rdfs:subClassOf": {"@id": "ex:Animal"}
    })];
    nodes.extend((0..n).map(|i| json!({"@id": format!("ex:dog{i}"), "@type": "ex:Dog"})));
    let seed = json!({"@context": ctx(), "@graph": nodes});
    fluree.insert(ledger, &seed).await.expect("seed");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert!(ledger.snapshot.range_provider.is_some());
    let extra = json!({"@context": ctx(), "@id": "ex:fido", "@type": "ex:Dog"});
    quiet_insert(fluree, ledger, &extra).await
}

fn animals_query() -> Value {
    json!({
        "@context": ctx(),
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Animal"},
        "reasoning": "owl2rl"
    })
}

async fn animals(fluree: &Fluree, view: &GraphDb) -> Vec<String> {
    let q = animals_query();
    let out = fluree
        .query(view, QueryInput::JsonLd(&q))
        .await
        .expect("reasoning query");
    let rows = out.to_jsonld(&view.snapshot).expect("jsonld");
    let mut names: Vec<String> = rows
        .as_array()
        .expect("rows")
        .iter()
        .map(|v| v.as_str().expect("iri").to_string())
        .collect();
    names.sort();
    names
}

/// A reasoning query's overlay is a `ReasoningOverlay` over the novelty. It
/// must vouch for a content version, or the whole-overlay translation is
/// rebuilt on every execution — the multi-second floor the cross-query
/// translation cache exists to remove for large materializations.
#[tokio::test]
async fn reasoning_query_reuses_overlay_translation_across_executions() {
    let _serial = SERIAL.lock().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "overlay-content-version/reuse:main";
    let ledger = indexed_ledger(&fluree, LEDGER, 4).await;
    let view = GraphDb::from_ledger_state(&ledger);

    // Unbounded: the whole-graph product is built and cached.
    let scan = json!({
        "@context": ctx(),
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"},
        "reasoning": "owl2rl"
    });

    let _ = drain_hits();
    let out = fluree
        .query(&view, QueryInput::JsonLd(&scan))
        .await
        .expect("first execution");
    let rows = out.to_jsonld(&view.snapshot).expect("jsonld").to_string();
    assert!(
        rows.contains("\"ex:Animal\""),
        "derived facts visible: {rows}"
    );
    let cold = drain_hits();
    assert!(
        cold.contains(&false),
        "first execution must translate the overlay (probe saw {cold:?})"
    );

    let out = fluree
        .query(&view, QueryInput::JsonLd(&scan))
        .await
        .expect("second execution");
    let again = out.to_jsonld(&view.snapshot).expect("jsonld").to_string();
    assert_eq!(rows, again);
    let warm = drain_hits();
    assert!(
        warm.contains(&true) && !warm.contains(&false),
        "second execution must be served from the cross-query translation cache (probe saw {warm:?})"
    );
}

/// The OWL2-RL materialization cache must never serve a preview's derived
/// facts for the committed state. A discarded preview's novelty reports the
/// epoch and `t` the next commit will report; keyed on the epoch, the
/// committed state would inherit the preview's entailments and miss its own.
#[tokio::test]
async fn reasoning_cache_never_serves_a_discarded_preview() {
    let _serial = SERIAL.lock().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "overlay-content-version/preview:main";
    let ledger = indexed_ledger(&fluree, LEDGER, 1).await;

    let ghost = json!({"@context": ctx(), "@id": "ex:ghost", "@type": "ex:Dog"});
    let staged = fluree
        .stage_owned(ledger.clone())
        .insert(&ghost)
        .stage()
        .await
        .expect("stage");
    let preview = GraphDb::from_staged(&staged).expect("preview view");
    let previewed = animals(&fluree, &preview).await;
    assert!(
        previewed.iter().any(|s| s == "ex:ghost"),
        "preview derives the staged dog's type: {previewed:?}"
    );
    drop(preview);
    drop(staged);

    let real = json!({"@context": ctx(), "@id": "ex:real", "@type": "ex:Dog"});
    let committed = quiet_insert(&fluree, ledger, &real).await;

    let view = GraphDb::from_ledger_state(&committed);
    let committed = animals(&fluree, &view).await;
    assert!(
        committed.iter().any(|s| s == "ex:real"),
        "committed state derives its own dog's type: {committed:?}"
    );
    assert!(
        !committed.iter().any(|s| s == "ex:ghost"),
        "committed state must not inherit the discarded preview's entailments: {committed:?}"
    );
}
