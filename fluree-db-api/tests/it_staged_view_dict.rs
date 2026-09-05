//! Staged-view dictionary coverage.
//!
//! A transaction's SHACL and post-state policy probes query a `StagedLedger`:
//! committed state plus the transaction's own staged flakes. Those probes run
//! on the binary lane once the ledger has a persisted index, and the lane
//! translates every overlay flake through the persisted dictionary plus
//! `DictNovelty`. The subjects and strings a transaction is *creating* are in
//! neither, so without a staged dictionary layer every such flake fails
//! translation ("subject not found in persisted or novelty dict"), is logged
//! at WARN, and is merged as a raw flake — once per probe, per new-subject
//! flake, with the whole graph novelty re-walked each time.
//!
//! These tests pin the translation outcome through a tracing probe rather
//! than timings, and pin post-commit reads so a product built over
//! uncommitted state can never be served for the committed state.
//!
//! Run with:
//!   cargo test -p fluree-db-api --test it_staged_view_dict --features shacl

#![cfg(all(feature = "native", feature = "shacl"))]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use fluree_db_api::TxnOpts;
use fluree_db_api::{CommitOpts, Fluree, FlureeBuilder, IndexConfig, QueryInput, ReindexOptions};
use serde_json::{json, Value};
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Layer;

/// Overlay-translation failures observed since the last drain, one entry per
/// WARN event, with the event's fields flattened for the assertion message.
static TRANSLATE_FAILURES: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Staging completions observed — proves the probe layer is the active
/// subscriber, so an empty failure list is a real observation.
static STAGING_SEEN: AtomicUsize = AtomicUsize::new(0);
/// The probe is process-global, so tests run one at a time to keep every
/// captured event attributable to the transaction under test.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serialize() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}

#[derive(Default)]
struct Flatten {
    text: String,
}

impl Visit for Flatten {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.text, "{}={:?} ", field.name(), value);
    }
}

struct ProbeLayer;

impl<S: tracing::Subscriber> Layer<S> for ProbeLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut flat = Flatten::default();
        event.record(&mut flat);
        if flat.text.contains("transaction staging completed") {
            STAGING_SEEN.fetch_add(1, Ordering::SeqCst);
        }
        let translate_failure = flat.text.contains("failed to translate overlay flake")
            || flat.text.contains("failed V3 translation")
            || flat.text.contains("not found in persisted or novelty dict");
        if translate_failure && *event.metadata().level() <= tracing::Level::WARN {
            TRANSLATE_FAILURES
                .lock()
                .expect("probe capture lock never poisoned")
                .push(flat.text);
        }
    }
}

fn install_probe() {
    let _ = tracing_subscriber::registry().with(ProbeLayer).try_init();
}

fn drain_failures() -> Vec<String> {
    std::mem::take(
        &mut *TRANSLATE_FAILURES
            .lock()
            .expect("probe capture lock never poisoned"),
    )
}

fn ctx() -> Value {
    json!({
        "ex": "http://example.org/ns/",
        "sh": "http://www.w3.org/ns/shacl#",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

/// Background indexing stays out of the way: the tests reindex explicitly.
fn quiet_index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 1 << 30,
        reindex_max_bytes: 1 << 31,
    }
}

fn person_shape() -> Value {
    json!({
        "@context": ctx(),
        "@graph": [
            {
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": {"@id": "ex:pshape_name"}
            },
            {
                "@id": "ex:pshape_name",
                "sh:path": {"@id": "ex:name"},
                "sh:minCount": 1,
                "sh:datatype": {"@id": "xsd:string"}
            }
        ]
    })
}

fn seed(n: usize) -> Value {
    let nodes: Vec<Value> = (0..n)
        .map(|i| {
            json!({
                "@id": format!("ex:person{i}"),
                "@type": "ex:Person",
                "ex:name": format!("Person {i}"),
                "ex:city": format!("City {}", i % 5)
            })
        })
        .collect();
    json!({"@context": ctx(), "@graph": nodes})
}

fn person(id: &str, name: Option<&str>) -> Value {
    let mut node = json!({
        "@context": ctx(),
        "@id": id,
        "@type": "ex:Person",
        "ex:city": "City new",
        "ex:knows": {"@id": "ex:person1"}
    });
    if let Some(name) = name {
        node["ex:name"] = json!(name);
    }
    node
}

async fn new_fluree() -> (Fluree, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("build");
    (fluree, dir)
}

/// Seed, reindex, and reload so the returned state carries a binary range
/// provider (the lane under test).
async fn indexed_ledger(fluree: &Fluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    fluree.insert(ledger, &seed(40)).await.expect("seed");
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    let ledger = fluree.ledger(ledger_id).await.expect("load");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "reindexed ledger must expose a binary range provider"
    );
    ledger
}

async fn name_of(fluree: &Fluree, ledger_id: &str, subject: &str) -> Vec<Value> {
    let view = fluree.db(ledger_id).await.expect("db view");
    let q = json!({
        "@context": ctx(),
        "select": ["?name"],
        "where": {"@id": subject, "ex:name": "?name"}
    });
    let out = fluree
        .query(&view, QueryInput::JsonLd(&q))
        .await
        .expect("query");
    out.to_jsonld(&view.snapshot)
        .expect("jsonld")
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// SHACL probes over the staged view must translate the transaction's own
/// new subjects instead of degrading to raw-flake merging with a WARN per
/// flake per probe.
#[tokio::test]
async fn shacl_over_staged_view_translates_new_subjects() {
    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/shacl:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    let opts = TxnOpts {
        shapes: Some(person_shape()),
        ..TxnOpts::default()
    };
    let _ = drain_failures();
    let staging_before = STAGING_SEEN.load(Ordering::SeqCst);

    fluree
        .insert_with_opts(
            ledger,
            &person("ex:newcomer", Some("Newcomer")),
            opts,
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect("valid Person under shape must be accepted");

    assert!(
        STAGING_SEEN.load(Ordering::SeqCst) > staging_before,
        "probe layer is not the active subscriber; the assertion below would be vacuous"
    );
    let failures = drain_failures();
    assert!(
        failures.is_empty(),
        "{} overlay flake(s) failed V3 translation during the staged SHACL pass; first: {}",
        failures.len(),
        failures.first().map(String::as_str).unwrap_or("")
    );

    // The staged dictionary is a view-local layer: the committed state must
    // read back through the canonical dictionaries.
    let rows = name_of(&fluree, LEDGER, "ex:newcomer").await;
    assert_eq!(rows, vec![json!(["Newcomer"])]);
}

/// The staged view must still *see* the new subject: a violating transaction
/// is rejected, proving the probes read the staged flakes rather than skipping
/// them.
#[tokio::test]
async fn shacl_over_staged_view_still_rejects_violations() {
    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/reject:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    let opts = TxnOpts {
        shapes: Some(person_shape()),
        ..TxnOpts::default()
    };
    let err = fluree
        .insert_with_opts(
            ledger,
            &person("ex:nameless", None),
            opts,
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect_err("Person without ex:name must be rejected by the shape");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation, got: {err:?}"
    );
}

/// Whole-graph scans after a commit must decode against the committed
/// dictionaries. A staged view reports the same overlay epoch and `to_t`
/// the committed novelty will report, so a translation product cached under
/// an epoch-only key during staging would be served for the committed state.
#[tokio::test]
async fn post_commit_whole_graph_scan_reads_committed_dictionaries() {
    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/post-commit:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    let opts = TxnOpts {
        shapes: Some(person_shape()),
        ..TxnOpts::default()
    };
    let batch = json!({
        "@context": ctx(),
        "@graph": [
            person("ex:alpha", Some("Alpha")),
            person("ex:beta", Some("Beta")),
            person("ex:gamma", Some("Gamma"))
        ]
    });
    fluree
        .insert_with_opts(
            ledger,
            &batch,
            opts,
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect("batch accepted");

    // Observe this scan only, not the staging pass above.
    let _ = drain_failures();
    let view = fluree.db(LEDGER).await.expect("db view");
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });
    let out = fluree
        .query(&view, QueryInput::JsonLd(&q))
        .await
        .expect("whole-graph scan");
    let rows = out.to_jsonld(&view.snapshot).expect("jsonld");
    let text = rows.to_string();
    for (id, name) in [
        ("ex:alpha", "Alpha"),
        ("ex:beta", "Beta"),
        ("ex:gamma", "Gamma"),
    ] {
        assert!(
            text.contains(&format!("\"{id}\"")) && text.contains(&format!("\"{name}\"")),
            "post-commit whole-graph scan lost {id}/{name}: {text}"
        );
    }
    let failures = drain_failures();
    assert!(
        failures.is_empty(),
        "post-commit scan hit translation failures; first: {}",
        failures.first().map(String::as_str).unwrap_or("")
    );
}

/// A staged preview (`GraphDb::from_staged`) is the third binary-lane reader
/// of uncommitted state: its scans must resolve the transaction's own
/// subjects through a staged dictionary layer rather than fall back to raw
/// merging with a WARN per flake per scan.
#[tokio::test]
async fn preview_over_staged_transaction_translates_new_subjects() {
    use fluree_db_api::GraphDb;

    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/preview:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    let staged = fluree
        .stage_owned(ledger)
        .insert(&person("ex:previewed", Some("Previewed")))
        .stage()
        .await
        .expect("stage");
    let preview = GraphDb::from_staged(&staged).expect("preview view");

    // Predicate-bound: the scan translates every staged `ex:name` flake (a
    // subject-bound pattern alone takes the overlay-only seek and never
    // translates).
    let _ = drain_failures();
    let q = json!({
        "@context": ctx(),
        "select": ["?s", "?name"],
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    let out = fluree
        .query(&preview, QueryInput::JsonLd(&q))
        .await
        .expect("preview query");
    let rows = out
        .to_jsonld(&preview.snapshot)
        .expect("jsonld")
        .to_string();
    assert!(
        rows.contains("\"ex:previewed\"") && rows.contains("\"Previewed\""),
        "preview must see the staged subject: {rows}"
    );
    let failures = drain_failures();
    assert!(
        failures.is_empty(),
        "{} overlay flake(s) failed V3 translation during the preview scan; first: {}",
        failures.len(),
        failures.first().map(String::as_str).unwrap_or("")
    );
}

/// The cross-query translation cache must never serve a product built over
/// uncommitted state for the committed state. A discarded preview's novelty
/// reports the very epoch and `to_t` the next commit will report, so an
/// epoch-keyed cache would hand that commit the preview's product: the
/// preview's values under the committed subject's freshly minted ids.
///
/// Integer objects are inline in a translated op (no dictionary), so the
/// aliased product decodes cleanly to the *wrong* value instead of failing.
#[tokio::test]
async fn post_commit_scan_is_never_served_a_discarded_previews_product() {
    use fluree_db_api::GraphDb;

    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/discarded-preview:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    let scan = json!({
        "@context": ctx(),
        "select": ["?s", "?p", "?o"],
        "where": {"@id": "?s", "?p": "?o"}
    });

    // Preview a transaction with an unbounded scan (a whole-graph product is
    // built and cached), then discard it.
    let discarded = json!({
        "@context": ctx(),
        "@id": "ex:ghost",
        "@type": "ex:Person",
        "ex:name": "Ghost",
        "ex:age": 1
    });
    let staged = fluree
        .stage_owned(ledger.clone())
        .insert(&discarded)
        .stage()
        .await
        .expect("stage");
    let preview = GraphDb::from_staged(&staged).expect("preview view");
    let out = fluree
        .query(&preview, QueryInput::JsonLd(&scan))
        .await
        .expect("preview scan");
    let rows = out
        .to_jsonld(&preview.snapshot)
        .expect("jsonld")
        .to_string();
    assert!(
        rows.contains("\"Ghost\""),
        "preview sees its own flakes: {rows}"
    );
    drop(preview);
    drop(staged);

    // Commit a different transaction at the same epoch and t. Indexing is
    // quiesced so the committed view keeps the preview's snapshot and store,
    // the state an epoch key cannot tell apart.
    let committed = json!({
        "@context": ctx(),
        "@id": "ex:real",
        "@type": "ex:Person",
        "ex:name": "Real",
        "ex:age": 2
    });
    let committed = fluree
        .insert_with_opts(
            ledger,
            &committed,
            TxnOpts::default(),
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect("commit")
        .ledger;

    // The same unbounded scan (same index order as the cached product). The
    // committed subject's and string's ids line up positionally with the
    // preview's, so an aliased product decodes to the committed subject
    // carrying the preview's age.
    let view = GraphDb::from_ledger_state(&committed);
    let out = fluree
        .query(&view, QueryInput::JsonLd(&scan))
        .await
        .expect("committed whole-graph scan");
    let rows = out.to_jsonld(&view.snapshot).expect("jsonld");
    let ages: Vec<Value> = rows
        .as_array()
        .expect("rows")
        .iter()
        .filter(|row| row[1] == json!("ex:age"))
        .map(|row| json!([&row[0], &row[2]]))
        .collect();
    assert_eq!(
        ages,
        vec![json!(["ex:real", 2])],
        "committed state must read its own flakes, not the discarded preview's"
    );
    assert!(
        !rows.to_string().contains("\"Ghost\""),
        "whole-graph scan after commit: {rows}"
    );
}

/// `f:queryState f:postState` policy conditions are the other binary-lane
/// reader of the staged view: the condition's ASK must resolve the subject
/// being created (and the reference it asserts) through the staged
/// dictionaries, not fall back to raw merging with a WARN per probe.
#[tokio::test]
async fn post_state_policy_over_staged_view_translates_new_subjects() {
    use fluree_db_api::{policy_builder, GovernanceOptions, TrackedTransactionInput, TxnType};
    use std::collections::HashMap;

    let _serial = serialize().await;
    install_probe();
    let (fluree, _dir) = new_fluree().await;
    const LEDGER: &str = "staged-view-dict/post-state:main";
    let ledger = indexed_ledger(&fluree, LEDGER).await;

    // "May create a Person only when it is owned by the caller and shares a
    // city with an existing Person" — the ex:owner edge exists only in the
    // staged flakes, and the predicate-bound neighbour pattern makes the
    // engine translate every staged `ex:city` flake (a subject-bound pattern
    // alone takes the overlay-only seek and never translates). The city is
    // one the indexed seed already holds: a join on a string the same
    // transaction introduces is denied on a staged view with or without a
    // staged dictionary layer, which is a separate gap (#1790).
    let policies = json!([
        {"@id": "ex:viewAll", "f:action": "f:view", "f:allow": true},
        {
            "@id": "ex:ownOnly",
            "f:onClass": [{"@id": "http://example.org/ns/Person"}],
            "f:action": "f:create",
            "f:queryState": {"@id": "f:postState"},
            "f:query": {
                "@type": "f:sparql",
                "@value": "ASK { $this <http://example.org/ns/owner> $identity . \
                           $this <http://example.org/ns/city> ?city . \
                           ?neighbour <http://example.org/ns/city> ?city }"
            }
        }
    ]);
    let opts = GovernanceOptions {
        policy: Some(policies),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/person1"}),
        )])),
        default_allow: Some(false),
        ..Default::default()
    };
    let policy_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("build policy context");

    let owned = json!({
        "@context": ctx(),
        "@id": "ex:owned",
        "@type": "ex:Person",
        "ex:name": "Owned",
        "ex:city": "City 1",
        "ex:owner": {"@id": "ex:person1"}
    });
    let _ = drain_failures();
    let staging_before = STAGING_SEEN.load(Ordering::SeqCst);
    let input =
        TrackedTransactionInput::new(TxnType::Insert, &owned, TxnOpts::default(), &policy_ctx);
    fluree
        .transact_tracked_with_policy(
            ledger.clone(),
            input,
            CommitOpts::default(),
            &quiet_index_cfg(),
        )
        .await
        .expect("post-state condition must see the staged ex:owner edge");
    assert!(
        STAGING_SEEN.load(Ordering::SeqCst) > staging_before,
        "probe layer is not the active subscriber; the assertion below would be vacuous"
    );
    let failures = drain_failures();
    assert!(
        failures.is_empty(),
        "{} overlay flake(s) failed V3 translation during the post-state policy pass; first: {}",
        failures.len(),
        failures.first().map(String::as_str).unwrap_or("")
    );

    // The condition still discriminates: no owner edge → denied.
    let unowned = json!({
        "@context": ctx(),
        "@id": "ex:unowned",
        "@type": "ex:Person",
        "ex:name": "Unowned"
    });
    let input =
        TrackedTransactionInput::new(TxnType::Insert, &unowned, TxnOpts::default(), &policy_ctx);
    let denied = fluree
        .transact_tracked_with_policy(ledger, input, CommitOpts::default(), &quiet_index_cfg())
        .await;
    assert!(denied.is_err(), "ownerless create must be denied");
}
