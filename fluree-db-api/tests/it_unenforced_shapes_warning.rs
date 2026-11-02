//! A ledger holding shapes that no config enforces says so when it loads.
//!
//! Shapes are enforced only where the ledger config sets `f:shaclEnabled
//! true`, so such a ledger admits every write without a word. Ledger info
//! reports it on demand (`configDiagnostics`); this pins the warning logged
//! once per ledger load, and only for that state.
//!
//! Its own `[[test]]` binary predates the shared capture helper; the capture
//! goes through `fluree_test_support::scoped_default`, so a sibling test can't
//! leave the callsite disabled. See `docs/contributing/tests.md`.

mod support;

use crate::support::span_capture;
use fluree_db_api::FlureeBuilder;

const WARNING: &str = "shapes present; SHACL enforcement not configured";

const SHAPE: &str =
    "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> .
    ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
      sh:property [ sh:path ex:name ; sh:minCount 1 ] .";

/// An untyped shape: a target predicate and no `sh:NodeShape` type.
const UNTYPED_SHAPE: &str =
    "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example.org/> .
    ex:PersonShape sh:targetClass ex:Person ;
      sh:property [ sh:path ex:name ; sh:minCount 1 ] .";

#[tokio::test(flavor = "current_thread")]
async fn a_ledger_with_unenforced_shapes_warns_once_per_load() {
    let (spans, _guard) = span_capture::init_test_tracing();
    let warnings = || {
        spans
            .all_events()
            .iter()
            .filter(|e| e.level == tracing::Level::WARN && e.message().contains(WARNING))
            .count()
    };

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    {
        let fluree = FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .expect("build");
        let seed = |name: &'static str, trig: String| {
            let fluree = &fluree;
            async move {
                fluree.create_ledger(name).await.expect("create");
                fluree
                    .graph(&format!("{name}:main"))
                    .transact()
                    .upsert_turtle(&trig)
                    .commit()
                    .await
                    .expect("seed");
            }
        };
        seed("it/shapes-unenforced", SHAPE.to_string()).await;
        // The same state with the shape in the index rather than in novelty.
        seed("it/shapes-unenforced-indexed", SHAPE.to_string()).await;
        fluree
            .reindex(
                "it/shapes-unenforced-indexed:main",
                fluree_db_api::ReindexOptions::default(),
            )
            .await
            .expect("reindex");
        seed(
            "it/shapes-enforced",
            format!(
                "{SHAPE}
                 @prefix f: <https://ns.flur.ee/db#> .
                 GRAPH <urn:fluree:it/shapes-enforced:main#config> {{
                   <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
                   <urn:config:shacl> f:shaclEnabled true .
                 }}"
            ),
        )
        .await;
        seed("it/untyped-unenforced", UNTYPED_SHAPE.to_string()).await;
        seed("it/untyped-unenforced-indexed", UNTYPED_SHAPE.to_string()).await;
        fluree
            .reindex(
                "it/untyped-unenforced-indexed:main",
                fluree_db_api::ReindexOptions::default(),
            )
            .await
            .expect("reindex");
        seed(
            "it/shapes-switched-off",
            format!(
                "{SHAPE}
                 @prefix f: <https://ns.flur.ee/db#> .
                 GRAPH <urn:fluree:it/shapes-switched-off:main#config> {{
                   <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
                   <urn:config:shacl> f:shaclEnabled false .
                 }}"
            ),
        )
        .await;
        seed(
            "it/no-shapes",
            "@prefix ex: <http://example.org/> . ex:alice ex:name \"Alice\" .".to_string(),
        )
        .await;
    }
    let before = warnings();

    let fluree = FlureeBuilder::file(path)
        .without_indexing()
        .build()
        .expect("build");
    let unindexed = fluree
        .ledger_cached("it/shapes-unenforced:main")
        .await
        .expect("load");
    assert_eq!(
        unindexed.snapshot().await.snapshot.t,
        0,
        "harness: the shape must be in novelty"
    );
    assert_eq!(warnings() - before, 1, "warned when the ledger loaded");
    fluree
        .ledger_cached("it/shapes-unenforced:main")
        .await
        .expect("cached");
    assert_eq!(warnings() - before, 1, "not again while it stays loaded");
    let indexed = fluree
        .ledger_cached("it/shapes-unenforced-indexed:main")
        .await
        .expect("load");
    assert!(
        indexed.snapshot().await.snapshot.t > 0,
        "harness: the shape must be in the index"
    );
    assert_eq!(warnings() - before, 2, "warned for the indexed ledger too");

    // An untyped shape warns as a typed one does, in novelty and indexed.
    fluree
        .ledger_cached("it/untyped-unenforced:main")
        .await
        .expect("load");
    assert_eq!(warnings() - before, 3, "warned for the untyped shape");
    fluree
        .ledger_cached("it/untyped-unenforced-indexed:main")
        .await
        .expect("load");
    assert_eq!(
        warnings() - before,
        4,
        "warned for the indexed untyped shape"
    );

    for ledger in [
        "it/shapes-enforced:main",
        "it/shapes-switched-off:main",
        "it/no-shapes:main",
    ] {
        fluree.ledger_cached(ledger).await.expect("load");
        assert_eq!(warnings() - before, 4, "{ledger} has nothing to warn about");
    }
}
