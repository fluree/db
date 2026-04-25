//! Integration tests for `f:schemaSource` + `owl:imports` closure support.
//!
//! Covers the v1 behavior of `fluree_db_api::ontology_imports` composed with
//! `fluree_db_query::schema_bundle::SchemaBundleOverlay`:
//!
//! - `f:schemaSource` pointing at a local named graph is honored.
//! - `owl:imports` is resolved transitively from same-ledger graphs.
//! - `f:ontologyImportMap` provides a mapping fallback.
//! - Unresolved imports fail the query (strict semantics).
//! - Cycles in the import graph don't loop forever.
//! - The schema-projection whitelist keeps instance data from leaking.

mod support;

use fluree_db_api::{ApiError, FlureeBuilder};
use serde_json::json;
use support::genesis_ledger;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Stage a TriG document and return the resulting ledger.
async fn apply_trig(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    trig: &str,
) -> fluree_db_api::LedgerState {
    fluree
        .stage_owned(ledger)
        .upsert_turtle(trig)
        .execute()
        .await
        .expect("trig stage should succeed")
        .ledger
}

// ============================================================================
// 1. Same-ledger auto resolution of a named-graph schema source
// ============================================================================

/// When `f:schemaSource` points at a named graph (not the default graph) and
/// reasoning is enabled, `rdfs:subClassOf` assertions in that named graph are
/// visible to the RDFS hierarchy expansion.
#[tokio::test]
async fn schema_source_in_named_graph_expands_subclass_queries() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-basic:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Put instance data (ex:alice typed as ex:Employee) in the DEFAULT graph.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:alice rdf:type ex:Employee .
        ",
    )
    .await;

    // Put `ex:Employee rdfs:subClassOf ex:Person` in a NAMED graph.
    // Also write config pointing `f:schemaSource` at that graph.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/ontology/core> {{
            ex:Employee rdfs:subClassOf ex:Person .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/core> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    // Query for all ex:Person — should include ex:alice via subclass reasoning.
    let q = json!({
        "@context": {
            "ex": "http://example.org/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
        },
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });

    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:alice")),
        "ex:alice should appear as a Person via rdfs:subClassOf in the schema-source graph; got {normalized:?}"
    );
}

// ============================================================================
// 2. Transitive owl:imports: A -> B
// ============================================================================

/// `schemaSource -> A`, `A owl:imports B`, followOwlImports=true.
/// A subclass edge that lives in B must be visible to reasoning on the default
/// graph (instance data stays in the default graph).
#[tokio::test]
async fn transitive_owl_imports_are_followed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-transitive:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Instance data in the default graph.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:alice rdf:type ex:Manager .
        ",
    )
    .await;

    // A imports B; B holds the subclass edges.
    //   A:  ex:Manager rdfs:subClassOf ex:Employee (+ owl:imports <B>)
    //   B:  ex:Employee rdfs:subClassOf ex:Person
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/ontology/A> {{
            <http://example.org/ontology/A> rdf:type owl:Ontology ;
                                            owl:imports <http://example.org/ontology/B> .
            ex:Manager rdfs:subClassOf ex:Employee .
        }}

        GRAPH <http://example.org/ontology/B> {{
            ex:Employee rdfs:subClassOf ex:Person .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> ;
                                   f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/A> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    // With the transitive closure, Manager ⊑ Employee ⊑ Person, so alice is a Person.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });

    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:alice")),
        "alice should be a Person via Manager ⊑ Employee ⊑ Person; got {normalized:?}"
    );
}

// ============================================================================
// 3. Mapped external import via f:ontologyImportMap
// ============================================================================

/// Import IRI doesn't match any local graph, but `f:ontologyImportMap` binds
/// it to a local graph — the closure is resolved via the mapping.
#[tokio::test]
async fn ontology_import_map_resolves_external_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-mapped:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:alice rdf:type ex:Staff .
        ",
    )
    .await;

    // Main ontology graph imports <http://upstream.example/bfo> which does
    // NOT exist as a local named graph; the config provides a mapping to
    // <http://example.org/local/bfo>.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/ontology/main> {{
            <http://example.org/ontology/main>
                rdf:type owl:Ontology ;
                owl:imports <http://upstream.example/bfo> .
        }}

        GRAPH <http://example.org/local/bfo> {{
            ex:Staff rdfs:subClassOf ex:Person .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning>
                f:schemaSource <urn:config:schema-ref> ;
                f:followOwlImports true ;
                f:ontologyImportMap <urn:config:bfo-binding> .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/main> .
            <urn:config:bfo-binding>
                f:ontologyIri <http://upstream.example/bfo> ;
                f:graphRef <urn:config:bfo-ref> .
            <urn:config:bfo-ref> rdf:type f:GraphRef ;
                                 f:graphSource <urn:config:bfo-source> .
            <urn:config:bfo-source> f:graphSelector <http://example.org/local/bfo> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:alice")),
        "mapped external import should expose ex:Staff ⊑ ex:Person; got {normalized:?}"
    );
}

// ============================================================================
// 4. Unresolved owl:imports -> strict error
// ============================================================================

#[tokio::test]
async fn unresolved_owl_import_errors() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-unresolved:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/ontology/main> {{
            <http://example.org/ontology/main>
                rdf:type owl:Ontology ;
                owl:imports <http://unknown.example/missing> .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> ;
                                   f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/main> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let err = fluree
        .query(&view, &q)
        .await
        .expect_err("unresolved import must fail the query");
    match err {
        ApiError::OntologyImport(msg) => {
            assert!(msg.contains("missing"), "error should name the IRI: {msg}");
        }
        other => panic!("expected OntologyImport, got {other:?}"),
    }
}

// ============================================================================
// 5. Cycles terminate
// ============================================================================

#[tokio::test]
async fn owl_imports_cycle_terminates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-cycle:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:alice rdf:type ex:Employee .
        ",
    )
    .await;

    // A imports B; B imports A (cycle). Subclass edge lives in B.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/ontology/A> {{
            <http://example.org/ontology/A>
                rdf:type owl:Ontology ;
                owl:imports <http://example.org/ontology/B> .
        }}

        GRAPH <http://example.org/ontology/B> {{
            <http://example.org/ontology/B>
                rdf:type owl:Ontology ;
                owl:imports <http://example.org/ontology/A> .
            ex:Employee rdfs:subClassOf ex:Person .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> ;
                                   f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/A> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    // Must complete — no infinite loop.
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:alice")),
        "cycle should not prevent closure; got {normalized:?}"
    );
}

// ============================================================================
// 6. Whitelist enforcement: instance data in a schema-source graph doesn't leak
// ============================================================================

/// The bundle overlay projects only schema-whitelisted predicates to `g_id=0`.
/// Instance triples that happen to live in the schema graph must not appear
/// as default-graph results.
#[tokio::test]
async fn instance_data_in_schema_graph_does_not_leak() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-whitelist:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Put ex:real in the default graph; put ex:leaked in the schema graph
    // (as instance data, not a schema axiom).
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        ex:real rdf:type ex:Person .
        ex:real ex:name "Real Person" .

        GRAPH <http://example.org/ontology/core> {{
            # A legitimate schema axiom — should be projected.
            ex:Employee rdfs:subClassOf ex:Person .
            # Instance data that lives in the schema graph —
            # MUST NOT surface in default-graph queries.
            ex:leaked rdf:type ex:Person .
            ex:leaked ex:name "Leaked Instance" .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/core> .
        }}
        "#
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:real")),
        "ex:real from the default graph should match; got {normalized:?}"
    );
    assert!(
        !normalized.contains(&json!("ex:leaked")),
        "ex:leaked lives in the schema graph and must NOT appear in default-graph results; got {normalized:?}"
    );
}

// ============================================================================
// 7. System-graph guard covers the `f:ontologyImportMap` path
// ============================================================================

/// A mapping entry that points `owl:imports <...>` at the ledger's txn-meta
/// graph must be rejected. Otherwise the mapping path would bypass the
/// system-graph guard that covers the direct graph-IRI resolution.
#[tokio::test]
async fn mapping_table_cannot_target_system_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-system-map:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Main ontology imports an external IRI that the mapping table then
    // aims at the txn-meta system graph (via `f:txnMetaGraph` sentinel).
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/ontology/main> {{
            <http://example.org/ontology/main>
                rdf:type owl:Ontology ;
                owl:imports <http://upstream.example/evil> .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning>
                f:schemaSource <urn:config:schema-ref> ;
                f:followOwlImports true ;
                f:ontologyImportMap <urn:config:evil-binding> .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/main> .
            <urn:config:evil-binding>
                f:ontologyIri <http://upstream.example/evil> ;
                f:graphRef <urn:config:evil-ref> .
            <urn:config:evil-ref> rdf:type f:GraphRef ;
                                  f:graphSource <urn:config:evil-source> .
            <urn:config:evil-source> f:graphSelector f:txnMetaGraph .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Thing"},
        "reasoning": "rdfs"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let err = fluree
        .query(&view, &q)
        .await
        .expect_err("mapping to a system graph must be rejected");
    match err {
        ApiError::OntologyImport(msg) => {
            assert!(
                msg.contains("reserved system graph"),
                "error should identify the reserved-graph rule: {msg}"
            );
        }
        other => panic!("expected OntologyImport, got {other:?}"),
    }
}

// ============================================================================
// 8. reasoning=none skips bundle resolution (no regression on broken imports)
// ============================================================================

/// A query with `"reasoning": "none"` must not fail because of an unresolved
/// `owl:imports` in the ledger's reasoning config. The bundle is a
/// reasoning-only concern; non-reasoning queries short-circuit before
/// touching it.
#[tokio::test]
async fn reasoning_none_skips_bundle_resolution() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-disabled:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed one fact in the default graph.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r#"
        @prefix ex: <http://example.org/> .
        ex:alice ex:name "Alice" .
        "#,
    )
    .await;

    // Configure a broken `owl:imports` chain. With reasoning ON, this would
    // error (covered by `unresolved_owl_import_errors`).
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/ontology/main> {{
            <http://example.org/ontology/main>
                rdf:type owl:Ontology ;
                owl:imports <http://unknown.example/missing> .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> ;
                                   f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/main> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    // Explicit reasoning=none — query must succeed and return ex:alice,
    // even though the config-graph import is broken.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "ex:name": "Alice"},
        "reasoning": "none"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .expect("reasoning=none must short-circuit past the broken import")
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);
    assert!(
        normalized.contains(&json!("ex:alice")),
        "query with reasoning=none should return normal results; got {normalized:?}"
    );
}

// ============================================================================
// 9. Deferred `GraphSourceRef` fields are rejected, not silently ignored
// ============================================================================

/// A schema source that pins `f:atT` is rejected: v1 resolves the entire
/// closure at the query's `to_t`, so honoring a per-source `at_t` would
/// require bookkeeping that isn't there yet.
#[tokio::test]
async fn schema_source_at_t_rejected() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-at-t:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/ontology/core> {{
            ex:Employee rdfs:subClassOf ex:Person .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:schemaSource <urn:config:schema-ref> .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source>
                f:graphSelector <http://example.org/ontology/core> ;
                f:atT 1 .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Person"},
        "reasoning": "rdfs"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let err = fluree
        .query(&view, &q)
        .await
        .expect_err("at_t on schema source must be rejected in v1");
    match err {
        ApiError::OntologyImport(msg) => {
            assert!(
                msg.contains("atT") || msg.contains("at_t"),
                "error should name the unsupported field: {msg}"
            );
        }
        other => panic!("expected OntologyImport, got {other:?}"),
    }
}

// ============================================================================
// 10. End-to-end: OWL2-RL rules declared in a transitive import actually fire
// ============================================================================
//
// These tests are the real proof of the feature. The earlier tests verify that
// a schema-predicate flake (rdfs:subClassOf) can traverse the projection path
// and influence hierarchy lookups. These go further:
//
//   1. The axiom lives in an imported graph B (reached via `A owl:imports B`).
//   2. Instance data lives in the default graph.
//   3. Reasoning is OWL2-RL, which requires the reasoner to scan for the
//      ontology axiom, materialize derived facts against instance data, and
//      make them visible to the query.
//
// If the projection layer only carried RDFS edges to `schema_hierarchy_with_overlay`
// but failed to expose OWL axioms (`rdf:type owl:TransitiveProperty`,
// `owl:inverseOf`, `rdfs:domain`) to `reason_owl2rl`, these would return
// empty bindings.

/// Case A: `owl:TransitiveProperty` declared in an imported graph B.
/// Instance chain `alice → bob → carol` in the default graph.
/// Query `alice hasAncestor ?x` with OWL2-RL must include `carol`.
#[tokio::test]
async fn owl2rl_transitive_property_axiom_from_transitive_import() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-owl2rl-trans:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Instance data in the default graph; ancestry chain of three.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        ex:alice ex:hasAncestor ex:bob .
        ex:bob   ex:hasAncestor ex:carol .
        ",
    )
    .await;

    // A imports B; B declares the transitive-property axiom.
    //   A: (no axioms, pure imports hub)
    //   B: ex:hasAncestor rdf:type owl:TransitiveProperty
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix owl:  <http://www.w3.org/2002/07/owl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/> .

        GRAPH <http://example.org/ontology/A> {{
            <http://example.org/ontology/A>
                rdf:type owl:Ontology ;
                owl:imports <http://example.org/ontology/B> .
        }}

        GRAPH <http://example.org/ontology/B> {{
            ex:hasAncestor rdf:type owl:TransitiveProperty .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning>
                f:schemaSource <urn:config:schema-ref> ;
                f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/A> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?x",
        "where": {"@id": "ex:alice", "ex:hasAncestor": "?x"},
        "reasoning": "owl2rl"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);

    // Base fact: alice hasAncestor bob.
    assert!(
        normalized.contains(&json!("ex:bob")),
        "alice hasAncestor bob (base); got {normalized:?}"
    );
    // Entailment: alice hasAncestor carol via transitive closure — ONLY
    // visible if the TransitiveProperty axiom from graph B reached the
    // reasoner at g_id=0.
    assert!(
        normalized.contains(&json!("ex:carol")),
        "alice hasAncestor carol should be entailed by owl:TransitiveProperty \
         declared in imported graph B; got {normalized:?}"
    );
}

/// Case B: `owl:inverseOf` declared in an imported graph B, query asks for
/// the inverse direction. Without the projection, the reasoner wouldn't see
/// the inverse axiom and the inverse query would return empty.
#[tokio::test]
async fn owl2rl_inverse_of_axiom_from_transitive_import() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-owl2rl-inverse:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Instance data: alice is bob's parent.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r"
        @prefix ex: <http://example.org/> .
        ex:alice ex:parentOf ex:bob .
        ",
    )
    .await;

    // A imports B; B declares parentOf owl:inverseOf childOf.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix owl:  <http://www.w3.org/2002/07/owl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:   <http://example.org/> .

        GRAPH <http://example.org/ontology/A> {{
            <http://example.org/ontology/A>
                rdf:type owl:Ontology ;
                owl:imports <http://example.org/ontology/B> .
        }}

        GRAPH <http://example.org/ontology/B> {{
            ex:parentOf owl:inverseOf ex:childOf .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning>
                f:schemaSource <urn:config:schema-ref> ;
                f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/A> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    // Query the INVERSE direction: who is alice a child of?
    //   No `childOf` triple in the data.
    //   Expect `ex:alice` via owl:inverseOf(parentOf, childOf) applied to
    //   `alice parentOf bob`  →  `bob childOf alice`.
    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?child",
        "where": {"@id": "?child", "ex:childOf": {"@id": "ex:alice"}},
        "reasoning": "owl2rl"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);

    assert!(
        normalized.contains(&json!("ex:bob")),
        "owl:inverseOf in the imported graph should entail `bob childOf alice`; \
         got {normalized:?}"
    );
}

/// Case C: `rdfs:domain` declared in an imported graph produces a class
/// typing for instance data in the default graph. Uses auto-RDFS rather
/// than an explicit mode to also verify that hierarchy availability
/// detection sees the imported schema.
#[tokio::test]
async fn owl2rl_domain_axiom_from_transitive_import_types_instance() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reasoning-imports-owl2rl-domain:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Instance data: alice has a name, no explicit rdf:type.
    let ledger = apply_trig(
        &fluree,
        ledger,
        r#"
        @prefix ex: <http://example.org/> .
        ex:alice ex:employeeName "Alice" .
        "#,
    )
    .await;

    // A imports B; B declares ex:employeeName rdfs:domain ex:Employee.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f:    <https://ns.flur.ee/db#> .
        @prefix owl:  <http://www.w3.org/2002/07/owl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex:   <http://example.org/> .

        GRAPH <http://example.org/ontology/A> {{
            <http://example.org/ontology/A>
                rdf:type owl:Ontology ;
                owl:imports <http://example.org/ontology/B> .
        }}

        GRAPH <http://example.org/ontology/B> {{
            ex:employeeName rdfs:domain ex:Employee .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig ;
                              f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning>
                f:schemaSource <urn:config:schema-ref> ;
                f:followOwlImports true .
            <urn:config:schema-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:schema-source> .
            <urn:config:schema-source> f:graphSelector <http://example.org/ontology/A> .
        }}
        "
    );
    let _ = apply_trig(&fluree, ledger, &trig).await;

    let q = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?s",
        "where": {"@id": "?s", "@type": "ex:Employee"},
        "reasoning": "owl2rl"
    });
    let view = fluree.db(ledger_id).await.unwrap();
    let rows = fluree
        .query(&view, &q)
        .await
        .unwrap()
        .to_jsonld(&view.snapshot)
        .unwrap();
    let normalized = support::normalize_rows(&rows);

    assert!(
        normalized.contains(&json!("ex:alice")),
        "rdfs:domain from imported graph B should type alice as Employee; \
         got {normalized:?}"
    );
}
