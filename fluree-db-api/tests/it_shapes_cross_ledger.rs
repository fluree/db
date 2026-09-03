//! End-to-end cross-ledger SHACL shape enforcement.
//!
//! Data ledger D's `#config` declares `f:shapesSource` with
//! `f:ledger` pointing at model ledger M's shapes graph. The
//! cross-ledger dispatch happens at the API boundary
//! (`stage_with_config_shacl`): we resolve M's shapes to an
//! IRI-form wire artifact before staging, thread the wire into
//! `StagedShaclContext`, then at SHACL validation time compile
//! the wire against the *staged* `NamespaceRegistry` (which has
//! D's snapshot namespaces plus any IRIs the in-flight
//! transaction introduced).

#![cfg(all(feature = "native", feature = "shacl"))]

use crate::support::genesis_ledger;
use fluree_db_api::FlureeBuilder;
use serde_json::json;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

#[tokio::test]
async fn data_ledger_tx_rejected_by_cross_ledger_shape() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/model:main";
    let model = genesis_ledger(&fluree, model_id);

    let shapes_graph_iri = "http://example.org/governance/shapes";
    let m_trig = format!(
        r"
        @prefix sh:   <http://www.w3.org/ns/shacl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{shapes_graph_iri}> {{
            ex:PersonShape
                rdf:type        sh:NodeShape ;
                sh:targetClass  ex:Person ;
                sh:property     ex:pshape_name .
            ex:pshape_name
                sh:path     ex:name ;
                sh:minCount 1 ;
                sh:datatype xsd:string .
        }}
    "
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&m_trig)
        .execute()
        .await
        .expect("seed M shapes");

    let data_id = "test/cross-ledger-shapes/data:main";
    let data = genesis_ledger(&fluree, data_id);

    let config_iri = config_graph_iri(data_id);
    let r1 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger SHACL config");
    let data = r1.ledger;

    // ex:Person without ex:name → must be rejected by M's shape.
    // This is the load-bearing assertion: the cross-ledger wire
    // must compile against the staged namespace registry (where
    // ex:Person is registered by the in-flight tx), not against
    // D's pre-stage snapshot (where ex: hasn't been allocated).
    let err = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect_err("violating Person under cross-ledger shape must be rejected");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation from M's cross-ledger shape, got: {err:?}"
    );
}

#[tokio::test]
async fn data_ledger_tx_passes_when_cross_ledger_shape_satisfied() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/valid-model:main";
    let model = genesis_ledger(&fluree, model_id);

    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:property     ex:pshape_name .
                ex:pshape_name
                    sh:path     ex:name ;
                    sh:minCount 1 ;
                    sh:datatype xsd:string .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M shapes");

    let data_id = "test/cross-ledger-shapes/valid-data:main";
    let data = genesis_ledger(&fluree, data_id);

    let config_iri = config_graph_iri(data_id);
    let r1 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");
    let data = r1.ledger;

    // ex:bob has the required ex:name. Shape should accept.
    fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:name": "Bob"
            }),
        )
        .await
        .expect("valid Person under cross-ledger shape must be accepted");
}

/// Cross-ledger `sh:class` value-set: model ledger M holds both the shape
/// (`sh:class ex:USState`) and the controlled vocabulary (`ex:illinois a
/// ex:USState`). Data ledger D references those value-set members. The shape is
/// enforced cross-ledger (via the wire); membership is resolved by querying M
/// live — the vocabulary is ABox and is NOT carried in the shapes wire.
#[tokio::test]
async fn cross_ledger_sh_class_value_set_resolved_against_model_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/vocab-model:main";
    let model = genesis_ledger(&fluree, model_id);

    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:property     ex:pshape_state .
                ex:pshape_state
                    sh:path  ex:homeState ;
                    sh:class ex:USState .

                ex:illinois rdf:type ex:USState .
                ex:iowa     rdf:type ex:USState .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M shapes + controlled vocabulary");

    let data_id = "test/cross-ledger-shapes/vocab-data:main";
    let data = genesis_ledger(&fluree, data_id);

    let config_iri = config_graph_iri(data_id);
    let r1 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger SHACL config");
    let data = r1.ledger;

    // ex:alice references ex:illinois — a value-set member defined ONLY in M.
    // Membership must resolve against M's live vocabulary.
    let ok = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:homeState": {"@id": "ex:illinois"}
            }),
        )
        .await
        .expect("value-set member defined in the model ledger must satisfy sh:class");
    let data = ok.ledger;

    // ex:bob references ex:atlantis — not a member of the value-set in M.
    let err = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:homeState": {"@id": "ex:atlantis"}
            }),
        )
        .await
        .expect_err("non-member value must be rejected by cross-ledger sh:class");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation for non-member, got: {err:?}"
    );
}

/// Cross-ledger RDFS entailment for enforcement: the class hierarchy lives
/// in model ledger M (`f:reasoningDefaults` / `f:schemaSource` with
/// `f:ledger`), while the shape lives locally in D. A Manager-typed record
/// must be governed by D's Employee-targeting shape because
/// `Manager rdfs:subClassOf Employee` is declared in M.
#[tokio::test]
async fn cross_ledger_schema_feeds_shacl_subclass_targeting() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-schema/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let ontology_graph_iri = "http://example.org/governance/ontology";
    let m_trig = format!(
        r"
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{ontology_graph_iri}> {{
            ex:Manager rdfs:subClassOf ex:Employee .
        }}
    "
    );
    fluree
        .stage_owned(model)
        .upsert_turtle(&m_trig)
        .execute()
        .await
        .expect("seed M ontology");

    let data_id = "test/cross-ledger-schema/data:main";
    let data = genesis_ledger(&fluree, data_id);

    // D: local Employee shape + config pointing the schema source at M.
    let config_iri = config_graph_iri(data_id);
    let r1 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:    <https://ns.flur.ee/db#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix ex:   <http://example.org/ns/> .

            ex:EmployeeShape rdf:type sh:NodeShape ;
                sh:targetClass ex:Employee ;
                sh:property ex:EmployeeShape-name .
            ex:EmployeeShape-name sh:path ex:name ;
                sh:minCount 1 .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig ;
                               f:shaclDefaults <urn:cfg:shacl> ;
                               f:reasoningDefaults <urn:cfg:reason> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:reason> f:schemaSource <urn:cfg:schema-ref> .
                <urn:cfg:schema-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:schema-src> .
                <urn:cfg:schema-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{ontology_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D shape + cross-ledger schema config");
    let data = r1.ledger;

    // Manager without ex:name → rejected via M's subclass edge.
    let err = fluree
        .insert(
            data.clone(),
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:grace",
                "@type": "ex:Manager"
            }),
        )
        .await
        .expect_err("Manager must be governed by the Employee shape via M's hierarchy");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation, got: {err:?}"
    );

    // Conforming Manager passes.
    fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:hana",
                "@type": "ex:Manager",
                "ex:name": "Hana"
            }),
        )
        .await
        .expect("conforming Manager must pass");
}

/// Shared seed: model ledger M with a `minCount 1` Person shape, data ledger D
/// whose `#config` points `f:shapesSource` at M. Returns D's post-config
/// `LedgerState`.
async fn seed_cross_ledger_person_shape(
    fluree: &fluree_db_api::Fluree,
    model_id: &str,
    data_id: &str,
    extra_shacl_config: &str,
) -> fluree_db_api::LedgerState {
    let model = genesis_ledger(fluree, model_id);
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:property     ex:pshape_name .
                ex:pshape_name
                    sh:path     ex:name ;
                    sh:minCount 1 ;
                    sh:datatype xsd:string .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M shapes");

    let data = genesis_ledger(fluree, data_id);
    let config_iri = config_graph_iri(data_id);
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                {extra_shacl_config}
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger SHACL config");
    r.ledger
}

fn assert_shacl_violation(err: fluree_db_api::ApiError, context: &str) {
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "{context}: expected ShaclViolation, got: {err:?}"
    );
}

/// The direct-flake Turtle insert path resolves and enforces a cross-ledger
/// `f:shapesSource` just like the JSON-LD staging path.
#[tokio::test]
async fn turtle_insert_rejected_by_cross_ledger_shape() {
    let fluree = FlureeBuilder::memory().build_memory();
    let data = seed_cross_ledger_person_shape(
        &fluree,
        "test/cross-ledger-shapes/ttl-model:main",
        "test/cross-ledger-shapes/ttl-data:main",
        "",
    )
    .await;

    let err = fluree
        .insert_turtle(
            data,
            r"
            @prefix ex: <http://example.org/ns/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            ex:alice rdf:type ex:Person .
        ",
        )
        .await
        .expect_err("violating Turtle insert under cross-ledger shape must be rejected");
    assert_shacl_violation(err, "turtle insert");

    // A conforming Turtle insert passes on the same path.
    let data_id = "test/cross-ledger-shapes/ttl-data:main";
    fluree
        .graph(data_id)
        .transact()
        .insert_turtle(
            r#"
            @prefix ex: <http://example.org/ns/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            ex:bob rdf:type ex:Person ;
                   ex:name "Bob" .
        "#,
        )
        .commit()
        .await
        .expect("conforming Turtle insert must pass");
}

/// `Fluree::validate_ledger` resolves a cross-ledger `f:shapesSource` and
/// reports violations of M's shapes against D's committed state.
#[tokio::test]
async fn validate_ledger_reports_cross_ledger_shape_violations() {
    let fluree = FlureeBuilder::memory().build_memory();
    let data_id = "test/cross-ledger-shapes/validate-data:main";
    let data = seed_cross_ledger_person_shape(
        &fluree,
        "test/cross-ledger-shapes/validate-model:main",
        data_id,
        "<urn:cfg:shacl> f:validationMode f:ValidationWarn .",
    )
    .await;

    // Warn mode: the violating Person commits (with a logged warning) so
    // there is non-conforming state for the report to find.
    fluree
        .insert(
            data,
            &serde_json::json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect("warn mode must admit the violating write");

    let report = fluree
        .validate_ledger(
            data_id,
            &fluree_db_api::validate::ValidateOptions::default(),
        )
        .await
        .expect("validate_ledger must resolve the cross-ledger shapes source");
    assert!(
        !report.conforms,
        "report must flag the missing ex:name (results: {:?})",
        report.results
    );
    // The cross-ledger path pins the data ledger's t, not the model's.
    let data_handle = fluree
        .ledger_cached(data_id)
        .await
        .expect("data ledger handle");
    assert_eq!(report.t, data_handle.t().await);
    assert!(
        report
            .results
            .iter()
            .any(|r| r.constraint_component.contains("MinCount")),
        "expected a MinCount violation, got: {:?}",
        report.results
    );
}

/// sh:sparql constraints survive the cross-ledger wire: the SPARQL text,
/// parsed at compile time on D, enforces against staged writes.
#[tokio::test]
async fn sh_sparql_constraint_enforced_across_ledgers() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/sparql-model:main";
    let model = genesis_ledger(&fluree, model_id);
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:sparql       ex:shortNameConstraint .
                ex:shortNameConstraint
                    sh:message "name shorter than 3 characters" ;
                    sh:select "SELECT $this ?value WHERE {{ $this <http://example.org/ns/name> ?value . FILTER(STRLEN(?value) < 3) }}" .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M sh:sparql shape");

    let data_id = "test/cross-ledger-shapes/sparql-data:main";
    let data = genesis_ledger(&fluree, data_id);
    let config_iri = config_graph_iri(data_id);
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");
    let data = r.ledger;

    // The very first write on D mints the `ex:` namespace — sh:sparql
    // lowering runs against the STAGED registry, so the constraint must
    // already see the in-flight transaction's data.
    let err = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:al",
                "@type": "ex:Person",
                "ex:name": "Al"
            }),
        )
        .await
        .expect_err("2-char name must violate the sh:sparql constraint");
    assert_shacl_violation(err, "sh:sparql over the wire (first-mint namespace)");

    fluree
        .graph(data_id)
        .transact()
        .insert(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:alice",
            "@type": "ex:Person",
            "ex:name": "Alice"
        }))
        .commit()
        .await
        .expect("3+-char name must conform");
}

/// A constraint over vocabulary the data ledger has never seen is silently
/// inert — never an error and never a spurious violation. The query's
/// unknown IRI lowers to a never-matching Sid, so it yields no rows.
#[tokio::test]
async fn sh_sparql_over_unknown_vocabulary_is_inert() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/unknown-vocab-model:main";
    let model = genesis_ledger(&fluree, model_id);
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Person ;
                    sh:sparql       ex:futureConstraint .
                ex:futureConstraint
                    sh:message "future-vocab rule" ;
                    sh:select "SELECT $this ?value WHERE {{ $this <http://not-yet.example/anywhere#p> ?value }}" .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M shape over unknown vocabulary");

    let data_id = "test/cross-ledger-shapes/unknown-vocab-data:main";
    let data = genesis_ledger(&fluree, data_id);
    let config_iri = config_graph_iri(data_id);
    let r = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");
    let data = r.ledger;

    fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("constraint over never-seen vocabulary must be inert, not an error");
}

/// Compiled-shape reuse stays sound across model-ledger updates: repeated
/// transactions reuse the compiled shapes while M's head is unchanged, and a
/// shape change on M (here: `sh:deactivated true`, which must also survive
/// the wire) takes effect on the very next transaction.
#[tokio::test]
async fn model_head_advance_recompiles_shapes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-shapes/cache-model:main";
    let data_id = "test/cross-ledger-shapes/cache-data:main";
    let data = seed_cross_ledger_person_shape(&fluree, model_id, data_id, "").await;

    // Conforming write first: registers D's `ex:` namespace so later
    // transactions have an empty namespace delta (the compile-cache
    // eligibility condition for cross-ledger sources).
    fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("conforming Person must pass");

    // Two violating writes in a row: the first compiles + caches, the second
    // runs on the cached compile. Both must reject identically.
    for attempt in ["compile-and-store", "cache-hit"] {
        let err = fluree
            .graph(data_id)
            .transact()
            .insert(&json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:bob",
                "@type": "ex:Person"
            }))
            .commit()
            .await
            .err()
            .unwrap_or_else(|| panic!("{attempt}: violating Person must be rejected"));
        assert!(
            err.to_string().contains("SHACL") || format!("{err:?}").contains("ShaclViolation"),
            "{attempt}: expected ShaclViolation, got: {err:?}"
        );
    }

    // Deactivate the shape on M — the head advance must invalidate the
    // cached compile, and sh:deactivated must survive the wire.
    fluree
        .graph(model_id)
        .transact()
        .upsert_turtle(
            r"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://example.org/ns/> .
            GRAPH <http://example.org/governance/shapes> {
                ex:PersonShape sh:deactivated true .
            }
        ",
        )
        .commit()
        .await
        .expect("deactivate shape on M");

    fluree
        .graph(data_id)
        .transact()
        .insert(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:bob",
            "@type": "ex:Person"
        }))
        .commit()
        .await
        .expect("deactivated shape must no longer reject");
}
