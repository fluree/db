//! End-to-end cross-ledger SHACL shape enforcement.
//!
//! Data ledger D's `#config` declares `f:shapesSource` with
//! `f:ledger` pointing at model ledger M's shapes graph. The SHACL
//! pass (`apply_shacl_policy_to_staged_view`) resolves M's shapes to
//! an IRI-form wire artifact once a graph the transaction writes has
//! SHACL enabled, then compiles the wire against the *staged*
//! `NamespaceRegistry` (which has D's snapshot namespaces plus any
//! IRIs the in-flight transaction introduced).

#![cfg(all(feature = "native", feature = "shacl"))]

use crate::support::{genesis_ledger, instances_sharing_storage};
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

/// A model ledger's shape with `INF` and `-INF` bounds reaches the data
/// ledger in the datatype's own lexical form, and is enforced there.
#[tokio::test]
async fn cross_ledger_shape_with_infinite_values_is_enforced() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-shapes/model-inf:main";
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(genesis_ledger(&fluree, model_id))
        .upsert_turtle(&format!(
            r#"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:ReadingShape
                    rdf:type        sh:NodeShape ;
                    sh:targetClass  ex:Reading ;
                    sh:property     ex:rshape_score , ex:rshape_level .
                ex:rshape_score
                    sh:path         ex:score ;
                    sh:maxInclusive "INF"^^xsd:double ;
                    sh:minExclusive "-INF"^^xsd:double .
                ex:rshape_level
                    sh:path ex:level ;
                    sh:in   ( "-INF"^^xsd:double "1.5"^^xsd:double ) .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M shapes");

    let data_id = "test/cross-ledger-shapes/data-inf:main";
    let config_iri = config_graph_iri(data_id);
    let data = fluree
        .stage_owned(genesis_ledger(&fluree, data_id))
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
        .expect("seed D cross-ledger SHACL config")
        .ledger;

    let reading = |id: &str, score: &str, level: &str| {
        json!({
            "@context": {
                "ex": "http://example.org/ns/",
                "xsd": "http://www.w3.org/2001/XMLSchema#"
            },
            "@id": id,
            "@type": "ex:Reading",
            "ex:score": {"@value": score, "@type": "xsd:double"},
            "ex:level": {"@value": level, "@type": "xsd:double"}
        })
    };
    let data = fluree
        .insert(data, &reading("ex:r1", "INF", "-INF"))
        .await
        .expect("INF is within [.., INF] and -INF is in the list")
        .ledger;
    let err = fluree
        .insert(data.clone(), &reading("ex:r2", "1.0", "2.5"))
        .await
        .expect_err("2.5 is not in the list");
    assert_shacl_violation(err, "sh:in with -INF");
    let err = fluree
        .insert(data, &reading("ex:r3", "-INF", "1.5"))
        .await
        .expect_err("-INF is not above -INF");
    assert_shacl_violation(err, "sh:minExclusive -INF");
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

/// `f:classMembershipSource` names graphs of the data ledger, but a cross-ledger
/// `f:shapesSource` resolves `sh:class` membership against the model ledger.
/// Rather than silently ignoring the setting, the combination is rejected.
#[tokio::test]
async fn cross_ledger_shapes_reject_class_membership_source() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/cross-ledger-shapes/membership-model:main";
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
                ex:PersonShape rdf:type sh:NodeShape ;
                               sh:targetClass ex:Person ;
                               sh:property ex:pshape_state .
                ex:pshape_state sh:path ex:homeState ;
                                sh:class ex:USState .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M shapes");

    let data_id = "test/cross-ledger-shapes/membership-data:main";
    let data = genesis_ledger(&fluree, data_id);
    let config_iri = config_graph_iri(data_id);
    let data = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <http://example.org/states> {{
                ex:illinois rdf:type ex:USState .
            }}
            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled true .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
                <urn:cfg:shacl> f:classMembershipSource <urn:cfg:states-ref> .
                <urn:cfg:states-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:states-src> .
                <urn:cfg:states-src> f:graphSelector <http://example.org/states> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger SHACL config with a membership source")
        .ledger;

    let err = fluree
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
        .expect_err("the combination must be rejected, not silently ignored");
    assert!(
        format!("{err:?}").contains("f:classMembershipSource with a cross-ledger f:shapesSource"),
        "expected the unsupported-combination error, got: {err:?}"
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
    seed_shapes_source_config(fluree, data_id, model_id, extra_shacl_config).await
}

/// Data ledger D's `#config`: SHACL on, with `f:shapesSource` pointing at
/// model ledger M's shapes graph. Returns D's post-config `LedgerState`.
async fn seed_shapes_source_config(
    fluree: &fluree_db_api::Fluree,
    data_id: &str,
    model_id: &str,
    extra_shacl_config: &str,
) -> fluree_db_api::LedgerState {
    let shapes_graph_iri = "http://example.org/governance/shapes";
    let config_iri = config_graph_iri(data_id);
    let r = fluree
        .stage_owned(genesis_ledger(fluree, data_id))
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

/// A model ledger dropped and created again under its name restarts at the
/// same `t`. D's compiled shapes are cached by the wire's origin, so a
/// recreated M with other shapes must still recompile.
#[tokio::test]
async fn recreated_model_ledger_shapes_replace_the_cached_compile() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-shapes/recreate-model:main";
    let data_id = "test/cross-ledger-shapes/recreate-data:main";
    let data = seed_cross_ledger_person_shape(&fluree, model_id, data_id, "").await;

    // Conforming write first: an empty namespace delta afterwards makes the
    // next transactions eligible for the compile cache.
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
    let nameless_bob = || async {
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
    };
    let err = nameless_bob()
        .await
        .expect_err("M requires a name: compiled and cached");
    assert!(
        err.to_string().contains("SHACL") || format!("{err:?}").contains("ShaclViolation"),
        "expected ShaclViolation, got: {err:?}"
    );

    // The new M, at the dropped M's t, deactivates the shape.
    fluree
        .drop_ledger(
            "test/cross-ledger-shapes/recreate-model",
            fluree_db_api::DropMode::Hard,
        )
        .await
        .expect("drop M");
    seed_person_name_shape(&fluree, model_id, false).await;

    nameless_bob()
        .await
        .expect("the new M's deactivated shape must no longer reject");
}

/// Commit model ledger M: a Person shape requiring a name, active or not.
async fn seed_person_name_shape(fluree: &fluree_db_api::Fluree, model_id: &str, active: bool) {
    let deactivated = !active;
    fluree
        .stage_owned(genesis_ledger(fluree, model_id))
        .upsert_turtle(&format!(
            r"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .
            GRAPH <http://example.org/governance/shapes> {{
                ex:PersonShape rdf:type sh:NodeShape ;
                               sh:targetClass ex:Person ;
                               sh:property ex:pshape_name ;
                               sh:deactivated {deactivated} .
                ex:pshape_name sh:path ex:name ; sh:minCount 1 .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M");
}

/// The reverse: a recreated M that rejects what the dropped one let through.
/// The compiled shapes cached from the dropped M must not keep passing it.
#[tokio::test]
async fn recreated_model_ledger_shapes_reject_what_the_cached_compile_passed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-shapes/recreate-strict-model:main";
    let data_id = "test/cross-ledger-shapes/recreate-strict-data:main";
    seed_person_name_shape(&fluree, model_id, false).await;
    let data = seed_shapes_source_config(&fluree, data_id, model_id, "").await;

    // Conforming write first: an empty namespace delta afterwards makes the
    // next transactions eligible for the compile cache.
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
    let nameless = |person: &'static str| {
        let fluree = &fluree;
        async move {
            fluree
                .graph(data_id)
                .transact()
                .insert(&json!({
                    "@context": {"ex": "http://example.org/ns/"},
                    "@id": person,
                    "@type": "ex:Person"
                }))
                .commit()
                .await
        }
    };
    nameless("ex:bob")
        .await
        .expect("M's shape is deactivated: compiled and cached");

    fluree
        .drop_ledger(
            "test/cross-ledger-shapes/recreate-strict-model",
            fluree_db_api::DropMode::Hard,
        )
        .await
        .expect("drop M");
    seed_person_name_shape(&fluree, model_id, true).await;

    let err = nameless("ex:carol")
        .await
        .expect_err("the new M, at the dropped M's t, requires a name");
    assert!(
        err.to_string().contains("SHACL") || format!("{err:?}").contains("ShaclViolation"),
        "expected ShaclViolation, got: {err:?}"
    );
}

/// A transaction reads a model ledger at the head its first cross-ledger
/// lookup found, for every operation it stages, as it reads its own ledger's
/// governance from the state it began on: a shape the model activates while
/// the transaction is open does not apply to the transaction's later
/// operations, also after a rollback stages the earlier ones again. A
/// transaction begun after that enforces it.
#[tokio::test]
async fn a_transaction_reads_a_model_ledger_at_one_head() {
    use fluree_db_api::{CommitOpts, TransactionOptions, TxnOperation};
    let fluree = FlureeBuilder::memory().build_memory();
    let model_id = "test/cross-ledger-shapes/txn-head-model:main";
    let data_id = "test/cross-ledger-shapes/txn-head-data:main";
    seed_person_name_shape(&fluree, model_id, false).await;
    seed_shapes_source_config(&fluree, data_id, model_id, "").await;
    let person = |id: &str, name: Option<&str>| {
        let mut node = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": id,
            "@type": "ex:Person"
        });
        if let Some(name) = name {
            node["ex:name"] = json!(name);
        }
        TxnOperation::Insert(node)
    };

    let mut txn = fluree
        .begin_transaction(data_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(person("ex:alice", Some("Alice")))
        .await
        .expect("the first operation reads M, whose shape is deactivated");
    fluree
        .graph(model_id)
        .transact()
        .upsert_turtle(
            r"
            @prefix sh: <http://www.w3.org/ns/shacl#> .
            @prefix ex: <http://example.org/ns/> .
            GRAPH <http://example.org/governance/shapes> {
                ex:PersonShape sh:deactivated false .
            }
        ",
        )
        .commit()
        .await
        .expect("M activates its shape");
    let savepoint = txn.savepoint();
    txn.stage(person("ex:dave", Some("Dave")))
        .await
        .expect("a named Person conforms under either head");
    txn.rollback_to(savepoint)
        .await
        .expect("rolling back stages the first operation again");
    txn.stage(person("ex:bob", None))
        .await
        .expect("a later operation reads M at the head the first one read");
    txn.commit(CommitOpts::default())
        .await
        .expect("the transaction commits");

    let mut txn = fluree
        .begin_transaction(data_id, TransactionOptions::default())
        .await
        .expect("begin");
    let err = match txn.stage(person("ex:carol", None)).await {
        Err(err) => err,
        Ok(()) => txn
            .commit(CommitOpts::default())
            .await
            .expect_err("a new transaction reads M's new head"),
    };
    assert_shacl_violation(err, "a transaction begun after M's change");
}

const US_STATE_SHAPES_GRAPH: &str = "http://example.org/governance/shapes";

/// Commit model ledger `model`: a shape whose `ex:homeState` must be an
/// `ex:USState`, and the vocabulary of `states`, in one graph.
async fn seed_us_state_model(fluree: &fluree_db_api::Fluree, model: &str, states: &[&str]) {
    let members: String = states
        .iter()
        .map(|state| format!("ex:{state} rdf:type ex:USState .\n"))
        .collect();
    fluree
        .stage_owned(genesis_ledger(fluree, &format!("{model}:main")))
        .upsert_turtle(&format!(
            r"
            @prefix sh:   <http://www.w3.org/ns/shacl#> .
            @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{US_STATE_SHAPES_GRAPH}> {{
                ex:PersonShape rdf:type sh:NodeShape ;
                               sh:targetClass ex:Person ;
                               sh:property ex:pshape_state .
                ex:pshape_state sh:path ex:homeState ; sh:class ex:USState .
                {members}
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M");
}

/// `sh:class` membership is read from M itself, not from the shapes wire.
/// With M recreated by another instance sharing the storage and nameservice,
/// nothing clears this instance's cached handle of the dropped M, which sits
/// at the `t` the new M reaches. Membership still comes from the new M.
#[tokio::test]
async fn sh_class_membership_follows_a_model_recreated_by_another_instance() {
    let (writer, reader) = instances_sharing_storage();
    let model = "test/cross-ledger-shapes/recreate-vocab-model";
    let data_id = "test/cross-ledger-shapes/recreate-vocab-data:main";
    seed_us_state_model(&writer, model, &["illinois", "iowa"]).await;
    seed_shapes_source_config(&writer, data_id, &format!("{model}:main"), "").await;

    let from_illinois = |person: &'static str| {
        let reader = &reader;
        async move {
            reader
                .graph(data_id)
                .transact()
                .insert(&json!({
                    "@context": {"ex": "http://example.org/ns/"},
                    "@id": person,
                    "@type": "ex:Person",
                    "ex:homeState": {"@id": "ex:illinois"}
                }))
                .commit()
                .await
        }
    };
    from_illinois("ex:alice")
        .await
        .expect("M lists Illinois, and the reader caches M");

    writer
        .drop_ledger(model, fluree_db_api::DropMode::Hard)
        .await
        .expect("drop M");
    seed_us_state_model(&writer, model, &["iowa"]).await;

    let err = from_illinois("ex:bob")
        .await
        .expect_err("the new M does not list Illinois");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation, got: {err:?}"
    );
}

/// A `f:shapesSource` whose `f:ledger` names the data ledger itself is a
/// same-ledger reference in cross-ledger clothing. A write that stages under
/// the ledger's write lock (a SPARQL update always does) then opened the
/// "model" ledger through the ledger cache, whose read of that same lock
/// waited forever. The self-reference is now served from the state staging
/// already holds.
#[tokio::test]
async fn self_referencing_shapes_source_does_not_deadlock_a_locked_write() {
    use std::time::Duration;

    let fluree = FlureeBuilder::memory().build_memory();
    let data_id = "test/cross-ledger-shapes/self-ref:main";
    fluree.create_ledger(data_id).await.expect("create");
    let handle = fluree.ledger_cached(data_id).await.expect("cache");

    let shapes_graph_iri = "http://example.org/governance/shapes";
    let config_iri = config_graph_iri(data_id);
    let seed = format!(
        r"
        @prefix sh:   <http://www.w3.org/ns/shacl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex:   <http://example.org/ns/> .
        @prefix f:    <https://ns.flur.ee/db#> .

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
        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
            <urn:cfg:shacl> f:shaclEnabled true .
            <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
            <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                 f:graphSource <urn:cfg:shapes-src> .
            <urn:cfg:shapes-src> f:ledger <{data_id}> ;
                                 f:graphSelector <{shapes_graph_iri}> .
        }}
    "
    );
    fluree
        .stage(&handle)
        .upsert_turtle(&seed)
        .execute()
        .await
        .expect("seed shapes and a self-referencing config");

    let wait = Duration::from_secs(20);
    let ok = "PREFIX ex: <http://example.org/ns/> \
              INSERT DATA { ex:alice a ex:Person ; ex:name \"Alice\" }";
    tokio::time::timeout(wait, fluree.stage(&handle).sparql_update(ok).execute())
        .await
        .expect("the write completed instead of waiting on its own ledger lock")
        .expect("a conforming Person passes the self-referenced shape");

    let bad = "PREFIX ex: <http://example.org/ns/> INSERT DATA { ex:bob a ex:Person }";
    let err = tokio::time::timeout(wait, fluree.stage(&handle).sparql_update(bad).execute())
        .await
        .expect("the write completed instead of waiting on its own ledger lock")
        .expect_err("a Person without a name is rejected by the self-referenced shape");
    assert_shacl_violation(err, "self-referencing shapes source");
}

// =============================================================================
// A model ledger that goes away: validation artifacts resolve only where SHACL
// is enabled, and a config-only write never needs them
// =============================================================================

/// Model ledger M with the `ex:PersonShape` shapes graph, and data ledger D
/// whose config points `f:shapesSource` at it with `f:shaclEnabled` set to
/// `enabled`; then M is dropped. Returns D's id and M's name.
async fn seed_then_drop_shapes_model(
    fluree: &fluree_db_api::Fluree,
    tag: &str,
    enabled: bool,
) -> (String, String) {
    let model_name = format!("test/xl-dropped/{tag}-model");
    let model_id = format!("{model_name}:main");
    let data_id = format!("test/xl-dropped/{tag}-data:main");
    let shapes_graph_iri = "http://example.org/governance/shapes";
    fluree
        .stage_owned(genesis_ledger(fluree, &model_id))
        .upsert_turtle(&format!(
            r"
            @prefix sh:  <http://www.w3.org/ns/shacl#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{shapes_graph_iri}> {{
                ex:PersonShape rdf:type sh:NodeShape ;
                               sh:targetClass ex:Person ;
                               sh:property ex:pshape_name .
                ex:pshape_name sh:path ex:name ;
                               sh:minCount 1 .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M shapes");
    fluree
        .stage_owned(genesis_ledger(fluree, &data_id))
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:shaclDefaults <urn:cfg:shacl> .
                <urn:cfg:shacl> f:shaclEnabled {enabled} .
                <urn:cfg:shacl> f:shapesSource <urn:cfg:shapes-ref> .
                <urn:cfg:shapes-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:shapes-src> .
                <urn:cfg:shapes-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{shapes_graph_iri}> .
            }}
        ",
            config_iri = config_graph_iri(&data_id),
        ))
        .execute()
        .await
        .expect("seed D cross-ledger SHACL config");
    fluree
        .drop_ledger(&model_name, fluree_db_api::DropMode::Soft)
        .await
        .expect("drop M");
    (data_id, model_name)
}

fn nameless_person(id: &str) -> serde_json::Value {
    json!({"@context": {"ex": "http://example.org/ns/"}, "@id": id, "@type": "ex:Person"})
}

/// The dropped-model brick: with SHACL disabled, the configured shapes source
/// is never resolved, so dropping the model ledger it names blocks nothing.
/// A data write and a config write both commit.
#[tokio::test]
async fn dropped_shapes_model_does_not_block_writes_where_shacl_is_off() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (data_id, _) = seed_then_drop_shapes_model(&fluree, "off", false).await;

    fluree
        .graph(&data_id)
        .transact()
        .insert(&nameless_person("ex:alice"))
        .commit()
        .await
        .expect("SHACL is off: the data write never resolves the dropped model");
    fluree
        .graph(&data_id)
        .transact()
        .sparql_update(&format!(
            "PREFIX f: <https://ns.flur.ee/db#> \
             INSERT DATA {{ GRAPH <{}> {{ <urn:cfg:shacl> f:validationMode f:ValidationWarn }} }}",
            config_graph_iri(&data_id)
        ))
        .commit()
        .await
        .expect("a config write never resolves the dropped model");
}

/// Where SHACL is enabled, a data write whose shapes model is gone fails
/// closed and names the model. The config repair that disables SHACL commits
/// (a config-only write is never validated), through JSON-LD and through
/// SPARQL alike, and data writes commit after it.
#[tokio::test]
async fn dropped_shapes_model_fails_closed_and_the_config_repair_commits() {
    for surface in ["json-ld", "sparql"] {
        let fluree = FlureeBuilder::memory().build_memory();
        let (data_id, model_name) =
            seed_then_drop_shapes_model(&fluree, &format!("on-{surface}"), true).await;

        let err = fluree
            .graph(&data_id)
            .transact()
            .insert(&nameless_person("ex:alice"))
            .commit()
            .await
            .expect_err("SHACL is on and its shapes source is gone: fail closed");
        assert!(
            err.to_string().contains(&model_name),
            "{surface}: the refusal names the missing model ledger: {err}"
        );

        match surface {
            "json-ld" => fluree
                .graph(&data_id)
                .transact()
                .upsert(&json!({
                    "@context": {"f": "https://ns.flur.ee/db#"},
                    "@id": "urn:cfg:shacl",
                    "@graph": "config",
                    "f:shaclEnabled": false
                }))
                .commit()
                .await
                .expect("json-ld: the config repair commits"),
            _ => fluree
                .graph(&data_id)
                .transact()
                .sparql_update(&format!(
                    "PREFIX f: <https://ns.flur.ee/db#> \
                     DELETE {{ GRAPH <{cfg}> {{ <urn:cfg:shacl> f:shaclEnabled ?on }} }} \
                     INSERT {{ GRAPH <{cfg}> {{ <urn:cfg:shacl> f:shaclEnabled false }} }} \
                     WHERE {{ GRAPH <{cfg}> {{ <urn:cfg:shacl> f:shaclEnabled ?on }} }}",
                    cfg = config_graph_iri(&data_id)
                ))
                .commit()
                .await
                .expect("sparql: the config repair commits"),
        };

        fluree
            .graph(&data_id)
            .transact()
            .insert(&nameless_person("ex:bob"))
            .commit()
            .await
            .unwrap_or_else(|e| panic!("{surface}: after the repair data writes commit: {e}"));
    }
}

/// A cross-ledger `f:schemaSource` (the ontology SHACL targeting reads) whose
/// model ledger is gone blocks nothing while SHACL is off: the transaction
/// path resolves it only for SHACL, and a policy context that would read it
/// is built only where policy applies.
#[tokio::test]
async fn dropped_schema_model_does_not_block_writes_where_shacl_is_off() {
    let fluree = FlureeBuilder::memory().build_memory();
    let model_name = "test/xl-dropped/schema-model";
    let model_id = format!("{model_name}:main");
    let data_id = "test/xl-dropped/schema-data:main";
    let ontology_graph_iri = "http://example.org/governance/ontology";
    fluree
        .stage_owned(genesis_ledger(&fluree, &model_id))
        .upsert_turtle(&format!(
            r"
            @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
            @prefix ex:   <http://example.org/ns/> .

            GRAPH <{ontology_graph_iri}> {{ ex:Manager rdfs:subClassOf ex:Employee . }}
        "
        ))
        .execute()
        .await
        .expect("seed M ontology");
    let data = fluree
        .stage_owned(genesis_ledger(&fluree, data_id))
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig ;
                               f:reasoningDefaults <urn:cfg:reason> .
                <urn:cfg:reason> f:schemaSource <urn:cfg:schema-ref> .
                <urn:cfg:schema-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:schema-src> .
                <urn:cfg:schema-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{ontology_graph_iri}> .
            }}
        ",
            config_iri = config_graph_iri(data_id),
        ))
        .execute()
        .await
        .expect("seed D cross-ledger schema config")
        .ledger;
    fluree
        .drop_ledger(model_name, fluree_db_api::DropMode::Soft)
        .await
        .expect("drop M");

    // No policy input: no policy context, so nothing reads the schema.
    let policy = fluree_db_api::build_transact_policy_context(
        &fluree,
        &data.snapshot,
        data.novelty.as_ref(),
        Some(data.novelty.as_ref()),
        data.t(),
        &fluree_db_api::GovernanceOptions::default(),
    )
    .await
    .expect("a write without policy does not resolve the schema source");
    assert!(policy.is_none());

    fluree
        .graph(data_id)
        .transact()
        .insert(&json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:grace",
            "@type": "ex:Manager"
        }))
        .commit()
        .await
        .expect("SHACL is off: the data write never resolves the dropped ontology model");
}
