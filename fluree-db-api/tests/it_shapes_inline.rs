//! Inline SHACL: per-transaction shape definitions passed via `opts.shapes`.
//!
//! Inline shapes parse against the *staged* `NamespaceRegistry`,
//! same as cross-ledger shapes. They never persist into the ledger —
//! the bundle is overlay-only, scoped to this validation pass.

#![cfg(all(feature = "native", feature = "shacl"))]

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig};
use fluree_db_transact::ir::TxnOpts;
use serde_json::json;

mod support;
use support::genesis_ledger;

fn test_index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    }
}

fn person_shape_jsonld() -> serde_json::Value {
    json!({
        "@context": {
            "ex":  "http://example.org/ns/",
            "sh":  "http://www.w3.org/ns/shacl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id":            "ex:PersonShape",
                "@type":          "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property":    {"@id": "ex:pshape_name"}
            },
            {
                "@id":         "ex:pshape_name",
                "sh:path":     {"@id": "ex:name"},
                "sh:minCount": 1,
                "sh:datatype": {"@id": "xsd:string"}
            }
        ]
    })
}

#[tokio::test]
async fn inline_shape_rejects_violating_tx() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-shapes/reject:main");

    let opts = TxnOpts {
        shapes: Some(person_shape_jsonld()),
        ..TxnOpts::default()
    };

    // ex:Person without ex:name → reject (inline shape requires name).
    let err = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":   "ex:alice",
                "@type": "ex:Person"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("inline shape must reject Person without name");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation from inline shape, got: {err:?}"
    );
}

#[tokio::test]
async fn inline_shape_accepts_valid_tx() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-shapes/accept:main");

    let opts = TxnOpts {
        shapes: Some(person_shape_jsonld()),
        ..TxnOpts::default()
    };

    fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":    "ex:bob",
                "@type":  "ex:Person",
                "ex:name": "Bob"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("valid Person under inline shape must be accepted");
}

#[tokio::test]
async fn inline_shapes_do_not_persist_after_tx() {
    // After a tx that supplies inline shapes, the shapes must not
    // remain enforced on a subsequent tx without `opts.shapes`.
    // (They were never staged into the ledger.)
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-shapes/transient:main");

    // First tx: pass inline shape + a valid Person.
    let opts = TxnOpts {
        shapes: Some(person_shape_jsonld()),
        ..TxnOpts::default()
    };
    let r1 = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":    "ex:carol",
                "@type":  "ex:Person",
                "ex:name": "Carol"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("first tx with inline shapes ok");
    let ledger = r1.ledger;

    // Second tx: NO opts.shapes. A Person without ex:name should
    // be accepted — the inline shape was transient.
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":   "ex:dave",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect("second tx without opts.shapes must not be subject to prior inline shape");
}

#[tokio::test]
async fn inline_shape_layered_on_cross_ledger_shape_enforces_both() {
    // M holds a shape requiring ex:name. Inline opts add a shape
    // requiring ex:email. A Person missing either → reject.
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "test/inline-shapes/layered-model:main";
    let model = genesis_ledger(&fluree, model_id);

    let shapes_graph_iri = "http://example.org/governance/shapes";
    let m_trig = format!(
        r"
        @prefix sh:   <http://www.w3.org/ns/shacl#> .
        @prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex:   <http://example.org/ns/> .

        GRAPH <{shapes_graph_iri}> {{
            ex:PersonNameShape
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
        .expect("seed M name-shape");

    let data_id = "test/inline-shapes/layered-data:main";
    let data = genesis_ledger(&fluree, data_id);

    let config_iri = format!("urn:fluree:{data_id}#config");
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
        .expect("seed D cross-ledger config");
    let data = r1.ledger;

    // Inline shape requires ex:email.
    let email_shape = json!({
        "@context": {
            "ex":  "http://example.org/ns/",
            "sh":  "http://www.w3.org/ns/shacl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "@graph": [
            {
                "@id":            "ex:PersonEmailShape",
                "@type":          "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property":    {"@id": "ex:pshape_email"}
            },
            {
                "@id":         "ex:pshape_email",
                "sh:path":     {"@id": "ex:email"},
                "sh:minCount": 1,
                "sh:datatype": {"@id": "xsd:string"}
            }
        ]
    });

    let opts = TxnOpts {
        shapes: Some(email_shape.clone()),
        ..TxnOpts::default()
    };

    // Has name (cross-ledger) but missing email (inline) → reject.
    let err = fluree
        .insert_with_opts(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":    "ex:eve",
                "@type":  "ex:Person",
                "ex:name": "Eve"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("inline shape (email) must reject Person without email");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected ShaclViolation from inline email shape, got: {err:?}"
    );
}
