//! Inline SHACL: per-transaction shape definitions passed via `opts.shapes`.
//!
//! Inline shapes parse against the *staged* `NamespaceRegistry`,
//! same as cross-ledger shapes. They never persist into the ledger —
//! the bundle is overlay-only, scoped to this validation pass.

#![cfg(all(feature = "native", feature = "shacl"))]

use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig};
use fluree_db_transact::ir::TxnOpts;
use serde_json::json;

use crate::support::genesis_ledger;

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

// =============================================================================
// Inline shapes under explicit-only SHACL: a request-time setting, gated by
// override control (not `f:shaclEnabled`), in a pass of their own
// =============================================================================

/// A new ledger whose `#config` graph holds `config` (Turtle, with `f:` and
/// `rdf:` bound).
async fn ledger_with_config(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    config: &str,
) -> fluree_db_api::LedgerState {
    let trig = format!(
        "@prefix f: <https://ns.flur.ee/db#> .\n\
         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
         GRAPH <urn:fluree:{ledger_id}#config> {{ {config} }}"
    );
    fluree
        .stage_owned(genesis_ledger(fluree, ledger_id))
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write")
        .ledger
}

fn person(id: &str, name: Option<&str>) -> serde_json::Value {
    let mut node = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": id,
        "@type": "ex:Person"
    });
    if let Some(name) = name {
        node["ex:name"] = json!(name);
    }
    node
}

fn inline_opts() -> TxnOpts {
    TxnOpts {
        shapes: Some(person_shape_jsonld()),
        ..TxnOpts::default()
    }
}

async fn insert_inline(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    doc: &serde_json::Value,
    opts: TxnOpts,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    fluree
        .insert_with_opts(ledger, doc, opts, CommitOpts::default(), &test_index_cfg())
        .await
}

fn assert_shacl_violation(err: &fluree_db_api::ApiError, what: &str) {
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "{what}: expected a SHACL violation, got {err:?}"
    );
}

fn assert_override_refused(err: &fluree_db_api::ApiError, graph: &str, control: &str) {
    match err {
        fluree_db_api::ApiError::Transact(
            fluree_db_transact::TransactError::RequestOverrideRefused {
                setting,
                graph: refused_graph,
                control: refused_control,
            },
        ) => {
            assert_eq!(setting, "inline SHACL shapes (opts.shapes)");
            assert_eq!(refused_graph, graph);
            assert_eq!(refused_control, control);
        }
        other => panic!("expected RequestOverrideRefused, got {other:?}"),
    }
    assert_eq!(err.status_code(), 400, "{err}");
}

async fn head_t(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> i64 {
    fluree.ledger(ledger_id).await.expect("load").t()
}

/// `f:shaclEnabled false` does not block inline shapes: they are a request
/// setting, and the default override control (`f:OverrideAll`) permits it.
#[tokio::test]
async fn inline_shapes_apply_where_config_disables_shacl() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with_config(
        &fluree,
        "test/inline-shapes/shacl-off:main",
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:shaclDefaults <urn:cfg:shacl> . \
         <urn:cfg:shacl> f:shaclEnabled false .",
    )
    .await;
    let err = insert_inline(&fluree, ledger, &person("ex:alice", None), inline_opts())
        .await
        .expect_err("the inline shape requires ex:name");
    assert_shacl_violation(&err, "inline shapes over f:shaclEnabled false");
}

/// Under `f:OverrideNone` inline shapes are refused and nothing commits,
/// even for a record the shapes would accept: dropping them silently would
/// leave the caller's data unchecked.
#[tokio::test]
async fn inline_shapes_are_refused_under_override_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/override-none:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:shaclDefaults <urn:cfg:shacl> . \
         <urn:cfg:shacl> f:overrideControl f:OverrideNone .",
    )
    .await;
    let t = ledger.t();
    let err = insert_inline(
        &fluree,
        ledger,
        &person("ex:bob", Some("Bob")),
        inline_opts(),
    )
    .await
    .expect_err("override control refuses inline shapes");
    assert_override_refused(&err, "the default graph", "f:OverrideNone");
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");
}

/// `f:IdentityRestricted`: the listed verified identity may send inline
/// shapes (they apply); any other identity is refused.
#[tokio::test]
async fn inline_shapes_follow_an_identity_restricted_override_control() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/identity-restricted:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:shaclDefaults <urn:cfg:shacl> . \
         <urn:cfg:shacl> f:overrideControl <urn:cfg:oc> . \
         <urn:cfg:oc> f:controlMode f:IdentityRestricted ; \
                      f:allowedIdentities <did:key:admin> .",
    )
    .await;
    let as_identity = |identity: &str| TxnOpts {
        server_identity: Some(fluree_db_api::VerifiedIdentity::new(identity)),
        ..inline_opts()
    };

    let err = insert_inline(
        &fluree,
        ledger.clone(),
        &person("ex:alice", None),
        as_identity("did:key:admin"),
    )
    .await
    .expect_err("the allowed identity's inline shape applies");
    assert_shacl_violation(&err, "allowed identity");

    let err = insert_inline(
        &fluree,
        ledger,
        &person("ex:bob", Some("Bob")),
        as_identity("did:key:other"),
    )
    .await
    .expect_err("another identity is refused");
    assert_override_refused(&err, "the default graph", "f:IdentityRestricted");
}

/// Each graph the transaction writes is gated on its own: `f:OverrideNone`
/// on graph B alone refuses a write that touches A and B, and leaves a write
/// to A alone under the inline shapes.
#[tokio::test]
async fn inline_shapes_are_refused_when_any_written_graph_refuses() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/per-graph:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:graphOverrides <urn:cfg:gb> . \
         <urn:cfg:gb> rdf:type f:GraphConfig ; \
                      f:targetGraph <http://example.org/g/b> ; \
                      f:shaclDefaults <urn:cfg:gb-shacl> . \
         <urn:cfg:gb-shacl> f:overrideControl f:OverrideNone .",
    )
    .await;
    let t = ledger.t();
    let both = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
             "@graph": "http://example.org/g/a"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:name": "Bob",
             "@graph": "http://example.org/g/b"}
        ]
    });
    let err = insert_inline(&fluree, ledger.clone(), &both, inline_opts())
        .await
        .expect_err("graph B's control refuses the inline shapes");
    assert_override_refused(&err, "graph <http://example.org/g/b>", "f:OverrideNone");
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");

    let only_a = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:carol",
        "@type": "ex:Person",
        "@graph": "http://example.org/g/a"
    });
    let err = insert_inline(&fluree, ledger, &only_a, inline_opts())
        .await
        .expect_err("graph A permits the inline shapes, which require ex:name");
    assert_shacl_violation(&err, "graph A alone");
}

/// Where inline shapes are permitted, a requested warn mode applies to them:
/// the violation is logged and the write commits.
#[tokio::test]
async fn inline_shapes_honor_a_requested_warn_mode() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-shapes/warn:main");
    let opts = TxnOpts {
        validation_mode: Some(fluree_db_core::ledger_config::ValidationMode::Warn),
        ..inline_opts()
    };
    insert_inline(&fluree, ledger, &person("ex:alice", None), opts)
        .await
        .expect("warn mode logs the violation and commits");
}

/// Inline shapes validate only themselves: they never switch on the ledger's
/// stored shapes where config disables SHACL.
#[tokio::test]
async fn inline_shapes_do_not_enable_stored_shapes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/stored-off:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:shaclDefaults <urn:cfg:shacl> . \
         <urn:cfg:shacl> f:shaclEnabled false .",
    )
    .await;
    // The stored shape requires ex:name on every ex:Person.
    let ledger = fluree
        .insert(ledger, &person_shape_jsonld())
        .await
        .expect("stored shape")
        .ledger;
    // The inline shape constrains ex:Employee only.
    let employee_shape = json!({
        "@context": {"ex": "http://example.org/ns/", "sh": "http://www.w3.org/ns/shacl#"},
        "@id": "ex:EmployeeShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Employee"},
        "sh:property": {"sh:path": {"@id": "ex:employeeId"}, "sh:minCount": 1}
    });
    let opts = TxnOpts {
        shapes: Some(employee_shape),
        ..TxnOpts::default()
    };
    insert_inline(&fluree, ledger, &person("ex:alice", None), opts)
        .await
        .expect("the stored shape stays unenforced; the inline one does not target ex:Person");
}

// =============================================================================
// Inline shapes in a policy-scoped request: a current limitation (support for
// inline shapes in policy-scoped requests is a follow-up)
// =============================================================================

fn assert_policy_scoped_refusal(err: &fluree_db_api::ApiError) {
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UnsupportedFeature(_)
            )
        ),
        "expected the policy-scoped refusal, got {err:?}"
    );
    assert!(
        err.to_string()
            .contains("inline request shapes are not supported in a policy-scoped request"),
        "{err}"
    );
    assert_eq!(err.status_code(), 400, "{err}");
}

/// A request that carries a policy context is refused, and nothing commits.
/// The policy here allows the write, so without the refusal it would commit.
#[tokio::test]
async fn inline_shapes_are_refused_in_a_policy_scoped_request() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/policy-request:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &person("ex:seed", Some("Seed")),
        )
        .await
        .expect("seed")
        .ledger;
    let t = ledger.t();
    let policy = fluree_db_api::build_policy_context(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &fluree_db_api::GovernanceOptions {
            policy: Some(json!([{
                "@id": "http://example.org/ns/allowAll",
                "https://ns.flur.ee/db#action": [
                    {"@id": "https://ns.flur.ee/db#view"},
                    {"@id": "https://ns.flur.ee/db#modify"}
                ],
                "https://ns.flur.ee/db#allow": true
            }])),
            default_allow: Some(true),
            ..Default::default()
        },
    )
    .await
    .expect("policy context");
    let err = fluree
        .stage_owned(ledger)
        .txn_opts(inline_opts())
        .insert(&person("ex:bob", Some("Bob")))
        .policy(policy)
        .execute()
        .await
        .expect_err("inline shapes in a policy-scoped request are refused");
    assert_policy_scoped_refusal(&err);
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");
}

/// Policy defaults in the ledger config make every request policy-scoped:
/// inline shapes are refused even when the request carries no policy input.
#[tokio::test]
async fn inline_shapes_are_refused_under_config_policy_defaults() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/policy-config:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> . \
         <urn:cfg:policy> f:defaultAllow false .",
    )
    .await;
    let t = ledger.t();
    let err = insert_inline(
        &fluree,
        ledger,
        &person("ex:bob", Some("Bob")),
        inline_opts(),
    )
    .await
    .expect_err("config policy defaults scope the request");
    assert_policy_scoped_refusal(&err);
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");
}

/// Policy defaults count wherever the config sets them: a policy group set
/// for one graph scopes the request too, whichever graph it writes.
#[tokio::test]
async fn inline_shapes_are_refused_under_policy_defaults_for_any_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/policy-graph-config:main";
    let ledger = ledger_with_config(
        &fluree,
        ledger_id,
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:graphOverrides <urn:cfg:restricted> . \
         <urn:cfg:restricted> rdf:type f:GraphConfig ; \
             f:targetGraph <http://example.org/restricted> ; \
             f:policyDefaults <urn:cfg:restricted-policy> . \
         <urn:cfg:restricted-policy> f:defaultAllow false .",
    )
    .await;
    let t = ledger.t();
    let err = insert_inline(
        &fluree,
        ledger,
        &person("ex:bob", Some("Bob")),
        inline_opts(),
    )
    .await
    .expect_err("a graph's policy defaults scope the request");
    assert_policy_scoped_refusal(&err);
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");
}

/// A config whose only policy default is an unrestricted `f:defaultAllow
/// true` leaves the effective policy unrestricted: inline shapes apply.
#[tokio::test]
async fn inline_shapes_apply_under_an_unrestricted_policy_default() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with_config(
        &fluree,
        "test/inline-shapes/policy-open:main",
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> . \
         <urn:cfg:policy> f:defaultAllow true .",
    )
    .await;
    let err = insert_inline(&fluree, ledger, &person("ex:alice", None), inline_opts())
        .await
        .expect_err("the inline shape requires ex:name");
    assert_shacl_violation(&err, "unrestricted policy default");
}

// =============================================================================
// The request's own constraints in the transaction body (`opts`)
// =============================================================================

/// `opts.shapes` in a transaction body is honored by the embedded API as it is
/// over HTTP, for an insert and an update alike: a record breaking the shapes
/// is refused and nothing commits. (Only the HTTP server used to read it; the
/// embedded API and the CLI's local mode dropped it and committed.)
#[tokio::test]
async fn inline_shapes_in_the_body_are_honored() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "test/inline-shapes/body:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &person("ex:seed", Some("Seed")),
        )
        .await
        .unwrap()
        .ledger;
    let t = ledger.t();

    let mut doc = person("ex:alice", None);
    doc["opts"] = json!({"shapes": person_shape_jsonld()});
    let err = fluree
        .insert(ledger.clone(), &doc)
        .await
        .expect_err("the body's inline shape requires ex:name");
    assert_shacl_violation(&err, "insert with body shapes");

    let err = fluree
        .update(
            ledger.clone(),
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "opts": {"shapes": person_shape_jsonld()},
                "insert": {"@id": "ex:bob", "@type": "ex:Person"}
            }),
        )
        .await
        .expect_err("the body's inline shape applies to an update too");
    assert_shacl_violation(&err, "update with body shapes");
    assert_eq!(head_t(&fluree, ledger_id).await, t, "nothing committed");

    // A conforming record commits under the same body shapes.
    let mut doc = person("ex:carol", Some("Carol"));
    doc["opts"] = json!({"shapes": person_shape_jsonld()});
    fluree.insert(ledger, &doc).await.expect("conforms");
}

/// The body's shapes follow the same policy-scope rule as the caller's own:
/// in a policy-scoped request they are refused.
#[tokio::test]
async fn inline_shapes_in_the_body_are_refused_in_a_policy_scoped_request() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = ledger_with_config(
        &fluree,
        "test/inline-shapes/body-policy:main",
        "<urn:cfg:main> rdf:type f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> . \
         <urn:cfg:policy> f:defaultAllow false .",
    )
    .await;
    let mut doc = person("ex:bob", Some("Bob"));
    doc["opts"] = json!({"shapes": person_shape_jsonld()});
    let err = fluree
        .insert(ledger, &doc)
        .await
        .expect_err("config policy defaults scope the request");
    assert_policy_scoped_refusal(&err);
}

/// `opts.uniqueProperties` in a transaction body is honored by the embedded
/// API too: a duplicate of an existing value is refused.
#[tokio::test]
async fn unique_properties_in_the_body_are_honored() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, "test/inline-unique/body:main"),
            &json!({"@context": {"ex": "http://example.org/ns/"}, "@id": "ex:u1", "ex:email": "a@x"}),
        )
        .await
        .unwrap()
        .ledger;
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "opts": {"uniqueProperties": ["http://example.org/ns/email"]},
                "@id": "ex:u2",
                "ex:email": "a@x"
            }),
        )
        .await
        .expect_err("the body's unique property is enforced");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "{err:?}"
    );
}
