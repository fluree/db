//! Inline uniqueness constraints via `opts.uniqueProperties`.
//!
//! Per-transaction property-IRI list that the uniqueness enforcer
//! treats as `f:enforceUnique true` for the duration of the
//! transaction. Unions additively with any `f:constraintsSource`
//! config; the list itself never persists into the ledger.

#![cfg(feature = "native")]

use crate::support::genesis_ledger;
use fluree_db_api::{CommitOpts, FlureeBuilder, IndexConfig, TransactionOptions, TxnOperation};
use fluree_db_transact::ir::TxnOpts;
use serde_json::json;

fn test_index_cfg() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 1_000_000,
    }
}

#[tokio::test]
async fn inline_unique_property_rejects_duplicate() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/duplicate:main").await;

    // Seed alice with email.
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":       "ex:alice",
        "ex:email":  "alice@example.org"
    });
    let r = fluree.insert(ledger, &seed).await.expect("seed alice");

    // Second tx: bob with the same email, marking ex:email inline-unique.
    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    let err = fluree
        .insert_with_opts(
            r.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("duplicate value on inline-unique property must be rejected");

    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

#[tokio::test]
async fn inline_unique_property_does_not_persist() {
    // After a tx that supplies opts.uniqueProperties, a later tx
    // without it must accept a duplicate value — the inline list
    // was transient.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/transient:main").await;

    // Tx 1: seed alice under the inline-unique constraint.
    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    let r1 = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:alice",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("seed alice with inline constraint");

    // Tx 2: bob with the same email, NO opts.uniqueProperties.
    // Without the inline list and no `f:constraintsSource` config,
    // there's no constraint to violate — must be accepted.
    fluree
        .insert(
            r1.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "alice@example.org"
            }),
        )
        .await
        .expect("without opts.uniqueProperties the prior tx's constraint must not apply");
}

#[tokio::test]
async fn inline_unique_property_accepts_unique_values() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/distinct:main").await;

    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id":       "ex:alice",
        "ex:email":  "alice@example.org"
    });
    let r = fluree.insert(ledger, &seed).await.expect("seed alice");

    let opts = TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    };
    fluree
        .insert_with_opts(
            r.ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:bob",
                "ex:email":  "bob@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect("distinct value must satisfy inline-unique constraint");
}

#[tokio::test]
async fn inline_property_unknown_to_ledger_fails_loudly() {
    // An inline IRI the ledger has never seen — neither
    // pre-tx nor introduced by this tx — must surface as a
    // parse error rather than silently becoming a no-op.
    // The non-strict `encode_iri` folds unknown IRIs into the
    // EMPTY namespace, which would match nothing and silently
    // disable enforcement; the strict path used here rejects
    // them up front so typos can't quietly weaken governance.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "test/inline-constraints/unknown:main").await;

    let opts = TxnOpts {
        unique_properties: Some(vec!["http://never-seen.org/ns/whatever".to_string()]),
        ..TxnOpts::default()
    };
    let err = fluree
        .insert_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id":       "ex:alice",
                "ex:email":  "alice@example.org"
            }),
            opts,
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("unknown inline-unique IRI must fail the tx, not silently no-op");

    let msg = err.to_string();
    assert!(
        msg.contains("opts.uniqueProperties") && msg.contains("never-seen.org"),
        "expected explicit unresolved-IRI diagnostic, got: {msg}"
    );
}

/// Two named graphs, registered in this order, with alice's email in the
/// second. A transaction that writes only to the second graph numbers it
/// first among its own graphs, so enforcement must translate to the ledger's
/// graph id to look in the right place.
async fn seed_email_in_second_graph(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id).await;
    fluree
        .stage_owned(ledger)
        .upsert_turtle(
            r#"
            GRAPH <http://example.org/g/a> {
                <http://example.org/ns/x> <http://example.org/ns/other> "1" .
            }
            GRAPH <http://example.org/g/b> {
                <http://example.org/ns/alice> <http://example.org/ns/email> "alice@example.org" .
            }
        "#,
        )
        .execute()
        .await
        .expect("seed two named graphs")
        .ledger
}

fn email_unique() -> TxnOpts {
    TxnOpts {
        unique_properties: Some(vec!["http://example.org/ns/email".to_string()]),
        ..TxnOpts::default()
    }
}

#[tokio::test]
async fn inline_unique_property_enforced_in_second_named_graph_sparql() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_email_in_second_graph(&fluree, "test/inline-constraints/second-graph-sparql:main")
            .await;
    let parsed = fluree_db_sparql::parse_sparql(
        r#"INSERT DATA { GRAPH <http://example.org/g/b> {
            <http://example.org/ns/bob> <http://example.org/ns/email> "alice@example.org" } }"#,
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, email_unique())
        .expect("lower SPARQL UPDATE");
    let err = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect_err("duplicate email in the second named graph must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

/// Inline uniqueness constraints from the programmatic options apply to a
/// Turtle insert whether or not the document has a `GRAPH` block (plain
/// Turtle and TriG take different parse paths), and are refused in a
/// policy-scoped Turtle request as on the other surfaces.
#[tokio::test]
async fn inline_unique_property_applies_to_turtle_with_and_without_graph_blocks() {
    let fluree = FlureeBuilder::memory().build_memory();
    let email = "http://example.org/ns/email";
    let unique = || TxnOpts {
        unique_properties: Some(vec![email.to_string()]),
        ..TxnOpts::default()
    };
    let seed = json!({"@context": {"ex": "http://example.org/ns/"}, "@id": "ex:alice", "ex:email": "alice@example.org"});
    for (i, (what, turtle)) in [
        (
            "plain Turtle",
            "@prefix ex: <http://example.org/ns/> . ex:bob ex:email \"alice@example.org\" .",
        ),
        (
            "TriG",
            "@prefix ex: <http://example.org/ns/> .
             GRAPH ex:other { ex:carol ex:p 1 }
             ex:bob ex:email \"alice@example.org\" .",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let ledger_id = format!("test/inline-constraints/turtle-{i}:main");
        let t = fluree
            .insert(genesis_ledger(&fluree, &ledger_id).await, &seed)
            .await
            .expect("seed")
            .ledger
            .t();
        let err = fluree
            .graph(&ledger_id)
            .transact()
            .insert_turtle(turtle)
            .txn_opts(unique())
            .commit()
            .await
            .expect_err(what);
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
                )
            ),
            "{what}: {err:?}"
        );
        assert_eq!(
            fluree.ledger(&ledger_id).await.unwrap().t(),
            t,
            "{what}: nothing committed"
        );
    }

    // Under a request policy the inline constraints are refused, as on the
    // other surfaces.
    let ledger_id = "test/inline-constraints/turtle-policy:main";
    let ledger = fluree
        .insert(genesis_ledger(&fluree, ledger_id).await, &seed)
        .await
        .expect("seed")
        .ledger;
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
    drop(ledger);
    let err = fluree
        .graph(ledger_id)
        .transact()
        .insert_turtle(
            "@prefix ex: <http://example.org/ns/> . ex:bob ex:email \"bob@example.org\" .",
        )
        .txn_opts(unique())
        .policy(policy)
        .commit()
        .await
        .expect_err("inline uniqueness in a policy-scoped Turtle request");
    assert_unique_properties_refused(&err, "plain Turtle under a request policy");
}

#[tokio::test]
async fn inline_unique_property_enforced_in_second_named_graph_jsonld() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_email_in_second_graph(&fluree, "test/inline-constraints/second-graph-jsonld:main")
            .await;
    let err = fluree
        .update_with_opts(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "graph": "http://example.org/g/b",
                "insert": {"@id": "ex:bob", "ex:email": "alice@example.org"}
            }),
            email_unique(),
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("duplicate email in the second named graph must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

/// `# PRAGMA unique-properties` is the SPARQL UPDATE twin of
/// `opts.uniqueProperties`, applied wherever the update is lowered. The same
/// duplicate without the pragma commits, so the rejection is the pragma's.
#[tokio::test]
async fn inline_unique_property_via_sparql_pragma() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "test/inline-constraints/sparql-pragma:main";
    fluree.create_ledger(alias).await.expect("create ledger");
    let insert = |pragma: &str, subject: &str| {
        format!(
            "{pragma}\nPREFIX ex: <http://example.org/ns/>\n\
             INSERT DATA {{ ex:{subject} ex:email \"alice@example.org\" }}"
        )
    };
    let update = |sparql: String| {
        let fluree = &fluree;
        async move {
            fluree
                .graph(alias)
                .transact()
                .sparql_update(&sparql)
                .commit()
                .await
        }
    };

    update(insert("", "alice")).await.expect("seed alice");
    update(insert("", "carol"))
        .await
        .expect("without the pragma a duplicate commits");
    let err = update(insert("# PRAGMA unique-properties: ex:email", "bob"))
        .await
        .expect_err("duplicate value on a pragma-unique property must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

/// An embedder that lowers the update itself and stages the `Txn` gets the
/// pragma enforced too, not just the builder's `sparql_update` path.
#[tokio::test]
async fn inline_unique_property_via_sparql_pragma_lowered_by_caller() {
    let fluree = FlureeBuilder::memory().build_memory();
    let alias = "test/inline-constraints/sparql-pragma-lowered:main";
    fluree.create_ledger(alias).await.expect("create ledger");
    fluree
        .graph(alias)
        .transact()
        .sparql_update(
            "PREFIX ex: <http://example.org/ns/>\n\
             INSERT DATA { ex:alice ex:email \"alice@example.org\" }",
        )
        .commit()
        .await
        .expect("seed alice");

    let parsed = fluree_db_sparql::parse_sparql(
        "# PRAGMA unique-properties: ex:email\n\
         PREFIX ex: <http://example.org/ns/>\n\
         INSERT DATA { ex:bob ex:email \"alice@example.org\" }",
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let ledger = fluree.ledger(alias).await.expect("load ledger");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect("lower SPARQL UPDATE");
    let err = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect_err("duplicate value on a pragma-unique property must be rejected");
    assert!(
        err.to_string().to_lowercase().contains("unique"),
        "expected uniqueness violation error, got: {err}"
    );
}

// =============================================================================
// Inline uniqueness in a policy-scoped request: a current limitation, as for
// inline shapes (support in policy-scoped requests is a follow-up)
// =============================================================================

fn assert_unique_properties_refused(err: &fluree_db_api::ApiError, what: &str) {
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UnsupportedFeature(_)
            )
        ),
        "{what}: expected the policy-scoped refusal, got {err:?}"
    );
    assert!(
        err.to_string().contains(
            "inline uniqueness constraints (opts.uniqueProperties) are not supported in a \
             policy-scoped request"
        ),
        "{what}: {err}"
    );
    assert_eq!(err.status_code(), 400, "{what}: {err}");
}

/// Inline uniqueness constraints are refused in a request that runs under a
/// policy context (from the request, or from policy defaults in the ledger
/// config), whether they come from the programmatic options, the body's
/// `opts` or a SPARQL `# PRAGMA unique-properties`, and in a `Transaction`
/// under request governance; nothing commits. Without a policy context they
/// apply (`inline_unique_property_rejects_duplicate`).
#[tokio::test]
async fn inline_unique_properties_are_refused_in_a_policy_scoped_request() {
    let fluree = FlureeBuilder::memory().build_memory();
    let email = "http://example.org/ns/email";
    let doc = |opts: bool| {
        let mut doc = json!({
            "@context": {"ex": "http://example.org/ns/"},
            "@id": "ex:bob",
            "ex:email": "bob@example.org"
        });
        if opts {
            doc["opts"] = json!({"uniqueProperties": [email]});
        }
        doc
    };
    let programmatic = || TxnOpts {
        unique_properties: Some(vec![email.to_string()]),
        ..TxnOpts::default()
    };
    let pragma = format!(
        "# PRAGMA unique-properties: <{email}>
         PREFIX ex: <http://example.org/ns/>
         INSERT DATA {{ ex:bob ex:email \"bob@example.org\" }}"
    );
    let seed = json!({"@context": {"ex": "http://example.org/ns/"}, "@id": "ex:seed", "ex:email": "seed@example.org"});
    // A request policy that allows the write.
    let allow_all = || fluree_db_api::GovernanceOptions {
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
    };

    // A policy context from the request: programmatic opts, body opts, and
    // the SPARQL pragma.
    for (i, what) in ["programmatic opts", "body opts", "SPARQL pragma"]
        .into_iter()
        .enumerate()
    {
        let ledger_id = format!("test/inline-constraints/policy-request-{i}:main");
        let ledger = fluree
            .insert(genesis_ledger(&fluree, &ledger_id).await, &seed)
            .await
            .expect("seed")
            .ledger;
        let t = ledger.t();
        let policy = fluree_db_api::build_policy_context(
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            Some(ledger.novelty.as_ref()),
            ledger.t(),
            &allow_all(),
        )
        .await
        .expect("policy context");
        let result = match what {
            "programmatic opts" => fluree
                .stage_owned(ledger)
                .txn_opts(programmatic())
                .insert(&doc(false))
                .policy(policy)
                .execute()
                .await
                .map(|_| ()),
            "body opts" => fluree
                .stage_owned(ledger)
                .insert(&doc(true))
                .policy(policy)
                .execute()
                .await
                .map(|_| ()),
            _ => {
                drop(ledger);
                fluree
                    .graph(&ledger_id)
                    .transact()
                    .sparql_update(&pragma)
                    .policy(policy)
                    .commit()
                    .await
                    .map(|_| ())
            }
        };
        let err = result.expect_err(what);
        assert_unique_properties_refused(&err, what);
        assert_eq!(
            fluree.ledger(&ledger_id).await.unwrap().t(),
            t,
            "{what}: nothing committed"
        );
    }

    // A `Transaction` under request governance: body opts, and the pragma.
    for (i, (what, operation)) in [
        ("transaction body opts", TxnOperation::Insert(doc(true))),
        (
            "transaction SPARQL pragma",
            TxnOperation::SparqlUpdate(pragma.clone(), None),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let ledger_id = format!("test/inline-constraints/policy-transaction-{i}:main");
        let t = fluree
            .insert(genesis_ledger(&fluree, &ledger_id).await, &seed)
            .await
            .expect("seed")
            .ledger
            .t();
        let mut transaction = fluree
            .begin_transaction(
                &ledger_id,
                TransactionOptions {
                    governance: allow_all(),
                    ..Default::default()
                },
            )
            .await
            .expect("begin");
        let err = transaction.stage(operation).await.expect_err(what);
        assert_unique_properties_refused(&err, what);
        drop(transaction);
        assert_eq!(
            fluree.ledger(&ledger_id).await.unwrap().t(),
            t,
            "{what}: nothing committed"
        );
    }

    // Policy defaults in the ledger config scope every request.
    let ledger_id = "test/inline-constraints/policy-config:main";
    let ledger = fluree
        .stage_owned(genesis_ledger(&fluree, ledger_id).await)
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <urn:fluree:{ledger_id}#config> {{
               <urn:cfg:main> a f:LedgerConfig ; f:policyDefaults <urn:cfg:policy> .
               <urn:cfg:policy> f:defaultAllow false .
             }}"
        ))
        .execute()
        .await
        .expect("config")
        .ledger;
    let t = ledger.t();
    let err = fluree
        .insert_with_opts(
            ledger,
            &doc(false),
            programmatic(),
            CommitOpts::default(),
            &test_index_cfg(),
        )
        .await
        .expect_err("config policy defaults scope the request");
    assert_unique_properties_refused(&err, "config policy defaults");
    assert_eq!(
        fluree.ledger(ledger_id).await.unwrap().t(),
        t,
        "nothing committed"
    );
}
