//! SPARQL 1.2 / RDF 1.2 edge-annotation integration tests.
//!
//! Covers both the JSON-LD `@annotation` insert surface and the
//! SPARQL 1.2 annotation tail / `~` reifier / `rdf:reifies` query
//! surface that lower to the same underlying IR.
//!
//! See `docs/concepts/edge-annotations.md` "SPARQL 1.2 / RDF 1.2
//! surface" for the user-facing surface contract and the
//! per-operation blank-node / variable rules.

mod support;

use fluree_db_api::FlureeBuilder;
use serde_json::{json, Value as JsonValue};
use support::{genesis_ledger, MemoryFluree, MemoryLedger};

fn ctx() -> JsonValue {
    json!({
        "ex": "http://example.org/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    })
}

async fn seed_alice_engineer(ledger_id: &str) -> (MemoryFluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:worksFor": {
            "@id": "ex:acme",
            "@annotation": { "ex:role": "Engineer" }
        }
    });
    let committed = fluree.insert(ledger0, &txn).await.expect("seed insert");
    (fluree, committed.ledger)
}

#[tokio::test]
async fn sparql_annotation_block_inline_query_returns_role() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/inline").await;

    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql annotation query");
    let rows = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let bindings = rows["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone();
    assert_eq!(bindings.len(), 1, "one annotation → one row: {bindings:#?}");
    let role = bindings[0]["role"]["value"].as_str().expect("role value");
    assert_eq!(role, "Engineer");
}

#[tokio::test]
async fn sparql_named_var_reifier_binds_annotation_subject() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/named-var").await;

    // The reifier var `?ann` is bindable in SELECT — but it's a
    // synthetic blank-node-like subject internally (the JSON-LD insert
    // was anonymous), so SPARQL should still surface it as a value.
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:alice ex:worksFor ex:acme ~ ?ann {| ex:role ?role |} .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("sparql named-var reifier query");
    let bindings = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));
}

#[tokio::test]
async fn sparql_rdf_reifies_form_returns_reified_edge() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/reifies").await;

    // Annotation-rooted: filter by metadata, return the reified base
    // edge endpoints. Sibling triple `?ann ex:role "Engineer"` lives
    // outside the AnnotationTarget and joins on `?ann` in the executor.
    let sparql = r#"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ex: <http://example.org/>
        SELECT ?person ?org WHERE {
          ?ann rdf:reifies <<( ?person ex:worksFor ?org )>> .
          ?ann ex:role "Engineer" .
        }
    "#;
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("rdf:reifies query");
    let bindings = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone();
    assert_eq!(bindings.len(), 1, "exactly one reified edge: {bindings:#?}");
    assert!(
        bindings[0]["person"]["value"]
            .as_str()
            .map(|s| s.ends_with("alice") || s == "ex:alice")
            .unwrap_or(false),
        "person should bind to ex:alice; got {:?}",
        bindings[0]["person"]
    );
    assert!(
        bindings[0]["org"]["value"]
            .as_str()
            .map(|s| s.ends_with("acme") || s == "ex:acme")
            .unwrap_or(false),
        "org should bind to ex:acme; got {:?}",
        bindings[0]["org"]
    );
}

#[tokio::test]
async fn sparql_bare_triple_unaffected_by_annotation_presence() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/bare").await;
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT ?person ?org WHERE {
          ?person ex:worksFor ?org .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("bare-triple query");
    let bindings = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone();
    assert_eq!(
        bindings.len(),
        1,
        "RDF set semantics for bare triples preserved: {bindings:#?}"
    );
}

// =============================================================================
// M4.4 — SPARQL UPDATE annotation round-trips
// =============================================================================

use fluree_db_api::LedgerState;
use fluree_db_transact::{NamespaceRegistry, Txn, TxnOpts};

fn lower_update(ledger: &LedgerState, sparql: &str) -> Txn {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL UPDATE parse failed: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("AST");
    let mut ns = NamespaceRegistry::from_db(&ledger.snapshot);
    fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect("lower SPARQL UPDATE")
}

#[tokio::test]
async fn sparql_insert_data_with_anonymous_annotation_round_trips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-update/insert-anon";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let update = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
        }
    "#;
    let txn = lower_update(&ledger0, update);
    let result = fluree
        .stage_owned(ledger0)
        .txn(txn)
        .execute()
        .await
        .expect("INSERT DATA with annotation");
    let ledger = result.ledger;

    // Read back via SPARQL annotation query.
    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, select)
        .await
        .expect("read-back query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));
}

#[tokio::test]
async fn sparql_insert_data_with_named_blank_reifier_round_trips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-update/insert-named-blank";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let update = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          ex:alice ex:worksFor ex:acme ~ _:ann {| ex:role "Manager" ; ex:since "2024" |} .
        }
    "#;
    let txn = lower_update(&ledger0, update);
    let result = fluree
        .stage_owned(ledger0)
        .txn(txn)
        .execute()
        .await
        .expect("INSERT DATA with named blank reifier");
    let ledger = result.ledger;

    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role ?since WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role ; ex:since ?since |} .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, select)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "got {bindings:#?}");
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Manager"));
    assert_eq!(bindings[0]["since"]["value"].as_str(), Some("2024"));
}

#[tokio::test]
async fn sparql_insert_data_with_named_iri_reifier_round_trips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-update/insert-named-iri";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let update = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          ex:alice ex:worksFor ex:acme ~ ex:employment {| ex:role "Engineer" |} .
        }
    "#;
    let txn = lower_update(&ledger0, update);
    let result = fluree
        .stage_owned(ledger0)
        .txn(txn)
        .execute()
        .await
        .expect("INSERT DATA with named IRI reifier");
    let ledger = result.ledger;

    let inline = r"
        PREFIX ex: <http://example.org/>
        SELECT ?ann ?role WHERE {
          ex:alice ex:worksFor ex:acme ~ ?ann {| ex:role ?role |} .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, inline)
        .await
        .expect("inline query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "got {bindings:#?}");
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));
    assert!(
        matches!(
            bindings[0]["ann"]["value"].as_str(),
            Some("ex:employment" | "http://example.org/employment")
        ),
        "expected explicit IRI reifier binding, got {:?}",
        bindings[0]["ann"]
    );

    let bare_reifier = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:employment ex:role ?role .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, bare_reifier)
        .await
        .expect("bare reifier query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "got {bindings:#?}");
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));

    let reifies = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:employment rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
          ex:employment ex:role ?role .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger, reifies)
        .await
        .expect("rdf:reifies query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "got {bindings:#?}");
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));
}

#[tokio::test]
async fn sparql_delete_data_blank_reifier_is_rejected() {
    // SPARQL §3.1.3: blank nodes are not allowed in DELETE DATA.
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/delete-blank-rej")
    };
    let update = r#"
        PREFIX ex: <http://example.org/>
        DELETE DATA {
          ex:alice ex:worksFor ex:acme ~ _:ann {| ex:role "Engineer" |} .
        }
    "#;
    let parsed = fluree_db_sparql::parse_sparql(update);
    assert!(
        !parsed.has_errors(),
        "parse should succeed: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("blank reifier in DELETE DATA must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("blank-node reifier in DELETE DATA"),
        "expected blank-node-rejection diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_insert_data_anonymous_in_delete_data_is_rejected() {
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/anon-in-delete-data")
    };
    let update = r#"
        PREFIX ex: <http://example.org/>
        DELETE DATA {
          ex:alice ex:worksFor ex:acme {| ex:role "Engineer" |} .
        }
    "#;
    let parsed = fluree_db_sparql::parse_sparql(update);
    assert!(!parsed.has_errors());
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("anonymous block in DELETE DATA must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("anonymous annotation block ({| |}) in DELETE DATA"),
        "expected anonymous-block-rejection, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_reifies_hidden_in_annotation_block_body_is_rejected() {
    // The pre-expansion firewall must walk into `{| ... |}` block bodies
    // too — otherwise expansion would emit a synthetic
    // `(reifier f:reifiesSubject ex:evil)` triple, smuggling a
    // system-controlled predicate past the top-level check.
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/reifies-bypass")
    };
    let update = r"
        PREFIX ex: <http://example.org/>
        PREFIX f:  <https://ns.flur.ee/db#>
        INSERT DATA {
          ex:alice ex:worksFor ex:acme {| f:reifiesSubject ex:evil |} .
        }
    ";
    let parsed = fluree_db_sparql::parse_sparql(update);
    assert!(
        !parsed.has_errors(),
        "parse should succeed: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("f:reifies* hidden in block body must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("user-authored f:reifies"),
        "expected reifies firewall diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_user_authored_reifies_in_insert_data_is_rejected() {
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/user-reifies")
    };
    let update = r"
        PREFIX ex: <http://example.org/>
        PREFIX f:  <https://ns.flur.ee/db#>
        INSERT DATA {
          _:ann f:reifiesSubject ex:alice .
        }
    ";
    let parsed = fluree_db_sparql::parse_sparql(update);
    assert!(!parsed.has_errors());
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("user-authored f:reifiesSubject must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("user-authored f:reifies"),
        "expected reifies firewall diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_select_star_does_not_leak_blank_node_reifier_named() {
    // `~ _:ann` registers the reifier under the literal name `_:ann`.
    // Per SPARQL §4.1.4 blank nodes in WHERE are non-distinguished
    // variables and must not appear in `SELECT *` results.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/star-blank-named").await;
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT * WHERE {
          ex:alice ex:worksFor ex:acme ~ _:ann {| ex:role ?role |} .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query");
    let head = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let names: Vec<String> = head["head"]["vars"]
        .as_array()
        .expect("head.vars")
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        names,
        vec!["role"],
        "blank-node reifier must not be projected; got {names:?}"
    );
}

#[tokio::test]
async fn sparql_select_star_does_not_leak_blank_node_reifier_reifies_form() {
    // The `_:ann rdf:reifies <<( ... )>>` form goes through
    // `lower_subject()`, which registers the blank node under `_:ann`.
    // Same §4.1.4 rule applies.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/star-blank-reifies").await;
    let sparql = r"
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ex:  <http://example.org/>
        SELECT * WHERE {
          _:ann rdf:reifies <<( ex:alice ex:worksFor ex:acme )>> .
          _:ann ex:role ?role .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("query");
    let head = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let names: Vec<String> = head["head"]["vars"]
        .as_array()
        .expect("head.vars")
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        names,
        vec!["role"],
        "blank-node reifier (rdf:reifies form) must not be projected; got {names:?}"
    );
}

#[tokio::test]
async fn sparql_select_star_does_not_leak_anonymous_reifier() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/star-leak").await;

    // SELECT * over an anonymous annotation block must NOT expose the
    // synthetic anonymous-reifier variable. Only ?role should appear.
    let sparql = r"
        PREFIX ex: <http://example.org/>
        SELECT * WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("select-star query");
    let head = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let vars = head["head"]["vars"]
        .as_array()
        .expect("head.vars array")
        .clone();
    let names: Vec<String> = vars
        .iter()
        .map(|v| v.as_str().unwrap_or("").to_string())
        .collect();
    assert_eq!(
        names,
        vec!["role"],
        "only user-visible vars should appear in SELECT *; got: {names:?}"
    );
    // And no binding row mentions an internal-prefixed key.
    let bindings = head["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    for b in &bindings {
        let obj = b.as_object().expect("binding obj");
        for key in obj.keys() {
            assert!(
                !key.starts_with("#") && !key.contains("__ann_"),
                "binding key looks internal: {key:?}"
            );
        }
    }
}

#[tokio::test]
async fn sparql_modify_insert_template_with_annotation_round_trips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-update/modify-insert";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Seed a base subject so WHERE binds something.
    let seed = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          ex:alice ex:name "Alice" .
        }
    "#;
    let seed_txn = lower_update(&ledger0, seed);
    let result = fluree
        .stage_owned(ledger0)
        .txn(seed_txn)
        .execute()
        .await
        .expect("seed insert");
    let ledger = result.ledger;

    // INSERT WHERE with annotation in template — per-solution blank-node
    // reifier minted by expansion.
    let update = r#"
        PREFIX ex: <http://example.org/>
        INSERT { ?p ex:worksFor ex:acme {| ex:role "Engineer" |} . }
        WHERE  { ?p ex:name "Alice" . }
    "#;
    let txn = lower_update(&ledger, update);
    let result2 = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("INSERT WHERE with annotation template");
    let ledger2 = result2.ledger;

    let select = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
        }
    ";
    let bindings = support::query_sparql(&fluree, &ledger2, select)
        .await
        .expect("read-back")
        .to_sparql_json(&ledger2.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .clone();
    assert_eq!(bindings.len(), 1, "got {bindings:#?}");
    assert_eq!(bindings[0]["role"]["value"].as_str(), Some("Engineer"));
}

#[tokio::test]
async fn sparql_annotation_query_returns_zero_for_unmatched_metadata() {
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/zero").await;
    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?person WHERE {
          ?person ex:worksFor ex:acme {| ex:role "Manager" |} .
        }
    "#;
    let result = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect("filtered-annotation query");
    let bindings = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings array")
        .clone();
    assert!(
        bindings.is_empty(),
        "no matching annotation → zero rows: {bindings:#?}"
    );
}

// =====================================================================
// Literal-object annotation parity (RDF 1.2)
// =====================================================================

/// SPARQL parity for a plain-string annotated literal. The SPARQL
/// surface uses Turtle-star-style `{| ... |}` after the literal; the
/// lowering attaches `Explicit(xsd:string)` to the base edge's dtc.
#[tokio::test]
async fn sparql_annotation_on_plain_string_literal_returns_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-lit/plain";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:name": {
            "@value": "Alice",
            "@annotation": { "ex:source": "hr" }
        }
    });
    let r1 = fluree.insert(ledger0, &txn).await.expect("seed");

    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?source WHERE {
          ex:alice ex:name "Alice" {| ex:source ?source |} .
        }
    "#;
    let result = support::query_sparql(&fluree, &r1.ledger, sparql)
        .await
        .expect("sparql annotated plain-literal query");
    let bindings = result
        .to_sparql_json(&r1.ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .cloned()
        .expect("bindings");
    assert_eq!(bindings.len(), 1, "one annotation → one row: {bindings:#?}");
    assert_eq!(bindings[0]["source"]["value"].as_str(), Some("hr"));
}

/// SPARQL parity for a language-tagged annotated literal. The lowering
/// attaches `LangTag("fr")` to the base edge's dtc so the f:reifiesObject
/// lookup constrains on language.
#[tokio::test]
async fn sparql_annotation_on_lang_tagged_literal_returns_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-lit/lang";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:label": {
            "@value": "chat",
            "@language": "fr",
            "@annotation": { "ex:source": "lexicon" }
        }
    });
    let r1 = fluree.insert(ledger0, &txn).await.expect("seed");

    let sparql = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?source WHERE {
          ex:alice ex:label "chat"@fr {| ex:source ?source |} .
        }
    "#;
    let result = support::query_sparql(&fluree, &r1.ledger, sparql)
        .await
        .expect("sparql annotated lang-literal query");
    let bindings = result
        .to_sparql_json(&r1.ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .cloned()
        .expect("bindings");
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0]["source"]["value"].as_str(), Some("lexicon"));
}

/// Wrong-language SPARQL queries miss the annotation. With an `@fr`
/// annotation in place, querying for `"chat"@en` must return zero
/// rows — the synthesized `f:reifiesObject` lookup's `LangTag` dtc
/// blocks the cross-language match.
///
/// (A two-language-coexistence regression would also be valuable but
/// is out of scope here: Fluree's insert path for same-lexical literals
/// across language tags has separate behavior tracked elsewhere.
/// Lower-layer coverage for the dtc propagation lives in
/// `fluree-db-sparql` `m43_annotated_lang_tagged_literal_object_carries_lang_dtc`
/// and `fluree-db-query`
/// `expand_edge_annotation_propagates_lang_tag_dtc_to_reifies_object`.)
#[tokio::test]
async fn sparql_annotation_lang_tag_blocks_wrong_language_match() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/sparql-ann-lit/lang-no-cross";
    let ledger0 = genesis_ledger(&fluree, ledger_id);
    let txn = json!({
        "@context": ctx(),
        "@id": "ex:alice",
        "ex:label": {
            "@value": "chat",
            "@language": "fr",
            "@annotation": { "ex:source": "fr-lexicon" }
        }
    });
    let r1 = fluree.insert(ledger0, &txn).await.expect("seed fr");

    let sparql_en = r#"
        PREFIX ex: <http://example.org/>
        SELECT ?source WHERE {
          ex:alice ex:label "chat"@en {| ex:source ?source |} .
        }
    "#;
    let result = support::query_sparql(&fluree, &r1.ledger, sparql_en)
        .await
        .expect("sparql wrong-lang query");
    let bindings = result
        .to_sparql_json(&r1.ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .cloned()
        .expect("bindings");
    assert!(
        bindings.is_empty(),
        "@en query against an @fr-only ledger must return zero rows: {bindings:#?}"
    );
}

#[tokio::test]
async fn sparql_select_with_reifies_property_path_is_rejected() {
    // The read-side firewall must walk `Pattern::PropertyPath`, not
    // just `Pattern::Triple`. A transitive path over a system
    // predicate like `?s f:reifiesSubject+ ?o` would otherwise let
    // the user enumerate the reifies bundle through a path
    // shorthand, defeating the same protection the bare-triple
    // case blocks.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/path-firewall").await;
    let sparql = r"
        PREFIX f: <https://ns.flur.ee/db#>
        SELECT ?s ?o WHERE {
          ?s f:reifiesSubject+ ?o .
        }
    ";
    let err = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect_err("property-path over f:reifies* must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("system-controlled"),
        "expected reifies firewall diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_quoted_triple_with_annotation_tail_is_rejected() {
    // Legacy `<< s p o >> ...` quoted-triple form is the f:t/f:op
    // metadata-binding shape; it has no representation for an
    // RDF 1.2 annotation tail (`{| ... |}`). Silently dropping the
    // tail would lose user intent — reject explicitly.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/quoted-triple-tail").await;
    let sparql = r"
        PREFIX ex: <http://example.org/>
        PREFIX f:  <https://ns.flur.ee/db#>
        SELECT ?t WHERE {
          << ex:alice ex:worksFor ?o >> f:t ?t {| ex:role ?role |} .
        }
    ";
    let err = support::query_sparql(&fluree, &ledger, sparql)
        .await
        .expect_err("quoted-triple subject + annotation tail must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("annotation tail") || msg.contains("quoted-triple"),
        "expected quoted-triple-annotation rejection, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_update_quoted_triple_with_annotation_tail_does_not_panic() {
    // Mirrors the read-side test but exercises the UPDATE path.
    // `<<:s :p :o>> ~ {| :ann :v |}` used to hit
    // `unreachable!()` inside `expand_annotated_triples` because
    // a QuotedTriple subject reached `subject_to_object`. The
    // expansion path must reject this explicitly with an
    // `UnsupportedFeature` error before that helper is called.
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/quoted-triple-tail")
    };
    let update = r"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          << ex:alice ex:worksFor ex:acme >> ex:ann ex:v {| ex:role ex:eng |} .
        }
    ";
    let parsed = fluree_db_sparql::parse_sparql(update);
    if parsed.has_errors() {
        // Parser may already reject this shape; that's also acceptable
        // — the point of the test is "no panic in the lowering path".
        return;
    }
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("quoted-triple subject + annotation tail must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("quoted-triple") || msg.contains("annotation tail"),
        "expected UnsupportedFeature on quoted-triple + tail, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_values_bound_reifies_predicate_does_not_leak() {
    // READ-1 regression: the read-side firewall rejects f:reifies* as a
    // predicate KEY, but a user can also smuggle the IRI in as VALUES
    // *data* bound to a predicate variable, then scan `?s ?p ?o`.
    // Without the firewall covering VALUES data this leaks the internal
    // f:reifiesSubject bundle as ordinary RDF. The query must either be
    // rejected or return zero f:reifies* rows (no includeSystemFacts).
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/values-pred-leak").await;
    let sparql = r"
        SELECT ?s ?o WHERE {
          VALUES ?p { <https://ns.flur.ee/db#reifiesSubject> }
          ?s ?p ?o .
        }
    ";
    let result = support::query_sparql(&fluree, &ledger, sparql).await;
    match result {
        Err(_) => { /* rejected by the firewall — correct */ }
        Ok(r) => {
            let bindings = r.to_sparql_json(&ledger.snapshot).expect("sparql json")["results"]
                ["bindings"]
                .as_array()
                .expect("bindings array")
                .clone();
            assert!(
                bindings.is_empty(),
                "VALUES-bound f:reifiesSubject predicate must not leak \
                 system facts; got {bindings:#?}"
            );
        }
    }
}

#[tokio::test]
async fn jsonld_values_bound_reifies_predicate_does_not_leak() {
    // READ-1 regression (JSON-LD): same leak as the SPARQL case but via
    // a JSON-LD `values` clause binding a variable predicate to the
    // f:reifiesSubject IRI. The key-only firewall misses the IRI in
    // VALUES data. Must be rejected or return zero f:reifies* rows.
    let (fluree, ledger) = seed_alice_engineer("it/jsonld-ann/values-pred-leak").await;
    let query = json!({
        "@context": { "ex": "http://example.org/", "f": "https://ns.flur.ee/db#" },
        "select": ["?s", "?o"],
        "where": { "@id": "?s", "?p": "?o" },
        "values": ["?p", [{ "@id": "f:reifiesSubject" }]]
    });
    match support::query_jsonld(&fluree, &ledger, &query).await {
        Err(_) => { /* rejected by the firewall — correct */ }
        Ok(r) => {
            let bindings = r.to_sparql_json(&ledger.snapshot).expect("sparql json")["results"]
                ["bindings"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            // Any row binding ?o to ex:alice means the f:reifiesSubject
            // bundle leaked.
            let leaked = bindings.iter().any(|row| {
                row.get("o")
                    .and_then(|v| v.get("value"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.contains("alice"))
                    .unwrap_or(false)
            });
            assert!(
                !leaked,
                "JSON-LD VALUES-bound f:reifiesSubject must not leak system facts; got {bindings:#?}"
            );
        }
    }
}

#[tokio::test]
async fn sparql_update_langstring_annotation_hydrates_via_jsonld() {
    // WRIT-1 regression: a SPARQL UPDATE annotation on a language-tagged
    // literal object must emit f:reifiesLang so the stored EdgeKey
    // carries lang=Some — matching the base edge's EdgeKey. Without it
    // the decoded EdgeKey diverges (lang=None) and the annotation
    // silently vanishes from JSON-LD @annotation hydration. SPARQL
    // {| |} read-back reads the same flakes so it can't catch this;
    // JSON-LD hydration is the path that breaks.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "it/sparql-ann-update/langstring-hydrate");

    let update = r#"
        PREFIX ex: <http://example.org/>
        INSERT DATA {
          ex:alice ex:label "chat"@fr {| ex:source "lexicon" |} .
        }
    "#;
    let txn = lower_update(&ledger0, update);
    let ledger = fluree
        .stage_owned(ledger0)
        .txn(txn)
        .execute()
        .await
        .expect("INSERT DATA with langString annotation")
        .ledger;

    let query = json!({
        "@context": ctx(),
        "select": {"?s": ["*"]},
        "where": {"@id": "?s", "ex:label": "?o"}
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("hydrate alice");
    let arr = rows.as_array().expect("array");
    assert_eq!(arr.len(), 1, "one subject row: {arr:#?}");

    let label = arr[0]
        .as_object()
        .and_then(|o| {
            o.get("ex:label")
                .or_else(|| o.get("http://example.org/label"))
        })
        .expect("ex:label present");
    let value_obj = label
        .as_object()
        .or_else(|| {
            label
                .as_array()
                .and_then(|a| a.first().and_then(|v| v.as_object()))
        })
        .expect("ex:label literal value object");

    assert_eq!(
        value_obj.get("@value").and_then(|v| v.as_str()),
        Some("chat"),
        "value object must carry the literal: {value_obj:#?}"
    );
    assert_eq!(
        value_obj.get("@language").and_then(|v| v.as_str()),
        Some("fr"),
        "value object must carry the language tag: {value_obj:#?}"
    );
    let ann = value_obj
        .get("@annotation")
        .expect("@annotation must hydrate on the langString edge (WRIT-1)");
    let ann_obj = ann
        .as_object()
        .or_else(|| {
            ann.as_array()
                .and_then(|a| a.first().and_then(|v| v.as_object()))
        })
        .expect("@annotation body object");
    assert_eq!(
        ann_obj
            .get("ex:source")
            .or_else(|| ann_obj.get("http://example.org/source"))
            .and_then(|v| v.as_str()),
        Some("lexicon"),
        "annotation body must surface ex:source: {ann_obj:#?}"
    );
}

/// Count the SELECT bindings a SPARQL query returns against `ledger`.
async fn sparql_row_count(fluree: &MemoryFluree, ledger: &MemoryLedger, sparql: &str) -> usize {
    support::query_sparql(fluree, ledger, sparql)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json")["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .len()
}

#[tokio::test]
async fn sparql_delete_where_annotation_retracts_base_edge_and_annotation() {
    // TEST-1: SPARQL DELETE WHERE with an annotation tail retracts the
    // whole matched pattern — base edge, f:reifies* bundle, and body.
    // The annotation syntax asserts the base triple too, so deleting the
    // pattern deletes it; the bundle leaves no orphan that the
    // {| |} read-back could still surface.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/delete-where").await;

    let ann_q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE { ex:alice ex:worksFor ex:acme {| ex:role ?role |} . }
    ";
    let base_q = r"
        PREFIX ex: <http://example.org/>
        SELECT ?o WHERE { ex:alice ex:worksFor ?o . }
    ";
    assert_eq!(
        sparql_row_count(&fluree, &ledger, ann_q).await,
        1,
        "annotation present before delete"
    );

    let del = r"
        PREFIX ex: <http://example.org/>
        DELETE WHERE { ex:alice ex:worksFor ex:acme {| ex:role ?role |} . }
    ";
    let txn = lower_update(&ledger, del);
    let ledger = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("DELETE WHERE annotation")
        .ledger;

    assert_eq!(
        sparql_row_count(&fluree, &ledger, ann_q).await,
        0,
        "annotation (body + f:reifies* bundle) must be retracted"
    );
    assert_eq!(
        sparql_row_count(&fluree, &ledger, base_q).await,
        0,
        "base edge is part of the matched annotation pattern, so it is retracted too"
    );
}

#[tokio::test]
async fn sparql_delete_template_anonymous_annotation_block_is_rejected() {
    // TEST-1: an anonymous {| |} block in a DELETE template has no
    // reifier to bind from the WHERE clause, so it must be rejected with
    // a clear message pointing at the named-reifier form.
    let ledger0 = {
        let fluree = FlureeBuilder::memory().build_memory();
        genesis_ledger(&fluree, "it/sparql-ann-update/delete-template-anon")
    };
    let update = r"
        PREFIX ex: <http://example.org/>
        DELETE { ex:alice ex:worksFor ex:acme {| ex:role ?r |} }
        WHERE  { ex:alice ex:worksFor ex:acme ~ ?ann {| ex:role ?r |} }
    ";
    let parsed = fluree_db_sparql::parse_sparql(update);
    assert!(
        !parsed.has_errors(),
        "parse should succeed: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.unwrap();
    let mut ns = NamespaceRegistry::from_db(&ledger0.snapshot);
    let err = fluree_db_transact::lower_sparql_update_ast(&ast, &mut ns, TxnOpts::default())
        .expect_err("anonymous {| |} in DELETE template must be rejected");
    let msg = format!("{err:?} {err}");
    assert!(
        msg.contains("anonymous annotation block") || msg.contains("DELETE template"),
        "expected DELETE-template anonymous-block rejection, got: {msg}"
    );
}

#[tokio::test]
async fn sparql_version_1_2_declaration_is_accepted() {
    // SPAR-1: a conformant SPARQL 1.2 query may open with the mandated
    // `VERSION "1.2"` declaration. Fluree runs the 1.2 surface ungated,
    // so the pragma is lex-and-accepted (not validated) rather than
    // hard-failing with per-character lexer errors. The query itself
    // (here an annotation read-back) must still execute normally.
    let (fluree, ledger) = seed_alice_engineer("it/sparql-ann/version-decl").await;
    let sparql = r#"
        VERSION "1.2"
        PREFIX ex: <http://example.org/>
        SELECT ?role WHERE {
          ex:alice ex:worksFor ex:acme {| ex:role ?role |} .
        }
    "#;
    assert_eq!(
        sparql_row_count(&fluree, &ledger, sparql).await,
        1,
        "VERSION \"1.2\" prologue must parse and the query must run"
    );
}
