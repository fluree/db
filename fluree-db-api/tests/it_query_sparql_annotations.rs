//! SPARQL 1.2 / RDF 1.2 edge-annotation integration tests.
//!
//! Inserts use the JSON-LD `@annotation` surface (M1); queries use the
//! new SPARQL annotation surface (M4). Once M4.4 lands, parallel
//! coverage with SPARQL UPDATE inserts joins these tests.
//!
//! See `SPARQL_EDGE_ANNOTATIONS_IMPL_PLAN.md` for the surface
//! contract and the per-context blank-node / variable rules.

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
