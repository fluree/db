//! JSON-LD graph scoping end to end (#1979).
//!
//! A JSON-LD node's statements, and those of every node nested in it
//! (`@id` nodes, anonymous nodes, `@list` items), land in the node's graph
//! scope: its own graph selector if it has one, else the enclosing node's,
//! else the update's `graph` key, else the default graph. Each test compares
//! against the SPARQL spelling of the same write where one exists.

use crate::support::{self, genesis_ledger};
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState};
use serde_json::{json, Value};

const G: &str = "http://example.org/g";
const G2: &str = "http://example.org/g2";

fn ctx() -> Value {
    json!({"ex": "http://example.org/"})
}

async fn sparql_rows(fluree: &Fluree, ledger: &LedgerState, sparql: &str) -> Vec<Value> {
    let rows = support::query_sparql_formatted(fluree, ledger, sparql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {e}\n{sparql}"));
    rows.as_array().cloned().unwrap_or_default()
}

/// Number of triples in `graph` (`None`: the default graph).
async fn count(fluree: &Fluree, ledger: &LedgerState, graph: Option<&str>) -> i64 {
    let sparql = match graph {
        Some(g) => format!("SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{g}> {{ ?s ?p ?o }} }}"),
        None => "SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }".to_string(),
    };
    let rows = sparql_rows(fluree, ledger, &sparql).await;
    rows.first()
        .and_then(Value::as_array)
        .and_then(|cols| cols.first())
        .and_then(Value::as_i64)
        .unwrap_or_else(|| panic!("count row missing: {rows:?}"))
}

/// Sorted string values of `?s ex:<p> ?v` in `graph` (`None`: default graph).
async fn values(
    fluree: &Fluree,
    ledger: &LedgerState,
    graph: Option<&str>,
    p: &str,
) -> Vec<String> {
    let pattern = format!("?s <http://example.org/{p}> ?v");
    let sparql = match graph {
        Some(g) => format!("SELECT ?v WHERE {{ GRAPH <{g}> {{ {pattern} }} }}"),
        None => format!("SELECT ?v WHERE {{ {pattern} }}"),
    };
    let mut out: Vec<String> = sparql_rows(fluree, ledger, &sparql)
        .await
        .iter()
        .filter_map(|row| {
            let v = row.as_array().and_then(|c| c.first()).unwrap_or(row);
            match v {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            }
        })
        .collect();
    out.sort();
    out
}

async fn insert(fluree: &Fluree, ledger: LedgerState, doc: &Value) -> LedgerState {
    fluree
        .insert(ledger, doc)
        .await
        .unwrap_or_else(|e| panic!("insert failed: {e}\n{doc}"))
        .ledger
}

async fn sparql_update(fluree: &Fluree, ledger: LedgerState, sparql: &str) -> LedgerState {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(!parsed.has_errors(), "{:?}", parsed.diagnostics);
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .unwrap_or_else(|e| panic!("SPARQL update failed: {e}\n{sparql}"))
        .ledger
}

/// #1979 P1 and its siblings: nested `@id` nodes and anonymous nodes under a
/// graph selector land in the selector's graph, as they do inside a SPARQL
/// `GRAPH` block.
#[tokio::test]
async fn nested_nodes_land_in_the_selector_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let jsonld = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-nested-jsonld:main"),
        &json!({
            "@context": ctx(),
            "@graph": [{
                "@id": "ex:a",
                "@graph": "ex:g",
                "ex:name": "a",
                "ex:child": {
                    "@id": "ex:b",
                    "ex:name": "b",
                    "ex:child": {"ex:name": "c"}
                }
            }]
        }),
    )
    .await;
    assert_eq!(
        values(&fluree, &jsonld, Some(G), "name").await,
        ["a", "b", "c"]
    );
    assert_eq!(
        count(&fluree, &jsonld, None).await,
        0,
        "nothing in the default graph"
    );

    let sparql = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-nested-sparql:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             INSERT DATA {{ GRAPH <{G}> {{
               ex:a ex:name \"a\" ; ex:child ex:b .
               ex:b ex:name \"b\" ; ex:child [ ex:name \"c\" ] .
             }} }}"
        ),
    )
    .await;
    assert_eq!(
        count(&fluree, &jsonld, Some(G)).await,
        count(&fluree, &sparql, Some(G)).await,
        "the JSON-LD and SPARQL spellings write the same statements to the graph"
    );
}

/// P10′: `@list` items' own properties land with the list, not in the
/// default graph.
#[tokio::test]
async fn list_items_share_the_list_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-list:main"),
        &json!({
            "@context": ctx(),
            "@graph": [{
                "@id": "ex:l",
                "@graph": "ex:g",
                "ex:items": {"@list": [{"ex:name": "i0"}, {"@id": "ex:i1", "ex:name": "i1"}]}
            }]
        }),
    )
    .await;
    assert_eq!(
        values(&fluree, &ledger, Some(G), "name").await,
        ["i0", "i1"]
    );
    assert!(values(&fluree, &ledger, None, "name").await.is_empty());
}

/// P7c: a node selector overrides the update's `graph` key for the node's
/// whole subtree, as `GRAPH <g2>` does under `WITH <g1>`.
#[tokio::test]
async fn node_selector_beats_the_update_graph_key_for_the_subtree() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/scope-update-key:main");
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "graph": "ex:g",
                "insert": {
                    "@id": "ex:a",
                    "@graph": "ex:g2",
                    "ex:child": {"@id": "ex:b", "ex:name": "b"}
                }
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert_eq!(values(&fluree, &ledger, Some(G2), "name").await, ["b"]);
    assert_eq!(count(&fluree, &ledger, Some(G2)).await, 2);
    assert!(values(&fluree, &ledger, Some(G), "name").await.is_empty());

    let twin = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-update-key-sparql:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             WITH <{G}> INSERT {{ GRAPH <{G2}> {{ ex:a ex:child ex:b . ex:b ex:name \"b\" }} }}
             WHERE {{}}"
        ),
    )
    .await;
    assert_eq!(count(&fluree, &twin, Some(G2)).await, 2);
}

/// P7e: deleting a nested `@id` node under a selector retracts it in the
/// selector's graph. It used to retract in the default graph, leaving the
/// named-graph statement in place.
#[tokio::test]
async fn delete_of_a_nested_node_retracts_in_its_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-delete:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             INSERT DATA {{ GRAPH <{G2}> {{ ex:e ex:child ex:f . ex:f ex:q \"6\" }} }}"
        ),
    )
    .await;
    assert_eq!(count(&fluree, &ledger, Some(G2)).await, 2);
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "delete": {
                    "@id": "ex:e",
                    "@graph": "ex:g2",
                    "ex:child": {"@id": "ex:f", "ex:q": "6"}
                }
            }),
        )
        .await
        .expect("delete")
        .ledger;
    assert_eq!(count(&fluree, &ledger, Some(G2)).await, 0);
}

/// P9: upserting a nested `@id` node under a selector replaces its values in
/// the selector's graph and leaves the default graph alone. It used to
/// retract the default-graph value it never addressed.
#[tokio::test]
async fn upsert_of_a_nested_node_replaces_in_its_graph_only() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-upsert:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             INSERT DATA {{
               ex:n ex:q \"old-in-default\" .
               GRAPH <{G}> {{ ex:n ex:q \"old-in-g\" }}
             }}"
        ),
    )
    .await;
    let ledger = fluree
        .upsert(
            ledger,
            &json!({
                "@context": ctx(),
                "@graph": [{
                    "@id": "ex:s",
                    "@graph": "ex:g",
                    "ex:child": {"@id": "ex:n", "ex:q": "new"}
                }]
            }),
        )
        .await
        .expect("upsert")
        .ledger;
    assert_eq!(
        values(&fluree, &ledger, None, "q").await,
        ["old-in-default"]
    );
    assert_eq!(values(&fluree, &ledger, Some(G), "q").await, ["new"]);
}

/// D-B4: a variable graph in update templates writes the graph the WHERE's
/// `GRAPH ?g` binds, as SPARQL `INSERT { GRAPH ?g {…} } WHERE { GRAPH ?g {…} }`
/// does. It used to write a graph literally named `?g`.
#[tokio::test]
async fn variable_graph_writes_the_where_bound_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let seed = format!(
        "PREFIX ex: <http://example.org/>
         INSERT DATA {{ GRAPH <{G}> {{ ex:s ex:p \"1\" }} GRAPH <{G2}> {{ ex:t ex:p \"2\" }} }}"
    );
    let ledger = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-var-jsonld:main"),
        &seed,
    )
    .await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": [["graph", "?g", {"@id": "?s", "ex:p": "?o"}]],
                "insert": [["graph", "?g", {"@id": "?s", "ex:seen": "yes"}]]
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert_eq!(values(&fluree, &ledger, Some(G), "seen").await, ["yes"]);
    assert_eq!(values(&fluree, &ledger, Some(G2), "seen").await, ["yes"]);
    let registered: Vec<String> = ledger
        .snapshot
        .graph_registry
        .iter_entries()
        .map(|(_, iri)| iri.to_string())
        .collect();
    assert!(
        !registered.iter().any(|iri| iri.contains('?')),
        "no graph named after the variable: {registered:?}"
    );

    let twin = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-var-sparql:main"),
        &seed,
    )
    .await;
    let twin = sparql_update(
        &fluree,
        twin,
        "PREFIX ex: <http://example.org/>
         INSERT { GRAPH ?g { ?s ex:seen \"yes\" } } WHERE { GRAPH ?g { ?s ex:p ?o } }",
    )
    .await;
    assert_eq!(
        count(&fluree, &ledger, Some(G)).await,
        count(&fluree, &twin, Some(G)).await
    );
}

/// L-X4: `"@graph": "config"` names the ledger's config graph, and nested
/// config groups follow it.
#[tokio::test]
async fn config_keyword_writes_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/scope-config-keyword:main";
    insert(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [{
                "@id": "urn:it:scope:config",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:policyDefaults": {"f:defaultAllow": false}
            }]
        }),
    )
    .await;
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config written to #config");
    assert_eq!(
        config.policy.as_ref().and_then(|p| p.default_allow),
        Some(false),
        "the nested policy group landed in #config with its field"
    );
}

/// P6a/P6b: a JSON-LD 1.1 named graph's content lands in the graph its
/// `@id` names, and the graph node's own properties land in the enclosing
/// graph, exactly as the SPARQL spelling writes them. The content used to be
/// dropped (and the first content node's `@id` read as a graph selector).
#[tokio::test]
async fn named_graph_object_content_lands_in_its_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ng = "http://example.org/NG";
    let jsonld = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-named-graph:main"),
        &json!({
            "@context": ctx(),
            "@id": "ex:NG",
            "ex:label": "w",
            "@graph": [{"@id": "ex:a", "ex:name": "a"}, {"@id": "ex:b", "ex:name": "b"}]
        }),
    )
    .await;
    assert_eq!(values(&fluree, &jsonld, Some(ng), "name").await, ["a", "b"]);
    assert_eq!(values(&fluree, &jsonld, None, "label").await, ["w"]);
    assert!(values(&fluree, &jsonld, None, "name").await.is_empty());

    let twin = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-named-graph-sparql:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             INSERT DATA {{ ex:NG ex:label \"w\" .
               GRAPH <{ng}> {{ ex:a ex:name \"a\" . ex:b ex:name \"b\" }} }}"
        ),
    )
    .await;
    assert_eq!(
        count(&fluree, &jsonld, Some(ng)).await,
        count(&fluree, &twin, Some(ng)).await
    );
    assert_eq!(
        count(&fluree, &jsonld, None).await,
        count(&fluree, &twin, None).await
    );
}

/// A graph sync names its graph in the request: a named-graph object in the
/// payload is refused rather than folded into the target graph.
#[tokio::test]
async fn sync_refuses_a_named_graph_object() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/scope-sync-named-graph:main";
    insert(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({"@context": ctx(), "@id": "ex:seed", "ex:p": 1}),
    )
    .await;
    let err = fluree
        .sync_named_graph(
            ledger_id,
            G,
            &json!({"@context": ctx(), "@id": "ex:g", "@graph": [{"@id": "ex:a", "ex:p": 1}]}),
            fluree_db_api::SyncGraphOpts::default(),
        )
        .await
        .expect_err("a payload that names a graph is refused");
    assert!(
        err.to_string().contains("must not address named graphs"),
        "{err}"
    );
}

/// N4: anonymous nodes in different `["graph", …]` items stay different
/// nodes through the commit. The label counter used to restart for every
/// item, so they shared one label and merged into one node.
#[tokio::test]
async fn anonymous_nodes_in_separate_graph_items_stay_distinct() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .update(
            genesis_ledger(&fluree, "it/scope-anon-items:main"),
            &json!({
                "@context": ctx(),
                "insert": [
                    ["graph", "ex:g", {"@id": "ex:a", "ex:p": {"ex:name": "x"}}],
                    ["graph", "ex:g", {"@id": "ex:b", "ex:p": {"ex:name": "y"}}]
                ]
            }),
        )
        .await
        .expect("update")
        .ledger;
    let rows = sparql_rows(
        &fluree,
        &ledger,
        &format!(
            "SELECT (COUNT(DISTINCT ?n) AS ?c) WHERE {{ GRAPH <{G}> {{ ?s <http://example.org/p> ?n }} }}"
        ),
    )
    .await;
    let distinct = rows
        .first()
        .and_then(Value::as_array)
        .and_then(|c| c.first())
        .and_then(Value::as_i64);
    assert_eq!(distinct, Some(2), "two anonymous nodes: {rows:?}");
    assert_eq!(values(&fluree, &ledger, Some(G), "name").await, ["x", "y"]);
}

/// The `ex:role` of the annotation on `ex:alice ex:worksFor ex:acme` in
/// `graph` (`None`: the default graph), through the SPARQL annotation tail.
async fn annotation_role(
    fluree: &Fluree,
    ledger: &LedgerState,
    graph: Option<&str>,
) -> Option<String> {
    let inner = "ex:alice ex:worksFor ex:acme {| ex:role ?role |}";
    let sparql = match graph {
        Some(g) => format!(
            "PREFIX ex: <http://example.org/> SELECT ?role WHERE {{ GRAPH <{g}> {{ {inner} }} }}"
        ),
        None => format!("PREFIX ex: <http://example.org/> SELECT ?role WHERE {{ {inner} }}"),
    };
    let result = support::query_sparql(fluree, ledger, &sparql)
        .await
        .expect("annotation-tail query");
    let json = result
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    json["results"]["bindings"]
        .as_array()
        .and_then(|b| b.first())
        .and_then(|row| row["role"]["value"].as_str())
        .map(String::from)
}

/// A1, A2, A3 and named-graph content: an annotated edge written through the
/// update `graph` key, a `["graph", …]` item, an array selector, or a
/// named graph's content commits, and its annotation is found with the edge
/// in that graph. A1 used to be refused; A2 and A3 committed the annotation
/// to the default graph, where its edge is not, so it was reachable from
/// neither graph.
#[tokio::test]
async fn annotations_follow_their_edge_into_the_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let edge = json!({"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme", "@annotation": {"ex:role": "Engineer"}}});
    let mut selector = edge.clone();
    selector["@graph"] = json!(["ex:g"]);
    let cases = [
        (
            "A1",
            true,
            json!({"@context": ctx(), "graph": "ex:g", "insert": edge}),
        ),
        (
            "A2",
            true,
            json!({"@context": ctx(), "insert": [["graph", "ex:g", edge]]}),
        ),
        (
            "A3",
            false,
            json!({"@context": ctx(), "@graph": [selector]}),
        ),
        (
            "named graph",
            false,
            json!({"@context": ctx(), "@id": "ex:g", "@graph": [edge]}),
        ),
    ];
    for (i, (what, update, doc)) in cases.into_iter().enumerate() {
        let ledger = genesis_ledger(&fluree, &format!("it/scope-annotation-{i}:main"));
        let result = if update {
            fluree.update(ledger, &doc).await
        } else {
            fluree.insert(ledger, &doc).await
        };
        let ledger = result.unwrap_or_else(|e| panic!("{what}: {e}")).ledger;
        assert_eq!(
            annotation_role(&fluree, &ledger, Some(G)).await.as_deref(),
            Some("Engineer"),
            "{what}: the annotation is with its edge in the graph"
        );
        assert_eq!(
            annotation_role(&fluree, &ledger, None).await,
            None,
            "{what}: nothing in the default graph"
        );
    }
}

/// An update whose `graph` key names the ledger's own address writes the
/// ledger's default graph, an annotated edge included: its annotation is
/// found with the edge in the default graph, as for an update with no
/// `graph` key.
#[tokio::test]
async fn annotation_under_the_ledger_address_lands_in_the_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/scope-annotation-address:main";
    let edge = json!({"@id": "ex:alice", "ex:worksFor": {"@id": "ex:acme", "@annotation": {"ex:role": "Engineer"}}});
    let ledger = fluree
        .update(
            genesis_ledger(&fluree, ledger_id),
            &json!({"@context": ctx(), "graph": format!("urn:fluree:{ledger_id}"), "insert": edge}),
        )
        .await
        .expect("an annotated edge written under the ledger's address")
        .ledger;
    assert_eq!(
        annotation_role(&fluree, &ledger, None).await.as_deref(),
        Some("Engineer"),
        "the annotation is with its edge in the default graph"
    );
}

/// A node-level `@graph` names the same graph in `where` as in `delete` and
/// `insert`: the one document writes one name, `ex:g`, compact, and the
/// update deletes what the insert wrote. (The `where` side used to take the
/// name as written, so it matched nothing and the update committed nothing.)
/// The keywords resolve alike in an update's `where`: `config` reads this
/// ledger's config graph, and `default` the default graph.
#[tokio::test]
async fn node_level_graph_in_where_resolves_names_like_templates() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/scope-where-names:main";
    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": ctx(),
            "@graph": [
                {"@id": "ex:a", "@graph": "ex:g", "ex:p": 1},
                {"@id": "ex:a", "ex:p": 2}
            ]
        }),
    )
    .await;

    let rows = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": {"@id": "ex:a", "@graph": "ex:g", "ex:p": "?v"}
        }),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("jsonld");
    assert_eq!(rows, json!([1]), "the compact name is the graph's IRI");

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "ex:a", "@graph": "ex:g", "ex:p": "?v"},
                "delete": {"@id": "ex:a", "@graph": "ex:g", "ex:p": "?v"}
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert!(
        values(&fluree, &ledger, Some(G), "p").await.is_empty(),
        "the update deleted what the insert wrote"
    );
    assert_eq!(values(&fluree, &ledger, None, "p").await, ["2"]);

    // `default` reads the default graph, and the update moves the value into
    // `ex:g`.
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "ex:a", "@graph": "default", "ex:p": "?v"},
                "delete": {"@id": "ex:a", "@graph": "default", "ex:p": "?v"},
                "insert": {"@id": "ex:a", "@graph": "ex:g", "ex:p": "?v"}
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert_eq!(values(&fluree, &ledger, Some(G), "p").await, ["2"]);
    assert!(values(&fluree, &ledger, None, "p").await.is_empty());

    // `config` reads the ledger's config graph, as it writes it.
    let config = fluree_db_core::graph_registry::config_graph_iri(ledger_id);
    let ledger = insert(
        &fluree,
        ledger,
        &json!({
            "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
            "@id": "urn:cfg:main",
            "@graph": "config",
            "@type": "https://ns.flur.ee/db#LedgerConfig",
            "rdfs:label": "main config"
        }),
    )
    .await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": {"rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
                "where": {"@id": "?c", "@graph": "config", "rdfs:label": "?l"},
                "delete": {"@id": "?c", "@graph": "config", "rdfs:label": "?l"}
            }),
        )
        .await
        .expect("update")
        .ledger;
    let labels = sparql_rows(
        &fluree,
        &ledger,
        &format!(
            "SELECT ?l WHERE {{ GRAPH <{config}> {{ ?c <http://www.w3.org/2000/01/rdf-schema#label> ?l }} }}"
        ),
    )
    .await;
    assert!(
        labels.is_empty(),
        "deleted from the config graph: {labels:?}"
    );
}

/// The `ex:p` values in the default graph and in `ex:g`.
async fn default_and_g(fluree: &Fluree, ledger: &LedgerState) -> (Vec<String>, Vec<String>) {
    (
        values(fluree, ledger, None, "p").await,
        values(fluree, ledger, Some(G), "p").await,
    )
}

/// A where node's `"@graph": "default"` names the ledger's default graph, as
/// a template's does, also when the update's `graph` key or `from` gives its
/// WHERE another default graph. So the natural move from the default graph
/// into `ex:g` moves: the where and the delete read and retract in the
/// default graph and the insert writes to `ex:g`, under a `graph` key and
/// under `from` alike. The SPARQL spelling moves the same way.
#[tokio::test]
async fn where_default_is_the_ledger_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let seed = json!({"@context": ctx(), "@id": "ex:a", "ex:p": "moved"});
    let expected = (Vec::<String>::new(), vec!["moved".to_string()]);

    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-move-graph:main"),
        &seed,
    )
    .await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "graph": "ex:g",
                "where": {"@id": "?s", "@graph": "default", "ex:p": "?o"},
                "delete": {"@id": "?s", "@graph": "default", "ex:p": "?o"},
                "insert": {"@id": "?s", "ex:p": "?o"}
            }),
        )
        .await
        .expect("the move under a graph key")
        .ledger;
    assert_eq!(default_and_g(&fluree, &ledger).await, expected, "graph key");

    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-move-from:main"),
        &seed,
    )
    .await;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "from": "ex:g2",
                "where": {"@id": "?s", "@graph": "default", "ex:p": "?o"},
                "delete": {"@id": "?s", "ex:p": "?o"},
                "insert": {"@id": "?s", "@graph": "ex:g", "ex:p": "?o"}
            }),
        )
        .await
        .expect("the move under from")
        .ledger;
    assert_eq!(default_and_g(&fluree, &ledger).await, expected, "from");

    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-move-sparql:main"),
        &seed,
    )
    .await;
    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!(
            "PREFIX ex: <http://example.org/>
             DELETE {{ ?s ex:p ?o }} INSERT {{ GRAPH <{G}> {{ ?s ex:p ?o }} }}
             WHERE {{ ?s ex:p ?o }}"
        ),
    )
    .await;
    assert_eq!(default_and_g(&fluree, &ledger).await, expected, "SPARQL");
}

/// The move from the default graph into `ex:g`, under a `graph` key.
fn move_into_g() -> Value {
    json!({
        "@context": ctx(),
        "graph": "ex:g",
        "where": {"@id": "?s", "@graph": "default", "ex:p": "?o"},
        "delete": {"@id": "?s", "@graph": "default", "ex:p": "?o"},
        "insert": {"@id": "?s", "ex:p": "?o"}
    })
}

/// A graph registered under `@default` (a SPARQL `<@default>` without a BASE
/// stays as written) is an ordinary named graph to an update whose where
/// reads the ledger's default graph by name: the where reads it by its own
/// name, `GRAPH ?g` lists it, and the move still moves.
#[tokio::test]
async fn where_default_leaves_a_graph_named_at_default_alone() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-move-at-default:main"),
        &json!({"@context": ctx(), "@id": "ex:a", "ex:p": "moved"}),
    )
    .await;
    let ledger = sparql_update(
        &fluree,
        ledger,
        "INSERT DATA { GRAPH <@default> { <http://example.org/b> <http://example.org/p> \"decoy\" } }",
    )
    .await;

    // Read by its name, beside a where node that reads the default graph.
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "graph": "ex:g",
                "where": [
                    {"@id": "?a", "@graph": "default", "ex:p": "moved"},
                    ["graph", "@default", {"@id": "?s", "ex:p": "?o"}]
                ],
                "insert": {"@id": "?s", "ex:byName": "?o"}
            }),
        )
        .await
        .expect("the graph read by its name")
        .ledger;
    assert_eq!(values(&fluree, &ledger, Some(G), "byName").await, ["decoy"]);

    // Listed by `GRAPH ?g`, likewise.
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "graph": "ex:g",
                "where": [
                    {"@id": "?a", "@graph": "default", "ex:p": "moved"},
                    ["graph", "?g", {"@id": "?s", "ex:p": "?o"}]
                ],
                "insert": {"@id": "?s", "ex:listed": "?o"}
            }),
        )
        .await
        .expect("the graph listed by GRAPH ?g")
        .ledger;
    assert_eq!(values(&fluree, &ledger, Some(G), "listed").await, ["decoy"]);

    let ledger = fluree
        .update(ledger, &move_into_g())
        .await
        .expect("the move beside a graph named @default")
        .ledger;
    assert_eq!(
        default_and_g(&fluree, &ledger).await,
        (Vec::<String>::new(), vec!["moved".to_string()])
    );
    assert_eq!(
        values(&fluree, &ledger, Some("@default"), "p").await,
        ["decoy"]
    );
}

/// The name an update's WHERE dataset gives the ledger's default graph is
/// the where's to resolve `"default"` to: an update refuses it written as a
/// graph name (node-level or `["graph", …]`), as a VALUES value (in the where
/// or the update's `values`), and as a `fromNamed` alias.
#[tokio::test]
async fn update_refuses_the_reserved_graph_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-reserved-name:main"),
        &json!({"@context": ctx(), "@id": "ex:a", "ex:p": "moved"}),
    )
    .await;
    let reserved = fluree_db_query::parse::LEDGER_DEFAULT_GRAPH;
    let in_graph = json!(["graph", "?g", {"@id": "?s", "ex:p": "?o"}]);
    for (case, extra) in [
        (
            "node-level @graph",
            json!({"where": {"@id": "?s", "@graph": reserved, "ex:p": "?o"}}),
        ),
        (
            "[\"graph\", …]",
            json!({"where": [["graph", reserved, {"@id": "?s", "ex:p": "?o"}]]}),
        ),
        (
            "where VALUES",
            json!({"where": [["values", ["?g", [reserved]]], in_graph]}),
        ),
        (
            "update values",
            json!({"values": ["?g", [reserved]], "where": [in_graph]}),
        ),
        (
            "fromNamed alias",
            json!({
                "fromNamed": [{"alias": reserved, "graph": "ex:g2"}],
                "where": {"@id": "?s", "ex:p": "?o"}
            }),
        ),
    ] {
        let mut update = json!({
            "@context": ctx(),
            "graph": "ex:g",
            "insert": {"@id": "?s", "ex:q": "?o"}
        });
        if let (Some(update), Some(extra)) = (update.as_object_mut(), extra.as_object()) {
            update.extend(extra.clone());
        }
        let err = fluree
            .update(ledger.clone(), &update)
            .await
            .expect_err(case)
            .to_string();
        assert!(err.contains("reserved"), "{case}: {err}");
    }
}

/// No graph written under an IRI has the reserved name, but the Turtle lexer
/// decodes a `\u0020` escape inside an IRI, so a TriG `GRAPH <@ledger\u0020default>`
/// registers one. An update whose where reads the ledger's default graph by
/// that name is then refused rather than reading that graph instead; an
/// update that does not is unaffected.
#[tokio::test]
async fn where_default_refuses_a_graph_registered_under_its_name() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = insert(
        &fluree,
        genesis_ledger(&fluree, "it/scope-move-collides:main"),
        &json!({"@context": ctx(), "@id": "ex:a", "ex:p": "moved"}),
    )
    .await;
    let reserved = fluree_db_query::parse::LEDGER_DEFAULT_GRAPH;
    let escaped = reserved.replace(' ', "\\u0020");
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&format!(
            "GRAPH <{escaped}> {{ <http://example.org/c> <http://example.org/p> \"collides\" . }}"
        ))
        .execute()
        .await
        .expect("a TriG graph under the reserved name")
        .ledger;
    let registered = support::query_jsonld_formatted(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?v",
            "where": [["graph", reserved, {"@id": "?s", "ex:p": "?v"}]]
        }),
    )
    .await
    .expect("a query reads the graph by its name");
    assert_eq!(registered, json!(["collides"]));

    let err = fluree
        .update(ledger.clone(), &move_into_g())
        .await
        .expect_err("the move beside a graph registered under the reserved name")
        .to_string();
    assert!(err.contains("registered under the name"), "{err}");

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "graph": "ex:g",
                "where": {"@id": "?s", "ex:p": "?o"},
                "insert": {"@id": "?s", "ex:q": "?o"}
            }),
        )
        .await
        .expect("an update that does not read the default graph by name")
        .ledger;
    assert_eq!(
        default_and_g(&fluree, &ledger).await,
        (vec!["moved".to_string()], Vec::<String>::new())
    );
}

/// D-B6: a node-level `@graph` in `where` scopes the pattern to the graph, as
/// `["graph", g, …]` and SPARQL `GRAPH <g>` do. A query returns the graph's
/// rows, and an update's WHERE matches: it used to match nothing, so the
/// update committed nothing and the target survived.
#[tokio::test]
async fn node_level_graph_in_where_scopes_the_pattern() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_update(
        &fluree,
        genesis_ledger(&fluree, "it/scope-where-graph:main"),
        &format!(
            "PREFIX ex: <http://example.org/>
             INSERT DATA {{ GRAPH <{G}> {{ ex:s ex:p \"in-g\" }} ex:s ex:p \"in-default\" }}"
        ),
    )
    .await;

    let rows = support::query_jsonld(
        &fluree,
        &ledger,
        &json!({
            "@context": ctx(),
            "select": "?o",
            "where": {"@id": "ex:s", "@graph": G, "ex:p": "?o"}
        }),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("jsonld");
    assert_eq!(
        rows,
        json!(["in-g"]),
        "the pattern matched in the graph only"
    );

    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "where": {"@id": "?s", "@graph": G, "ex:p": "?o"},
                "delete": {"@id": "?s", "@graph": G, "ex:p": "?o"}
            }),
        )
        .await
        .expect("update")
        .ledger;
    assert!(
        values(&fluree, &ledger, Some(G), "p").await.is_empty(),
        "deleted in the graph"
    );
    assert_eq!(values(&fluree, &ledger, None, "p").await, ["in-default"]);
}
