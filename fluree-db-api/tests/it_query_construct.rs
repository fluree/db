//! CONSTRUCT integration tests
//!

use crate::support;
use fluree_db_api::{FlureeBuilder, LedgerState, Novelty};
use fluree_db_core::LedgerSnapshot;
use serde_json::{json, Map, Value as JsonValue};

fn context_people() -> JsonValue {
    json!({
        // Allow un-prefixed terms like "label"/"name"/"config"/"date"
        "@vocab": "http://example.org/",
        "person": "http://example.org/Person#",
        "ex": "http://example.org/",
        "foaf": "http://xmlns.com/foaf/0.1/",
        "schema": "http://schema.org/"
    })
}

fn people_data() -> JsonValue {
    json!([
        {"@id":"ex:jdoe","@type":"ex:Person","person:handle":"jdoe","person:fullName":"Jane Doe","person:favNums":[3,7,42,99]},
        {"@id":"ex:bbob","@type":"ex:Person","person:handle":"bbob","person:fullName":"Billy Bob","person:friend":{"@id":"ex:jbob"},"person:favNums":[23]},
        {"@id":"ex:jbob","@type":"ex:Person","person:handle":"jbob","person:friend":{"@id":"ex:fbueller"},"person:fullName":"Jenny Bob","person:favNums":[8,6,7,5,3,0,9]},
        {"@id":"ex:fbueller","@type":"ex:Person","person:handle":"dankeshön","person:fullName":"Ferris Bueller"},
        {"@id":"ex:alice","foaf:givenname":"Alice","foaf:family_name":"Hacker"},
        {"@id":"ex:bob","foaf:firstname":"Bob","foaf:surname":"Hacker"},
        {"@id":"ex:fran",
         "name":{"@value":"Francois","@language":"fr"},
         // Rust transact currently supports @json values only when @value is a string.
         // Expected CONSTRUCT output also stringifies the JSON.
         "config":{"@type":"@json","@value":"{\"paths\":[\"dev\",\"src\"]}"},
         "date":{"@value":"2020-10-20","@type":"http://www.w3.org/2001/XMLSchema#date"}}
    ])
}

async fn seed_people() -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/construct:people";

    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));

    let tx = json!({
        "@context": context_people(),
        "@graph": people_data()
    });

    let committed = fluree.insert(ledger0, &tx).await.expect("insert people");
    (fluree, committed.ledger)
}

fn normalize_construct(mut v: JsonValue) -> JsonValue {
    // Sort @graph entries by @id and sort any array values for stable comparison.
    let obj = v
        .as_object_mut()
        .expect("construct result must be an object");
    if let Some(JsonValue::Array(graph)) = obj.get_mut("@graph") {
        for node in graph.iter_mut() {
            if let JsonValue::Object(m) = node {
                for (_k, vv) in m.iter_mut() {
                    if let JsonValue::Array(arr) = vv {
                        arr.sort_by_key(std::string::ToString::to_string);
                    }
                }
            }
        }
        graph.sort_by(|a, b| {
            let aid = a.get("@id").and_then(|x| x.as_str()).unwrap_or("");
            let bid = b.get("@id").and_then(|x| x.as_str()).unwrap_or("");
            aid.cmp(bid)
        });
    }
    v
}

#[tokio::test]
async fn construct_basic() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","person:fullName":"?fullName"}],
        "construct": [{"@id":"?s","label":"?fullName"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","label":["Billy Bob"]},
            {"@id":"ex:fbueller","label":["Ferris Bueller"]},
            {"@id":"ex:jbob","label":["Jenny Bob"]},
            {"@id":"ex:jdoe","label":["Jane Doe"]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_no_prefix_context_full_iris() {
    // We prefer explicit contexts.
    // This variant uses an empty @context and full IRIs in WHERE/CONSTRUCT.
    let (fluree, ledger) = seed_people().await;

    let query = json!({
        "@context": {},
        "where": [{"@id":"?s","http://example.org/Person#fullName":"?fullName"}],
        "construct": [{"@id":"?s","http://example.org/label":"?fullName"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": {},
        "@graph": [
            {"@id":"http://example.org/bbob","http://example.org/label":["Billy Bob"]},
            {"@id":"http://example.org/fbueller","http://example.org/label":["Ferris Bueller"]},
            {"@id":"http://example.org/jbob","http://example.org/label":["Jenny Bob"]},
            {"@id":"http://example.org/jdoe","http://example.org/label":["Jane Doe"]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_multiple_clauses() {
    let (fluree, ledger) = seed_people().await;

    // Include "id" aliasing for @id
    let mut ctx_map: Map<String, JsonValue> = context_people()
        .as_object()
        .expect("context object")
        .clone();
    ctx_map.insert("id".to_string(), JsonValue::String("@id".to_string()));
    let ctx = JsonValue::Object(ctx_map);

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","person:fullName":"?fullName"},
            {"@id":"?s","person:favNums":"?num"}
        ],
        "construct": [
            {"@id":"?s","name":"?fullName"},
            {"@id":"?s","num":"?num"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","name":["Billy Bob"],"num":[23]},
            {"@id":"ex:jbob","name":["Jenny Bob"],"num":[0,3,5,6,7,8,9]},
            {"@id":"ex:jdoe","name":["Jane Doe"],"num":[3,7,42,99]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_multiple_clauses_different_subjects() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","person:fullName":"?fullName"},
            {"@id":"?s","person:friend":"?friend"},
            {"@id":"?friend","person:fullName":"?friendName"},
            {"@id":"?friend","person:favNums":"?friendNum"}
        ],
        "construct": [
            {"@id":"?s","myname":"?fullName"},
            {"@id":"?s","friendname":"?friendName"},
            {"@id":"?friend","name":"?friendName"},
            {"@id":"?friend","num":"?friendNum"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","myname":["Billy Bob"],"friendname":["Jenny Bob"]},
            {"@id":"ex:jbob","name":["Jenny Bob"],"num":[0,3,5,6,7,8,9]}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_at_type_values_are_unwrapped() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"?o"}],
        "construct": [{"@id":"?s","@type":"?o"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    // Only the 4 ex:Person nodes have @type
    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","@type":"Person"},
            {"@id":"ex:fbueller","@type":"Person"},
            {"@id":"ex:jbob","@type":"Person"},
            {"@id":"ex:jdoe","@type":"Person"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_class_patterns_in_template() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"ex:Person"}],
        "construct": [{"@id":"?s","@type":"ex:Human"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:bbob","@type":"Human"},
            {"@id":"ex:fbueller","@type":"Human"},
            {"@id":"ex:jbob","@type":"Human"},
            {"@id":"ex:jdoe","@type":"Human"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_id_only_patterns_produce_no_triples() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [{"@id":"?s","@type":"ex:Person"}],
        "construct": [{"@id":"?s"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": []
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_unbound_vars_are_not_included() {
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","?p":"?o"},
            ["optional", {"@id":"?s","@type":"?type"}],
            ["optional", {"@id":"?s","foaf:givenname":"?name"}]
        ],
        "construct": [{"@id":"?s","name":"?name","@type":"?type"}]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [
            {"@id":"ex:alice","name":["Alice"]},
            {"@id":"ex:bbob","@type":"Person"},
            {"@id":"ex:fbueller","@type":"Person"},
            {"@id":"ex:jbob","@type":"Person"},
            {"@id":"ex:jdoe","@type":"Person"}
        ]
    }));

    assert_eq!(actual, expected);
}

#[tokio::test]
async fn construct_value_metadata_displays() {
    // Scenario: "value metadata displays" (language tags + xsd:date + @json)
    let (fluree, ledger) = seed_people().await;
    let ctx = context_people();

    let query = json!({
        "@context": ctx,
        "where": [
            {"@id":"?s","config":"?config"},
            {"@id":"?s","name":"?name"},
            {"@id":"?s","date":"?date"}
        ],
        "construct": [
            {"@id":"?s","json":"?config"},
            {"@id":"?s","name":"?name"},
            {"@id":"?s","date":"?date"}
        ]
    });

    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("query");
    let actual = normalize_construct(result.to_construct(&ledger.snapshot).expect("to_construct"));

    // Note: Rust formats dates as typed strings.
    let expected = normalize_construct(json!({
        "@context": ctx,
        "@graph": [{
            "@id": "ex:fran",
            // Note: Rust emits RDF 1.1 JSON datatype IRI (rdf:JSON) in output formatting.
            "json": [{"@value":"{\"paths\":[\"dev\",\"src\"]}","@type":"http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON"}],
            "name": [{"@value":"Francois","@language":"fr"}],
            "date": [{"@value":"2020-10-20","@type":"http://www.w3.org/2001/XMLSchema#date"}]
        }]
    }));

    assert_eq!(actual, expected);
}

/// Regression for issue #1274: a SPARQL CONSTRUCT executed through the default
/// formatted path (no explicit `.format()`) must NOT error with "CONSTRUCT
/// queries only support JSON-LD output format". The SPARQL default is
/// SPARQL-results JSON, but a graph has no binding-table form, so the formatter
/// coerces the result to JSON-LD instead of rejecting it.
#[tokio::test]
async fn sparql_construct_default_format_yields_jsonld_graph() {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    let sparql = "PREFIX person: <http://example.org/Person#> \
                  CONSTRUCT { ?s person:fullName ?n } WHERE { ?s person:fullName ?n }";

    // Default path — exactly what the server's no-Accept / application/json /
    // application/ld+json branches drive.
    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT default format must not 400");

    // JSON-LD graph shape: an object carrying an @graph array of nodes.
    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("CONSTRUCT output is a JSON-LD @graph object");
    assert!(!graph.is_empty(), "expected constructed triples");
    assert!(
        graph.iter().all(|node| node.get("@id").is_some()),
        "each constructed node carries an @id"
    );
}

/// PR-W2 regression: a SPARQL CONSTRUCT whose template contains a blank node
/// must instantiate a FRESH blank node per solution row — shared by every
/// template triple within the row, distinct across rows. Before the fix,
/// template blank nodes lowered to never-bound variables that the output path
/// dropped, so the graph came back empty (W3C data-r2 construct-3/4,
/// data-sparql11 constructlist).
///
/// This mirrors construct-3: an anonymous `[ ... ]` reification node linking
/// rdf:subject / rdf:predicate / rdf:object for every matched triple.
#[tokio::test]
async fn sparql_construct_anonymous_bnode_template_is_fresh_per_solution() {
    assert_reification_construct(
        "PREFIX person: <http://example.org/Person#> \
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
         CONSTRUCT { [ rdf:subject ?s ; rdf:predicate person:handle ; rdf:object ?h ] } \
         WHERE { ?s person:handle ?h }",
    )
    .await;
}

/// PR-W2 regression, construct-4 shape: a *labeled* template blank node (`_:a`)
/// is likewise scoped to each solution — every row mints its own `_:a`, so the
/// output has one reification node per match, not a single shared node.
#[tokio::test]
async fn sparql_construct_labeled_bnode_template_is_fresh_per_solution() {
    assert_reification_construct(
        "PREFIX person: <http://example.org/Person#> \
         PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \
         CONSTRUCT { _:a rdf:subject ?s ; rdf:predicate person:handle ; rdf:object ?h } \
         WHERE { ?s person:handle ?h }",
    )
    .await;
}

/// Shared body for the two reification-CONSTRUCT regressions above. Four people
/// carry `person:handle`, so a correct engine emits four independent reification
/// blank nodes, each linking exactly subject+predicate+object for its solution.
async fn assert_reification_construct(sparql: &str) {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT with a blank-node template must execute");

    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("CONSTRUCT output is a JSON-LD @graph object");

    // Fresh blank node per solution: four people have person:handle, so four
    // separate reification nodes — never a single merged node nor an empty graph.
    assert_eq!(
        graph.len(),
        4,
        "expected one fresh blank node per solution, got: {out:#}"
    );

    let mut ids = std::collections::HashSet::new();
    for node in graph {
        let obj = node.as_object().expect("each @graph entry is an object");

        // Each reification node is a blank node — an explicit `_:` @id or an
        // anonymous node — never an IRI subject.
        if let Some(id) = obj.get("@id").and_then(JsonValue::as_str) {
            assert!(
                id.starts_with("_:"),
                "reification node must be a blank node, got @id {id}"
            );
            assert!(
                ids.insert(id.to_string()),
                "template blank labels must be distinct across solutions: {id}"
            );
        }

        // The single per-solution blank node links all three reification
        // predicates (subject/predicate/object) within its row.
        let props = obj.keys().filter(|k| !k.starts_with('@')).count();
        assert_eq!(
            props, 3,
            "each reification bnode carries subject+predicate+object: {node:#}"
        );
    }

    // Data still flows through: every handle appears as an rdf:object value.
    let dump = out.to_string();
    for handle in ["jdoe", "bbob", "jbob", "dankeshön"] {
        assert!(
            dump.contains(handle),
            "handle {handle} missing from CONSTRUCT output: {out:#}"
        );
    }
}

/// Recursively collect every blank-node label (`_:…` string) appearing anywhere
/// in a JSON-LD value — as an `@id`, as an `@id` object reference, or as a bare
/// value — so a test can reason about which blank nodes the output actually
/// contains regardless of nesting/compaction.
fn collect_blank_ids(v: &JsonValue, out: &mut std::collections::HashSet<String>) {
    match v {
        JsonValue::String(s) if s.starts_with("_:") => {
            out.insert(s.clone());
        }
        JsonValue::Array(a) => a.iter().for_each(|x| collect_blank_ids(x, out)),
        JsonValue::Object(m) => m.values().for_each(|x| collect_blank_ids(x, out)),
        _ => {}
    }
}

/// O7 regression: a bare `[]` template blank must never collide with an explicit
/// `_:bN` template blank. Before the fix, `[]` lowered to `_:b{len}` (the
/// current variable count at that point), so a user-written `_:bN` with
/// `N == len` folded into the SAME template variable → one minted blank instead
/// of two, silently merging two intended-distinct nodes (a merge the
/// isomorphism-based W3C CONSTRUCT suite cannot catch). The template below is
/// shaped so the anon's `len` is 2 (the two WHERE vars `?s`,`?h`) at the first
/// template triple, aligning the old scheme's `_:b2` with the explicit `_:b2`.
///
/// FAILS on the pre-fix lowering (4 merged nodes), PASSES after (8 distinct).
#[tokio::test]
async fn sparql_construct_anon_and_labeled_blank_never_merge() {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    // `[]` is lowered first (len == 2 WHERE vars → old scheme mints `_:b2`),
    // then the explicit `_:b2`; the buggy lowering makes them one variable.
    let sparql = "PREFIX person: <http://example.org/Person#> \
         CONSTRUCT { [] person:tagQ ?s . _:b2 person:tagP ?s } \
         WHERE { ?s person:handle ?h }";

    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("CONSTRUCT output is a JSON-LD @graph object");

    // seed_people has 4 handles → 4 solution rows. The `[]` node and the `_:b2`
    // node are DISTINCT template blanks, so each row yields TWO blank subjects
    // (one carrying person:tagQ, one carrying person:tagP) = 8 nodes. The buggy
    // lowering merges them to ONE node per row (both predicates on one blank) =
    // 4 nodes.
    assert_eq!(
        graph.len(),
        8,
        "anon `[]` and explicit `_:b2` must stay distinct template blanks \
         (8 nodes = 2/row × 4 rows); a count of 4 means they merged: {out:#}"
    );

    // Signature of the merge: a single node carrying BOTH tag predicates. No
    // output node may carry both.
    for node in graph {
        let obj = node.as_object().expect("each @graph entry is an object");
        let has_p = obj.keys().any(|k| k.contains("tagP"));
        let has_q = obj.keys().any(|k| k.contains("tagQ"));
        assert!(
            !(has_p && has_q),
            "a single blank carries both tagP and tagQ ⇒ the two template \
             blanks merged: {node:#}"
        );
    }
}

/// P4 soundness lock: a minted template blank (`cst…`) can never collide with a
/// STORED data blank (`fdb-…`). A CONSTRUCT that wraps a data blank node inside
/// a fresh template blank must yield TWO distinct blanks (wrapper ≠ wrapped),
/// never a single self-referential node. (Holds already because `cst`/`fdb-`
/// are prefix-disjoint — this locks that invariant into the always-run api
/// workspace so a future minter change can't regress it invisibly.)
#[tokio::test]
async fn sparql_construct_minted_blank_disjoint_from_data_blank() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/construct:datablanks";
    let db0 = LedgerSnapshot::genesis(ledger_id);
    let ledger0 = LedgerState::new(db0, Novelty::new(0));
    // `ex:alice ex:knows _:blank` where `_:blank` is a stored (fdb-) blank node.
    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:alice", "ex:knows": {"ex:nick": "Ally"}}]
    });
    let committed = fluree
        .insert(ledger0, &tx)
        .await
        .expect("insert data blank");
    let ledger = committed.ledger;
    let db = support::graphdb_from_ledger(&ledger);

    // Wrap the data blank `?b` inside a freshly-minted template blank.
    let sparql = "PREFIX ex: <http://example.org/> \
         CONSTRUCT { [ ex:wraps ?b ] } WHERE { ?s ex:knows ?b }";
    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");

    let mut blanks = std::collections::HashSet::new();
    collect_blank_ids(&out, &mut blanks);
    assert_eq!(
        blanks.len(),
        2,
        "expected two distinct blanks (minted wrapper + stored data blank), a \
         count of 1 means the minted blank merged with the data blank: {out:#}"
    );

    // And the wrapper must actually point at the OTHER blank, not itself.
    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("@graph array");
    for node in graph {
        let obj = node.as_object().expect("object node");
        let (Some(id), Some(wraps)) = (
            obj.get("@id").and_then(JsonValue::as_str),
            obj.keys().find(|k| k.contains("wraps")),
        ) else {
            continue;
        };
        let mut targets = std::collections::HashSet::new();
        collect_blank_ids(&obj[wraps], &mut targets);
        assert!(
            !targets.contains(id),
            "the minted wrapper blank ex:wraps ITSELF ⇒ merged with the data \
             blank: {node:#}"
        );
    }
}

/// P4 lock: an RDF collection `( … )` in a CONSTRUCT template desugars to
/// rdf:first / rdf:rest / rdf:nil cells (fresh `#coll…` blanks, which are
/// hardened `#`-prefixed and so unforgeable), producing a well-formed list in
/// the output.
#[tokio::test]
async fn sparql_construct_collection_desugars_to_list() {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    let sparql = "PREFIX person: <http://example.org/Person#> \
         PREFIX ex: <http://example.org/> \
         CONSTRUCT { ex:root ex:items ( ?h ) } \
         WHERE { ?s person:handle ?h }";
    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT with a collection object must execute");

    let dump = out.to_string();
    for frag in ["syntax-ns#first", "syntax-ns#rest", "syntax-ns#nil"] {
        assert!(
            dump.contains(frag),
            "collection did not desugar to an rdf list ({frag} missing): {out:#}"
        );
    }
}

/// P4 lock: nested property lists `[ :p [ :q ?x ] ]` mint DISTINCT blank nodes
/// for the outer and inner node (both via the parser's hardened `#bnpl…`
/// scheme), so the output has the inner node linked from the outer, never a
/// single conflated node.
#[tokio::test]
async fn sparql_construct_nested_property_lists_stay_distinct() {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    let sparql = "PREFIX person: <http://example.org/Person#> \
         PREFIX ex: <http://example.org/> \
         CONSTRUCT { [ ex:outer [ ex:inner ?h ] ] } \
         WHERE { ?s person:handle ?h }";
    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT with nested property lists must execute");
    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("@graph array");

    // 4 rows × 2 nested blanks (outer + inner) = 8 distinct nodes.
    assert_eq!(
        graph.len(),
        8,
        "nested `[ :outer [ :inner ?h ] ]` must mint 2 distinct blanks per row \
         (8 total); fewer means the nested blanks conflated: {out:#}"
    );
    // The outer node carries ex:outer, the inner carries ex:inner; no single
    // node carries both.
    for node in graph {
        let obj = node.as_object().expect("object node");
        let has_outer = obj.keys().any(|k| k.contains("outer"));
        let has_inner = obj.keys().any(|k| k.contains("inner"));
        assert!(
            !(has_outer && has_inner),
            "outer and inner property-list blanks merged: {node:#}"
        );
    }
}

/// P4 negative guardrail (W2BC): CONSTRUCT has no aggregation stage, so an
/// inline-aggregate ORDER BY (e.g. `ORDER BY COUNT(?h)`) cannot be hoisted and
/// the query is rejected rather than mis-executed.
#[tokio::test]
async fn sparql_construct_aggregate_order_by_is_rejected() {
    let (fluree, ledger) = seed_people().await;
    let db = support::graphdb_from_ledger(&ledger);

    let sparql = "PREFIX person: <http://example.org/Person#> \
         CONSTRUCT { ?s person:handle ?h } \
         WHERE { ?s person:handle ?h } ORDER BY (COUNT(?h))";
    let result = db.query(&fluree).sparql(sparql).execute_formatted().await;
    assert!(
        result.is_err(),
        "CONSTRUCT + inline-aggregate ORDER BY must be rejected, got: {result:#?}"
    );
}

/// §16.2: the template is instantiated once per SOLUTION (the sequence, not
/// the distinct set), and each row mints fresh template blanks. Two rows with
/// IDENTICAL variable bindings (two subjects sharing the same handle value,
/// template using only `?h`) must therefore yield TWO distinct blank nodes —
/// per-row minting, not per-distinct-binding. (Jena/oxigraph agree.)
#[tokio::test]
async fn sparql_construct_duplicate_rows_mint_distinct_blanks() {
    let fluree = FlureeBuilder::memory().build_memory();
    let db0 = LedgerSnapshot::genesis("it/construct:duprows");
    let ledger0 = LedgerState::new(db0, Novelty::new(0));
    let tx = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [
            {"@id": "ex:a", "ex:v": "same"},
            {"@id": "ex:b", "ex:v": "same"}
        ]
    });
    let committed = fluree.insert(ledger0, &tx).await.expect("insert");
    let ledger = committed.ledger;
    let db = support::graphdb_from_ledger(&ledger);

    // Template projects only ?h — both WHERE rows carry the identical binding.
    let sparql = "PREFIX ex: <http://example.org/> \
         CONSTRUCT { [ ex:tag ?h ] } WHERE { ?s ex:v ?h }";
    let out = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    let graph = out
        .get("@graph")
        .and_then(JsonValue::as_array)
        .expect("@graph array");
    assert_eq!(
        graph.len(),
        2,
        "two solution rows (even with identical bindings) mint two distinct \
         template blanks — per-row, not per-distinct-binding: {out:#}"
    );
}

// =============================================================================
// RDF set semantics for CONSTRUCT / DESCRIBE results
// =============================================================================
//
// A CONSTRUCT result is an RDF graph, so triple identity is the full
// `(s, p, o)` tuple and the result is a SET union of the instantiated
// templates (SPARQL 1.1 §16.2). Both graph serializations are pinned here so
// they cannot drift apart on that rule.

/// Seed the two-predicate fixture. `same_value` selects the shape where one
/// literal is shared by both predicates, or the distinct-value control.
async fn seed_shared_object(
    ledger_id: &str,
    same_value: bool,
) -> (fluree_db_api::Fluree, LedgerState) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = support::genesis_ledger(&fluree, ledger_id);
    let (v1, v2) = if same_value {
        ("same", "same")
    } else {
        ("alpha", "beta")
    };
    let tx = json!({
        "@graph": [{
            "@id": "http://ex/s",
            "http://ex/p1": v1,
            "http://ex/p2": v2
        }]
    });
    let committed = fluree.insert(ledger0, &tx).await.expect("insert fixture");
    (fluree, committed.ledger)
}

/// Count object values across every `@graph` node, excluding `@id`/`@context`.
fn count_graph_values(v: &JsonValue) -> usize {
    let count_node = |o: &Map<String, JsonValue>| -> usize {
        o.iter()
            .filter(|(k, _)| k.as_str() != "@id" && k.as_str() != "@context")
            .map(|(_, val)| match val {
                JsonValue::Array(a) => a.len(),
                _ => 1,
            })
            .sum()
    };
    match v.get("@graph").and_then(JsonValue::as_array) {
        Some(graph) => graph
            .iter()
            .filter_map(JsonValue::as_object)
            .map(count_node)
            .sum(),
        None => v.as_object().map_or(0, count_node),
    }
}

const CONSTRUCT_ALL: &str = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }";

/// A constant template instantiated once per solution row yields the SAME
/// triple N times; the result graph holds it once. Asserted in BOTH graph
/// serializations — the JSON-LD formatter's value dedupe used to be the only
/// thing collapsing it, which left RDF/XML emitting the duplicate.
#[tokio::test]
async fn sparql_construct_repeated_triple_collapses_in_both_serializations() {
    // Two solution rows x one constant template triple.
    let (fluree, ledger) = seed_shared_object("it/construct:repeat", false).await;
    let db = support::graphdb_from_ledger(&ledger);
    let sparql = "CONSTRUCT { <http://ex/s> <http://ex/pc> \"dup\" } WHERE { ?s ?p ?o }";

    let jsonld = db
        .query(&fluree)
        .sparql(sparql)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    assert_eq!(
        count_graph_values(&jsonld),
        1,
        "JSON-LD collapses the repeated triple: {jsonld:#}"
    );

    let rdfxml = db
        .query(&fluree)
        .sparql(sparql)
        .format(fluree_db_api::FormatterConfig::rdf_xml())
        .execute_formatted_string()
        .await
        .expect("RDF/XML must execute");
    assert_eq!(
        rdfxml.matches("<ns0:pc>").count(),
        1,
        "RDF/XML applies the same set semantics: {rdfxml}"
    );
}

/// One subject, one literal, two predicates: two distinct RDF triples. The
/// JSON-LD formatter's object dedupe once tracked seen values per SUBJECT and
/// dropped the second. The SELECT row path is the oracle.
#[tokio::test]
async fn sparql_construct_shared_object_across_predicates_novelty() {
    let (fluree, ledger) = seed_shared_object("it/construct:shared-obj", true).await;
    let db = support::graphdb_from_ledger(&ledger);

    let rows = support::query_sparql(&fluree, &ledger, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
        .await
        .expect("select");
    let row_count: usize = rows.batches.iter().map(fluree_db_api::Batch::len).sum();
    assert_eq!(row_count, 2, "SELECT oracle sees both triples");

    let out = db
        .query(&fluree)
        .sparql(CONSTRUCT_ALL)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    assert_eq!(
        count_graph_values(&out),
        2,
        "same literal under p1 and p2 are distinct triples: {out:#}"
    );
}

/// Same assertion on the indexed read path: the binary index feeds a different
/// scan operator, and the reported reproduction reindexed before querying.
#[tokio::test]
async fn sparql_construct_shared_object_across_predicates_indexed() {
    let ledger_id = "it/construct:shared-obj-idx";
    let (fluree, _ledger) = seed_shared_object(ledger_id, true).await;
    support::rebuild_and_publish_index(&fluree, ledger_id).await;
    let db = fluree.db(ledger_id).await.expect("indexed view");

    let out = db
        .query(&fluree)
        .sparql(CONSTRUCT_ALL)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    assert_eq!(
        count_graph_values(&out),
        2,
        "indexed lane must agree with novelty: {out:#}"
    );
}

/// Control: distinct objects were never affected. Guards against a "fix" that
/// simply disables deduplication.
#[tokio::test]
async fn sparql_construct_distinct_objects_across_predicates() {
    let (fluree, ledger) = seed_shared_object("it/construct:distinct-obj", false).await;
    let db = support::graphdb_from_ledger(&ledger);

    let out = db
        .query(&fluree)
        .sparql(CONSTRUCT_ALL)
        .execute_formatted()
        .await
        .expect("CONSTRUCT must execute");
    assert_eq!(count_graph_values(&out), 2, "control: {out:#}");
}

/// DESCRIBE lowers to the same `QueryOutput::Construct`, so it shares the
/// result-graph contract.
#[tokio::test]
async fn sparql_describe_shared_object_across_predicates() {
    let (fluree, ledger) = seed_shared_object("it/construct:describe-obj", true).await;
    let db = support::graphdb_from_ledger(&ledger);

    let out = db
        .query(&fluree)
        .sparql("DESCRIBE <http://ex/s>")
        .execute_formatted()
        .await
        .expect("DESCRIBE must execute");
    assert_eq!(
        count_graph_values(&out),
        2,
        "DESCRIBE must emit both triples: {out:#}"
    );
}

/// Three-surface parity: the FQL `construct` form shares the same formatter and
/// must agree with the SPARQL surface.
#[tokio::test]
async fn fql_construct_shared_object_across_predicates() {
    let (fluree, ledger) = seed_shared_object("it/construct:fql-obj", true).await;

    let query = json!({
        "where": [{"@id": "?s", "?p": "?o"}],
        "construct": [{"@id": "?s", "?p": "?o"}]
    });
    let result = support::query_jsonld(&fluree, &ledger, &query)
        .await
        .expect("FQL construct must execute");
    let out = result
        .to_jsonld_async(support::graphdb_from_ledger(&ledger).as_graph_db_ref())
        .await
        .expect("format");
    assert_eq!(
        count_graph_values(&out),
        2,
        "FQL construct must agree with SPARQL: {out:#}"
    );
}

/// RDF/XML must carry both predicates for the shared-object shape. Pins the
/// serializer that was already correct, so the dedupe-scope change cannot
/// over-correct it into the JSON-LD failure mode.
#[tokio::test]
async fn sparql_construct_shared_object_rdfxml_keeps_both_predicates() {
    let (fluree, ledger) = seed_shared_object("it/construct:shared-obj-xml", true).await;
    let db = support::graphdb_from_ledger(&ledger);

    let rdfxml = db
        .query(&fluree)
        .sparql(CONSTRUCT_ALL)
        .format(fluree_db_api::FormatterConfig::rdf_xml())
        .execute_formatted_string()
        .await
        .expect("RDF/XML must execute");
    assert!(
        rdfxml.contains("p1") && rdfxml.contains("p2"),
        "RDF/XML must carry both predicates: {rdfxml}"
    );
}
