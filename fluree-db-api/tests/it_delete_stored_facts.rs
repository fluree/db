//! A DELETE retracts only facts that are stored.
//!
//! A retraction of a fact that is not stored used to be committed anyway
//! (a "phantom"), with two effects:
//! - `DELETE DATA` of an absent triple committed a new `t` holding one
//!   retraction, though `docs/transactions/retractions.md` says it is a
//!   no-op;
//! - `DELETE {x} INSERT {x}` with `x` absent committed nothing and never
//!   wrote `x`: the phantom retraction cancelled the insert, against SPARQL
//!   1.1 Update's `(DS − D) ∪ I`.
//!
//! Every DELETE intent that a WHERE triple does not witness is now matched
//! against the stored facts of its slot, and one that names none stages
//! nothing. Each case has SPARQL and JSON-LD twins (Cypher where its
//! lowering produces the shape), on a novelty-only ledger and an indexed
//! one.
//!
//! On an indexed ledger an intent also names a fact by its index key, which
//! keeps less than some terms carry: a big integer's XSD subtype. (A
//! `dateTime` or `time` is read to the microsecond its key keeps.)

use crate::support;
use fluree_db_api::policy_builder::build_policy_context_from_opts;
use fluree_db_api::{
    CommitRef, ConflictStrategy, Fluree, FlureeBuilder, GovernanceOptions, LedgerState,
    PolicyContext, ReindexOptions, SyncGraphOpts, TransactResult,
};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest};
use serde_json::{json, Value as JsonValue};

const EX: &str = "http://example.org/ns/";
const G1: &str = "http://example.org/g1";

fn ctx() -> JsonValue {
    json!({"ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"})
}

async fn sparql_update(fluree: &Fluree, ledger: LedgerState, body: &str) -> TransactResult {
    let text =
        format!("PREFIX ex: <{EX}>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{body}");
    let parsed = fluree_db_sparql::parse_sparql(&text);
    assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("SPARQL update")
}

async fn jsonld_update(fluree: &Fluree, ledger: LedgerState, mut txn: JsonValue) -> TransactResult {
    txn["@context"] = ctx();
    fluree.update(ledger, &txn).await.expect("JSON-LD update")
}

/// `SELECT ?o` over `?s ?p ?o` for `ex:s`, as `p=value[@lang]`, sorted.
async fn facts(fluree: &Fluree, ledger: &LedgerState) -> Vec<String> {
    let q = format!(
        "SELECT ?p ?v ?l WHERE {{ <{EX}s> ?p ?o BIND(STR(?o) AS ?v) BIND(LANG(?o) AS ?l) }}"
    );
    let result = support::query_sparql(fluree, ledger, &q)
        .await
        .expect("query");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let cell = |c: &JsonValue| {
        c.as_str()
            .or_else(|| c.get("@id").and_then(JsonValue::as_str))
            .or_else(|| c.get("@value").and_then(JsonValue::as_str))
            .map(str::to_string)
            .unwrap_or_else(|| c.to_string())
    };
    let mut rows: Vec<String> = json
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            let r = r.as_array().expect("row");
            let p = cell(&r[0]);
            let p = p.rsplit(['/', '#', ':']).next().unwrap_or(&p).to_string();
            let lang = cell(&r[2]);
            if lang.is_empty() || lang == "null" {
                format!("{p}={}", cell(&r[1]))
            } else {
                format!("{p}={}@{lang}", cell(&r[1]))
            }
        })
        .collect();
    rows.sort();
    rows
}

/// A file-backed ledger holding `seed`, optionally indexed.
async fn seeded(
    name: &str,
    seed: JsonValue,
    indexed: bool,
) -> (tempfile::TempDir, Fluree, LedgerState) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("file fluree");
    let id = format!("it/delete-stored-{name}-{indexed}:main");
    let ledger = fluree.create_ledger(&id).await.expect("create");
    let mut doc = seed;
    doc["@context"] = ctx();
    let ledger = fluree.insert(ledger, &doc).await.expect("seed").ledger;
    if !indexed {
        return (dir, fluree, ledger);
    }
    drop(ledger);
    support::rebuild_and_publish_index(&fluree, &id).await;
    let ledger = fluree.ledger(&id).await.expect("reload");
    assert!(ledger.snapshot.range_provider.is_some());
    (dir, fluree, ledger)
}

/// [`seeded`], with the seed written by a SPARQL update.
async fn seeded_with(
    name: &str,
    update: &str,
    indexed: bool,
) -> (tempfile::TempDir, Fluree, LedgerState) {
    let dir = tempfile::tempdir().expect("tempdir");
    let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
        .build()
        .expect("file fluree");
    let id = format!("it/delete-stored-{name}-{indexed}:main");
    let ledger = fluree.create_ledger(&id).await.expect("create");
    let ledger = sparql_update(&fluree, ledger, update).await.ledger;
    if !indexed {
        return (dir, fluree, ledger);
    }
    drop(ledger);
    support::rebuild_and_publish_index(&fluree, &id).await;
    let ledger = fluree.ledger(&id).await.expect("reload");
    assert!(ledger.snapshot.range_provider.is_some());
    (dir, fluree, ledger)
}

fn no_commit(r: &TransactResult, t_before: i64, what: &str) {
    assert_eq!(
        (r.receipt.flake_count, r.ledger.t()),
        (0, t_before),
        "{what}: expected no commit"
    );
}

#[tokio::test]
async fn deleting_an_absent_fact_commits_nothing() {
    for indexed in [false, true] {
        let seed = json!({"@id": "ex:s", "ex:p": "present"});

        let (_d, fluree, ledger) = seeded("absent-sparql", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = sparql_update(&fluree, ledger, "DELETE DATA { ex:s ex:p \"absent\" }").await;
        no_commit(&r, t, &format!("SPARQL DELETE DATA (indexed={indexed})"));
        assert_eq!(facts(&fluree, &r.ledger).await, ["p=present"]);

        let (_d, fluree, ledger) = seeded("absent-jsonld", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = jsonld_update(
            &fluree,
            ledger,
            json!({"delete": {"@id": "ex:s", "ex:p": "absent"}}),
        )
        .await;
        no_commit(&r, t, &format!("JSON-LD delete (indexed={indexed})"));

        // A retraction of a present fact next to an absent one retracts
        // only the present one.
        let (_d, fluree, ledger) = seeded("absent-mixed", seed.clone(), indexed).await;
        let r = sparql_update(
            &fluree,
            ledger,
            "DELETE DATA { ex:s ex:p \"absent\" . ex:s ex:p \"present\" }",
        )
        .await;
        assert_eq!(
            (r.receipt.retract_count, r.receipt.assert_count),
            (1, 0),
            "indexed={indexed}"
        );
        assert!(facts(&fluree, &r.ledger).await.is_empty());

        // Cypher `REMOVE n:Label` retracts `rdf:type Label`, a constant.
        let (_d, fluree, ledger) = seeded("absent-cypher", seed.clone(), indexed).await;
        let ledger = fluree
            .transact_cypher(ledger, "CREATE (n:Person {name: 'n'})")
            .await
            .expect("cypher CREATE")
            .ledger;
        let t = ledger.t();
        let r = fluree
            .transact_cypher(ledger, "MATCH (n:Person {name: 'n'}) REMOVE n:Missing")
            .await
            .expect("cypher REMOVE");
        no_commit(
            &r,
            t,
            &format!("Cypher REMOVE of an absent label (indexed={indexed})"),
        );
    }
}

/// A delete that names a graph the ledger does not have commits nothing and
/// registers no graph, as in the default graph; it used to land an empty
/// commit that registered the graph. A graph an INSERT template names is still
/// registered when the WHERE matches nothing, delete beside it or not
/// (`no_match_update_naming_a_new_graph_commits_its_registration`).
#[tokio::test]
async fn a_delete_in_a_new_graph_commits_nothing() {
    const NEW: &str = "http://example.org/new";
    let registered = |r: &TransactResult| r.ledger.snapshot.graph_registry.graph_id_for_iri(NEW);
    for indexed in [false, true] {
        let seed = json!({"@id": "ex:s", "ex:p": "present"});

        let (_d, fluree, ledger) = seeded("new-graph-sparql", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = sparql_update(
            &fluree,
            ledger,
            &format!("DELETE DATA {{ GRAPH <{NEW}> {{ ex:s ex:p \"present\" }} }}"),
        )
        .await;
        let what = format!("SPARQL DELETE DATA (indexed={indexed})");
        no_commit(&r, t, &what);
        assert!(registered(&r).is_none(), "{what}: registered {NEW}");

        let (_d, fluree, ledger) = seeded("new-graph-jsonld", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = jsonld_update(
            &fluree,
            ledger,
            json!({"graph": NEW, "delete": {"@id": "ex:s", "ex:p": "present"}}),
        )
        .await;
        let what = format!("JSON-LD delete (indexed={indexed})");
        no_commit(&r, t, &what);
        assert!(registered(&r).is_none(), "{what}: registered {NEW}");

        let (_d, fluree, ledger) = seeded("new-graph-insert", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = sparql_update(
            &fluree,
            ledger,
            &format!(
                "DELETE {{ GRAPH <{NEW}> {{ ?s ex:q \"x\" }} }} \
                 INSERT {{ GRAPH <{NEW}> {{ ?s ex:q \"y\" }} }} WHERE {{ ?s ex:p \"nothing\" }}"
            ),
        )
        .await;
        let what = format!("DELETE and INSERT, no match (indexed={indexed})");
        assert_eq!(
            (r.receipt.flake_count, r.ledger.t()),
            (0, t + 1),
            "{what}: the registration commits"
        );
        assert!(registered(&r).is_some(), "{what}: {NEW} not registered");
    }
}

/// A WHERE-driven DELETE removes every stored fact its rows name, list
/// entries holding the same value included: each identical row used to take
/// the first stored entry, so `DELETE WHERE` over `["v", "v", "w"]` left one
/// `"v"` behind, and over a plain `"v"` beside a list `["v"]` left the plain
/// one. A WHERE that repeats each row (a second pattern matching twice)
/// retracts each stored fact once.
#[tokio::test]
async fn a_where_driven_delete_removes_every_identical_list_entry() {
    for indexed in [false, true] {
        for (what, seed, delete, retracted) in [
            (
                "repeated list value",
                json!({"@id": "ex:s", "ex:list": {"@list": ["v", "v", "w"]}}),
                "DELETE WHERE { ex:s ex:list ?o }",
                3,
            ),
            (
                "plain value beside a list entry",
                json!({"@id": "ex:s", "ex:list": ["v", {"@list": ["v"]}]}),
                "DELETE WHERE { ex:s ex:list ?o }",
                2,
            ),
            (
                "rows repeated by another pattern",
                json!({"@graph": [
                    {"@id": "ex:s", "ex:list": {"@list": ["v", "v"]}},
                    {"@id": "ex:x1", "ex:q": 1},
                    {"@id": "ex:x2", "ex:q": 2}
                ]}),
                "DELETE { ex:s ex:list ?o } WHERE { ex:s ex:list ?o . ?x ex:q ?y }",
                2,
            ),
        ] {
            let (_d, fluree, ledger) = seeded("identical-entries", seed, indexed).await;
            let r = sparql_update(&fluree, ledger, delete).await;
            let what = format!("{what} (indexed={indexed})");
            assert_eq!(r.receipt.retract_count, retracted, "{what}");
            assert!(facts(&fluree, &r.ledger).await.is_empty(), "{what}");
        }
    }
}

#[tokio::test]
async fn delete_insert_of_an_absent_fact_inserts_it() {
    for indexed in [false, true] {
        let seed = json!({"@id": "ex:s", "ex:other": "o"});

        let (_d, fluree, ledger) = seeded("di-sparql", seed.clone(), indexed).await;
        let r = sparql_update(
            &fluree,
            ledger,
            "DELETE { ex:s ex:p \"x\" } INSERT { ex:s ex:p \"x\" } WHERE {}",
        )
        .await;
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            ["other=o", "p=x"],
            "SPARQL, x absent (indexed={indexed})"
        );
        // x present now: the same update is a no-op.
        let t = r.ledger.t();
        let r = sparql_update(
            &fluree,
            r.ledger,
            "DELETE { ex:s ex:p \"x\" } INSERT { ex:s ex:p \"x\" } WHERE {}",
        )
        .await;
        no_commit(&r, t, &format!("SPARQL, x present (indexed={indexed})"));
        assert_eq!(facts(&fluree, &r.ledger).await, ["other=o", "p=x"]);

        let (_d, fluree, ledger) = seeded("di-jsonld", seed.clone(), indexed).await;
        let txn = json!({
            "delete": {"@id": "ex:s", "ex:p": "x"},
            "insert": {"@id": "ex:s", "ex:p": "x"}
        });
        let r = jsonld_update(&fluree, ledger, txn.clone()).await;
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            ["other=o", "p=x"],
            "JSON-LD, x absent (indexed={indexed})"
        );
        let t = r.ledger.t();
        let r = jsonld_update(&fluree, r.ledger, txn).await;
        no_commit(&r, t, &format!("JSON-LD, x present (indexed={indexed})"));
    }
}

#[tokio::test]
async fn delete_data_names_terms_exactly() {
    for indexed in [false, true] {
        // A language tag compares case-insensitively; the stored tag is the
        // one retracted.
        let (_d, fluree, ledger) = seeded(
            "lang",
            json!({"@id": "ex:s", "ex:label": {"@value": "a", "@language": "en"}}),
            indexed,
        )
        .await;
        let r = sparql_update(&fluree, ledger, "DELETE DATA { ex:s ex:label \"a\"@EN }").await;
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "indexed={indexed}"
        );

        // The datatype is part of the term: an xsd:int is not deleted by an
        // xsd:integer of the same value.
        let (_d, fluree, ledger) = seeded(
            "dt",
            json!({"@id": "ex:s", "ex:n": {"@value": "1", "@type": "xsd:int"}}),
            indexed,
        )
        .await;
        let t = ledger.t();
        let r = sparql_update(&fluree, ledger, "DELETE DATA { ex:s ex:n 1 }").await;
        no_commit(
            &r,
            t,
            &format!("xsd:integer vs stored xsd:int (indexed={indexed})"),
        );
        assert_eq!(facts(&fluree, &r.ledger).await, ["n=1"]);
        // The same term deletes it (spelled in JSON-LD here;
        // `delete_data_of_typed_literals_deletes_them` covers SPARQL).
        let r = jsonld_update(
            &fluree,
            r.ledger,
            json!({"delete": {"@id": "ex:s", "ex:n": {"@value": "1", "@type": "xsd:int"}}}),
        )
        .await;
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "indexed={indexed}"
        );
    }
}

/// SPARQL `DELETE DATA` of typed literals (`xsd:int`, `xsd:long`,
/// `xsd:dateTime`). SPARQL UPDATE used to keep their lexical form as a
/// string, so the intent named no stored term and the delete committed
/// phantom retractions that removed nothing. The lowering now coerces them
/// as JSON-LD does (#1988), the intent names the stored term exactly, and
/// the resolver deletes it.
#[tokio::test]
async fn delete_data_of_typed_literals_deletes_them() {
    for indexed in [false, true] {
        let (_d, fluree, ledger) = seeded(
            "typed",
            json!({
                "@id": "ex:s",
                "ex:n": {"@value": "1", "@type": "xsd:int"},
                "ex:l": {"@value": "5", "@type": "xsd:long"},
                "ex:d": {"@value": "2020-01-01T00:00:00Z", "@type": "xsd:dateTime"}
            }),
            indexed,
        )
        .await;
        let r = sparql_update(
            &fluree,
            ledger,
            "DELETE DATA { ex:s ex:n \"1\"^^xsd:int . ex:s ex:l \"5\"^^xsd:long . \
             ex:s ex:d \"2020-01-01T00:00:00Z\"^^xsd:dateTime }",
        )
        .await;
        assert_eq!(r.receipt.retract_count, 3, "indexed={indexed}");
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "indexed={indexed}"
        );
    }
}

/// The SPARQL and JSON-LD twins of the upsert-over-languages case: a
/// same-shape DELETE/INSERT WHERE replaces every language, because each
/// WHERE row is the decode of the stored fact, tag included.
#[tokio::test]
async fn delete_insert_where_replaces_every_language() {
    for indexed in [false, true] {
        let seed = json!({
            "@id": "ex:s",
            "ex:label": [{"@value": "a", "@language": "en"}, {"@value": "a", "@language": "fr"}]
        });
        let (_d, fluree, ledger) = seeded("langs-sparql", seed.clone(), indexed).await;
        let r = sparql_update(
            &fluree,
            ledger,
            "DELETE { ex:s ex:label ?o } INSERT { ex:s ex:label \"b\"@en } WHERE { ex:s ex:label ?o }",
        )
        .await;
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            ["label=b@en"],
            "indexed={indexed}"
        );

        let (_d, fluree, ledger) = seeded("langs-jsonld", seed, indexed).await;
        let r = jsonld_update(
            &fluree,
            ledger,
            json!({
                "where": {"@id": "ex:s", "ex:label": "?o"},
                "delete": {"@id": "ex:s", "ex:label": "?o"},
                "insert": {"@id": "ex:s", "ex:label": {"@value": "b", "@language": "en"}}
            }),
        )
        .await;
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            ["label=b@en"],
            "indexed={indexed}"
        );
    }
}

/// A big integer typed with an XSD subtype decodes from the index with a
/// lossy datatype (`NUM_BIG` stores none). A witnessed DELETE WHERE row
/// carries that decode and still deletes the value through the shared
/// storage key, as before. On a list-bearing ledger the row also goes
/// through the resolver for list positions; this pins that the exact
/// datatype rule does not drop it there.
async fn numbig_rows(fluree: &Fluree, ledger: &LedgerState) -> usize {
    let q = format!("PREFIX ex: <{EX}>\nSELECT ?o WHERE {{ GRAPH <{G1}> {{ ?s ex:n ?o }} }}");
    let r = support::query_sparql(fluree, ledger, &q).await.expect("q");
    r.to_jsonld(&ledger.snapshot)
        .expect("jsonld")
        .as_array()
        .map_or(0, Vec::len)
}

#[tokio::test]
async fn witnessed_delete_of_an_indexed_big_integer_in_a_named_graph() {
    for with_list in [false, true] {
        let mut graph = vec![json!({
            "@id": "ex:s", "@graph": G1,
            "ex:n": {"@value": "18446744073709551615", "@type": "xsd:unsignedLong"}
        })];
        if with_list {
            graph.push(json!({"@id": "ex:other", "ex:items": {"@list": ["a", "b"]}}));
        }
        for surface in ["sparql", "jsonld"] {
            let (_d, fluree, ledger) = seeded(
                &format!("numbig-{surface}-{with_list}"),
                json!({"@graph": graph.clone()}),
                true,
            )
            .await;
            assert_eq!(ledger.snapshot.has_list_meta, Some(with_list));
            assert_eq!(numbig_rows(&fluree, &ledger).await, 1, "precondition");
            let r = if surface == "sparql" {
                sparql_update(
                    &fluree,
                    ledger,
                    &format!("DELETE WHERE {{ GRAPH <{G1}> {{ ?s ex:n ?o }} }}"),
                )
                .await
            } else {
                jsonld_update(
                    &fluree,
                    ledger,
                    json!({
                        "where": [["graph", G1, {"@id": "?s", "ex:n": "?o"}]],
                        "delete": [["graph", G1, {"@id": "?s", "ex:n": "?o"}]]
                    }),
                )
                .await
            };
            assert_eq!(
                numbig_rows(&fluree, &r.ledger).await,
                0,
                "{surface}, with_list={with_list}"
            );
        }
    }
}

// =============================================================================
// A fact the update both deletes and inserts is kept, whatever the rows
// =============================================================================

/// `ex:s ex:p "v"`, in `graph` when one is given, and three items: a WHERE
/// over the items has three rows, and only `ex:i1`'s binds `?keep`.
fn kept_seed(graph: Option<&str>) -> JsonValue {
    let mut fact = json!({"@id": "ex:s", "ex:p": "v"});
    if let Some(g) = graph {
        fact["@graph"] = json!(g);
    }
    json!({"@graph": [
        fact,
        {"@id": "ex:i1", "ex:kind": "item", "ex:keeps": {"@id": "ex:s"}},
        {"@id": "ex:i2", "ex:kind": "item"},
        {"@id": "ex:i3", "ex:kind": "item"}
    ]})
}

/// The constant DELETE names the fact on all three rows, the INSERT on the
/// one row that binds `?keep`. The fact is in the update's insert set, so
/// under `(DS − D) ∪ I` it stays. Netting each fact by how many rows named
/// it on each side kept the surplus retraction and deleted it.
#[tokio::test]
async fn a_fact_deleted_on_more_rows_than_it_is_inserted_on_is_kept() {
    for indexed in [false, true] {
        for graph in [None, Some(G1)] {
            let wrap = |body: &str| match graph {
                None => body.to_string(),
                Some(g) => format!("GRAPH <{g}> {{ {body} }}"),
            };
            let sparql = [
                format!(
                    "DELETE {{ {} }} INSERT {{ {} }} \
                     WHERE {{ ?i ex:kind \"item\" OPTIONAL {{ ?i ex:keeps ?keep }} }}",
                    wrap("ex:s ex:p \"v\""),
                    wrap("?keep ex:p \"v\""),
                ),
                format!(
                    "DELETE {{ {} }} INSERT {{ {} }} \
                     WHERE {{ VALUES (?x ?keep) {{ (1 ex:s) (2 UNDEF) (3 UNDEF) }} }}",
                    wrap("ex:s ex:p \"v\""),
                    wrap("?keep ex:p \"v\""),
                ),
            ];
            for body in sparql {
                let (_d, fluree, ledger) = seeded("kept-sparql", kept_seed(graph), indexed).await;
                let t = ledger.t();
                let r = sparql_update(&fluree, ledger, &body).await;
                let what = format!("{body} (indexed={indexed})");
                no_commit(&r, t, &what);
                assert_eq!(graph_objects(&r.ledger, graph).await, ["v"], "{what}");
            }

            let template = |node: JsonValue| match graph {
                None => node,
                Some(g) => json!([["graph", g, node]]),
            };
            let (_d, fluree, ledger) = seeded("kept-jsonld", kept_seed(graph), indexed).await;
            let t = ledger.t();
            let r = jsonld_update(
                &fluree,
                ledger,
                json!({
                    "where": [
                        {"@id": "?i", "ex:kind": "item"},
                        ["optional", {"@id": "?i", "ex:keeps": "?keep"}]
                    ],
                    "delete": template(json!({"@id": "ex:s", "ex:p": "v"})),
                    "insert": template(json!({"@id": "?keep", "ex:p": "v"}))
                }),
            )
            .await;
            let what = format!("JSON-LD, graph={graph:?}, indexed={indexed}");
            no_commit(&r, t, &what);
            assert_eq!(graph_objects(&r.ledger, graph).await, ["v"], "{what}");
        }
    }
}

/// `ex:a ex:v 1 . ex:b ex:v 2`.
fn restated_seed() -> JsonValue {
    json!({"@graph": [{"@id": "ex:a", "ex:v": 1}, {"@id": "ex:b", "ex:v": 2}]})
}

/// Every `?s ex:v ?o`, as `s=o`, sorted.
async fn v_values(fluree: &Fluree, ledger: &LedgerState) -> Vec<String> {
    let q = format!("SELECT ?s ?o WHERE {{ ?s <{EX}v> ?o }}");
    let result = support::query_sparql(fluree, ledger, &q)
        .await
        .expect("query");
    let json = result.to_jsonld(&ledger.snapshot).expect("jsonld");
    let mut rows: Vec<String> = json
        .as_array()
        .expect("rows")
        .iter()
        .map(|r| {
            let s = r[0].as_str().expect("subject");
            format!("{}={}", s.rsplit([':', '/']).next().unwrap_or(s), r[1])
        })
        .collect();
    rows.sort();
    rows
}

/// Replaces every `ex:v` with `ex:a ex:v 1 . ex:b ex:v 3`: the constant
/// INSERT is instantiated on every WHERE row.
async fn replace_v(fluree: &Fluree, ledger: LedgerState, sparql: bool) -> TransactResult {
    if sparql {
        let body =
            "DELETE { ?s ex:v ?o } INSERT { ex:a ex:v 1 . ex:b ex:v 3 } WHERE { ?s ex:v ?o }";
        sparql_update(fluree, ledger, body).await
    } else {
        let txn = json!({
            "where": {"@id": "?s", "ex:v": "?o"},
            "delete": {"@id": "?s", "ex:v": "?o"},
            "insert": [{"@id": "ex:a", "ex:v": 1}, {"@id": "ex:b", "ex:v": 3}]
        });
        jsonld_update(fluree, ledger, txn).await
    }
}

/// Over two rows the INSERT names the unchanged `ex:a ex:v 1` twice while
/// the DELETE retracts it once. The commit holds only the change to `ex:b`,
/// and restating both facts as they are commits nothing.
#[tokio::test]
async fn a_fact_inserted_on_more_rows_than_it_is_deleted_on_is_not_asserted_again() {
    for indexed in [false, true] {
        for sparql in [true, false] {
            let (_d, fluree, ledger) =
                seeded(&format!("restated-{sparql}"), restated_seed(), indexed).await;
            let what = format!("sparql={sparql}, indexed={indexed}");
            let r = replace_v(&fluree, ledger, sparql).await;
            assert_eq!(r.receipt.flake_count, 2, "{what}: retract b=2, assert b=3");
            assert_eq!(v_values(&fluree, &r.ledger).await, ["a=1", "b=3"], "{what}");

            let t = r.ledger.t();
            let r = replace_v(&fluree, r.ledger, sparql).await;
            no_commit(&r, t, &format!("{what}, restating both"));
            assert_eq!(v_values(&fluree, &r.ledger).await, ["a=1", "b=3"], "{what}");
        }
    }
}

// =============================================================================
// Indexed facts are named by their index key
// =============================================================================

/// A big integer past the range of `i64`. The persisted index keys such a
/// value by the value alone: its XSD subtype is not stored.
const BIG: &str = "123456789012345678901234567890";

/// The subtypes the big-integer cells cover, each with a big value it can
/// hold.
fn big_subtypes() -> [(&'static str, String); 4] {
    [
        ("nonNegativeInteger", BIG.to_string()),
        ("positiveInteger", BIG.to_string()),
        ("negativeInteger", format!("-{BIG}")),
        ("unsignedLong", "18446744073709551615".to_string()),
    ]
}

fn node_seed() -> JsonValue {
    json!({"@id": "ex:s", "@type": "ex:Node"})
}

const NODE_ONLY: [&str; 1] = ["type=http://example.org/ns/Node"];

/// A DELETE that names a big integer under its declared subtype deletes it,
/// on a novelty-only ledger and on an indexed one, where the index decodes
/// the value as `xsd:integer`: `DELETE DATA`, a JSON-LD delete, and a
/// constant `DELETE` template alike.
#[tokio::test]
async fn constant_deletes_name_big_integers_under_their_subtype() {
    let mut seed = node_seed();
    let mut del = json!({"@id": "ex:s"});
    let mut triples = Vec::new();
    for (dt, v) in big_subtypes() {
        let term = json!({"@value": v, "@type": format!("xsd:{dt}")});
        seed[format!("ex:{dt}")] = term.clone();
        del[format!("ex:{dt}")] = term;
        triples.push(format!("ex:s ex:{dt} \"{v}\"^^xsd:{dt}"));
    }
    let triples = triples.join(" . ");
    for indexed in [false, true] {
        for form in ["data", "jsonld", "template"] {
            let (_d, fluree, ledger) = seeded(&format!("big-{form}"), seed.clone(), indexed).await;
            let r = match form {
                "data" => {
                    sparql_update(&fluree, ledger, &format!("DELETE DATA {{ {triples} }}")).await
                }
                "template" => {
                    sparql_update(
                        &fluree,
                        ledger,
                        &format!("DELETE {{ {triples} }} WHERE {{ ex:s a ex:Node }}"),
                    )
                    .await
                }
                _ => jsonld_update(&fluree, ledger, json!({"delete": del.clone()})).await,
            };
            assert_eq!(
                facts(&fluree, &r.ledger).await,
                NODE_ONLY,
                "{form}, indexed={indexed}"
            );
            assert_eq!(r.receipt.retract_count, 4, "{form}, indexed={indexed}");
        }
    }
}

/// A novelty retraction of an indexed fact under another identity with the
/// same index key leaves no ghost: a revert retracts a big integer under its
/// declared subtype, which the index row does not keep, and queries stop
/// seeing the value. The stored-fact read used to keep reporting it, so a
/// later upsert of the same value cancelled against it and committed
/// nothing, and the value stayed gone until the next reindex.
#[tokio::test]
async fn a_reverted_indexed_big_integer_can_be_written_again() {
    for dt in ["integer", "nonNegativeInteger"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("file fluree");
        let name = format!("it/delete-stored-revert-{dt}");
        let id = format!("{name}:main");
        let ledger = fluree.create_ledger(&id).await.expect("create");
        let ledger = fluree
            .insert(
                ledger,
                &json!({"@context": ctx(), "@id": "ex:s", "ex:other": 1}),
            )
            .await
            .expect("seed")
            .ledger;
        let value = json!({"@context": ctx(), "@id": "ex:s",
                           "ex:n": {"@value": BIG, "@type": format!("xsd:{dt}")}});
        let inserted = fluree.insert(ledger, &value).await.expect("insert");
        let commit = inserted.receipt.commit_id.clone();
        drop(inserted);
        support::rebuild_and_publish_index(&fluree, &id).await;
        fluree
            .revert_commit(
                &name,
                "main",
                CommitRef::Exact(commit),
                ConflictStrategy::TakeSource,
            )
            .await
            .expect("revert");
        let ledger = fluree.ledger(&id).await.expect("reload");
        assert_eq!(
            facts(&fluree, &ledger).await,
            ["other=1"],
            "xsd:{dt}: reverted"
        );

        let r = fluree.upsert(ledger, &value).await.expect("upsert");
        assert_eq!(
            (r.receipt.assert_count, r.receipt.retract_count),
            (1, 0),
            "xsd:{dt}: the upsert writes the value"
        );
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            [format!("n={BIG}"), "other=1".to_string()],
            "xsd:{dt}: written again"
        );
    }
}

/// A value bound by OPTIONAL, UNION or BIND is not a witness, so its DELETE
/// row is matched against the stored facts of its slot. The row carries the
/// query's decode of the value, which on an indexed ledger has lost a
/// declared subtype, and the row still names the fact.
///
/// Not covered: BIND on a novelty-only ledger with a subtype. BIND gives a
/// big integer `xsd:integer` whatever its declared subtype, so there the row
/// names a term that is not stored. That is a query-side gap, on `main` too.
#[tokio::test]
async fn where_bound_rows_name_big_integers() {
    for indexed in [false, true] {
        for dt in ["integer", "nonNegativeInteger"] {
            let mut seed = node_seed();
            seed["ex:n"] = json!({"@value": BIG, "@type": format!("xsd:{dt}")});
            for (shape, body) in [
                (
                    "optional",
                    "DELETE { ex:s ex:n ?o } WHERE { ex:s a ex:Node OPTIONAL { ex:s ex:n ?o } }",
                ),
                (
                    "union",
                    "DELETE { ex:s ex:n ?o } WHERE { { ex:s ex:n ?o } UNION { ex:s ex:none ?o } }",
                ),
                (
                    "bind",
                    "DELETE { ex:s ex:n ?o } WHERE { ex:s ex:n ?v BIND(?v AS ?o) }",
                ),
            ] {
                if shape == "bind" && dt != "integer" && !indexed {
                    continue;
                }
                let (_d, fluree, ledger) =
                    seeded(&format!("big-{shape}-{dt}"), seed.clone(), indexed).await;
                let r = sparql_update(&fluree, ledger, body).await;
                assert_eq!(
                    facts(&fluree, &r.ledger).await,
                    NODE_ONLY,
                    "{shape}, xsd:{dt}, indexed={indexed}"
                );
            }
            let (_d, fluree, ledger) =
                seeded(&format!("big-optional-jsonld-{dt}"), seed.clone(), indexed).await;
            let r = jsonld_update(
                &fluree,
                ledger,
                json!({
                    "where": [{"@id": "ex:s", "@type": "ex:Node"}, ["optional", {"@id": "ex:s", "ex:n": "?o"}]],
                    "delete": {"@id": "ex:s", "ex:n": "?o"}
                }),
            )
            .await;
            assert_eq!(
                facts(&fluree, &r.ledger).await,
                NODE_ONLY,
                "JSON-LD optional, xsd:{dt}, indexed={indexed}"
            );
        }
    }
}

/// Cypher `SET` replaces a big-integer property and `DETACH DELETE` removes
/// it with its node, on both lanes: both read the old value through an
/// OPTIONAL row.
#[tokio::test]
async fn cypher_set_and_detach_delete_reach_big_integers() {
    for indexed in [false, true] {
        let mut seed = node_seed();
        seed["ex:big"] = json!({"@value": BIG, "@type": "xsd:integer"});

        let (_d, fluree, ledger) = seeded("big-cypher-set", seed.clone(), indexed).await;
        let r = fluree
            .transact_cypher(
                ledger,
                "MATCH (n:`http://example.org/ns/Node`) SET n.`http://example.org/ns/big` = 5",
            )
            .await
            .expect("cypher SET");
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            ["big=5", "type=http://example.org/ns/Node"],
            "SET, indexed={indexed}"
        );

        let (_d, fluree, ledger) = seeded("big-cypher-detach", seed, indexed).await;
        let r = fluree
            .transact_cypher(
                ledger,
                "MATCH (n:`http://example.org/ns/Node`) DETACH DELETE n",
            )
            .await
            .expect("cypher DETACH DELETE");
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "DETACH DELETE, indexed={indexed}: {:?}",
            facts(&fluree, &r.ledger).await
        );
    }
}

/// A `dateTime` or `time` written past the microsecond is deleted by the
/// same literal on both lanes: both are read to the microsecond.
#[tokio::test]
async fn delete_data_names_sub_microsecond_temporals() {
    let seed = json!({
        "@id": "ex:s",
        "ex:d": {"@value": "2020-01-01T00:00:00.123456789Z", "@type": "xsd:dateTime"},
        "ex:t": {"@value": "12:00:00.123456789", "@type": "xsd:time"}
    });
    for indexed in [false, true] {
        let (_d, fluree, ledger) = seeded("sub-us-sparql", seed.clone(), indexed).await;
        let r = sparql_update(
            &fluree,
            ledger,
            "DELETE DATA { ex:s ex:d \"2020-01-01T00:00:00.123456789Z\"^^xsd:dateTime . \
             ex:s ex:t \"12:00:00.123456789\"^^xsd:time }",
        )
        .await;
        assert_eq!(r.receipt.retract_count, 2, "SPARQL, indexed={indexed}");
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "SPARQL, indexed={indexed}"
        );

        let (_d, fluree, ledger) = seeded("sub-us-jsonld", seed.clone(), indexed).await;
        let mut del = seed.clone();
        del["@id"] = json!("ex:s");
        let r = jsonld_update(&fluree, ledger, json!({"delete": del})).await;
        assert_eq!(r.receipt.retract_count, 2, "JSON-LD, indexed={indexed}");
        assert!(
            facts(&fluree, &r.ledger).await.is_empty(),
            "JSON-LD, indexed={indexed}"
        );
    }
}

/// The index key names one value: a DELETE of another big integer under
/// the same subtype, or of the same digits as a decimal, names nothing and
/// commits nothing.
#[tokio::test]
async fn a_different_big_value_is_not_deleted() {
    let mut seed = node_seed();
    seed["ex:n"] = json!({"@value": BIG, "@type": "xsd:nonNegativeInteger"});
    for indexed in [false, true] {
        let (_d, fluree, ledger) = seeded("big-other", seed.clone(), indexed).await;
        let t = ledger.t();
        let r = sparql_update(
            &fluree,
            ledger,
            &format!(
                "DELETE DATA {{ ex:s ex:n \"{BIG}1\"^^xsd:nonNegativeInteger . \
                 ex:s ex:n \"{BIG}.0\"^^xsd:decimal }}"
            ),
        )
        .await;
        no_commit(&r, t, &format!("indexed={indexed}"));
        assert_eq!(
            facts(&fluree, &r.ledger).await,
            [
                format!("n={BIG}"),
                "type=http://example.org/ns/Node".to_string()
            ],
            "indexed={indexed}"
        );
    }
}

/// The index keys a big integer per graph and predicate, so a named graph's
/// fact is named by its own graph's key.
#[tokio::test]
async fn a_delete_names_a_big_integer_in_a_named_graph() {
    let seed = json!({"@graph": [{
        "@id": "ex:s", "@graph": G1,
        "ex:n": {"@value": BIG, "@type": "xsd:nonNegativeInteger"}
    }]});
    for indexed in [false, true] {
        let (_d, fluree, ledger) = seeded("big-graph", seed.clone(), indexed).await;
        assert_eq!(numbig_rows(&fluree, &ledger).await, 1, "precondition");
        let r = sparql_update(
            &fluree,
            ledger,
            &format!(
                "DELETE DATA {{ GRAPH <{G1}> {{ ex:s ex:n \"{BIG}\"^^xsd:nonNegativeInteger }} }}"
            ),
        )
        .await;
        assert_eq!(r.receipt.retract_count, 1, "indexed={indexed}");
        assert_eq!(
            numbig_rows(&fluree, &r.ledger).await,
            0,
            "indexed={indexed}"
        );
    }
}

// =============================================================================
// A WHERE-matched DELETE over a value stored more than once in a slot
// =============================================================================

/// A WHERE-matched DELETE removes every stored entry its WHERE matched: a list
/// holding `"v"` twice empties, as does `"v"` stored both plain and in a list.
/// The WHERE yields one row per stored entry, and identical rows take
/// successive entries; they used to take the first one each, leaving a `"v"`.
#[tokio::test]
async fn a_where_matched_delete_removes_each_entry_of_a_repeated_value() {
    let shapes = [
        (
            "repeated in a list",
            json!({"@id": "ex:s", "ex:items": {"@list": ["v", "v", "w"]}}),
            3,
        ),
        (
            "plain and in a list",
            json!({"@graph": [
                {"@id": "ex:s", "ex:items": "v"},
                {"@id": "ex:s", "ex:items": {"@list": ["v"]}}
            ]}),
            2,
        ),
    ];
    for indexed in [false, true] {
        for (shape, seed, stored) in &shapes {
            for surface in ["SPARQL", "JSON-LD"] {
                let (_d, fluree, ledger) = seeded("repeated-value", seed.clone(), indexed).await;
                let r = if surface == "SPARQL" {
                    sparql_update(&fluree, ledger, "DELETE WHERE { ex:s ex:items ?o }").await
                } else {
                    jsonld_update(
                        &fluree,
                        ledger,
                        json!({
                            "where": {"@id": "ex:s", "ex:items": "?o"},
                            "delete": {"@id": "ex:s", "ex:items": "?o"}
                        }),
                    )
                    .await
                };
                let what = format!("{shape}, {surface}, indexed={indexed}");
                assert_eq!(r.receipt.retract_count, *stored, "{what}");
                assert!(facts(&fluree, &r.ledger).await.is_empty(), "{what}");
            }
        }
    }
}

// =============================================================================
// Modify policy judges every delete a transaction asks for
// =============================================================================

async fn policy(ledger: &LedgerState, opts: GovernanceOptions) -> PolicyContext {
    build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
        &[0],
    )
    .await
    .expect("build policy context")
}

/// The identity may view and modify `ex:name` and nothing else.
async fn name_only(ledger: &LedgerState) -> PolicyContext {
    policy(
        ledger,
        GovernanceOptions {
            policy: Some(json!([{
                "@id": "http://example.org/ns/nameOnly",
                "f:onProperty": [{"@id": "http://example.org/ns/name"}],
                "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
                "f:allow": true
            }])),
            default_allow: Some(false),
            ..Default::default()
        },
    )
    .await
}

/// The identity may modify everything but `ex:ssn`.
async fn all_but_ssn(ledger: &LedgerState) -> PolicyContext {
    policy(
        ledger,
        GovernanceOptions {
            policy: Some(json!([{
                "@id": "http://example.org/ns/denySsn",
                "f:required": true,
                "f:onProperty": [{"@id": "http://example.org/ns/ssn"}],
                "f:action": "f:modify",
                "f:allow": false
            }])),
            default_allow: Some(true),
            ..Default::default()
        },
    )
    .await
}

async fn sparql_update_as(
    fluree: &Fluree,
    ledger: LedgerState,
    body: &str,
    policy: PolicyContext,
) -> Result<TransactResult, fluree_db_api::ApiError> {
    let text =
        format!("PREFIX ex: <{EX}>\nPREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n{body}");
    let parsed = fluree_db_sparql::parse_sparql(&text);
    assert!(!parsed.has_errors(), "{text}: {:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("ast"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .policy(policy)
        .execute()
        .await
}

fn salary_seed() -> JsonValue {
    json!({"@id": "ex:alice", "ex:name": "Alice", "ex:salary": 100})
}

/// A delete the identity may not perform is refused with one answer whether
/// or not the value it names is stored: here `ex:salary 100` is stored and
/// 999 and 50 are not.
#[tokio::test]
async fn delete_refused_by_policy_regardless_of_stored_value() {
    for indexed in [false, true] {
        for body in [
            "DELETE DATA { ex:alice ex:salary 100 }",
            "DELETE DATA { ex:alice ex:salary 999 }",
            "DELETE DATA { ex:alice ex:salary 50 }",
            "DELETE { ?s ex:salary 100 } WHERE { ?s ex:name ?n }",
            "DELETE { ?s ex:salary 999 } WHERE { ?s ex:name ?n }",
        ] {
            let (_d, fluree, ledger) = seeded("policy-sparql", salary_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let err = sparql_update_as(&fluree, ledger, body, policy)
                .await
                .map(|r| r.receipt.flake_count)
                .expect_err(body);
            assert!(
                err.to_string()
                    .contains("Policy enforcement prevents modification"),
                "{body}, indexed={indexed}: {err}"
            );
        }
        for salary in [100, 999] {
            let (_d, fluree, ledger) = seeded("policy-jsonld", salary_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let txn = json!({
                "@context": ctx(),
                "delete": {"@id": "ex:alice", "ex:salary": salary}
            });
            let err = fluree
                .stage_owned(ledger)
                .update(&txn)
                .policy(policy)
                .execute()
                .await
                .map(|r| r.receipt.flake_count)
                .expect_err("JSON-LD delete");
            assert!(
                err.to_string()
                    .contains("Policy enforcement prevents modification"),
                "JSON-LD {salary}, indexed={indexed}: {err}"
            );
        }
    }
}

/// An identity the policy lets modify the value: deleting it retracts it,
/// and deleting a value that is not stored commits nothing.
#[tokio::test]
async fn a_permitted_delete_of_an_absent_value_commits_nothing() {
    for indexed in [false, true] {
        let (_d, fluree, ledger) = seeded("policy-allowed", salary_seed(), indexed).await;
        let t = ledger.t();
        let policy = all_but_ssn(&ledger).await;
        let r = sparql_update_as(
            &fluree,
            ledger,
            "DELETE DATA { ex:alice ex:salary 999 }",
            policy,
        )
        .await
        .expect("permitted");
        no_commit(&r, t, &format!("absent value, indexed={indexed}"));

        let policy = all_but_ssn(&r.ledger).await;
        let r = sparql_update_as(
            &fluree,
            r.ledger,
            "DELETE DATA { ex:alice ex:salary 100 }",
            policy,
        )
        .await
        .expect("permitted");
        assert_eq!(r.receipt.retract_count, 1, "indexed={indexed}");
    }
}

/// The identity may view everything, and `verb` an `ex:Thing` and nothing
/// else.
async fn view_all_and(ledger: &LedgerState, verb: &str) -> PolicyContext {
    policy(
        ledger,
        GovernanceOptions {
            policy: Some(json!([
                {
                    "@id": "http://example.org/ns/viewAll",
                    "f:action": "f:view",
                    "f:allow": true
                },
                {
                    "@id": "http://example.org/ns/verbOnThing",
                    "f:onClass": [{"@id": "http://example.org/ns/Thing"}],
                    "f:action": verb,
                    "f:allow": true
                }
            ])),
            default_allow: Some(false),
            ..Default::default()
        },
    )
    .await
}

/// A write-verb policy tells a subject's full removal by fact identity, as
/// storage holds the facts. An indexed `x^^xsd:integer` with
/// `x^^xsd:nonNegativeInteger` written since the build is one stored fact:
/// the pre-state read returns the indexed term, and the removal retracts the
/// newer one. Removing every fact of the subject (with a language-tagged
/// value and a list beside it) is a delete, so a delete-only grant permits
/// it and an update-only grant does not, on SPARQL and JSON-LD. Never
/// indexed, both terms are read and retracted. Each attempt has a ledger of
/// its own, read cold, with no background indexer.
#[tokio::test]
async fn a_full_removal_is_a_delete_whichever_term_names_a_fact() {
    let mut failures = Vec::new();
    for indexed in [false, true] {
        for surface in ["SPARQL", "JSON-LD"] {
            for (verb, permitted) in [("f:update", false), ("f:delete", true)] {
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().to_string_lossy().to_string();
                let open = || {
                    FlureeBuilder::file(path.clone())
                        .without_indexing()
                        .build()
                        .expect("file fluree")
                };
                let id = "it/delete-stored-lifecycle:main";
                let fluree = open();
                let ledger = fluree.create_ledger(id).await.expect("create");
                // A language tag and list positions are part of a fact's
                // identity, so the removal has to name those as stored too.
                let seed = json!({
                    "@context": ctx(),
                    "@id": "ex:s",
                    "@type": "ex:Thing",
                    "ex:p": {"@value": BIG, "@type": "xsd:integer"},
                    "ex:label": {"@value": "Al", "@language": "en"},
                    "ex:scores": {"@list": [1, 2]}
                });
                fluree.insert(ledger, &seed).await.expect("seed");
                if indexed {
                    support::rebuild_and_publish_index(&fluree, id).await;
                }
                let fluree = open();
                let ledger = fluree.ledger(id).await.expect("ledger");
                assert_eq!(ledger.snapshot.range_provider.is_some(), indexed);
                let since =
                    format!("INSERT DATA {{ ex:s ex:p \"{BIG}\"^^xsd:nonNegativeInteger }}");
                sparql_update(&fluree, ledger, &since).await;
                let fluree = open();
                let ledger = fluree.ledger(id).await.expect("ledger");
                let policy = view_all_and(&ledger, verb).await;
                let r = if surface == "SPARQL" {
                    sparql_update_as(&fluree, ledger, "DELETE WHERE { ex:s ?p ?o }", policy).await
                } else {
                    let txn = json!({
                        "@context": ctx(),
                        "where": {"@id": "ex:s", "?p": "?o"},
                        "delete": {"@id": "ex:s", "?p": "?o"}
                    });
                    fluree
                        .stage_owned(ledger)
                        .update(&txn)
                        .policy(policy)
                        .execute()
                        .await
                };
                let what = format!("{surface} under {verb} alone, indexed={indexed}");
                match (permitted, r) {
                    (false, r) => failures.extend(not_refused(r, &what)),
                    (true, Err(e)) => failures.push(format!("{what}: {e}")),
                    (true, Ok(r)) => {
                        let left = facts(&fluree, &r.ledger).await;
                        if !left.is_empty() {
                            failures.push(format!("{what}: left {left:?}"));
                        }
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

// =============================================================================
// Modify policy judges a restated value the identity cannot view
// =============================================================================

/// `ex:alice` with a plain value, a language-tagged one and a list.
fn restate_seed() -> JsonValue {
    json!({
        "@id": "ex:alice",
        "ex:name": "Alice",
        "ex:salary": 100,
        "ex:label": {"@value": "Al", "@language": "en"},
        "ex:scores": {"@list": [1, 2, 3]}
    })
}

/// The identity may view everything and modify only `ex:name`.
async fn view_all_modify_name(ledger: &LedgerState) -> PolicyContext {
    policy(
        ledger,
        GovernanceOptions {
            policy: Some(json!([
                {
                    "@id": "http://example.org/ns/viewAll",
                    "@type": "f:AccessPolicy",
                    "f:action": "f:view",
                    "f:allow": true
                },
                {
                    "@id": "http://example.org/ns/modifyName",
                    "@type": "f:AccessPolicy",
                    "f:onProperty": [{"@id": "http://example.org/ns/name"}],
                    "f:action": "f:modify",
                    "f:allow": true
                }
            ])),
            default_allow: Some(false),
            ..Default::default()
        },
    )
    .await
}

/// Upserts that restate a stored value, and their twins that write a value
/// that is not stored.
fn restating_upserts() -> Vec<(&'static str, JsonValue)> {
    vec![
        (
            "plain, stored",
            json!({"@id": "ex:alice", "ex:salary": 100}),
        ),
        (
            "plain, not stored",
            json!({"@id": "ex:alice", "ex:salary": 999}),
        ),
        (
            "language-tagged, stored",
            json!({"@id": "ex:alice", "ex:label": {"@value": "Al", "@language": "en"}}),
        ),
        (
            "language-tagged, not stored",
            json!({"@id": "ex:alice", "ex:label": {"@value": "Bo", "@language": "en"}}),
        ),
        (
            "list, stored",
            json!({"@id": "ex:alice", "ex:scores": {"@list": [1, 2, 3]}}),
        ),
        (
            "list, not stored",
            json!({"@id": "ex:alice", "ex:scores": {"@list": [1, 2, 4]}}),
        ),
    ]
}

async fn upsert_as(
    fluree: &Fluree,
    ledger: LedgerState,
    mut doc: JsonValue,
    policy: Option<PolicyContext>,
) -> Result<TransactResult, fluree_db_api::ApiError> {
    doc["@context"] = ctx();
    let builder = fluree.stage_owned(ledger).upsert(&doc);
    match policy {
        Some(policy) => builder.policy(policy).execute().await,
        None => builder.execute().await,
    }
}

/// `None` when `result` is the modify-policy refusal, else what happened.
fn not_refused(
    result: Result<TransactResult, fluree_db_api::ApiError>,
    what: &str,
) -> Option<String> {
    match result {
        Ok(r) => Some(format!(
            "{what}: committed {} flakes",
            r.receipt.flake_count
        )),
        Err(e)
            if e.to_string()
                .contains("Policy enforcement prevents modification") =>
        {
            None
        }
        Err(e) => Some(format!("{what}: {e}")),
    }
}

/// An upsert the identity may not perform is refused with one answer
/// whether it restates a stored value or writes one that is not stored:
/// plain, language-tagged and list values, on JSON-LD and Turtle. The
/// identity may neither view nor modify them.
#[tokio::test]
async fn upsert_refused_by_policy_regardless_of_stored_value() {
    let mut failures = Vec::new();
    for indexed in [false, true] {
        for (what, doc) in restating_upserts() {
            let (_d, fluree, ledger) = seeded("restate-upsert", restate_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let r = upsert_as(&fluree, ledger, doc, Some(policy)).await;
            failures.extend(not_refused(
                r,
                &format!("JSON-LD {what}, indexed={indexed}"),
            ));
        }
        for salary in [100, 999] {
            let (_d, fluree, ledger) = seeded("restate-turtle", restate_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let ttl = format!("@prefix ex: <{EX}> .\nex:alice ex:salary {salary} .\n");
            let r = fluree
                .stage_owned(ledger)
                .upsert_turtle(&ttl)
                .policy(policy)
                .execute()
                .await;
            failures.extend(not_refused(
                r,
                &format!("Turtle {salary}, indexed={indexed}"),
            ));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A DELETE and INSERT of one value is refused with one answer whether or
/// not the value is stored, on SPARQL and JSON-LD; so is a graph sync whose
/// payload restates a graph the identity cannot view.
#[tokio::test]
async fn delete_insert_of_one_value_refused_by_policy_regardless_of_stored_value() {
    let mut failures = Vec::new();
    for indexed in [false, true] {
        for salary in [100, 999] {
            let (_d, fluree, ledger) = seeded("restate-sparql", restate_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let body = format!(
                "DELETE {{ ?s ex:salary {salary} }} INSERT {{ ?s ex:salary {salary} }} \
                 WHERE {{ ?s ex:name \"Alice\" }}"
            );
            let r = sparql_update_as(&fluree, ledger, &body, policy).await;
            failures.extend(not_refused(
                r,
                &format!("SPARQL {salary}, indexed={indexed}"),
            ));

            let (_d, fluree, ledger) = seeded("restate-jsonld", restate_seed(), indexed).await;
            let policy = name_only(&ledger).await;
            let txn = json!({
                "@context": ctx(),
                "where": {"@id": "?s", "ex:name": "Alice"},
                "delete": {"@id": "?s", "ex:salary": salary},
                "insert": {"@id": "?s", "ex:salary": salary}
            });
            let r = fluree
                .stage_owned(ledger)
                .update(&txn)
                .policy(policy)
                .execute()
                .await;
            failures.extend(not_refused(
                r,
                &format!("JSON-LD {salary}, indexed={indexed}"),
            ));
        }
    }
    for salary in [100, 999] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger = fluree
            .create_ledger("it/delete-stored-restate-sync:main")
            .await
            .expect("create");
        let payload = |salary: i64| {
            json!({
                "@context": ctx(),
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice", "ex:salary": salary}]
            })
        };
        let seed = payload(100);
        let ledger = fluree
            .stage_owned(ledger)
            .sync_graph(G1, &seed)
            .execute()
            .await
            .expect("seed the graph")
            .ledger;
        let policy = name_only(&ledger).await;
        let next = payload(salary);
        let r = fluree
            .stage_owned(ledger)
            .sync_graph(G1, &next)
            .policy(policy)
            .execute()
            .await;
        failures.extend(not_refused(r, &format!("graph sync {salary}")));
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A transfer judges a fact the destination already holds as a restatement:
/// `ADD` or `COPY` of a value the identity can neither view nor modify is
/// refused with one answer whether or not the destination holds it. A held
/// fact used to stage nothing and commit, so the outcome told the identity
/// what the destination held.
#[tokio::test]
async fn transfer_refused_by_policy_regardless_of_destination_contents() {
    const G2: &str = "http://example.org/g2";
    for indexed in [false, true] {
        for dest_holds in [true, false] {
            for verb in ["ADD", "COPY"] {
                let dir = tempfile::tempdir().expect("tempdir");
                let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
                    .build()
                    .expect("file fluree");
                let id = format!("it/delete-stored-transfer-{verb}-{dest_holds}-{indexed}:main");
                let ledger = fluree.create_ledger(&id).await.expect("create");
                let dest = if dest_holds {
                    "ex:alice ex:salary 100"
                } else {
                    "ex:alice ex:name \"Alice\""
                };
                let ledger = sparql_update(
                    &fluree,
                    ledger,
                    &format!(
                        "INSERT DATA {{ GRAPH <{G1}> {{ ex:alice ex:salary 100 }} \
                         GRAPH <{G2}> {{ {dest} }} }}"
                    ),
                )
                .await
                .ledger;
                let ledger = if indexed {
                    drop(ledger);
                    support::rebuild_and_publish_index(&fluree, &id).await;
                    fluree.ledger(&id).await.expect("reload")
                } else {
                    ledger
                };
                let policy = name_only(&ledger).await;
                let body = format!("{verb} GRAPH <{G1}> TO GRAPH <{G2}>");
                let err = sparql_update_as(&fluree, ledger, &body, policy)
                    .await
                    .map(|r| r.receipt.flake_count)
                    .expect_err(&body);
                assert!(
                    err.to_string()
                        .contains("Policy enforcement prevents modification"),
                    "{body}, destination holds it={dest_holds}, indexed={indexed}: {err}"
                );
            }
        }
    }
}

/// Restating a value the identity can view commits nothing, without modify
/// permission, as it did before; a changed value is still refused. As root,
/// a restatement commits nothing either.
#[tokio::test]
async fn restating_a_viewable_value_commits_nothing_without_modify() {
    for indexed in [false, true] {
        for (what, doc) in restating_upserts() {
            let stored = what.ends_with(", stored");
            let (_d, fluree, ledger) = seeded("restate-viewable", restate_seed(), indexed).await;
            let t = ledger.t();
            let policy = view_all_modify_name(&ledger).await;
            let r = upsert_as(&fluree, ledger, doc.clone(), Some(policy)).await;
            let what = format!("{what}, indexed={indexed}");
            if stored {
                no_commit(&r.expect(&what), t, &what);
            } else {
                assert_eq!(not_refused(r, &what), None);
            }
            if stored {
                let (_d, fluree, ledger) = seeded("restate-root", restate_seed(), indexed).await;
                let t = ledger.t();
                let r = upsert_as(&fluree, ledger, doc, None).await;
                no_commit(&r.expect(&what), t, &format!("root, {what}"));
            }
        }
        let (_d, fluree, ledger) = seeded("restate-viewable-sparql", restate_seed(), indexed).await;
        let t = ledger.t();
        let policy = view_all_modify_name(&ledger).await;
        let r = sparql_update_as(
            &fluree,
            ledger,
            "DELETE { ?s ex:salary 100 } INSERT { ?s ex:salary 100 } WHERE { ?s ex:name \"Alice\" }",
            policy,
        )
        .await
        .expect("restating a viewable value");
        no_commit(&r, t, &format!("SPARQL, indexed={indexed}"));
    }
}

const G2: &str = "http://example.org/g2";

/// `G1` holding Alice's name and salary, and with `dest_holds` `G2` holding
/// the same facts; optionally indexed.
async fn transfer_seed(
    name: &str,
    dest_holds: bool,
    indexed: bool,
) -> (tempfile::TempDir, Fluree, LedgerState) {
    let alice = r#"ex:alice ex:name "Alice" ; ex:salary 100 ."#;
    let dest = if dest_holds {
        format!("GRAPH <{G2}> {{ {alice} }}")
    } else {
        String::new()
    };
    seeded_with(
        name,
        &format!("INSERT DATA {{ GRAPH <{G1}> {{ {alice} }} {dest} }}"),
        indexed,
    )
    .await
}

/// ADD, COPY and MOVE into a graph are refused with one answer whether or
/// not the destination already holds the source's facts, the source holding
/// a name the identity may change beside the salary it may not. A fact the
/// destination holds is restated, not skipped unseen: when the identity
/// cannot view it, both sides go to the modify check, as for upsert and
/// sync. (MOVE also retracts the source, which this identity may not.)
#[tokio::test]
async fn add_copy_and_move_are_refused_regardless_of_destination_contents() {
    let mut failures = Vec::new();
    for indexed in [false, true] {
        for dest_holds in [false, true] {
            for verb in ["ADD", "COPY", "MOVE"] {
                let (_d, fluree, ledger) =
                    transfer_seed("restate-transfer", dest_holds, indexed).await;
                let policy = name_only(&ledger).await;
                let r =
                    sparql_update_as(&fluree, ledger, &format!("{verb} <{G1}> TO <{G2}>"), policy)
                        .await;
                failures.extend(not_refused(
                    r,
                    &format!("{verb}, dest_holds={dest_holds}, indexed={indexed}"),
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// A destination fact the identity can view, restated by ADD or COPY,
/// commits nothing without modify permission, as before; as root as well.
#[tokio::test]
async fn transferring_viewable_facts_the_destination_holds_commits_nothing() {
    for indexed in [false, true] {
        for verb in ["ADD", "COPY"] {
            let body = format!("{verb} <{G1}> TO <{G2}>");
            let (_d, fluree, ledger) = transfer_seed("restate-transfer-view", true, indexed).await;
            let t = ledger.t();
            let policy = view_all_modify_name(&ledger).await;
            let r = sparql_update_as(&fluree, ledger, &body, policy)
                .await
                .expect(&body);
            no_commit(&r, t, &format!("{body}, indexed={indexed}"));

            let (_d, fluree, ledger) = transfer_seed("restate-transfer-root", true, indexed).await;
            let t = ledger.t();
            let r = sparql_update(&fluree, ledger, &body).await;
            no_commit(&r, t, &format!("root, {body}, indexed={indexed}"));
        }
    }
}

// =============================================================================
// A DELETE naming a graph the ledger does not have
// =============================================================================

/// A DELETE naming a graph the ledger does not have commits nothing and
/// registers no graph: nothing is stored there to retract. An INSERT into a
/// new graph, a `CREATE GRAPH`, and a DELETE beside an INSERT into the same
/// new graph still register it, and the graph a transaction inserts into
/// beside a DELETE-only new graph gets the id the commit gives it.
#[tokio::test]
async fn a_delete_naming_a_graph_the_ledger_lacks_commits_nothing() {
    const NEW: &str = "http://example.org/new";
    const ALSO_NEW: &str = "http://example.org/also-new";
    let registered = |ledger: &LedgerState, iri: &str| {
        ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .is_some()
    };
    for indexed in [false, true] {
        for body in [
            format!("DELETE DATA {{ GRAPH <{NEW}> {{ ex:alice ex:salary 100 }} }}"),
            format!("DELETE {{ GRAPH <{NEW}> {{ ?s ex:salary ?o }} }} WHERE {{ ?s ex:salary ?o }}"),
            format!("WITH <{NEW}> DELETE {{ ?s ex:salary ?o }} WHERE {{ ?s ex:salary ?o }}"),
            format!(
                "DELETE {{ GRAPH ?g {{ ?s ex:salary ?o }} }} \
                 WHERE {{ ?s ex:salary ?o VALUES ?g {{ <{NEW}> }} }}"
            ),
        ] {
            let (_d, fluree, ledger) = seeded("missing-graph", salary_seed(), indexed).await;
            let t = ledger.t();
            let r = sparql_update(&fluree, ledger, &body).await;
            no_commit(&r, t, &format!("{body}, indexed={indexed}"));
            assert!(
                !registered(&r.ledger, NEW),
                "{body}, indexed={indexed}: registered"
            );
        }
        for body in [
            format!("INSERT DATA {{ GRAPH <{NEW}> {{ ex:alice ex:salary 1 }} }}"),
            format!("CREATE GRAPH <{NEW}>"),
            format!(
                "DELETE {{ GRAPH <{NEW}> {{ ?s ex:salary ?o }} }} \
                 INSERT {{ GRAPH <{NEW}> {{ ?s ex:salary 1 }} }} WHERE {{ ?s ex:salary ?o }}"
            ),
        ] {
            let (_d, fluree, ledger) = seeded("new-graph", salary_seed(), indexed).await;
            let r = sparql_update(&fluree, ledger, &body).await;
            assert!(
                registered(&r.ledger, NEW),
                "{body}, indexed={indexed}: not registered"
            );
        }

        // `ALSO_NEW` sorts before `NEW`: leaving it out of the commit must not
        // shift the id the inserted graph's flakes were routed to.
        let (_d, fluree, ledger) = seeded("new-graph-beside", salary_seed(), indexed).await;
        let r = sparql_update(
            &fluree,
            ledger,
            &format!(
                "DELETE {{ GRAPH <{ALSO_NEW}> {{ ?s ex:salary ?o }} }} \
                 INSERT {{ GRAPH <{NEW}> {{ ?s ex:salary 1 }} }} WHERE {{ ?s ex:salary ?o }}"
            ),
        )
        .await;
        assert!(!registered(&r.ledger, ALSO_NEW), "indexed={indexed}");
        let q = format!("SELECT ?o WHERE {{ GRAPH <{NEW}> {{ ex:alice ex:salary ?o }} }}");
        let rows = support::query_sparql(&fluree, &r.ledger, &format!("PREFIX ex: <{EX}>\n{q}"))
            .await
            .expect("query")
            .to_jsonld(&r.ledger.snapshot)
            .expect("jsonld");
        assert_eq!(rows, json!([[1]]), "indexed={indexed}");
    }
}

// =============================================================================
// A witness reads the graph its template writes
// =============================================================================

/// The strings stored as `ex:s ex:p` in the graph the ledger has under
/// `iri` (`None`: the default graph), read at the flake level: a query's
/// `GRAPH <iri>` may resolve the name another way.
async fn graph_objects(ledger: &LedgerState, iri: Option<&str>) -> Vec<String> {
    let g_id = match iri {
        None => 0,
        Some(iri) => ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .expect("graph"),
    };
    let s = ledger.snapshot.encode_iri(&format!("{EX}s")).expect("s");
    let p = ledger.snapshot.encode_iri(&format!("{EX}p")).expect("p");
    let flakes = range_with_overlay(
        &ledger.snapshot,
        g_id,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(s, p),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range");
    let mut out: Vec<String> = flakes
        .iter()
        .filter(|f| f.op)
        .map(|f| match &f.o {
            FlakeValue::String(v) => v.clone(),
            other => format!("{other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// Rebuild and publish a branch's index, reading its commits through the
/// branch's ancestry (the commits before its branch point are its source's).
async fn rebuild_and_publish_branch_index(fluree: &Fluree, ledger_id: &str) {
    let record = fluree
        .nameservice()
        .lookup(ledger_id)
        .await
        .expect("nameservice lookup")
        .expect("ledger record should exist");
    let store = fluree
        .branched_content_store(ledger_id)
        .await
        .expect("branched content store");
    let result = fluree_db_indexer::rebuild_index_from_commits(
        store,
        ledger_id,
        &record,
        fluree_db_indexer::IndexerConfig::default(),
    )
    .await
    .expect("index rebuild should succeed");
    fluree
        .publisher()
        .expect("read-write nameservice")
        .publish_index(ledger_id, result.index_t, &result.root_id)
        .await
        .expect("publish index");
}

/// A ledger branch with a named graph registered under its own address: the
/// legacy layout, built the one way still open (`main` writes a graph named
/// by the branch's address, which on `main` is just a graph IRI, and the
/// branch inherits it). The update reads `USING <address>`, the default
/// graph, and its templates write `GRAPH <address#address>`, the legacy
/// graph. A WHERE row witnesses a template only when the graph the WHERE
/// read is the graph the template writes, compared as graph ids, so the
/// row's value is inserted into the legacy graph and the default graph is
/// left alone.
#[tokio::test]
async fn a_graph_named_by_the_ledger_address_gets_what_its_templates_write() {
    for indexed in [false, true] {
        let dir = tempfile::tempdir().expect("tempdir");
        let fluree = FlureeBuilder::file(dir.path().to_string_lossy().to_string())
            .build()
            .expect("file fluree");
        let name = format!("legacyaddress{indexed}");
        let id = format!("{name}:dev");
        let main = fluree
            .create_ledger(&format!("{name}:main"))
            .await
            .expect("create");
        let trig = format!(
            "@prefix ex: <{EX}> .\nex:s ex:p \"d\" .\nGRAPH <{id}> {{ ex:s ex:p \"l\" . }}\n"
        );
        fluree.insert_turtle(main, &trig).await.expect("seed");
        fluree
            .create_branch(&name, "dev", None, None)
            .await
            .expect("create the branch");
        let mut ledger = fluree.ledger(&id).await.expect("load the branch");
        assert!(
            ledger
                .snapshot
                .graph_registry
                .graph_id_for_iri(&id)
                .is_some(),
            "the fixture registers a graph under the ledger's address"
        );
        if indexed {
            drop(ledger);
            rebuild_and_publish_branch_index(&fluree, &id).await;
            ledger = fluree.ledger(&id).await.expect("reload");
        }
        let r = sparql_update(
            &fluree,
            ledger,
            &format!(
                "DELETE {{ GRAPH <{id}#{id}> {{ ?s ex:p ?o }} }} \
                 INSERT {{ GRAPH <{id}#{id}> {{ ?s ex:p \"d\" }} }} \
                 USING <{id}> WHERE {{ ?s ex:p ?o }}"
            ),
        )
        .await;
        assert!(
            graph_objects(&r.ledger, Some(&id))
                .await
                .contains(&"d".to_string()),
            "indexed={indexed}: {:?}",
            graph_objects(&r.ledger, Some(&id)).await
        );
        assert_eq!(
            graph_objects(&r.ledger, None).await,
            ["d"],
            "indexed={indexed}"
        );
    }
}

// =============================================================================
// A write that restates a stored value keeps it
// =============================================================================

/// One write that retracts a stored value and asserts it again, as written,
/// keeps it. On an indexed ledger the delete side names the value as the
/// index stores it, which holds less than the write for a big integer with
/// an XSD subtype; the two are still one fact to storage, so they cancel, as
/// a value stored exactly as written does (as a `dateTime` or `time` written
/// past the microsecond now is, read to the microsecond on both sides):
/// - `DELETE { x } INSERT { x } WHERE {}` and its JSON-LD twin commit
///   nothing (`retractions.md`'s example);
/// - an upsert or a graph sync of a payload that holds `x` keeps it.
///
/// Read cold, after the write and again after the next index build, with
/// the slot held by the index alone or with a novelty op beside it.
#[tokio::test]
async fn a_write_that_restates_a_stored_value_keeps_it() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    const TTL_PFX: &str = "@prefix ex: <http://example.org/ns/> .\n\
                           @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
    const G: &str = "http://example.org/ns/g";
    // (SPARQL/Turtle literal, JSON-LD value). The last two are stored as
    // written and are the controls.
    let values = [
        (
            "\"123456789012345678901234567890\"^^xsd:nonNegativeInteger",
            json!({"@value": "123456789012345678901234567890", "@type": "xsd:nonNegativeInteger"}),
        ),
        (
            "\"2020-01-01T00:00:00.123456789Z\"^^xsd:dateTime",
            json!({"@value": "2020-01-01T00:00:00.123456789Z", "@type": "xsd:dateTime"}),
        ),
        (
            "\"12:00:00.123456789\"^^xsd:time",
            json!({"@value": "12:00:00.123456789", "@type": "xsd:time"}),
        ),
        (
            "\"2020-01-01T00:00:00.123456Z\"^^xsd:dateTime",
            json!({"@value": "2020-01-01T00:00:00.123456Z", "@type": "xsd:dateTime"}),
        ),
        ("\"x\"", json!("x")),
    ];
    let lanes: [(&str, bool); 7] = [
        ("DELETE { x } INSERT { x } WHERE {}", false),
        ("DELETE { x } INSERT { x } WHERE {}", true),
        ("JSON-LD delete and insert", false),
        ("JSON-LD delete and insert", true),
        ("JSON-LD upsert", false),
        ("Turtle upsert", false),
        ("graph sync", true),
    ];
    let mut failures = Vec::new();
    for (lit, jv) in &values {
        for novelty_beside in [false, true] {
            for (lane, named) in lanes {
                let wrap = |triples: &str| {
                    if named {
                        format!("GRAPH <{G}> {{ {triples} }}")
                    } else {
                        triples.to_string()
                    }
                };
                let dir = tempfile::tempdir().expect("tempdir");
                let path = dir.path().to_string_lossy().to_string();
                let id = "it/restated-lossy:main";
                {
                    let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
                    fluree.create_ledger(id).await.expect("create");
                    let seed = format!(
                        "{PFX}INSERT DATA {{ {} }}",
                        wrap(&format!("ex:s ex:p {lit} ; ex:k \"keep\" ."))
                    );
                    fluree
                        .graph(id)
                        .transact()
                        .sparql_update(&seed)
                        .commit()
                        .await
                        .expect("seed");
                    support::rebuild_and_publish_index(&fluree, id).await;
                    if novelty_beside {
                        let sib = format!("{PFX}INSERT DATA {{ {} }}", wrap("ex:s ex:p \"sib\" ."));
                        fluree
                            .graph(id)
                            .transact()
                            .sparql_update(&sib)
                            .commit()
                            .await
                            .expect("novelty beside");
                    }
                }
                let read = |path: String| {
                    let q = format!(
                        "{PFX}SELECT ?o (DATATYPE(?o) AS ?dt) WHERE {{ {} }}",
                        wrap("ex:s ex:p ?o .")
                    );
                    async move {
                        let fluree = FlureeBuilder::file(path).build().expect("reload");
                        let ledger = fluree.ledger(id).await.expect("ledger");
                        let json = support::query_sparql(&fluree, &ledger, &q)
                            .await
                            .expect("query")
                            .to_sparql_json(&ledger.snapshot)
                            .expect("json");
                        let mut rows: Vec<String> = json["results"]["bindings"]
                            .as_array()
                            .expect("bindings")
                            .iter()
                            .map(|b| {
                                format!(
                                    "{}^^{}",
                                    b["o"]["value"].as_str().unwrap_or("?"),
                                    b["dt"]["value"].as_str().unwrap_or("")
                                )
                            })
                            .collect();
                        rows.sort();
                        rows
                    }
                };
                let before = read(path.clone()).await;
                let restates = !lane.ends_with("upsert") && lane != "graph sync";
                let flakes = {
                    let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                    let graph = fluree.graph(id);
                    let tx = graph.transact();
                    let x = wrap(&format!("ex:s ex:p {lit} ."));
                    let update = format!("{PFX}DELETE {{ {x} }} INSERT {{ {x} }} WHERE {{}}");
                    let node = json!({"@id": "ex:s", "ex:p": jv});
                    let delins = if named {
                        json!({"@context": ctx(), "graph": G, "delete": node, "insert": node})
                    } else {
                        json!({"@context": ctx(), "delete": node, "insert": node})
                    };
                    let upsert = json!({"@context": ctx(), "@id": "ex:s", "ex:p": jv});
                    let ttl = format!("{TTL_PFX}ex:s ex:p {lit} .\n");
                    let synced = json!({
                        "@context": ctx(),
                        "@graph": [{"@id": "ex:s", "ex:p": jv, "ex:k": "keep"}]
                    });
                    if lane == "graph sync" {
                        let report = fluree
                            .sync_named_graph(id, G, &synced, SyncGraphOpts::default())
                            .await
                            .expect("sync");
                        report.asserted + report.retracted
                    } else {
                        let tx = match lane {
                            "JSON-LD delete and insert" => tx.update(&delins),
                            "JSON-LD upsert" => tx.upsert(&upsert),
                            "Turtle upsert" => tx.upsert_turtle(&ttl),
                            _ => tx.sparql_update(&update),
                        };
                        tx.commit().await.expect("write").receipt.flake_count
                    }
                };
                let after = read(path.clone()).await;
                {
                    let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                    support::rebuild_and_publish_index(&fluree, id).await;
                }
                let reindexed = read(path.clone()).await;
                // The upserts and the sync replace `ex:p`'s values with the
                // payload's, so a novelty value beside `x` goes.
                let want: Vec<String> = before
                    .iter()
                    .filter(|v| restates || !v.starts_with("sib^^"))
                    .cloned()
                    .collect();
                let committed_nothing = flakes == 0;
                if after != want || reindexed != want || (restates && !committed_nothing) {
                    failures.push(format!(
                        "{lit}, {lane}{}{}: flakes {}, values {after:?}, after reindex \
                         {reindexed:?}; expected {want:?}{}",
                        if named { " in a named graph" } else { "" },
                        if novelty_beside {
                            ", novelty beside"
                        } else {
                            ""
                        },
                        flakes,
                        if restates { " and no commit" } else { "" },
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The values of `ex:s`'s `pred`, read cold from the ledger at `path`.
async fn values_of(path: &str, id: &str, pred: &str) -> Vec<String> {
    let fluree = FlureeBuilder::file(path.to_string())
        .without_indexing()
        .build()
        .expect("reload");
    let ledger = fluree.ledger(id).await.expect("ledger");
    let q = format!("PREFIX ex: <{EX}>\nSELECT ?o WHERE {{ ex:s {pred} ?o }}");
    let json = support::query_sparql(&fluree, &ledger, &q)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    let mut values: Vec<String> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["o"]["value"].as_str().unwrap_or("?").to_string())
        .collect();
    values.sort();
    values
}

/// An integer too large for 64 bits is one stored fact per value, whatever
/// XSD subtype it is written with: the index keys it by value. So a write
/// that retracts it under one subtype and asserts it under another commits
/// nothing and keeps it, whether the index holds it, holds the value for
/// another subject only, has not seen it since its last build, or does not
/// exist yet; and it is still there after the next index build. In one
/// operation (SPARQL and JSON-LD) and across two of one request.
#[tokio::test]
async fn changing_a_big_integers_subtype_keeps_it() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    const BIG: &str = "123456789012345678901234567890";
    let nonneg = format!("\"{BIG}\"^^xsd:nonNegativeInteger");
    let posint = format!("\"{BIG}\"^^xsd:positiveInteger");
    // (case, indexed seed, novelty seed after the index build; no build
    // without an indexed seed)
    let cases = [
        (
            "the index holds it",
            format!("ex:s ex:p {nonneg} ; ex:k \"keep\" ."),
            String::new(),
        ),
        (
            "written since the last index build, which holds the value for another subject",
            format!("ex:t ex:p {nonneg} . ex:s ex:k \"keep\" ."),
            format!("ex:s ex:p {nonneg} ."),
        ),
        (
            "written since the last index build, which never saw the value",
            "ex:t ex:p 1 . ex:s ex:k \"keep\" .".to_string(),
            format!("ex:s ex:p {nonneg} ."),
        ),
        (
            "never indexed",
            String::new(),
            format!("ex:s ex:p {nonneg} ; ex:k \"keep\" ."),
        ),
    ];
    let jv = |dt: &str| json!({"@value": BIG, "@type": format!("xsd:{dt}")});
    let lanes = [
        "DELETE { x } INSERT { y } WHERE {}",
        "JSON-LD delete and insert",
        "DELETE DATA { x } ; INSERT DATA { y }",
    ];
    let mut failures = Vec::new();
    for (case, indexed, novelty) in &cases {
        for lane in lanes {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let id = "it/big-subtype:main";
            {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
                fluree.create_ledger(id).await.expect("create");
                for (seed, index) in [(indexed, true), (novelty, false)] {
                    if seed.is_empty() {
                        continue;
                    }
                    fluree
                        .graph(id)
                        .transact()
                        .sparql_update(&format!("{PFX}INSERT DATA {{ {seed} }}"))
                        .commit()
                        .await
                        .expect("seed");
                    if index {
                        support::rebuild_and_publish_index(&fluree, id).await;
                    }
                }
            }
            let flakes = {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                let graph = fluree.graph(id);
                let tx = graph.transact();
                let update = match lane {
                    "DELETE { x } INSERT { y } WHERE {}" => format!(
                        "{PFX}DELETE {{ ex:s ex:p {nonneg} }} INSERT {{ ex:s ex:p {posint} }} \
                         WHERE {{}}"
                    ),
                    _ => format!(
                        "{PFX}DELETE DATA {{ ex:s ex:p {nonneg} }} ;\n\
                         INSERT DATA {{ ex:s ex:p {posint} }}"
                    ),
                };
                let delins = json!({
                    "@context": ctx(),
                    "delete": {"@id": "ex:s", "ex:p": jv("nonNegativeInteger")},
                    "insert": {"@id": "ex:s", "ex:p": jv("positiveInteger")}
                });
                let tx = match lane {
                    "JSON-LD delete and insert" => tx.update(&delins),
                    _ => tx.sparql_update(&update),
                };
                tx.commit().await.expect("write").receipt.flake_count
            };
            let after = values_of(&path, id, "ex:p").await;
            {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                support::rebuild_and_publish_index(&fluree, id).await;
            }
            let reindexed = values_of(&path, id, "ex:p").await;
            if flakes != 0 || after != [BIG] || reindexed != [BIG] {
                failures.push(format!(
                    "{case}, {lane}: flakes {flakes}, values {after:?}, after reindex \
                     {reindexed:?}; expected no commit and [{BIG}]"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A list that holds one value twice holds two facts, one per position,
/// for the kinds of value storage keys by value too (a double, an integer
/// too large for 64 bits). A write that retracts the value and asserts
/// something else retracts both entries, before and after an index build.
#[tokio::test]
async fn a_write_retracting_a_value_a_list_holds_twice_retracts_both_entries() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    const TTL_PFX: &str = "@prefix ex: <http://example.org/ns/> .\n\
                           @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
    const BIG: &str = "\"123456789012345678901234567890\"^^xsd:integer";
    let mut failures = Vec::new();
    for (list, v) in [
        ("( 1.5E0 1.5E0 2.5E0 )".to_string(), "1.5E0"),
        (format!("( {BIG} {BIG} 7 )"), BIG),
    ] {
        for indexed in [false, true] {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let id = "it/list-twice:main";
            {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
                fluree.create_ledger(id).await.expect("create");
                fluree
                    .graph(id)
                    .transact()
                    .upsert_turtle(&format!("{TTL_PFX}ex:s ex:l {list} ; ex:k \"keep\" .\n"))
                    .commit()
                    .await
                    .expect("seed");
                if indexed {
                    support::rebuild_and_publish_index(&fluree, id).await;
                }
            }
            let before = values_of(&path, id, "ex:l").await;
            let receipt = {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                let update = format!(
                    "{PFX}DELETE {{ ex:s ex:l ?o }} INSERT {{ ex:s ex:q \"done\" }} \
                     WHERE {{ ex:s ex:l ?o FILTER(?o = {v}) }}"
                );
                fluree
                    .graph(id)
                    .transact()
                    .sparql_update(&update)
                    .commit()
                    .await
                    .expect("write")
                    .receipt
            };
            let after = values_of(&path, id, "ex:l").await;
            {
                let fluree = FlureeBuilder::file(path.clone()).build().expect("reload");
                support::rebuild_and_publish_index(&fluree, id).await;
            }
            let reindexed = values_of(&path, id, "ex:l").await;
            let got = (receipt.flake_count, receipt.retract_count);
            if before.len() != 3 || got != (3, 2) || after.len() != 1 || reindexed != after {
                failures.push(format!(
                    "{list}, indexed={indexed}: list {before:?}, (flakes, retractions) {got:?}, \
                     list {after:?}, after reindex {reindexed:?}; expected three entries, (3, 2) \
                     and one entry left"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// ADD, COPY and MOVE tell the facts of the source and the destination apart
/// the way storage does: a big integer by value, whatever XSD subtype either
/// side holds it with. So a value both graphs hold, one in its index and the
/// other since its last build, is a restatement: it stays in the destination,
/// read in process, after a reload and after the next full index build (no
/// background indexer, so the index state is the test's). Before,
/// the destination's term was retracted and the source's asserted at one
/// `t`, and the retraction won. Both directions (indexed destination,
/// novelty source and the reverse), into a named graph and into the default
/// graph. A `dateTime` written past the microsecond is a control: both sides
/// hold it at the microsecond.
#[tokio::test]
async fn a_transfer_keeps_a_value_both_graphs_hold() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    const BIG: &str = "\"123456789012345678901234567890\"";
    // (source spelling, destination spelling)
    let values = [
        (
            format!("{BIG}^^xsd:nonNegativeInteger"),
            format!("{BIG}^^xsd:nonNegativeInteger"),
        ),
        (
            format!("{BIG}^^xsd:positiveInteger"),
            format!("{BIG}^^xsd:nonNegativeInteger"),
        ),
        (
            "\"2020-01-01T00:00:00.123456789Z\"^^xsd:dateTime".to_string(),
            "\"2020-01-01T00:00:00.1234567Z\"^^xsd:dateTime".to_string(),
        ),
        ("\"x\"".to_string(), "\"x\"".to_string()),
    ];
    let src = "<http://example.org/g1>";
    let count = |fluree: Fluree, id: String, graph: Option<&'static str>| async move {
        let ledger = fluree.ledger(&id).await.expect("ledger");
        let pattern = match graph {
            Some(g) => format!("GRAPH {g} {{ ex:s ex:p ?o }}"),
            None => "ex:s ex:p ?o".to_string(),
        };
        let q = format!("{PFX}SELECT (COUNT(?o) AS ?n) WHERE {{ {pattern} }}");
        let json = support::query_sparql(&fluree, &ledger, &q)
            .await
            .expect("query")
            .to_sparql_json(&ledger.snapshot)
            .expect("json");
        json["results"]["bindings"][0]["n"]["value"]
            .as_str()
            .expect("count")
            .to_string()
    };
    let mut failures = Vec::new();
    let mut case = 0;
    for (src_value, dest_value) in &values {
        for dest in [Some("<http://example.org/g2>"), None] {
            for dest_indexed in [true, false] {
                for verb in ["ADD", "COPY", "MOVE"] {
                    case += 1;
                    let id = &format!("it/transfer-keeps-{case}:main");
                    let dir = tempfile::tempdir().expect("tempdir");
                    let path = dir.path().to_string_lossy().to_string();
                    let insert = |graph: Option<&str>, value: &str| {
                        let triple = format!("ex:s ex:p {value}");
                        match graph {
                            Some(g) => format!("{PFX}INSERT DATA {{ GRAPH {g} {{ {triple} }} }}"),
                            None => format!("{PFX}INSERT DATA {{ {triple} }}"),
                        }
                    };
                    // The indexed side first, then the other since the build.
                    let (first, second) = if dest_indexed {
                        (insert(dest, dest_value), insert(Some(src), src_value))
                    } else {
                        (insert(Some(src), src_value), insert(dest, dest_value))
                    };
                    let fluree = FlureeBuilder::file(path.clone())
                        .without_indexing()
                        .build()
                        .expect("build");
                    fluree.create_ledger(id).await.expect("create");
                    for (n, seed) in [first, second].iter().enumerate() {
                        fluree
                            .graph(id)
                            .transact()
                            .sparql_update(seed)
                            .commit()
                            .await
                            .expect("seed");
                        if n == 0 {
                            support::rebuild_and_publish_index(&fluree, id).await;
                        }
                    }
                    let fluree = FlureeBuilder::file(path.clone())
                        .without_indexing()
                        .build()
                        .expect("reload");
                    let transfer = format!("{verb} {src} TO {}", dest.unwrap_or("DEFAULT"));
                    fluree
                        .graph(id)
                        .transact()
                        .sparql_update(&transfer)
                        .commit()
                        .await
                        .expect("transfer");
                    let reload = || {
                        FlureeBuilder::file(path.clone())
                            .without_indexing()
                            .build()
                            .expect("reload")
                    };
                    // Read with no build since the transfer, after an
                    // incremental build over the seeding's full one, and
                    // after a full rebuild. The rebuild is published through
                    // `reindex`, which replaces an index of the same `t`;
                    // `publish_index` would keep the incremental one.
                    let mut got = vec![
                        count(fluree, id.clone(), dest).await,
                        count(reload(), id.clone(), dest).await,
                    ];
                    let mut src_left = vec![count(reload(), id.clone(), Some(src)).await];
                    support::build_incrementally_and_publish_index(&reload(), id).await;
                    got.push(count(reload(), id.clone(), dest).await);
                    // An incremental build can bring back a fact inserted and
                    // retracted since its base in a graph the base holds
                    // nothing for (#2118), which a MOVE source written since
                    // the seeding's build is. A full rebuild cannot.
                    if !(verb == "MOVE" && dest_indexed) {
                        src_left.push(count(reload(), id.clone(), Some(src)).await);
                    }
                    reload()
                        .reindex(id, ReindexOptions::default())
                        .await
                        .expect("full rebuild");
                    got.push(count(reload(), id.clone(), dest).await);
                    src_left.push(count(reload(), id.clone(), Some(src)).await);
                    let want_src = if verb == "MOVE" { "0" } else { "1" };
                    if got.iter().any(|n| n != "1") || src_left.iter().any(|n| n != want_src) {
                        failures.push(format!(
                            "{verb} of {src_value} onto {dest_value} ({} indexed) into {}: \
                             destination values in process, reloaded, after an incremental \
                             and a full build = {got:?}, source values reloaded and after \
                             the builds {src_left:?}; expected 1 each and {want_src}",
                            if dest_indexed {
                                "destination"
                            } else {
                                "source"
                            },
                            dest.unwrap_or("the default graph"),
                        ));
                    }
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Merge and revert net a range of commits per fact, and an integer too large
/// for 64 bits is one fact whatever XSD subtype it is written with. So a
/// range that deletes it under one subtype and writes it again under another
/// replays as that one fact's last op, not as a retraction of one term and
/// an assertion of the other at one `t`, which storage resolves as the
/// retraction. Reverting `DELETE x^^integer` and `INSERT x^^nonNegativeInteger`
/// brings the value back, and merging a branch that ends by writing it keeps
/// it, on a ledger indexed before the range and on one never indexed, read
/// after the write and after the next index build.
#[tokio::test]
async fn merge_and_revert_keep_a_big_integer_written_under_another_subtype() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    let integer = format!("\"{BIG}\"^^xsd:integer");
    let nonneg = format!("\"{BIG}\"^^xsd:nonNegativeInteger");
    let write = |fluree: &Fluree, id: &str, body: String| {
        let (fluree, id) = (fluree.clone(), id.to_string());
        async move {
            fluree
                .graph(&id)
                .transact()
                .sparql_update(&format!("{PFX}{body}"))
                .commit()
                .await
                .expect("write")
                .receipt
        }
    };
    let mut failures = Vec::new();
    for indexed in [false, true] {
        let index_note = if indexed { "indexed" } else { "never indexed" };
        // Revert both commits of `DELETE x^^integer`, `INSERT x^^nonNegativeInteger`.
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("build");
            let name = format!("it/revert-subtype-{indexed}");
            let id = format!("{name}:main");
            fluree.create_ledger(&id).await.expect("create");
            write(
                &fluree,
                &id,
                format!("INSERT DATA {{ ex:s ex:p {integer} ; ex:k \"keep\" }}"),
            )
            .await;
            if indexed {
                support::rebuild_and_publish_index(&fluree, &id).await;
            }
            let deleted = write(
                &fluree,
                &id,
                format!("DELETE DATA {{ ex:s ex:p {integer} }}"),
            )
            .await;
            let inserted = write(
                &fluree,
                &id,
                format!("INSERT DATA {{ ex:s ex:p {nonneg} }}"),
            )
            .await;
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("reload");
            fluree
                .revert_commits(
                    &name,
                    "main",
                    vec![
                        CommitRef::Exact(deleted.commit_id),
                        CommitRef::Exact(inserted.commit_id),
                    ],
                    ConflictStrategy::TakeSource,
                )
                .await
                .expect("revert");
            let after = values_of(&path, &id, "ex:p").await;
            support::build_and_publish_index(
                &FlureeBuilder::file(path.clone())
                    .without_indexing()
                    .build()
                    .expect("reload"),
                &id,
            )
            .await;
            let reindexed = values_of(&path, &id, "ex:p").await;
            if after != [BIG] || reindexed != [BIG] {
                failures.push(format!(
                    "revert, {index_note}: values {after:?}, after a build {reindexed:?}"
                ));
            }
        }
        // Revert both writes of a value novelty holds under two subtypes
        // (a ledger never indexed): the net retraction retracts it under
        // each, so it is gone at once, not only after the next build.
        if !indexed {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("build");
            let name = "it/revert-two-terms";
            let id = format!("{name}:main");
            fluree.create_ledger(&id).await.expect("create");
            write(
                &fluree,
                &id,
                "INSERT DATA { ex:s ex:k \"keep\" }".to_string(),
            )
            .await;
            let first = write(
                &fluree,
                &id,
                format!("INSERT DATA {{ ex:s ex:p {integer} }}"),
            )
            .await;
            let second = write(
                &fluree,
                &id,
                format!("INSERT DATA {{ ex:s ex:p {nonneg} }}"),
            )
            .await;
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("reload");
            fluree
                .revert_commits(
                    name,
                    "main",
                    vec![
                        CommitRef::Exact(first.commit_id),
                        CommitRef::Exact(second.commit_id),
                    ],
                    ConflictStrategy::TakeSource,
                )
                .await
                .expect("revert");
            let after = values_of(&path, &id, "ex:p").await;
            support::rebuild_and_publish_index(
                &FlureeBuilder::file(path.clone())
                    .without_indexing()
                    .build()
                    .expect("reload"),
                &id,
            )
            .await;
            let reindexed = values_of(&path, &id, "ex:p").await;
            if !after.is_empty() || !reindexed.is_empty() {
                failures.push(format!(
                    "revert of both subtypes, never indexed: values {after:?}, after a build \
                     {reindexed:?}; expected none"
                ));
            }
        }
        // Merge a branch that inserts, deletes, and inserts again under
        // another subtype, into a main that moved on meanwhile.
        {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().to_string_lossy().to_string();
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("build");
            let name = format!("it/merge-subtype-{indexed}");
            let id = format!("{name}:main");
            let dev = format!("{name}:dev");
            fluree.create_ledger(&id).await.expect("create");
            write(
                &fluree,
                &id,
                "INSERT DATA { ex:base ex:k \"b\" }".to_string(),
            )
            .await;
            if indexed {
                support::rebuild_and_publish_index(&fluree, &id).await;
            }
            fluree
                .create_branch(&name, "dev", None, None)
                .await
                .expect("branch");
            write(
                &fluree,
                &dev,
                format!("INSERT DATA {{ ex:s ex:p {integer} }}"),
            )
            .await;
            write(
                &fluree,
                &dev,
                format!("DELETE DATA {{ ex:s ex:p {integer} }}"),
            )
            .await;
            write(
                &fluree,
                &dev,
                format!("INSERT DATA {{ ex:s ex:p {nonneg} }}"),
            )
            .await;
            write(
                &fluree,
                &id,
                "INSERT DATA { ex:other ex:k \"o\" }".to_string(),
            )
            .await;
            let fluree = FlureeBuilder::file(path.clone())
                .without_indexing()
                .build()
                .expect("reload");
            let report = fluree
                .merge_branch(&name, "dev", None, ConflictStrategy::default())
                .await
                .expect("merge");
            assert!(!report.fast_forward, "the merge replays the branch");
            let after = values_of(&path, &id, "ex:p").await;
            support::build_and_publish_index(
                &FlureeBuilder::file(path.clone())
                    .without_indexing()
                    .build()
                    .expect("reload"),
                &id,
            )
            .await;
            let reindexed = values_of(&path, &id, "ex:p").await;
            if after != [BIG] || reindexed != [BIG] {
                failures.push(format!(
                    "merge, {index_note}: values {after:?}, after a build {reindexed:?}"
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
