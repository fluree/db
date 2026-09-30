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

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, TransactResult};
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
        // The same term deletes it. (Spelled in JSON-LD: SPARQL UPDATE lowers
        // `"1"^^xsd:int` to a string value, a separate gap that deletes
        // nothing before or after this change.)
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

/// SPARQL `DELETE DATA` of typed literals whose datatype SPARQL UPDATE does
/// not coerce today (`xsd:int`, `xsd:long`, `xsd:dateTime`): the lowering
/// keeps the lexical form as a string, so the intent names no stored term.
/// Before the retraction resolver that committed phantom retractions that
/// deleted nothing; with it alone nothing is committed and the values still
/// stay. #1988 lowers these literals through the same coercion JSON-LD uses,
/// after which the intent names the stored term and the resolver deletes it.
#[tokio::test]
#[ignore = "needs #1988: SPARQL UPDATE keeps typed literal values"]
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
