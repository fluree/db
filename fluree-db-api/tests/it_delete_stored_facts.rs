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
//! keeps less than some terms carry: a big integer's XSD subtype, a
//! `dateTime`'s digits past the microsecond.

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

/// A value bound by OPTIONAL, UNION or BIND is not a witness, so its DELETE
/// row is matched against the stored facts of its slot. The row carries the
/// query's decode of the value; on an indexed ledger that decode and the
/// stored fact's differ in datatype, and the row still names the fact.
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
/// same literal on both lanes; the index keeps microseconds.
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
