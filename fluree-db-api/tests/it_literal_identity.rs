//! Literal term identity in constant-object matching.
//!
//! `"bob"`, `"bob"@en` and `"bob"@fr` are three distinct RDF terms. They
//! share one string-dictionary key, and SPARQL lowering used to attach no
//! datatype/language constraint to ordinary triple objects, so a bound
//! literal matched all three (and `VALUES ?o { "bob"@en }` projected the
//! wrong literal for the false matches). Per-row join/OPTIONAL probes
//! rebuilt the object from a binding the same way, so joins on a string
//! variable equated every tag.
//!
//! Now SPARQL string literals (plain, tagged, explicitly typed) carry their
//! term constraint into the scan, join/OPTIONAL probes carry the binding's
//! string constraint, and novelty-side filters honour the tag. Bare numerics
//! keep their lenient cross-subtype matching, as does a plain JSON string in
//! the JSON-LD query surface (pinned below so a change there is deliberate).
//!
//! The same holds for the rest of the string dictionary — `xsd:anyURI`,
//! `xsd:token`, `xsd:normalizedString` and customer-defined datatypes all
//! intern their lexical form alongside `xsd:string`, and an indexed read used
//! to decode them onto `xsd:string` outright.

#![cfg(feature = "native")]

mod support;
use crate::support::{
    query_jsonld_formatted, query_sparql, span_capture, start_background_indexer_local,
    trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, ReindexOptions};
use serde_json::{json, Value};

const PREFIX: &str = "PREFIX ex: <http://example.org/ns/> \
                      PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

async fn sparql(
    fluree: &fluree_db_api::Fluree,
    view: &fluree_db_api::LedgerState,
    body: &str,
) -> Value {
    let rows = query_sparql(fluree, view, &format!("{PREFIX}{body}"))
        .await
        .expect("query");
    rows.to_jsonld(&view.snapshot).expect("jsonld")
}

#[tokio::test]
async fn string_literals_match_by_term_identity_in_novelty_and_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/literal-identity:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:d4", "ex:authoredBy": {"@value": "bob", "@language": "en"}},
                {"@id": "ex:d5", "ex:authoredBy": {"@value": "bob", "@language": "fr"}},
                {"@id": "ex:d6", "ex:authoredBy": "bob"},
                {"@id": "ex:d7", "ex:editedBy": {"@value": "bob", "@language": "fr"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            // Every shape is checked against novelty-only state, then again
            // once the same data is in the binary index (different scan,
            // probe, and overlay paths).
            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                assert_eq!(
                    view.snapshot.range_provider.is_some(),
                    phase == "indexed",
                    "{phase}: setup"
                );

                let cases: &[(&str, &str, Value)] = &[
                    (
                        "tagged en",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"@en }"#,
                        json!([["ex:d4"]]),
                    ),
                    (
                        "tagged fr",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"@fr }"#,
                        json!([["ex:d5"]]),
                    ),
                    (
                        "plain is xsd:string only",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob" }"#,
                        json!([["ex:d6"]]),
                    ),
                    (
                        "explicit xsd:string",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy "bob"^^xsd:string }"#,
                        json!([["ex:d6"]]),
                    ),
                    (
                        "one-row VALUES keeps its literal",
                        r#"SELECT ?s ?o WHERE { VALUES ?o { "bob"@en } ?s ex:authoredBy ?o }"#,
                        json!([["ex:d4", {"@value": "bob", "@language": "en"}]]),
                    ),
                    (
                        "multi-row VALUES probes per tag",
                        r#"SELECT ?s ?o WHERE { VALUES ?o { "bob"@en "bob"@fr } ?s ex:authoredBy ?o }
                           ORDER BY ?s"#,
                        json!([
                            ["ex:d4", {"@value": "bob", "@language": "en"}],
                            ["ex:d5", {"@value": "bob", "@language": "fr"}]
                        ]),
                    ),
                    (
                        "self-join on a string var is term-exact",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o . ?b ex:authoredBy ?o FILTER(?a != ?b) }",
                        json!([]),
                    ),
                    (
                        "cross-predicate join matches the same tag only",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o . ?b ex:editedBy ?o }",
                        json!([["ex:d5", "ex:d7"]]),
                    ),
                    (
                        "OPTIONAL probe is term-exact",
                        "SELECT ?a ?b WHERE { ?a ex:authoredBy ?o .
                                            OPTIONAL { ?b ex:authoredBy ?o FILTER(?a != ?b) } }
                         ORDER BY ?a",
                        json!([["ex:d4", null], ["ex:d5", null], ["ex:d6", null]]),
                    ),
                    (
                        "property-path endpoint keeps its tag",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy|ex:editedBy "bob"@fr } ORDER BY ?s"#,
                        json!([["ex:d5"], ["ex:d7"]]),
                    ),
                    (
                        "FILTER equality (already exact) agrees",
                        r#"SELECT ?s WHERE { ?s ex:authoredBy ?o FILTER(?o = "bob"@en) }"#,
                        json!([["ex:d4"]]),
                    ),
                ];
                for (name, body, expected) in cases {
                    let got = sparql(&fluree, &view, body).await;
                    assert_eq!(&got, expected, "{phase}: {name}");
                }

                // JSON-LD surface: `@language` / `@type` objects are exact;
                // a plain JSON string keeps matching every string datatype
                // and tag (pinned on purpose — changing it is a product
                // decision, not a side effect of this fix).
                let jl = |o: Value| {
                    json!({"@context": {"ex": "http://example.org/ns/"},
                           "select": "?s", "where": {"@id": "?s", "ex:authoredBy": o}})
                };
                let en = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "bob", "@language": "en"})),
                )
                .await
                .unwrap();
                assert_eq!(en, json!(["ex:d4"]), "{phase}: json-ld @language");
                let typed = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "bob", "@type": "http://www.w3.org/2001/XMLSchema#string"})),
                )
                .await
                .unwrap();
                assert_eq!(typed, json!(["ex:d6"]), "{phase}: json-ld @type");
                let mut plain = query_jsonld_formatted(&fluree, &view, &jl(json!("bob")))
                    .await
                    .unwrap();
                plain.as_array_mut().unwrap().sort_by_key(std::string::ToString::to_string);
                assert_eq!(
                    plain,
                    json!(["ex:d4", "ex:d5", "ex:d6"]),
                    "{phase}: json-ld plain string stays lenient"
                );
            }
        })
        .await;
}

/// The rest of the string dictionary. `xsd:string`, `rdf:langString` and
/// `@fulltext` are the only string datatypes with a reserved `DatatypeDictId`,
/// so an indexed read used to decode `"abc"^^xsd:anyURI`, `"abc"^^xsd:token`
/// and `"abc"^^ex:custom` onto `xsd:string`: `DATATYPE()` reported the wrong
/// IRI, `FILTER(?x = ?y)` called all four literals equal, and a self-join
/// swapped each one's identity row for a pairing with the plain string (#1729).
///
/// Both a standard XSD subtype and a customer-defined datatype are pinned, on
/// both surfaces, and in all three lanes — novelty-only, index-only, and
/// novelty layered over a published index, where the two representations meet.
#[tokio::test]
async fn string_dict_datatypes_keep_their_identity_in_novelty_and_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/string-dict-identity:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:s1", "ex:p": "abc"},
                {"@id": "ex:s2", "ex:p": {"@value": "abc", "@type": "http://example.org/ns/custom"}},
                {"@id": "ex:s3", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#anyURI"}},
                {"@id": "ex:s4", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#token"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            let identity_pairs = json!([
                ["ex:s1", "ex:s1"],
                ["ex:s2", "ex:s2"],
                ["ex:s3", "ex:s3"],
                ["ex:s4", "ex:s4"]
            ]);

            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                assert_eq!(
                    view.snapshot.range_provider.is_some(),
                    phase == "indexed",
                    "{phase}: setup"
                );

                let cases: &[(&str, &str, Value)] = &[
                    // Four distinct RDF terms, so a self-join pairs each with
                    // itself and nothing else.
                    (
                        "self-join keeps each literal's own identity row",
                        "SELECT ?a ?b WHERE { ?a ex:p ?o . ?b ex:p ?o } ORDER BY ?a ?b",
                        identity_pairs.clone(),
                    ),
                    // `=` is value equality, which for non-numeric literals
                    // requires matching datatypes.
                    (
                        "FILTER(?x = ?y) separates the datatypes",
                        "SELECT ?a ?b WHERE { ?a ex:p ?x . ?b ex:p ?y FILTER(?x = ?y) } ORDER BY ?a ?b",
                        identity_pairs.clone(),
                    ),
                    (
                        "sameTerm agrees with =",
                        "SELECT ?a ?b WHERE { ?a ex:p ?x . ?b ex:p ?y FILTER(sameTerm(?x, ?y)) }
                         ORDER BY ?a ?b",
                        identity_pairs.clone(),
                    ),
                    // The decoded binding carries its own datatype, so
                    // projection reports it.
                    (
                        "DATATYPE reports the stored datatype",
                        "SELECT ?s (DATATYPE(?o) AS ?d) WHERE { ?s ex:p ?o } ORDER BY ?s",
                        json!([
                            ["ex:s1", "xsd:string"],
                            ["ex:s2", "ex:custom"],
                            ["ex:s3", "xsd:anyURI"],
                            ["ex:s4", "xsd:token"]
                        ]),
                    ),
                    (
                        "DISTINCT counts four terms",
                        "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ex:p ?o }",
                        json!([[4]]),
                    ),
                    (
                        "OPTIONAL probe is term-exact",
                        "SELECT ?a ?b WHERE { ?a ex:p ?o .
                                            OPTIONAL { ?b ex:p ?o FILTER(?a != ?b) } }
                         ORDER BY ?a",
                        json!([["ex:s1", null], ["ex:s2", null], ["ex:s3", null], ["ex:s4", null]]),
                    ),
                    (
                        "constant custom datatype",
                        r#"SELECT ?s WHERE { ?s ex:p "abc"^^<http://example.org/ns/custom> }"#,
                        json!([["ex:s2"]]),
                    ),
                    (
                        "constant xsd:anyURI",
                        r#"SELECT ?s WHERE { ?s ex:p "abc"^^xsd:anyURI }"#,
                        json!([["ex:s3"]]),
                    ),
                    (
                        "constant xsd:token",
                        r#"SELECT ?s WHERE { ?s ex:p "abc"^^xsd:token }"#,
                        json!([["ex:s4"]]),
                    ),
                    (
                        "constant xsd:string",
                        r#"SELECT ?s WHERE { ?s ex:p "abc"^^xsd:string }"#,
                        json!([["ex:s1"]]),
                    ),
                ];
                for (name, body, expected) in cases {
                    let got = sparql(&fluree, &view, body).await;
                    assert_eq!(&got, expected, "{phase}: {name}");
                }

                // JSON-LD twin: value objects select one datatype, and the
                // self-join is term-exact on this surface too.
                let jl = |o: Value| {
                    json!({"@context": {"ex": "http://example.org/ns/"},
                           "select": "?s", "where": {"@id": "?s", "ex:p": o}})
                };
                for (dt, expected) in [
                    ("http://example.org/ns/custom", "ex:s2"),
                    ("http://www.w3.org/2001/XMLSchema#anyURI", "ex:s3"),
                    ("http://www.w3.org/2001/XMLSchema#token", "ex:s4"),
                    ("http://www.w3.org/2001/XMLSchema#string", "ex:s1"),
                ] {
                    let got =
                        query_jsonld_formatted(&fluree, &view, &jl(json!({"@value": "abc", "@type": dt})))
                            .await
                            .unwrap();
                    assert_eq!(got, json!([expected]), "{phase}: json-ld @type {dt}");
                }
                // A bare JSON string in the constant-object position stays
                // lenient across the whole string family — unchanged here, and
                // a product decision like the numeric one above.
                let mut plain = query_jsonld_formatted(&fluree, &view, &jl(json!("abc")))
                    .await
                    .unwrap();
                plain
                    .as_array_mut()
                    .unwrap()
                    .sort_by_key(std::string::ToString::to_string);
                assert_eq!(
                    plain,
                    json!(["ex:s1", "ex:s2", "ex:s3", "ex:s4"]),
                    "{phase}: json-ld plain string stays lenient"
                );
                let mut got = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &json!({"@context": {"ex": "http://example.org/ns/"},
                            "select": ["?a", "?b"],
                            "where": [{"@id": "?a", "ex:p": "?o"}, {"@id": "?b", "ex:p": "?o"}]}),
                )
                .await
                .unwrap();
                got.as_array_mut()
                    .unwrap()
                    .sort_by_key(std::string::ToString::to_string);
                assert_eq!(got, identity_pairs, "{phase}: json-ld self-join");
            }

            // Third lane: novelty layered over the published index, where a
            // decoded `Lit` from novelty meets an `EncodedLit` from the index
            // at the join and DISTINCT keys. `ex:s5` repeats `ex:s2`'s custom
            // literal and `ex:s6` repeats `ex:s1`'s plain one.
            let more = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:s5", "ex:p": {"@value": "abc", "@type": "http://example.org/ns/custom"}},
                {"@id": "ex:s6", "ex:p": "abc"}
            ]});
            let head = fluree.ledger(id).await.unwrap();
            fluree.insert(head, &more).await.unwrap();
            let view = fluree.ledger(id).await.unwrap();
            assert!(view.snapshot.range_provider.is_some(), "mixed: setup");
            assert_eq!(
                sparql(
                    &fluree,
                    &view,
                    "SELECT ?a ?b WHERE { ?a ex:p ?o . ?b ex:p ?o } ORDER BY ?a ?b"
                )
                .await,
                json!([
                    ["ex:s1", "ex:s1"],
                    ["ex:s1", "ex:s6"],
                    ["ex:s2", "ex:s2"],
                    ["ex:s2", "ex:s5"],
                    ["ex:s3", "ex:s3"],
                    ["ex:s4", "ex:s4"],
                    ["ex:s5", "ex:s2"],
                    ["ex:s5", "ex:s5"],
                    ["ex:s6", "ex:s1"],
                    ["ex:s6", "ex:s6"]
                ]),
                "mixed: self-join across the lane switch"
            );
            assert_eq!(
                sparql(
                    &fluree,
                    &view,
                    "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ?s ex:p ?o }"
                )
                .await,
                json!([[4]]),
                "mixed: the two lanes agree on one key per term"
            );
        })
        .await;
}

const BATCHED_JOIN_LANE: &str = "join batched binary scan complete";
const BATCHED_OPTIONAL_LANE: &str = "optional batched probe complete";

/// Runs `run`, asserting the lane that logs `lane` served it.
async fn served_by<F: std::future::Future<Output = Value>>(
    lane: &str,
    what: &str,
    run: F,
) -> Value {
    let (spans, guard) = span_capture::init_test_tracing();
    let rows = run.await;
    drop(guard);
    assert!(
        !spans.find_events(lane).is_empty(),
        "{what}: expected the lane that logs {lane:?}"
    );
    rows
}

/// The same literals reached through a subject join. The nested-loop join's
/// batched lane builds its own object bindings and kept handing every
/// string-dictionary datatype `xsd:string`'s id after #1729, so a join
/// collapsed the four literals again. Pinned on the indexed lane and with
/// novelty layered over it, where the lane injects the novelty rows.
#[tokio::test]
async fn string_dict_datatypes_keep_their_identity_through_a_subject_join() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/string-dict-join:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
        {"@id": "ex:hub", "ex:ref": [{"@id": "ex:s1"}, {"@id": "ex:s2"}, {"@id": "ex:s3"}, {"@id": "ex:s4"}]},
        {"@id": "ex:s1", "ex:p": "abc"},
        {"@id": "ex:s2", "ex:p": {"@value": "abc", "@type": "http://example.org/ns/custom"}},
        {"@id": "ex:s3", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#anyURI"}},
        {"@id": "ex:s4", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#token"}}
    ]});
    fluree.insert(ledger, &data).await.unwrap();
    fluree.reindex(id, ReindexOptions::default()).await.unwrap();

    let datatypes =
        "SELECT ?s (DATATYPE(?o) AS ?d) WHERE { ex:hub ex:ref ?s . ?s ex:p ?o } ORDER BY ?s";
    let distinct = "SELECT (COUNT(DISTINCT ?o) AS ?c) WHERE { ex:hub ex:ref ?s . ?s ex:p ?o }";
    let jsonld_where = json!([
        {"@id": "ex:hub", "ex:ref": "?s"},
        {"@id": "?s", "ex:p": "?o"}
    ]);
    let jsonld_context = json!({
        "ex": "http://example.org/ns/",
        "xsd": "http://www.w3.org/2001/XMLSchema#"
    });

    let mut want = vec![
        json!(["ex:s1", "xsd:string"]),
        json!(["ex:s2", "ex:custom"]),
        json!(["ex:s3", "xsd:anyURI"]),
        json!(["ex:s4", "xsd:token"]),
    ];
    for phase in ["indexed", "mixed"] {
        if phase == "mixed" {
            // A novelty subject with a novelty lexical form, so its row is
            // injected by the lane and its string resolves through the
            // dictionary overlay.
            let more = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:hub", "ex:ref": {"@id": "ex:s5"}},
                {"@id": "ex:s5", "ex:p": {"@value": "xyz", "@type": "http://www.w3.org/2001/XMLSchema#anyURI"}}
            ]});
            let head = fluree.ledger(id).await.unwrap();
            fluree.insert(head, &more).await.unwrap();
            want.push(json!(["ex:s5", "xsd:anyURI"]));
        }
        let view = fluree.ledger(id).await.unwrap();
        assert!(view.snapshot.range_provider.is_some(), "{phase}: setup");

        let got = served_by(BATCHED_JOIN_LANE, phase, sparql(&fluree, &view, datatypes)).await;
        assert_eq!(got, json!(want), "{phase}: DATATYPE through the join");
        let got = served_by(BATCHED_JOIN_LANE, phase, sparql(&fluree, &view, distinct)).await;
        assert_eq!(
            got,
            json!([[want.len()]]),
            "{phase}: one DISTINCT key per term"
        );

        let got = served_by(BATCHED_JOIN_LANE, phase, async {
            query_jsonld_formatted(
                &fluree,
                &view,
                &json!({"@context": jsonld_context, "select": ["?s", "?d"],
                        "where": [jsonld_where[0], jsonld_where[1], ["bind", "?d", "(datatype ?o)"]],
                        "orderBy": "?s"}),
            )
            .await
            .unwrap()
        })
        .await;
        assert_eq!(
            got,
            json!(want),
            "{phase}: json-ld datatype through the join"
        );
        let got = served_by(BATCHED_JOIN_LANE, phase, async {
            query_jsonld_formatted(
                &fluree,
                &view,
                &json!({"@context": jsonld_context,
                        "select": ["(as (count-distinct ?o) ?c)"],
                        "where": jsonld_where}),
            )
            .await
            .unwrap()
        })
        .await;
        assert_eq!(
            got,
            json!([[want.len()]]),
            "{phase}: json-ld count-distinct"
        );
    }
}

/// `xsd:date`, `xsd:time` and `xsd:dateTime` keep one key whichever lane
/// produced them. The batched probe lanes emit them encoded, while a scan over
/// novelty (and, on an indexed ledger, the nested-loop join's own builder)
/// emitted them decoded, and the equality surfaces normalized neither form
/// into the other: a value reached both ways was two DISTINCT keys, two
/// groups, and survived its own MINUS.
#[tokio::test]
async fn temporal_literals_keep_one_key_across_join_lanes() {
    let typed = |value: &str, dt: &str| json!({"@value": value, "@type": format!("http://www.w3.org/2001/XMLSchema#{dt}")});
    assert_one_key_across_join_lanes(
        "it/temporal-join-lanes:main",
        &[
            typed("2024-01-01", "date"),
            typed("2024-01-01T10:00:00Z", "dateTime"),
            typed("10:00:00", "time"),
        ],
    )
    .await;
}

/// The rest of what the scan keeps encoded — `xsd:float`, `@fulltext` and
/// `@json` — under the same lanes and surfaces as the temporal types.
#[tokio::test]
async fn scan_encoded_literals_keep_one_key_across_join_lanes() {
    let float = |v: &str| json!({"@value": v, "@type": "http://www.w3.org/2001/XMLSchema#float"});
    assert_one_key_across_join_lanes(
        "it/float-join-lanes:main",
        &[float("1.5"), float("2.5"), float("-3.25")],
    )
    .await;
    let fulltext = |v: &str| json!({"@value": v, "@type": "@fulltext"});
    assert_one_key_across_join_lanes(
        "it/fulltext-join-lanes:main",
        &[fulltext("red fox"), fulltext("blue fox"), fulltext("fox")],
    )
    .await;
    let json_lit = |v: Value| json!({"@value": v, "@type": "@json"});
    assert_one_key_across_join_lanes(
        "it/json-join-lanes:main",
        &[
            json_lit(json!({"a": 1, "b": [true, "x"]})),
            json_lit(json!([1, 2])),
            json_lit(json!("s")),
        ],
    )
    .await;
}

/// Stores each of three distinct `values` twice, then checks every equality
/// surface keeps one key per value across a UNION of each batched lane with a
/// plain scan, indexed and with novelty over the index.
async fn assert_one_key_across_join_lanes(id: &str, values: &[Value; 3]) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree.create_ledger(id).await.unwrap();
    // Each value twice, so a split key shows as a doubled count.
    let whens: Vec<&Value> = values.iter().flat_map(|v| [v, v]).collect();
    let mut graph: Vec<Value> = whens
        .iter()
        .enumerate()
        .map(|(i, when)| json!({"@id": format!("ex:s{i}"), "ex:name": format!("n{i}"), "ex:when": when}))
        .collect();
    graph.push(json!({"@id": "ex:hub",
                      "ex:ref": (0..whens.len()).map(|i| json!({"@id": format!("ex:s{i}")})).collect::<Vec<_>>()}));
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": graph}),
        )
        .await
        .unwrap();
    fluree.reindex(id, ReindexOptions::default()).await.unwrap();

    let via_join = "{ ex:hub ex:ref ?s . ?s ex:when ?d }";
    let via_optional = "{ ?s ex:name ?n OPTIONAL { ?s ex:when ?d } }";
    let via_scan = "{ ?t ex:when ?d }";
    for phase in ["indexed", "mixed"] {
        if phase == "mixed" {
            // Any novelty makes the scan decode its objects.
            let head = fluree.ledger(id).await.unwrap();
            fluree
                .insert(
                    head,
                    &json!({"@context": {"ex": "http://example.org/ns/"},
                                     "@id": "ex:other", "ex:unrelated": "x"}),
                )
                .await
                .unwrap();
        }
        let view = fluree.ledger(id).await.unwrap();
        assert!(view.snapshot.range_provider.is_some(), "{phase}: setup");

        for (lane, lane_name, branch) in [
            (BATCHED_JOIN_LANE, "join", via_join),
            (BATCHED_OPTIONAL_LANE, "optional", via_optional),
        ] {
            let what = format!("{id} {phase}: {lane_name}");
            let q =
                format!("SELECT (COUNT(DISTINCT ?d) AS ?c) WHERE {{ {branch} UNION {via_scan} }}");
            let got = served_by(lane, &what, sparql(&fluree, &view, &q)).await;
            assert_eq!(got, json!([[3]]), "{what}: COUNT(DISTINCT) across lanes");
            let q = format!(
                "SELECT (COUNT(*) AS ?rows) WHERE {{ {branch} UNION {via_scan} }} GROUP BY ?d"
            );
            let got = served_by(lane, &what, sparql(&fluree, &view, &q)).await;
            assert_eq!(got, json!([[4], [4], [4]]), "{what}: GROUP BY across lanes");
            let q = format!("SELECT ?d WHERE {{ ?t ex:when ?d MINUS {branch} }}");
            let got = served_by(lane, &what, sparql(&fluree, &view, &q)).await;
            assert_eq!(got, json!([]), "{what}: MINUS across lanes");
        }

        let got = served_by(BATCHED_JOIN_LANE, phase, async {
            query_jsonld_formatted(
                &fluree,
                &view,
                &json!({"@context": {"ex": "http://example.org/ns/"},
                        "select": ["(as (count-distinct ?d) ?c)"],
                        "where": [["union",
                                   [{"@id": "ex:hub", "ex:ref": "?s"}, {"@id": "?s", "ex:when": "?d"}],
                                   {"@id": "?t", "ex:when": "?d"}]]}),
            )
            .await
            .unwrap()
        })
        .await;
        assert_eq!(
            got,
            json!([[3]]),
            "{id} {phase}: json-ld count-distinct across lanes"
        );
    }
}

/// Bare numerics are deliberately unconstrained: `25` still matches a value
/// stored under any integer subtype, as before.
#[tokio::test]
async fn bare_numeric_literals_stay_lenient_across_subtypes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let id = "it/literal-identity-num:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
        {"@id": "ex:a", "ex:age": 25},
        {"@id": "ex:b", "ex:age": {"@value": "25", "@type": "http://www.w3.org/2001/XMLSchema#int"}},
        {"@id": "ex:c", "ex:age": {"@value": "25", "@type": "http://www.w3.org/2001/XMLSchema#long"}}
    ]});
    fluree.insert(ledger, &data).await.unwrap();
    let view = fluree.ledger(id).await.unwrap();
    let got = sparql(
        &fluree,
        &view,
        "SELECT ?s WHERE { ?s ex:age 25 } ORDER BY ?s",
    )
    .await;
    assert_eq!(got, json!([["ex:a"], ["ex:b"], ["ex:c"]]));
}

/// BCP 47 tags are case-insensitive: a value written as `@EN` is stored,
/// matched and reported as `en`, whether the query writes `@en` or `@EN`.
#[tokio::test]
async fn language_tags_are_case_insensitive() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/literal-identity-case:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:upper", "ex:name": {"@value": "chat", "@language": "EN"}},
                {"@id": "ex:lower", "ex:name": {"@value": "chat", "@language": "en"}},
                {"@id": "ex:fr", "ex:name": {"@value": "chat", "@language": "fr-CA"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                for q in [
                    r#"SELECT ?s WHERE { ?s ex:name "chat"@en } ORDER BY ?s"#,
                    r#"SELECT ?s WHERE { ?s ex:name "chat"@EN } ORDER BY ?s"#,
                ] {
                    assert_eq!(
                        sparql(&fluree, &view, q).await,
                        json!([["ex:lower"], ["ex:upper"]]),
                        "{phase}: {q}"
                    );
                }
                assert_eq!(
                    sparql(&fluree, &view, r#"SELECT ?s WHERE { ?s ex:name "chat"@FR-ca }"#).await,
                    json!([["ex:fr"]]),
                    "{phase}: region subtag case"
                );
                // LANG() reports the canonical lowercase form for every row.
                assert_eq!(
                    sparql(
                        &fluree,
                        &view,
                        "SELECT ?s (LANG(?n) AS ?l) WHERE { ?s ex:name ?n } ORDER BY ?s"
                    )
                    .await,
                    json!([["ex:fr", "fr-ca"], ["ex:lower", "en"], ["ex:upper", "en"]]),
                    "{phase}: LANG() canonical"
                );
                let jl = json!({"@context": {"ex": "http://example.org/ns/"}, "select": "?s",
                                "where": {"@id": "?s", "ex:name": {"@value": "chat", "@language": "En"}}});
                let mut got = query_jsonld_formatted(&fluree, &view, &jl).await.unwrap();
                got.as_array_mut().unwrap().sort_by_key(std::string::ToString::to_string);
                assert_eq!(got, json!(["ex:lower", "ex:upper"]), "{phase}: json-ld @language case");
            }
        })
        .await;
}

/// A datatype IRI whose namespace is not registered on the ledger can match
/// NOTHING: ingest registers every namespace it stores, so no stored term
/// carries such a datatype. SPARQL lowering used to rewrite the unresolvable
/// datatype to `xsd:string`, so `"a"^^ex:NoSuchType` matched every
/// plain-string `"a"` row — fail-open — while the JSON-LD surface already
/// answered the empty result (#1686). Both surfaces are pinned in both
/// lanes, on the triple-pattern site and its `term_to_binding` VALUES twin,
/// with a registered-custom-datatype control that must keep matching.
#[tokio::test]
async fn unencodable_datatype_matches_nothing_in_novelty_and_index() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/literal-identity-unenc:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:u1", "ex:q": "a"},
                {"@id": "ex:u2", "ex:q": {"@value": "a", "@language": "en"}},
                {"@id": "ex:u3", "ex:q": {"@value": "z", "@type": "http://custom.example.com/T"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            const BOGUS: &str = "http://no-such-namespace.example.com/NoSuchType";
            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                assert_eq!(
                    view.snapshot.range_provider.is_some(),
                    phase == "indexed",
                    "{phase}: setup"
                );

                let bogus_pattern = format!(r#"SELECT ?s WHERE {{ ?s ex:q "a"^^<{BOGUS}> }}"#);
                let bogus_values =
                    format!(r#"SELECT ?s WHERE {{ VALUES ?o {{ "a"^^<{BOGUS}> }} ?s ex:q ?o }}"#);
                let cases: &[(&str, &str, Value)] = &[
                    (
                        "unregistered datatype in a triple pattern matches nothing",
                        &bogus_pattern,
                        json!([]),
                    ),
                    (
                        "unregistered datatype in a VALUES row matches nothing",
                        &bogus_values,
                        json!([]),
                    ),
                    // Controls: a custom datatype whose namespace WAS
                    // registered by ingest still matches exactly its row.
                    (
                        "registered custom datatype still matches (pattern)",
                        r#"SELECT ?s WHERE { ?s ex:q "z"^^<http://custom.example.com/T> }"#,
                        json!([["ex:u3"]]),
                    ),
                    (
                        "registered custom datatype still matches (VALUES)",
                        r#"SELECT ?s WHERE { VALUES ?o { "z"^^<http://custom.example.com/T> } ?s ex:q ?o }"#,
                        json!([["ex:u3"]]),
                    ),
                ];
                for (name, body, expected) in cases {
                    let got = sparql(&fluree, &view, body).await;
                    assert_eq!(&got, expected, "{phase}: {name}");
                }

                // JSON-LD parity: the surface that always used the
                // non-strict encoder answers the same on both shapes.
                let jl = |o: Value| {
                    json!({"@context": {"ex": "http://example.org/ns/"},
                           "select": "?s", "where": {"@id": "?s", "ex:q": o}})
                };
                let bogus_jl = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "a", "@type": BOGUS})),
                )
                .await
                .unwrap();
                assert_eq!(bogus_jl, json!([]), "{phase}: json-ld unregistered datatype");
                let control_jl = query_jsonld_formatted(
                    &fluree,
                    &view,
                    &jl(json!({"@value": "z", "@type": "http://custom.example.com/T"})),
                )
                .await
                .unwrap();
                assert_eq!(
                    control_jl,
                    json!(["ex:u3"]),
                    "{phase}: json-ld registered custom datatype"
                );
            }
        })
        .await;
}

/// A typed literal in expression position keeps its datatype when that
/// datatype has no value kind of its own (a customer datatype, `xsd:anyURI`,
/// `xsd:token`): SPARQL lowered `"abc"^^ex:custom` there to a bare string
/// constant, an `xsd:string`, so `sameTerm` and `IN` matched the plain
/// `"abc"` instead, a BIND of it reported `xsd:string`, and `=` was pushed
/// into the scan as a bare string. It now lowers as `STRDT("abc", ex:custom)`
/// would, as a language-tagged literal already lowers as `STRLANG`. JSON-LD
/// writes the same constant with `strdt`, which kept its datatype; pinned
/// alongside, as the twin.
#[tokio::test]
async fn typed_literal_constants_in_expressions_keep_their_datatype() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut fluree = FlureeBuilder::file(tmp.path().to_string_lossy().to_string())
        .build()
        .unwrap();
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree.nameservice_mode().publisher_arc().unwrap(),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let id = "it/typed-literal-expressions:main";
            let ledger = fluree.create_ledger(id).await.unwrap();
            let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
                {"@id": "ex:s1", "ex:p": "abc"},
                {"@id": "ex:s2", "ex:p": {"@value": "abc", "@type": "http://example.org/ns/custom"}},
                {"@id": "ex:s3", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#anyURI"}},
                {"@id": "ex:s4", "ex:p": {"@value": "abc", "@type": "http://www.w3.org/2001/XMLSchema#token"}},
                {"@id": "ex:s5", "ex:p": {"@value": "abc", "@language": "fr"}}
            ]});
            let r = fluree.insert(ledger, &data).await.unwrap();

            for phase in ["novelty", "indexed"] {
                if phase == "indexed" {
                    trigger_index_and_wait_outcome(&handle, id, r.receipt.t).await;
                }
                let view = fluree.ledger(id).await.unwrap();
                assert_eq!(
                    view.snapshot.range_provider.is_some(),
                    phase == "indexed",
                    "{phase}: setup"
                );

                let cases: &[(&str, &str, Value)] = &[
                    (
                        "sameTerm with a customer datatype",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(sameTerm(?o, "abc"^^ex:custom)) }"#,
                        json!([["ex:s2"]]),
                    ),
                    (
                        "sameTerm with xsd:anyURI",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(sameTerm(?o, "abc"^^xsd:anyURI)) }"#,
                        json!([["ex:s3"]]),
                    ),
                    (
                        "= with a customer datatype",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(?o = "abc"^^ex:custom) }"#,
                        json!([["ex:s2"]]),
                    ),
                    (
                        "= with xsd:token",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(?o = "abc"^^xsd:token) }"#,
                        json!([["ex:s4"]]),
                    ),
                    (
                        "IN over two typed literals",
                        r#"SELECT ?s WHERE { ?s ex:p ?o
                             FILTER(?o IN ("abc"^^ex:custom, "abc"^^xsd:token)) } ORDER BY ?s"#,
                        json!([["ex:s2"], ["ex:s4"]]),
                    ),
                    (
                        "BIND reports the literal's datatype",
                        r#"SELECT (DATATYPE(?x) AS ?d) WHERE { BIND("abc"^^ex:custom AS ?x) }"#,
                        json!([["ex:custom"]]),
                    ),
                    // Controls: a plain string and an explicit xsd:string are
                    // the one term `"abc"`.
                    (
                        "sameTerm with a plain string",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(sameTerm(?o, "abc")) }"#,
                        json!([["ex:s1"]]),
                    ),
                    (
                        "sameTerm with xsd:string",
                        r#"SELECT ?s WHERE { ?s ex:p ?o FILTER(sameTerm(?o, "abc"^^xsd:string)) }"#,
                        json!([["ex:s1"]]),
                    ),
                ];
                for (name, body, expected) in cases {
                    let got = sparql(&fluree, &view, body).await;
                    assert_eq!(&got, expected, "{phase}: {name}");
                }

                let jl = |filter: &str| {
                    json!({"@context": {"ex": "http://example.org/ns/"},
                           "select": "?s",
                           "where": [{"@id": "?s", "ex:p": "?o"}, ["filter", filter]]})
                };
                for (name, filter, expected) in [
                    (
                        "json-ld sameTerm with strdt",
                        r#"(sameTerm ?o (strdt "abc" "http://example.org/ns/custom"))"#,
                        json!(["ex:s2"]),
                    ),
                    (
                        "json-ld = with strdt",
                        r#"(= ?o (strdt "abc" "http://www.w3.org/2001/XMLSchema#token"))"#,
                        json!(["ex:s4"]),
                    ),
                ] {
                    let got = query_jsonld_formatted(&fluree, &view, &jl(filter))
                        .await
                        .unwrap();
                    assert_eq!(got, expected, "{phase}: {name}");
                }
            }
        })
        .await;
}

/// A datatype IRI in a namespace the ledger has never registered.
const UNSEEN_DT: &str = "http://nowhere.example/T";

/// `STRDT` and a typed literal in an expression build their literal for any
/// datatype IRI (SPARQL 1.1 §17.4.2.9), including one in a namespace the
/// ledger has never registered. A BIND of either used to fail the whole query
/// with "Unknown datatype IRI" (#1765). The literal is its own term: equal to
/// itself, to nothing stored, and `DATATYPE` names the IRI whether the
/// literal was bound first or not. JSON-LD's `strdt` is the twin.
#[tokio::test]
async fn unseen_datatype_literal_from_an_expression_is_a_term() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/unseen-datatype-expression:main";
    let ledger = fluree.create_ledger(id).await.unwrap();
    let data = json!({"@context": {"ex": "http://example.org/ns/"}, "@graph": [
        {"@id": "ex:s1", "ex:p": "a"},
        {"@id": "ex:s2", "ex:p": {"@value": "a", "@type": "http://example.org/ns/custom"}}
    ]});
    fluree.insert(ledger, &data).await.unwrap();

    let t = format!("<{UNSEEN_DT}>");
    let cases: Vec<(&str, String, Value)> = vec![
        (
            "STRDT in a BIND",
            format!(
                r#"SELECT ?x (DATATYPE(?x) AS ?d) (STR(?x) AS ?l)
                   WHERE {{ BIND(STRDT("a", {t}) AS ?x) }}"#
            ),
            json!([[{"@value": "a", "@type": UNSEEN_DT}, UNSEEN_DT, "a"]]),
        ),
        (
            "a typed literal in a BIND",
            format!(r#"SELECT (DATATYPE(?x) AS ?d) WHERE {{ BIND("a"^^{t} AS ?x) }}"#),
            json!([[UNSEEN_DT]]),
        ),
        (
            "STRDT over a stored value",
            format!(
                r#"SELECT (DATATYPE(?x) AS ?d) (STR(?x) AS ?l)
                   WHERE {{ ex:s1 ex:p ?v BIND(STRDT(?v, {t}) AS ?x) }}"#
            ),
            json!([[UNSEEN_DT, "a"]]),
        ),
        (
            "the literal is sameTerm and = to itself",
            format!(
                r#"SELECT (sameTerm(?x, ?y) AS ?same) (?x = ?y AS ?eq)
                   WHERE {{ BIND(STRDT("a", {t}) AS ?x) BIND("a"^^{t} AS ?y) }}"#
            ),
            json!([[true, true]]),
        ),
        (
            "it matches nothing stored",
            format!(
                r#"SELECT ?s WHERE {{ ?s ex:p ?o BIND(STRDT("a", {t}) AS ?x)
                   FILTER(sameTerm(?o, ?x) || ?o = ?x) }}"#
            ),
            json!([]),
        ),
        (
            "DATATYPE of the bound literal is the IRI",
            format!(
                r#"SELECT (DATATYPE(?x) = {t} AS ?eq) (sameTerm(DATATYPE(?x), {t}) AS ?same)
                   WHERE {{ BIND(STRDT("a", {t}) AS ?x) }}"#
            ),
            json!([[true, true]]),
        ),
        (
            "an absolute IRI a JSON-LD @type would refuse",
            r#"SELECT (DATATYPE(?x) AS ?d) (DATATYPE(?y) AS ?e)
               WHERE { BIND(STRDT("a", <bad:datatype>) AS ?x) BIND("a"^^<bad:datatype> AS ?y) }"#
                .to_string(),
            json!([["bad:datatype", "bad:datatype"]]),
        ),
        (
            "DATATYPE of the literal, bound or not, joins as one term",
            format!(
                r#"SELECT ?a WHERE {{ ex:s1 ex:p ?v BIND(DATATYPE(STRDT(?v, {t})) AS ?d)
                     {{ SELECT ?d ?a WHERE {{ ex:s1 ex:p ?w BIND(STRDT(?w, {t}) AS ?y)
                                             BIND(DATATYPE(?y) AS ?d) BIND(1 AS ?a) }} }} }}"#
            ),
            json!([[1]]),
        ),
        (
            "DATATYPE of a VALUES literal joins DATATYPE(STRDT(..))",
            format!(
                r#"SELECT ?a WHERE {{ VALUES ?x {{ "a"^^{t} }} BIND(DATATYPE(?x) AS ?d)
                     {{ SELECT ?d ?a WHERE {{ BIND(DATATYPE(STRDT("a", {t})) AS ?d) BIND(1 AS ?a) }} }} }}"#
            ),
            json!([[1]]),
        ),
        (
            "DATATYPE of the literal, bound or not, prints one way",
            format!(
                r#"PREFIX nw: <http://nowhere.example/>
                   SELECT ?bound ?unbound WHERE {{ ex:s1 ex:p ?v BIND(STRDT(?v, {t}) AS ?x)
                     BIND(DATATYPE(?x) AS ?bound) BIND(DATATYPE(STRDT(?v, {t})) AS ?unbound) }}"#
            ),
            json!([["nw:T", "nw:T"]]),
        ),
    ];
    let jl = |bind: &str| {
        json!({"@context": {"ex": "http://example.org/ns/"},
               "select": ["?d", "?l"],
               "where": [{"@id": "ex:s1", "ex:p": "?v"},
                         ["bind", "?x", bind, "?d", "(datatype ?x)", "?l", "(str ?x)"]]})
    };
    let jl_cases = [
        ("json-ld strdt", format!(r#"(strdt ?v "{UNSEEN_DT}")"#)),
        (
            "json-ld str-dt over an iri",
            format!(r#"(str-dt ?v (iri "{UNSEEN_DT}"))"#),
        ),
    ];

    for phase in ["novelty", "indexed"] {
        if phase == "indexed" {
            fluree.reindex(id, ReindexOptions::default()).await.unwrap();
        }
        let view = fluree.ledger(id).await.unwrap();
        assert_eq!(
            view.snapshot.range_provider.is_some(),
            phase == "indexed",
            "{phase}: setup"
        );
        for (name, body, expected) in &cases {
            let got = sparql(&fluree, &view, body).await;
            assert_eq!(&got, expected, "{phase}: {name}");
        }
        for (name, bind) in &jl_cases {
            let got = query_jsonld_formatted(&fluree, &view, &jl(bind))
                .await
                .unwrap_or_else(|e| panic!("{phase}: {name}: {e}"));
            assert_eq!(got, json!([[UNSEEN_DT, "a"]]), "{phase}: {name}");
        }
        let forms = json!({
            "@context": {"ex": "http://example.org/ns/", "nw": "http://nowhere.example/"},
            "select": ["?bound", "?unbound"],
            "where": [{"@id": "ex:s1", "ex:p": "?v"},
                      ["bind", "?x", format!(r#"(strdt ?v "{UNSEEN_DT}")"#),
                       "?bound", "(datatype ?x)",
                       "?unbound", format!(r#"(datatype (strdt ?v "{UNSEEN_DT}"))"#)]]
        });
        let got = query_jsonld_formatted(&fluree, &view, &forms)
            .await
            .unwrap();
        assert_eq!(
            got,
            json!([["nw:T", "nw:T"]]),
            "{phase}: json-ld DATATYPE forms"
        );
    }
}

/// Every flake in `id`'s commits, head first.
async fn committed_flakes(fluree: &fluree_db_api::Fluree, id: &str) -> Vec<fluree_db_core::Flake> {
    let store = fluree.content_store(id);
    let mut next = fluree.ledger(id).await.unwrap().head_commit_id;
    let mut flakes = Vec::new();
    while let Some(cid) = next {
        let bytes = store.get(&cid).await.unwrap();
        let commit = fluree_db_core::commit::codec::read_commit(&bytes).unwrap();
        next = commit.parents.first().cloned();
        flakes.extend(commit.flakes);
    }
    flakes
}

/// Run each `(name, query, expected)` SPARQL read against `id`, after
/// checking the ledger reads from an index exactly when `indexed`.
async fn check_reads(
    fluree: &fluree_db_api::Fluree,
    id: &str,
    reads: &[(&str, &str, Value)],
    indexed: bool,
    phase: &str,
) {
    let view = fluree.ledger(id).await.unwrap();
    assert_eq!(
        view.snapshot.range_provider.is_some(),
        indexed,
        "{phase}: setup"
    );
    for (name, body, expected) in reads {
        let got = sparql(fluree, &view, body).await;
        assert_eq!(&got, expected, "{phase}: {name}");
    }
}

/// A literal whose datatype is in a namespace the ledger has not registered
/// is stored under the code the write gives that namespace, never under the
/// EMPTY-namespace full-IRI form a query builds for an unseen datatype, so a
/// lookup by the literal finds what was written: before and after an index
/// build and a reindex at the same `t`, and after a reload. A DELETE of a
/// literal no fact can carry retracts nothing and registers nothing.
///
/// An update that builds the literal stores the same term as a direct insert
/// of it: SPARQL `STRDT`, a SPARQL typed literal and JSON-LD `str-dt` each
/// write one predicate before the direct insert, and a DELETE that builds the
/// literal with `STRDT` retracts the stored fact. SPARQL `STRDT` with
/// `<bad:datatype>` stores the term SPARQL `INSERT DATA` stores.
#[tokio::test]
async fn an_unseen_datatype_literal_is_stored_under_its_namespace_code() {
    let tmp = tempfile::TempDir::new().unwrap();
    let path = tmp.path().to_string_lossy().to_string();
    let open = || {
        FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .unwrap()
    };
    let id = "it/unseen-datatype-store:main";
    const CTX: &str = "PREFIX ex: <http://example.org/ns/> ";
    let ctx = json!({"ex": "http://example.org/ns/"});

    let fluree = open();
    let ledger = fluree.create_ledger(id).await.unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [
                {"@id": "ex:a", "@type": "ex:C"},
                {"@id": "ex:b", "@type": "ex:C"}
            ]}),
        )
        .await
        .unwrap();
    let graph = || fluree.graph(id);
    graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} INSERT {{ ?s ex:p ?x }}
               WHERE {{ ?s a ex:C . BIND(STRDT("a", <http://nowhere.example/T>) AS ?x) }}"#
        ))
        .commit()
        .await
        .expect("SPARQL STRDT update");
    graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} INSERT {{ ?s ex:q ?x }}
               WHERE {{ ?s a ex:C . BIND("b"^^<http://typed.example/U> AS ?x) }}"#
        ))
        .commit()
        .await
        .expect("SPARQL typed-literal update");
    graph()
        .transact()
        .update(&json!({
            "@context": ctx,
            "where": [
                {"@id": "?s", "@type": "ex:C"},
                ["bind", "?x", "(str-dt \"c\" \"urn:example:dt\")"]
            ],
            "insert": {"@id": "?s", "ex:r": "?x"}
        }))
        .commit()
        .await
        .expect("JSON-LD str-dt update");
    graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} INSERT {{ ?s ex:t ?x }}
               WHERE {{ ?s a ex:C . BIND(STRDT("d", <bad:datatype>) AS ?x) }}"#
        ))
        .commit()
        .await
        .expect("SPARQL STRDT update with <bad:datatype>");
    // The direct-insert twin, written after the updates registered the
    // namespaces.
    graph()
        .transact()
        .insert(&json!({"@context": ctx, "@id": "ex:d",
            "ex:p": {"@value": "a", "@type": "http://nowhere.example/T"},
            "ex:q": {"@value": "b", "@type": "http://typed.example/U"},
            "ex:r": {"@value": "c", "@type": "urn:example:dt"}}))
        .commit()
        .await
        .expect("direct insert");
    // SPARQL writes any absolute IRI as a datatype, `<bad:datatype>`
    // included, which a JSON-LD `@type` refuses as an unexpanded compact IRI.
    graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} INSERT DATA {{ ex:d ex:t "d"^^<bad:datatype> }}"#
        ))
        .commit()
        .await
        .expect("SPARQL INSERT DATA");
    // A DELETE of a literal no fact can carry: nothing to retract. Whether it
    // commits an empty transaction or refuses one, it stores nothing.
    let _ = graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} DELETE {{ ?s ex:p ?x }}
               WHERE {{ ?s a ex:C . BIND(STRDT("a", <http://never.example/Z>) AS ?x) }}"#
        ))
        .commit()
        .await;
    graph()
        .transact()
        .sparql_update(&format!(
            r#"{CTX} DELETE {{ ex:a ex:p ?x }}
               WHERE {{ BIND(STRDT("a", <http://nowhere.example/T>) AS ?x) }}"#
        ))
        .commit()
        .await
        .expect("DELETE of a stored literal built by STRDT");

    let empty_dt: Vec<String> = committed_flakes(&fluree, id)
        .await
        .iter()
        .filter(|f| f.dt.namespace_code == fluree_vocab::namespaces::EMPTY)
        .map(|f| format!("{f:?}"))
        .collect();
    assert!(
        empty_dt.is_empty(),
        "EMPTY-namespace datatypes stored: {empty_dt:#?}"
    );
    let snapshot = fluree.ledger(id).await.unwrap().snapshot;
    assert!(
        snapshot
            .encode_iri_strict("http://never.example/Z")
            .is_none(),
        "the DELETE registered a namespace"
    );

    let reads: [(&str, &str, Value); 6] = [
        (
            "STRDT and the direct insert are one term",
            r#"SELECT ?s WHERE { ?s ex:p "a"^^<http://nowhere.example/T> } ORDER BY ?s"#,
            json!([["ex:b"], ["ex:d"]]),
        ),
        (
            "a typed literal and the direct insert are one term",
            r#"SELECT ?s WHERE { ?s ex:q "b"^^<http://typed.example/U> } ORDER BY ?s"#,
            json!([["ex:a"], ["ex:b"], ["ex:d"]]),
        ),
        (
            "JSON-LD str-dt and the direct insert are one term",
            r#"SELECT ?s WHERE { ?s ex:r "c"^^<urn:example:dt> } ORDER BY ?s"#,
            json!([["ex:a"], ["ex:b"], ["ex:d"]]),
        ),
        (
            "SPARQL STRDT and INSERT DATA are one term (an IRI JSON-LD refuses)",
            r#"SELECT ?s WHERE { ?s ex:t "d"^^<bad:datatype> } ORDER BY ?s"#,
            json!([["ex:a"], ["ex:b"], ["ex:d"]]),
        ),
        (
            "one stored term per predicate",
            r#"SELECT ?p (COUNT(DISTINCT ?o) AS ?n)
               WHERE { VALUES ?p { ex:p ex:q ex:r ex:t } ?s ?p ?o } GROUP BY ?p ORDER BY ?p"#,
            json!([["ex:p", 1], ["ex:q", 1], ["ex:r", 1], ["ex:t", 1]]),
        ),
        (
            "DATATYPE reads the IRI back",
            r#"SELECT DISTINCT ?p (DATATYPE(?o) AS ?d)
               WHERE { VALUES ?p { ex:p ex:q ex:r ex:t } ?s ?p ?o } ORDER BY ?p"#,
            json!([
                ["ex:p", "http://nowhere.example/T"],
                ["ex:q", "http://typed.example/U"],
                ["ex:r", "urn:example:dt"],
                ["ex:t", "bad:datatype"]
            ]),
        ),
    ];
    check_reads(&fluree, id, &reads, false, "novelty").await;
    fluree.disconnect().await;
    drop(fluree);
    let fluree = open();
    check_reads(&fluree, id, &reads, false, "reloaded").await;
    crate::support::build_and_publish_index(&fluree, id).await;
    fluree.disconnect().await;
    drop(fluree);
    let fluree = open();
    check_reads(&fluree, id, &reads, true, "indexed, reloaded").await;
    fluree.reindex(id, ReindexOptions::default()).await.unwrap();
    fluree.disconnect().await;
    drop(fluree);
    let fluree = open();
    check_reads(
        &fluree,
        id,
        &reads,
        true,
        "reindexed at the same t, reloaded",
    )
    .await;
}

/// A datatype name with no scheme (`"T"`) splits to the empty prefix, so
/// every write stores it under the EMPTY namespace: a JSON-LD insert, SPARQL
/// `INSERT DATA`, JSON-LD `str-dt` and SPARQL `STRDT` all store the one term,
/// and a lookup by it finds all four.
#[tokio::test]
async fn a_datatype_name_with_no_scheme_is_one_term_on_every_write_path() {
    let fluree = FlureeBuilder::memory().build_memory();
    let id = "it/bare-datatype-name:main";
    let ctx = json!({"ex": "http://example.org/ns/"});
    fluree.create_ledger(id).await.unwrap();
    let graph = || fluree.graph(id);
    graph()
        .transact()
        .insert(&json!({"@context": ctx, "@id": "ex:d", "ex:p": {"@value": "x", "@type": "T"}}))
        .commit()
        .await
        .expect("JSON-LD insert");
    graph()
        .transact()
        .sparql_update(r#"PREFIX ex: <http://example.org/ns/> INSERT DATA { ex:e ex:p "x"^^<T> }"#)
        .commit()
        .await
        .expect("SPARQL INSERT DATA");
    graph()
        .transact()
        .update(&json!({"@context": ctx,
            "where": [["bind", "?x", "(str-dt \"x\" \"T\")"]],
            "insert": {"@id": "ex:u", "ex:p": "?x"}}))
        .commit()
        .await
        .expect("JSON-LD str-dt update");
    graph()
        .transact()
        .sparql_update(
            r#"PREFIX ex: <http://example.org/ns/>
               INSERT { ex:v ex:p ?x } WHERE { BIND(STRDT("x", <T>) AS ?x) }"#,
        )
        .commit()
        .await
        .expect("SPARQL STRDT update");

    let dts: Vec<_> = committed_flakes(&fluree, id)
        .await
        .into_iter()
        .filter(|f| f.p.name.as_ref() == "p")
        .map(|f| f.dt)
        .collect();
    assert_eq!(dts.len(), 4, "{dts:?}");
    assert!(
        dts.iter()
            .all(|dt| dt.namespace_code == fluree_vocab::namespaces::EMPTY
                && dt.name.as_ref() == "T"),
        "{dts:?}"
    );
    let view = fluree.ledger(id).await.unwrap();
    assert_eq!(
        sparql(
            &fluree,
            &view,
            "SELECT (COUNT(DISTINCT ?o) AS ?n) WHERE { ?s ex:p ?o }"
        )
        .await,
        json!([[1]])
    );
    let found = query_jsonld_formatted(
        &fluree,
        &view,
        &json!({"@context": ctx, "select": "?s", "orderBy": "?s",
                "where": {"@id": "?s", "ex:p": {"@value": "x", "@type": "T"}}}),
    )
    .await
    .unwrap();
    assert_eq!(found, json!(["ex:d", "ex:e", "ex:u", "ex:v"]));
}
