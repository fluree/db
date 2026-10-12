//! Typed literals keep their value through the binary index (issue #1987).
//!
//! SPARQL UPDATE committed well-typed literals such as `"2026-09-08"^^xsd:date`
//! as strings, and the index stored a string under its datatype's inline
//! o_type, so the string-dictionary id read back as the value (`1970-01-01`,
//! `i64::MIN + 4`). These tests read every lane — novelty, novelty over an
//! index, full rebuild, incremental build — for values written through SPARQL
//! and JSON-LD, for commits written before the fix, and for ill-typed
//! literals, which keep their lexical form.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::{Fluree, FlureeBuilder, LedgerState, ReindexOptions};
use fluree_db_core::FlakeValue;
use fluree_db_transact::TemplateTerm;
use serde_json::{json, Value};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const EX: &str = "http://example.org/";

/// `(predicate, lexical, datatype)`: every datatype the issue reported, plus
/// `xsd:float`, which the SPARQL coercion skipped too. Lexicals are canonical
/// so they read back unchanged.
const TYPED: &[(&str, &str, &str)] = &[
    ("date", "2026-09-08", "date"),
    ("dateTime", "2026-09-08T12:34:56Z", "dateTime"),
    ("time", "12:34:56", "time"),
    ("gYear", "2026", "gYear"),
    ("long", "20705", "long"),
    ("float", "1.5E0", "float"),
    ("duration", "P3D", "duration"),
];

type Row = (String, String, String);

fn expected_rows() -> Vec<Row> {
    let mut rows: Vec<Row> = TYPED
        .iter()
        .map(|(p, lexical, dt)| (p.to_string(), lexical.to_string(), dt.to_string()))
        .collect();
    rows.sort();
    rows
}

fn sparql_insert_typed(subject: &str) -> String {
    let triples: String = TYPED
        .iter()
        .map(|(p, lexical, dt)| format!("<{EX}{subject}> <{EX}{p}> \"{lexical}\"^^<{XSD}{dt}> .\n"))
        .collect();
    format!("INSERT DATA {{\n{triples}}}")
}

fn jsonld_insert_typed(subject: &str) -> Value {
    let mut node = serde_json::Map::new();
    node.insert("@id".into(), json!(format!("{EX}{subject}")));
    for (p, lexical, dt) in TYPED {
        node.insert(
            format!("{EX}{p}"),
            json!({"@value": lexical, "@type": format!("{XSD}{dt}")}),
        );
    }
    Value::Object(node)
}

fn lower_sparql_update(ledger: &LedgerState, sparql: &str) -> fluree_db_transact::Txn {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(!parsed.has_errors(), "{:?}", parsed.diagnostics);
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE")
}

async fn commit_txn(
    fluree: &Fluree,
    ledger: LedgerState,
    txn: fluree_db_transact::Txn,
) -> LedgerState {
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect("commit")
        .ledger
}

async fn sparql_update(fluree: &Fluree, ledger: LedgerState, sparql: &str) -> LedgerState {
    let txn = lower_sparql_update(&ledger, sparql);
    commit_txn(fluree, ledger, txn).await
}

/// `(predicate, value, datatype)` for every object of `subject`, local names only.
async fn objects_of(fluree: &Fluree, ledger: &LedgerState, subject: &str) -> Vec<Row> {
    let q = format!("SELECT ?p ?o (DATATYPE(?o) AS ?dt) WHERE {{ <{EX}{subject}> ?p ?o }}");
    let out = support::query_sparql(fluree, ledger, &q)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let local = |b: &Value, var: &str, ns: &str| {
        b[var]["value"]
            .as_str()
            .unwrap_or_else(|| panic!("?{var} unbound in {b}"))
            .trim_start_matches(ns)
            .to_string()
    };
    let mut rows: Vec<Row> = out["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| (local(b, "p", EX), local(b, "o", ""), local(b, "dt", XSD)))
        .collect();
    rows.sort();
    rows
}

/// Subjects whose `predicate` is the constant `"lexical"^^xsd:datatype`, via SPARQL.
async fn sparql_subjects_with(
    fluree: &Fluree,
    ledger: &LedgerState,
    (predicate, lexical, datatype): (&str, &str, &str),
) -> Vec<String> {
    let q = format!("SELECT ?s WHERE {{ ?s <{EX}{predicate}> \"{lexical}\"^^<{XSD}{datatype}> }}");
    let out = support::query_sparql(fluree, ledger, &q)
        .await
        .expect("query")
        .to_sparql_json(&ledger.snapshot)
        .expect("sparql json");
    let mut subjects: Vec<String> = out["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["s"]["value"].as_str().expect("?s").to_string())
        .collect();
    subjects.sort();
    subjects
}

/// JSON-LD twin of [`sparql_subjects_with`].
async fn jsonld_subjects_with(
    fluree: &Fluree,
    ledger: &LedgerState,
    (predicate, lexical, datatype): (&str, &str, &str),
) -> Vec<String> {
    let q = json!({
        "select": ["?s"],
        "where": {
            "@id": "?s",
            format!("{EX}{predicate}"): {"@value": lexical, "@type": format!("{XSD}{datatype}")}
        }
    });
    let out = support::query_jsonld(fluree, ledger, &q)
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    let mut subjects: Vec<String> = out
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            let s = if row.is_array() { &row[0] } else { row };
            s.as_str().expect("subject IRI").to_string()
        })
        .collect();
    subjects.sort();
    subjects
}

async fn reindexed(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    fluree
        .reindex(ledger_id, ReindexOptions::default())
        .await
        .expect("reindex");
    fully_indexed(fluree, ledger_id).await
}

async fn fully_indexed(fluree: &Fluree, ledger_id: &str) -> LedgerState {
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    assert!(
        ledger.snapshot.range_provider.is_some(),
        "reads must go through the binary index"
    );
    assert_eq!(
        ledger.snapshot.t,
        ledger.t(),
        "every commit must be indexed, so no value is served from novelty"
    );
    ledger
}

fn memory_fluree() -> Fluree {
    FlureeBuilder::memory().build_memory()
}

#[tokio::test]
async fn sparql_typed_literals_read_back_on_every_lane() {
    let fluree = memory_fluree();
    let ledger_id = "typed-literal-index:lanes";
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");

    let ledger = sparql_update(&fluree, ledger, &sparql_insert_typed("sparql")).await;
    let ledger = fluree
        .insert(ledger, &jsonld_insert_typed("jsonld"))
        .await
        .expect("JSON-LD insert")
        .ledger;
    for subject in ["sparql", "jsonld"] {
        assert_eq!(
            objects_of(&fluree, &ledger, subject).await,
            expected_rows(),
            "novelty, ex:{subject}"
        );
    }

    let indexed = reindexed(&fluree, ledger_id).await;
    for subject in ["sparql", "jsonld"] {
        assert_eq!(
            objects_of(&fluree, &indexed, subject).await,
            expected_rows(),
            "full rebuild, ex:{subject}"
        );
    }

    // Novelty over an index: the overlay encodes these values itself.
    let over_index = sparql_update(&fluree, indexed, &sparql_insert_typed("overlay")).await;
    assert!(over_index.snapshot.range_provider.is_some());
    assert_eq!(
        objects_of(&fluree, &over_index, "overlay").await,
        expected_rows(),
        "novelty over an index"
    );

    support::build_and_publish_index(&fluree, ledger_id).await;
    let incremental = fully_indexed(&fluree, ledger_id).await;
    assert_eq!(
        objects_of(&fluree, &incremental, "overlay").await,
        expected_rows(),
        "incremental build"
    );
}

#[tokio::test]
async fn bound_typed_literal_matches_values_from_both_surfaces() {
    let fluree = memory_fluree();
    let ledger_id = "typed-literal-index:bound";
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    let ledger = sparql_update(&fluree, ledger, &sparql_insert_typed("sparql")).await;
    let ledger = fluree
        .insert(ledger, &jsonld_insert_typed("jsonld"))
        .await
        .expect("JSON-LD insert")
        .ledger;
    let both = vec![format!("{EX}jsonld"), format!("{EX}sparql")];

    let indexed_ledger_id = ledger_id;
    for (lane, ledger) in [
        ("novelty", ledger),
        ("indexed", reindexed(&fluree, indexed_ledger_id).await),
    ] {
        for &literal in TYPED {
            assert_eq!(
                sparql_subjects_with(&fluree, &ledger, literal).await,
                both,
                "{lane}: SPARQL constant {literal:?}"
            );
            assert_eq!(
                jsonld_subjects_with(&fluree, &ledger, literal).await,
                both,
                "{lane}: JSON-LD constant {literal:?}"
            );
        }
    }
}

#[tokio::test]
async fn typed_literal_deletes_across_surfaces() {
    let fluree = memory_fluree();
    let ledger_id = "typed-literal-index:delete";
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");

    let ledger = fluree
        .insert(ledger, &jsonld_insert_typed("jsonld"))
        .await
        .expect("JSON-LD insert")
        .ledger;
    let ledger = sparql_update(&fluree, ledger, &sparql_insert_typed("sparql")).await;

    let ledger = sparql_update(
        &fluree,
        ledger,
        &sparql_insert_typed("jsonld").replacen("INSERT DATA", "DELETE DATA", 1),
    )
    .await;
    let mut delete = jsonld_insert_typed("sparql");
    let ledger = fluree
        .update(ledger, &json!({ "delete": delete.take() }))
        .await
        .expect("JSON-LD delete")
        .ledger;

    for subject in ["jsonld", "sparql"] {
        assert_eq!(
            objects_of(&fluree, &ledger, subject).await,
            Vec::<Row>::new(),
            "novelty: ex:{subject} should be fully retracted"
        );
    }
    let indexed = reindexed(&fluree, ledger_id).await;
    for subject in ["jsonld", "sparql"] {
        assert_eq!(
            objects_of(&fluree, &indexed, subject).await,
            Vec::<Row>::new(),
            "indexed: ex:{subject} should be fully retracted"
        );
    }
}

#[tokio::test]
async fn ill_typed_literals_keep_their_lexical_form() {
    let fluree = memory_fluree();
    let ledger_id = "typed-literal-index:ill-typed";
    let ledger = fluree.create_ledger(ledger_id).await.expect("create");
    fluree
        .insert_turtle(ledger, &format!("<{EX}seed> <{EX}name> \"seed\" ."))
        .await
        .expect("seed");
    let indexed = reindexed(&fluree, ledger_id).await;

    let turtle = format!(
        "<{EX}a> <{EX}date> \"1990-00-00\"^^<{XSD}date> ;\n\
         <{EX}int> \"abc\"^^<{XSD}integer> ;\n\
         <{EX}good> \"2026-09-08\"^^<{XSD}date> ."
    );
    let ledger = fluree
        .insert_turtle(indexed, &turtle)
        .await
        .expect("Turtle keeps ill-typed literals")
        .ledger;
    let expected: Vec<Row> = [
        ("date", "1990-00-00", "date"),
        ("good", "2026-09-08", "date"),
        ("int", "abc", "integer"),
    ]
    .iter()
    .map(|(p, o, dt)| (p.to_string(), o.to_string(), dt.to_string()))
    .collect();

    assert!(ledger.snapshot.range_provider.is_some());
    assert_eq!(
        objects_of(&fluree, &ledger, "a").await,
        expected,
        "novelty over an index"
    );
    let indexed = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        objects_of(&fluree, &indexed, "a").await,
        expected,
        "full rebuild"
    );

    // SPARQL rejects the literal as a constant, so a delete binds it instead.
    let ledger = sparql_update(
        &fluree,
        indexed,
        &format!(
            "DELETE {{ ?s <{EX}date> ?o }} WHERE {{ ?s <{EX}date> ?o FILTER(STR(?o) = \"1990-00-00\") }}"
        ),
    )
    .await;
    let remaining = expected[1..].to_vec();
    assert_eq!(
        objects_of(&fluree, &ledger, "a").await,
        remaining,
        "delete by bound variable, over an index"
    );
    let indexed = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        objects_of(&fluree, &indexed, "a").await,
        remaining,
        "delete by bound variable, full rebuild"
    );
}

#[tokio::test]
async fn sparql_update_rejects_ill_typed_literal() {
    let fluree = memory_fluree();
    let ledger_id = "typed-literal-index:reject";
    fluree.create_ledger(ledger_id).await.expect("create");
    for op in ["INSERT DATA", "DELETE DATA"] {
        let sparql = format!("{op} {{ <{EX}a> <{EX}date> \"1990-00-00\"^^<{XSD}date> }}");
        let err = fluree
            .graph(ledger_id)
            .transact()
            .sparql_update(&sparql)
            .commit()
            .await
            .expect_err("an ill-typed xsd:date must not lower");
        assert!(
            err.to_string().contains("xsd:date"),
            "{op}: error should name the datatype: {err}"
        );
    }
}

/// Commits written before the fix hold these literals as strings. They read
/// back — from novelty after a restart, and once reindexed — as the typed
/// values, so recovering a ledger needs only a reindex.
#[tokio::test]
async fn commits_holding_string_typed_literals_read_as_typed_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let ledger_id = "typed-literal-index:legacy";

    {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");
        let mut txn = lower_sparql_update(&ledger, &sparql_insert_typed("legacy"));
        // The object shape SPARQL UPDATE committed before the fix.
        for (template, (_, lexical, _)) in txn.insert_templates.iter_mut().zip(TYPED) {
            template.object = TemplateTerm::Value(FlakeValue::String(lexical.to_string()));
        }
        commit_txn(&fluree, ledger, txn).await;
    }

    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let ledger = fluree.ledger(ledger_id).await.expect("load from commits");
    assert!(ledger.snapshot.range_provider.is_none(), "novelty only");
    assert_eq!(
        objects_of(&fluree, &ledger, "legacy").await,
        expected_rows(),
        "novelty loaded from commits"
    );
    assert_eq!(
        sparql_subjects_with(&fluree, &ledger, TYPED[0]).await,
        vec![format!("{EX}legacy")],
        "a date constant matches the committed string-typed date"
    );

    let indexed = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        objects_of(&fluree, &indexed, "legacy").await,
        expected_rows(),
        "reindexed"
    );
    // Renders alike either way; only a typed value matches a typed constant.
    for &literal in TYPED {
        assert_eq!(
            sparql_subjects_with(&fluree, &indexed, literal).await,
            vec![format!("{EX}legacy")],
            "reindexed: constant {literal:?} matches the repaired value"
        );
    }
}

/// SPARQL UPDATE also committed `geo:wktLiteral` POINTs as strings. SPARQL now
/// lowers the same literal to a geo point, so the legacy string must read back
/// as one for a SPARQL `DELETE DATA` to retract it.
#[tokio::test]
async fn commits_holding_string_wkt_points_retract_through_sparql() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().to_string_lossy().to_string();
    let ledger_id = "typed-literal-index:legacy-wkt";
    let point = "POINT(2.35 48.85)";
    let insert = format!(
        "INSERT DATA {{ <{EX}paris> <{EX}loc> \"{point}\"^^<{}> }}",
        fluree_vocab::geo::WKT_LITERAL
    );

    {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
        let ledger = fluree.create_ledger(ledger_id).await.expect("create");
        let mut txn = lower_sparql_update(&ledger, &insert);
        // The object shape SPARQL UPDATE committed before the fix.
        txn.insert_templates[0].object = TemplateTerm::Value(FlakeValue::String(point.into()));
        commit_txn(&fluree, ledger, txn).await;
    }

    let fluree = FlureeBuilder::file(path).build().expect("reopen");
    let ledger = fluree.ledger(ledger_id).await.expect("load from commits");
    assert_eq!(objects_of(&fluree, &ledger, "paris").await.len(), 1);

    let ledger = sparql_update(
        &fluree,
        ledger,
        &insert.replacen("INSERT DATA", "DELETE DATA", 1),
    )
    .await;
    assert_eq!(
        objects_of(&fluree, &ledger, "paris").await,
        Vec::<Row>::new(),
        "novelty: the SPARQL delete retracts the legacy point"
    );
    let indexed = reindexed(&fluree, ledger_id).await;
    assert_eq!(
        objects_of(&fluree, &indexed, "paris").await,
        Vec::<Row>::new(),
        "reindexed: the SPARQL delete retracts the legacy point"
    );
}
