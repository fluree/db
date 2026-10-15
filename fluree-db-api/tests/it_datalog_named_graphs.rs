//! Rules that derive into named graphs, and rules that read them.
//!
//! A head names its graph with `GRAPH <iri> { … }` (SPARQL CONSTRUCT) or
//! `"@graph": "<iri>"` on an insert node (JSON-LD). The graph must already
//! exist and be a user named graph. Derived facts are visible in that graph
//! only — to `GRAPH` patterns in queries and in other rule bodies, through a
//! dataset's named graphs too — and the default graph never sees them.

use crate::it_datalog_rule_schedule::{fact_set, run, sparql_rules};
use crate::support::{genesis_ledger, normalize_rows, query_jsonld, query_sparql, MemoryLedger};
use fluree_db_api::{Fluree, FlureeBuilder};
use fluree_db_query::datalog_rules::RuleSchedule;
use serde_json::{json, Value};

const DATA: &str = r#"
@prefix ex: <http://example.org/> .
ex:a ex:parent ex:b . ex:b ex:parent ex:c . ex:c ex:parent ex:d .
GRAPH ex:inferred { ex:marker ex:note "inferred facts" . }
GRAPH ex:stage { ex:marker ex:note "staged facts" . }
"#;

async fn trig_ledger(ledger_id: &str) -> (Fluree, MemoryLedger) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(DATA)
        .execute()
        .await
        .expect("seed")
        .ledger;
    (fluree, ledger)
}

async fn store_sparql_rule(
    fluree: &Fluree,
    ledger: MemoryLedger,
    id: &str,
    construct: &str,
) -> MemoryLedger {
    let rule = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": format!("http://example.org/{id}"),
        "f:rule": {
            "@type": "f:sparql",
            "@value": format!("PREFIX ex: <http://example.org/> {construct}")
        }
    });
    fluree.insert(ledger, &rule).await.expect("rule").ledger
}

async fn sparql_rows(fluree: &Fluree, ledger: &MemoryLedger, select: &str) -> Vec<Value> {
    let rows = query_sparql(
        fluree,
        ledger,
        &format!("# PRAGMA reasoning: datalog\nPREFIX ex: <http://example.org/>\n{select}"),
    )
    .await
    .expect("query")
    .to_jsonld(&ledger.snapshot)
    .expect("format");
    let mut rows = normalize_rows(&rows);
    rows.sort_by_key(ToString::to_string);
    rows
}

fn grandparents() -> Vec<Value> {
    vec![json!(["ex:a", "ex:c"]), json!(["ex:b", "ex:d"])]
}

#[tokio::test]
async fn a_sparql_rule_derives_into_a_named_graph_only() {
    let (fluree, ledger) = trig_ledger("datalog/graphs-sparql:main").await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "grandparent",
        "CONSTRUCT { GRAPH ex:inferred { ?x ex:grandparent ?z } } \
         WHERE { ?x ex:parent ?y . ?y ex:parent ?z }",
    )
    .await;
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "SELECT ?x ?z WHERE { GRAPH ex:inferred { ?x ex:grandparent ?z } }"
        )
        .await,
        grandparents()
    );
    assert!(sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?x ?z WHERE { ?x ex:grandparent ?z }"
    )
    .await
    .is_empty());
    assert!(sparql_rows(
        &fluree,
        &ledger,
        "SELECT ?x ?z WHERE { GRAPH ex:stage { ?x ex:grandparent ?z } }"
    )
    .await
    .is_empty());
}

/// The JSON-LD spellings: `"@graph": "<iri>"` on a node, as in a
/// transaction, and a JSON-LD named graph object.
#[tokio::test]
async fn json_ld_rules_derive_into_a_named_graph() {
    let (fluree, ledger) = trig_ledger("datalog/graphs-jsonld:main").await;
    let ctx = json!({"ex": "http://example.org/"});
    let body = json!([
        {"@id": "?x", "ex:parent": "?y"},
        {"@id": "?y", "ex:parent": "?z"}
    ]);
    for insert in [
        json!({"@id": "?x", "@graph": "ex:inferred", "ex:grandparent": {"@id": "?z"}}),
        json!({"@id": "ex:inferred", "@graph": [{"@id": "?x", "ex:grandparent": {"@id": "?z"}}]}),
    ] {
        let query = json!({
            "@context": ctx,
            "select": ["?x", "?z"],
            // A where-clause graph name is not expanded against @context.
            "where": [["graph", "http://example.org/inferred", {"@id": "?x", "ex:grandparent": "?z"}]],
            "reasoning": "datalog",
            "rules": [{"@context": ctx, "where": body, "insert": insert}]
        });
        let rows = query_jsonld(&fluree, &ledger, &query)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("format");
        let mut rows = normalize_rows(&rows);
        rows.sort_by_key(ToString::to_string);
        assert_eq!(rows, grandparents(), "{insert}");

        let default_graph = json!({
            "@context": ctx,
            "select": ["?x", "?z"],
            "where": {"@id": "?x", "ex:grandparent": "?z"},
            "reasoning": "datalog",
            "rules": [{"@context": ctx, "where": body, "insert": insert}]
        });
        let rows = query_jsonld(&fluree, &ledger, &default_graph)
            .await
            .expect("query")
            .to_jsonld(&ledger.snapshot)
            .expect("format");
        assert!(normalize_rows(&rows).is_empty(), "{insert}");
    }
}

/// One rule stages facts in a named graph and another reads that graph to
/// derive into the default graph. Dependency order runs the reader after the
/// writer, and recursion inside a named graph reaches its closure there.
#[tokio::test]
async fn rules_chain_and_recurse_through_a_named_graph() {
    let (fluree, ledger) = trig_ledger("datalog/graphs-chain:main").await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "lineageReader",
        "CONSTRUCT { ?x ex:hasLineage true } WHERE { GRAPH ex:stage { ?x ex:lineage ?y } }",
    )
    .await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "lineageStep",
        "CONSTRUCT { GRAPH ex:stage { ?x ex:lineage ?z } } \
         WHERE { ?x ex:parent ?y GRAPH ex:stage { ?y ex:lineage ?z } }",
    )
    .await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "lineageBase",
        "CONSTRUCT { GRAPH ex:stage { ?x ex:lineage ?y } } WHERE { ?x ex:parent ?y }",
    )
    .await;
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "SELECT ?x ?y WHERE { GRAPH ex:stage { ?x ex:lineage ?y } }"
        )
        .await,
        vec![
            json!(["ex:a", "ex:b"]),
            json!(["ex:a", "ex:c"]),
            json!(["ex:a", "ex:d"]),
            json!(["ex:b", "ex:c"]),
            json!(["ex:b", "ex:d"]),
            json!(["ex:c", "ex:d"]),
        ]
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "SELECT ?x WHERE { ?x ex:hasLineage true }"
        )
        .await,
        vec![json!(["ex:a"]), json!(["ex:b"]), json!(["ex:c"])]
    );
}

/// A dataset query reads named graphs through its own graph references; the
/// derived facts must reach those too.
#[tokio::test]
async fn a_dataset_query_sees_facts_derived_into_its_named_graph() {
    let (fluree, ledger) = trig_ledger("datalog/graphs-dataset:main").await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "grandparent",
        "CONSTRUCT { GRAPH ex:inferred { ?x ex:grandparent ?z } } \
         WHERE { ?x ex:parent ?y . ?y ex:parent ?z }",
    )
    .await;
    let _ = ledger;
    let ledger_id = "datalog/graphs-dataset:main";
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "fromNamed": [format!("{ledger_id}#http://example.org/inferred")],
        "select": ["?x", "?z"],
        "where": [["graph", "?g", {"@id": "?x", "ex:grandparent": "?z"}]],
        "reasoning": "datalog"
    });
    let rows = fluree
        .query_from()
        .jsonld(&query)
        .execute_formatted()
        .await
        .expect("dataset query");
    let mut rows = normalize_rows(&rows);
    rows.sort_by_key(ToString::to_string);
    assert_eq!(rows, grandparents());
}

/// A head graph must be a user named graph the ledger already has.
#[tokio::test]
async fn a_head_graph_must_be_an_existing_user_graph() {
    let (_, ledger) = trig_ledger("datalog/graphs-refused:main").await;
    for (template, expected) in [
        (
            "GRAPH ex:nowhere { ?x ex:g ?z }",
            "<http://example.org/nowhere>, which this ledger does not have",
        ),
        (
            "GRAPH <urn:fluree:datalog/graphs-refused:main#txn-meta> { ?x ex:g ?z }",
            "a system graph",
        ),
        (
            "GRAPH <urn:default> { ?x ex:g ?z }",
            "names the default graph",
        ),
        ("GRAPH ?g { ?x ex:g ?z }", "is a variable"),
    ] {
        let construct = format!("CONSTRUCT {{ {template} }} WHERE {{ ?x ex:parent ?z }}");
        let error = rule_error(&ledger, &construct).await;
        assert!(error.contains(expected), "{template}: {error}");
    }
}

/// The error a single SPARQL rule fails with, at parse or at run time.
async fn rule_error(ledger: &MemoryLedger, construct: &str) -> String {
    let rule = json!({
        "@id": "ex:rule",
        "f:rule": {
            "@type": "f:sparql",
            "@value": format!("PREFIX ex: <http://example.org/> {construct}")
        }
    });
    let parsed =
        match fluree_db_query::datalog_rules::parse_query_time_rule(&rule, &ledger.snapshot, 0) {
            Ok(parsed) => parsed,
            Err(e) => return e.to_string(),
        };
    let mut rules = fluree_db_query::datalog_rules::DatalogRuleSet::new();
    rules.add_rule(parsed).expect("one rule");
    fluree_db_query::datalog_rules::run_fixpoint_with_schedule(
        ledger.as_graph_db_ref(0),
        &rules,
        100,
        &fluree_db_query::reasoning::ReasoningBudget::unlimited(),
        fluree_db_query::datalog_rules::RuleExecutionEnv::default(),
        RuleSchedule::Dependencies,
    )
    .await
    .err()
    .unwrap_or_else(|| panic!("{construct}: the rule ran"))
    .to_string()
}

/// A recursive group writing into both the default graph and a named graph:
/// re-reading only new facts must never seed a default-graph pattern with a
/// fact from the named graph. The whole-program reference never seeds.
#[tokio::test]
async fn new_facts_seed_only_patterns_of_their_own_graph() {
    let (_, ledger) = trig_ledger("datalog/graphs-seeding:main").await;
    let rules = sparql_rules(
        &ledger,
        &[
            "CONSTRUCT { ?x ex:link ?y } WHERE { ?x ex:parent ?y }".to_string(),
            "CONSTRUCT { GRAPH ex:inferred { ?y ex:link ?x } } WHERE { ?x ex:link ?y }".to_string(),
            "CONSTRUCT { ?x ex:link ?z } WHERE { ?x ex:link ?y . ?y ex:link ?z \
             FILTER(?x != ?z) }"
                .to_string(),
        ],
    );
    let scheduled = run(&ledger, &rules, RuleSchedule::Dependencies).await;
    let reference = run(&ledger, &rules, RuleSchedule::WholeProgram).await;
    assert_eq!(fact_set(&scheduled), fact_set(&reference));
    let in_named = scheduled
        .derived_flakes
        .iter()
        .filter(|f| f.g.is_some())
        .count();
    let in_default = scheduled.derived_flakes.len() - in_named;
    // a->b->c->d: six forward links in the default graph, and each reversed
    // into the named graph.
    assert_eq!((in_default, in_named), (6, 6));
}

/// One triple in two graphs is two facts: deriving it into the default
/// graph must not stop it being derived into a named graph.
#[tokio::test]
async fn one_triple_can_be_derived_into_two_graphs() {
    let (fluree, ledger) = trig_ledger("datalog/graphs-twice:main").await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "kinDefault",
        "CONSTRUCT { ?x ex:kin ?y } WHERE { ?x ex:parent ?y }",
    )
    .await;
    let ledger = store_sparql_rule(
        &fluree,
        ledger,
        "kinNamed",
        "CONSTRUCT { GRAPH ex:inferred { ?x ex:kin ?y } } WHERE { ?x ex:parent ?y }",
    )
    .await;
    let expected = vec![
        json!(["ex:a", "ex:b"]),
        json!(["ex:b", "ex:c"]),
        json!(["ex:c", "ex:d"]),
    ];
    assert_eq!(
        sparql_rows(&fluree, &ledger, "SELECT ?x ?y WHERE { ?x ex:kin ?y }").await,
        expected
    );
    assert_eq!(
        sparql_rows(
            &fluree,
            &ledger,
            "SELECT ?x ?y WHERE { GRAPH ex:inferred { ?x ex:kin ?y } }"
        )
        .await,
        expected
    );
}

/// Over a published index, and then with a later commit in novelty, rules
/// derive into and read from named graphs as they do over novelty alone.
#[tokio::test]
async fn named_graph_rules_work_over_an_index() {
    use crate::support::{instances_sharing_storage, rebuild_and_publish_index};

    let ledger_id = "datalog/graphs-indexed:main";
    let (writer, reader) = instances_sharing_storage();
    let ledger = writer
        .stage_owned(genesis_ledger(&writer, ledger_id))
        .upsert_turtle(DATA)
        .execute()
        .await
        .expect("seed")
        .ledger;
    let rules = [
        (
            "grandparent",
            "CONSTRUCT { GRAPH ex:inferred { ?x ex:grandparent ?z } } \
             WHERE { ?x ex:parent ?y . ?y ex:parent ?z }",
        ),
        (
            "lineageBase",
            "CONSTRUCT { GRAPH ex:stage { ?x ex:lineage ?y } } WHERE { ?x ex:parent ?y }",
        ),
        (
            "lineageStep",
            "CONSTRUCT { GRAPH ex:stage { ?x ex:lineage ?z } } \
             WHERE { ?x ex:parent ?y GRAPH ex:stage { ?y ex:lineage ?z } }",
        ),
        (
            "lineageReader",
            "CONSTRUCT { ?x ex:hasLineage true } WHERE { GRAPH ex:stage { ?x ex:lineage ?y } }",
        ),
    ];
    let mut ledger = ledger;
    for (id, construct) in rules {
        ledger = store_sparql_rule(&writer, ledger, id, construct).await;
    }
    rebuild_and_publish_index(&writer, ledger_id).await;
    let ledger = reader.ledger(ledger_id).await.expect("reader load");
    assert!(ledger.index_t() > 0, "the reader loads the published index");

    let grandparents_in_graph = "SELECT ?x ?z WHERE { GRAPH ex:inferred { ?x ex:grandparent ?z } }";
    let with_lineage = "SELECT ?x WHERE { ?x ex:hasLineage true }";
    assert_eq!(
        sparql_rows(&reader, &ledger, grandparents_in_graph).await,
        grandparents()
    );
    assert!(sparql_rows(
        &reader,
        &ledger,
        "SELECT ?x ?z WHERE { ?x ex:grandparent ?z }"
    )
    .await
    .is_empty());
    assert_eq!(
        sparql_rows(&reader, &ledger, with_lineage).await,
        vec![json!(["ex:a"]), json!(["ex:b"]), json!(["ex:c"])]
    );

    let ledger = reader
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:d", "ex:parent": {"@id": "ex:e"}}),
        )
        .await
        .expect("a commit into novelty")
        .ledger;
    let mut extended = grandparents();
    extended.push(json!(["ex:c", "ex:e"]));
    assert_eq!(
        sparql_rows(&reader, &ledger, grandparents_in_graph).await,
        extended
    );
    assert_eq!(
        sparql_rows(&reader, &ledger, with_lineage).await,
        vec![
            json!(["ex:a"]),
            json!(["ex:b"]),
            json!(["ex:c"]),
            json!(["ex:d"])
        ]
    );
}
