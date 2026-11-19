//! Rule dependency analysis over rules parsed by the real JSON-LD and SPARQL
//! rule parsers.
//!
//! The analysis is diagnostic only: these tests pin which rules may observe
//! which others' conclusions, and which rules form recursive groups, before
//! any scheduling relies on it.

use crate::it_datalog_program_reachability;
use crate::support::{genesis_ledger, MemoryLedger};
use fluree_db_api::FlureeBuilder;
use fluree_db_query::datalog_rules::{
    parse_query_time_rule, Access, DatalogRuleSet, GraphScope, PredicateFootprint, RuleDependencies,
};
use serde_json::{json, Value};
use std::sync::Arc;

/// A ledger whose snapshot knows the `ex:` namespace, so rule IRIs encode.
async fn ledger() -> MemoryLedger {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "datalog/rule-dependencies:main");
    let seed = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:seed",
        "ex:controls": {"@id": "ex:other"}
    });
    fluree.insert(ledger, &seed).await.expect("seed").ledger
}

fn stored(id: &str, rule: Value) -> Value {
    json!({"@id": id, "f:rule": rule})
}

fn jsonld(mut rule: Value) -> Value {
    rule["@context"] = json!({"ex": "http://example.org/"});
    rule
}

fn sparql(construct: &str) -> Value {
    json!({"@type": "f:sparql", "@value": format!("PREFIX ex: <http://example.org/>\n{construct}")})
}

struct Program {
    rules: DatalogRuleSet,
    dependencies: RuleDependencies,
}

impl Program {
    fn parse(ledger: &MemoryLedger, rules: &[(&str, Value)]) -> Self {
        let mut set = DatalogRuleSet::new();
        for (i, (id, rule)) in rules.iter().enumerate() {
            let parsed = parse_query_time_rule(&stored(id, rule.clone()), &ledger.snapshot, i)
                .unwrap_or_else(|e| panic!("rule {id} failed to parse: {e}"));
            set.add_rule(parsed).expect("unique rule ids");
        }
        let dependencies = set.dependencies();
        Program {
            rules: set,
            dependencies,
        }
    }

    fn index(&self, name: &str) -> usize {
        self.rules
            .iter_in_order()
            .position(|r| r.name == name)
            .unwrap_or_else(|| panic!("no rule named {name}"))
    }

    fn name(&self, index: usize) -> String {
        self.rules.iter_in_order().nth(index).unwrap().name.clone()
    }

    fn producers_of(&self, name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .dependencies
            .producers_of(self.index(name))
            .iter()
            .map(|&i| self.name(i))
            .collect();
        names.sort();
        names
    }

    /// Each group as (sorted rule names, recursive), producers first.
    fn groups(&self) -> Vec<(Vec<String>, bool)> {
        self.dependencies
            .components()
            .iter()
            .map(|c| {
                let mut names: Vec<String> = c.rules.iter().map(|&i| self.name(i)).collect();
                names.sort();
                (names, c.recursive)
            })
            .collect()
    }

    fn position(&self, name: &str) -> usize {
        self.groups()
            .iter()
            .position(|(names, _)| names.iter().any(|n| n == name))
            .unwrap()
    }
}

fn names(list: &[&str]) -> Vec<String> {
    list.iter().map(ToString::to_string).collect()
}

#[tokio::test]
async fn reachability_program_has_two_recursive_groups_and_six_run_once_rules() {
    let ledger = ledger().await;
    let ids = [
        "ex:controlsBase",
        "ex:controlsStep",
        "ex:associatedBase",
        "ex:associatedSymmetric",
        "ex:associatedStep",
        "ex:exposure",
        "ex:exposedType",
        "ex:reviewQueue",
        "ex:associatedExposure",
    ];
    let rules: Vec<(&str, Value)> = ids
        .iter()
        .copied()
        .zip(
            it_datalog_program_reachability::rules()
                .into_iter()
                .map(jsonld),
        )
        .collect();
    let program = Program::parse(&ledger, &rules);

    let groups = program.groups();
    assert_eq!(groups.len(), 8, "groups: {groups:?}");
    assert!(groups.contains(&(names(&["ex:controlsStep"]), true)));
    assert!(groups.contains(&(
        names(&["ex:associatedStep", "ex:associatedSymmetric"]),
        true
    )));
    for run_once in [
        "ex:controlsBase",
        "ex:associatedBase",
        "ex:exposure",
        "ex:exposedType",
        "ex:reviewQueue",
        "ex:associatedExposure",
    ] {
        assert!(
            groups.contains(&(names(&[run_once]), false)),
            "{run_once} should be a non-recursive group of its own: {groups:?}"
        );
    }

    let p = |name| program.position(name);
    assert!(p("ex:controlsBase") < p("ex:controlsStep"));
    assert!(p("ex:associatedBase") < p("ex:associatedStep"));
    assert!(p("ex:controlsStep") < p("ex:exposure"));
    assert!(p("ex:exposure") < p("ex:exposedType"));
    assert!(p("ex:exposedType") < p("ex:reviewQueue"));
    assert!(p("ex:exposedType") < p("ex:associatedExposure"));
    assert!(p("ex:associatedStep") < p("ex:associatedExposure"));
}

#[tokio::test]
async fn class_reads_depend_only_on_matching_or_generic_type_writes() {
    let ledger = ledger().await;
    let program = Program::parse(
        &ledger,
        &[
            (
                "ex:makesA",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:controls": "?y"},
                    "insert": {"@id": "?x", "@type": "ex:A"}
                })),
            ),
            (
                "ex:makesAnyClass",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:classifiedAs": "?c"},
                    "insert": {"@id": "?x", "@type": {"@id": "?c"}}
                })),
            ),
            (
                "ex:readsB",
                jsonld(json!({
                    "where": {"@id": "?x", "@type": "ex:B"},
                    "insert": {"@id": "?x", "ex:isB": true}
                })),
            ),
            (
                "ex:readsAnyType",
                sparql("CONSTRUCT { ?x ex:typed true } WHERE { ?x a ?c }"),
            ),
        ],
    );

    assert_eq!(
        program.producers_of("ex:readsB"),
        names(&["ex:makesAnyClass"])
    );
    assert_eq!(
        program.producers_of("ex:readsAnyType"),
        names(&["ex:makesA", "ex:makesAnyClass"])
    );
}

/// Shapes whose reads a constant-predicate collector does not see: a variable
/// predicate, a property path and the steps of a sequence inside one, a
/// GRAPH scope, an edge annotation, and an EXISTS nested inside a FILTER
/// expression. A variable-predicate head is likewise a write of anything.
#[tokio::test]
async fn variable_predicates_paths_and_nested_exists_are_read() {
    let ledger = ledger().await;
    let mut path_rule = jsonld(json!({
        "where": {"@id": "?x", "controlsPlus": "?y"},
        "insert": {"@id": "?x", "ex:reaches": {"@id": "?y"}}
    }));
    path_rule["@context"]["controlsPlus"] = json!({"@path": "ex:controls+"});

    let program = Program::parse(
        &ledger,
        &[
            (
                "ex:writesControls",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:owns": "?y"},
                    "insert": {"@id": "?x", "ex:controls": {"@id": "?y"}}
                })),
            ),
            (
                "ex:writesWatched",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:flaggedBy": "?y"},
                    "insert": {"@id": "?x", "ex:watched": {"@id": "?y"}}
                })),
            ),
            ("ex:readsPath", path_rule),
            (
                "ex:readsNestedExists",
                sparql(
                    "CONSTRUCT { ?x ex:alert ex:yes } WHERE { \
                       ?x ex:owns ?y \
                       FILTER(EXISTS { ?x ex:watched ?z } || ?y = ex:k) }",
                ),
            ),
            (
                "ex:readsSequence",
                sparql("CONSTRUCT { ?x ex:q ?y } WHERE { ?x (ex:owns/ex:watched)+ ?y }"),
            ),
            (
                "ex:readsGraph",
                sparql(
                    "CONSTRUCT { ?x ex:g true } WHERE { \
                       GRAPH <http://example.org/g> { ?x ex:watched ?y } }",
                ),
            ),
            (
                "ex:readsAnnotation",
                sparql("CONSTRUCT { ?x ex:a true } WHERE { ?x ex:watched ?y {| ex:since ?d |} }"),
            ),
            (
                "ex:readsVariablePredicate",
                sparql("CONSTRUCT { ?s ex:touched true } WHERE { ?s ?p ?o }"),
            ),
            (
                "ex:writesVariablePredicate",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:relation": "?p", "ex:target": "?y"},
                    "insert": {"@id": "?x", "?p": {"@id": "?y"}}
                })),
            ),
        ],
    );

    let all_writers = |extra: &[&str]| {
        let mut list = vec!["ex:writesVariablePredicate"];
        list.extend_from_slice(extra);
        let mut list = names(&list);
        list.sort();
        list
    };
    assert_eq!(
        program.producers_of("ex:readsPath"),
        all_writers(&["ex:writesControls"])
    );
    assert_eq!(
        program.producers_of("ex:readsNestedExists"),
        all_writers(&["ex:writesWatched"])
    );
    for reader in ["ex:readsSequence", "ex:readsGraph", "ex:readsAnnotation"] {
        assert_eq!(
            program.producers_of(reader),
            all_writers(&["ex:writesWatched"]),
            "{reader}"
        );
    }
    let every_rule: Vec<String> = {
        let mut list: Vec<String> = (0..program.rules.len()).map(|i| program.name(i)).collect();
        list.sort();
        list
    };
    assert_eq!(
        program.producers_of("ex:readsVariablePredicate"),
        every_rule
    );

    let path_reads = &program
        .dependencies
        .footprint(program.index("ex:readsPath"))
        .reads;
    let controls = ledger
        .snapshot
        .encode_iri("http://example.org/controls")
        .expect("ex:controls encodes");
    assert!(path_reads
        .iter()
        .any(|read| read.predicate == PredicateFootprint::Exact(controls.clone())));

    let reads = |name| {
        program
            .dependencies
            .footprint(program.index(name))
            .reads
            .clone()
    };
    let exact = |local: &str| {
        PredicateFootprint::Exact(
            ledger
                .snapshot
                .encode_iri(&format!("http://example.org/{local}"))
                .expect("encodes"),
        )
    };
    // A named graph's IRI may denote the default graph, so the producers
    // above cannot tell a scoped read from an unscoped one.
    assert_eq!(
        reads("ex:readsGraph"),
        vec![Access {
            graph: GraphScope::Named(Arc::from("http://example.org/g")),
            predicate: exact("watched"),
        }]
    );
    let annotation_reads = reads("ex:readsAnnotation");
    for predicate in [
        exact("watched"),
        PredicateFootprint::Reifier,
        exact("since"),
    ] {
        assert!(
            annotation_reads.contains(&Access {
                graph: GraphScope::Default,
                predicate: predicate.clone(),
            }),
            "the annotated read must cover {predicate}: {annotation_reads:?}"
        );
    }
}

#[tokio::test]
async fn rules_generating_values_are_not_repeatable_but_now_is() {
    let ledger = ledger().await;
    let program = Program::parse(
        &ledger,
        &[
            (
                "ex:plain",
                sparql("CONSTRUCT { ?x ex:seen true } WHERE { ?x ex:controls ?y }"),
            ),
            (
                "ex:stampsNow",
                sparql("CONSTRUCT { ?x ex:checkedAt ?t } WHERE { ?x ex:controls ?y BIND(NOW() AS ?t) }"),
            ),
            (
                "ex:mintsId",
                sparql("CONSTRUCT { ?x ex:caseId ?id } WHERE { ?x ex:controls ?y BIND(STRUUID() AS ?id) }"),
            ),
            (
                "ex:samples",
                sparql("CONSTRUCT { ?x ex:sampled true } WHERE { ?x ex:controls ?y FILTER(RAND() < 0.5) }"),
            ),
        ],
    );
    let repeatable = |name| {
        program
            .dependencies
            .footprint(program.index(name))
            .repeatable
    };
    assert!(repeatable("ex:plain"));
    for volatile in ["ex:mintsId", "ex:samples"] {
        assert!(!repeatable(volatile), "{volatile} must not be repeatable");
    }
    // Every rule body in a run sees one evaluation time, so NOW() repeats.
    let footprint = program
        .dependencies
        .footprint(program.index("ex:stampsNow"));
    assert!(footprint.repeatable && footprint.reads_clock);
    assert!(
        !program
            .dependencies
            .footprint(program.index("ex:plain"))
            .reads_clock
    );
}

/// A path that can match zero hops pairs every term in the graph with itself,
/// so a rule deriving any new term, under any predicate, can add a match.
#[tokio::test]
async fn zero_length_paths_depend_on_every_writer() {
    let ledger = ledger().await;
    let mut jsonld_star = jsonld(json!({
        "where": {"@id": "?x", "controlsStar": "?y"},
        "insert": {"@id": "?x", "ex:reachesJsonLd": {"@id": "?y"}}
    }));
    jsonld_star["@context"]["controlsStar"] = json!({"@path": "ex:controls*"});

    let program = Program::parse(
        &ledger,
        &[
            (
                "ex:writesUnrelated",
                jsonld(json!({
                    "where": {"@id": "?x", "ex:owns": "?y"},
                    "insert": {"@id": "?x", "ex:unrelated": {"@id": "?y"}}
                })),
            ),
            (
                "ex:readsStar",
                sparql("CONSTRUCT { ?x ex:reachesStar ?y } WHERE { ?x ex:controls* ?y }"),
            ),
            (
                "ex:readsOptionalHop",
                sparql("CONSTRUCT { ?x ex:reachesOptional ?y } WHERE { ?x ex:controls? ?y }"),
            ),
            ("ex:readsJsonLdStar", jsonld_star),
            (
                "ex:readsPlus",
                sparql("CONSTRUCT { ?x ex:reachesPlus ?y } WHERE { ?x ex:controls+ ?y }"),
            ),
        ],
    );

    for zero_length in ["ex:readsStar", "ex:readsOptionalHop", "ex:readsJsonLdStar"] {
        let producers = program.producers_of(zero_length);
        assert!(
            producers.contains(&"ex:writesUnrelated".to_string()),
            "{zero_length} must depend on a writer of an unrelated predicate, got {producers:?}"
        );
        assert!(
            producers.contains(&zero_length.to_string()),
            "{zero_length} must depend on itself: its own conclusions add terms"
        );
    }
    assert!(
        program.producers_of("ex:readsPlus").is_empty(),
        "a one-or-more path reads only its predicate"
    );
}
