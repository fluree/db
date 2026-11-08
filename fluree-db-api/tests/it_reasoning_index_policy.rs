//! Reasoning indexes under view policy.
//!
//! A restricted caller is served a generation at exactly its basis, and its
//! policy judges the served facts as query-time reasoning's: each fact by its
//! own subject, predicate and object, class policies by the subject's reasoned
//! classes, and conditions against the derived facts the query reads.
//!
//! Each served case checks three things. Its answer is written out by hand.
//! Query-time reasoning gives the same answer on a twin ledger with no index.
//! And the index answered with no closure computed: the response names the
//! index (an OWL RL closure would replace that report with its own), and the
//! served query carries a one-fact reasoning budget and `exact` freshness,
//! under which the datalog closure here, seven facts, is an error rather than
//! an answer.

use crate::support::{genesis_ledger, normalize_rows};
use fluree_db_api::{
    DataSetDb, Fluree, FlureeBuilder, GovernanceOptions, GraphDb, OwnedStreamQuery,
    QueryExecutionOptions, ReindexOptions, Tracker,
};
use fluree_db_core::reasoning_index::ReasoningIndexId;
use fluree_db_core::ReasoningIndexTally;
use serde_json::{json, Value};

const EX: &str = "http://example.org/";

fn ex(name: &str) -> String {
    format!("{EX}{name}")
}

fn ctx() -> Value {
    json!({
        "ex": EX,
        "f": "https://ns.flur.ee/db#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
    })
}

fn stored_rule(id: &str, rule: Value) -> Value {
    json!({"@id": id, "f:rule": {"@type": "@json", "@value": rule}})
}

/// Stored datalog rules derive `ex:ancestor` and `ex:HighEarner` (a salary
/// over 200,000, which only `ex:alice` has); under OWL RL `ex:erin`, a
/// `Manager`, is an `Employee`. `ex:alice` is a stored `Person` and
/// `ex:carol` a stored `Retiree`.
fn family() -> Value {
    let rule_ctx = json!({"ex": EX});
    json!({
        "@context": ctx(),
        "@graph": [
            stored_rule("ex:ancestorBase", json!({
                "@context": rule_ctx,
                "where": {"@id": "?x", "ex:parent": "?y"},
                "insert": {"@id": "?x", "ex:ancestor": {"@id": "?y"}}
            })),
            stored_rule("ex:ancestorStep", json!({
                "@context": rule_ctx,
                "where": {"@id": "?x", "ex:ancestor": {"ex:ancestor": "?z"}},
                "insert": {"@id": "?x", "ex:ancestor": {"@id": "?z"}}
            })),
            stored_rule("ex:highEarnerRule", json!({
                "@context": rule_ctx,
                "where": [{"@id": "?p", "ex:salary": "?s"}, ["filter", "(> ?s 200000)"]],
                "insert": {"@id": "?p", "@type": "ex:HighEarner"}
            })),
            {"@id": "ex:alice", "@type": "ex:Person", "ex:name": "Alice",
             "ex:parent": {"@id": "ex:bob"}, "ex:salary": 250_000},
            {"@id": "ex:bob", "ex:name": "Bob", "ex:parent": {"@id": "ex:carol"},
             "ex:salary": 90000},
            {"@id": "ex:carol", "@type": "ex:Retiree", "ex:name": "Carol",
             "ex:parent": {"@id": "ex:dave"}},
            {"@id": "ex:dave", "ex:name": "Dave"},
            {"@id": "ex:Manager", "rdfs:subClassOf": {"@id": "ex:Employee"}},
            {"@id": "ex:erin", "@type": "ex:Manager", "ex:name": "Erin"}
        ]
    })
}

fn published(outcome: fluree_db_api::reasoning_index::RefreshOutcome) {
    assert!(
        matches!(
            outcome,
            fluree_db_api::reasoning_index::RefreshOutcome::Published { .. }
        ),
        "{outcome:?}"
    );
}

/// A view policy, as inline rules and a default.
#[derive(Clone, Debug)]
struct Policy {
    rules: Value,
    default_allow: bool,
}

fn deny(target: &str, iri: &str) -> Policy {
    Policy {
        rules: json!([{
            "@id": "ex:deny",
            "f:required": true,
            target: [{"@id": ex(iri)}],
            "f:action": "f:view",
            "f:allow": false
        }]),
        default_allow: true,
    }
}

fn deny_property(property: &str) -> Policy {
    deny("f:onProperty", property)
}

fn deny_class(class: &str) -> Policy {
    deny("f:onClass", class)
}

/// Denies each class, as one policy.
fn deny_classes(classes: &[&str]) -> Policy {
    let rules = classes
        .iter()
        .map(|class| deny_class(class).rules[0].clone());
    Policy {
        rules: Value::Array(
            rules
                .enumerate()
                .map(|(i, mut rule)| {
                    rule["@id"] = json!(format!("ex:deny{i}"));
                    rule
                })
                .collect(),
        ),
        default_allow: true,
    }
}

fn deny_subject(subject: &str) -> Policy {
    deny("f:onSubject", subject)
}

/// Default deny, with names allowed.
fn only_names() -> Policy {
    Policy {
        rules: json!([{
            "@id": "ex:names",
            "f:onProperty": [{"@id": ex("name")}],
            "f:action": "f:view",
            "f:allow": true
        }]),
        default_allow: false,
    }
}

/// A salary shows only for a `HighEarner`: a condition on a derived class.
fn salaries_of_high_earners() -> Policy {
    Policy {
        rules: json!([{
            "@id": "ex:highEarnerSalaries",
            "f:onProperty": [{"@id": ex("salary")}],
            "f:action": "f:view",
            "f:query": {"@type": "@json", "@value": {
                "@context": {"ex": EX},
                "where": [{"@id": "?$this", "@type": "ex:HighEarner"}]
            }}
        }]),
        default_allow: true,
    }
}

impl Policy {
    fn governance(&self) -> GovernanceOptions {
        GovernanceOptions {
            policy: Some(self.rules.clone()),
            default_allow: Some(self.default_allow),
            ..Default::default()
        }
    }
}

fn query(select: Value, where_: Value, reasoning: &str) -> Value {
    json!({
        "@context": ctx(),
        "select": select,
        "where": where_,
        "reasoning": reasoning
    })
}

fn with(mut query: Value, key: &str, value: Value) -> Value {
    query[key] = value;
    query
}

/// `query` such that only an index can answer it: any closure computed at
/// query time is capped, and `exact` refuses a capped answer.
fn index_only(query: &Value) -> Value {
    with(
        with(query.clone(), "reasoningBudget", json!({"maxFacts": 1})),
        "reasoningFreshness",
        json!("exact"),
    )
}

#[derive(Clone, Copy, Debug)]
enum Entry {
    /// A policy-wrapped view.
    View,
    /// A connection query naming its ledger, with the policy in its opts.
    Connection,
}

const ENTRIES: [Entry; 2] = [Entry::View, Entry::Connection];

type Answer = (Vec<Value>, Option<ReasoningIndexTally>);

/// Two ledgers with the same facts at the same `t`, each indexed: `served`
/// also has a published generation for each program, `query_time` has none.
struct Twins {
    fluree: Fluree,
    served: String,
    query_time: String,
    indexes: Vec<ReasoningIndexId>,
}

async fn twins(name: &str, programs: &[&str]) -> Twins {
    seeded(FlureeBuilder::memory().build_memory(), name, programs).await
}

async fn seeded(fluree: Fluree, name: &str, programs: &[&str]) -> Twins {
    let served = format!("reasoning-index-policy/{name}:main");
    let query_time = format!("reasoning-index-policy/{name}-query-time:main");
    for id in [&served, &query_time] {
        let ledger = genesis_ledger(&fluree, id).await;
        fluree.insert(ledger, &family()).await.expect("seed");
        fluree
            .reindex(id, ReindexOptions::default())
            .await
            .expect("reindex");
    }
    let mut twins = Twins {
        fluree,
        served,
        query_time,
        indexes: Vec::new(),
    };
    for program in programs {
        let index = twins
            .fluree
            .register_reasoning_index(&twins.served, &json!({"reasoning": program}))
            .await
            .expect("register");
        twins.refresh(&index).await;
        twins.indexes.push(index);
    }
    twins
}

impl Twins {
    async fn refresh(&self, index: &ReasoningIndexId) {
        published(
            self.fluree
                .refresh_reasoning_index(&self.served, index)
                .await
                .expect("refresh"),
        );
    }

    async fn view(&self, ledger: &str, policy: &Policy) -> GraphDb {
        let state = self.fluree.ledger(ledger).await.expect("ledger");
        self.fluree
            .wrap_policy(GraphDb::from_ledger_state(&state), &policy.governance())
            .await
            .expect("policy view")
    }

    async fn run(
        &self,
        ledger: &str,
        query: &Value,
        policy: &Policy,
        entry: Entry,
    ) -> Result<Answer, String> {
        let response = match entry {
            Entry::View => {
                let view = self.view(ledger, policy).await;
                view.query(&self.fluree)
                    .jsonld(query)
                    .execute_tracked()
                    .await
                    .map_err(|e| format!("{e:?}"))?
            }
            Entry::Connection => {
                let mut query = with(query.clone(), "from", json!(ledger));
                query["opts"] = json!({
                    "policy": policy.rules,
                    "default-allow": policy.default_allow
                });
                self.fluree
                    .query_connection_tracked(&query)
                    .await
                    .map_err(|e| format!("{e:?}"))?
            }
        };
        Ok((
            normalize_rows(&response.result),
            response.reasoning.and_then(|r| r.index).map(|h| *h),
        ))
    }

    /// `query` under `policy` answers `expected` at query time and from the
    /// index, through each entry.
    async fn check(&self, what: &str, query: &Value, policy: &Policy, expected: Value) {
        let expected = normalize_rows(&expected);
        for entry in ENTRIES {
            let (rows, index) = self
                .run(&self.query_time, query, policy, entry)
                .await
                .unwrap_or_else(|e| panic!("{what}, query time, {entry:?}: {e}"));
            assert_eq!(rows, expected, "{what}, query time, {entry:?}");
            assert!(index.is_none(), "{what}, query time, {entry:?}");

            let (rows, index) = self
                .run(&self.served, &index_only(query), policy, entry)
                .await
                .unwrap_or_else(|e| panic!("{what}, served, {entry:?}: {e}"));
            assert_eq!(rows, expected, "{what}, served, {entry:?}");
            assert!(index.is_some(), "{what}, served, {entry:?}");
        }
    }

    /// `query` under `policy` on the indexed ledger, served or not.
    async fn answer(&self, query: &Value, policy: &Policy) -> Answer {
        self.run(&self.served, query, policy, Entry::View)
            .await
            .expect("query")
    }
}

fn names() -> Value {
    json!({"@id": "?s", "ex:name": "?n"})
}

fn all_names_but(hidden: &str) -> Value {
    let all = [
        ("ex:alice", "Alice"),
        ("ex:bob", "Bob"),
        ("ex:carol", "Carol"),
        ("ex:dave", "Dave"),
        ("ex:erin", "Erin"),
    ];
    Value::Array(
        all.iter()
            .filter(|(s, _)| *s != hidden)
            .map(|(s, n)| json!([s, n]))
            .collect(),
    )
}

#[tokio::test]
async fn rule_conclusions_are_judged_by_their_own_facts() {
    let twins = twins("rules", &["datalog"]).await;
    let ancestors = query(
        json!(["?x", "?y"]),
        json!({"@id": "?x", "ex:ancestor": "?y"}),
        "datalog",
    );
    twins
        .check(
            "a hidden premise leaves its conclusion visible",
            &query(
                json!("?s"),
                json!({"@id": "?s", "@type": "ex:HighEarner"}),
                "datalog",
            ),
            &deny_property("salary"),
            json!(["ex:alice"]),
        )
        .await;
    twins
        .check(
            "a denied derived predicate",
            &ancestors,
            &deny_property("ancestor"),
            json!([]),
        )
        .await;
    twins
        .check(
            "a derived class member's stored facts",
            &query(json!(["?s", "?n"]), names(), "datalog"),
            &deny_class("HighEarner"),
            all_names_but("ex:alice"),
        )
        .await;
    twins
        .check(
            "a stored class member's derived facts",
            &ancestors,
            &deny_class("Retiree"),
            json!([
                ["ex:alice", "ex:bob"],
                ["ex:alice", "ex:carol"],
                ["ex:alice", "ex:dave"],
                ["ex:bob", "ex:carol"],
                ["ex:bob", "ex:dave"]
            ]),
        )
        .await;
}

#[tokio::test]
async fn an_owl_rl_class_is_judged_as_reasoned() {
    let twins = twins("owl", &["owl2rl"]).await;
    twins
        .check(
            "a class a subclass axiom derives",
            &query(json!(["?s", "?n"]), names(), "owl2rl"),
            &deny_class("Employee"),
            all_names_but("ex:erin"),
        )
        .await;
}

#[tokio::test]
async fn policies_read_the_reasoned_state() {
    let twins = twins("evaluation", &["datalog"]).await;
    twins
        .check(
            "default deny",
            &query(
                json!(["?p", "?o"]),
                json!({"@id": "ex:alice", "?p": "?o"}),
                "datalog",
            ),
            &only_names(),
            json!([["ex:name", "Alice"]]),
        )
        .await;
    twins
        .check(
            "a subject restriction",
            &query(
                json!(["?x", "?y"]),
                json!({"@id": "?x", "ex:ancestor": "?y"}),
                "datalog",
            ),
            &deny_subject("carol"),
            json!([
                ["ex:alice", "ex:bob"],
                ["ex:alice", "ex:carol"],
                ["ex:alice", "ex:dave"],
                ["ex:bob", "ex:carol"],
                ["ex:bob", "ex:dave"]
            ]),
        )
        .await;

    // A condition on a derived class reads the derived facts in a reasoning
    // query, and the stored ones in a plain query.
    let salaries = |reasoning| {
        query(
            json!(["?s", "?v"]),
            json!({"@id": "?s", "ex:salary": "?v"}),
            reasoning,
        )
    };
    twins
        .check(
            "a condition on a derived class",
            &salaries("datalog"),
            &salaries_of_high_earners(),
            json!([["ex:alice", 250_000]]),
        )
        .await;
    let (plain, index) = twins
        .answer(&salaries("none"), &salaries_of_high_earners())
        .await;
    assert_eq!(plain, Vec::<Value>::new());
    assert!(index.is_none());
}

#[tokio::test]
async fn operators_cannot_reach_past_a_reasoned_deny() {
    let twins = twins("operators", &["datalog"]).await;
    let high_earners = deny_class("HighEarner");
    twins
        .check(
            "count",
            &query(
                json!(["(count ?s)"]),
                json!({"@id": "?s", "ex:salary": "?v"}),
                "datalog",
            ),
            &high_earners,
            json!([[1]]),
        )
        .await;
    twins
        .check(
            "join",
            &query(
                json!(["?s", "?n", "?v"]),
                json!([
                    {"@id": "?s", "ex:name": "?n"},
                    {"@id": "?s", "ex:salary": "?v"}
                ]),
                "datalog",
            ),
            &high_earners,
            json!([["ex:bob", "Bob", 90000]]),
        )
        .await;
    let mut path = query(
        json!(["?y"]),
        json!({"@id": "ex:bob", "parentPlus": "?y"}),
        "datalog",
    );
    path["@context"]["parentPlus"] = json!({"@path": "ex:parent+"});
    twins
        .check("path", &path, &deny_class("Retiree"), json!([["ex:carol"]]))
        .await;
    twins
        .check(
            "a predicate the policy does not name",
            &query(json!(["?s", "?n"]), names(), "datalog"),
            &deny_property("salary"),
            all_names_but("none"),
        )
        .await;

    // SPARQL reads the same served state.
    let sparql = |index_only: bool| {
        let pin = if index_only {
            "# PRAGMA reasoning-max-facts: 1\n# PRAGMA reasoning-freshness: exact\n"
        } else {
            ""
        };
        format!(
            "# PRAGMA reasoning: datalog\n{pin}PREFIX ex: <{EX}>\n\
             SELECT ?s ?n ?v WHERE {{ ?s ex:name ?n ; ex:salary ?v }}"
        )
    };
    for (ledger, index_only) in [(&twins.query_time, false), (&twins.served, true)] {
        let view = twins.view(ledger, &high_earners).await;
        let response = view
            .query(&twins.fluree)
            .sparql(&sparql(index_only))
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("sparql, served: {index_only}: {e:?}"));
        let served = response.reasoning.and_then(|r| r.index).is_some();
        assert_eq!(served, index_only);
        let bindings = response.result["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .clone();
        assert_eq!(bindings.len(), 1, "served: {index_only}: {bindings:?}");
        assert_eq!(bindings[0]["n"]["value"], json!("Bob"));
    }
}

#[tokio::test]
async fn a_dataset_member_keeps_its_own_policy_when_served() {
    let twins = twins("dataset", &["datalog"]).await;
    let state = twins.fluree.ledger(&twins.served).await.expect("ledger");
    let root = GraphDb::from_ledger_state(&state);
    let hiding_alice = twins.view(&twins.served, &deny_class("HighEarner")).await;
    let hiding_salaries = twins.view(&twins.served, &deny_property("salary")).await;
    let dataset = DataSetDb::new()
        .with_default(root.clone())
        .with_named("urn:root", root)
        .with_named("urn:hiding-alice", hiding_alice)
        .with_named("urn:hiding-salaries", hiding_salaries);
    let salaries = |graph: &str| {
        index_only(&query(
            json!(["?s", "?v"]),
            json!([["graph", graph, {"@id": "?s", "ex:salary": "?v"}]]),
            "datalog",
        ))
    };
    for (graph, expected) in [
        (
            "urn:root",
            json!([["ex:alice", 250_000], ["ex:bob", 90000]]),
        ),
        ("urn:hiding-alice", json!([["ex:bob", 90000]])),
        ("urn:hiding-salaries", json!([])),
    ] {
        let response = dataset
            .query(&twins.fluree)
            .jsonld(&salaries(graph))
            .execute_tracked()
            .await
            .unwrap_or_else(|e| panic!("{graph}: {e:?}"));
        assert_eq!(
            normalize_rows(&response.result),
            normalize_rows(&expected),
            "{graph}"
        );
        assert!(
            response.reasoning.and_then(|r| r.index).is_some(),
            "{graph}"
        );
    }
}

#[tokio::test]
async fn a_restricted_stream_is_served_under_its_policy() {
    let twins = twins("stream", &["datalog"]).await;
    let view = twins.view(&twins.served, &deny_class("HighEarner")).await;
    let plan = twins
        .fluree
        .plan_stream_query(
            &view,
            &OwnedStreamQuery::JsonLd(index_only(&query(json!(["?s", "?n"]), names(), "datalog"))),
        )
        .await
        .expect("plan");
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    twins
        .fluree
        .run_stream_query(
            plan,
            Tracker::disabled(),
            QueryExecutionOptions::default(),
            tx,
        )
        .await;
    let mut text = String::new();
    while let Some(chunk) = rx.recv().await {
        text.push_str(std::str::from_utf8(&chunk).expect("utf-8"));
    }
    let names: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("ndjson"))
        .filter(|record| record["type"] == "row")
        .map(|record| record["row"]["n"]["value"].clone())
        .collect();
    assert!(text.contains("\"end\""), "{text}");
    assert_eq!(
        normalize_rows(&Value::Array(names)),
        normalize_rows(&json!(["Bob", "Carol", "Dave", "Erin"])),
        "{text}"
    );
}

/// The policy context a wrapped view shares between its queries keeps the
/// classes it resolved for the base index apart from those it resolved for a
/// generation at the same `t`: alice is a stored `Person` in both, and a
/// `HighEarner` only in the generation. The deny on `Retiree`, a class with a
/// stored named member, makes a plain query resolve every named subject's
/// classes too. Read through an instance that loads both from storage, the
/// two states differ only in their index roots.
#[tokio::test]
async fn one_view_keeps_plain_and_served_class_lookups_apart() {
    let (writer, reader) = crate::support::instances_sharing_storage();
    let mut twins = seeded(writer, "isolation", &["datalog"]).await;
    twins.fluree = reader;
    let names_with = |reasoning| query(json!(["?s", "?n"]), names(), reasoning);
    let policy = deny_classes(&["HighEarner", "Retiree"]);
    let plain_names = normalize_rows(&json!([
        ["ex:alice", "Alice"],
        ["ex:bob", "Bob"],
        ["ex:dave", "Dave"],
        ["ex:erin", "Erin"]
    ]));
    let served_names = normalize_rows(&json!([
        ["ex:bob", "Bob"],
        ["ex:dave", "Dave"],
        ["ex:erin", "Erin"]
    ]));
    for order in ["plain first", "served first"] {
        let view = twins.view(&twins.served, &policy).await;
        let run = |q: Value| {
            let (view, fluree) = (&view, &twins.fluree);
            async move {
                let response = view
                    .query(fluree)
                    .jsonld(&q)
                    .execute_tracked()
                    .await
                    .expect("query");
                (
                    normalize_rows(&response.result),
                    response.reasoning.and_then(|r| r.index).is_some(),
                )
            }
        };
        let plain = || run(names_with("none"));
        let served = || run(index_only(&names_with("datalog")));
        let (first, second) = match order {
            "plain first" => (plain().await, served().await),
            _ => {
                let s = served().await;
                (plain().await, s)
            }
        };
        assert_eq!(first.0, plain_names, "{order}");
        assert!(!first.1, "{order}");
        assert_eq!(second.0, served_names, "{order}");
        assert!(second.1, "{order}");
    }
}

/// A root caller and restricted callers with opposite policies share one
/// instance's caches, in either order and concurrently, and none narrows or
/// widens another's answer.
#[tokio::test]
async fn callers_do_not_share_their_answers() {
    let twins = twins("callers", &["datalog"]).await;
    let salaries = index_only(&query(
        json!(["?s", "?v"]),
        json!({"@id": "?s", "ex:salary": "?v"}),
        "datalog",
    ));
    let everyone = normalize_rows(&json!([["ex:alice", 250_000], ["ex:bob", 90000]]));
    let only_bob = normalize_rows(&json!([["ex:bob", 90000]]));
    let only_alice = normalize_rows(&json!([["ex:alice", 250_000]]));
    let root = Policy {
        rules: json!([]),
        default_allow: true,
    };

    let (hiding_alice, high_earner_salaries) =
        (deny_class("HighEarner"), salaries_of_high_earners());
    let (a, b, c) = tokio::join!(
        twins.answer(&salaries, &hiding_alice),
        twins.answer(&salaries, &high_earner_salaries),
        twins.answer(&salaries, &root),
    );
    assert_eq!(a.0, only_bob);
    assert_eq!(b.0, only_alice);
    assert_eq!(c.0, everyone);
    assert!(a.1.is_some() && b.1.is_some() && c.1.is_some());

    for policy in [&root, &deny_class("HighEarner"), &root] {
        let expected = if policy.rules == json!([]) {
            &everyone
        } else {
            &only_bob
        };
        assert_eq!(&twins.answer(&salaries, policy).await.0, expected);
    }
}

/// Two programs at one `t` each serve their own reasoned classes.
#[tokio::test]
async fn two_programs_at_one_commit_keep_their_classes() {
    let twins = twins("programs", &["datalog", "owl2rl"]).await;
    let checks = [
        ("datalog", deny_class("HighEarner"), "ex:alice"),
        ("owl2rl", deny_class("Employee"), "ex:erin"),
    ];
    for (reasoning, policy, hidden) in checks {
        let (rows, index) = twins
            .answer(
                &index_only(&query(json!(["?s", "?n"]), names(), reasoning)),
                &policy,
            )
            .await;
        assert_eq!(rows, normalize_rows(&all_names_but(hidden)), "{reasoning}");
        assert!(index.is_some(), "{reasoning}");
    }
}

/// A restricted caller is served only at its own basis. A commit that makes
/// bob a `HighEarner` leaves the generation behind: `default` reasons at
/// query time, `asOf` does too rather than read the older generation, and
/// `strict` fails until the next generation is published.
#[tokio::test]
async fn freshness_serves_a_restricted_caller_only_at_its_basis() {
    let twins = twins("freshness", &["datalog"]).await;
    twins
        .fluree
        .set_reasoning_index_strict_wait(std::time::Duration::from_millis(100));
    let policy = deny_class("HighEarner");
    let names = query(json!(["?s", "?n"]), names(), "datalog");
    let freshness = |mode: &str| with(names.clone(), "reasoningFreshness", json!(mode));

    for mode in ["default", "exact", "strict", "asOf"] {
        let (rows, index) = twins.answer(&freshness(mode), &policy).await;
        assert_eq!(rows, normalize_rows(&all_names_but("ex:alice")), "{mode}");
        assert!(index.is_some(), "{mode}");
    }

    let ledger = twins.fluree.ledger(&twins.served).await.expect("ledger");
    twins
        .fluree
        .update(
            ledger,
            &json!({
                "@context": ctx(),
                "delete": {"@id": "ex:bob", "ex:salary": 90000},
                "insert": {"@id": "ex:bob", "ex:salary": 300_000}
            }),
        )
        .await
        .expect("raise bob's salary");
    let neither = normalize_rows(&json!([
        ["ex:carol", "Carol"],
        ["ex:dave", "Dave"],
        ["ex:erin", "Erin"]
    ]));
    for mode in ["default", "asOf"] {
        let (rows, index) = twins.answer(&freshness(mode), &policy).await;
        assert_eq!(rows, neither, "{mode}");
        assert!(index.is_none(), "{mode}");
    }
    let root = Policy {
        rules: json!([]),
        default_allow: true,
    };
    let head = twins_head(&twins).await;
    let (_, index) = twins.answer(&freshness("asOf"), &root).await;
    assert!(
        index.is_some_and(|tally| tally.basis_t < head),
        "a root caller still reads the older generation"
    );
    let strict = twins
        .run(&twins.served, &freshness("strict"), &policy, Entry::View)
        .await
        .expect_err("no generation at the new commit");
    assert!(strict.contains("strict"), "{strict}");

    twins.refresh(&twins.indexes[0]).await;
    let (rows, index) = twins.answer(&freshness("strict"), &policy).await;
    assert_eq!(rows, neither);
    assert!(index.is_some());
}

async fn twins_head(twins: &Twins) -> i64 {
    twins
        .fluree
        .ledger(&twins.served)
        .await
        .expect("ledger")
        .t()
}

/// A restricted caller's own rules are stripped before an index is chosen:
/// a query bringing a rule is served by the stored-rule program and never
/// sees the rule's conclusions.
#[tokio::test]
async fn a_restricted_callers_rules_never_reach_an_index() {
    let twins = twins("inline-rules", &["datalog"]).await;
    let rule = json!({
        "@context": {"ex": EX},
        "where": {"@id": "?x", "ex:name": "?n"},
        "insert": {"@id": "?x", "ex:named": true}
    });
    let q = with(
        query(
            json!(["?s"]),
            json!({"@id": "?s", "ex:named": true}),
            "datalog",
        ),
        "rules",
        json!([rule]),
    );
    let (rows, index) = twins
        .answer(&index_only(&q), &deny_property("salary"))
        .await;
    assert_eq!(rows, Vec::<Value>::new());
    assert!(index.is_some());
}

/// A program no index materializes falls back to query time.
#[tokio::test]
async fn an_unmatched_program_falls_back() {
    let twins = twins("unmatched", &["datalog"]).await;
    let (rows, index) = twins
        .answer(
            &query(json!(["?s", "?n"]), names(), "owl2rl"),
            &deny_class("Employee"),
        )
        .await;
    assert_eq!(rows, normalize_rows(&all_names_but("ex:erin")));
    assert!(index.is_none());
}

/// A graph crawl in a reasoning query under a view policy is refused before
/// an index is consulted.
#[tokio::test]
async fn a_crawl_is_refused_with_an_index_too() {
    let twins = twins("crawl", &["datalog"]).await;
    let crawl = json!({
        "@context": ctx(),
        "select": {"ex:alice": ["*"]},
        "reasoning": "datalog"
    });
    let error = twins
        .run(
            &twins.served,
            &crawl,
            &deny_class("HighEarner"),
            Entry::View,
        )
        .await
        .expect_err("refused");
    assert!(error.contains("graph crawl"), "{error}");
}
