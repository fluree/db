//! A query's `rulesFrom`: reason with the stored rules of a graph of some
//! ledger, at a pin, in place of the queried ledger's configured ones.
//!
//! The rules ledger R holds `ex:r1` (`@json`: a score over 10 makes an
//! `ex:Candidate`) and `ex:r2` (`f:sparql`: anything scored is an
//! `ex:SparqlCandidate`) in `ex:rules` at its first commit, and later gains
//! `ex:r3` (a score over 1 makes an `ex:Candidate`). The data ledger D scores
//! alice 5 and bob 50; it stores its own rule `ex:own` (every account is an
//! `ex:OwnDerived`) and its config reads `f:rulesSource` from another
//! ledger, M, whose `ex:configured` makes every account an
//! `ex:ConfiguredDerived`. So which classes come back says which rules ran.

#![cfg(feature = "native")]

use crate::support::{genesis_ledger, normalize_rows, normalize_sparql_bindings};
use fluree_db_api::{
    CommitReceipt, Fluree, FlureeBuilder, QueryExecutionOptions, ReasoningTally, RulesSourceTally,
    TrackedErrorResponse, VerifiedIdentity,
};
use serde_json::{json, Value};

const RULES_GRAPH: &str = "http://example.org/rules";
const CONFIGURED_GRAPH: &str = "http://example.org/configured-rules";
const EX: &str = "http://example.org/";

fn config_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

fn rule_r1() -> Value {
    json!({"@id": "ex:r1", "f:rule": {"@type": "@json", "@value": {
        "@context": {"ex": EX},
        "where": [{"@id": "?a", "ex:score": "?s"}, ["filter", "(> ?s 10)"]],
        "insert": {"@id": "?a", "@type": "ex:Candidate"}
    }}})
}

fn rule_r2() -> Value {
    json!({"@id": "ex:r2", "f:rule": {"@type": "f:sparql", "@value":
        "PREFIX ex: <http://example.org/> \
         CONSTRUCT { ?a a ex:SparqlCandidate } WHERE { ?a ex:score ?s }"}})
}

fn rule_r3() -> Value {
    json!({"@id": "ex:r3", "f:rule": {"@type": "@json", "@value": {
        "@context": {"ex": EX},
        "where": [{"@id": "?a", "ex:score": "?s"}, ["filter", "(> ?s 1)"]],
        "insert": {"@id": "?a", "@type": "ex:Candidate"}
    }}})
}

/// A rule making every account an instance of `class`.
fn account_rule(id: &str, class: &str) -> Value {
    json!({"@id": id, "f:rule": {"@type": "@json", "@value": {
        "@context": {"ex": EX},
        "where": {"@id": "?a", "@type": "ex:Account"},
        "insert": {"@id": "?a", "@type": class}
    }}})
}

/// Add `rules` to `graph` of `ledger_id` (created when `create`).
async fn store_rules(
    fluree: &Fluree,
    ledger_id: &str,
    graph: &str,
    rules: Vec<Value>,
    create: bool,
) -> CommitReceipt {
    let ledger = if create {
        genesis_ledger(fluree, ledger_id)
    } else {
        fluree.ledger(ledger_id).await.expect("load ledger")
    };
    let insert: Vec<Value> = rules
        .into_iter()
        .map(|rule| json!(["graph", graph, rule]))
        .collect();
    fluree
        .update(
            ledger,
            &json!({
                "@context": {"ex": EX, "f": "https://ns.flur.ee/db#"},
                "insert": insert
            }),
        )
        .await
        .expect("store rules")
        .receipt
}

/// D: alice (5) and bob (50), both accounts; its own `ex:own` rule in its
/// default graph; `extra_config` in its config graph (a rules source).
async fn data_ledger(fluree: &Fluree, data_id: &str, extra_config: &str) {
    let data = genesis_ledger(fluree, data_id);
    let data = fluree
        .insert(
            data,
            &json!({"@context": {"ex": EX, "f": "https://ns.flur.ee/db#"}, "@graph": [
                {"@id": "ex:alice", "@type": "ex:Account", "ex:score": 5},
                {"@id": "ex:bob", "@type": "ex:Account", "ex:score": 50},
                account_rule("ex:own", "ex:OwnDerived")
            ]}),
        )
        .await
        .expect("seed D")
        .ledger;
    let cfg = config_iri(data_id);
    fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            GRAPH <{cfg}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                {extra_config}
            }}"
        ))
        .execute()
        .await
        .expect("seed D config");
}

/// D's config reads its rules from `model_id`'s configured graph.
fn cross_ledger_rules_source(model_id: &str) -> String {
    format!(
        r"
        <urn:cfg:main>      f:datalogDefaults <urn:cfg:datalog> .
        <urn:cfg:datalog>   f:datalogEnabled  true ;
                            f:rulesSource     <urn:cfg:rules-ref> .
        <urn:cfg:rules-ref> rdf:type          f:GraphRef ;
                            f:graphSource     <urn:cfg:rules-src> .
        <urn:cfg:rules-src> f:ledger          <{model_id}> ;
                            f:graphSelector   <{CONFIGURED_GRAPH}> ."
    )
}

/// The ledgers of the module docs: R (with `ex:r1` and `ex:r2` at its first
/// commit, whose receipt this returns), M, and D reading M's rules.
async fn ledgers(fluree: &Fluree, rules_id: &str, model_id: &str, data_id: &str) -> CommitReceipt {
    let pinned = store_rules(
        fluree,
        rules_id,
        RULES_GRAPH,
        vec![rule_r1(), rule_r2()],
        true,
    )
    .await;
    store_rules(
        fluree,
        model_id,
        CONFIGURED_GRAPH,
        vec![account_rule("ex:configured", "ex:ConfiguredDerived")],
        true,
    )
    .await;
    data_ledger(fluree, data_id, &cross_ledger_rules_source(model_id)).await;
    pinned
}

/// The `(subject, class)` rows a tracked query over D returns, with the
/// tracked `reasoning` block. `extra` is merged into the query at the top
/// level.
async fn classes(
    fluree: &Fluree,
    data_id: &str,
    extra: Value,
) -> Result<(Vec<Value>, ReasoningTally), TrackedErrorResponse> {
    let mut query = json!({
        "@context": {"ex": EX},
        "select": ["?a", "?c"],
        "where": {"@id": "?a", "@type": "?c"},
        "opts": {"meta": true}
    });
    for (key, value) in extra.as_object().expect("an object") {
        query[key] = value.clone();
    }
    let view = fluree.db(data_id).await.expect("load D");
    let response = view.query(fluree).jsonld(&query).execute_tracked().await?;
    let rows = normalize_rows(&response.result);
    let tally = response
        .reasoning
        .expect("a reasoning query reports its reasoning");
    Ok((rows, tally))
}

fn has(rows: &[Value], subject: &str, class: &str) -> bool {
    rows.contains(&json!([subject, class]))
}

fn rule_ids(tally: &ReasoningTally) -> Vec<&str> {
    tally.rules.iter().map(|r| r.id.as_str()).collect()
}

/// R's rules as of its first commit ran, and nothing else of the stored kind.
#[track_caller]
fn assert_ran_pinned_rules(rows: &[Value], what: &str) {
    assert!(has(rows, "ex:bob", "ex:Candidate"), "{what}: {rows:?}");
    assert!(
        has(rows, "ex:alice", "ex:SparqlCandidate"),
        "{what}: {rows:?}"
    );
    assert!(
        has(rows, "ex:bob", "ex:SparqlCandidate"),
        "{what}: {rows:?}"
    );
    assert!(
        !has(rows, "ex:alice", "ex:Candidate"),
        "ex:r3 came after the pin: {what}: {rows:?}"
    );
    assert!(
        !has(rows, "ex:alice", "ex:OwnDerived") && !has(rows, "ex:alice", "ex:ConfiguredDerived"),
        "rulesFrom replaces D's own and configured rules: {what}: {rows:?}"
    );
}

/// Pinned by commit or by `t`, `rulesFrom` reads R's rules as they were at
/// the pin: a rule added to R after it does not run, and the tally names R,
/// its graph, the pinned `t` and the pinned commit. Unpinned, it reads R's
/// head. Either way D's own and configured rules do not run.
#[tokio::test]
async fn rules_from_reads_the_rules_at_its_pin() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/pin-rules:main",
        "test/rules-from/pin-model:main",
        "test/rules-from/pin-data:main",
    );
    let pinned = ledgers(&fluree, rules_id, model_id, data_id).await;
    let head = store_rules(&fluree, rules_id, RULES_GRAPH, vec![rule_r3()], false).await;
    assert!(head.t > pinned.t);

    let pinned_source = RulesSourceTally {
        ledger: rules_id.to_string(),
        graph: RULES_GRAPH.to_string(),
        t: pinned.t,
        commit_id: Some(pinned.commit_id.to_string()),
    };
    for (rules_from, what) in [
        (
            format!("{rules_id}@commit:{}#{RULES_GRAPH}", pinned.commit_id),
            "pinned by commit",
        ),
        (
            format!("{rules_id}@t:{}#{RULES_GRAPH}", pinned.t),
            "pinned by t",
        ),
    ] {
        let (rows, tally) = classes(&fluree, data_id, json!({"rulesFrom": rules_from}))
            .await
            .unwrap_or_else(|e| panic!("{what}: {e:?}"));
        assert_ran_pinned_rules(&rows, what);
        assert_eq!(tally.rules_source.as_ref(), Some(&pinned_source), "{what}");
        assert_eq!(
            rule_ids(&tally),
            [format!("{EX}r1"), format!("{EX}r2")],
            "{what}"
        );
    }

    // At the head, ex:r3 runs too, and the tally names the head's commit.
    let (rows, tally) = classes(
        &fluree,
        data_id,
        json!({"rulesFrom": format!("{rules_id}#{RULES_GRAPH}")}),
    )
    .await
    .expect("head");
    assert!(has(&rows, "ex:alice", "ex:Candidate"), "{rows:?}");
    assert!(!has(&rows, "ex:alice", "ex:OwnDerived"), "{rows:?}");
    assert_eq!(
        tally.rules_source,
        Some(RulesSourceTally {
            t: head.t,
            commit_id: Some(head.commit_id.to_string()),
            ..pinned_source
        })
    );
    assert_eq!(
        rule_ids(&tally),
        [format!("{EX}r1"), format!("{EX}r2"), format!("{EX}r3")]
    );
}

/// The same ledger queried with and without `rulesFrom`, back to back and
/// again with the reasoning cache warm: each query gets its own rules' facts
/// and rule set. A `rulesFrom` that left the configured rules in what the
/// reasoning settles to, or a cache key that did not tell the two apart,
/// would serve one query the other's answer.
#[tokio::test]
async fn rules_from_and_the_configured_rules_do_not_share_an_answer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/cache-rules:main",
        "test/rules-from/cache-model:main",
        "test/rules-from/cache-data:main",
    );
    let pinned = ledgers(&fluree, rules_id, model_id, data_id).await;
    let rules_from = json!({
        "rulesFrom": format!("{rules_id}@commit:{}#{RULES_GRAPH}", pinned.commit_id)
    });

    let mut rule_sets = Vec::new();
    for round in ["cold", "warm"] {
        let (rows, configured) = classes(&fluree, data_id, json!({"reasoning": "datalog"}))
            .await
            .expect("configured rules");
        assert!(has(&rows, "ex:alice", "ex:OwnDerived"), "{round}: {rows:?}");
        assert!(
            has(&rows, "ex:alice", "ex:ConfiguredDerived"),
            "{round}: {rows:?}"
        );
        assert!(!has(&rows, "ex:bob", "ex:Candidate"), "{round}: {rows:?}");
        assert_eq!(
            configured.rules_source.as_ref().map(|s| s.ledger.as_str()),
            Some(model_id),
            "{round}"
        );

        let (rows, named) = classes(&fluree, data_id, rules_from.clone())
            .await
            .expect("rulesFrom");
        assert_ran_pinned_rules(&rows, round);
        assert_eq!(
            named.rules_source.as_ref().map(|s| s.ledger.as_str()),
            Some(rules_id),
            "{round}"
        );
        assert_ne!(configured.rule_set, named.rule_set, "{round}");
        rule_sets.push((configured.rule_set, named.rule_set));
    }
    assert_eq!(
        rule_sets[0], rule_sets[1],
        "a warm cache answers as a cold one"
    );
}

/// A local `f:rulesSource` graph is replaced too: its rules do not run.
#[tokio::test]
async fn rules_from_replaces_a_local_rules_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rules_id = "test/rules-from/local-rules:main";
    let data_id = "test/rules-from/local-data:main";
    let local_graph = "http://example.org/local-rules";
    store_rules(
        &fluree,
        rules_id,
        RULES_GRAPH,
        vec![rule_r1(), rule_r2()],
        true,
    )
    .await;
    data_ledger(
        &fluree,
        data_id,
        &format!(
            r"
            <urn:cfg:main>      f:datalogDefaults <urn:cfg:datalog> .
            <urn:cfg:datalog>   f:rulesSource     <urn:cfg:rules-ref> .
            <urn:cfg:rules-ref> rdf:type          f:GraphRef ;
                                f:graphSource     <urn:cfg:rules-src> .
            <urn:cfg:rules-src> f:graphSelector   <{local_graph}> ."
        ),
    )
    .await;
    store_rules(
        &fluree,
        data_id,
        local_graph,
        vec![account_rule("ex:local", "ex:LocalDerived")],
        false,
    )
    .await;

    let (rows, _) = classes(&fluree, data_id, json!({"reasoning": "datalog"}))
        .await
        .expect("configured");
    assert!(has(&rows, "ex:alice", "ex:LocalDerived"), "{rows:?}");

    let (rows, tally) = classes(
        &fluree,
        data_id,
        json!({"rulesFrom": format!("{rules_id}#{RULES_GRAPH}")}),
    )
    .await
    .expect("rulesFrom");
    assert!(!has(&rows, "ex:alice", "ex:LocalDerived"), "{rows:?}");
    assert!(has(&rows, "ex:bob", "ex:Candidate"), "{rows:?}");
    assert_eq!(rule_ids(&tally), [format!("{EX}r1"), format!("{EX}r2")]);
}

/// Query-time `rules` still run beside the rules `rulesFrom` names, and
/// `rulesFrom` is read from `opts` as the other reasoning keys are.
#[tokio::test]
async fn query_time_rules_run_beside_rules_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/beside-rules:main",
        "test/rules-from/beside-model:main",
        "test/rules-from/beside-data:main",
    );
    ledgers(&fluree, rules_id, model_id, data_id).await;
    let (rows, tally) = classes(
        &fluree,
        data_id,
        json!({
            "opts": {"meta": true, "rulesFrom": format!("{rules_id}#{RULES_GRAPH}")},
            "rules": [{
                "@context": {"ex": EX},
                "where": {"@id": "?a", "@type": "ex:Account"},
                "insert": {"@id": "?a", "@type": "ex:QueryDerived"}
            }]
        }),
    )
    .await
    .expect("rules and rulesFrom");
    assert!(has(&rows, "ex:alice", "ex:QueryDerived"), "{rows:?}");
    assert!(has(&rows, "ex:bob", "ex:Candidate"), "{rows:?}");
    assert!(!has(&rows, "ex:alice", "ex:OwnDerived"), "{rows:?}");
    assert_eq!(tally.rules.len(), 3, "{:?}", tally.rules);
    assert_eq!(
        tally.rules_source.map(|s| s.ledger),
        Some(rules_id.to_string())
    );
}

/// SPARQL twin: `# PRAGMA rules-from:` reads the same rules at the same pin.
#[tokio::test]
async fn rules_from_pragma_reads_the_rules_at_its_pin() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/sparql-rules:main",
        "test/rules-from/sparql-model:main",
        "test/rules-from/sparql-data:main",
    );
    let pinned = ledgers(&fluree, rules_id, model_id, data_id).await;
    store_rules(&fluree, rules_id, RULES_GRAPH, vec![rule_r3()], false).await;

    let sparql = format!(
        "# PRAGMA rules-from: {rules_id}@commit:{}#{RULES_GRAPH}\n\
         # PRAGMA meta: true\n\
         PREFIX ex: <http://example.org/>\n\
         SELECT ?a ?c WHERE {{ ?a a ?c }}",
        pinned.commit_id
    );
    let view = fluree.db(data_id).await.expect("load D");
    let response = view
        .query(&fluree)
        .sparql(&sparql)
        .execute_tracked()
        .await
        .expect("SPARQL rulesFrom");
    let rows: Vec<Value> = normalize_sparql_bindings(&response.result)
        .iter()
        .map(|b| {
            let short = |v: &str| {
                let iri = b[v]["value"].as_str().expect("an IRI");
                json!(iri.replacen(EX, "ex:", 1))
            };
            json!([short("a"), short("c")])
        })
        .collect();
    assert_ran_pinned_rules(&rows, "SPARQL");
    let tally = response.reasoning.expect("reasoning reported");
    assert_eq!(
        tally.rules_source,
        Some(RulesSourceTally {
            ledger: rules_id.to_string(),
            graph: RULES_GRAPH.to_string(),
            t: pinned.t,
            commit_id: Some(pinned.commit_id.to_string()),
        })
    );
}

/// A rule in the named graph that does not parse fails the query naming it
/// and the ledger it came from; one of another datatype fails naming the
/// rule, as it does for a configured rules source.
#[tokio::test]
async fn an_invalid_rule_from_is_named_with_its_ledger() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/invalid-rules:main",
        "test/rules-from/invalid-model:main",
        "test/rules-from/invalid-data:main",
    );
    ledgers(&fluree, rules_id, model_id, data_id).await;
    let rules_from = json!({"rulesFrom": format!("{rules_id}#{RULES_GRAPH}")});
    store_rules(
        &fluree,
        rules_id,
        RULES_GRAPH,
        vec![
            json!({"@id": "ex:r5", "f:rule": {"@type": "@json", "@value": {
                "@context": {"ex": EX},
                "where": [{"@id": "?a", "ex:score": "?s"},
                          ["optional", {"@id": "?a", "ex:note": "?n"}]],
                "insert": {"@id": "?a", "@type": "ex:Noted"}
            }}}),
        ],
        false,
    )
    .await;
    let err = classes(&fluree, data_id, rules_from.clone())
        .await
        .expect_err("an OPTIONAL rule is refused");
    assert_eq!(err.status, 400, "{}", err.error);
    assert!(
        err.error
            .contains(&format!("datalog rule <{EX}r5> from ledger '{rules_id}'"))
            && err.error.contains("OPTIONAL"),
        "{}",
        err.error
    );
}

/// What `rulesFrom` names has to exist: a ledger missing from this instance
/// and a graph R does not hold fail as a configured cross-ledger source's do
/// (502), and a pin past R's head as a query's own pin does (400). An
/// address that does not parse, or names a system graph, is a 400.
#[tokio::test]
async fn what_rules_from_names_has_to_exist() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/missing-rules:main",
        "test/rules-from/missing-model:main",
        "test/rules-from/missing-data:main",
    );
    let pinned = ledgers(&fluree, rules_id, model_id, data_id).await;
    for (rules_from, status, needle, what) in [
        (
            format!("test/rules-from/nope:main#{RULES_GRAPH}"),
            502,
            "model ledger 'test/rules-from/nope:main' is not present".to_string(),
            "a ledger that is not here",
        ),
        (
            format!("{rules_id}#http://example.org/no-such-graph"),
            502,
            format!("graph 'http://example.org/no-such-graph' is not present in model ledger '{rules_id}'"),
            "a graph R does not hold",
        ),
        (
            format!("{rules_id}@t:{}#{RULES_GRAPH}", pinned.t + 5),
            400,
            format!(
                "Target t={} is beyond current head t={} for '{rules_id}'",
                pinned.t + 5,
                pinned.t
            ),
            "a pin past R's head",
        ),
        (
            format!("{rules_id}#config"),
            400,
            "holds no rules".to_string(),
            "a system graph",
        ),
        (
            format!("{rules_id}@nope:1#{RULES_GRAPH}"),
            400,
            "rulesFrom".to_string(),
            "an address that does not parse",
        ),
    ] {
        let err = classes(&fluree, data_id, json!({"rulesFrom": rules_from}))
            .await
            .expect_err(what);
        assert_eq!(err.status, status, "{what}: {}", err.error);
        assert!(err.error.contains(&needle), "{what}: {}", err.error);
    }
}

/// `rulesFrom` lets a caller choose the rules a run reasons with, so it is
/// admin-only exactly where query-time `rules` are: refused, naming it,
/// under a non-root view policy and where the ledger's config denies
/// query-time rules to the request. The allow-listed identity runs it.
#[tokio::test]
async fn rules_from_is_refused_where_query_time_rules_are() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rules_id = "test/rules-from/gate-rules:main";
    store_rules(
        &fluree,
        rules_id,
        RULES_GRAPH,
        vec![rule_r1(), rule_r2()],
        true,
    )
    .await;
    let rules_from = format!("{rules_id}#{RULES_GRAPH}");

    // Under a permissive but non-root view policy.
    let policy_id = "test/rules-from/gate-policy:main";
    data_ledger(&fluree, policy_id, "").await;
    let err = fluree
        .query_connection(&json!({
            "@context": {"ex": EX},
            "from": policy_id,
            "opts": {"policy": [{"f:action": "f:view", "f:allow": true}], "default-allow": true},
            "select": "?a",
            "where": {"@id": "?a", "@type": "ex:Candidate"},
            "rulesFrom": rules_from
        }))
        .await
        .expect_err("rulesFrom under a non-root view policy");
    assert_eq!(err.status_code(), 400, "{err}");
    assert!(
        err.to_string()
            .contains("rules named with rulesFrom need an unrestricted view"),
        "{err}"
    );

    // Where the config denies query-time rules to all but one identity.
    let denied_id = "test/rules-from/gate-config:main";
    data_ledger(
        &fluree,
        denied_id,
        r"
        <urn:cfg:main>     f:datalogDefaults    <urn:cfg:datalog> .
        <urn:cfg:datalog>  f:datalogEnabled     true ;
                           f:allowQueryTimeRules false ;
                           f:overrideControl    <urn:cfg:oc> .
        <urn:cfg:oc>       f:controlMode        f:IdentityRestricted ;
                           f:allowedIdentities  <did:key:admin> .",
    )
    .await;
    let query = json!({
        "@context": {"ex": EX},
        "select": "?a",
        "where": {"@id": "?a", "@type": "ex:Candidate"},
        "rulesFrom": rules_from
    });
    let candidates_as = |identity: Option<&'static str>| {
        let (fluree, query) = (&fluree, &query);
        async move {
            let state = fluree.ledger(denied_id).await.expect("load");
            let db = fluree_db_api::GraphDb::from_ledger_state(&state);
            let mut options = QueryExecutionOptions::new();
            if let Some(id) = identity {
                options = options.with_server_identity(VerifiedIdentity::new(id));
            }
            fluree
                .query_with_options(&db, query, options)
                .await
                .map(|r| r.to_jsonld(&state.snapshot).expect("to_jsonld"))
        }
    };
    assert_eq!(
        candidates_as(Some("did:key:admin")).await.expect("admin"),
        json!(["ex:bob"])
    );
    for identity in [Some("did:key:other"), None] {
        let err = candidates_as(identity).await.expect_err("denied");
        assert_eq!(err.status_code(), 400, "{identity:?}: {err}");
        assert!(
            err.to_string()
                .contains("does not allow rules named with rulesFrom for this request"),
            "{identity:?}: {err}"
        );
    }
}

/// Where the config sets the request's reasoning, naming other rules is
/// changing it, so `rulesFrom` is refused as query-time `rules` are; where
/// it turns datalog off, `rulesFrom` (which turns datalog on) is refused
/// saying so. A history query refuses it as it refuses any reasoning.
#[tokio::test]
async fn rules_from_is_refused_where_the_config_or_the_query_rules_out_reasoning() {
    let fluree = FlureeBuilder::memory().build_memory();
    let rules_id = "test/rules-from/forced-rules:main";
    store_rules(&fluree, rules_id, RULES_GRAPH, vec![rule_r1()], true).await;
    let rules_from = format!("{rules_id}#{RULES_GRAPH}");

    for (data_id, config, needle) in [
        (
            "test/rules-from/forced-modes:main",
            r"
            <urn:cfg:main>   f:reasoningDefaults <urn:cfg:reason> .
            <urn:cfg:reason> f:reasoningModes    f:datalog ;
                             f:overrideControl   f:OverrideNone .",
            "so a query cannot name its own rules with rulesFrom",
        ),
        (
            "test/rules-from/datalog-off:main",
            r"
            <urn:cfg:main>    f:datalogDefaults <urn:cfg:datalog> .
            <urn:cfg:datalog> f:datalogEnabled  false ;
                              f:overrideControl f:OverrideNone .",
            "reasoning mode 'datalog' (rulesFrom turns it on)",
        ),
    ] {
        data_ledger(&fluree, data_id, config).await;
        let err = classes(&fluree, data_id, json!({"rulesFrom": rules_from}))
            .await
            .expect_err(needle);
        assert_eq!(err.status, 400, "{needle}: {}", err.error);
        assert!(err.error.contains(needle), "{needle}: {}", err.error);
    }

    let history_id = "test/rules-from/history:main";
    data_ledger(&fluree, history_id, "").await;
    let err = fluree
        .query_from()
        .jsonld(&json!({
            "@context": {"ex": EX},
            "from": format!("{history_id}@t:1"),
            "to": format!("{history_id}@t:latest"),
            "select": ["?v", "?t"],
            "where": [{"@id": "ex:alice", "ex:score": {"@value": "?v", "@t": "?t"}}],
            "rulesFrom": rules_from
        }))
        .execute_tracked()
        .await
        .expect_err("rulesFrom on a history query");
    assert_eq!(err.status, 400, "{}", err.error);
    assert!(
        err.error
            .contains("reasoning is not supported for history/changes queries"),
        "{}",
        err.error
    );
}

/// A write evaluates no reasoning, so a SPARQL update naming a rules source
/// is refused rather than ignored.
#[tokio::test]
async fn a_sparql_update_refuses_the_rules_from_pragma() {
    let fluree = FlureeBuilder::memory().build_memory();
    let data_id = "test/rules-from/update:main";
    data_ledger(&fluree, data_id, "").await;
    let t_before = fluree.ledger(data_id).await.unwrap().t();
    let err = fluree
        .graph(data_id)
        .transact()
        .sparql_update(
            "# PRAGMA rules-from: test/rules-from/any:main#http://example.org/rules\n\
             PREFIX ex: <http://example.org/>\n\
             INSERT { ?a ex:flagged true } WHERE { ?a a ex:Candidate }",
        )
        .commit()
        .await
        .expect_err("the rules-from pragma on an update");
    assert_eq!(err.status_code(), 400, "{err}");
    assert!(
        err.to_string()
            .contains("pragma `rules-from` applies to queries, not updates"),
        "{err}"
    );
    assert_eq!(fluree.ledger(data_id).await.unwrap().t(), t_before);
}

/// An explanation reads the reasoning keys a query does, `rulesFrom`
/// included: bob's candidacy is explained by R's `ex:r1`, and the rule set it
/// reports is the one the query ran.
#[tokio::test]
async fn an_explanation_reads_rules_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let (rules_id, model_id, data_id) = (
        "test/rules-from/explain-rules:main",
        "test/rules-from/explain-model:main",
        "test/rules-from/explain-data:main",
    );
    ledgers(&fluree, rules_id, model_id, data_id).await;
    let rules_from = format!("{rules_id}#{RULES_GRAPH}");
    let (_, tally) = classes(&fluree, data_id, json!({"rulesFrom": rules_from}))
        .await
        .expect("query");

    let view = fluree.db(data_id).await.expect("load D");
    let explanation = fluree
        .explain_derived(
            &view,
            &json!({
                "@context": {"ex": EX},
                "rulesFrom": rules_from,
                "explain": {"@id": "ex:bob", "@type": "ex:Candidate"}
            }),
        )
        .await
        .expect("explain");
    assert_eq!(Some(explanation.rule_set), tally.rule_set);
    let rules: Vec<&str> = explanation
        .facts
        .iter()
        .flat_map(|fact| fact.derivations.iter().map(|d| d.rule.as_str()))
        .collect();
    assert_eq!(rules, [format!("{EX}r1")], "{:?}", explanation.facts);
}
