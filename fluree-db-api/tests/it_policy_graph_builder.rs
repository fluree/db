//! #1766: the graph-scoped query builder ignores a query's policy.
//!
//! `fluree.graph(<ledger>).query()` loads its own view, and the execution methods
//! it hands that view to expect it to already carry any policy. Nothing on the
//! path wraps it. A native ledger is read unfiltered whatever the request
//! carries.
//!
//! Every expected count here is derived from the fixture. Each test also runs the
//! request through the `from`-driven builder as a cross-check. That is meaningful
//! only on the row path, where both builders converge on `Fluree::wrap_policy`
//! and cannot disagree about policy semantics.
//!
//! Request-side IRIs are absolute throughout. A compact IRI inside `opts.policy`
//! is not expanded against the request `@context` and would match nothing
//! (`docs/security/policy-model.md`).

use crate::support::assert_index_defaults;
use fluree_db_api::{Fluree, FlureeBuilder, QueryExecutionOptions, TimeSpec, VerifiedIdentity};
use serde_json::{json, Value};

const NAME: &str = "http://example.org/ns/name";
const SSN: &str = "http://example.org/ns/ssn";
const PERSON: &str = "http://example.org/ns/Person";
const READER: &str = "http://example.org/ns/ReaderPolicy";
const ALICE_ID: &str = "http://example.org/ns/aliceIdentity";

fn ctx() -> Value {
    json!({ "ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#" })
}

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Five people, each with a name and an SSN. `ex:aliceIdentity` carries a stored
/// `f:policyClass` selecting two reader rules: `ex:hideSsn` reveals an SSN only
/// to the identity linked to that subject, and `ex:allowRest` permits the rest.
/// Committed rather than staged, so both builders read the same ledger.
async fn setup(ledger_id: &str) -> Fluree {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    let mut graph: Vec<Value> = (1..=5)
        .map(|i| {
            json!({
                "@id": format!("ex:p{i}"),
                "@type": "ex:Person",
                "ex:name": format!("Person {i}"),
                "ex:ssn": format!("ssn-{i}"),
            })
        })
        .collect();
    graph.push(json!({
        "@id": "ex:aliceIdentity",
        "f:policyClass": [{ "@id": "ex:ReaderPolicy" }],
        "ex:user": { "@id": "ex:p1" }
    }));
    graph.push(json!({
        "@id": "ex:hideSsn",
        "@type": ["f:AccessPolicy", "ex:ReaderPolicy"],
        "f:required": true,
        "f:action": { "@id": "f:view" },
        "f:onProperty": [{ "@id": "ex:ssn" }],
        "f:query": serde_json::to_string(&json!({
            "where": { "@id": "?$identity", "http://example.org/ns/user": { "@id": "?$this" } }
        }))
        .expect("policy query")
    }));
    graph.push(json!({
        "@id": "ex:allowRest",
        "@type": ["f:AccessPolicy", "ex:ReaderPolicy"],
        "f:action": { "@id": "f:view" },
        "f:query": serde_json::to_string(&json!({})).expect("allow-all query")
    }));

    fluree
        .graph(ledger_id)
        .transact()
        .insert(&json!({ "@context": ctx(), "@graph": graph }))
        .commit()
        .await
        .expect("seed");
    fluree
}

// --- query shapes ---------------------------------------------------------

/// Bind every subject through `ex:name`. A deny on `ex:name` leaves nothing to
/// bind `?s`, so an enforced request answers nothing.
fn by_name() -> Value {
    json!({ "@context": ctx(), "select": ["?s"], "where": { "@id": "?s", "ex:name": "?n" } })
}

/// The same through `ex:ssn`.
fn by_ssn() -> Value {
    json!({ "@context": ctx(), "select": ["?s"], "where": { "@id": "?s", "ex:ssn": "?v" } })
}

fn with_opts(mut q: Value, opts: Value) -> Value {
    q["opts"] = opts;
    q
}

/// An inline rule denying one property, paired with an allow-everything default.
fn deny_property(iri: &str) -> Value {
    json!({
        "policy": [{
            "@id": "http://example.org/ns/denyProp",
            "@type": "f:AccessPolicy",
            "f:action": { "@id": "f:view" },
            "f:onProperty": [{ "@id": iri }],
            "f:allow": false
        }],
        "default-allow": true
    })
}

// --- execution helpers ----------------------------------------------------

fn rows(v: &Value) -> usize {
    v.as_array().map_or(0, Vec::len)
}

/// Run a request through the builder under test.
async fn via_graph(fluree: &Fluree, ledger_id: &str, q: &Value) -> Value {
    fluree
        .graph(ledger_id)
        .query()
        .jsonld(q)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("graph-scoped query failed: {e}"))
}

/// The row-path cross-check.
async fn via_from(fluree: &Fluree, ledger_id: &str, q: &Value) -> Value {
    let mut q = q.clone();
    q["from"] = json!(ledger_id);
    fluree
        .query_from()
        .jsonld(&q)
        .execute_formatted()
        .await
        .unwrap_or_else(|e| panic!("from-driven query failed: {e}"))
}

/// `expected` comes from the fixture and is the standard. The `from`-driven run
/// only cross-checks it. The unrestricted control runs first, so an enforced
/// count cannot pass merely because the fixture is unreadable.
async fn assert_enforced(
    ledger_id: &str,
    fluree: &Fluree,
    base: Value,
    opts: Value,
    expected: usize,
) {
    let control = via_graph(fluree, ledger_id, &base).await;
    assert_eq!(
        rows(&control),
        5,
        "control: without policy inputs the whole fixture must read, or an \
         expected count of {expected} proves nothing: {control}"
    );

    let q = with_opts(base, opts);
    let cross_check = via_from(fluree, ledger_id, &q).await;
    assert_eq!(
        rows(&cross_check),
        expected,
        "cross-check: the from-driven builder must also yield this: {cross_check}"
    );

    let actual = via_graph(fluree, ledger_id, &q).await;
    assert_eq!(
        rows(&actual),
        expected,
        "the graph-scoped builder must enforce the same request: {actual}"
    );
}

// =========================================================================
// opts.policy: the issue's own repro
// =========================================================================

/// The reported case: an inline rule denying `ex:name` under `default-allow`.
#[tokio::test]
async fn inline_policy_denies_property() {
    const LEDGER: &str = "repro/gqb-inline-prop:main";
    let fluree = setup(LEDGER).await;
    assert_enforced(LEDGER, &fluree, by_name(), deny_property(NAME), 0).await;
}

/// The deny must stay scoped to the property it names. Without this, a fix that
/// wraps an over-broad policy would satisfy the case above.
#[tokio::test]
async fn inline_policy_deny_leaves_other_properties_readable() {
    const LEDGER: &str = "repro/gqb-inline-scope:main";
    let fluree = setup(LEDGER).await;
    assert_enforced(LEDGER, &fluree, by_ssn(), deny_property(NAME), 5).await;
}

/// The class-targeted twin.
#[tokio::test]
async fn inline_policy_denies_class() {
    const LEDGER: &str = "repro/gqb-inline-class:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({
        "policy": [{
            "@id": "http://example.org/ns/denyClass",
            "@type": "f:AccessPolicy",
            "f:action": { "@id": "f:view" },
            "f:onClass": [{ "@id": PERSON }],
            "f:allow": false
        }],
        "default-allow": true
    });
    assert_enforced(LEDGER, &fluree, by_name(), opts, 0).await;
}

// =========================================================================
// The other policy inputs `has_any_policy_inputs()` recognizes
// =========================================================================

/// An empty rule selection with a deny default can return nothing. Ignoring it
/// returns the whole ledger.
#[tokio::test]
async fn empty_policy_with_deny_default_returns_nothing() {
    const LEDGER: &str = "repro/gqb-deny-default:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({ "policy": [], "default-allow": false });
    assert_enforced(LEDGER, &fluree, by_name(), opts, 0).await;
}

/// A deny default alone keeps the configured rule set but must not take the
/// unrestricted shortcut. `has_any_policy_inputs` counts it where
/// `selects_policy_set` does not.
#[tokio::test]
async fn deny_default_alone_engages_enforcement() {
    const LEDGER: &str = "repro/gqb-deny-only:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({ "default-allow": false });
    assert_enforced(LEDGER, &fluree, by_name(), opts, 0).await;
}

/// `opts.policy-class` selects the stored reader rules, and `opts.policy-values`
/// binds their `?$identity`. Only Alice's own subject may show an SSN.
#[tokio::test]
async fn policy_class_selects_stored_rules() {
    const LEDGER: &str = "repro/gqb-policy-class:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({
        "policy-class": READER,
        "policy-values": { "?$identity": { "@id": ALICE_ID } },
        "default-allow": false
    });
    assert_enforced(LEDGER, &fluree, by_ssn(), opts, 1).await;
}

/// `opts.policy-values` alone selects a policy set, the last branch of
/// `selects_policy_set`. No rules are selected and no default is given, so the
/// request is fail-closed. Bindings for a policy query are not a grant.
#[tokio::test]
async fn policy_values_alone_engage_enforcement() {
    const LEDGER: &str = "repro/gqb-policy-values:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({ "policy-values": { "?$identity": { "@id": ALICE_ID } } });
    assert_enforced(LEDGER, &fluree, by_name(), opts, 0).await;
}

/// `opts.identity` reaches the same rules through the identity's own stored
/// `f:policyClass`, with no class named by the request.
#[tokio::test]
async fn identity_selects_its_stored_policy_class() {
    const LEDGER: &str = "repro/gqb-identity:main";
    let fluree = setup(LEDGER).await;
    let opts = json!({ "identity": ALICE_ID });
    assert_enforced(LEDGER, &fluree, by_ssn(), opts, 1).await;
}

// =========================================================================
// Every terminal, not just `execute_formatted`
// =========================================================================

/// `.execute()` returns rows before formatting, so it cannot inherit whatever
/// the formatter filters.
#[tokio::test]
async fn raw_execute_enforces_policy() {
    const LEDGER: &str = "repro/gqb-raw:main";
    let fluree = setup(LEDGER).await;
    let q = with_opts(by_name(), deny_property(NAME));

    let control = fluree
        .graph(LEDGER)
        .query()
        .jsonld(&by_name())
        .execute()
        .await
        .expect("control execute");
    assert_eq!(control.row_count(), 5, "control must read the fixture");

    let mut from_q = q.clone();
    from_q["from"] = json!(LEDGER);
    let cross_check = fluree
        .query_from()
        .jsonld(&from_q)
        .execute()
        .await
        .expect("cross-check execute");
    assert_eq!(cross_check.row_count(), 0, "cross-check must enforce");

    let actual = fluree
        .graph(LEDGER)
        .query()
        .jsonld(&q)
        .execute()
        .await
        .expect("graph-scoped execute");
    assert_eq!(
        actual.row_count(),
        0,
        "`.execute()` must enforce the request's policy"
    );
}

/// `.execute_tracked()` is the third terminal. Its `policy_enforcement` field is
/// absent exactly when a request ran unenforced, which pins the bug directly
/// rather than through a row count.
#[tokio::test]
async fn tracked_execute_enforces_and_reports_policy() {
    const LEDGER: &str = "repro/gqb-tracked:main";
    let fluree = setup(LEDGER).await;
    let q = with_opts(by_name(), deny_property(NAME));

    let mut from_q = q.clone();
    from_q["from"] = json!(LEDGER);
    let cross_check = fluree
        .query_from()
        .jsonld(&from_q)
        .track_all()
        .execute_tracked()
        .await
        .expect("cross-check tracked");
    assert_eq!(rows(&cross_check.result), 0, "cross-check must enforce");
    assert!(
        cross_check.policy_enforcement.is_some(),
        "cross-check must report the request as enforced"
    );

    let actual = fluree
        .graph(LEDGER)
        .query()
        .jsonld(&q)
        .track_all()
        .execute_tracked()
        .await
        .expect("graph-scoped tracked");
    assert_eq!(
        rows(&actual.result),
        0,
        "`.execute_tracked()` must enforce the request's policy: {}",
        actual.result
    );
    assert!(
        actual.policy_enforcement.is_some(),
        "an enforced request must be reported as enforced, not as an \
         unenforced read"
    );
}

/// A crawl is filtered during hydration rather than on the row path, and
/// `execute_formatted` selects `format_async_with_policy` only when
/// `view.policy()` is set. An unwrapped view hydrates forbidden nodes without
/// changing the row count, so this asserts on node contents.
///
/// No cross-check here. The `from`-driven builder re-loads an unwrapped view
/// before formatting and leaks the denied property, so matching it would encode
/// that leak. Tracked separately.
#[tokio::test]
async fn subject_crawl_omits_denied_property() {
    const LEDGER: &str = "repro/gqb-crawl:main";
    let fluree = setup(LEDGER).await;
    let base = json!({
        "@context": ctx(),
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "@type": "ex:Person" },
    });

    let shows = |v: &Value, needle: &str| -> bool {
        serde_json::to_string(v)
            .expect("serialize crawl")
            .contains(needle)
    };

    let control = via_graph(&fluree, LEDGER, &base).await;
    assert_eq!(rows(&control), 5, "control: five people: {control}");
    assert!(
        shows(&control, "ssn-"),
        "control: an unrestricted crawl must hydrate ex:ssn, or its absence \
         below proves nothing: {control}"
    );

    let actual = via_graph(&fluree, LEDGER, &with_opts(base, deny_property(SSN))).await;
    assert_eq!(
        rows(&actual),
        5,
        "the deny targets one property: every person still binds: {actual}"
    );
    assert!(
        !shows(&actual, "ssn-"),
        "the graph-scoped builder hydrated a denied property: {actual}"
    );
    assert!(
        shows(&actual, "Person 1"),
        "the rest of each node must survive, or the crawl was denied wholesale \
         rather than filtered: {actual}"
    );
}

// =========================================================================
// Configured ledger defaults: no request policy inputs at all
// =========================================================================

/// A ledger whose config declares `f:policyDefaults` selecting a stored rule
/// that denies `ex:name`. The `from` path applies this through
/// `apply_source_or_global_policy`'s `wrap_policy_defaults` fallback, which the
/// graph-scoped builder never reaches.
async fn setup_configured(ledger_id: &str) -> Fluree {
    let fluree = setup(ledger_id).await;
    fluree
        .graph(ledger_id)
        .transact()
        .insert(&json!({
            "@context": ctx(),
            "@graph": [{
                "@id": "ex:hideNames",
                "@type": ["f:AccessPolicy", "ex:ConfiguredPolicy"],
                "f:action": { "@id": "f:view" },
                "f:onProperty": [{ "@id": "ex:name" }],
                "f:allow": false
            }]
        }))
        .commit()
        .await
        .expect("seed configured rule");

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex:  <http://example.org/ns/> .

        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
            <urn:cfg:policy> f:defaultAllow true .
            <urn:cfg:policy> f:policyClass ex:ConfiguredPolicy .
        }}
    "
    );
    fluree
        .graph(ledger_id)
        .transact()
        .upsert_turtle(&trig)
        .commit()
        .await
        .expect("seed config");

    let view = fluree.db(ledger_id).await.expect("load configured view");
    assert!(
        view.ledger_config()
            .and_then(|c| c.policy.as_ref())
            .is_some(),
        "precondition: the ledger must carry f:policyDefaults"
    );
    fluree
}

/// A configured default must apply even though the request carries no `opts`.
#[tokio::test]
async fn configured_policy_defaults_apply_without_request_opts() {
    const LEDGER: &str = "repro/gqb-config-defaults:main";
    let fluree = setup_configured(LEDGER).await;

    let q = by_name();
    let cross_check = via_from(&fluree, LEDGER, &q).await;
    assert_eq!(
        rows(&cross_check),
        0,
        "cross-check: configured defaults apply with no request opts: {cross_check}"
    );

    let actual = via_graph(&fluree, LEDGER, &q).await;
    assert_eq!(
        rows(&actual),
        0,
        "the graph-scoped builder must apply the ledger's configured policy \
         defaults: {actual}"
    );

    let readable = via_graph(&fluree, LEDGER, &by_ssn()).await;
    assert_eq!(
        rows(&readable),
        5,
        "control: the configured deny targets ex:name only: {readable}"
    );
}

/// The SPARQL twin. SPARQL carries no body `opts`, so a configured default is
/// the only policy this builder can express for it.
#[tokio::test]
async fn configured_policy_defaults_apply_to_sparql() {
    const LEDGER: &str = "repro/gqb-config-sparql:main";
    let fluree = setup_configured(LEDGER).await;

    let bindings = |v: &Value| -> usize {
        v.pointer("/results/bindings")
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    };

    let denied = fluree
        .graph(LEDGER)
        .query()
        .sparql(&format!("SELECT ?s WHERE {{ ?s <{NAME}> ?n }}"))
        .execute_formatted()
        .await
        .expect("graph-scoped sparql");
    assert_eq!(
        bindings(&denied),
        0,
        "the graph-scoped builder must apply configured policy defaults on the \
         SPARQL surface too: {denied}"
    );

    let open = fluree
        .graph(LEDGER)
        .query()
        .sparql(&format!("SELECT ?s WHERE {{ ?s <{SSN}> ?v }}"))
        .execute_formatted()
        .await
        .expect("control sparql");
    assert_eq!(
        bindings(&open),
        5,
        "control: the configured deny targets ex:name only: {open}"
    );
}

// =========================================================================
// Time travel
// =========================================================================

/// Time spec and policy are independent, so a historical read must still be
/// filtered. `graph_at` uses the same `load_view`.
#[tokio::test]
async fn historical_read_enforces_policy() {
    const LEDGER: &str = "repro/gqb-time-travel:main";
    let fluree = setup(LEDGER).await;
    let t = fluree.ledger(LEDGER).await.expect("ledger").t();

    let q = with_opts(by_name(), deny_property(NAME));
    let mut from_q = q.clone();
    from_q["from"] = json!({ "@id": LEDGER, "t": t });
    let cross_check = fluree
        .query_from()
        .jsonld(&from_q)
        .execute_formatted()
        .await
        .expect("cross-check historical");
    assert_eq!(
        rows(&cross_check),
        0,
        "cross-check must enforce at t={t}: {cross_check}"
    );

    let control = fluree
        .graph_at(LEDGER, TimeSpec::AtT(t))
        .query()
        .jsonld(&by_name())
        .execute_formatted()
        .await
        .expect("control historical");
    assert_eq!(rows(&control), 5, "control: t={t} holds the fixture");

    let actual = fluree
        .graph_at(LEDGER, TimeSpec::AtT(t))
        .query()
        .jsonld(&q)
        .execute_formatted()
        .await
        .expect("graph-scoped historical");
    assert_eq!(
        rows(&actual),
        0,
        "a historical read through the graph-scoped builder must enforce the \
         request's policy: {actual}"
    );
}

// =========================================================================
// The verified identity, which rides the builder not the body
// =========================================================================

/// A ledger whose `f:defaultAllow false` may be softened only by a named
/// verified identity, guarded by an `f:IdentityRestricted` override control.
async fn setup_override_controlled(ledger_id: &str) -> Fluree {
    let fluree = setup(ledger_id).await;
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f:   <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:cfg:main> rdf:type f:LedgerConfig .
            <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
            <urn:cfg:policy> f:defaultAllow false .
            <urn:cfg:policy> f:overrideControl <urn:cfg:oc> .
            <urn:cfg:oc> f:controlMode f:IdentityRestricted .
            <urn:cfg:oc> f:allowedIdentities <did:key:admin> .
        }}
    "
    );
    fluree
        .graph(ledger_id)
        .transact()
        .upsert_turtle(&trig)
        .commit()
        .await
        .expect("seed override-controlled config");
    fluree
}

/// The verified identity never arrives in the request body. It rides the
/// builder's execution options, and the wrap has to copy it across. Left unset,
/// an `f:IdentityRestricted` control refuses the request's `default-allow: true`
/// and the config's deny stands.
///
/// The unverified half fails today, since no policy is applied at all. The admin
/// half passes today for the wrong reason. It is here to catch a fix that wraps
/// policy but drops the identity.
#[tokio::test]
async fn verified_identity_from_execution_options_gates_config_override() {
    const LEDGER: &str = "repro/gqb-server-identity:main";
    let fluree = setup_override_controlled(LEDGER).await;
    let q = with_opts(by_name(), json!({ "default-allow": true }));

    let run = |identity: Option<&'static str>| {
        let fluree = fluree.clone();
        let q = q.clone();
        async move {
            let execution = match identity {
                Some(id) => {
                    QueryExecutionOptions::default().with_server_identity(VerifiedIdentity::new(id))
                }
                None => QueryExecutionOptions::default(),
            };
            let out = fluree
                .graph(LEDGER)
                .query()
                .jsonld(&q)
                .execution_options(execution)
                .execute_formatted()
                .await
                .expect("graph-scoped query");
            rows(&out)
        }
    };

    assert_eq!(
        run(Some("did:key:admin")).await,
        5,
        "the listed verified identity may soften the config's deny, so its \
         override must survive the wrap"
    );
    assert_eq!(
        run(None).await,
        0,
        "with no verified identity the config's override control refuses the \
         request's default-allow and its own deny stands"
    );
}
