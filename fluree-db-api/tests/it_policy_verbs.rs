//! Write-verb policy enforcement tests (`f:create` / `f:update` / `f:delete`).
//!
//! Verbs classify each subject's lifecycle within the transaction —
//! (exists pre, exists post): create (no, yes), update (yes, yes),
//! delete (yes, no) — and every staged flake inherits its subject's verb.
//! Class targeting for verb policies uses pre∪post classes, and `rdf:type`
//! flakes match by the class they assert or retract (minting C = operation
//! on C). Bare `f:modify` keeps legacy pre-state class semantics.

use crate::support;
use crate::support::{assert_index_defaults, genesis_ledger};
use fluree_db_api::policy_builder;
use fluree_db_api::{
    CommitOpts, FlureeBuilder, GovernanceOptions, IndexConfig, LedgerState,
    TrackedTransactionInput, TxnOpts, TxnType,
};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;

fn index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

/// Inline view-allow-everything policy so WHERE clauses read normally under
/// default-deny; write behavior is then governed entirely by the verb
/// policies under test.
fn view_all() -> JsonValue {
    json!({
        "@id": "ex:viewAll",
        "f:action": "f:view",
        "f:allow": true
    })
}

/// Seed a Lead (with two properties) and a Person.
async fn seed(fluree: &support::MemoryFluree, ledger_id: &str) -> LedgerState {
    let ledger0 = genesis_ledger(fluree, ledger_id);
    let txn = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@graph": [
            {"@id": "ex:lead1", "@type": "ex:Lead", "ex:name": "First", "ex:stage": "new"},
            {"@id": "ex:bob", "@type": "ex:Person", "ex:nickname": "Bob"}
        ]
    });
    fluree.insert(ledger0, &txn).await.expect("seed").ledger
}

async fn policy_ctx(ledger: &LedgerState, policies: JsonValue) -> fluree_db_policy::PolicyContext {
    let qc_opts = GovernanceOptions {
        policy: Some(policies),
        default_allow: false,
        ..Default::default()
    };
    policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context")
}

async fn try_txn(
    fluree: &support::MemoryFluree,
    ledger: LedgerState,
    txn_type: TxnType,
    txn: &JsonValue,
    ctx: &fluree_db_policy::PolicyContext,
) -> std::result::Result<LedgerState, String> {
    let input = TrackedTransactionInput::new(txn_type, txn, TxnOpts::default(), ctx);
    fluree
        .transact_tracked_with_policy(ledger, input, CommitOpts::default(), &index_config())
        .await
        .map(|(tx_result, _tally)| tx_result.ledger)
        .map_err(|e| format!("{e:?}"))
}

/// "May create new Leads": a create-scoped class allow permits inserting a
/// brand-new subject typed as the class, including never-before-seen
/// properties — inexpressible before write verbs (class targeting resolved
/// pre-state, where a new subject has no class).
#[tokio::test]
async fn create_allows_new_instance_of_class() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_create_allow").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:leadCreators",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:create",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead2",
        "@type": "ex:Lead",
        "ex:name": "Second",
        "ex:brandNewField": 7
    });
    let result = try_txn(&fluree, ledger, TxnType::Insert, &insert, &ctx).await;
    assert!(
        result.is_ok(),
        "create-scoped class allow must permit a new instance: {result:?}"
    );
}

/// The create grant is class-exact: `rdf:type` flakes match by the class
/// they mint, so an allow scoped to Lead can never mint another class —
/// alone or smuggled alongside a legitimate Lead typing.
#[tokio::test]
async fn create_denies_minting_other_class() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_create_mint").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:leadCreators",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:create",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    let other_class = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:c1",
        "@type": "ex:Contract",
        "ex:name": "Sneaky"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &other_class, &ctx).await;
    assert!(result.is_err(), "minting ex:Contract must be denied");

    // Multi-typing a legitimate Lead with a second class must also fail:
    // the rdf:type ex:Contract flake matches no create grant even though
    // the subject IS a (post-state) Lead.
    let smuggled = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead3",
        "@type": ["ex:Lead", "ex:Contract"],
        "ex:name": "Smuggler"
    });
    let result = try_txn(&fluree, ledger, TxnType::Insert, &smuggled, &ctx).await;
    assert!(
        result.is_err(),
        "smuggling a second class alongside a permitted one must be denied"
    );
}

/// A create-only grant covers creation, not modification of existing
/// instances.
#[tokio::test]
async fn create_does_not_allow_update() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_create_not_update").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:leadCreators",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:create",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    let edit = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead1",
        "ex:name": "Renamed"
    });
    let result = try_txn(&fluree, ledger, TxnType::Insert, &edit, &ctx).await;
    assert!(
        result.is_err(),
        "create-only grant must not permit modifying an existing instance"
    );
}

/// Update covers value changes (retract old + assert new) and partial
/// retraction of a persisting subject — but neither creation nor entity
/// removal.
#[tokio::test]
async fn update_scopes_to_existing_persisting_subjects() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_update").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:leadEditors",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:update",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    // Value change: retract old + assert new, both classify as update.
    let change = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "qualified"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &change, &ctx).await;
    assert!(result.is_ok(), "value change must be update: {result:?}");
    let ledger = result.unwrap();
    // The policy context pins pre-txn state for its own lookups but is
    // rebuilt per request in production; rebuild it on the advanced head.
    let ctx = policy_ctx(
        &ledger,
        json!([
            view_all(),
            {
                "@id": "ex:leadEditors",
                "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
                "f:action": "f:update",
                "f:allow": true
            }
        ]),
    )
    .await;

    // Partial retraction: clear one property, subject persists.
    let clear = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:name": "?n"},
        "delete": {"@id": "ex:lead1", "ex:name": "?n"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &clear, &ctx).await;
    assert!(
        result.is_ok(),
        "partial retraction must be update: {result:?}"
    );

    // Creation is not update.
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead9",
        "@type": "ex:Lead",
        "ex:name": "New"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &insert, &ctx).await;
    assert!(
        result.is_err(),
        "update-only grant must not permit creation"
    );

    // Entity removal is not update: retracting ALL of the subject's flakes
    // classifies as delete.
    let remove = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "?p": "?o"},
        "delete": {"@id": "ex:lead1", "?p": "?o"}
    });
    let result = try_txn(&fluree, ledger, TxnType::Update, &remove, &ctx).await;
    assert!(
        result.is_err(),
        "update-only grant must not permit entity removal"
    );
}

/// Delete covers entity removal only — not value edits, not partial
/// retraction.
#[tokio::test]
async fn delete_allows_entity_removal_only() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_delete").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:leadReapers",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:delete",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    // Value change is update, not delete.
    let change = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "qualified"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &change, &ctx).await;
    assert!(
        result.is_err(),
        "delete-only grant must not permit value changes"
    );

    // Full removal of the entity is delete.
    let remove = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "?p": "?o"},
        "delete": {"@id": "ex:lead1", "?p": "?o"}
    });
    let result = try_txn(&fluree, ledger, TxnType::Update, &remove, &ctx).await;
    assert!(
        result.is_ok(),
        "delete grant must permit entity removal: {result:?}"
    );
}

/// The audit-log pattern, verb form: deny update+delete, allow create.
/// New events insert; edits, un-typing, and removal are all denied —
/// including the un-typing escape (retracting `rdf:type` while keeping the
/// entity), which matches by the class being retracted.
#[tokio::test]
async fn immutable_audit_events_with_verbs() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "verbs_audit");
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt1",
        "@type": "ex:AuditEvent",
        "ex:detail": "created account"
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:auditImmutable",
            "f:onClass": [{"@id": "http://example.org/ns/AuditEvent"}],
            "f:action": [{"@id": "f:update"}, {"@id": "f:delete"}],
            "f:allow": false,
            "f:exMessage": "Audit events are immutable."
        },
        {
            "@id": "ex:auditAppend",
            "f:onClass": [{"@id": "http://example.org/ns/AuditEvent"}],
            "f:action": "f:create",
            "f:allow": true
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    // New events insert freely.
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt2",
        "@type": "ex:AuditEvent",
        "ex:detail": "login"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &insert, &ctx).await;
    assert!(result.is_ok(), "new audit events must insert: {result:?}");

    // Edits are denied.
    let edit = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt1",
        "ex:detail": "tampered"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &edit, &ctx).await;
    assert!(result.is_err(), "audit events must not be editable");

    // The un-typing escape is closed: retracting rdf:type while the entity
    // persists is an update ON ex:AuditEvent (object-class matching).
    let untype = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:evt1", "@type": "?t"},
        "delete": {"@id": "ex:evt1", "@type": "?t"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &untype, &ctx).await;
    assert!(result.is_err(), "un-typing an audit event must be denied");

    // Entity removal is denied.
    let remove = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:evt1", "?p": "?o"},
        "delete": {"@id": "ex:evt1", "?p": "?o"}
    });
    let result = try_txn(&fluree, ledger, TxnType::Update, &remove, &ctx).await;
    assert!(result.is_err(), "audit events must not be removable");
}

/// Bare `f:modify` keeps its legacy pre-state class semantics: a
/// class-targeted deny does not catch creation of new instances (the
/// documented immutable-records pattern relies on this to keep new inserts
/// flowing under default-allow).
#[tokio::test]
async fn legacy_modify_class_semantics_preserved() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger0 = genesis_ledger(&fluree, "verbs_legacy_modify");
    let seed = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt1",
        "@type": "ex:AuditEvent",
        "ex:detail": "created account"
    });
    let ledger = fluree.insert(ledger0, &seed).await.expect("seed").ledger;

    let policies = json!([{
        "@id": "ex:auditImmutableLegacy",
        "f:required": true,
        "f:onClass": [{"@id": "http://example.org/ns/AuditEvent"}],
        "f:action": "f:modify",
        "f:allow": false
    }]);
    let qc_opts = GovernanceOptions {
        policy: Some(policies),
        default_allow: true,
        ..Default::default()
    };
    let ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    // Legacy semantics: a NEW AuditEvent has no pre-state class, so the
    // class-targeted deny does not match and default-allow admits it.
    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt2",
        "@type": "ex:AuditEvent",
        "ex:detail": "login"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &insert, &ctx).await;
    assert!(
        result.is_ok(),
        "legacy f:modify deny must not catch creates: {result:?}"
    );

    // Existing instances stay protected.
    let edit = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:evt1",
        "ex:detail": "tampered"
    });
    let result = try_txn(&fluree, ledger, TxnType::Insert, &edit, &ctx).await;
    assert!(result.is_err(), "legacy f:modify deny must catch edits");
}

/// Stored verb policies (loaded via `policy-class`) parse identically to
/// inline ones: the loader recognizes f:create/f:update/f:delete action
/// IRIs and applies exact lifecycle semantics.
#[tokio::test]
async fn stored_verb_policy_via_policy_class() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_stored").await;

    let stored = json!({
        "@context": {"ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#"},
        "@graph": [
            {
                "@id": "ex:leadCreators",
                "@type": ["f:AccessPolicy", "ex:AppPolicy"],
                "f:onClass": [{"@id": "ex:Lead"}],
                "f:action": {"@id": "f:create"},
                "f:allow": true
            },
            {
                "@id": "ex:viewAllStored",
                "@type": ["f:AccessPolicy", "ex:AppPolicy"],
                "f:action": {"@id": "f:view"},
                "f:allow": true
            }
        ]
    });
    let ledger = fluree
        .insert(ledger, &stored)
        .await
        .expect("store policies")
        .ledger;

    let qc_opts = GovernanceOptions {
        policy_class: Some(vec!["http://example.org/ns/AppPolicy".to_string()]),
        default_allow: false,
        ..Default::default()
    };
    let ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &qc_opts,
        &[0],
    )
    .await
    .expect("build policy context");

    let insert = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead5",
        "@type": "ex:Lead",
        "ex:name": "Stored"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &insert, &ctx).await;
    assert!(
        result.is_ok(),
        "stored create grant must permit a new Lead: {result:?}"
    );

    let edit = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead1",
        "ex:name": "Renamed"
    });
    let result = try_txn(&fluree, ledger, TxnType::Insert, &edit, &ctx).await;
    assert!(
        result.is_err(),
        "stored create grant must not permit editing an existing Lead"
    );
}

/// `?$value` / `?$op` condition bindings: a required value gate on a
/// property constrains what may be ASSERTED while exempting retractions —
/// a value change retracts the old value, whose `?$value` would otherwise
/// fail the constraint.
#[tokio::test]
async fn value_op_gate_constrains_asserted_values_sparql() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_value_gate").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:stageGate",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/ns/stage"}],
            "f:action": "f:modify",
            "f:exMessage": "stage may only be set to 'qualified'",
            "f:query": {
                "@type": "f:sparql",
                "@value": "ASK { FILTER($op = \"retract\" || $value = \"qualified\") }"
            }
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    let good = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "qualified"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &good, &ctx).await;
    assert!(
        result.is_ok(),
        "setting stage to the allowed value must pass: {result:?}"
    );

    let bad = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "junk"}
    });
    let result = try_txn(&fluree, ledger, TxnType::Update, &bad, &ctx).await;
    assert!(
        result.is_err(),
        "setting stage to a disallowed value must be denied"
    );
}

/// The same value gate in the JSON-LD `ask` form: `?$value` / `?$op` bind
/// in every condition language.
#[tokio::test]
async fn value_op_gate_constrains_asserted_values_jsonld() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_value_gate_jsonld").await;

    let policies = json!([
        view_all(),
        {
            "@id": "ex:stageGate",
            "f:required": true,
            "f:onProperty": [{"@id": "http://example.org/ns/stage"}],
            "f:action": "f:modify",
            "f:query": serde_json::to_string(&json!({
                "ask": [["filter", "(or (= ?$op \"retract\") (= ?$value \"qualified\"))"]]
            }))
            .unwrap()
        }
    ]);
    let ctx = policy_ctx(&ledger, policies).await;

    let good = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "qualified"}
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Update, &good, &ctx).await;
    assert!(result.is_ok(), "allowed value must pass: {result:?}");

    let bad = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "where":  {"@id": "ex:lead1", "ex:stage": "?s"},
        "delete": {"@id": "ex:lead1", "ex:stage": "?s"},
        "insert": {"@id": "ex:lead1", "ex:stage": "junk"}
    });
    let result = try_txn(&fluree, ledger, TxnType::Update, &bad, &ctx).await;
    assert!(result.is_err(), "disallowed value must be denied");
}

/// `f:queryState f:postState`: "may create Leads owned by self" — the
/// condition must see the ex:owner property being asserted in the same
/// transaction, which pre-state (the default) cannot.
#[tokio::test]
async fn post_state_condition_sees_staged_flakes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed(&fluree, "verbs_post_state").await;

    let create_own = |query_state: Option<&str>| {
        let mut policy = json!({
            "@id": "ex:leadCreatorsOwnOnly",
            "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
            "f:action": "f:create",
            "f:query": {
                "@type": "f:sparql",
                "@value": "ASK { $this <http://example.org/ns/owner> $identity }"
            }
        });
        if let Some(state) = query_state {
            policy["f:queryState"] = json!({"@id": state});
        }
        json!([view_all(), policy])
    };

    let opts_for = |policies: serde_json::Value| GovernanceOptions {
        policy: Some(policies),
        policy_values: Some(HashMap::from([(
            "?$identity".to_string(),
            json!({"@id": "http://example.org/ns/apiUser"}),
        )])),
        default_allow: false,
        ..Default::default()
    };

    // The identity must resolve to a subject for $identity to bind.
    let seed_identity = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:apiUser",
        "ex:kind": "service"
    });
    let ledger = fluree
        .insert(ledger, &seed_identity)
        .await
        .expect("seed identity")
        .ledger;

    let owned = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead7",
        "@type": "ex:Lead",
        "ex:name": "Mine",
        "ex:owner": {"@id": "ex:apiUser"}
    });

    // Post-state condition sees the staged ex:owner assert → allowed.
    let post_opts = opts_for(create_own(Some("f:postState")));
    let ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &post_opts,
        &[0],
    )
    .await
    .expect("build policy context");
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &owned, &ctx).await;
    assert!(
        result.is_ok(),
        "post-state create-with-owner condition must pass: {result:?}"
    );

    // Without an owner the condition never holds.
    let unowned = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:lead8",
        "@type": "ex:Lead",
        "ex:name": "Nobody's"
    });
    let result = try_txn(&fluree, ledger.clone(), TxnType::Insert, &unowned, &ctx).await;
    assert!(result.is_err(), "ownerless create must be denied");

    // Control: the same policy WITHOUT f:queryState evaluates against
    // pre-state, where the new Lead's owner does not exist yet → denied.
    // This pins that the selector is what switches the state.
    let pre_opts = opts_for(create_own(None));
    let pre_ctx = policy_builder::build_policy_context_from_opts(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &pre_opts,
        &[0],
    )
    .await
    .expect("build policy context");
    let result = try_txn(&fluree, ledger, TxnType::Insert, &owned, &pre_ctx).await;
    assert!(
        result.is_err(),
        "pre-state (default) condition must not see staged flakes"
    );
}
