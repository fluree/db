//! Config graph integration tests
//!
//! End-to-end coverage that the config graph (g_id=2) is reserved/readable,
//! config can be written via TriG, defaults apply automatically, per-graph
//! overrides work, override control blocks/permits query-time overrides,
//! and config is time-travel consistent.

mod support;

use fluree_db_api::config_resolver;
use fluree_db_api::{FlureeBuilder, QueryConnectionOptions};
use serde_json::json;
use support::genesis_ledger;

/// Build the config graph IRI for a canonical ledger id.
fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

// =============================================================================
// Test 1: config graph reserved at g_id=2
// =============================================================================

#[tokio::test]
async fn config_graph_reserved_at_gid2() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-gid2:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Transact anything to materialize the ledger
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();

    let view = fluree.db(ledger_id).await.unwrap();
    // Config graph IRI should resolve to g_id=2
    let config_iri = config_graph_iri(ledger_id);
    let g_id = view.snapshot.graph_registry.graph_id_for_iri(&config_iri);
    assert_eq!(g_id, Some(2), "config graph should be reserved at g_id=2");
}

// =============================================================================
// Test 2: config write via TriG roundtrip
// =============================================================================

#[tokio::test]
async fn config_write_trig_roundtrip() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-trig:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view
        .ledger_config()
        .expect("config should be attached after writing to config graph");
    let policy = config
        .policy
        .as_ref()
        .expect("policy defaults should be read from config");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "defaultAllow should round-trip as false"
    );
}

// =============================================================================
// Test 3: config write via JSON-LD insert
// =============================================================================

#[tokio::test]
async fn config_write_json_ld() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-jsonld:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write config using TriG with JSON-LD-style structure
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    assert_eq!(
        config.policy.as_ref().and_then(|p| p.default_allow),
        Some(true)
    );
    assert_eq!(
        config
            .reasoning
            .as_ref()
            .and_then(|r| r.modes.as_ref())
            .map(std::vec::Vec::as_slice),
        Some(["https://ns.flur.ee/db#rdfs".to_string()].as_slice()),
        "reasoning modes should round-trip as full IRIs"
    );
}

// =============================================================================
// Test 4: policy defaults apply
// =============================================================================

#[tokio::test]
async fn policy_defaults_apply() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-policy-apply:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed data and a policy class in the default graph
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/ns/",
                    "f": "https://ns.flur.ee/db#",
                    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                },
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice"},
                    {"@id": "ex:bob", "@type": "ex:User", "ex:name": "Bob"},
                    {
                        "@id": "ex:DenyAllPolicy",
                        "@type": ["f:Policy"],
                        "f:targetClass": {"@id": "ex:User"},
                        "f:allow": []
                    }
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: defaultAllow=false, policyClass=ex:DenyAllPolicy
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/ns/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:policyClass ex:DenyAllPolicy .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify config is attached and has the expected defaults
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let policy = config.policy.as_ref().expect("policy defaults");
    assert_eq!(policy.default_allow, Some(false));
    assert!(policy.policy_class.is_some(), "policy_class should be set");

    // 4. Verify config defaults actually flow through merge_policy_opts:
    //    empty opts → config's defaultAllow and policyClass should be applied
    let resolved = view.resolved_config().expect("resolved config");
    let empty_opts = QueryConnectionOptions::default();
    let merged = config_resolver::merge_policy_opts(resolved, &empty_opts, None);
    assert!(
        !merged.default_allow,
        "config's defaultAllow=false should apply when no query opts"
    );
    assert!(
        merged.policy_class.is_some(),
        "config's policyClass should apply when no query opts"
    );
}

// =============================================================================
// Test 5: reasoning defaults apply (RDFS subProperty)
// =============================================================================

#[tokio::test]
async fn reasoning_defaults_apply() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-reasoning-apply:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed ontology + data: ex:childName rdfs:subPropertyOf ex:name
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    {"@id": "ex:childName", "rdfs:subPropertyOf": {"@id": "ex:name"}},
                    {"@id": "ex:alice", "ex:childName": "Alice"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: reasoningDefaults with modes=["rdfs"], overrideControl=f:OverrideAll
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs .
            <urn:config:reasoning> f:overrideControl f:OverrideAll .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify config is attached
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let reasoning = config.reasoning.as_ref().expect("reasoning defaults");
    assert_eq!(
        reasoning.modes.as_deref(),
        Some(["https://ns.flur.ee/db#rdfs".to_string()].as_slice()),
        "reasoning modes should be stored as full IRIs"
    );

    // 4. Query: SELECT ?v WHERE { ex:alice ex:name ?v } — without "reasoning" in query
    //    If config reasoning is applied, RDFS subProperty expansion should find "Alice"
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });

    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    // Should find "Alice" via RDFS subProperty expansion (ex:childName subPropertyOf ex:name)
    assert_eq!(
        jsonld,
        json!(["Alice"]),
        "RDFS reasoning from config should auto-expand subProperty"
    );

    // 5. Query WITH "reasoning": "none" — should override config (AllowAll permits override)
    let query_none = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"},
        "reasoning": "none"
    });

    let result_none = fluree
        .query_connection(&query_none)
        .await
        .expect("query none");
    let jsonld_none = result_none
        .to_jsonld(&ledger_state.snapshot)
        .expect("to_jsonld");

    // With reasoning disabled, subProperty expansion should NOT happen
    assert_eq!(
        jsonld_none,
        json!([]),
        "reasoning='none' should override config AllowAll and return no results"
    );
}

// =============================================================================
// Test 5b: config reasoning "none" disables reasoning by default
// =============================================================================

#[tokio::test]
async fn reasoning_none_disables_by_default() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-reasoning-none:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed ontology + data: ex:childName rdfs:subPropertyOf ex:name
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "rdfs": "http://www.w3.org/2000/01/rdf-schema#"
                },
                "@graph": [
                    {"@id": "ex:childName", "rdfs:subPropertyOf": {"@id": "ex:name"}},
                    {"@id": "ex:alice", "ex:childName": "Alice"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: reasoningDefaults with modes=["none"]
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:none .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify from_mode_strings produces explicit_none
    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config should be attached");
    let reasoning = config.reasoning.as_ref().expect("reasoning defaults");
    let mode_strings = reasoning.modes.as_deref().unwrap();
    let modes = fluree_db_query::ReasoningModes::from_mode_strings(mode_strings);
    assert!(
        modes.is_disabled(),
        "from_mode_strings(['none']) should produce explicit_none=true"
    );
    assert!(
        !modes.has_any_enabled(),
        "from_mode_strings(['none']) should have no enabled modes"
    );

    // 4. Query asking for subProperty expansion — should NOT work because config
    //    force-disables reasoning via "none"
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": ["?v"],
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });

    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!([]),
        "config reasoning 'none' should disable RDFS expansion by default"
    );
}

// =============================================================================
// Test 6: per-graph override
// =============================================================================

#[tokio::test]
async fn per_graph_override() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-per-graph:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // 1. Seed data in default graph via TriG
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        ex:alice ex:name "Alice" .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:defaultAllow false .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed + config");

    // 2. Load view and check resolved config
    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");

    // Default graph should have the per-graph override (defaultAllow=false)
    let policy = resolved.policy.as_ref().expect("resolved policy");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "per-graph override should set defaultAllow=false for default graph"
    );
}

// =============================================================================
// Test 7: override_control_none_blocks
// =============================================================================

#[tokio::test]
async fn override_control_none_blocks() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-override-none:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // 1. Seed data
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // 2. Write config: policyDefaults with defaultAllow=false, overrideControl=OverrideNone
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl f:OverrideNone .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    // 3. Verify the override control is set correctly
    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "config sets defaultAllow=false"
    );

    // 4. Check that merge_policy_opts respects OverrideNone:
    //    Even with opts specifying default_allow=true, the config should win
    let opts_with_override = QueryConnectionOptions {
        default_allow: true,
        ..Default::default()
    };
    let merged = config_resolver::merge_policy_opts(resolved, &opts_with_override, None);
    assert!(
        !merged.default_allow,
        "OverrideNone should block query-time default_allow override"
    );
}

// =============================================================================
// Test 8: override_control_identity_restricted
// =============================================================================

#[tokio::test]
async fn override_control_identity_restricted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-identity:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write config with IdentityRestricted override control
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl <urn:config:oc> .
            <urn:config:oc> f:controlMode f:IdentityRestricted .
            <urn:config:oc> f:allowedIdentities <did:key:admin> .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");

    // Verify the override control round-trips correctly
    assert!(
        matches!(
            &policy.override_control,
            fluree_db_core::ledger_config::OverrideControl::IdentityRestricted { .. }
        ),
        "override_control should be IdentityRestricted"
    );

    // Test actual gating behavior via merge_policy_opts
    let opts = QueryConnectionOptions {
        default_allow: true,
        ..Default::default()
    };

    // Admin identity → override permitted (opts.default_allow=true passes through)
    let merged_admin = config_resolver::merge_policy_opts(resolved, &opts, Some("did:key:admin"));
    assert!(
        merged_admin.default_allow,
        "admin identity should be permitted to override"
    );

    // Unknown identity → override denied (config.default_allow=false applied)
    let merged_user = config_resolver::merge_policy_opts(resolved, &opts, Some("did:key:user"));
    assert!(
        !merged_user.default_allow,
        "non-admin identity should be denied override"
    );

    // No identity → override denied (config.default_allow=false applied)
    let merged_none = config_resolver::merge_policy_opts(resolved, &opts, None);
    assert!(
        !merged_none.default_allow,
        "no identity should be denied override"
    );
}

// =============================================================================
// Test 9: override monotonicity
// =============================================================================

#[tokio::test]
async fn override_monotonicity() {
    // Case A: ledger-wide AllowAll + per-graph OverrideNone → tightened to OverrideNone
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-mono-a:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow true .
            <urn:config:policy> f:overrideControl f:OverrideAll .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:overrideControl f:OverrideNone .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let policy = resolved.policy.as_ref().expect("policy");

    // effective_min(AllowAll, OverrideNone) = OverrideNone
    assert!(
        matches!(
            &policy.override_control,
            fluree_db_core::ledger_config::OverrideControl::None
        ),
        "Case A: per-graph OverrideNone should tighten AllowAll to None"
    );

    // Case B: ledger-wide OverrideNone + per-graph AllowAll → ledger-wide blocks per-graph entirely
    let fluree_b = FlureeBuilder::memory().build_memory();
    let ledger_id_b = "it/config-mono-b:main";
    let ledger_b = genesis_ledger(&fluree_b, ledger_id_b);

    let config_iri_b = config_graph_iri(ledger_id_b);
    let trig_b = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri_b}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
            <urn:config:policy> f:overrideControl f:OverrideNone .
            <urn:config:main> f:graphOverrides <urn:config:go1> .
            <urn:config:go1> f:targetGraph f:defaultGraph .
            <urn:config:go1> f:policyDefaults <urn:config:go1-policy> .
            <urn:config:go1-policy> f:defaultAllow true .
            <urn:config:go1-policy> f:overrideControl f:OverrideAll .
        }}
    "
    );

    fluree_b
        .stage_owned(ledger_b)
        .upsert_turtle(&trig_b)
        .execute()
        .await
        .expect("config write");

    let view_b = fluree_b.db(ledger_id_b).await.unwrap();
    let resolved_b = view_b.resolved_config().expect("resolved config");
    let policy_b = resolved_b.policy.as_ref().expect("policy");

    // Ledger-wide OverrideNone blocks per-graph entirely
    assert!(
        matches!(
            &policy_b.override_control,
            fluree_db_core::ledger_config::OverrideControl::None
        ),
        "Case B: ledger-wide OverrideNone should block per-graph override"
    );
    assert_eq!(
        policy_b.default_allow,
        Some(false),
        "Case B: defaultAllow should remain false (ledger-wide value, per-graph blocked)"
    );
}

// =============================================================================
// Test 10: empty config returns None
// =============================================================================

#[tokio::test]
async fn empty_config_returns_none() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-empty:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Transact data (no config)
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();

    let view = fluree.db(ledger_id).await.unwrap();
    assert!(
        view.ledger_config().is_none(),
        "ledger_config should be None when no config is written"
    );
    assert!(
        view.resolved_config().is_none(),
        "resolved_config should be None when no config is written"
    );
}

// =============================================================================
// Test 11: multiple configs — lexicographic tiebreaker
// =============================================================================

#[tokio::test]
async fn multiple_configs_lexicographic_tiebreaker() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-tiebreak:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);

    // Write two LedgerConfig nodes: alpha (defaultAllow=false) and beta (defaultAllow=true)
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:alpha> rdf:type f:LedgerConfig .
            <urn:config:alpha> f:policyDefaults <urn:config:alpha-policy> .
            <urn:config:alpha-policy> f:defaultAllow false .

            <urn:config:beta> rdf:type f:LedgerConfig .
            <urn:config:beta> f:policyDefaults <urn:config:beta-policy> .
            <urn:config:beta-policy> f:defaultAllow true .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view
        .ledger_config()
        .expect("config should be attached with tiebreaker");
    let policy = config.policy.as_ref().expect("policy defaults");

    // "urn:config:alpha" sorts before "urn:config:beta" lexicographically
    assert_eq!(
        policy.default_allow,
        Some(false),
        "alpha (lex-first) config should win with defaultAllow=false"
    );
}

// =============================================================================
// Test 12: config is time-travel consistent
// =============================================================================

#[tokio::test]
async fn config_time_travel_consistent() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-timetravel:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // t=1: seed data (no config)
    let result1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [{"@id": "ex:a", "ex:val": 1}]
            }),
        )
        .await
        .unwrap();
    assert_eq!(result1.receipt.t, 1);

    // t=2: write config
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow false .
        }}
    "
    );

    let result2 = fluree
        .stage_owned(result1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");
    assert_eq!(result2.receipt.t, 2);

    // View at t=1: no config
    let view_t1 = fluree.db_at_t(ledger_id, 1).await.unwrap();
    assert!(
        view_t1.ledger_config().is_none(),
        "at t=1, config graph should be empty (no config)"
    );

    // View at t=2: config present
    let view_t2 = fluree.db_at_t(ledger_id, 2).await.unwrap();
    let config = view_t2
        .ledger_config()
        .expect("at t=2, config should be present");
    let policy = config.policy.as_ref().expect("policy defaults");
    assert_eq!(
        policy.default_allow,
        Some(false),
        "at t=2, config should have defaultAllow=false"
    );
}

// =============================================================================
// Test 14: SHACL config disables validation
// =============================================================================

/// When config sets `shaclDefaults.enabled = false`, SHACL violations should
/// be ignored and the transaction should succeed.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_config_disables_validation() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-disable:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Seed a SHACL shape requiring ex:name on ex:Person
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1,
                    "sh:datatype": {"@id": "xsd:string"}
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: Verify violation fails WITHOUT config (shapes-exist heuristic)
    let err = fluree
        .insert(
            ledger.clone(),
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "@type": "ex:Person"
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "without config, shapes-exist heuristic should trigger SHACL rejection: {err:?}"
    );

    // Step 3: Write config disabling SHACL
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled false .
        }}
    "
    );

    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");
    let ledger = result.ledger;

    // Step 4: Same violating data should now succeed
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect("SHACL disabled by config — violation should be ignored");
}

// =============================================================================
// Test 15: SHACL config warn mode
// =============================================================================

/// When config sets `validationMode = f:ValidationWarn`, SHACL violations
/// should be logged but the transaction should still succeed.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_config_warn_mode() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-warn:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed a SHACL shape
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1,
                    "sh:datatype": {"@id": "xsd:string"}
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Write config: SHACL enabled but in Warn mode
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:validationMode f:ValidationWarn .
        }}
    "
    );

    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");
    let ledger = result.ledger;

    // Violating data should succeed in Warn mode (logged, not rejected)
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:charlie",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect("SHACL warn mode — violation should be logged but transaction succeeds");
}

// =============================================================================
// Test 15b: f:shapesSource points shape compilation at a named graph
// =============================================================================

/// Shapes stored in a named graph (referenced by `f:shapesSource`) are
/// enforced. Shapes in the default graph are NOT — the source config
/// scopes which graph(s) shapes are compiled from.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_shapes_source_points_to_named_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-shapes-source:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Write the shape into a named graph (ex:shapes), NOT the default graph.
    // Same transaction also writes config pointing `f:shapesSource` at that
    // named graph and enabling SHACL.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/shapes> {{
            ex:PersonShape rdf:type sh:NodeShape ;
                           sh:targetClass ex:Person ;
                           sh:property ex:pshape_name .
            ex:pshape_name sh:path ex:name ;
                           sh:minCount 1 ;
                           sh:datatype xsd:string .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:shapesSource <urn:config:shapes-ref> .
            <urn:config:shapes-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:shapes-source> .
            <urn:config:shapes-source> f:graphSelector <http://example.org/shapes> .
        }}
    "
    );

    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shape write should succeed");
    let ledger = result.ledger;

    // Inserting a violating ex:Person (no ex:name) must be rejected — the
    // named-graph shape is discovered via f:shapesSource. Pre-fix, the
    // engine only compiled from the default graph, so the shape was
    // invisible and this txn would have passed.
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:charlie",
                "@type": "ex:Person"
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "shape in named graph referenced by f:shapesSource must be enforced: {err:?}"
    );
}

/// When `f:shapesSource` points at a named graph, shapes in the **default
/// graph** are NOT compiled — only the configured source graph contributes.
/// This is the other half of the `f:shapesSource` contract: the source is
/// authoritative, not additive.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_shapes_source_excludes_default_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-shapes-source-exclusive:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Stash a shape in the DEFAULT graph that would fail the violating data.
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Write config pointing `f:shapesSource` at a DIFFERENT named graph
    // (which has no SHACL shapes). SHACL is enabled.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/shapes> {{
            ex:someMarker ex:unrelated "value" .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:shapesSource <urn:config:shapes-ref> .
            <urn:config:shapes-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:shapes-source> .
            <urn:config:shapes-source> f:graphSelector <http://example.org/shapes> .
        }}
    "#
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");
    let ledger = result.ledger;

    // Violating ex:Person must PASS — the default-graph shape is no longer
    // the source of truth. f:shapesSource explicitly directed compilation to
    // a graph that has no shapes.
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:dan",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect(
            "f:shapesSource is authoritative — default-graph shapes must NOT \
             leak in when the source is an unrelated named graph",
        );
}

// =============================================================================
// Test 15d: Per-graph SHACL enable/disable
// =============================================================================

/// A per-graph `f:shaclEnabled false` override must disable SHACL for that
/// graph only — while ledger-wide SHACL remains enabled for every other
/// graph. Pre-fix, `resolve_txn_shacl_config` ORed enablement across all
/// graphs, so any graph's `enabled: true` (including the ledger-wide
/// baseline) forced SHACL on for every participating graph. That broke the
/// documented semantic in `docs/ledger-config/override-control.md`.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_per_graph_disable_honored() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-pergraph-disable:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed a shape + enable SHACL ledger-wide, AND disable SHACL for a
    // specific named graph (ex:scratch). Writes that land in ex:scratch
    // must pass even if they would violate the shape; writes in the
    // default graph must still fail.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://example.org/> .

        ex:PersonShape rdf:type sh:NodeShape ;
                       sh:targetClass ex:Person ;
                       sh:property ex:pshape_name .
        ex:pshape_name sh:path ex:name ;
                       sh:minCount 1 ;
                       sh:datatype xsd:string .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .

            <urn:config:main> f:graphOverrides <urn:config:scratch-override> .
            <urn:config:scratch-override> rdf:type f:GraphConfig ;
                                          f:targetGraph <http://example.org/scratch> ;
                                          f:shaclDefaults <urn:config:scratch-shacl> .
            <urn:config:scratch-shacl> f:shaclEnabled false .
        }}
    "
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shape write should succeed");
    let ledger = result.ledger;

    // Violating ex:Person written into ex:scratch MUST pass — that graph
    // has SHACL disabled via per-graph override.
    let scratch_trig = r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/scratch> {
            ex:temp_person rdf:type ex:Person .
        }
    ";
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(scratch_trig)
        .execute()
        .await
        .expect(
            "violating ex:Person in ex:scratch must pass — per-graph \
             shaclEnabled=false disables SHACL for that graph only",
        );
    let ledger = result.ledger;

    // Same violating payload written to the DEFAULT graph must fail —
    // ledger-wide SHACL is still enabled and there's no per-graph override
    // disabling the default graph.
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:other_person",
                "@type": "ex:Person"
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "default-graph violation must still be rejected: {err:?}"
    );
}

/// Per-graph SHACL mode: a `Warn`-mode graph with violations must NOT
/// reject the transaction even when other graphs are in `Reject` mode.
/// Conversely, violations in a `Reject` graph still fail the transaction.
///
/// Pre-fix, modes were merged "strictest wins" at the txn level, so any
/// graph in `Reject` forced every graph's violations to fail the txn.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_per_graph_mode_warn_vs_reject() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-pergraph-mode:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Ledger-wide SHACL enabled in Reject mode. Named graph ex:scratch
    // overridden to Warn mode. Shape applies to both.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix ex: <http://example.org/> .

        ex:PersonShape rdf:type sh:NodeShape ;
                       sh:targetClass ex:Person ;
                       sh:property ex:pshape_name .
        ex:pshape_name sh:path ex:name ;
                       sh:minCount 1 ;
                       sh:datatype xsd:string .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true ;
                               f:validationMode f:ValidationReject .

            <urn:config:main> f:graphOverrides <urn:config:scratch-override> .
            <urn:config:scratch-override> rdf:type f:GraphConfig ;
                                          f:targetGraph <http://example.org/scratch> ;
                                          f:shaclDefaults <urn:config:scratch-shacl> .
            <urn:config:scratch-shacl> f:shaclEnabled true ;
                                       f:validationMode f:ValidationWarn .
        }}
    "
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shape write should succeed");
    let ledger = result.ledger;

    // Violating ex:Person in ex:scratch (warn mode) must pass — even though
    // the ledger-wide mode is Reject. Pre-fix, "strictest wins" would have
    // promoted this to Reject and failed the txn.
    let scratch_trig = r"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <http://example.org/scratch> {
            ex:temp_person rdf:type ex:Person .
        }
    ";
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(scratch_trig)
        .execute()
        .await
        .expect(
            "violating ex:Person in ex:scratch (warn mode) must pass — \
             per-graph mode must not be overridden by ledger-wide Reject",
        );
    let ledger = result.ledger;

    // Same violating payload in the default graph (reject mode) must fail.
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:default_person",
                "@type": "ex:Person"
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "default-graph violation under Reject mode must fail: {err:?}"
    );
}

// =============================================================================
// Test 16: SHACL shapes-exist heuristic (no config)
// =============================================================================

/// When no config graph exists but SHACL shapes are present in the database,
/// the shapes-exist heuristic kicks in and SHACL validation runs (backward compat).
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_default_shapes_exist_heuristic() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-heuristic:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed SHACL shapes — no config graph at all
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1,
                    "sh:datatype": {"@id": "xsd:string"}
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Violating data should fail — shapes exist → implicit SHACL enablement
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person"
            }),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "shapes-exist heuristic should trigger SHACL rejection: {err:?}"
    );
}

// =============================================================================
// Test 17: No shapes, no config — SHACL skips
// =============================================================================

/// When neither SHACL shapes nor config graph exist, SHACL validation is
/// entirely skipped and any data transacts successfully.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_no_shapes_no_config_skips() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-noop:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // No shapes, no config — any data should succeed
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:anything",
                "@type": "ex:Whatever",
                "ex:arbitrary": "value"
            }),
        )
        .await
        .expect("no shapes + no config = SHACL has nothing to do");
}

// =============================================================================
// Test 17b: Turtle insert is SHACL-validated under Reject mode
// =============================================================================

/// Before this refactor, `stage_turtle_insert` bypassed config-aware SHACL
/// entirely — it called `stage_flakes` directly without any validation hook.
/// After the shared post-stage helper lands, Turtle inserts must honor the
/// ledger's SHACL policy the same way JSON-LD inserts do.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_turtle_insert_rejected_when_violating() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-turtle-reject:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed a SHACL shape requiring ex:name on every ex:Person.
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1,
                    "sh:datatype": {"@id": "xsd:string"}
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Violating Turtle: an ex:Person with no ex:name.
    let turtle = r"
        @prefix ex: <http://example.org/> .
        ex:charlie a ex:Person .
    ";

    let err = fluree
        .insert_turtle(ledger, turtle)
        .await
        .expect_err("Turtle insert must honor SHACL reject mode (shapes-exist heuristic)");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "Turtle path should route through the shared SHACL helper: {err:?}"
    );
}

// =============================================================================
// Test 17c: Turtle insert under Warn mode logs but succeeds
// =============================================================================

/// Mirrors `shacl_config_warn_mode` but for the Turtle write surface. Verifies
/// that the shared post-stage helper applies warn-mode demotion consistently.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_turtle_insert_warn_mode_logs_but_succeeds() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-turtle-warn:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed the shape first.
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{
                    "sh:path": {"@id": "ex:name"},
                    "sh:minCount": 1,
                    "sh:datatype": {"@id": "xsd:string"}
                }]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Switch SHACL to Warn mode.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:validationMode f:ValidationWarn .
        }}
    "
    );
    let result = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");
    let ledger = result.ledger;

    // Violating Turtle should now succeed (warn-logged, not rejected).
    let turtle = r"
        @prefix ex: <http://example.org/> .
        ex:dave a ex:Person .
    ";
    fluree
        .insert_turtle(ledger, turtle)
        .await
        .expect("Turtle insert must respect Warn mode — violation is logged, not rejected");
}

// =============================================================================
// Test 18: Datalog config disables reasoning (merge_datalog_opts)
// =============================================================================

/// When config sets `datalogDefaults.enabled = false` with OverrideNone,
/// merge_datalog_opts should return enabled=false and override_allowed=false.
#[tokio::test]
async fn datalog_config_disables_reasoning() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/datalog-disable:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Write config: datalog disabled, no overrides permitted
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:datalogDefaults <urn:config:datalog> .
            <urn:config:datalog> f:datalogEnabled false .
            <urn:config:datalog> f:overrideControl f:OverrideNone .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let datalog = config_resolver::merge_datalog_opts(resolved, None)
        .expect("datalog config should be present");

    assert!(!datalog.enabled, "datalog should be disabled by config");
    assert!(
        !datalog.override_allowed,
        "OverrideNone should block overrides"
    );
}

// =============================================================================
// Test 19: Datalog config blocks query-time rules
// =============================================================================

/// When config sets `allowQueryTimeRules = false`, merge_datalog_opts should
/// reflect this. Combined with override_allowed=false, query-time rule
/// injection is blocked.
#[tokio::test]
async fn datalog_config_blocks_query_time_rules() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/datalog-no-rules:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Write config: datalog enabled but query-time rules blocked
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:datalogDefaults <urn:config:datalog> .
            <urn:config:datalog> f:datalogEnabled true .
            <urn:config:datalog> f:allowQueryTimeRules false .
            <urn:config:datalog> f:overrideControl f:OverrideNone .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let datalog = config_resolver::merge_datalog_opts(resolved, None)
        .expect("datalog config should be present");

    assert!(datalog.enabled, "datalog should remain enabled");
    assert!(
        !datalog.allow_query_time_rules,
        "query-time rules should be blocked"
    );
    assert!(
        !datalog.override_allowed,
        "OverrideNone should block overrides"
    );
}

// =============================================================================
// Test 20: Datalog override control identity-restricted
// =============================================================================

/// When config uses IdentityRestricted override control, only the allowed
/// identity can override datalog settings.
#[tokio::test]
async fn datalog_override_control_identity_restricted() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/datalog-identity:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Write config: datalog disabled with identity-restricted override
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:datalogDefaults <urn:config:datalog> .
            <urn:config:datalog> f:datalogEnabled false .
            <urn:config:datalog> f:overrideControl <urn:config:oc> .
            <urn:config:oc> f:controlMode f:IdentityRestricted .
            <urn:config:oc> f:allowedIdentities <did:key:admin> .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");

    // No identity → override denied
    let no_identity = config_resolver::merge_datalog_opts(resolved, None).expect("datalog config");
    assert!(!no_identity.enabled, "datalog disabled by config");
    assert!(
        !no_identity.override_allowed,
        "no identity → override denied"
    );

    // Admin identity → override permitted
    let admin = config_resolver::merge_datalog_opts(resolved, Some("did:key:admin"))
        .expect("datalog config");
    assert!(!admin.enabled, "config still says disabled");
    assert!(
        admin.override_allowed,
        "admin identity → override permitted"
    );

    // Non-admin identity → override denied
    let other = config_resolver::merge_datalog_opts(resolved, Some("did:key:other"))
        .expect("datalog config");
    assert!(
        !other.override_allowed,
        "non-admin identity → override denied"
    );
}

// =============================================================================
// Test 21: merge_shacl_opts unit test
// =============================================================================

/// Verify merge_shacl_opts correctly resolves SHACL config from a written
/// config graph, including ValidationWarn mode.
#[tokio::test]
async fn merge_shacl_opts_unit_test() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-merge:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Write config with SHACL in Warn mode
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:validationMode f:ValidationWarn .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let view = fluree.db(ledger_id).await.unwrap();
    let resolved = view.resolved_config().expect("resolved config");
    let shacl =
        config_resolver::merge_shacl_opts(resolved, None).expect("shacl config should be present");

    assert!(shacl.enabled, "SHACL should be enabled");
    assert_eq!(
        shacl.validation_mode,
        fluree_db_core::ledger_config::ValidationMode::Warn,
        "validation mode should be Warn"
    );
}

// =============================================================================
// Unique constraint enforcement tests
// =============================================================================

/// Helper: write config enabling unique enforcement with default graph as constraint source.
async fn write_unique_config(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_ledger::LedgerState,
    ledger_id: &str,
) -> fluree_db_api::tx::TransactResult {
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:transactDefaults <urn:config:transact> .
            <urn:config:transact> f:uniqueEnabled true .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed")
}

/// Test: annotations + config enabled → duplicate values rejected.
#[tokio::test]
async fn unique_basic_enforcement() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-basic:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Add enforceUnique annotation + seed data
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:email": "alice@example.com"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: Enable unique enforcement in config
    let result = write_unique_config(&fluree, ledger, ledger_id).await;
    let ledger = result.ledger;

    // Step 3: Try to insert duplicate email — should fail
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "duplicate email should trigger unique constraint violation: {err:?}"
    );
}

/// Test: annotations exist but uniqueEnabled not set → duplicates allowed.
#[tokio::test]
async fn unique_not_enabled_no_enforcement() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-not-enabled:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Add enforceUnique annotation + data but NO config enabling it
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:email": "alice@example.com"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Insert duplicate — should succeed (no config enabling uniqueness)
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .expect("duplicate should be allowed when uniqueEnabled is not set");
}

/// Test: enable config in same txn as duplicates → allowed (lagging config).
#[tokio::test]
async fn unique_config_not_retroactive() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-not-retroactive:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Add annotation only
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@id": "ex:email",
                "f:enforceUnique": true
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: In a SINGLE transaction, enable config AND insert duplicates.
    // The config enablement is lagging (read from pre-txn state), so
    // the duplicates should be allowed.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        ex:alice ex:email "alice@example.com" .
        ex:bob ex:email "alice@example.com" .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:transactDefaults <urn:config:transact> .
            <urn:config:transact> f:uniqueEnabled true .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + duplicates in same txn should succeed (lagging config)");
}

/// Test: two subjects with same unique value in one txn → rejected.
#[tokio::test]
async fn unique_intra_txn_bulk() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-bulk:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Add annotation
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@id": "ex:email",
                "f:enforceUnique": true
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: Enable config
    let result = write_unique_config(&fluree, ledger, ledger_id).await;
    let ledger = result.ledger;

    // Step 3: Insert TWO subjects with the same email in one txn → should fail
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:email": "shared@example.com"},
                    {"@id": "ex:bob", "ex:email": "shared@example.com"}
                ]
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "intra-txn duplicates should be rejected: {err:?}"
    );
}

/// Test: different values for unique property → allowed.
#[tokio::test]
async fn unique_different_values_allowed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-diff-vals:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Add annotation + first subject
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:email": "alice@example.com"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Enable config
    let result = write_unique_config(&fluree, ledger, ledger_id).await;
    let ledger = result.ledger;

    // Insert with different value → should succeed
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "bob@example.com"
            }),
        )
        .await
        .expect("different values for unique property should be allowed");
}

// =============================================================================
// Issue #127: Upsert self-conflict on f:enforceUnique
// =============================================================================

/// Regression test for https://github.com/fluree/db-r/issues/127
///
/// Upserting an entity with the same `f:enforceUnique` value it already holds
/// should be an idempotent no-op (retraction + assertion cancel out), NOT a
/// unique constraint violation against itself.
#[tokio::test]
async fn upsert_enforce_unique_self_conflict_should_be_noop() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-self-upsert:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Add enforceUnique annotation on ex:userId + seed an entity
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:userId", "f:enforceUnique": true},
                    {
                        "@id": "ex:user1",
                        "@type": "ex:User",
                        "ex:userId": "u-001",
                        "ex:displayName": "alice"
                    }
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: Enable unique enforcement in config
    let result = write_unique_config(&fluree, ledger, ledger_id).await;
    let ledger = result.ledger;

    // Step 3: Upsert the SAME entity with the SAME unique value (idempotent update)
    // This is the exact pattern from issue #127: re-asserting the same
    // f:enforceUnique value on the same entity should succeed.
    let result = fluree
        .upsert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:user1",
                "@type": "ex:User",
                "ex:userId": "u-001",
                "ex:displayName": "alice-updated"
            }),
        )
        .await;

    assert!(
        result.is_ok(),
        "upsert of same entity with same unique value should succeed (not self-conflict): {:?}",
        result.err()
    );
    let ledger = result.unwrap().ledger;

    // Step 4: Verify entity appears exactly once
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?u", "@type": "ex:User", "ex:userId": "u-001"},
        "select": {"?u": ["*"]}
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");
    let arr = rows.as_array().expect("result should be array");
    assert_eq!(
        arr.len(),
        1,
        "entity should appear exactly once after idempotent upsert, got: {arr:#?}"
    );

    // Step 5: Verify the displayName was updated
    let entity = &arr[0];
    assert_eq!(
        entity.get("ex:displayName").and_then(|v| v.as_str()),
        Some("alice-updated"),
        "displayName should be updated"
    );
}

/// Regression test for https://github.com/fluree/db-r/issues/127 (corruption variant)
///
/// Even if an upsert fails (e.g., a genuine unique constraint violation), the
/// failure must NOT corrupt in-memory state. A subsequent successful transaction
/// must leave the ledger in a consistent state.
#[tokio::test]
async fn failed_upsert_does_not_corrupt_in_memory_state() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-no-corrupt:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Step 1: Add enforceUnique annotation + seed two entities with distinct values
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:email": "alice@example.com", "ex:name": "Alice"},
                    {"@id": "ex:bob", "ex:email": "bob@example.com", "ex:name": "Bob"}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = result.ledger;

    // Step 2: Enable unique enforcement
    let result = write_unique_config(&fluree, ledger, ledger_id).await;
    let ledger = result.ledger;

    // Step 3: Attempt an insert that SHOULD fail (bob tries to claim alice's email)
    let err = fluree
        .insert(
            ledger.clone(),
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "alice@example.com"
            }),
        )
        .await;
    assert!(err.is_err(), "should fail with unique constraint violation");

    // Step 4: Perform a SECOND successful transaction on the same ledger state.
    // If the failed transaction corrupted in-memory state, this commit
    // would persist the corruption.
    let result = fluree
        .insert(
            ledger.clone(),
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:charlie",
                "ex:email": "charlie@example.com",
                "ex:name": "Charlie"
            }),
        )
        .await
        .expect("subsequent insert should succeed");
    let ledger = result.ledger;

    // Step 5: Verify alice is NOT duplicated
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?u", "ex:email": "alice@example.com"},
        "select": {"?u": ["*"]}
    });
    let rows = support::query_jsonld_formatted(&fluree, &ledger, &query)
        .await
        .expect("query should succeed");
    let arr = rows.as_array().expect("result should be array");
    assert_eq!(
        arr.len(),
        1,
        "alice should appear exactly once (no duplication from failed txn): {arr:#?}"
    );

    // Step 6: Verify alice's properties are intact
    let alice = &arr[0];
    assert_eq!(
        alice.get("ex:name").and_then(|v| v.as_str()),
        Some("Alice"),
        "alice's properties should be intact after failed upsert + subsequent commit"
    );
}

// =============================================================================
// Full-text config (`f:fullTextDefaults`) tests
// =============================================================================

/// Minimal `f:fullTextDefaults` round-trip:
/// - `f:defaultLanguage` reads back verbatim.
/// - `f:property` list round-trips all configured `f:target` IRIs.
#[tokio::test]
async fn fulltext_defaults_round_trip() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-rt:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:defaultLanguage "fr" .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft> f:property <urn:config:ft:body> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .
            <urn:config:ft:body> rdf:type f:FullTextProperty .
            <urn:config:ft:body> f:target ex:body .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view
        .ledger_config()
        .expect("config should be attached after writing to config graph");
    let ft = config
        .full_text
        .as_ref()
        .expect("full_text defaults should be read from config");

    assert_eq!(
        ft.default_language.as_deref(),
        Some("fr"),
        "defaultLanguage should round-trip verbatim"
    );
    let targets: std::collections::HashSet<&str> =
        ft.properties.iter().map(|p| p.target.as_str()).collect();
    assert!(
        targets.contains("http://example.org/title"),
        "ex:title should be in the configured property list: {targets:?}"
    );
    assert!(
        targets.contains("http://example.org/body"),
        "ex:body should be in the configured property list: {targets:?}"
    );
    assert_eq!(
        ft.properties.len(),
        2,
        "exactly two properties were configured: {targets:?}"
    );
}

/// `f:fullTextDefaults` per-graph override is additive: the override's
/// `f:property` list is appended to the ledger-wide list (deduping by
/// target IRI; per-graph wins), and the override's `f:defaultLanguage`
/// shadows the ledger-wide one.
#[tokio::test]
async fn fulltext_defaults_per_graph_override_additive() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-override:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let target_graph = "urn:test:productCatalog";
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:defaultLanguage "en" .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .

            <urn:config:main> f:graphOverrides <urn:config:gc:catalog> .
            <urn:config:gc:catalog> rdf:type f:GraphConfig .
            <urn:config:gc:catalog> f:targetGraph <{target_graph}> .
            <urn:config:gc:catalog> f:fullTextDefaults <urn:config:ft-catalog> .
            <urn:config:ft-catalog> rdf:type f:FullTextDefaults .
            <urn:config:ft-catalog> f:defaultLanguage "es" .
            <urn:config:ft-catalog> f:property <urn:config:ft-catalog:name> .
            <urn:config:ft-catalog:name> rdf:type f:FullTextProperty .
            <urn:config:ft-catalog:name> f:target ex:productName .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config attached");

    // Ledger-wide: one property, language "en".
    let ft = config.full_text.as_ref().expect("ledger-wide full_text");
    assert_eq!(ft.default_language.as_deref(), Some("en"));
    assert_eq!(ft.properties.len(), 1);
    assert_eq!(ft.properties[0].target, "http://example.org/title");

    // Effective for the catalog graph: additive merge.
    let effective = config_resolver::resolve_effective_config(config, Some(target_graph));
    let effective_ft = effective
        .full_text
        .as_ref()
        .expect("merged full_text defaults for catalog graph");
    assert_eq!(
        effective_ft.default_language.as_deref(),
        Some("es"),
        "per-graph defaultLanguage 'es' should shadow ledger-wide 'en'"
    );
    let targets: std::collections::HashSet<&str> = effective_ft
        .properties
        .iter()
        .map(|p| p.target.as_str())
        .collect();
    assert!(
        targets.contains("http://example.org/title"),
        "additive merge keeps ledger-wide ex:title: {targets:?}"
    );
    assert!(
        targets.contains("http://example.org/productName"),
        "per-graph ex:productName should be added: {targets:?}"
    );
    assert_eq!(
        effective_ft.properties.len(),
        2,
        "additive merge produces exactly two entries: {targets:?}"
    );
}

/// Ledger-wide `f:OverrideNone` on `f:fullTextDefaults` blocks any per-graph
/// override from taking effect — the effective config is the ledger-wide
/// group unchanged.
#[tokio::test]
async fn fulltext_defaults_override_none_blocks_per_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-override-none:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let target_graph = "urn:test:productCatalog";
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:defaultLanguage "en" .
            <urn:config:ft> f:overrideControl f:OverrideNone .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .

            <urn:config:main> f:graphOverrides <urn:config:gc:catalog> .
            <urn:config:gc:catalog> rdf:type f:GraphConfig .
            <urn:config:gc:catalog> f:targetGraph <{target_graph}> .
            <urn:config:gc:catalog> f:fullTextDefaults <urn:config:ft-catalog> .
            <urn:config:ft-catalog> rdf:type f:FullTextDefaults .
            <urn:config:ft-catalog> f:defaultLanguage "es" .
            <urn:config:ft-catalog> f:property <urn:config:ft-catalog:name> .
            <urn:config:ft-catalog:name> rdf:type f:FullTextProperty .
            <urn:config:ft-catalog:name> f:target ex:productName .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config attached");

    let effective = config_resolver::resolve_effective_config(config, Some(target_graph));
    let effective_ft = effective
        .full_text
        .as_ref()
        .expect("effective full_text when ledger-wide is Some");

    assert_eq!(
        effective_ft.default_language.as_deref(),
        Some("en"),
        "OverrideNone should keep ledger-wide language, rejecting per-graph 'es'"
    );
    assert_eq!(
        effective_ft.properties.len(),
        1,
        "per-graph property should NOT be added under OverrideNone"
    );
    assert_eq!(
        effective_ft.properties[0].target, "http://example.org/title",
        "ledger-wide property is the only entry"
    );
}

/// `configured_fulltext_properties_for_indexer` produces the flat list the
/// indexer expects: ledger-wide entries as `AnyGraph`, named-graph overrides
/// as `NamedGraph(iri)`, and `f:defaultGraph` / `f:txnMetaGraph` sentinel
/// overrides mapped to their dedicated scope variants so the indexer can
/// route them to the correct `GraphId` without treating the sentinel IRI
/// as a literal graph.
#[tokio::test]
async fn configured_fulltext_properties_for_indexer_shape() {
    use fluree_db_indexer::ConfiguredFulltextScope;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-config-indexer-shape:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let target_graph = "urn:test:productCatalog";
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:fullTextDefaults <urn:config:ft> .
            <urn:config:ft> rdf:type f:FullTextDefaults .
            <urn:config:ft> f:defaultLanguage "en" .
            <urn:config:ft> f:property <urn:config:ft:title> .
            <urn:config:ft:title> rdf:type f:FullTextProperty .
            <urn:config:ft:title> f:target ex:title .

            <urn:config:main> f:graphOverrides <urn:config:gc:catalog> ,
                                                <urn:config:gc:default> ,
                                                <urn:config:gc:txn> .

            <urn:config:gc:catalog> rdf:type f:GraphConfig .
            <urn:config:gc:catalog> f:targetGraph <{target_graph}> .
            <urn:config:gc:catalog> f:fullTextDefaults <urn:config:ft-catalog> .
            <urn:config:ft-catalog> rdf:type f:FullTextDefaults .
            <urn:config:ft-catalog> f:property <urn:config:ft-catalog:name> .
            <urn:config:ft-catalog:name> rdf:type f:FullTextProperty .
            <urn:config:ft-catalog:name> f:target ex:productName .

            <urn:config:gc:default> rdf:type f:GraphConfig .
            <urn:config:gc:default> f:targetGraph f:defaultGraph .
            <urn:config:gc:default> f:fullTextDefaults <urn:config:ft-default> .
            <urn:config:ft-default> rdf:type f:FullTextDefaults .
            <urn:config:ft-default> f:property <urn:config:ft-default:note> .
            <urn:config:ft-default:note> rdf:type f:FullTextProperty .
            <urn:config:ft-default:note> f:target ex:note .

            <urn:config:gc:txn> rdf:type f:GraphConfig .
            <urn:config:gc:txn> f:targetGraph f:txnMetaGraph .
            <urn:config:gc:txn> f:fullTextDefaults <urn:config:ft-txn> .
            <urn:config:ft-txn> rdf:type f:FullTextDefaults .
            <urn:config:ft-txn> f:property <urn:config:ft-txn:memo> .
            <urn:config:ft-txn:memo> rdf:type f:FullTextProperty .
            <urn:config:ft-txn:memo> f:target ex:memo .
        }}
    "#
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config attached");
    let props = config_resolver::configured_fulltext_properties_for_indexer(config);

    // Ledger-wide → AnyGraph.
    let any_graph: Vec<&str> = props
        .iter()
        .filter(|p| matches!(p.scope, ConfiguredFulltextScope::AnyGraph))
        .map(|p| p.property_iri.as_str())
        .collect();
    assert!(
        any_graph.contains(&"http://example.org/title"),
        "ledger-wide entry must land under AnyGraph: {any_graph:?}"
    );

    // Named-graph override → NamedGraph(target_graph).
    let named_graph: Vec<&str> = props
        .iter()
        .filter(
            |p| matches!(&p.scope, ConfiguredFulltextScope::NamedGraph(iri) if iri == target_graph),
        )
        .map(|p| p.property_iri.as_str())
        .collect();
    assert_eq!(
        named_graph,
        vec!["http://example.org/productName"],
        "named-graph override must be scoped to its target IRI"
    );

    // `f:defaultGraph` sentinel → DefaultGraph (not AnyGraph!).
    let default_graph: Vec<&str> = props
        .iter()
        .filter(|p| matches!(p.scope, ConfiguredFulltextScope::DefaultGraph))
        .map(|p| p.property_iri.as_str())
        .collect();
    assert_eq!(
        default_graph,
        vec!["http://example.org/note"],
        "`f:defaultGraph` override must land under DefaultGraph, distinct from AnyGraph"
    );

    // `f:txnMetaGraph` sentinel → TxnMetaGraph (NOT NamedGraph(sentinel_iri)).
    let txn_meta: Vec<&str> = props
        .iter()
        .filter(|p| matches!(p.scope, ConfiguredFulltextScope::TxnMetaGraph))
        .map(|p| p.property_iri.as_str())
        .collect();
    assert_eq!(
        txn_meta,
        vec!["http://example.org/memo"],
        "`f:txnMetaGraph` override must land under TxnMetaGraph, not a named graph"
    );

    // And critically: no entry should carry the literal sentinel IRI in a
    // `NamedGraph` scope — that was the bug we're guarding against.
    for prop in &props {
        if let ConfiguredFulltextScope::NamedGraph(iri) = &prop.scope {
            assert_ne!(
                iri, "https://ns.flur.ee/db#defaultGraph",
                "f:defaultGraph sentinel leaked into NamedGraph variant"
            );
            assert_ne!(
                iri, "https://ns.flur.ee/db#txnMetaGraph",
                "f:txnMetaGraph sentinel leaked into NamedGraph variant"
            );
        }
    }
}
