//! Policy + query-connection integration tests
//!
//! Focus:
//! - identity-based policy loading via `f:policyClass` on the identity subject
//! - view policy enforcement on direct selects and expansion formatting

use crate::support::{assert_index_defaults, genesis_ledger, normalize_rows, seed_people_with_ssn};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

#[tokio::test]
async fn policy_inline_denies_restricted_property_in_direct_select() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/inline:main").await;

    // Inline policy: deny viewing `schema:ssn` for everyone.
    //
    // We set `default-allow: true` so other properties remain visible:
    // default_allow only applies when *no* policies apply for a flake).
    // NOTE: Rust `opts.policy` expects **a policy object or array of policy objects**,
    // not a JSON-LD wrapper like `{"@graph":[...]}`.
    let policy = json!([{
        "@id": "ex:ssnRestriction",
        "f:required": true,
        // Use fully-expanded IRI here to avoid any namespace/term-resolution ambiguity.
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": ["?s", "?ssn"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:ssn": "?ssn"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree.ledger("policy/inline:main").await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Denying schema:ssn removes all solutions to a query that requires schema:ssn.
    assert_eq!(jsonld, json!([]));
}

#[tokio::test]
async fn policy_inline_denies_restricted_property_in_expansion() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/inline:main").await;

    // NOTE: Rust `opts.policy` expects **a policy object or array of policy objects**,
    // not a JSON-LD wrapper like `{"@graph":[...]}`.
    let policy = json!([{
        "@id": "ex:ssnRestriction",
        "f:required": true,
        // Use fully-expanded IRI here to avoid any namespace/term-resolution ambiguity.
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": policy,
            "default-allow": true
        },
        "select": { "?s": ["*"] },
        "where": { "@id": "?s", "@type": "ex:User" }
    });

    // Sanity check: flat selects should still work (default-allow allows all non-SSN predicates).
    let sanity = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": "policy/inline:main",
        "opts": {
            "policy": query["opts"]["policy"].clone(),
            "default-allow": true
        },
        "select": "?name",
        "where": { "@id": "?s", "@type": "ex:User", "schema:name": "?name" }
    });
    let sanity_result = fluree
        .query_connection(&sanity)
        .await
        .expect("sanity query_connection");
    let ledger = fluree.ledger("policy/inline:main").await.expect("ledger");
    let sanity_jsonld = sanity_result
        .to_jsonld(&ledger.snapshot)
        .expect("sanity to_jsonld");
    assert_eq!(
        normalize_rows(&sanity_jsonld),
        normalize_rows(&json!(["Alice", "John"]))
    );

    // Use the tracked connection query entrypoint, which performs **policy-aware**
    // expansion formatting.
    let tracked = fluree
        .query_connection_tracked(&query)
        .await
        .expect("query_connection_tracked");
    let jsonld = tracked.result;

    // In a crawl, `schema:ssn` is removed everywhere, while other fields remain.
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([
            {
                "@id": "ex:alice",
                "@type": "ex:User",
                "schema:name": "Alice",
                "schema:email": "alice@flur.ee",
                "schema:birthDate": "2022-08-17"
            },
            {
                "@id": "ex:john",
                "@type": "ex:User",
                "schema:name": "John",
                "schema:email": "john@flur.ee",
                "schema:birthDate": "2021-08-17"
            }
        ]))
    );
}

#[tokio::test]
async fn policy_per_source_override_takes_precedence_over_global() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let _ = seed_people_with_ssn(&fluree, "policy/per-source:main").await;

    // Query with global policy (default-allow: false) but per-source override (default-allow: true).
    // The per-source policy should take precedence, allowing data visibility.
    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/"
        },
        "from": {
            "@id": "policy/per-source:main",
            "policy": {
                "default-allow": true
            }
        },
        "opts": {
            "default-allow": false
        },
        "select": "?name",
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/per-source:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per-source policy (default-allow: true) should allow data visibility
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "John"]))
    );
}

#[tokio::test]
async fn policy_per_source_override_denies_when_global_allows() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    seed_people_with_ssn(&fluree, "policy/per-source-deny:main").await;

    // Per-source policy with an explicit deny rule for schema:name.
    // Global policy uses default-allow: true, but per-source has a deny rule.
    // The per-source policy should take precedence, denying the specific property.
    let deny_name_policy = json!([{
        "@id": "ex:nameRestriction",
        "f:required": true,
        "f:onProperty": [{"@id": "http://schema.org/name"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    let query = json!({
        "@context": {
            "ex": "http://example.org/ns/",
            "schema": "http://schema.org/",
            "f": "https://ns.flur.ee/db#"
        },
        "from": {
            "@id": "policy/per-source-deny:main",
            "policy": {
                "policy": deny_name_policy,
                "default-allow": true
            }
        },
        "opts": {
            "default-allow": true
        },
        "select": ["?name"],
        "where": {
            "@id": "?s",
            "@type": "ex:User",
            "schema:name": "?name"
        }
    });

    let result = fluree
        .query_connection(&query)
        .await
        .expect("query_connection");
    let ledger = fluree
        .ledger("policy/per-source-deny:main")
        .await
        .expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");

    // Per-source policy denies schema:name, so query returns empty
    assert_eq!(jsonld, json!([]));
}

/// V3 regression (hydration): an `f:onClass` view policy must be honored when a
/// subject is reached **only** through hydration (a direct-id select or
/// nested-ref expansion), not a WHERE scan. Such subjects are never scanned, so
/// the policy class cache is empty unless hydration populates it itself — before
/// the fix the onClass restriction silently dropped to `default_allow` and the
/// subject's data leaked.
#[tokio::test]
async fn policy_onclass_denies_hydration_only_subject() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/onclass-hydration:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let seed = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "@graph": [
            {"@id": "ex:alice", "@type": "ex:User",   "schema:name": "Alice"},
            {"@id": "ex:drBob", "@type": "ex:Doctor", "schema:name": "Roberta", "ex:secret": "TOPSECRET"}
        ]
    });
    fluree.insert(ledger0, &seed).await.unwrap();

    // Hide every flake of Doctor instances. f:onClass needs the subject's class
    // membership, which a hydration-only fetch must resolve itself.
    let policy = json!([{
        "@id": "ex:doctorHidden",
        "f:required": true,
        "f:onClass": [{"@id": "http://example.org/ns/Doctor"}],
        "f:action": "f:view",
        "f:allow": false
    }]);

    // Direct-id select of the Doctor — fetched purely via hydration, never scanned.
    let attack = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {"policy": policy.clone(), "default-allow": true},
        "select": {"ex:drBob": ["*"]}
    });
    let attack_result = fluree
        .query_connection_tracked(&attack)
        .await
        .expect("attack query_connection_tracked");
    let attack_json = attack_result.result.to_string();
    assert!(
        !attack_json.contains("TOPSECRET"),
        "Doctor's ex:secret leaked through hydration-only fetch: {}",
        attack_result.result
    );
    assert!(
        !attack_json.contains("Roberta"),
        "Doctor's name leaked through hydration-only fetch: {}",
        attack_result.result
    );

    // Control: a non-Doctor subject stays fully visible via the same path.
    let control = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {"policy": policy, "default-allow": true},
        "select": {"ex:alice": ["*"]}
    });
    let control_result = fluree
        .query_connection_tracked(&control)
        .await
        .expect("control query_connection_tracked");
    assert!(
        control_result.result.to_string().contains("Alice"),
        "non-Doctor subject must remain visible: {}",
        control_result.result
    );
}

/// Regression: inline `opts.policy` must merge when `opts.identity` is also
/// present. Identity-mode selection previously replaced — rather than
/// combined with — an explicitly supplied inline policy, which under
/// default-deny meant deny-all with no signal.
///
/// The identity node here has NO `f:policyClass`, so identity-mode selection
/// loads zero stored policies: every flake visible below is proof the inline
/// policy merged. The `f:query` rule additionally proves `?$identity` still
/// binds from the identity alongside inline policies.
#[tokio::test]
async fn inline_policy_merges_with_identity_only() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/inline-identity:main";
    let ledger = seed_people_with_ssn(&fluree, ledger_id).await;

    let identity = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:aliceIdentity",
        "ex:user": {"@id": "ex:alice"}
    });
    fluree
        .insert(ledger, &identity)
        .await
        .expect("insert identity");

    let policy = json!([
        {
            "@id": "ex:nameVisible",
            "f:onProperty": [{"@id": "http://schema.org/name"}],
            "f:action": "f:view",
            "f:allow": true
        },
        {
            "@id": "ex:ownSsnOnly",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:action": "f:view",
            "f:query": serde_json::to_string(&json!({
                "where": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            }))
            .unwrap()
        }
    ]);

    // Names: allowed for everyone via the inline static allow.
    let names = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {
            "policy": policy.clone(),
            "identity": "http://example.org/ns/aliceIdentity",
            "default-allow": false
        },
        "select": "?name",
        "where": {"@id": "?s", "schema:name": "?name"}
    });
    let result = fluree.query_connection(&names).await.expect("names query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!(["Alice", "John"])),
        "inline policy dropped: identity + inline opts.policy must merge"
    );

    // SSNs: the inline f:query rule allows only the identity's own user, so
    // ?$identity must be bound AND the inline policy must be in the set.
    let ssns = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {
            "policy": policy,
            "identity": "http://example.org/ns/aliceIdentity",
            "default-allow": false
        },
        "select": ["?s", "?ssn"],
        "where": {"@id": "?s", "schema:ssn": "?ssn"}
    });
    let result = fluree.query_connection(&ssns).await.expect("ssn query");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    let rendered = jsonld.to_string();
    assert!(
        rendered.contains("111-11-1111"),
        "own SSN must be visible via inline f:query rule: {rendered}"
    );
    assert!(
        !rendered.contains("888-88-8888"),
        "other user's SSN must stay hidden under default-deny: {rendered}"
    );
}

/// Inline `opts.policy` merges ON TOP of the stored policies selected by the
/// identity's `f:policyClass` — selection modes choose which stored policies
/// load, they never gate inline ones.
#[tokio::test]
async fn inline_policy_merges_with_identity_stored_policies() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/inline-identity-stored:main";
    let ledger = seed_people_with_ssn(&fluree, ledger_id).await;

    // Stored policy (selected via the identity's f:policyClass): names only.
    let setup = json!({
        "@context": {"ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#"},
        "@graph": [
            {
                "@id": "ex:namePolicy",
                "@type": ["f:AccessPolicy", "ex:AppPolicy"],
                "f:onProperty": [{"@id": "http://schema.org/name"}],
                "f:action": {"@id": "f:view"},
                "f:allow": true
            },
            {
                "@id": "ex:bobIdentity",
                "f:policyClass": [{"@id": "ex:AppPolicy"}]
            }
        ]
    });
    fluree.insert(ledger, &setup).await.expect("insert setup");

    // Inline policy adds SSN visibility on top of the stored set.
    let inline = json!([{
        "@id": "ex:ssnVisible",
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": true
    }]);

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {
            "policy": inline,
            "identity": "http://example.org/ns/bobIdentity",
            "default-allow": false
        },
        "select": ["?name", "?ssn"],
        "where": {"@id": "?s", "schema:name": "?name", "schema:ssn": "?ssn"}
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Alice", "111-11-1111"], ["John", "888-88-8888"]])),
        "stored (name) and inline (ssn) policies must both apply"
    );
}

/// Inline `opts.policy` merges when `policy-class` selects stored policies
/// (no identity). The class-only arm previously loaded stored policies XOR
/// parsed the inline policy.
#[tokio::test]
async fn inline_policy_merges_with_policy_class_only() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/inline-class:main";
    let ledger = seed_people_with_ssn(&fluree, ledger_id).await;

    let setup = json!({
        "@context": {"ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#"},
        "@graph": [{
            "@id": "ex:namePolicy",
            "@type": ["f:AccessPolicy", "ex:AppPolicy"],
            "f:onProperty": [{"@id": "http://schema.org/name"}],
            "f:action": {"@id": "f:view"},
            "f:allow": true
        }]
    });
    fluree.insert(ledger, &setup).await.expect("insert setup");

    let inline = json!([{
        "@id": "ex:ssnVisible",
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:allow": true
    }]);

    let query = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {
            "policy": inline,
            "policy-class": "http://example.org/ns/AppPolicy",
            "default-allow": false
        },
        "select": ["?name", "?ssn"],
        "where": {"@id": "?s", "schema:name": "?name", "schema:ssn": "?ssn"}
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let jsonld = result.to_jsonld(&ledger.snapshot).expect("to_jsonld");
    assert_eq!(
        normalize_rows(&jsonld),
        normalize_rows(&json!([["Alice", "111-11-1111"], ["John", "888-88-8888"]])),
        "class-selected (name) and inline (ssn) policies must both apply"
    );
}

/// The JSON-LD `ask` form is the preferred spelling of a policy condition —
/// identical semantics to the legacy `{"where": ...}` form (both are
/// existence checks).
#[tokio::test]
async fn jsonld_ask_condition_form() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/ask-form:main";
    let ledger = seed_people_with_ssn(&fluree, ledger_id).await;

    let identity = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "@id": "ex:aliceIdentity",
        "ex:user": {"@id": "ex:alice"}
    });
    fluree
        .insert(ledger, &identity)
        .await
        .expect("insert identity");

    let policy = json!([
        {
            "@id": "ex:nameVisible",
            "f:onProperty": [{"@id": "http://schema.org/name"}],
            "f:action": "f:view",
            "f:allow": true
        },
        {
            "@id": "ex:ownSsnOnly",
            "f:onProperty": [{"@id": "http://schema.org/ssn"}],
            "f:action": "f:view",
            "f:query": serde_json::to_string(&json!({
                "ask": {
                    "@id": "?$identity",
                    "http://example.org/ns/user": {"@id": "?$this"}
                }
            }))
            .unwrap()
        }
    ]);

    let ssns = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {
            "policy": policy,
            "identity": "http://example.org/ns/aliceIdentity",
            "default-allow": false
        },
        "select": ["?s", "?ssn"],
        "where": {"@id": "?s", "schema:ssn": "?ssn"}
    });
    let result = fluree.query_connection(&ssns).await.expect("ssn query");
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let rendered = result
        .to_jsonld(&ledger.snapshot)
        .expect("to_jsonld")
        .to_string();
    assert!(
        rendered.contains("111-11-1111"),
        "own SSN must be visible via ask-form condition: {rendered}"
    );
    assert!(
        !rendered.contains("888-88-8888"),
        "other user's SSN must stay hidden: {rendered}"
    );
}

/// A condition carrying BOTH `ask` and `where` is ambiguous and must fail
/// closed — the protected data never leaks.
#[tokio::test]
async fn jsonld_ask_and_where_together_fails_closed() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/ask-ambiguous:main";
    let _ = seed_people_with_ssn(&fluree, ledger_id).await;

    let policy = json!([{
        "@id": "ex:ambiguous",
        "f:onProperty": [{"@id": "http://schema.org/ssn"}],
        "f:action": "f:view",
        "f:query": serde_json::to_string(&json!({
            "ask": {"@id": "?$identity"},
            "where": {"@id": "?$identity"}
        }))
        .unwrap()
    }]);

    let ssns = json!({
        "@context": {"ex": "http://example.org/ns/", "schema": "http://schema.org/"},
        "from": ledger_id,
        "opts": {"policy": policy, "default-allow": false},
        "select": ["?s", "?ssn"],
        "where": {"@id": "?s", "schema:ssn": "?ssn"}
    });
    match fluree.query_connection(&ssns).await {
        Err(_) => {} // condition error surfacing as a query error is fail-closed
        Ok(result) => {
            let ledger = fluree.ledger(ledger_id).await.expect("ledger");
            let rendered = result
                .to_jsonld(&ledger.snapshot)
                .expect("to_jsonld")
                .to_string();
            assert!(
                !rendered.contains("111-11-1111") && !rendered.contains("888-88-8888"),
                "ambiguous ask+where condition must not leak protected data: {rendered}"
            );
        }
    }
}
