//! Write-path policy enforcement driven by the ledger's `#config` graph.
//!
//! `build_transact_policy_context` is the write-side counterpart of
//! `wrap_policy`: it merges config policy defaults (`f:policyClass`,
//! `f:defaultAllow`) into the request's governance options and resolves
//! `f:policySource` — same-ledger named graphs AND cross-ledger model
//! references — before building the `PolicyContext` a transaction stages
//! under. The consensus transact path (local + Raft), credential transact,
//! push, and the CLI all route through it.
//!
//! Before this existed, writes built policy exclusively from request
//! inputs against the default graph: config-declared policy was enforced
//! on reads but silently ignored on writes (issue #1416).

#![cfg(feature = "native")]

use crate::support::{assert_index_defaults, genesis_ledger};
use fluree_db_api::{
    build_transact_policy_context, CommitOpts, FlureeBuilder, GovernanceOptions, IndexConfig,
    TxnOpts,
};
use serde_json::json;

fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

fn test_index_config() -> IndexConfig {
    IndexConfig {
        reindex_min_bytes: 100_000,
        reindex_max_bytes: 1_000_000_000,
    }
}

/// No config, no request inputs → the transaction runs under root
/// (`None`), preserving the pre-existing behavior for unconfigured
/// ledgers.
#[tokio::test]
async fn no_config_no_inputs_yields_root() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "policy/write-root:main");

    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &GovernanceOptions::default(),
    )
    .await
    .expect("build");
    assert!(
        ctx.is_none(),
        "no config + no request inputs must run under root"
    );
}

/// Config-declared policy defaults (`f:policyClass` + `f:defaultAllow`)
/// govern writes even when the request carries NO policy inputs. The
/// policy rules live in the default graph; only the defaults-merge is
/// under test here.
#[tokio::test]
async fn config_policy_class_defaults_enforced_on_writes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/write-config-defaults:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Seed data (registers ex: namespace) plus the write policy itself in
    // the default graph: deny modifying ex:ssn, typed ex:WritePolicy.
    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/ns/", "f": "https://ns.flur.ee/db#"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:ssn": "111-11-1111"},
                    {
                        "@id": "ex:noSsnWrite",
                        "@type": "ex:WritePolicy",
                        "f:required": true,
                        "f:onProperty": {"@id": "ex:ssn"},
                        "f:action": {"@id": "f:modify"},
                        "f:allow": false
                    }
                ]
            }),
        )
        .await
        .expect("seed data + policy");

    // Config: defaultAllow=true so ONLY the ex:ssn rule blocks anything;
    // policyClass opts the ex:WritePolicy-typed rule in.
    let config_iri = config_graph_iri(ledger_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass ex:WritePolicy .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed config");
    let ledger = r2.ledger;

    // Empty request opts: everything comes from config.
    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &GovernanceOptions::default(),
    )
    .await
    .expect("build")
    .expect("config policyClass must produce a policy context for writes");

    let cfg = test_index_config();
    let denied_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:ssn \"999-99-9999\" .\n";
    let denied = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            denied_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        denied.is_err(),
        "config-declared modify-deny on ex:ssn must reject the write, got: {denied:?}"
    );

    let allowed_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:name \"Bob\" .\n";
    let allowed = fluree
        .insert_turtle_with_opts(
            ledger,
            allowed_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "defaultAllow=true must let unrelated writes through, got: {:?}",
        allowed.err()
    );
}

/// A same-ledger `f:policySource` pointing at a named graph: the write
/// path must load policy rules from THAT graph (previously it hardcoded
/// the default graph, where no rules exist, and allowed everything).
#[tokio::test]
async fn config_policy_source_named_graph_enforced_on_writes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/write-named-graph-source:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    // Seed data in the default graph (no policy rules there).
    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:ssn": "111-11-1111"
            }),
        )
        .await
        .expect("seed data");

    // Policy rules live exclusively in a named graph; config redirects
    // the policy-rule lookup there via f:policySource.
    let policy_graph_iri = "http://example.org/d-policies";
    let config_iri = config_graph_iri(ledger_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:noSsnWrite
                    rdf:type     ex:WritePolicy ;
                    f:required   true ;
                    f:onProperty ex:ssn ;
                    f:action     f:modify ;
                    f:allow      false .
            }}

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass ex:WritePolicy .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed policy graph + config");
    let ledger = r2.ledger;

    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &GovernanceOptions::default(),
    )
    .await
    .expect("build")
    .expect("config with f:policySource must produce a policy context");

    let cfg = test_index_config();
    let denied_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:ssn \"999-99-9999\" .\n";
    let denied = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            denied_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        denied.is_err(),
        "modify-deny loaded from the f:policySource graph must reject the write, got: {denied:?}"
    );

    // Root control: the identical write with no policy context succeeds,
    // proving the rejection above came from the named-graph rules.
    let ok = fluree
        .insert_turtle_with_opts(
            ledger,
            denied_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            None,
        )
        .await;
    assert!(
        ok.is_ok(),
        "root write must succeed without the policy context, got: {:?}",
        ok.err()
    );
}

/// A same-ledger `f:policySource` with an *invalid* graph selector must
/// fail closed on writes even when the request carries no policy inputs
/// and config declares no `f:defaultAllow`/`f:policyClass`. The read path
/// (`wrap_policy`) resolves the selector unconditionally and errors on an
/// unknown graph; the write builder must do the same rather than silently
/// running under root — the read/write divergence this path eliminates.
#[tokio::test]
async fn config_invalid_policy_source_fails_closed_on_writes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "policy/write-invalid-source:main";
    let ledger0 = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger0,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User"
            }),
        )
        .await
        .expect("seed data");

    // Config declares ONLY a policySource whose graph selector points at a
    // graph that does not exist in this ledger's registry. No policyClass
    // or defaultAllow — so the request has no policy inputs and the pre-fix
    // no-inputs shortcut would have returned Ok(None) (root) before ever
    // validating the selector.
    let missing_graph_iri = "http://example.org/nonexistent-policy-graph";
    let config_iri = config_graph_iri(ledger_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:graphSelector <{missing_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed config");
    let ledger = r2.ledger;

    let result = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &GovernanceOptions::default(),
    )
    .await;

    assert!(
        result.is_err(),
        "an invalid same-ledger f:policySource must fail closed on writes, got: {result:?}"
    );
}

/// Cross-ledger `f:policySource`: model ledger M holds the policy rules;
/// data ledger D's config points at M. A write to D that violates M's
/// modify rules must be rejected — with NO policy inputs on the request.
/// This is the write-side counterpart of the read-path enforcement in
/// `it_policy_cross_ledger.rs`.
#[tokio::test]
async fn cross_ledger_policy_source_enforced_on_writes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    // --- model ledger M: modify-deny on ex:ssn in a named policy graph
    let model_id = "policy/write-xledger/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/m-policies";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:noSsnWrite
                    rdf:type     f:AccessPolicy ;
                    f:required   true ;
                    f:onProperty ex:ssn ;
                    f:action     f:modify ;
                    f:allow      false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M policy graph");

    // --- data ledger D: data + cross-ledger config, no policy IRIs in D
    let data_id = "policy/write-xledger/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@id": "ex:alice",
                "@type": "ex:User",
                "ex:ssn": "111-11-1111"
            }),
        )
        .await
        .expect("seed D data");

    let config_iri = config_graph_iri(data_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger config");
    let ledger = r2.ledger;

    // Empty request opts: a cross-ledger source must still build a
    // context — M's rules govern D regardless of request inputs (the
    // default f:AccessPolicy class filter applies).
    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &GovernanceOptions::default(),
    )
    .await
    .expect("build")
    .expect("cross-ledger f:policySource must produce a policy context for writes");

    let cfg = test_index_config();
    let denied_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:ssn \"999-99-9999\" .\n";
    let denied = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            denied_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        denied.is_err(),
        "M's modify-deny on ex:ssn must reject the write to D, got: {denied:?}"
    );

    let allowed_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:name \"Bob\" .\n";
    let allowed = fluree
        .insert_turtle_with_opts(
            ledger,
            allowed_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "defaultAllow=true must let writes M's rules don't target through, got: {:?}",
        allowed.err()
    );
}

/// Identity-mode + cross-ledger `f:policySource` fails closed on the
/// write builder, matching the read-path Phase 1a contract.
#[tokio::test]
async fn cross_ledger_plus_identity_fails_closed_on_writes() {
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "policy/write-xledger-id/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/m-policies";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:rule1 rdf:type f:AccessPolicy ; f:action f:modify ; f:allow true .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M");

    let data_id = "policy/write-xledger-id/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let config_iri = config_graph_iri(data_id);
    let r1 = fluree
        .stage_owned(data)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D config");
    let ledger = r1.ledger;

    let opts = GovernanceOptions {
        identity: Some("http://example.org/users/alice".into()),
        ..Default::default()
    };
    let err = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await
    .expect_err("identity + cross-ledger must fail closed on the write builder");

    let msg = err.to_string();
    assert!(
        msg.contains("identity") && msg.contains("cross-ledger"),
        "expected fail-closed diagnostic mentioning both, got: {msg}"
    );
}

/// Identity + cross-ledger `f:policySource` works when a policy class is
/// available (here from D's config): M's rules load via the class filter,
/// the identity is bind-only, and enforcement applies. This is the common
/// authenticated-deployment case — previously it failed closed even though
/// the config named the governing class.
#[tokio::test]
async fn cross_ledger_identity_with_config_policy_class_enforced_on_writes() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "policy/write-xledger-idclass/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/m-policies";
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:noSsnWrite
                    rdf:type     f:AccessPolicy ;
                    f:required   true ;
                    f:onProperty ex:ssn ;
                    f:action     f:modify ;
                    f:allow      false .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed M policy graph");

    let data_id = "policy/write-xledger-idclass/data:main";
    let data = genesis_ledger(&fluree, data_id);
    // The identity must exist as a subject in D for ?$identity binding.
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:ssn": "111-11-1111"},
                    {"@id": "ex:aliceIdentity", "ex:user": {"@id": "ex:alice"}}
                ]
            }),
        )
        .await
        .expect("seed D data + identity");

    let config_iri = config_graph_iri(data_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass f:AccessPolicy .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger config with policyClass");
    let ledger = r2.ledger;

    // Identity-carrying request: must build (not fail closed) because the
    // config's f:policyClass selects M's rules; the identity binds only.
    //
    // default_allow is set on the request: identity counts as a policy
    // input, so under the default f:OverrideAll the request's options take
    // precedence and the config's f:defaultAllow is NOT merged (same
    // long-standing contract as same-ledger reads). Operators who want the
    // config to always win set f:overrideControl accordingly.
    let opts = GovernanceOptions {
        identity: Some("http://example.org/ns/aliceIdentity".into()),
        default_allow: Some(true),
        ..Default::default()
    };
    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await
    .expect("identity + config policyClass must not fail closed")
    .expect("cross-ledger source must produce a policy context");

    let cfg = test_index_config();
    let denied_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:ssn \"999-99-9999\" .\n";
    let denied = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            denied_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        denied.is_err(),
        "M's modify-deny on ex:ssn must reject the identity-carrying write, got: {denied:?}"
    );

    let allowed_turtle = "@prefix ex: <http://example.org/ns/> .\nex:bob ex:name \"Bob\" .\n";
    let allowed = fluree
        .insert_turtle_with_opts(
            ledger,
            allowed_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        allowed.is_ok(),
        "defaultAllow=true must let untargeted writes through, got: {:?}",
        allowed.err()
    );
}

/// The `?$identity` binding actually drives f:query rules from M: an
/// owner-only modify rule in the model ledger allows the identity to write
/// its own user's email and rejects writes to anyone else's. This pins the
/// bind-only contract — the identity resolves in D and feeds `?$identity`,
/// while the rule itself lives exclusively in M.
#[tokio::test]
async fn cross_ledger_identity_binding_drives_fquery_modify_rule() {
    assert_index_defaults();
    let fluree = FlureeBuilder::memory().build_memory();

    let model_id = "policy/write-xledger-fquery/model:main";
    let model = genesis_ledger(&fluree, model_id);
    let policy_graph_iri = "http://example.org/m-policies";
    // Full IRIs inside f:query — it executes against D, where prefixed
    // names from M's turtle context wouldn't expand.
    let owner_query =
        r#"{"where": {"@id": "?$identity", "http://example.org/ns/user": {"@id": "?$this"}}}"#;
    fluree
        .stage_owned(model)
        .upsert_turtle(&format!(
            r#"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex:  <http://example.org/ns/> .

            GRAPH <{policy_graph_iri}> {{
                ex:ownerEmailOnly
                    rdf:type     f:AccessPolicy ;
                    f:required   true ;
                    f:onProperty ex:email ;
                    f:action     f:modify ;
                    f:query      """{owner_query}""" .
            }}
        "#
        ))
        .execute()
        .await
        .expect("seed M owner-only f:query rule");

    let data_id = "policy/write-xledger-fquery/data:main";
    let data = genesis_ledger(&fluree, data_id);
    let r1 = fluree
        .insert(
            data,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:email": "alice@flur.ee"},
                    {"@id": "ex:bob",   "ex:email": "bob@flur.ee"},
                    {"@id": "ex:aliceIdentity", "ex:user": {"@id": "ex:alice"}}
                ]
            }),
        )
        .await
        .expect("seed D users + identity");

    let config_iri = config_graph_iri(data_id);
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&format!(
            r"
            @prefix f:   <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:cfg:main> rdf:type f:LedgerConfig .
                <urn:cfg:main> f:policyDefaults <urn:cfg:policy> .
                <urn:cfg:policy> f:defaultAllow true .
                <urn:cfg:policy> f:policyClass f:AccessPolicy .
                <urn:cfg:policy> f:policySource <urn:cfg:policy-ref> .
                <urn:cfg:policy-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:cfg:policy-src> .
                <urn:cfg:policy-src> f:ledger <{model_id}> ;
                                     f:graphSelector <{policy_graph_iri}> .
            }}
        "
        ))
        .execute()
        .await
        .expect("seed D cross-ledger config");
    let ledger = r2.ledger;

    let opts = GovernanceOptions {
        identity: Some("http://example.org/ns/aliceIdentity".into()),
        ..Default::default()
    };
    let ctx = build_transact_policy_context(
        &fluree,
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await
    .expect("build")
    .expect("cross-ledger source must produce a policy context");

    let cfg = test_index_config();
    // aliceIdentity owns ex:alice → writing alice's email matches the
    // ?$identity → ex:user → ?$this chain and is allowed.
    let own_turtle =
        "@prefix ex: <http://example.org/ns/> .\nex:alice ex:email \"new-alice@flur.ee\" .\n";
    let own = fluree
        .insert_turtle_with_opts(
            ledger.clone(),
            own_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        own.is_ok(),
        "identity must be able to write its own user's email via M's f:query rule, got: {:?}",
        own.err()
    );

    // bob is not aliceIdentity's user → the required rule's f:query binds
    // nothing → rejected.
    let other_turtle =
        "@prefix ex: <http://example.org/ns/> .\nex:bob ex:email \"hacked@flur.ee\" .\n";
    let other = fluree
        .insert_turtle_with_opts(
            ledger,
            other_turtle,
            TxnOpts::default(),
            CommitOpts::default(),
            &cfg,
            Some(&ctx),
        )
        .await;
    assert!(
        other.is_err(),
        "identity must NOT be able to write another user's email, got: {other:?}"
    );
}
