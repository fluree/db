//! Config graph integration tests
//!
//! End-to-end coverage that the config graph (g_id=2) is reserved/readable,
//! config can be written via TriG, defaults apply automatically, per-graph
//! overrides work, override control blocks/permits query-time overrides,
//! and config is time-travel consistent.

use crate::support;
use crate::support::genesis_ledger;
use fluree_db_api::config_resolver;
use fluree_db_api::{FlureeBuilder, GovernanceOptions, QueryExecutionOptions, VerifiedIdentity};
use serde_json::json;

/// Build the config graph IRI for a canonical ledger id.
fn config_graph_iri(ledger_id: &str) -> String {
    format!("urn:fluree:{ledger_id}#config")
}

/// Commit a JSON-LD insert without the API's post-staging checks (SHACL,
/// uniqueness, the config guard): the way history written before those
/// checks existed reaches a ledger. Seeds degenerate config state.
async fn commit_unchecked(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    doc: &serde_json::Value,
) -> fluree_db_api::LedgerState {
    let ledger_id = ledger.snapshot.ledger_id.to_string();
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::parse_transaction(
        doc,
        fluree_db_transact::TxnType::Insert,
        fluree_db_transact::TxnOpts::default(),
        &mut ns,
        &ledger_id,
    )
    .expect("parse");
    let (view, ns) =
        fluree_db_transact::stage(ledger, txn, ns, fluree_db_transact::StageOptions::new())
            .await
            .expect("stage");
    fluree
        .commit_staged(
            view,
            ns,
            &fluree.default_index_config(),
            fluree_db_api::CommitOpts::default(),
        )
        .await
        .expect("commit")
        .1
}

/// The codes of a ledger's `configDiagnostics` (ledger info), sorted.
async fn config_diagnostic_codes(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> Vec<String> {
    let info = fluree
        .ledger_info(ledger_id)
        .execute()
        .await
        .expect("ledger info");
    let mut codes: Vec<String> = info
        .get("configDiagnostics")
        .and_then(serde_json::Value::as_array)
        .map(|d| {
            d.iter()
                .filter_map(|d| d["code"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    codes.sort();
    codes
}

/// A config with two `f:shaclDefaults` values: the group whose IRI sorts first
/// is empty in `#config` (its field was written to the default graph, the
/// #1979 shape), the other is the corrected group. Written without the config
/// guard, as a config re-inserted before the guard existed would be.
async fn seed_double_shacl_pointer(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
) -> fluree_db_api::LedgerState {
    commit_unchecked(
        fluree,
        ledger,
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [
                {
                    "@id": "urn:it:cfg:ledger",
                    "@type": "f:LedgerConfig",
                    "@graph": "config",
                    "f:shaclDefaults": [
                        {"@id": "urn:it:cfg:a-empty"},
                        {"@id": "urn:it:cfg:b-full", "f:shaclEnabled": true}
                    ]
                },
                {"@id": "urn:it:cfg:a-empty", "f:shaclEnabled": true}
            ]
        }),
    )
    .await
}

/// A double group pointer resolves deterministically to the non-empty group
/// (lowest IRI among the non-empty ones). The first binding used to win,
/// which here is the empty group: SHACL read as disabled.
#[tokio::test]
async fn reader_picks_the_non_empty_group_of_a_double_pointer() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-double-pointer:main";
    seed_double_shacl_pointer(&fluree, genesis_ledger(&fluree, ledger_id)).await;
    let view = fluree.db(ledger_id).await.unwrap();
    let shacl = view
        .ledger_config()
        .and_then(|c| c.shacl.clone())
        .expect("a SHACL group is read");
    assert_eq!(shacl.enabled, Some(true), "the populated group wins");
}

/// Ledger info reports the degenerate state: the ambiguous pointer, the
/// empty group, and the fields stranded in the default graph.
#[tokio::test]
async fn diagnostics_report_empty_stranded_and_ambiguous_config() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics:main";
    seed_double_shacl_pointer(&fluree, genesis_ledger(&fluree, ledger_id)).await;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["ambiguous-pointer", "empty-group", "stranded-fields"]
    );
}

/// The query group is diagnosed like every setting group: one whose
/// `f:unionDefaultGraph` was written outside the config graph reads as empty,
/// with its field stranded.
#[tokio::test]
async fn diagnostics_report_an_empty_query_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-query:main";
    commit_unchecked(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [
                {
                    "@id": "urn:it:cfg:ledger",
                    "@type": "f:LedgerConfig",
                    "@graph": "config",
                    "f:queryDefaults": {"@id": "urn:it:cfg:query"}
                },
                {"@id": "urn:it:cfg:query", "f:unionDefaultGraph": true}
            ]
        }),
    )
    .await;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["empty-group", "stranded-fields"]
    );
    let view = fluree.db(ledger_id).await.unwrap();
    assert!(
        view.ledger_config().and_then(|c| c.query.clone()).is_none(),
        "an empty query group reads as absent"
    );
}

/// Shapes with no config enabling SHACL are reported ("shapes present; SHACL
/// enforcement not configured"); enabling SHACL on the existing config
/// subject clears it.
#[tokio::test]
async fn diagnostics_report_shapes_without_enforcement() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-shapes:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"sh": "http://www.w3.org/ns/shacl#", "ex": "http://example.org/"},
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"}
            }),
        )
        .await
        .unwrap()
        .ledger;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["shacl-not-configured"]
    );

    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:it:cfg:ledger",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:shaclDefaults": {"f:shaclEnabled": true}
            }),
        )
        .await
        .unwrap();
    assert!(config_diagnostic_codes(&fluree, ledger_id).await.is_empty());

    // An untyped shape (a target predicate and no `sh:NodeShape` type) is a
    // shape the compiler enforces where SHACL is on, so it is reported too.
    let ledger_id = "it/config-diagnostics-untyped-shape:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"sh": "http://www.w3.org/ns/shacl#", "ex": "http://example.org/"},
                "@id": "ex:PersonShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": {"sh:path": {"@id": "ex:name"}, "sh:minCount": 1}
            }),
        )
        .await
        .unwrap()
        .ledger;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["shacl-not-configured"]
    );

    // SHACL switched off in the config is the ledger's posture, not a gap.
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:it:cfg:ledger",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:shaclDefaults": {"f:shaclEnabled": false}
            }),
        )
        .await
        .unwrap();
    assert!(config_diagnostic_codes(&fluree, ledger_id).await.is_empty());
}

/// SHACL enabled for one graph only is configured: the shapes are enforced
/// there, so no "not configured" diagnostic is reported.
#[tokio::test]
async fn diagnostics_count_shacl_enabled_for_one_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-graph-shacl:main";
    fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {
                    "f": "https://ns.flur.ee/db#",
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/"
                },
                "@graph": [
                    {
                        "@id": "ex:PersonShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": {"@id": "ex:Person"}
                    },
                    {
                        "@id": "urn:it:cfg:ledger",
                        "@type": "f:LedgerConfig",
                        "@graph": "config",
                        "f:graphOverrides": {
                            "@id": "urn:it:cfg:scratch",
                            "@type": "f:GraphConfig",
                            "f:targetGraph": {"@id": "http://example.org/scratch"},
                            "f:shaclDefaults": {"f:shaclEnabled": true}
                        }
                    }
                ]
            }),
        )
        .await
        .unwrap();
    assert!(config_diagnostic_codes(&fluree, ledger_id).await.is_empty());
}

/// Two graph overrides for one graph: the reader uses one of them, and ledger
/// info says so.
#[tokio::test]
async fn diagnostics_report_duplicate_graph_overrides() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-dup-override:main";
    let cfg = config_graph_iri(ledger_id);
    fluree
        .stage_owned(genesis_ledger(&fluree, ledger_id))
        .upsert_turtle(&format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{cfg}> {{
                <urn:config:main> rdf:type f:LedgerConfig ;
                    f:graphOverrides <urn:config:a>, <urn:config:b> .
                <urn:config:a> rdf:type f:GraphConfig ;
                    f:targetGraph <http://example.org/scratch> ;
                    f:shaclDefaults <urn:config:a-shacl> .
                <urn:config:a-shacl> f:shaclEnabled true .
                <urn:config:b> rdf:type f:GraphConfig ;
                    f:targetGraph <http://example.org/scratch> ;
                    f:shaclDefaults <urn:config:b-shacl> .
                <urn:config:b-shacl> f:shaclEnabled false .
            }}
        "
        ))
        .execute()
        .await
        .expect("config write");
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["duplicate-graph-override"]
    );
}

/// A clean config has no diagnostics (the field is omitted).
#[tokio::test]
async fn diagnostics_are_empty_for_a_clean_config() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-clean:main";
    fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:it:cfg:ledger",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:policyDefaults": {"f:defaultAllow": true}
            }),
        )
        .await
        .unwrap();
    assert!(config_diagnostic_codes(&fluree, ledger_id).await.is_empty());
}

// =============================================================================
// Config guard: degenerate config writes are refused
// =============================================================================

/// The message of a config write the guard refused, checked to render as a
/// parse error (the class clients map to a 400).
fn config_guard_refusal(err: fluree_db_api::ApiError) -> String {
    match err {
        fluree_db_api::ApiError::Transact(e @ fluree_db_transact::TransactError::Parse(_)) => {
            let message = e.to_string();
            assert!(message.starts_with("Parse error: "), "{message}");
            message
        }
        other => panic!("expected a config guard refusal, got {other:?}"),
    }
}

/// A config group whose fields land outside the config graph reads as empty,
/// so the write is refused, on each surface that can split one: a SPARQL blank
/// node shared across a GRAPH boundary, a TriG group named in the config graph
/// with its fields in the default graph, and a JSON-LD group explicitly scoped
/// to the default graph.
#[tokio::test]
async fn config_guard_refuses_a_group_split_across_graphs() {
    let ledger_id = "it/config-guard-split:main";
    let cfg = config_graph_iri(ledger_id);

    let fluree = FlureeBuilder::memory().build_memory();
    let err = sparql_update(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#> \
             INSERT DATA {{ \
               GRAPH <{cfg}> {{ <urn:config:main> a f:LedgerConfig ; f:shaclDefaults _:g }} \
               _:g f:shaclEnabled true \
             }}"
        ),
    )
    .await
    .expect_err("SPARQL: a blank group split across graphs");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:shaclDefaults")
            && message.contains("f:shaclEnabled")
            && message.contains("the default graph"),
        "{message}"
    );

    let fluree = FlureeBuilder::memory().build_memory();
    let err = fluree
        .stage_owned(genesis_ledger(&fluree, ledger_id))
        .upsert_turtle(&format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{cfg}> {{
                <urn:config:main> rdf:type f:LedgerConfig ;
                                  f:shaclDefaults <urn:config:shacl> .
            }}
            <urn:config:shacl> f:shaclEnabled true .
        "
        ))
        .execute()
        .await
        .expect_err("TriG: the group's fields outside the config graph");
    config_guard_refusal(err);

    let fluree = FlureeBuilder::memory().build_memory();
    let err = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:shaclDefaults": {
                    "@id": "urn:config:shacl",
                    "@graph": "default",
                    "f:shaclEnabled": true
                }
            }),
        )
        .await
        .expect_err("JSON-LD: a group explicitly scoped out of the config graph");
    config_guard_refusal(err);
}

/// The same split cannot be reached in two accepted writes, in either order:
/// the edge first and the fields later outside the config graph (the #1979
/// shape: a policy group's `f:defaultAllow false` lost), or the fields first
/// outside it and the edge later. Moving the fields into the config graph in
/// the transaction that adds the edge is accepted.
#[tokio::test]
async fn config_guard_refuses_a_group_split_across_transactions() {
    let ledger_id = "it/config-guard-split-txns:main";
    let cfg = config_graph_iri(ledger_id);
    let f_ctx = json!({"f": "https://ns.flur.ee/db#"});

    // Edge first, fields later.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = sparql_update(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#> \
             INSERT DATA {{ GRAPH <{cfg}> {{ \
               <urn:config:main> a f:LedgerConfig ; f:policyDefaults <urn:config:policy> \
             }} }}"
        ),
    )
    .await
    .expect("the edge alone")
    .ledger;
    let err = fluree
        .insert(
            ledger,
            &json!({"@context": f_ctx, "@id": "urn:config:policy", "f:defaultAllow": false}),
        )
        .await
        .expect_err("the group's fields outside the config graph");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:policyDefaults")
            && message.contains("f:defaultAllow")
            && message.contains("the default graph"),
        "{message}"
    );

    // Fields first, edge later.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({"@context": f_ctx, "@id": "urn:config:shacl", "f:shaclEnabled": true}),
        )
        .await
        .expect("f: data in the default graph, not yet part of any config")
        .ledger;
    let edge = format!(
        "PREFIX f: <https://ns.flur.ee/db#> \
         INSERT DATA {{ GRAPH <{cfg}> {{ \
           <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> \
         }} }}"
    );
    let err = sparql_update(&fluree, ledger.clone(), &edge)
        .await
        .expect_err("an edge to a group whose fields are in the default graph");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:shaclDefaults")
            && message.contains("f:shaclEnabled")
            && message.contains("the default graph"),
        "{message}"
    );

    // The same edge, moving the fields into the config graph with it.
    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#> \
             DELETE {{ <urn:config:shacl> f:shaclEnabled true }} \
             INSERT {{ GRAPH <{cfg}> {{ \
               <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> . \
               <urn:config:shacl> f:shaclEnabled true \
             }} }} WHERE {{}}"
        ),
    )
    .await
    .expect("the repair moves the fields with the edge")
    .ledger;
    let view = fluree.db(ledger_id).await.unwrap();
    assert_eq!(
        view.ledger_config()
            .and_then(|c| c.shacl.clone())
            .and_then(|s| s.enabled),
        Some(true)
    );
    let _ = ledger;
}

// -----------------------------------------------------------------------------
// The guard on branch operations: they re-check the target's config groups.
// -----------------------------------------------------------------------------

/// The message of a branch operation the guard refused: a conflict with the
/// target's state, naming the group and the repair.
fn branch_op_refusal(err: fluree_db_api::ApiError, group: &str) -> String {
    assert_eq!(err.status_code(), 409, "{err}");
    let message = match err {
        fluree_db_api::ApiError::BranchConflict(message) => message,
        other => panic!("expected a branch conflict, got {other:?}"),
    };
    assert!(message.contains(group), "{message}");
    assert!(
        message.contains("Repairing a config split across graphs"),
        "{message}"
    );
    message
}

/// A ledger `name` with one data node, and a `dev` branch of it.
async fn ledger_with_branch(
    fluree: &fluree_db_api::Fluree,
    name: &str,
) -> fluree_db_api::LedgerState {
    let ledger = fluree.create_ledger(name).await.unwrap();
    let main = fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:seed", "ex:p": 1}),
        )
        .await
        .unwrap()
        .ledger;
    fluree.create_branch(name, "dev", None, None).await.unwrap();
    main
}

/// A config linking the SHACL group `urn:config:shacl`, which has no fields
/// yet.
fn config_linking_the_shacl_group() -> serde_json::Value {
    json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "urn:config:main",
        "@type": "f:LedgerConfig",
        "@graph": "config",
        "f:shaclDefaults": {"@id": "urn:config:shacl"}
    })
}

/// `urn:config:shacl f:shaclEnabled true`, written to the default graph.
fn shacl_group_field_in_the_default_graph() -> serde_json::Value {
    json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@id": "urn:config:shacl",
        "f:shaclEnabled": true
    })
}

/// Config written on a branch is in that branch's own config graph
/// (`urn:fluree:<name>:<branch>#config`) and configures that branch only. A
/// merge or fast-forward carrying it to another branch is refused, naming the
/// branch and the graph, rather than landing it in an ordinary graph of the
/// target where it would never be read; the preview reports it. Merging the
/// other way, from the branch a branch was created from, is not refused:
/// that parent's config graph is already an ordinary graph on the branch.
#[tokio::test]
async fn a_merge_carrying_config_written_on_another_branch_is_refused() {
    let edits = [
        // A whole config of the branch's own.
        "<urn:config:dev> a f:LedgerConfig ; f:shaclDefaults <urn:config:devshacl> .
         <urn:config:devshacl> f:shaclEnabled false .",
        // A setting on a group main's config links.
        "<urn:config:shacl> f:validationMode f:ValidationWarn .",
    ];
    for (i, edit) in edits.into_iter().enumerate() {
        for fast_forward in [true, false] {
            let fluree = FlureeBuilder::memory().build_memory();
            let name = format!("it-config-branch-own-{i}-{fast_forward}");
            let ledger = fluree.create_ledger(&name).await.unwrap();
            let main = fluree
                .stage_owned(ledger)
                .upsert_turtle(&format!(
                    "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
                     ex:seed ex:p 1 .
                     GRAPH <{}> {{ <urn:config:main> a f:LedgerConfig ;
                       f:shaclDefaults <urn:config:shacl> . <urn:config:shacl> f:shaclEnabled true . }}",
                    config_graph_iri(&format!("{name}:main"))
                ))
                .execute()
                .await
                .expect("main's config")
                .ledger;
            fluree
                .create_branch(&name, "dev", None, None)
                .await
                .unwrap();
            let dev_cfg = config_graph_iri(&format!("{name}:dev"));
            let dev = fluree.ledger(&format!("{name}:dev")).await.unwrap();
            fluree
                .stage_owned(dev)
                .upsert_turtle(&format!(
                    "@prefix f: <https://ns.flur.ee/db#> . GRAPH <{dev_cfg}> {{ {edit} }}"
                ))
                .execute()
                .await
                .expect("dev configures itself");
            if !fast_forward {
                fluree
                    .insert(main, &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:x", "ex:p": 2}))
                    .await
                    .unwrap();
            }
            let preview = fluree
                .merge_preview_with(
                    &name,
                    "dev",
                    None,
                    fluree_db_api::MergePreviewOpts::default(),
                )
                .await
                .expect("preview");
            assert!(
                !preview.mergeable,
                "{i}/{fast_forward}: the preview refuses"
            );
            let err = fluree
                .merge_branch(
                    &name,
                    "dev",
                    None,
                    fluree_db_api::ConflictStrategy::default(),
                )
                .await
                .expect_err("a merge carrying dev's config");
            assert_eq!(err.status_code(), 409, "{i}/{fast_forward}: {err}");
            let message = err.to_string();
            assert!(
                message.contains("branch dev's config graph") && message.contains(&dev_cfg),
                "{i}/{fast_forward}: {message}"
            );
            let view = fluree.db(&format!("{name}:main")).await.unwrap();
            assert_eq!(
                view.ledger_config()
                    .and_then(|c| c.shacl.as_ref())
                    .and_then(|s| s.enabled),
                Some(true),
                "{i}/{fast_forward}: main's config is untouched"
            );
        }
    }

    // From the parent: main's config change after the fork merges into dev.
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it-config-branch-parent";
    let main = ledger_with_branch(&fluree, name).await;
    let dev = fluree.ledger(&format!("{name}:dev")).await.unwrap();
    fluree
        .insert(
            dev,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:d", "ex:p": 1}),
        )
        .await
        .unwrap();
    fluree
        .stage_owned(main)
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <{}> {{ <urn:config:main> a f:LedgerConfig ;
               f:shaclDefaults <urn:config:shacl> . <urn:config:shacl> f:shaclEnabled true . }}",
            config_graph_iri(&format!("{name}:main"))
        ))
        .execute()
        .await
        .expect("main's config");
    fluree
        .merge_branch(
            name,
            "main",
            Some("dev"),
            fluree_db_api::ConflictStrategy::default(),
        )
        .await
        .expect("main's history merges into dev");

    // Plain data a branch wrote to its own config graph is refused the same
    // way, also into a target whose config governs no writes, where a
    // fast-forward is checked only when its commits carry config settings.
    for fast_forward in [true, false] {
        let fluree = FlureeBuilder::memory().build_memory();
        let name = format!("it-config-branch-own-data-{fast_forward}");
        let ledger = fluree.create_ledger(&name).await.unwrap();
        let main = fluree
            .insert(
                ledger,
                &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:seed", "ex:p": 1}),
            )
            .await
            .unwrap()
            .ledger;
        fluree
            .create_branch(&name, "dev", None, None)
            .await
            .unwrap();
        let dev_cfg = config_graph_iri(&format!("{name}:dev"));
        fluree
            .graph(&format!("{name}:dev"))
            .transact()
            .upsert_turtle(&format!(
                "@prefix ex: <http://example.org/> . GRAPH <{dev_cfg}> {{ ex:note ex:p 1 . }}"
            ))
            .commit()
            .await
            .expect("dev writes data to its config graph");
        if !fast_forward {
            fluree
                .insert(
                    main,
                    &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:x", "ex:p": 2}),
                )
                .await
                .unwrap();
        }
        let err = fluree
            .merge_branch(
                &name,
                "dev",
                None,
                fluree_db_api::ConflictStrategy::default(),
            )
            .await
            .expect_err("a merge carrying dev's config graph data");
        assert_eq!(err.status_code(), 409, "{fast_forward}: {err}");
        let message = err.to_string();
        assert!(
            message.contains("branch dev's config graph") && message.contains(&dev_cfg),
            "{fast_forward}: {message}"
        );
    }
}

/// A merge may not change the target's own config or txn-meta graph with
/// what another branch wrote there: on the branch, the target's system graph
/// IRIs name ordinary graphs, so a value written (or deleted) there is data,
/// not a change to the target's config. A fast-forward and a general merge
/// are refused (409), the preview says so, and the target's config is
/// untouched, also into a target whose config governs no writes.
#[tokio::test]
async fn a_merge_changing_the_targets_system_graph_from_another_branch_is_refused() {
    let edits = [
        (
            "a setting replaced",
            true,
            "config",
            "DELETE DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled true } } ;
             INSERT DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled false } }",
        ),
        (
            "a setting deleted",
            true,
            "config",
            "DELETE DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled true } }",
        ),
        (
            "commit metadata written",
            true,
            "txn-meta",
            "INSERT DATA { GRAPH <TXN> { <urn:x:commit> f:message \"from dev\" } }",
        ),
        (
            "commit metadata written, into a target that governs no writes",
            false,
            "txn-meta",
            "INSERT DATA { GRAPH <TXN> { <urn:x:commit> f:message \"from dev\" } }",
        ),
    ];
    for (i, (what, governed, role, edit)) in edits.into_iter().enumerate() {
        for fast_forward in [true, false] {
            let fluree = FlureeBuilder::memory().build_memory();
            let name = format!("it-config-branch-target-{i}-{fast_forward}");
            let ledger = fluree.create_ledger(&name).await.unwrap();
            let main_id = format!("{name}:main");
            let main_cfg = config_graph_iri(&main_id);
            let main_txn = fluree_db_core::graph_registry::txn_meta_graph_iri(&main_id);
            let main = fluree
                .stage_owned(ledger)
                .upsert_turtle(&format!(
                    "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
                     ex:seed ex:p 1 .
                     GRAPH <{main_cfg}> {{ <urn:config:main> a f:LedgerConfig ;
                       f:shaclDefaults <urn:config:shacl> . <urn:config:shacl> f:shaclEnabled {governed} . }}"
                ))
                .execute()
                .await
                .expect("main's config")
                .ledger;
            fluree
                .create_branch(&name, "dev", None, None)
                .await
                .unwrap();
            fluree
                .graph(&format!("{name}:dev"))
                .transact()
                .sparql_update(&format!(
                    "PREFIX f: <https://ns.flur.ee/db#> {}",
                    edit.replace("CFG", &main_cfg).replace("TXN", &main_txn)
                ))
                .commit()
                .await
                .unwrap_or_else(|e| panic!("{what}: an ordinary graph on dev: {e}"));
            if !fast_forward {
                fluree
                    .insert(main, &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:x", "ex:p": 2}))
                    .await
                    .unwrap();
            }
            let case = format!("{what}/{fast_forward}");
            let preview = fluree
                .merge_preview_with(
                    &name,
                    "dev",
                    None,
                    fluree_db_api::MergePreviewOpts::default(),
                )
                .await
                .expect("preview");
            assert!(!preview.mergeable, "{case}: the preview refuses");
            let err = fluree
                .merge_branch(
                    &name,
                    "dev",
                    None,
                    fluree_db_api::ConflictStrategy::default(),
                )
                .await
                .expect_err(&case);
            assert_eq!(err.status_code(), 409, "{case}: {err}");
            let message = err.to_string();
            let graph = if role == "config" {
                &main_cfg
            } else {
                &main_txn
            };
            assert!(
                message.contains(&format!("this branch's {role} graph")) && message.contains(graph),
                "{case}: {message}"
            );
            let view = fluree.db(&main_id).await.unwrap();
            assert_eq!(
                view.ledger_config()
                    .and_then(|c| c.shacl.as_ref())
                    .and_then(|s| s.enabled),
                Some(governed),
                "{case}: main's config is untouched"
            );
        }
    }
}

/// A merge may not split one of the target's config groups. After the fork
/// the target links a group from its config graph, and the branch writes the
/// group's field in the default graph (through JSON-LD, or SPARQL). Each
/// write is accepted where it is made; the merge that would bring them
/// together is refused, naming the group and the repair, and its preview
/// says so.
#[tokio::test]
async fn config_guard_refuses_a_merge_that_splits_a_group() {
    for (i, sparql) in [false, true].into_iter().enumerate() {
        let fluree = FlureeBuilder::memory().build_memory();
        let name = format!("it-config-guard-merge-split-{i}");
        let main = ledger_with_branch(&fluree, &name).await;
        fluree
            .insert(main, &config_linking_the_shacl_group())
            .await
            .expect("the target links the group");
        let dev = fluree.ledger(&format!("{name}:dev")).await.unwrap();
        if sparql {
            sparql_update(
                &fluree,
                dev,
                "PREFIX f: <https://ns.flur.ee/db#>
                 INSERT DATA { <urn:config:shacl> f:shaclEnabled true }",
            )
            .await
            .expect("the branch writes the field");
        } else {
            fluree
                .insert(dev, &shacl_group_field_in_the_default_graph())
                .await
                .expect("the branch writes the field");
        }

        let preview = fluree
            .merge_preview(&name, "dev", None)
            .await
            .expect("preview");
        assert!(!preview.fast_forward);
        assert!(!preview.mergeable);
        let validation = preview.validation.expect("the merge validates");
        assert!(!validation.conforms);
        let report = validation.report.unwrap_or_default();
        assert!(report.contains("urn:config:shacl"), "{report}");

        let err = fluree
            .merge_branch(
                &name,
                "dev",
                None,
                fluree_db_api::ConflictStrategy::default(),
            )
            .await
            .expect_err("the merge would split the group");
        branch_op_refusal(err, "urn:config:shacl");
    }
}

/// A fast-forward may not split one of the target's config groups either. The
/// target links a group before the fork; the branch carries the group's field
/// in the default graph from before these checks (seeded unchecked, as that
/// history reaches a ledger). The fast-forward is refused until the config is
/// repaired, and its preview says so.
#[tokio::test]
async fn config_guard_refuses_a_fast_forward_carrying_a_split_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it-config-guard-ff-split";
    let ledger = fluree.create_ledger(name).await.unwrap();
    fluree
        .insert(ledger, &config_linking_the_shacl_group())
        .await
        .expect("the target links the group");
    fluree.create_branch(name, "dev", None, None).await.unwrap();
    let dev = fluree.ledger(&format!("{name}:dev")).await.unwrap();
    commit_unchecked(&fluree, dev, &shacl_group_field_in_the_default_graph()).await;

    let preview = fluree
        .merge_preview(name, "dev", None)
        .await
        .expect("preview");
    assert!(preview.fast_forward);
    assert!(!preview.mergeable);
    let validation = preview
        .validation
        .expect("a fast-forward carrying config settings is checked");
    assert!(!validation.conforms);

    let err = fluree
        .merge_branch(
            name,
            "dev",
            None,
            fluree_db_api::ConflictStrategy::default(),
        )
        .await
        .expect_err("the fast-forward would split the group");
    branch_op_refusal(err, "urn:config:shacl");
}

/// A revert may not split a config group: undoing the repair of a split config
/// (the #1979 state, seeded unchecked, then repaired with the recipe that moves
/// the fields into the config graph) would put the field back where the reader
/// does not look. The revert is refused, and its preview says so.
#[tokio::test]
async fn config_guard_refuses_a_revert_that_undoes_a_repair() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it-config-guard-revert-repair";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger = fluree.create_ledger(name).await.unwrap();
    let ledger = commit_unchecked(
        &fluree,
        ledger,
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [
                {
                    "@id": "urn:config:main",
                    "@type": "f:LedgerConfig",
                    "@graph": "config",
                    "f:shaclDefaults": {"@id": "urn:config:shacl"}
                },
                {"@id": "urn:config:shacl", "f:shaclEnabled": true}
            ]
        }),
    )
    .await;
    let repair = sparql_update(
        &fluree,
        ledger,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#>
             DELETE {{ ?n ?p ?o }}
             INSERT {{ GRAPH <{cfg}> {{ ?n ?p ?o }} }}
             WHERE {{ GRAPH <{cfg}> {{ ?parent f:shaclDefaults ?n }} ?n ?p ?o . }}"
        ),
    )
    .await
    .expect("the repair commits");
    let commit = fluree_db_api::CommitRef::Exact(repair.receipt.commit_id.clone());

    let preview = fluree
        .revert_commit_preview(name, "main", commit.clone())
        .await
        .expect("preview");
    assert!(!preview.revertable);
    let validation = preview.validation.expect("the revert validates");
    assert!(!validation.conforms);

    let err = fluree
        .revert_commit(name, "main", commit, fluree_db_api::ConflictStrategy::Abort)
        .await
        .expect_err("the revert would split the group again");
    branch_op_refusal(err, "urn:config:shacl");
}

/// The re-check does not stand in the way of a merge that leaves the target's
/// config groups whole: the target changes its config after the fork while
/// the branch writes data, an `f:enforceUnique` annotation among it, which
/// the guard looks at and accepts. The merge commits.
#[tokio::test]
async fn config_guard_merges_a_branch_while_the_target_changes_its_config() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it-config-guard-merge-clean";
    let main = ledger_with_branch(&fluree, name).await;
    fluree
        .insert(
            main,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:transactDefaults": {"@id": "urn:config:transact", "f:uniqueEnabled": true}
            }),
        )
        .await
        .expect("the target changes its config");
    let dev = fluree.ledger(&format!("{name}:dev")).await.unwrap();
    fluree
        .insert(
            dev,
            &json!({
                "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:u1", "ex:email": "a@x"}
                ]
            }),
        )
        .await
        .expect("the branch writes data");

    let preview = fluree
        .merge_preview(name, "dev", None)
        .await
        .expect("preview");
    assert!(!preview.fast_forward);
    assert!(preview.mergeable);
    fluree
        .merge_branch(
            name,
            "dev",
            None,
            fluree_db_api::ConflictStrategy::default(),
        )
        .await
        .expect("the merge leaves the config groups whole");
}

/// A revert that leaves the config groups whole is accepted: switching SHACL
/// off and reverting that brings the setting back, with one value.
#[tokio::test]
async fn config_guard_accepts_a_revert_of_a_config_change() {
    let fluree = FlureeBuilder::memory().build_memory();
    let name = "it-config-guard-revert-clean";
    let ledger = fluree.create_ledger(name).await.unwrap();
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:shaclDefaults": {"@id": "urn:config:shacl", "f:shaclEnabled": true}
            }),
        )
        .await
        .unwrap()
        .ledger;
    let change = fluree
        .upsert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:shacl",
                "@graph": "config",
                "f:shaclEnabled": false
            }),
        )
        .await
        .expect("switch SHACL off");
    fluree
        .revert_commit(
            name,
            "main",
            fluree_db_api::CommitRef::Exact(change.receipt.commit_id),
            fluree_db_api::ConflictStrategy::Abort,
        )
        .await
        .expect("the revert leaves one value");
    let view = fluree.db(&format!("{name}:main")).await.unwrap();
    assert_eq!(
        view.ledger_config()
            .and_then(|c| c.shacl.clone())
            .and_then(|s| s.enabled),
        Some(true)
    );
}

/// A config typed outside the config graph is never read, so the write is
/// refused: the JSON-LD shape `concepts/reasoning.md` showed (no graph, so the
/// default graph), and the same config through Turtle insert.
#[tokio::test]
async fn config_guard_refuses_a_config_typed_outside_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-guard-inert:main";
    let err = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "f:reasoningDefaults": {"f:reasoningModes": {"@id": "f:rdfs"}}
            }),
        )
        .await
        .expect_err("a config in the default graph has no effect");
    let message = config_guard_refusal(err);
    assert!(
        message.contains(&config_graph_iri(ledger_id)) && message.contains("the default graph"),
        "{message}"
    );

    let err = fluree
        .insert_turtle(
            genesis_ledger(&fluree, "it/config-guard-inert-ttl:main"),
            "@prefix f: <https://ns.flur.ee/db#> . <urn:config:main> a f:LedgerConfig .",
        )
        .await
        .expect_err("Turtle insert: a config in the default graph has no effect");
    config_guard_refusal(err);
}

/// The guard runs on a branch as it does on main: a config typed outside the
/// config graph is refused, and `"@graph": "config"` takes it.
#[tokio::test]
async fn config_guard_runs_on_a_branch() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = fluree
        .create_ledger("it-config-guard-branch")
        .await
        .unwrap();
    fluree
        .insert(
            ledger,
            &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:seed", "ex:p": 1}),
        )
        .await
        .unwrap();
    fluree
        .create_branch("it-config-guard-branch", "dev", None, None)
        .await
        .unwrap();
    let dev = fluree.ledger("it-config-guard-branch:dev").await.unwrap();
    let config = |graph: &str| {
        json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@id": "urn:config:main",
            "@type": "f:LedgerConfig",
            "@graph": graph,
            "f:shaclDefaults": {"f:shaclEnabled": true}
        })
    };

    let err = fluree
        .insert(dev.clone(), &config("default"))
        .await
        .expect_err("a config typed outside the config graph");
    let message = config_guard_refusal(err);
    assert!(message.contains("f:LedgerConfig"), "{message}");

    fluree
        .insert(dev, &config("config"))
        .await
        .expect("the config graph takes it");
}

/// A single-valued setting the config graph would hold two values of is
/// refused (a re-insert with a new value), while an identical re-insert and
/// an upsert are not. A second `f:LedgerConfig` subject is refused too.
#[tokio::test]
async fn config_guard_refuses_a_second_value_for_a_single_valued_setting() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-guard-values:main";
    let group = |enabled: bool| {
        json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@id": "urn:config:shacl",
            "@graph": "config",
            "f:shaclEnabled": enabled
        })
    };
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:shaclDefaults": {"@id": "urn:config:shacl", "f:shaclEnabled": true}
            }),
        )
        .await
        .expect("first config write")
        .ledger;

    let err = fluree
        .insert(ledger.clone(), &group(false))
        .await
        .expect_err("a second value for f:shaclEnabled");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:shaclEnabled") && message.contains("upsert"),
        "{message}"
    );

    let ledger = fluree
        .insert(ledger, &group(true))
        .await
        .expect("an identical re-insert adds no value")
        .ledger;
    let ledger = fluree
        .upsert(ledger, &group(false))
        .await
        .expect("an upsert replaces the value")
        .ledger;
    let view = fluree.db(ledger_id).await.unwrap();
    assert_eq!(
        view.ledger_config()
            .and_then(|c| c.shacl.clone())
            .and_then(|s| s.enabled),
        Some(false)
    );

    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:other",
                "@type": "f:LedgerConfig",
                "@graph": "config"
            }),
        )
        .await
        .expect_err("a second f:LedgerConfig subject");
    let message = config_guard_refusal(err);
    assert!(message.contains("f:LedgerConfig"), "{message}");
}

/// The query group is a setting group like the others: its
/// `f:unionDefaultGraph` belongs in the config graph with the edge (R1), and
/// takes one value (R3).
#[tokio::test]
async fn config_guard_checks_the_query_defaults_group() {
    let fluree = FlureeBuilder::memory().build_memory();
    let err = fluree
        .insert(
            genesis_ledger(&fluree, "it/config-guard-query-split:main"),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:queryDefaults": {
                    "@id": "urn:config:query",
                    "@graph": "default",
                    "f:unionDefaultGraph": true
                }
            }),
        )
        .await
        .expect_err("a query group split across graphs");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:queryDefaults") && message.contains("f:unionDefaultGraph"),
        "{message}"
    );

    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, "it/config-guard-query-values:main"),
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:main",
                "@type": "f:LedgerConfig",
                "@graph": "config",
                "f:queryDefaults": {"@id": "urn:config:query", "f:unionDefaultGraph": true}
            }),
        )
        .await
        .expect("the query group")
        .ledger;
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "@id": "urn:config:query",
                "@graph": "config",
                "f:unionDefaultGraph": false
            }),
        )
        .await
        .expect_err("a second value for f:unionDefaultGraph");
    let message = config_guard_refusal(err);
    assert!(
        message.contains("f:unionDefaultGraph") && message.contains("upsert"),
        "{message}"
    );
}

/// A multi-operation SPARQL update on `name` (created here, seeded with
/// `trig`): every operation is governed by the state before the request.
async fn seed_multi_op_ledger(fluree: &fluree_db_api::Fluree, name: &str, trig: &str) -> String {
    fluree.create_ledger(name).await.expect("create");
    let ledger_id = format!("{name}:main");
    fluree
        .graph(&ledger_id)
        .transact()
        .upsert_turtle(trig)
        .commit()
        .await
        .expect("seed");
    ledger_id
}

async fn multi_op(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    sparql: &str,
) -> fluree_db_api::Result<fluree_db_api::TransactResultRef> {
    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(sparql)
        .commit()
        .await
}

/// Every operation of a multi-operation update is checked against the config,
/// shapes and uniqueness constraints in effect before the request, as a
/// single transaction is: an earlier operation that turns enforcement off, or
/// removes the shape or constraint, does not admit a later operation's data,
/// even when the request turns it back on. Enforcement switched on inside the
/// request does not apply to it either, as in one transaction.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn multi_operation_update_is_governed_by_the_state_before_it() {
    let fluree = FlureeBuilder::memory().build_memory();
    let shacl_seed = |cfg: &str, enabled: bool| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix sh: <http://www.w3.org/ns/shacl#> .
             @prefix ex: <http://example.org/> .
             ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
               sh:property [ sh:path ex:name ; sh:minCount 1 ] .
             GRAPH <{cfg}> {{
               <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
               <urn:config:shacl> f:shaclEnabled {enabled} .
             }}"
        )
    };
    let prefixes = "PREFIX f: <https://ns.flur.ee/db#> PREFIX ex: <http://example.org/>
                    PREFIX sh: <http://www.w3.org/ns/shacl#>";
    for (i, (what, ops)) in [
        (
            "SHACL off, then the violation",
            "DELETE DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled true } } ;
             INSERT DATA { ex:bad a ex:Person }",
        ),
        (
            "SHACL off, the violation, SHACL back on",
            "DELETE DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled true } } ;
             INSERT DATA { ex:bad a ex:Person } ;
             INSERT DATA { GRAPH <CFG> { <urn:config:shacl> f:shaclEnabled true } }",
        ),
        (
            "the shape removed, then the violation",
            "DELETE WHERE { ex:PersonShape sh:targetClass ex:Person } ;
             INSERT DATA { ex:bad a ex:Person }",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("it/multi-op-governed-shacl-{i}");
        let cfg = config_graph_iri(&format!("{name}:main"));
        let ledger_id = seed_multi_op_ledger(&fluree, &name, &shacl_seed(&cfg, true)).await;
        let err = multi_op(
            &fluree,
            &ledger_id,
            &format!("{prefixes} {}", ops.replace("CFG", &cfg)),
        )
        .await
        .expect_err(what);
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::ShaclViolation(_)
                )
            ),
            "{what}: {err:?}"
        );
    }

    // Enabled inside the request: not in effect for it.
    let name = "it/multi-op-governed-shacl-enable";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &shacl_seed(&cfg, false)).await;
    multi_op(
        &fluree,
        &ledger_id,
        &format!(
            "{prefixes}
             DELETE DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled false }} }} ;
             INSERT DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled true }} }} ;
             INSERT DATA {{ ex:early a ex:Person }}"
        ),
    )
    .await
    .expect("enforcement switched on inside the request does not apply to it");
    let err = multi_op(
        &fluree,
        &ledger_id,
        &format!("{prefixes} INSERT DATA {{ ex:late a ex:Person }}"),
    )
    .await
    .expect_err("it applies to the next request");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "{err:?}"
    );
}

/// The same for uniqueness, switched off in config or by removing the
/// property's `f:enforceUnique` annotation (data in the default graph).
#[tokio::test]
async fn multi_operation_update_is_governed_by_the_state_before_it_for_uniqueness() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, (what, off, on)) in [
        (
            "uniqueness off in config",
            "DELETE DATA { GRAPH <CFG> { <urn:config:tx> f:uniqueEnabled true } }",
            "INSERT DATA { GRAPH <CFG> { <urn:config:tx> f:uniqueEnabled true } }",
        ),
        (
            "the constraint annotation removed",
            "DELETE DATA { ex:email f:enforceUnique true }",
            "INSERT DATA { ex:email f:enforceUnique true }",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("it/multi-op-governed-unique-{i}");
        let cfg = config_graph_iri(&format!("{name}:main"));
        let ledger_id = seed_multi_op_ledger(
            &fluree,
            &name,
            &format!(
                "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
                 ex:email f:enforceUnique true .
                 ex:u1 ex:email \"a@x\" .
                 GRAPH <{cfg}> {{
                   <urn:config:main> a f:LedgerConfig ; f:transactDefaults <urn:config:tx> .
                   <urn:config:tx> f:uniqueEnabled true .
                 }}"
            ),
        )
        .await;
        let err = multi_op(
            &fluree,
            &ledger_id,
            &format!(
                "PREFIX f: <https://ns.flur.ee/db#> PREFIX ex: <http://example.org/>
                 {} ; INSERT DATA {{ ex:u2 ex:email \"a@x\" }} ; {}",
                off.replace("CFG", &cfg),
                on.replace("CFG", &cfg)
            ),
        )
        .await
        .expect_err(what);
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
                )
            ),
            "{what}: {err:?}"
        );
    }
}

/// The config guard judges a multi-operation update by what it commits, not
/// by the order of its operations: replacing a single-valued setting by
/// inserting the new value before deleting the old one is accepted, and a
/// request whose result holds two values is still refused.
#[tokio::test]
async fn multi_operation_update_is_guarded_on_its_result() {
    let fluree = FlureeBuilder::memory().build_memory();
    let seed = |cfg: &str| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <{cfg}> {{
               <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
               <urn:config:shacl> f:shaclEnabled true .
             }}"
        )
    };
    let name = "it/multi-op-guard-result";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &seed(&cfg)).await;
    multi_op(
        &fluree,
        &ledger_id,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#>
             INSERT DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled false }} }} ;
             DELETE DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled true }} }}"
        ),
    )
    .await
    .expect("insert-then-delete leaves one value");
    let view = fluree.db(&ledger_id).await.unwrap();
    assert_eq!(
        view.ledger_config()
            .and_then(|c| c.shacl.as_ref())
            .and_then(|s| s.enabled),
        Some(false)
    );

    let name = "it/multi-op-guard-result-refused";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &seed(&cfg)).await;
    let err = multi_op(
        &fluree,
        &ledger_id,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#> PREFIX ex: <http://example.org/>
             INSERT DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled false }} }} ;
             INSERT DATA {{ ex:x ex:p 1 }}"
        ),
    )
    .await
    .expect_err("two values in the result");
    let message = config_guard_refusal(err);
    assert!(message.contains("f:shaclEnabled"), "{message}");
}

/// A transaction ([`fluree_db_api::Transaction`]) is governed like a
/// multi-operation update: every operation it stages, a whole multi-operation
/// SPARQL update or a Cypher statement included, reads SHACL and the config
/// from the state the transaction began on, its shapes and uniqueness
/// constraints as well as its config. An operation that turns SHACL off, or
/// removes a shape or a property's `f:enforceUnique`, does not admit a later
/// one's violating data; one that turns SHACL on, or adds `f:enforceUnique`,
/// does not apply it to the transaction.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn transaction_is_governed_by_the_state_it_began_on() {
    use fluree_db_api::{TransactionOptions, TxnOperation};
    let fluree = FlureeBuilder::memory().build_memory();
    let shacl_seed = |cfg: &str, enabled: bool| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix sh: <http://www.w3.org/ns/shacl#> .
             @prefix ex: <http://example.org/> .
             ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
               sh:property [ sh:path ex:name ; sh:minCount 1 ] .
             GRAPH <{cfg}> {{
               <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
               <urn:config:shacl> f:shaclEnabled {enabled} .
             }}"
        )
    };
    let flip = |cfg: &str, from: bool, to: bool| {
        format!(
            "PREFIX f: <https://ns.flur.ee/db#>
             DELETE DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled {from} }} }} ;
             INSERT DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled {to} }} }}"
        )
    };
    let violating = || {
        TxnOperation::Insert(json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:nameless",
            "@type": "ex:Person"
        }))
    };

    // SHACL on: turning it off in the transaction does not admit the data,
    // whether the data is its own operation, a Cypher statement, or a later
    // operation of the same SPARQL update.
    for (case, data) in [
        ("JSON-LD", Some(violating())),
        ("Cypher", None),
        ("in the same SPARQL update", None),
        ("in a later multi-operation SPARQL update", None),
    ] {
        let name = format!("it/txn-gov-on-{}", case.replace(' ', "-"));
        let cfg = config_graph_iri(&format!("{name}:main"));
        let ledger_id = seed_multi_op_ledger(&fluree, &name, &shacl_seed(&cfg, true)).await;
        let mut txn = fluree
            .begin_transaction(&ledger_id, TransactionOptions::default())
            .await
            .expect("begin");
        let result = match (case, data) {
            (_, Some(data)) => {
                txn.stage(TxnOperation::SparqlUpdate(flip(&cfg, true, false), None))
                    .await
                    .expect("the config change stages");
                txn.stage(data).await
            }
            ("Cypher", None) => {
                txn.stage(TxnOperation::SparqlUpdate(flip(&cfg, true, false), None))
                    .await
                    .expect("the config change stages");
                txn.stage_cypher("CREATE (:`http://example.org/Person`)", None)
                    .await
                    .map(|_| ())
            }
            ("in a later multi-operation SPARQL update", None) => {
                txn.stage(TxnOperation::SparqlUpdate(flip(&cfg, true, false), None))
                    .await
                    .expect("the config change stages");
                txn.stage(TxnOperation::SparqlUpdate(
                    "PREFIX ex: <http://example.org/>
                     INSERT DATA { ex:other ex:p 1 } ;
                     INSERT DATA { ex:nameless a ex:Person }"
                        .to_string(),
                    None,
                ))
                .await
            }
            _ => {
                let both = format!(
                    "{} ;
                     PREFIX ex: <http://example.org/>
                     INSERT DATA {{ ex:nameless a ex:Person }}",
                    flip(&cfg, true, false)
                );
                txn.stage(TxnOperation::SparqlUpdate(both, None)).await
            }
        };
        let err = result.expect_err(case);
        assert!(err.to_string().contains("SHACL"), "{case}: {err}");
    }

    // SHACL off: turning it on in the transaction does not apply it to the
    // transaction's own data.
    let name = "it/txn-gov-off";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &shacl_seed(&cfg, false)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(TxnOperation::SparqlUpdate(flip(&cfg, false, true), None))
        .await
        .expect("the config change stages");
    txn.stage(violating())
        .await
        .expect("not enforced within the transaction that enables it");
    txn.commit(Default::default()).await.expect("commit");

    // The shape removed in the transaction still applies to it.
    let name = "it/txn-gov-shape-removed";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &shacl_seed(&cfg, true)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(TxnOperation::SparqlUpdate(
        "PREFIX sh: <http://www.w3.org/ns/shacl#> PREFIX ex: <http://example.org/>
         DELETE DATA { ex:PersonShape sh:targetClass ex:Person }"
            .to_string(),
        None,
    ))
    .await
    .expect("the shape change stages");
    let err = txn.stage(violating()).await.expect_err("shape removed");
    assert!(err.to_string().contains("SHACL"), "shape removed: {err}");

    // Uniqueness: a constraint removed in the transaction still applies to
    // it, and one added does not yet.
    let unique_seed = |cfg: &str, annotated: bool| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> . @prefix ex: <http://example.org/> .
             {}
             ex:u1 ex:email \"a@x\" .
             GRAPH <{cfg}> {{
               <urn:config:main> a f:LedgerConfig ; f:transactDefaults <urn:config:tx> .
               <urn:config:tx> f:uniqueEnabled true .
             }}",
            if annotated {
                "ex:email f:enforceUnique true ."
            } else {
                ""
            }
        )
    };
    let annotation = |op: &str| {
        TxnOperation::SparqlUpdate(
            format!(
                "PREFIX f: <https://ns.flur.ee/db#> PREFIX ex: <http://example.org/>
                 {op} DATA {{ ex:email f:enforceUnique true }}"
            ),
            None,
        )
    };
    let duplicate = || {
        TxnOperation::Insert(json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:u2",
            "ex:email": "a@x"
        }))
    };
    let name = "it/txn-gov-unique-removed";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &unique_seed(&cfg, true)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(annotation("DELETE"))
        .await
        .expect("the constraint change stages");
    let err = txn
        .stage(duplicate())
        .await
        .expect_err("constraint removed");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "constraint removed: {err:?}"
    );

    let name = "it/txn-gov-unique-added";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &unique_seed(&cfg, false)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(annotation("INSERT"))
        .await
        .expect("the constraint change stages");
    txn.stage(duplicate())
        .await
        .expect("not enforced within the transaction that adds it");
    txn.commit(Default::default()).await.expect("commit");
}

/// A transaction's config guard judges what it commits, once, as a
/// multi-operation update's does: a second value inserted before the old one
/// is deleted commits; a second value left in the result is refused when the
/// transaction commits.
#[tokio::test]
async fn transaction_is_guarded_on_its_result() {
    use fluree_db_api::{TransactionOptions, TxnOperation};
    let fluree = FlureeBuilder::memory().build_memory();
    let seed = |cfg: &str| {
        format!(
            "@prefix f: <https://ns.flur.ee/db#> .
             GRAPH <{cfg}> {{
               <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
               <urn:config:shacl> f:shaclEnabled true .
             }}"
        )
    };
    let stage = |cfg: &str, op: &str| {
        TxnOperation::SparqlUpdate(
            format!(
                "PREFIX f: <https://ns.flur.ee/db#>
                 {op} DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled {} }} }}",
                op == "DELETE"
            ),
            None,
        )
    };

    let name = "it/txn-guard-result";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &seed(&cfg)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(stage(&cfg, "INSERT"))
        .await
        .expect("insert false");
    txn.stage(stage(&cfg, "DELETE")).await.expect("delete true");
    txn.commit(Default::default())
        .await
        .expect("insert-then-delete leaves one value");
    let view = fluree.db(&ledger_id).await.unwrap();
    assert_eq!(
        view.ledger_config()
            .and_then(|c| c.shacl.as_ref())
            .and_then(|s| s.enabled),
        Some(false)
    );

    // The same when the second value comes from a multi-operation update:
    // that update's own result is not guarded on its own, only the
    // transaction's.
    let name = "it/txn-guard-result-nested";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &seed(&cfg)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(TxnOperation::SparqlUpdate(
        format!(
            "PREFIX f: <https://ns.flur.ee/db#> PREFIX ex: <http://example.org/>
             INSERT DATA {{ GRAPH <{cfg}> {{ <urn:config:shacl> f:shaclEnabled false }} }} ;
             INSERT DATA {{ ex:x ex:p 1 }}"
        ),
        None,
    ))
    .await
    .expect("the guard waits for the transaction's result");
    txn.stage(stage(&cfg, "DELETE")).await.expect("delete true");
    txn.commit(Default::default())
        .await
        .expect("one value in the transaction's result");

    let name = "it/txn-guard-result-refused";
    let cfg = config_graph_iri(&format!("{name}:main"));
    let ledger_id = seed_multi_op_ledger(&fluree, name, &seed(&cfg)).await;
    let mut txn = fluree
        .begin_transaction(&ledger_id, TransactionOptions::default())
        .await
        .expect("begin");
    txn.stage(stage(&cfg, "INSERT"))
        .await
        .expect("the guard waits for the result");
    txn.stage(TxnOperation::Insert(json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:x",
        "ex:p": 1
    })))
    .await
    .expect("data stages");
    let err = txn
        .commit(Default::default())
        .await
        .expect_err("two values in the result");
    let message = config_guard_refusal(err);
    assert!(message.contains("f:shaclEnabled"), "{message}");
}

/// A repair recipe exactly as `docs/ledger-config/writing-config.md` gives
/// it, for `ledger_id`: the first (`which` 0) starts from the config subject
/// in the config graph, the second from the recommended subject when the
/// config graph holds none.
#[cfg(feature = "shacl")]
fn docs_repair_recipe(ledger_id: &str, which: usize) -> String {
    let doc = include_str!("../../docs/ledger-config/writing-config.md");
    let section = doc
        .split_once("## Repairing a config split across graphs")
        .expect("the repair section")
        .1;
    let recipe = section
        .split("```sparql")
        .nth(which + 1)
        .and_then(|rest| rest.split_once("```"))
        .expect("the recipe")
        .0;
    recipe
        .replace("urn:fluree:mydb:main#config", &config_graph_iri(ledger_id))
        .replace(
            "urn:fluree:mydb:main:config:ledger",
            &format!("urn:fluree:{ledger_id}:config:ledger"),
        )
}

/// The repair recipe (`docs/ledger-config/writing-config.md`, run as the docs
/// give it) restores each config the pre-fix JSON-LD parser could split
/// across graphs: a setting group's fields in the default graph; the config
/// subject's own group link and group there; a per-graph override's target
/// and groups there; a group partly in the config graph; an anonymous group
/// (a blank node, as the documented JSON-LD form wrote it); and (with the second
/// recipe) a whole config on the recommended subject in the default graph,
/// untyped, with no config subject in the config graph. Before the repair
/// each reads without its SHACL setting (the forbidden record commits) and
/// ledger info reports `stranded-fields`; after it, nothing is stranded and
/// SHACL enforces again.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn repair_recipe_restores_a_split_config() {
    let shape = json!({
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:property": [{"sh:path": {"@id": "ex:name"}, "sh:minCount": 1}]
    });
    let cases: Vec<(&str, Option<&str>, Vec<serde_json::Value>)> = vec![
        (
            "the whole config on the recommended subject in the default graph",
            None,
            vec![
                json!({"@id": "urn:fluree:{ledger}:config:ledger",
                       "f:shaclDefaults": {"@id": "urn:config:shacl"}}),
                json!({"@id": "urn:config:shacl", "f:shaclEnabled": true}),
            ],
        ),
        (
            "a group's fields in the default graph",
            None,
            vec![
                json!({"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                       "f:shaclDefaults": {"@id": "urn:config:shacl"}}),
                json!({"@id": "urn:config:shacl", "f:shaclEnabled": true,
                       "f:validationMode": {"@id": "f:ValidationReject"}}),
            ],
        ),
        (
            "the config subject's group link and group in the default graph",
            None,
            vec![
                json!({"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config"}),
                json!({"@id": "urn:config:main", "f:shaclDefaults": {"@id": "urn:config:shacl"}}),
                json!({"@id": "urn:config:shacl", "f:shaclEnabled": true}),
            ],
        ),
        (
            "a per-graph override's target and group in the default graph",
            Some("ex:g"),
            vec![
                json!({"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                       "f:graphOverrides": {"@id": "urn:config:g"}}),
                json!({"@id": "urn:config:g", "@type": "f:GraphConfig",
                       "f:targetGraph": {"@id": "ex:g"},
                       "f:shaclDefaults": {"@id": "urn:config:gshacl"}}),
                json!({"@id": "urn:config:gshacl", "f:shaclEnabled": true}),
            ],
        ),
        (
            "a group partly in the config graph",
            None,
            vec![
                json!({"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                       "f:shaclDefaults": {"@id": "urn:config:shacl"}}),
                json!({"@id": "urn:config:shacl", "@graph": "config",
                       "f:validationMode": {"@id": "f:ValidationReject"}}),
                json!({"@id": "urn:config:shacl", "f:shaclEnabled": true}),
            ],
        ),
        (
            "an anonymous group (a blank node) in the default graph",
            None,
            vec![
                json!({"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                       "f:shaclDefaults": {"@id": "_:group"}}),
                json!({"@id": "_:group", "f:shaclEnabled": true,
                       "f:validationMode": {"@id": "f:ValidationReject"}}),
            ],
        ),
    ];
    for (i, (what, data_graph, config)) in cases.into_iter().enumerate() {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("it/config-repair-recipe-{i}:main");
        let forbidden = |id: &str| {
            let mut doc =
                json!({"@context": {"ex": "http://example.org/"}, "@id": id, "@type": "ex:Person"});
            if let Some(g) = data_graph {
                doc["@graph"] = json!(g);
            }
            doc
        };
        let mut seed = vec![shape.clone()];
        seed.extend(config.into_iter().map(|node| {
            let node = node.to_string().replace("{ledger}", &ledger_id);
            serde_json::from_str::<serde_json::Value>(&node).unwrap()
        }));
        let recipe = docs_repair_recipe(&ledger_id, usize::from(i == 0));
        // The state as the pre-fix parser (and writes before the config guard)
        // left it, seeded unchecked, as that history reaches a ledger.
        let ledger = commit_unchecked(
            &fluree,
            genesis_ledger(&fluree, &ledger_id),
            &json!({
                "@context": {
                    "f": "https://ns.flur.ee/db#",
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/"
                },
                "@graph": seed
            }),
        )
        .await;
        assert!(
            config_diagnostic_codes(&fluree, &ledger_id)
                .await
                .contains(&"stranded-fields".to_string()),
            "{what}: reported before the repair"
        );
        let ledger = fluree
            .insert(ledger, &forbidden("ex:before"))
            .await
            .unwrap_or_else(|e| panic!("{what}: the split setting is not in effect: {e}"))
            .ledger;

        let ledger = sparql_update(&fluree, ledger, &recipe)
            .await
            .unwrap_or_else(|e| panic!("{what}: the repair recipe commits: {e}"))
            .ledger;

        assert!(
            !config_diagnostic_codes(&fluree, &ledger_id)
                .await
                .contains(&"stranded-fields".to_string()),
            "{what}: nothing stranded after the repair"
        );
        let err = fluree
            .insert(ledger, &forbidden("ex:after"))
            .await
            .expect_err(what);
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::ShaclViolation(_)
                )
            ),
            "{what}: {err:?}"
        );
    }
}

/// Fields of the ledger's config subject belong in the config graph like a
/// group's: written without it (JSON-LD with no `@graph`, SPARQL `INSERT
/// DATA` with no `GRAPH`), they would never be read, so the write is
/// refused, naming the config subject and the config graph. So is typing a
/// subject `f:LedgerConfig` in the config graph whose config fields already
/// sit in another graph.
#[tokio::test]
async fn config_guard_refuses_fields_of_the_config_subject_outside_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, form) in ["jsonld", "sparql"].into_iter().enumerate() {
        let name = format!("it/config-guard-root-{i}");
        fluree.create_ledger(&name).await.unwrap();
        let ledger_id = format!("{name}:main");
        let cfg = config_graph_iri(&ledger_id);
        let root = format!("urn:fluree:{ledger_id}:config:ledger");
        fluree
            .graph(&ledger_id)
            .transact()
            .upsert_turtle(&format!(
                "@prefix f: <https://ns.flur.ee/db#> . GRAPH <{cfg}> {{ <{root}> a f:LedgerConfig . }}"
            ))
            .commit()
            .await
            .expect("the config subject");
        let result = if form == "jsonld" {
            fluree
                .graph(&ledger_id)
                .transact()
                .insert(&json!({
                    "@context": {"f": "https://ns.flur.ee/db#"},
                    "@id": root,
                    "f:shaclDefaults": {"f:shaclEnabled": true}
                }))
                .commit()
                .await
        } else {
            fluree
                .graph(&ledger_id)
                .transact()
                .sparql_update(&format!(
                    "PREFIX f: <https://ns.flur.ee/db#>
                     INSERT DATA {{ <{root}> f:shaclDefaults <urn:x:shacl> .
                                    <urn:x:shacl> f:shaclEnabled true . }}"
                ))
                .commit()
                .await
        };
        let message = config_guard_refusal(result.expect_err(form));
        assert!(
            message.contains("config subject") && message.contains(&cfg),
            "{form}: {message}"
        );
    }

    // The reverse order: the fields first (no config yet), then the typing.
    let name = "it/config-guard-root-late";
    fluree.create_ledger(name).await.unwrap();
    let ledger_id = format!("{name}:main");
    let cfg = config_graph_iri(&ledger_id);
    fluree
        .graph(&ledger_id)
        .transact()
        .insert(&json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@id": "urn:config:main",
            "f:shaclDefaults": {"@id": "urn:config:shacl"}
        }))
        .commit()
        .await
        .expect("an f: field on a subject that is not config yet is data");
    let err = fluree
        .graph(&ledger_id)
        .transact()
        .upsert_turtle(&format!(
            "@prefix f: <https://ns.flur.ee/db#> . GRAPH <{cfg}> {{ <urn:config:main> a f:LedgerConfig . }}"
        ))
        .commit()
        .await
        .expect_err("typing it in the config graph would leave its group link outside");
    let message = config_guard_refusal(err);
    assert!(message.contains("already has fields"), "{message}");
}

/// The conventional config subject (`urn:fluree:<ledger>:config:ledger`) is
/// the config subject by its IRI, before the config graph holds it: on a
/// ledger with no config, its fields written without the config graph would
/// never be read, so the write is refused (JSON-LD with no `@graph`, SPARQL
/// `INSERT DATA` with no `GRAPH`), naming the config graph. Written past the
/// guard (data from an earlier release), ledger info reports them.
#[tokio::test]
async fn config_guard_refuses_the_conventional_config_subject_outside_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    for (i, form) in ["jsonld", "sparql"].into_iter().enumerate() {
        let name = format!("it/config-guard-conventional-{i}");
        fluree.create_ledger(&name).await.unwrap();
        let ledger_id = format!("{name}:main");
        let cfg = config_graph_iri(&ledger_id);
        let root = format!("urn:fluree:{ledger_id}:config:ledger");
        let result = if form == "jsonld" {
            fluree
                .graph(&ledger_id)
                .transact()
                .insert(&json!({
                    "@context": {"f": "https://ns.flur.ee/db#"},
                    "@id": root,
                    "f:shaclDefaults": {"@id": "urn:x:shacl", "f:shaclEnabled": true}
                }))
                .commit()
                .await
        } else {
            fluree
                .graph(&ledger_id)
                .transact()
                .sparql_update(&format!(
                    "PREFIX f: <https://ns.flur.ee/db#>
                     INSERT DATA {{ <{root}> f:shaclDefaults <urn:x:shacl> .
                                    <urn:x:shacl> f:shaclEnabled true . }}"
                ))
                .commit()
                .await
        };
        let message = config_guard_refusal(result.expect_err(form));
        assert!(
            message.contains("config subject") && message.contains(&root) && message.contains(&cfg),
            "{form}: {message}"
        );
    }

    let ledger_id = "it/config-diagnostics-conventional:main";
    let root = format!("urn:fluree:{ledger_id}:config:ledger");
    commit_unchecked(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@id": root,
            "f:shaclDefaults": {"@id": "urn:x:shacl", "f:shaclEnabled": true}
        }),
    )
    .await;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["stranded-fields"]
    );
}

/// Ledger info reports every config node with config fields outside the
/// config graph, not only empty groups: a per-graph override whose target and
/// groups were written to the default graph (reported, with the override the
/// reader skips for want of a target), and a group partly in the config
/// graph.
#[tokio::test]
async fn diagnostics_report_every_stranded_config_node() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-diagnostics-override:main";
    commit_unchecked(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/"},
            "@graph": [
                {"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                 "f:graphOverrides": {"@id": "urn:config:g"}},
                {"@id": "urn:config:g", "@type": "f:GraphConfig",
                 "f:targetGraph": {"@id": "ex:g"},
                 "f:shaclDefaults": {"@id": "urn:config:gshacl"}},
                {"@id": "urn:config:gshacl", "f:shaclEnabled": true}
            ]
        }),
    )
    .await;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["empty-group", "stranded-fields"]
    );

    let ledger_id = "it/config-diagnostics-partial:main";
    commit_unchecked(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [
                {"@id": "urn:config:main", "@type": "f:LedgerConfig", "@graph": "config",
                 "f:shaclDefaults": {"@id": "urn:config:shacl"}},
                {"@id": "urn:config:shacl", "@graph": "config",
                 "f:validationMode": {"@id": "f:ValidationReject"}},
                {"@id": "urn:config:shacl", "f:shaclEnabled": true}
            ]
        }),
    )
    .await;
    assert_eq!(
        config_diagnostic_codes(&fluree, ledger_id).await,
        ["stranded-fields"]
    );
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
async fn config_write_trig_iri_groups() {
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

/// #1979: the JSON-LD recipe in `docs/ledger-config/writing-config.md`,
/// verbatim apart from the ledger name. The nested `f:shaclDefaults` group
/// inherits the node's `"@graph"` selector, so all four statements land in
/// `#config` (two used to land in the default graph, leaving an empty SHACL
/// group that read as disabled) and the configured SHACL enforces.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn config_write_json_ld_nested_groups_land_in_config() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-jsonld-nested:main";
    let config_iri = config_graph_iri(ledger_id);
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"sh": "http://www.w3.org/ns/shacl#", "ex": "http://example.org/"},
                "@id": "ex:PersonShape",
                "@type": "sh:NodeShape",
                "sh:targetClass": {"@id": "ex:Person"},
                "sh:property": [{"sh:path": {"@id": "ex:name"}, "sh:minCount": 1}]
            }),
        )
        .await
        .unwrap()
        .ledger;

    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": { "f": "https://ns.flur.ee/db#" },
                "@graph": [
                    {
                        "@id": format!("urn:fluree:{ledger_id}:config:ledger"),
                        "@type": "f:LedgerConfig",
                        "@graph": config_iri,
                        "f:shaclDefaults": {
                            "f:shaclEnabled": true,
                            "f:validationMode": { "@id": "f:ValidationReject" }
                        }
                    }
                ]
            }),
        )
        .await
        .expect("documented config write")
        .ledger;

    let view = fluree.db(ledger_id).await.unwrap();
    let shacl = view
        .ledger_config()
        .and_then(|c| c.shacl.clone())
        .expect("SHACL group read from #config");
    assert_eq!(
        shacl.enabled,
        Some(true),
        "f:shaclEnabled landed in #config"
    );
    assert_eq!(
        shacl.validation_mode,
        Some(fluree_db_core::ledger_config::ValidationMode::Reject),
        "f:validationMode landed in #config"
    );

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
        .expect_err("the record the shape forbids must be refused");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "{err:?}"
    );
}

/// A transaction that writes an unrecognized `f:reasoningModes` into #config
/// must be rejected at commit, not silently accepted (and then ignored at
/// query time).
#[tokio::test]
async fn config_write_rejects_unknown_reasoning_mode() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-bad-reasoning:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let config_iri = config_graph_iri(ledger_id);

    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:owltypo .
        }}
    "
    );

    let err = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect_err("unknown reasoning mode in #config must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("reasoningModes") && msg.contains("owltypo"),
        "error should name the offending mode, got: {msg}"
    );
}

/// Recognized `f:reasoningModes` (multiple mode IRIs) commit normally and
/// round-trip — the validator only rejects unrecognized names.
#[tokio::test]
async fn config_write_accepts_reasoning_modes() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-good-reasoning:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let config_iri = config_graph_iri(ledger_id);

    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            <urn:config:reasoning> f:reasoningModes f:rdfs, f:owl2rl .
        }}
    "
    );

    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("recognized reasoning modes must commit");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config attached");
    let mut modes = config
        .reasoning
        .as_ref()
        .and_then(|r| r.modes.as_ref())
        .expect("modes present")
        .clone();
    modes.sort();
    assert_eq!(
        modes,
        vec![
            "https://ns.flur.ee/db#owl2rl".to_string(),
            "https://ns.flur.ee/db#rdfs".to_string(),
        ]
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
    let empty_opts = GovernanceOptions::default();
    let merged = config_resolver::merge_policy_opts(resolved, &empty_opts);
    assert_eq!(
        merged.default_allow,
        Some(false),
        "config's defaultAllow=false should apply when no query opts"
    );
    assert!(
        merged.policy_class.is_some(),
        "config's policyClass should apply when no query opts"
    );
}

/// `f:policyClass` written as an RDF list reads its members, whether the list
/// is stored compactly (`( … )`) or as the `rdf:first`/`rdf:rest` triples it
/// stands for — a chain head used to be read as one blank-node "class".
#[tokio::test]
async fn policy_class_list_reads_its_members_in_either_shape() {
    for (name, list) in [
        ("collection", "( ex:A ex:B )".to_string()),
        (
            "chain",
            "_:c0 . _:c0 rdf:first ex:A ; rdf:rest _:c1 . _:c1 rdf:first ex:B ; rdf:rest rdf:nil"
                .to_string(),
        ),
    ] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("it/config-policy-class-{name}:main");
        let ledger = genesis_ledger(&fluree, &ledger_id);
        let config_iri = config_graph_iri(&ledger_id);
        let trig = format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix ex: <http://example.org/ns/> .

            GRAPH <{config_iri}> {{
                <urn:config:main> rdf:type f:LedgerConfig .
                <urn:config:main> f:policyDefaults <urn:config:policy> .
                <urn:config:policy> f:policyClass {list} .
            }}
        "
        );
        fluree
            .stage_owned(ledger)
            .upsert_turtle(&trig)
            .execute()
            .await
            .unwrap_or_else(|e| panic!("{name}: config write: {e}"));

        let view = fluree.db(&ledger_id).await.unwrap();
        let config = view.ledger_config().expect("config should be attached");
        let mut classes = config
            .policy
            .as_ref()
            .and_then(|p| p.policy_class.clone())
            .unwrap_or_default();
        classes.sort();
        assert_eq!(
            classes,
            ["http://example.org/ns/A", "http://example.org/ns/B"],
            "{name}"
        );
    }
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
        "select": "?v",
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
        "select": "?v",
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
    let opts_with_override = GovernanceOptions {
        default_allow: Some(true),
        ..Default::default()
    };
    let merged = config_resolver::merge_policy_opts(resolved, &opts_with_override);
    assert_eq!(
        merged.default_allow,
        Some(false),
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

    // Test actual gating behavior via merge_policy_opts. The identity that
    // gates the override is the auth-layer-verified `server_identity`.
    let opts_as = |server_identity: Option<&str>| GovernanceOptions {
        default_allow: Some(true),
        server_identity: server_identity.map(VerifiedIdentity::new),
        ..Default::default()
    };

    // Admin identity → override permitted (opts.default_allow=true passes through)
    let merged_admin =
        config_resolver::merge_policy_opts(resolved, &opts_as(Some("did:key:admin")));
    assert_eq!(
        merged_admin.default_allow,
        Some(true),
        "admin identity should be permitted to override"
    );

    // Unknown identity → override denied (config.default_allow=false applied)
    let merged_user = config_resolver::merge_policy_opts(resolved, &opts_as(Some("did:key:user")));
    assert_eq!(
        merged_user.default_allow,
        Some(false),
        "non-admin identity should be denied override"
    );

    // No identity → override denied (config.default_allow=false applied)
    let merged_none = config_resolver::merge_policy_opts(resolved, &opts_as(None));
    assert_eq!(
        merged_none.default_allow,
        Some(false),
        "no identity should be denied override"
    );

    // The allow-listed DID carried only as the policy `identity` (what a
    // caller can write into `opts`) must not satisfy the allow-list.
    let identity_only = GovernanceOptions {
        identity: Some("did:key:admin".into()),
        ..opts_as(None)
    };
    let merged_identity_only = config_resolver::merge_policy_opts(resolved, &identity_only);
    assert_eq!(
        merged_identity_only.default_allow,
        Some(false),
        "opts.identity alone must not authorize an override"
    );
}

/// Query `ex:alice`'s name through a policy-wrapped view built from `opts`.
async fn names_through_policy(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    opts: GovernanceOptions,
) -> serde_json::Value {
    let view = fluree.db(ledger_id).await.expect("db");
    let view = fluree.wrap_policy(view, &opts).await.expect("wrap policy");
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    fluree
        .query(&view, &query)
        .await
        .expect("query")
        .to_jsonld(&ledger_state.snapshot)
        .expect("to_jsonld")
}

/// The same allow-list driven through `wrap_policy` and a real query. Config
/// closes policy (`f:defaultAllow false`); a request asking to open it
/// (`default_allow: true`) is honored only for the allow-listed verified
/// identity, so the row is visible to it and hidden from everyone else.
#[tokio::test]
async fn policy_override_identity_restricted_end_to_end() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-identity-e2e:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "ex:name": "Alice"
            }),
        )
        .await
        .expect("seed")
        .ledger;

    let config_iri = config_graph_iri(ledger_id);
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

    // Every request names a policy identity so a real policy context is
    // built; with no identity at all there is nothing to enforce. The only
    // thing that varies is the verified identity.
    let opens_as = |server_identity: Option<&str>| GovernanceOptions {
        identity: Some("did:key:user".into()),
        default_allow: Some(true),
        server_identity: server_identity.map(VerifiedIdentity::new),
        ..Default::default()
    };

    assert_eq!(
        names_through_policy(&fluree, ledger_id, opens_as(Some("did:key:admin"))).await,
        json!(["Alice"]),
        "the allow-listed verified identity may open policy"
    );
    assert_eq!(
        names_through_policy(&fluree, ledger_id, opens_as(Some("did:key:other"))).await,
        json!([]),
        "a verified identity outside the allow-list is denied; config's closed policy wins"
    );
    assert_eq!(
        names_through_policy(&fluree, ledger_id, opens_as(None)).await,
        json!([]),
        "an anonymous request is denied; config's closed policy wins"
    );
    let identity_only = GovernanceOptions {
        identity: Some("did:key:admin".into()),
        ..opens_as(None)
    };
    assert_eq!(
        names_through_policy(&fluree, ledger_id, identity_only).await,
        json!([]),
        "opts.identity naming the allow-listed DID must not open policy"
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

    // Two LedgerConfig nodes: alpha (defaultAllow=false) and beta
    // (defaultAllow=true). The config guard refuses a second subject, so they
    // are seeded the way history written before the guard reaches a ledger.
    commit_unchecked(
        &fluree,
        genesis_ledger(&fluree, ledger_id),
        &json!({
            "@context": {"f": "https://ns.flur.ee/db#"},
            "@graph": [
                {
                    "@id": "urn:config:alpha",
                    "@type": "f:LedgerConfig",
                    "@graph": "config",
                    "f:policyDefaults": {"@id": "urn:config:alpha-policy", "f:defaultAllow": false}
                },
                {
                    "@id": "urn:config:beta",
                    "@type": "f:LedgerConfig",
                    "@graph": "config",
                    "f:policyDefaults": {"@id": "urn:config:beta-policy", "f:defaultAllow": true}
                }
            ]
        }),
    )
    .await;

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

    // Step 2: Enable SHACL in config; the violation fails
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
        }}
    "
    );
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed")
        .ledger;
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
        "with SHACL enabled, the violation is rejected: {err:?}"
    );

    // Step 3: Write config disabling SHACL
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

/// `sh:class` value-set: the enumerated members of the target class live in
/// the **shapes-source graph**, while the referencing data lives in a different
/// (default) graph. Membership resolution unions the focus node's data graph
/// with the `f:shapesSource` vocabulary graph, so a shared value-set (here: US
/// states) is honoured cross-graph.
///
/// Pre-fix, `validate_class_constraint` looked up each value's `rdf:type` only
/// in the focus node's own graph, so the vocabulary living in the shapes graph
/// was invisible and the valid insert would have been rejected as a false
/// violation.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_class_value_set_in_shapes_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-class-value-set:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Shapes graph carries BOTH the shape and the value-set vocabulary (the US
    // states). Config points `f:shapesSource` at it.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/shapes> {{
            ex:PersonShape rdf:type sh:NodeShape ;
                           sh:targetClass ex:Person ;
                           sh:property ex:pshape_state .
            ex:pshape_state sh:path ex:homeState ;
                            sh:class ex:USState .

            ex:illinois rdf:type ex:USState .
            ex:iowa rdf:type ex:USState .
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
        .expect("config + shape + vocabulary write should succeed");
    let ledger = result.ledger;

    // A person in the DEFAULT graph referencing a state defined only in the
    // shapes graph must PASS — membership unions the shapes-source graph.
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person",
                "ex:homeState": {"@id": "ex:illinois"}
            }),
        )
        .await
        .expect(
            "sh:class value-set member defined in the f:shapesSource graph must \
             satisfy the constraint even though the data is in another graph",
        );
    let ledger = result.ledger;

    // Multiple subjects referencing the SAME value-set member in one
    // transaction exercise the per-transaction membership memo — the second
    // `(ex:illinois, ex:USState, g0)` check is a cache hit.
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:carol", "@type": "ex:Person", "ex:homeState": {"@id": "ex:illinois"}},
                    {"@id": "ex:dave", "@type": "ex:Person", "ex:homeState": {"@id": "ex:illinois"}}
                ]
            }),
        )
        .await
        .expect("repeated value-set members in one transaction must pass (memoized)");
    let ledger = result.ledger;

    // A person referencing a non-member must be REJECTED — ex:atlantis is not
    // typed ex:USState in any consulted graph.
    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "@type": "ex:Person",
                "ex:homeState": {"@id": "ex:atlantis"}
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "value outside the sh:class value-set must be rejected: {err:?}"
    );
}

/// Shapes graph, catalog graph and config for the `f:classMembershipSource`
/// tests: listings must reference an `ex:Commodity`, and the commodities are
/// typed in their own catalog graph, which is neither the focus graph nor the
/// shapes source.
#[cfg(feature = "shacl")]
fn catalog_membership_trig(ledger_id: &str, with_membership_source: bool) -> String {
    let config_iri = config_graph_iri(ledger_id);
    let membership_source = if with_membership_source {
        r"
            <urn:config:shacl> f:classMembershipSource <urn:config:catalog-ref> .
            <urn:config:catalog-ref> rdf:type f:GraphRef ;
                                     f:graphSource <urn:config:catalog-source> .
            <urn:config:catalog-source> f:graphSelector <http://example.org/catalog> ."
    } else {
        ""
    };
    format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .

        GRAPH <http://example.org/shapes> {{
            ex:ListingShape rdf:type sh:NodeShape ;
                            sh:targetClass ex:Listing ;
                            sh:property ex:lshape_commodity .
            ex:lshape_commodity sh:path ex:commodity ;
                                sh:class ex:Commodity .
        }}

        GRAPH <http://example.org/catalog> {{
            ex:coffee rdf:type ex:Commodity .
        }}

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true .
            <urn:config:shacl> f:shapesSource <urn:config:shapes-ref> .
            <urn:config:shapes-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:shapes-source> .
            <urn:config:shapes-source> f:graphSelector <http://example.org/shapes> .{membership_source}
        }}
    "
    )
}

#[cfg(feature = "shacl")]
fn listing_in_org_graph(id: &str, commodity: &str) -> String {
    format!(
        r"
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix ex: <http://example.org/> .
        GRAPH <http://example.org/org-a> {{
            ex:{id} rdf:type ex:Listing ;
                    ex:commodity ex:{commodity} .
        }}
    "
    )
}

#[cfg(feature = "shacl")]
fn assert_shacl_violation(err: fluree_db_api::ApiError, why: &str) {
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "{why}: {err:?}"
    );
}

/// Without `f:classMembershipSource`, membership is the focus graph plus the
/// shapes source only, so a type asserted in a separate catalog graph doesn't
/// count.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_class_ignores_other_graphs_without_membership_source() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-class-catalog-unlisted:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&catalog_membership_trig(ledger_id, false))
        .execute()
        .await
        .expect("config + shapes + catalog write should succeed")
        .ledger;

    let err = fluree
        .stage_owned(ledger)
        .upsert_turtle(&listing_in_org_graph("listing-1", "coffee"))
        .execute()
        .await
        .unwrap_err();
    assert_shacl_violation(err, "the catalog graph is not consulted unless configured");
}

/// `f:classMembershipSource` adds a graph to the `sh:class` membership lookup,
/// so data in one named graph can reference members typed in a dedicated
/// catalog graph while non-members are still rejected.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_class_membership_source_adds_a_catalog_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-class-catalog-listed:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&catalog_membership_trig(ledger_id, true))
        .execute()
        .await
        .expect("config + shapes + catalog write should succeed")
        .ledger;

    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&listing_in_org_graph("listing-1", "coffee"))
        .execute()
        .await
        .expect("a commodity typed in the configured catalog graph satisfies sh:class")
        .ledger;

    let err = fluree
        .stage_owned(ledger)
        .upsert_turtle(&listing_in_org_graph("listing-2", "tea"))
        .execute()
        .await
        .unwrap_err();
    assert_shacl_violation(
        err,
        "a value typed in no consulted graph must still be rejected",
    );
}

/// An unknown `f:classMembershipSource` graph fails like an unknown
/// `f:shapesSource` instead of being silently skipped.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_class_membership_source_rejects_unknown_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-class-catalog-missing:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let trig = catalog_membership_trig(ledger_id, true).replace(
        "f:graphSelector <http://example.org/catalog>",
        "f:graphSelector <http://example.org/nowhere>",
    );
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shapes + catalog write should succeed")
        .ledger;

    let err = fluree
        .stage_owned(ledger)
        .upsert_turtle(&listing_in_org_graph("listing-1", "coffee"))
        .execute()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}")
            .contains("f:classMembershipSource graph 'http://example.org/nowhere' not found"),
        "an unknown membership graph must be reported: {err:?}"
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

/// Per-graph SHACL governance reaches graphs a SPARQL UPDATE writes through a
/// `GRAPH ?g` template. The graph is only known once the WHERE runs, so it
/// must still be counted among the transaction's graphs: missed, the Warn
/// override below would not apply and the ledger-wide Reject would.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_per_graph_mode_applies_to_graph_variable_targets() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-pergraph-graph-var:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

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
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shape write should succeed")
        .ledger;

    let update = |graph: &str| {
        format!(
            r"INSERT {{ GRAPH ?g {{
                  <http://example.org/p1> a <http://example.org/Person> }} }}
              WHERE {{ BIND(<{graph}> AS ?g) }}"
        )
    };
    let stage = |ledger: fluree_db_api::LedgerState, sparql: String| {
        let fluree = &fluree;
        async move {
            let parsed = fluree_db_sparql::parse_sparql(&sparql);
            assert!(!parsed.has_errors(), "{:?}", parsed.diagnostics);
            let ast = parsed.ast.expect("SPARQL AST");
            let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
            let txn = fluree_db_transact::lower_sparql_update_ast(
                &ast,
                &mut ns,
                fluree_db_transact::TxnOpts::default(),
            )
            .expect("lower SPARQL UPDATE");
            fluree.stage_owned(ledger).txn(txn).execute().await
        }
    };

    let ledger = stage(ledger, update("http://example.org/scratch"))
        .await
        .expect("violation in the Warn-mode graph must pass when reached through ?g")
        .ledger;

    let err = stage(ledger, update("http://example.org/strict"))
        .await
        .expect_err("violation in a Reject-mode graph reached through ?g must fail");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected a SHACL violation: {err:?}"
    );
}

/// Per-graph SHACL governance reaches a new graph written beside a new graph
/// the transaction only deletes from. The commit does not register the
/// second, so the first takes the id the second would have taken (it sorts
/// first); the staged flakes must be routed to that id, or validation looks
/// under another id and the Reject override, the only one enabled, never
/// applies.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_per_graph_mode_reaches_a_new_graph_beside_one_only_deleted_from() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-pergraph-delete-only:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .

        ex:PersonShape rdf:type sh:NodeShape ;
                       sh:targetClass ex:Person ;
                       sh:property ex:pshape_name .
        ex:pshape_name sh:path ex:name ;
                       sh:minCount 1 .
        ex:alice ex:name "Alice" .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:graphOverrides <urn:config:strict-override> .
            <urn:config:strict-override> rdf:type f:GraphConfig ;
                                         f:targetGraph <http://example.org/strict> ;
                                         f:shaclDefaults <urn:config:strict-shacl> .
            <urn:config:strict-shacl> f:shaclEnabled true ;
                                      f:validationMode f:ValidationReject .
        }}
    "#
    );
    let ledger = fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config + shape write should succeed")
        .ledger;

    let parsed = fluree_db_sparql::parse_sparql(
        r"PREFIX ex: <http://example.org/>
          DELETE { GRAPH <http://example.org/also> { ?s ex:name ?o } }
          INSERT { GRAPH <http://example.org/strict> { ex:bob a ex:Person } }
          WHERE { ?s ex:name ?o }",
    );
    assert!(!parsed.has_errors(), "{:?}", parsed.diagnostics);
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &parsed.ast.expect("SPARQL AST"),
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    let err = fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .expect_err("a nameless ex:Person in the Reject-mode graph must fail");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "expected a SHACL violation: {err:?}"
    );
}

/// Shape requiring `ex:name` on every `ex:Person`, ledger-wide SHACL in
/// Reject mode, and two named graphs registered in order — `g/a`, then `g/b`
/// holding alice's name. A transaction numbers its own graphs from a small
/// local counter, so a write to `g/b` or to a new graph carries a local id
/// that names a different ledger graph; validation must read the graph the
/// write actually lands in.
#[cfg(feature = "shacl")]
async fn seed_person_shape_and_named_graphs(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix ex: <http://example.org/> .

        ex:PersonShape rdf:type sh:NodeShape ;
                       sh:targetClass ex:Person ;
                       sh:property ex:pshape_name .
        ex:pshape_name sh:path ex:name ;
                       sh:minCount 1 .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            <urn:config:shacl> f:shaclEnabled true ;
                               f:validationMode f:ValidationReject .
        }}
        GRAPH <http://example.org/g/a> {{ ex:x ex:other "1" . }}
        GRAPH <http://example.org/g/b> {{ ex:alice ex:name "Alice" . }}
    "#
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed shape, config and named graphs")
        .ledger
}

#[cfg(feature = "shacl")]
async fn sparql_type_person(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    graph: &str,
    person: &str,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    let parsed = fluree_db_sparql::parse_sparql(&format!(
        "INSERT DATA {{ GRAPH <{graph}> {{ <http://example.org/{person}> a <http://example.org/Person> }} }}"
    ));
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    fluree.stage_owned(ledger).txn(txn).execute().await
}

#[cfg(feature = "shacl")]
async fn jsonld_type_person(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    graph: &str,
    person: &str,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    fluree
        .update(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "graph": graph,
                "insert": {"@id": format!("ex:{person}"), "@type": "ex:Person"}
            }),
        )
        .await
}

#[cfg(feature = "shacl")]
const NAMED_GRAPH_TARGETS: [&str; 3] = [
    "http://example.org/g/a",
    "http://example.org/g/b",
    "http://example.org/g/new",
];

#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_rejects_violation_in_any_named_graph_sparql() {
    for graph in NAMED_GRAPH_TARGETS {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger =
            seed_person_shape_and_named_graphs(&fluree, "it/shacl-ng-reject-sparql:main").await;
        let err = sparql_type_person(&fluree, ledger, graph, "bob")
            .await
            .expect_err("a nameless ex:Person must be rejected");
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::ShaclViolation(_)
                )
            ),
            "<{graph}>: expected a SHACL violation: {err:?}"
        );
    }
}

#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_rejects_violation_in_any_named_graph_jsonld() {
    for graph in NAMED_GRAPH_TARGETS {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger =
            seed_person_shape_and_named_graphs(&fluree, "it/shacl-ng-reject-jsonld:main").await;
        let err = jsonld_type_person(&fluree, ledger, graph, "bob")
            .await
            .expect_err("a nameless ex:Person must be rejected");
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::ShaclViolation(_)
                )
            ),
            "<{graph}>: expected a SHACL violation: {err:?}"
        );
    }
}

#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_reads_the_named_graph_the_write_lands_in() {
    // Alice's name is in g/b, so typing her there conforms on both surfaces.
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_person_shape_and_named_graphs(&fluree, "it/shacl-ng-own-data-sparql:main").await;
    sparql_type_person(&fluree, ledger, "http://example.org/g/b", "alice")
        .await
        .expect("SPARQL: alice has a name in the graph she is typed in");
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger =
        seed_person_shape_and_named_graphs(&fluree, "it/shacl-ng-own-data-jsonld:main").await;
    jsonld_type_person(&fluree, ledger, "http://example.org/g/b", "alice")
        .await
        .expect("JSON-LD: alice has a name in the graph she is typed in");
}

// =============================================================================
// Test 16: shapes without config do not enforce (explicit-only SHACL)
// =============================================================================

/// Shapes alone never enable SHACL: a ledger that holds shapes but no config
/// enabling SHACL accepts the record they forbid (#1979's control).
/// `config_write_json_ld_nested_groups_land_in_config` refuses the same record
/// once the documented config enables SHACL.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_shapes_without_config_do_not_enforce() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-shapes-only:main";
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

    // The record the shape forbids commits: no config enables SHACL.
    fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person"
            }),
        )
        .await
        .expect("no config enables SHACL, so the shapes are not enforced");
}

/// Run a SPARQL UPDATE against `ledger`.
async fn sparql_update(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> fluree_db_api::Result<fluree_db_api::TransactResult> {
    let ast = fluree_db_sparql::parse_sparql(sparql)
        .ast
        .expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE");
    fluree.stage_owned(ledger).txn(txn).execute().await
}

/// A shape requiring `ex:name` on every `ex:Person`, in the default graph.
#[cfg(feature = "shacl")]
fn person_name_shape() -> serde_json::Value {
    json!({
        "@context": {"sh": "http://www.w3.org/ns/shacl#", "ex": "http://example.org/"},
        "@id": "ex:PersonShape",
        "@type": "sh:NodeShape",
        "sh:targetClass": {"@id": "ex:Person"},
        "sh:property": [{"sh:path": {"@id": "ex:name"}, "sh:minCount": 1}]
    })
}

/// Explicit-only SHACL: whether a ledger that holds shapes enforces them
/// depends on its config's SHACL group and nothing else. Each config here is
/// written through SPARQL, the record the shape forbids through SPARQL too.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_posture_is_explicit_only() {
    let shacl_enabled = |flag: &str| {
        format!(
            "<urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> . \
             <urn:config:shacl> f:shaclEnabled {flag} ."
        )
    };
    let per_graph_only = "<urn:config:main> a f:LedgerConfig ; \
                            f:graphOverrides <urn:config:ga> . \
                          <urn:config:ga> a f:GraphConfig ; \
                            f:targetGraph <http://example.org/g/a> ; \
                            f:shaclDefaults <urn:config:ga-shacl> . \
                          <urn:config:ga-shacl> f:shaclEnabled true ."
        .to_string();
    // (case, #config content, graph the forbidden record is written to,
    // whether it is refused)
    let cases: [(&str, String, Option<&str>, bool); 8] = [
        (
            "a config with only reasoning defaults",
            "<urn:config:main> a f:LedgerConfig ; f:reasoningDefaults <urn:config:r> . \
             <urn:config:r> f:reasoningModes f:rdfs ."
                .to_string(),
            None,
            false,
        ),
        (
            "a bare f:LedgerConfig",
            "<urn:config:main> a f:LedgerConfig .".to_string(),
            None,
            false,
        ),
        (
            "an empty SHACL group (read as absent)",
            "<urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .".to_string(),
            None,
            false,
        ),
        ("f:shaclEnabled false", shacl_enabled("false"), None, false),
        ("f:shaclEnabled true", shacl_enabled("true"), None, true),
        (
            "f:shaclEnabled true, write to a named graph",
            shacl_enabled("true"),
            Some("http://example.org/g/a"),
            true,
        ),
        (
            "per-graph f:shaclEnabled true, write to that graph",
            per_graph_only.clone(),
            Some("http://example.org/g/a"),
            true,
        ),
        (
            "per-graph f:shaclEnabled true, write to the default graph",
            per_graph_only,
            None,
            false,
        ),
    ];
    for (case, config, graph, refused) in cases {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = "it/shacl-explicit-only:main";
        let ledger = fluree
            .insert(genesis_ledger(&fluree, ledger_id), &person_name_shape())
            .await
            .expect("shape insert")
            .ledger;
        let ledger = sparql_update(
            &fluree,
            ledger,
            &format!(
                "PREFIX f: <https://ns.flur.ee/db#> \
                 INSERT DATA {{ GRAPH <{}> {{ {config} }} }}",
                config_graph_iri(ledger_id)
            ),
        )
        .await
        .unwrap_or_else(|e| panic!("{case}: config write: {e}"))
        .ledger;

        let record = "<http://example.org/alice> a <http://example.org/Person> .";
        let write = match graph {
            Some(g) => format!("INSERT DATA {{ GRAPH <{g}> {{ {record} }} }}"),
            None => format!("INSERT DATA {{ {record} }}"),
        };
        let result = sparql_update(&fluree, ledger, &write).await;
        match (refused, result) {
            (true, Err(err)) => assert!(
                matches!(
                    err,
                    fluree_db_api::ApiError::Transact(
                        fluree_db_transact::TransactError::ShaclViolation(_)
                    )
                ),
                "{case}: expected a SHACL violation: {err:?}"
            ),
            (true, Ok(_)) => panic!("{case}: the forbidden record must be refused"),
            (false, Err(err)) => panic!("{case}: the record must commit: {err:?}"),
            (false, Ok(_)) => {}
        }
    }
}

/// `f:shaclEnabled true` with a local `f:shapesSource` naming a graph the
/// ledger does not have: data writes fail closed, and the config repair
/// commits (a config-only write resolves no shapes source), after which data
/// writes commit.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn unknown_local_shapes_source_fails_closed_and_the_config_repair_commits() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/shacl-unknown-shapes-graph:main";
    let config_iri = config_graph_iri(ledger_id);
    let ledger = fluree
        .stage_owned(genesis_ledger(&fluree, ledger_id))
        .upsert_turtle(&format!(
            r"
            @prefix f: <https://ns.flur.ee/db#> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            GRAPH <{config_iri}> {{
                <urn:config:main> rdf:type f:LedgerConfig .
                <urn:config:main> f:shaclDefaults <urn:config:shacl> .
                <urn:config:shacl> f:shaclEnabled true .
                <urn:config:shacl> f:shapesSource <urn:config:shapes-ref> .
                <urn:config:shapes-ref> rdf:type f:GraphRef ;
                                        f:graphSource <urn:config:shapes-src> .
                <urn:config:shapes-src> f:graphSelector <http://example.org/no-such-graph> .
            }}
        "
        ))
        .execute()
        .await
        .expect("config write")
        .ledger;
    let record = json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:alice",
        "ex:name": "Alice"
    });

    let err = fluree
        .insert(ledger.clone(), &record)
        .await
        .expect_err("SHACL is on and its shapes graph does not exist: fail closed");
    assert!(err.to_string().contains("no-such-graph"), "{err}");

    let ledger = sparql_update(
        &fluree,
        ledger,
        &format!(
            "PREFIX f: <https://ns.flur.ee/db#> \
             DELETE {{ GRAPH <{config_iri}> {{ <urn:config:shacl> f:shaclEnabled ?on }} }} \
             INSERT {{ GRAPH <{config_iri}> {{ <urn:config:shacl> f:shaclEnabled false }} }} \
             WHERE {{ GRAPH <{config_iri}> {{ <urn:config:shacl> f:shaclEnabled ?on }} }}"
        ),
    )
    .await
    .expect("the config repair resolves no shapes source and commits")
    .ledger;
    fluree
        .insert(ledger, &record)
        .await
        .expect("after the repair data writes commit");
}

/// A Turtle insert on a ledger that holds shapes but no config enabling SHACL
/// is not validated (`shacl_turtle_insert_rejected_when_violating` is the
/// enabled twin).
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_turtle_insert_without_config_is_not_validated() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, "it/shacl-turtle-no-config:main");
    let ledger = fluree
        .insert(ledger, &person_name_shape())
        .await
        .expect("shape insert")
        .ledger;
    fluree
        .insert_turtle(
            ledger,
            "@prefix ex: <http://example.org/> . ex:charlie a ex:Person .",
        )
        .await
        .expect("no config enables SHACL, so the Turtle insert is not validated");
}

/// Push replays a commit under the receiving ledger's configured posture.
/// The source commits here skip the source's own checks, so the forbidden
/// record reaches the receiver either way: where the pushed config enables
/// SHACL it is refused, and where the ledger only holds shapes it is not.
/// The receiver is another instance of the same ledger, as in replication.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn push_validates_only_where_config_enables_shacl() {
    for enabled in [false, true] {
        let source = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("it/shacl-push-{enabled}:main");
        let mut seed = vec![json!({
            "@id": "ex:PersonShape",
            "@type": "sh:NodeShape",
            "sh:targetClass": {"@id": "ex:Person"},
            "sh:property": [{"sh:path": {"@id": "ex:name"}, "sh:minCount": 1}]
        })];
        if enabled {
            seed.push(support::shacl_enabled_config_node());
        }
        let src = commit_unchecked(
            &source,
            genesis_ledger(&source, &ledger_id),
            &json!({
                "@context": {"sh": "http://www.w3.org/ns/shacl#", "ex": "http://example.org/"},
                "@graph": seed
            }),
        )
        .await;
        commit_unchecked(
            &source,
            src,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:alice",
                "@type": "ex:Person"
            }),
        )
        .await;

        let target = FlureeBuilder::memory().build_memory();
        target
            .create_ledger(&ledger_id)
            .await
            .expect("create target");
        let result = target
            .push_commits(
                &ledger_id,
                newest_commits(&source, &ledger_id, 100).await,
                &GovernanceOptions::default(),
                &target.default_index_config(),
            )
            .await;
        match (enabled, result) {
            (false, Ok(pushed)) => assert_eq!(pushed.accepted, 2),
            (false, Err(err)) => panic!("shapes without config are not enforced on push: {err}"),
            (true, Err(err)) => {
                assert_eq!(err.status_code(), 422, "{err}");
                assert!(err.to_string().contains("SHACL"), "{err}");
            }
            (true, Ok(_)) => panic!("the pushed config enables SHACL: the push must be refused"),
        }
    }
}

/// The newest `limit` commits of `ledger_id`, oldest first, as a push sends
/// them.
async fn newest_commits(
    from: &fluree_db_api::Fluree,
    ledger_id: &str,
    limit: usize,
) -> fluree_db_api::PushCommitsRequest {
    let handle = from.ledger_cached(ledger_id).await.expect("source handle");
    let export = from
        .export_commit_range(
            &handle,
            &fluree_db_api::ExportCommitsRequest {
                limit: Some(limit),
                ..Default::default()
            },
        )
        .await
        .expect("export commits");
    let mut commits = export.commits;
    commits.reverse();
    fluree_db_api::PushCommitsRequest {
        commits,
        blobs: export.blobs,
        missing_blobs: Vec::new(),
        merged_commits: Vec::new(),
    }
}

#[cfg(feature = "shacl")]
fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create dir");
    for entry in std::fs::read_dir(from).expect("read dir") {
        let entry = entry.expect("dir entry");
        let dest = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).expect("copy file");
        }
    }
}

/// A pushed commit is validated in each named graph it writes, under that
/// graph's config, whether or not the receiving ledger is indexed: a graph
/// the ledger has registered, one the pushed commits register (in the same
/// commit or an earlier one of the push), and the default graph alike. The
/// receiver is an unindexed copy of the sender. SHACL is on ledger-wide and
/// off for `ex:a` and for `ex:g7`, which no commit has written before the
/// push; each pushed commit holds a record the shapes forbid, committed at
/// the source under a requested warn.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn push_validates_each_named_graph_under_its_own_config() {
    let dir = tempfile::tempdir().expect("tempdir");
    let base = dir.path().join("base");
    let file_fluree = |path: &std::path::Path| {
        FlureeBuilder::file(path.to_str().expect("utf-8 path"))
            .without_indexing()
            .build()
            .expect("file fluree")
    };
    let name = "it/push-graph-config";
    let ledger_id = format!("{name}:main");
    let cfg = config_graph_iri(&ledger_id);
    {
        let fluree = file_fluree(&base);
        fluree.create_ledger(name).await.expect("create");
        for trig in [
            format!(
                "@prefix f: <https://ns.flur.ee/db#> . @prefix sh: <http://www.w3.org/ns/shacl#> .
                 @prefix ex: <http://example.org/> .
                 ex:PersonShape a sh:NodeShape ; sh:targetClass ex:Person ;
                   sh:property [ sh:path ex:name ; sh:minCount 1 ] .
                 GRAPH <{cfg}> {{
                   <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> ;
                     f:graphOverrides <urn:config:ga>, <urn:config:g7> .
                   <urn:config:shacl> f:shaclEnabled true .
                   <urn:config:ga> a f:GraphConfig ; f:targetGraph ex:a ;
                     f:shaclDefaults <urn:config:gashacl> .
                   <urn:config:gashacl> f:shaclEnabled false .
                   <urn:config:g7> a f:GraphConfig ; f:targetGraph ex:g7 ;
                     f:shaclDefaults <urn:config:g7shacl> .
                   <urn:config:g7shacl> f:shaclEnabled false .
                 }}"
            ),
            "@prefix ex: <http://example.org/> . GRAPH ex:a { ex:x1 ex:p 1 . }".to_string(),
            "@prefix ex: <http://example.org/> . GRAPH ex:z { ex:x2 ex:p 2 . }".to_string(),
        ] {
            fluree
                .graph(&ledger_id)
                .transact()
                .upsert_turtle(&trig)
                .commit()
                .await
                .expect("seed");
        }
        // Retire the root's WAL before copying the root. Dropping the instance
        // can leave its log live, and the log's flush thread deletes a segment
        // once the files it covers are on the device, which a copy running at
        // the same time finds missing.
        fluree.disconnect().await;
    }

    let bad = |graph: &str| match graph {
        "" => "ex:bad a ex:Person".to_string(),
        g => format!("GRAPH {g} {{ ex:bad a ex:Person }}"),
    };
    let fine = |graph: &str| format!("GRAPH {graph} {{ ex:fine ex:p 1 }}");
    // (case, the pushed commits' INSERT DATA bodies, whether the push is accepted)
    let cases: Vec<(&str, Vec<String>, bool)> = vec![
        ("a new graph", vec![bad("ex:g2")], false),
        (
            "two new graphs",
            vec![format!("{} {}", fine("ex:g1"), bad("ex:g2"))],
            false,
        ),
        ("a registered graph", vec![bad("ex:z")], false),
        ("the default graph", vec![bad("")], false),
        ("a graph with SHACL off", vec![bad("ex:a")], true),
        ("a new graph with SHACL off", vec![bad("ex:g7")], true),
        (
            "a graph an earlier pushed commit registers",
            vec![fine("ex:g3"), bad("ex:g3")],
            false,
        ),
    ];
    for (i, (case, bodies, accepted)) in cases.into_iter().enumerate() {
        let src_dir = dir.path().join(format!("src-{i}"));
        let tgt_dir = dir.path().join(format!("tgt-{i}"));
        copy_dir(&base, &src_dir);
        copy_dir(&base, &tgt_dir);
        let source = file_fluree(&src_dir);
        for body in &bodies {
            source
                .graph(&ledger_id)
                .transact()
                .txn_opts(fluree_db_api::TxnOpts {
                    validation_mode: Some(fluree_db_core::ledger_config::ValidationMode::Warn),
                    ..Default::default()
                })
                .sparql_update(&format!(
                    "PREFIX ex: <http://example.org/> INSERT DATA {{ {body} }}"
                ))
                .commit()
                .await
                .unwrap_or_else(|e| panic!("{case}: source commit: {e}"));
        }
        let target = file_fluree(&tgt_dir);
        let result = target
            .push_commits(
                &ledger_id,
                newest_commits(&source, &ledger_id, bodies.len()).await,
                &GovernanceOptions::default(),
                &target.default_index_config(),
            )
            .await;
        match (accepted, result) {
            (true, Ok(pushed)) => assert_eq!(pushed.accepted, bodies.len(), "{case}"),
            (true, Err(err)) => panic!("{case}: SHACL is off for this graph: {err}"),
            (false, Err(err)) => {
                assert_eq!(err.status_code(), 422, "{case}: {err}");
                assert!(err.to_string().contains("SHACL"), "{case}: {err}");
            }
            (false, Ok(_)) => panic!("{case}: the shapes apply in this graph"),
        }
    }
}

/// Graphs a push registers are validated apart from the ledger's other
/// graphs, under the ids the ledger gives them when the commits apply, and
/// their data reads back from each graph after the ledger reloads. Here a
/// subject has one email in an existing graph and one in a graph the push
/// registers; each graph conforms to a one-email shape on its own.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn pushed_graphs_are_validated_apart_from_the_ledgers_other_graphs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file_fluree = |path: &std::path::Path| {
        FlureeBuilder::file(path.to_str().expect("utf-8 path"))
            .without_indexing()
            .build()
            .expect("file fluree")
    };
    let name = "it/push-graph-ids";
    let ledger_id = format!("{name}:main");
    let cfg = config_graph_iri(&ledger_id);
    let base = dir.path().join("base");
    {
        let fluree = file_fluree(&base);
        fluree.create_ledger(name).await.expect("create");
        fluree
            .graph(&ledger_id)
            .transact()
            .upsert_turtle(&format!(
                "@prefix f: <https://ns.flur.ee/db#> . @prefix sh: <http://www.w3.org/ns/shacl#> .
                 @prefix ex: <http://example.org/> .
                 ex:PersonShape a sh:NodeShape ; sh:targetSubjectsOf ex:email ;
                   sh:property [ sh:path ex:email ; sh:maxCount 1 ] .
                 GRAPH <{cfg}> {{
                   <urn:config:main> a f:LedgerConfig ; f:shaclDefaults <urn:config:shacl> .
                   <urn:config:shacl> f:shaclEnabled true .
                 }}
                 GRAPH ex:m {{ ex:s ex:email \"a@example.org\" . }}"
            ))
            .commit()
            .await
            .expect("seed");
        // Retire the root's WAL before copying it, as in the test above.
        fluree.disconnect().await;
    }
    let src_dir = dir.path().join("src");
    let tgt_dir = dir.path().join("tgt");
    copy_dir(&base, &src_dir);
    copy_dir(&base, &tgt_dir);
    let source = file_fluree(&src_dir);
    for body in [
        "GRAPH ex:n1 { ex:t ex:email \"t@example.org\" }",
        "GRAPH ex:n2 { ex:s ex:email \"b@example.org\" }",
    ] {
        source
            .graph(&ledger_id)
            .transact()
            .sparql_update(&format!(
                "PREFIX ex: <http://example.org/> INSERT DATA {{ {body} }}"
            ))
            .commit()
            .await
            .expect("source commit");
    }
    {
        let target = file_fluree(&tgt_dir);
        let pushed = target
            .push_commits(
                &ledger_id,
                newest_commits(&source, &ledger_id, 2).await,
                &GovernanceOptions::default(),
                &target.default_index_config(),
            )
            .await
            .expect("each graph conforms on its own");
        assert_eq!(pushed.accepted, 2);
    }
    let reloaded = file_fluree(&tgt_dir);
    for (graph, email) in [
        ("m", "a@example.org"),
        ("n1", "t@example.org"),
        ("n2", "b@example.org"),
    ] {
        let out = reloaded
            .query_connection(&json!({
                "@context": {"ex": "http://example.org/"},
                "from": format!("{ledger_id}#http://example.org/{graph}"),
                "select": "?email",
                "where": {"@id": "?s", "ex:email": "?email"}
            }))
            .await
            .unwrap_or_else(|e| panic!("query graph {graph}: {e}"));
        let view = reloaded.ledger(&ledger_id).await.expect("reload");
        let rows = out.to_jsonld(&view.snapshot).expect("jsonld");
        assert_eq!(rows, json!([email]), "graph {graph}");
    }
}

/// A pushed flake in a graph that neither the receiving ledger nor the
/// pushed commits register has no graph id to apply under, so the push is
/// refused. Here the sender is a different ledger whose commit writes its
/// own config graph without registering it (a commit staged and committed
/// directly, without the transaction path's graph registration).
#[tokio::test]
async fn push_refuses_a_graph_nothing_registers() {
    let source = FlureeBuilder::memory().build_memory();
    let src_id = "it/push-unregistered-src:main";
    commit_unchecked(
        &source,
        genesis_ledger(&source, src_id),
        &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:name": "Alice"},
                support::shacl_enabled_config_node()
            ]
        }),
    )
    .await;

    let dir = tempfile::tempdir().expect("tempdir");
    let tgt_id = "it/push-unregistered-tgt:main";
    let file_fluree = || {
        FlureeBuilder::file(dir.path().to_str().expect("utf-8 path"))
            .without_indexing()
            .build()
            .expect("file fluree")
    };
    let target = file_fluree();
    target.create_ledger(tgt_id).await.expect("create target");
    let result = target
        .push_commits(
            tgt_id,
            newest_commits(&source, src_id, 1).await,
            &GovernanceOptions::default(),
            &target.default_index_config(),
        )
        .await;
    match result {
        Err(err) => {
            assert_eq!(err.status_code(), 422, "{err}");
            let message = err.to_string();
            assert!(
                message.contains(&config_graph_iri(src_id))
                    && message.contains("neither this ledger nor the pushed commits register"),
                "{message}"
            );
        }
        Ok(_) => {
            let reload = file_fluree().ledger(tgt_id).await.map(|_| ());
            panic!("accepted a graph nothing registers; the ledger reloads as: {reload:?}");
        }
    }
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
    let ledger = support::enable_shacl(&fluree, result.ledger).await;

    // Violating Turtle: an ex:Person with no ex:name.
    let turtle = r"
        @prefix ex: <http://example.org/> .
        ex:charlie a ex:Person .
    ";

    let err = fluree
        .insert_turtle(ledger, turtle)
        .await
        .expect_err("Turtle insert must honor SHACL reject mode");

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
    let admin = config_resolver::merge_datalog_opts(
        resolved,
        Some(&VerifiedIdentity::new("did:key:admin")),
    )
    .expect("datalog config");
    assert!(!admin.enabled, "config still says disabled");
    assert!(
        admin.override_allowed,
        "admin identity → override permitted"
    );

    // Non-admin identity → override denied
    let other = config_resolver::merge_datalog_opts(
        resolved,
        Some(&VerifiedIdentity::new("did:key:other")),
    )
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
    let shacl = config_resolver::merge_shacl_opts(resolved, None, None)
        .expect("shacl config should be present");

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
            &err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { value, .. }
            ) if value == "alice@example.com"
        ),
        "duplicate email should trigger unique constraint violation: {err:?}"
    );
}

/// Test: a duplicate reference is reported as the IRI it points to.
#[tokio::test]
async fn unique_ref_value_reports_its_iri() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-ref:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [
                    {"@id": "ex:badge", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:badge": {"@id": "ex:badge-7"}}
                ]
            }),
        )
        .await
        .unwrap();
    let ledger = write_unique_config(&fluree, result.ledger, ledger_id)
        .await
        .ledger;

    let err = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:badge": {"@id": "ex:badge-7"}
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            &err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { value, .. }
            ) if value == "http://example.org/badge-7"
        ),
        "{err:?}"
    );
}

/// Every write lane runs the same post-staging checks, uniqueness included:
/// a plain Turtle insert (the streaming lane that skips JSON-LD) refuses the
/// duplicate a JSON-LD insert refuses. It used to commit it.
#[tokio::test]
async fn unique_enforced_on_turtle_insert() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/unique-turtle:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": {"ex": "http://example.org/", "f": "https://ns.flur.ee/db#"},
                "@graph": [
                    {"@id": "ex:email", "f:enforceUnique": true},
                    {"@id": "ex:alice", "ex:email": "alice@example.com"}
                ]
            }),
        )
        .await
        .unwrap()
        .ledger;
    let ledger = write_unique_config(&fluree, ledger, ledger_id).await.ledger;
    let t = ledger.t();

    let err = fluree
        .stage_owned(ledger)
        .insert_turtle(
            "<http://example.org/bob> <http://example.org/email> \"alice@example.com\" .",
        )
        .execute()
        .await
        .expect_err("the Turtle lane enforces uniqueness");
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(
                fluree_db_transact::TransactError::UniqueConstraintViolation { .. }
            )
        ),
        "{err:?}"
    );
    assert_eq!(
        fluree.ledger(ledger_id).await.unwrap().t(),
        t,
        "nothing committed"
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

// =============================================================================
// f:policySource fail-closed behavior
//
// `build_policy_context` (the path used by server transact handlers and the CLI
// insert command) must NOT silently fall back to the default graph when the
// configured `f:policySource` cannot be resolved. Falling back would re-enable
// any default-graph policy rules under a config the operator clearly intended
// to redirect, and would also swallow the cross-ledger / temporal rejection
// emitted by `resolve_policy_source_g_ids`.
// =============================================================================

#[tokio::test]
async fn policy_source_unknown_graph_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-source-unknown:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Some data in the default graph (irrelevant to the assertion, but proves
    // the fallback would have had something to enforce against).
    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice"}]
            }),
        )
        .await
        .unwrap();

    // Config points f:policySource at a graph IRI that does not exist in the
    // registry. Pre-fix this logged a warning and returned `[0]`.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:policySource <urn:config:policy-ref> .
            <urn:config:policy-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:policy-source> .
            <urn:config:policy-source> f:graphSelector <http://example.org/does-not-exist> .
        }}
    "
    );
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");
    let ledger = r2.ledger;

    let opts = GovernanceOptions::default();
    let err = fluree_db_api::build_policy_context(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await
    .expect_err("unknown f:policySource graph must fail closed");

    let msg = err.to_string();
    assert!(
        msg.contains("policySource") && msg.contains("not found"),
        "expected unknown-graph error, got: {msg}"
    );
}

#[tokio::test]
async fn policy_source_cross_ledger_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/policy-source-xledger:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [{"@id": "ex:alice", "@type": "ex:User"}]
            }),
        )
        .await
        .unwrap();

    // Config asks for cross-ledger policy via f:ledger. `resolve_policy_source_g_ids`
    // already rejects this; the regression we're guarding is the prior fallback
    // in `resolve_policy_graphs_from_config` that swallowed the error.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:policySource <urn:config:policy-ref> .
            <urn:config:policy-ref> rdf:type f:GraphRef ;
                                    f:graphSource <urn:config:policy-source> .
            <urn:config:policy-source> f:ledger <urn:fluree:model-ledger:main> ;
                                       f:graphSelector <http://example.org/policy> .
        }}
    "
    );
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");
    let ledger = r2.ledger;

    let opts = GovernanceOptions::default();
    let err = fluree_db_api::build_policy_context(
        &ledger.snapshot,
        ledger.novelty.as_ref(),
        Some(ledger.novelty.as_ref()),
        ledger.t(),
        &opts,
    )
    .await
    .expect_err("cross-ledger f:policySource must fail closed");

    let msg = err.to_string();
    assert!(
        msg.contains("cross-ledger") || msg.contains("f:ledger"),
        "expected cross-ledger rejection, got: {msg}"
    );
}

// =============================================================================
// f:constraintsSource fail-closed behavior
//
// The previous resolver silently skipped unknown graph selectors (a
// uniqueness constraint would just stop being enforced) and silently ignored
// `f:ledger` / `f:atT` / `f:trustPolicy` / `f:rollbackGuard`. Both are
// security-relevant: an operator who configures a constraint source
// should never get less enforcement than they asked for. Each must now
// surface as a parse error at transact time.
// =============================================================================

#[tokio::test]
async fn constraints_source_unknown_graph_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/constraints-source-unknown:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed annotation + data
    let r1 = fluree
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

    // Enable unique and point constraintsSource at a graph that doesn't exist.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:transactDefaults <urn:config:transact> .
            <urn:config:transact> f:uniqueEnabled true .
            <urn:config:transact> f:constraintsSource <urn:config:constraints-ref> .
            <urn:config:constraints-ref> rdf:type f:GraphRef ;
                                         f:graphSource <urn:config:constraints-src> .
            <urn:config:constraints-src> f:graphSelector <http://example.org/missing-constraints> .
        }}
    "
    );
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let err = fluree
        .insert(
            r2.ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "alice@example.com"
            }),
        )
        .await
        .expect_err("unknown constraintsSource graph must reject the transaction");

    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::Parse(_))
        ),
        "expected TransactError::Parse, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("constraintsSource") && msg.contains("not found"),
        "expected unknown-graph diagnostic, got: {msg}"
    );
}

#[tokio::test]
async fn constraints_source_cross_ledger_fails_closed() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/constraints-source-xledger:main";
    let ledger = genesis_ledger(&fluree, ledger_id);

    let r1 = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "ex": "http://example.org/",
                    "f": "https://ns.flur.ee/db#"
                },
                "@graph": [{"@id": "ex:email", "f:enforceUnique": true}]
            }),
        )
        .await
        .unwrap();

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:transactDefaults <urn:config:transact> .
            <urn:config:transact> f:uniqueEnabled true .
            <urn:config:transact> f:constraintsSource <urn:config:constraints-ref> .
            <urn:config:constraints-ref> rdf:type f:GraphRef ;
                                         f:graphSource <urn:config:constraints-src> .
            <urn:config:constraints-src> f:ledger <urn:fluree:model-ledger:main> ;
                                         f:graphSelector <http://example.org/constraints> .
        }}
    "
    );
    let r2 = fluree
        .stage_owned(r1.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write should succeed");

    let err = fluree
        .insert(
            r2.ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@id": "ex:bob",
                "ex:email": "bob@example.com"
            }),
        )
        .await
        .expect_err("cross-ledger constraintsSource must reject the transaction");

    let msg = err.to_string();
    assert!(
        msg.contains("cross-ledger") || msg.contains("f:ledger"),
        "expected cross-ledger rejection, got: {msg}"
    );
}

// =============================================================================
// reasoningModes value shapes: string literal and RDF collection
// =============================================================================
//
// `f:reasoningModes f:rdfs` (IRI objects) is covered by
// `reasoning_defaults_apply` above. Users also naturally write string
// literals and RDF collections — both previously parsed to *no modes*
// silently, so config-declared reasoning never engaged and queries
// returned non-entailed results with no error. These pin the permissive
// reader.

/// Shared body: seed subProperty ontology + data, write the given
/// `f:reasoningModes` statement(s) into config, query WITHOUT a
/// "reasoning" key, and expect the RDFS expansion to fire.
///
/// `modes_stmts` is one or more full Turtle statements on
/// `<urn:config:reasoning>` (plus any list-node triples they need).
async fn assert_reasoning_modes_shape_engages(ledger_id: &str, modes_stmts: &str) {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);

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

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            {modes_stmts}
        }}
    "
    );
    fluree
        .stage_owned(result.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "from": ledger_id,
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });
    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    assert_eq!(
        jsonld,
        json!(["Alice"]),
        "config `{modes_stmts}` should engage RDFS reasoning"
    );
}

#[tokio::test]
async fn reasoning_modes_string_literal_engages() {
    assert_reasoning_modes_shape_engages(
        "it/config-reasoning-modes-string:main",
        r#"<urn:config:reasoning> f:reasoningModes "rdfs" ."#,
    )
    .await;
}

// The two collection tests write the `rdf:first`/`rdf:rest` structure
// explicitly (a JSON-LD `@list` or any list-producing ingest lands the
// same triples) because the Turtle parser does not support the `( .. )`
// collection shorthand.

#[tokio::test]
async fn reasoning_modes_rdf_collection_engages() {
    assert_reasoning_modes_shape_engages(
        "it/config-reasoning-modes-collection:main",
        r#"<urn:config:reasoning> f:reasoningModes <urn:config:modes-list> .
            <urn:config:modes-list> rdf:first "rdfs" ;
                                    rdf:rest  rdf:nil ."#,
    )
    .await;
}

#[tokio::test]
async fn reasoning_modes_collection_of_iris_engages() {
    assert_reasoning_modes_shape_engages(
        "it/config-reasoning-modes-iri-collection:main",
        r"<urn:config:reasoning> f:reasoningModes <urn:config:modes-list> .
            <urn:config:modes-list> rdf:first f:rdfs ;
                                    rdf:rest  rdf:nil .",
    )
    .await;
}

// =============================================================================
// Test 20: ledger-configured f:defaultAllow reaches identity-carrying requests
// =============================================================================

/// Seed two named subjects and (optionally) a policy config declaring
/// `f:defaultAllow <value>`. Returns the ledger id.
async fn seed_default_allow_ledger(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    configured_default_allow: Option<bool>,
) {
    let ledger = genesis_ledger(fluree, ledger_id);
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/ns/"},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "ex:name": "Alice"},
                    {"@id": "ex:bob", "@type": "ex:User", "ex:name": "Bob"}
                ]
            }),
        )
        .await
        .expect("seed");

    let Some(default_allow) = configured_default_allow else {
        return;
    };

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:policyDefaults <urn:config:policy> .
            <urn:config:policy> f:defaultAllow {default_allow} .
        }}
    "
    );

    fluree
        .stage_owned(result.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");
}

/// Run `SELECT ?name WHERE { ?s ex:name ?name }` with the given `opts` and
/// return the names visible to that request.
async fn visible_names(
    fluree: &support::MemoryFluree,
    ledger_id: &str,
    opts: serde_json::Value,
) -> Vec<String> {
    let mut query = json!({
        "@context": {"ex": "http://example.org/ns/"},
        "from": ledger_id,
        "select": "?name",
        "where": {"@id": "?s", "ex:name": "?name"}
    });
    if !opts.is_null() {
        query["opts"] = opts;
    }

    let result = fluree.query_connection(&query).await.expect("query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let jsonld = result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld");

    let mut names: Vec<String> = jsonld
        .as_array()
        .expect("array result")
        .iter()
        .filter_map(|v| v.as_str().map(std::string::ToString::to_string))
        .collect();
    names.sort();
    names
}

/// The operator scenario: a ledger configured `f:defaultAllow true` must apply
/// to a request that carries nothing but an identity.
///
/// Before `GovernanceOptions::default_allow` became tri-state this returned
/// nothing: `merge_policy_opts` treats any request with policy inputs as an
/// override, and a bare `bool` made the derive-default `false` indistinguishable
/// from "the caller never said," so config's `true` was silently discarded and
/// the identity fell through to default-deny.
#[tokio::test]
async fn config_default_allow_true_applies_to_identity_only_request() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-default-allow-identity:main";
    seed_default_allow_ledger(&fluree, ledger_id, Some(true)).await;

    let names = visible_names(
        &fluree,
        ledger_id,
        json!({"identity": "did:key:z6MkUnknownIdentity"}),
    )
    .await;

    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string()],
        "config's f:defaultAllow true must govern an identity-only request"
    );
}

/// A request that names `default-allow` explicitly still wins over config —
/// in both directions.
#[tokio::test]
async fn request_default_allow_overrides_config_both_ways() {
    let fluree = FlureeBuilder::memory().build_memory();

    // Config says allow; request explicitly says deny → deny wins.
    let open_ledger = "it/config-default-allow-request-false:main";
    seed_default_allow_ledger(&fluree, open_ledger, Some(true)).await;
    let names = visible_names(
        &fluree,
        open_ledger,
        json!({"identity": "did:key:z6MkUnknownIdentity", "default-allow": false}),
    )
    .await;
    assert!(
        names.is_empty(),
        "explicit request default-allow:false must override config true; got: {names:?}"
    );

    // Config says deny; request explicitly says allow → allow wins (the config
    // here uses the default OverrideControl::AllowAll).
    let closed_ledger = "it/config-default-allow-request-true:main";
    seed_default_allow_ledger(&fluree, closed_ledger, Some(false)).await;
    let names = visible_names(
        &fluree,
        closed_ledger,
        json!({"identity": "did:key:z6MkUnknownIdentity", "default-allow": true}),
    )
    .await;
    assert_eq!(
        names,
        vec!["Alice".to_string(), "Bob".to_string()],
        "explicit request default-allow:true must override config false"
    );
}

/// Unchanged posture: with no ledger config at all, an identity-only request is
/// still fail-closed. This is the same pin as the server-side
/// `identity_without_policy_class_default_allow_false_denies_all`, asserted at
/// the API layer so the tri-state change can't quietly widen the default.
#[tokio::test]
async fn identity_only_without_config_still_denies_all() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-default-allow-no-config:main";
    seed_default_allow_ledger(&fluree, ledger_id, None).await;

    let names = visible_names(
        &fluree,
        ledger_id,
        json!({"identity": "did:key:z6MkUnknownIdentity"}),
    )
    .await;

    assert!(
        names.is_empty(),
        "no config + identity only must stay fail-closed; got: {names:?}"
    );
}

/// Anonymous connection queries still enforce configured ledger defaults.
#[tokio::test]
async fn anonymous_request_enforces_config_default_allow_false() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-default-allow-anonymous:main";
    seed_default_allow_ledger(&fluree, ledger_id, Some(false)).await;

    let names = visible_names(&fluree, ledger_id, serde_json::Value::Null).await;

    assert!(
        names.is_empty(),
        "configured deny must apply to anonymous reads"
    );
}

// =============================================================================
// Reasoning defaults: mode spelling, `f:schemaSource`, and the SPARQL path
// =============================================================================
//
// `reasoning_defaults_apply` above covers one config shape on the JSON-LD
// connection path, which resolves config while building its view. These
// cover the path that does not: a view built by `GraphDb::from_ledger_state`
// and queried with `Fluree::query`, which reaches query preparation carrying
// no resolved config. That is the crate-level shape of fluree/db#1577 —
// every test below fails without `complete_config_defaults`.
//
// They also widen the config surface along the three dimensions a real
// config varies in: the mode written as an uppercase IRI (`f:RDFS`), an
// `f:schemaSource` GraphRef naming the default graph, and the SPARQL
// front-end.

/// Seed the subProperty ontology + data, then write a reasoning config.
///
/// `extra_stmts` carries any additional statements on
/// `<urn:config:reasoning>` the caller needs.
async fn seed_reasoning_defaults_with(
    ledger_id: &str,
    modes_stmt: &str,
    extra_stmts: &str,
) -> fluree_db_api::Fluree {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = genesis_ledger(&fluree, ledger_id);

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

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:reasoningDefaults <urn:config:reasoning> .
            {modes_stmt}
            {extra_stmts}
        }}
    "
    );
    fluree
        .stage_owned(result.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write");

    fluree
}

/// An `f:schemaSource` block: a `f:GraphRef` whose nested `f:graphSource`
/// selects the ledger's default graph.
const SCHEMA_SOURCE_DEFAULT_GRAPH: &str = r"
            <urn:config:reasoning> f:schemaSource <urn:config:reasoning:source> .
            <urn:config:reasoning:source> rdf:type f:GraphRef .
            <urn:config:reasoning:source> f:graphSource <urn:config:reasoning:source:graph> .
            <urn:config:reasoning:source:graph> f:graphSelector f:defaultGraph .";

/// Run the JSON-LD query that the RDFS subProperty expansion should answer.
///
/// Deliberately goes through `GraphDb::from_ledger_state` + `Fluree::query`
/// rather than `query_connection`: that pair is the crate's documented
/// quickstart path (see the `lib.rs` module docs), and it is the one that
/// hands query preparation a view with no resolved config — the same shape
/// the ledger-scoped server routes build, and the shape that made
/// fluree/db#1577 reproduce. Routing these through `query_connection`
/// instead would make every assertion below pass with or without the fix.
async fn entailed_names_jsonld(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
) -> serde_json::Value {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger_state);
    let result = fluree.query(&db, &query).await.expect("query");
    result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld")
}

/// Run the same query as SPARQL, over the same bare view.
async fn entailed_names_sparql(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    prologue: &str,
) -> Vec<serde_json::Value> {
    let sparql = format!(
        "{prologue}PREFIX ex: <http://example.org/>
         SELECT ?v WHERE {{ ex:alice ex:name ?v }}"
    );
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger_state);
    let result = fluree.query(&db, &sparql).await.expect("sparql query");
    let json = result
        .to_sparql_json(&ledger_state.snapshot)
        .expect("to_sparql_json");
    support::normalize_sparql_bindings(&json)
}

/// Mode names are matched case-insensitively once the namespace prefix is
/// stripped, so the uppercase IRI `f:RDFS` names the same mode as `f:rdfs`.
#[tokio::test]
async fn reasoning_modes_uppercase_iri_engages() {
    let ledger_id = "it/config-reasoning-uppercase-mode:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:RDFS .",
        "",
    )
    .await;

    assert_eq!(
        entailed_names_jsonld(&fluree, ledger_id).await,
        json!(["Alice"]),
        "`f:reasoningModes f:RDFS` should engage RDFS reasoning"
    );
}

/// Config defaults reach the SPARQL front-end, not only JSON-LD.
#[tokio::test]
async fn reasoning_defaults_apply_on_sparql_path() {
    let ledger_id = "it/config-reasoning-sparql:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        "",
    )
    .await;

    let bindings = entailed_names_sparql(&fluree, ledger_id, "").await;
    assert_eq!(
        bindings.len(),
        1,
        "config RDFS defaults should apply on the SPARQL path, got {bindings:?}"
    );
    assert_eq!(bindings[0]["v"]["value"], json!("Alice"));
}

/// An `f:schemaSource` naming the default graph resolves to the graph the
/// ontology already lives in, so it neither adds nor removes entailments.
#[tokio::test]
async fn reasoning_defaults_with_default_graph_schema_source_engage() {
    let ledger_id = "it/config-reasoning-schema-source:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        SCHEMA_SOURCE_DEFAULT_GRAPH,
    )
    .await;

    assert_eq!(
        entailed_names_jsonld(&fluree, ledger_id).await,
        json!(["Alice"]),
        "an `f:schemaSource` selecting the default graph should not suppress reasoning"
    );
}

/// All three dimensions at once — uppercase mode IRI, `f:schemaSource`,
/// SPARQL — with a pragma control that leaves the mode source as the only
/// difference between the two queries.
#[tokio::test]
async fn config_reasoning_matches_pragma_reasoning() {
    let ledger_id = "it/config-reasoning-combined:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:RDFS .",
        SCHEMA_SOURCE_DEFAULT_GRAPH,
    )
    .await;

    // Control: the same query with the modes supplied per-query instead.
    let with_pragma = entailed_names_sparql(&fluree, ledger_id, "# PRAGMA reasoning: rdfs\n").await;
    assert_eq!(
        with_pragma.len(),
        1,
        "pragma control should entail, got {with_pragma:?}"
    );

    let without_pragma = entailed_names_sparql(&fluree, ledger_id, "").await;
    assert_eq!(
        without_pragma.len(),
        1,
        "config defaults should entail without the pragma, got {without_pragma:?}"
    );
    assert_eq!(without_pragma[0]["v"]["value"], json!("Alice"));
}

// =============================================================================
// Config defaults vs. an explicit view-level reasoning wrapper
// =============================================================================
//
// Config supplies *defaults*. A caller who attaches reasoning to the view has
// made a decision, and `with_reasoning_precedence` overwrites `reasoning`
// outright, so applying config defaults after the caller would discard that
// decision with no way to recover it: `effective_reasoning` arbitrates only
// between the wrapper and the query's own modes, never against a wrapper that
// was already thrown away.
//
// Both directions are pinned, so a guard that simply stops applying config
// cannot pass.

/// Run the entailment query against a caller-built view.
async fn entailed_names_for_view(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    view: &fluree_db_api::GraphDb,
) -> serde_json::Value {
    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"}
    });
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let result = fluree.query(view, &query).await.expect("query");
    result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld")
}

/// A caller who turns reasoning off keeps it off, even though the ledger
/// configures `f:reasoningModes f:rdfs`.
#[tokio::test]
async fn explicit_view_reasoning_none_survives_config_defaults() {
    let ledger_id = "it/config-reasoning-explicit-none:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        "",
    )
    .await;

    // The documented quickstart shape: load a view, then attach reasoning.
    let view = fluree
        .db(ledger_id)
        .await
        .expect("db")
        .with_reasoning(fluree_db_query::ir::reasoning::ReasoningModes::none());

    assert_eq!(
        entailed_names_for_view(&fluree, ledger_id, &view).await,
        json!([]),
        "an explicit `with_reasoning(none())` must not be overwritten by config defaults"
    );
}

/// The converse: a caller who turns reasoning on keeps it on, even though the
/// ledger configures `f:reasoningModes f:none`. Without this, a guard that
/// merely stopped applying config would look correct.
#[tokio::test]
async fn explicit_view_reasoning_on_survives_config_none() {
    let ledger_id = "it/config-reasoning-explicit-on:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:none .",
        "",
    )
    .await;

    let view = fluree
        .db(ledger_id)
        .await
        .expect("db")
        .with_reasoning(fluree_db_query::ir::reasoning::ReasoningModes::rdfs());

    assert_eq!(
        entailed_names_for_view(&fluree, ledger_id, &view).await,
        json!(["Alice"]),
        "an explicit `with_reasoning(rdfs())` must survive a config default of `f:none`"
    );
}

/// Control: with no wrapper attached, the ledger's config still governs.
/// This is the fluree/db#1577 behavior, and the guard above must not weaken it.
#[tokio::test]
async fn config_defaults_still_apply_without_an_explicit_wrapper() {
    let ledger_id = "it/config-reasoning-no-wrapper:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        "",
    )
    .await;

    let view = fluree.db(ledger_id).await.expect("db");

    assert_eq!(
        entailed_names_for_view(&fluree, ledger_id, &view).await,
        json!(["Alice"]),
        "config defaults must still apply to a view the caller left alone"
    );
}

// =============================================================================
// Transaction-requested SHACL validation mode (opts.validationMode)
// =============================================================================

/// Shared setup for the validation-mode override tests: a ledger with a
/// minCount shape on ex:Person and a config graph carrying the given SHACL
/// group body (caller supplies the `<urn:config:shacl>` triples).
#[cfg(feature = "shacl")]
async fn seed_shacl_mode_ledger(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    shacl_group_trig: &str,
) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    let result = fluree
        .insert(
            ledger,
            &json!({
                "@context": {
                    "sh": "http://www.w3.org/ns/shacl#",
                    "ex": "http://example.org/",
                    "xsd": "http://www.w3.org/2001/XMLSchema#"
                },
                "@graph": [
                    {
                        "@id": "ex:PersonShape",
                        "@type": "sh:NodeShape",
                        "sh:targetClass": {"@id": "ex:Person"},
                        "sh:property": [{
                            "sh:path": {"@id": "ex:name"},
                            "sh:minCount": 1
                        }]
                    },
                    // The allow-listed DID exists as a subject, as a policy
                    // identity would. The pre-fix gate decoded the policy
                    // context's identity, so it only ever matched an IRI the
                    // ledger knew; without this subject that gate denies an
                    // allow-listed policy identity for the wrong reason and
                    // `shacl_txn_validation_mode_ignores_policy_identity`
                    // would pass against the very bug it pins. Untyped, so the
                    // `ex:Person` shape does not target it.
                    {"@id": "did:key:remediator", "ex:name": "Remediator"}
                ]
            }),
        )
        .await
        .expect("shape insert");

    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        GRAPH <{config_iri}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> f:shaclDefaults <urn:config:shacl> .
            {shacl_group_trig}
        }}
    "
    );
    fluree
        .stage_owned(result.ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("config write")
        .ledger
}

/// A violating transaction document (ex:Person without ex:name).
#[cfg(feature = "shacl")]
fn violating_person() -> serde_json::Value {
    json!({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:nameless",
        "@type": "ex:Person"
    })
}

/// Under the default override control (`f:OverrideAll`), a transaction may
/// soften a configured `Reject` posture to warn-and-commit for itself only —
/// the standing posture still rejects other writers.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_txn_validation_mode_softens_reject_under_default_override() {
    use fluree_db_core::ledger_config::ValidationMode;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_shacl_mode_ledger(
        &fluree,
        "it/shacl-mode-soften:main",
        "<urn:config:shacl> f:shaclEnabled true .",
    )
    .await;

    // Without the opt: configured Reject applies.
    let err = fluree
        .insert(ledger.clone(), &violating_person())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "configured reject posture must reject: {err:?}"
    );

    // With opts.validationMode = warn: commits (findings logged, not fatal).
    let opts = fluree_db_transact::TxnOpts {
        validation_mode: Some(ValidationMode::Warn),
        ..Default::default()
    };
    fluree
        .stage_owned(ledger)
        .txn_opts(opts)
        .insert(&violating_person())
        .execute()
        .await
        .expect("warn-mode request under default override control must commit");
}

/// `f:overrideControl f:OverrideNone` pins the configured posture: a
/// transaction-requested warn is denied and the write still rejects.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_txn_validation_mode_denied_by_override_none() {
    use fluree_db_core::ledger_config::ValidationMode;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_shacl_mode_ledger(
        &fluree,
        "it/shacl-mode-pinned:main",
        r"<urn:config:shacl> f:shaclEnabled true .
          <urn:config:shacl> f:overrideControl f:OverrideNone .",
    )
    .await;

    let opts = fluree_db_transact::TxnOpts {
        validation_mode: Some(ValidationMode::Warn),
        ..Default::default()
    };
    let err = fluree
        .stage_owned(ledger)
        .txn_opts(opts)
        .insert(&violating_person())
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "OverrideNone must pin the configured reject posture: {err:?}"
    );
}

/// SHACL group: only `did:key:remediator` may soften the posture.
#[cfg(feature = "shacl")]
const SHACL_OVERRIDE_REMEDIATOR_ONLY: &str = r"<urn:config:shacl> f:shaclEnabled true .
          <urn:config:shacl> f:overrideControl <urn:config:oc> .
          <urn:config:oc> f:controlMode f:IdentityRestricted .
          <urn:config:oc> f:allowedIdentities <did:key:remediator> .";

/// A warn-mode write of the violating document by the given verified
/// identity, optionally under a policy context.
#[cfg(feature = "shacl")]
async fn warn_mode_insert_as(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    server_identity: Option<&str>,
    policy: Option<fluree_db_api::PolicyContext>,
) -> fluree_db_api::Result<()> {
    use fluree_db_core::ledger_config::ValidationMode;

    let opts = fluree_db_transact::TxnOpts {
        validation_mode: Some(ValidationMode::Warn),
        ..Default::default()
    };
    let mut builder = fluree
        .stage_owned(ledger)
        .txn_opts(opts)
        .server_identity(server_identity.map(VerifiedIdentity::new));
    if let Some(ctx) = policy {
        builder = builder.policy(ctx);
    }
    builder
        .insert(&violating_person())
        .execute()
        .await
        .map(|_| ())
}

/// The policy-wrapped staging path reports the violation as an HTTP-shaped
/// error; the plain path keeps the transact variant. Both are the rejection.
#[cfg(feature = "shacl")]
fn assert_shacl_rejected(result: fluree_db_api::Result<()>, what: &str) {
    let err = result.expect_err(what);
    let rejected = match &err {
        fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_)) => {
            true
        }
        fluree_db_api::ApiError::Http { status, message } => {
            *status == 400 && message.contains("SHACL validation failed")
        }
        _ => false,
    };
    assert!(rejected, "{what}: expected a SHACL rejection, got {err:?}");
}

/// An identity-restricted SHACL override is gated on the builder-supplied
/// verified identity: the allow-listed one may soften, others and anonymous
/// callers may not.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_txn_validation_mode_identity_restricted_end_to_end() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_shacl_mode_ledger(
        &fluree,
        "it/shacl-mode-identity:main",
        SHACL_OVERRIDE_REMEDIATOR_ONLY,
    )
    .await;

    warn_mode_insert_as(&fluree, ledger.clone(), Some("did:key:remediator"), None)
        .await
        .expect("the allow-listed verified identity may soften the posture");
    assert_shacl_rejected(
        warn_mode_insert_as(&fluree, ledger.clone(), Some("did:key:other"), None).await,
        "a verified identity outside the allow-list is denied",
    );
    assert_shacl_rejected(
        warn_mode_insert_as(&fluree, ledger, None, None).await,
        "an anonymous request is denied",
    );
}

/// The gate must read the verified identity, never the policy context's.
/// With no auth layer, the policy identity is whatever the caller wrote into
/// `opts.identity`, so a policy context for the allow-listed DID without a
/// verified identity must not soften the posture.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_txn_validation_mode_ignores_policy_identity() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_shacl_mode_ledger(
        &fluree,
        "it/shacl-mode-policy-identity:main",
        SHACL_OVERRIDE_REMEDIATOR_ONLY,
    )
    .await;

    // A policy context whose identity is the allow-listed DID, as the API
    // builds it from caller-supplied governance. `default_allow: true` keeps
    // the write policy from rejecting the flakes, so the only thing that can
    // refuse the write is SHACL.
    let remediator_policy = || async {
        fluree_db_api::build_transact_policy_context(
            &fluree,
            &ledger.snapshot,
            ledger.novelty.as_ref(),
            Some(ledger.novelty.as_ref()),
            ledger.t(),
            &GovernanceOptions {
                identity: Some("did:key:remediator".into()),
                default_allow: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("build policy context")
        .expect("identity is a policy input")
    };

    assert_shacl_rejected(
        warn_mode_insert_as(
            &fluree,
            ledger.clone(),
            None,
            Some(remediator_policy().await),
        )
        .await,
        "a policy context naming the allow-listed DID must not soften the posture",
    );
    // Control: the same policy context plus the verified identity is permitted,
    // so the rejection above is about provenance, not about the policy.
    warn_mode_insert_as(
        &fluree,
        ledger.clone(),
        Some("did:key:remediator"),
        Some(remediator_policy().await),
    )
    .await
    .expect("the verified identity alongside the same policy context is permitted");
}

/// Strengthening needs no permission: on a warn-mode ledger, a transaction
/// may request reject for itself even under `f:OverrideNone`.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn shacl_txn_validation_mode_reject_strengthens_warn() {
    use fluree_db_core::ledger_config::ValidationMode;

    let fluree = FlureeBuilder::memory().build_memory();
    let ledger = seed_shacl_mode_ledger(
        &fluree,
        "it/shacl-mode-strengthen:main",
        r"<urn:config:shacl> f:shaclEnabled true .
          <urn:config:shacl> f:validationMode f:ValidationWarn .
          <urn:config:shacl> f:overrideControl f:OverrideNone .",
    )
    .await;

    // Configured warn: the violation commits.
    let result = fluree
        .insert(ledger.clone(), &violating_person())
        .await
        .expect("warn posture commits violations");

    // Requested reject: strengthening is always honored.
    let opts = fluree_db_transact::TxnOpts {
        validation_mode: Some(ValidationMode::Reject),
        ..Default::default()
    };
    let err = fluree
        .stage_owned(result.ledger)
        .txn_opts(opts)
        .insert(&json!({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:nameless2",
            "@type": "ex:Person"
        }))
        .execute()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            fluree_db_api::ApiError::Transact(fluree_db_transact::TransactError::ShaclViolation(_))
        ),
        "requested reject must strengthen a warn posture: {err:?}"
    );
}

// =============================================================================
// Config resolution is cached on the read path; a config change must still win
// =============================================================================

/// Reads resolve config through the marker-keyed cache the write path uses.
/// This pins the invalidation contract from the outside: a commit to the config
/// graph advances `config_write_t`, so the next read over the new state must
/// miss and re-resolve rather than serve the previous config.
///
/// Not a pin for the cache itself, which passes with or without it. It is the
/// guard against a wrong key: anything that kept serving the first config after
/// a config write, keying on the ledger alone, say, fails here.
#[tokio::test]
async fn config_change_is_visible_to_the_next_read() {
    let ledger_id = "it/config-cache-invalidation:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        "",
    )
    .await;

    assert_eq!(
        entailed_names_jsonld(&fluree, ledger_id).await,
        json!(["Alice"]),
        "first read: the rdfs config governs"
    );

    // Flip the configured default to `none` in a new commit.
    let config_iri = config_graph_iri(ledger_id);
    let trig = format!(
        r"
        @prefix f: <https://ns.flur.ee/db#> .
        GRAPH <{config_iri}> {{
            <urn:config:reasoning> f:reasoningModes f:none .
        }}
    "
    );
    let ledger = fluree.ledger(ledger_id).await.expect("load ledger");
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("rewrite config");

    assert_eq!(
        entailed_names_jsonld(&fluree, ledger_id).await,
        json!([]),
        "second read: the config write must invalidate what the first read cached"
    );
}

// =============================================================================
// f:IdentityRestricted override control, end to end on the query path
// =============================================================================
//
// The identity `f:overrideControl` gates on is the auth-layer-verified
// `QueryExecutionOptions::server_identity`, threaded from the request boundary
// through query preparation. Nothing a caller writes in the body can stand in
// for it. These drive the whole path and assert on the result shape: config
// says `f:reasoningModes f:rdfs`, the query says `"reasoning": "none"`, so an
// entailed row means the override was denied and no row means it was allowed.

const REASONING_OVERRIDE_ADMIN_ONLY: &str = r"
            <urn:config:reasoning> f:overrideControl <urn:config:oc> .
            <urn:config:oc> f:controlMode f:IdentityRestricted .
            <urn:config:oc> f:allowedIdentities <did:key:admin> .";

fn reasoning_none_query(
    ledger_id: Option<&str>,
    opts: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?v",
        "where": {"@id": "ex:alice", "ex:name": "?v"},
        "reasoning": "none"
    });
    if let Some(ledger_id) = ledger_id {
        query["from"] = json!(ledger_id);
    }
    if let Some(opts) = opts {
        query["opts"] = opts;
    }
    query
}

/// Single-view path: the documented quickstart shape, over a bare view.
async fn entailed_names_as(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    server_identity: Option<&str>,
) -> serde_json::Value {
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger_state);
    let mut options = QueryExecutionOptions::new();
    if let Some(id) = server_identity {
        options = options.with_server_identity(VerifiedIdentity::new(id));
    }
    let result = fluree
        .query_with_options(&db, &reasoning_none_query(None, None), options)
        .await
        .expect("query");
    result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld")
}

/// Connection (`from`) path, where the body's `opts` are parsed into
/// `GovernanceOptions` inside the API.
async fn entailed_names_connection_as(
    fluree: &fluree_db_api::Fluree,
    ledger_id: &str,
    opts: Option<serde_json::Value>,
    server_identity: Option<&str>,
) -> serde_json::Value {
    let mut options = QueryExecutionOptions::new();
    if let Some(id) = server_identity {
        options = options.with_server_identity(VerifiedIdentity::new(id));
    }
    let result = fluree
        .query_connection_with_options(&reasoning_none_query(Some(ledger_id), opts), options)
        .await
        .expect("connection query");
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    result.to_jsonld(&ledger_state.snapshot).expect("to_jsonld")
}

#[tokio::test]
async fn reasoning_override_identity_restricted_end_to_end() {
    let ledger_id = "it/config-reasoning-identity-e2e:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        REASONING_OVERRIDE_ADMIN_ONLY,
    )
    .await;

    assert_eq!(
        entailed_names_as(&fluree, ledger_id, Some("did:key:admin")).await,
        json!([]),
        "the allow-listed verified identity may turn reasoning off"
    );
    assert_eq!(
        entailed_names_as(&fluree, ledger_id, Some("did:key:other")).await,
        json!(["Alice"]),
        "a verified identity outside the allow-list is denied; config's rdfs wins"
    );
    assert_eq!(
        entailed_names_as(&fluree, ledger_id, None).await,
        json!(["Alice"]),
        "an anonymous request is denied; config's rdfs wins"
    );
}

/// The allow-listed DID written into the body as the policy `identity` is not
/// a verified identity and must not unlock the override. `default-allow: true`
/// keeps the policy wrap from hiding the row, so an empty result can only mean
/// the override was permitted.
#[tokio::test]
async fn reasoning_override_ignores_body_identity() {
    let ledger_id = "it/config-reasoning-identity-body:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        REASONING_OVERRIDE_ADMIN_ONLY,
    )
    .await;
    let body_opts = json!({"identity": "did:key:admin", "default-allow": true});

    assert_eq!(
        entailed_names_connection_as(&fluree, ledger_id, Some(body_opts.clone()), None).await,
        json!(["Alice"]),
        "opts.identity naming the allow-listed DID must not authorize the override"
    );
    // Control: the same body with the verified identity present is permitted,
    // so the assertion above is about the identity's provenance, not the body.
    assert_eq!(
        entailed_names_connection_as(&fluree, ledger_id, Some(body_opts), Some("did:key:admin"))
            .await,
        json!([]),
        "the verified identity alongside the same body is permitted"
    );
    assert_eq!(
        entailed_names_connection_as(&fluree, ledger_id, None, Some("did:key:admin")).await,
        json!([]),
        "connection path: the verified identity alone is permitted"
    );
}

/// SPARQL shares the IR with JSON-LD; the pragma is the query-time override.
#[tokio::test]
async fn reasoning_override_identity_restricted_sparql() {
    let ledger_id = "it/config-reasoning-identity-sparql:main";
    let fluree = seed_reasoning_defaults_with(
        ledger_id,
        "<urn:config:reasoning> f:reasoningModes f:rdfs .",
        REASONING_OVERRIDE_ADMIN_ONLY,
    )
    .await;
    let sparql = "# PRAGMA reasoning: none\nPREFIX ex: <http://example.org/>\n\
                  SELECT ?v WHERE { ex:alice ex:name ?v }";
    let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
    let db = fluree_db_api::GraphDb::from_ledger_state(&ledger_state);

    let rows = |result: fluree_db_api::QueryResult| {
        let json = result
            .to_sparql_json(&ledger_state.snapshot)
            .expect("to_sparql_json");
        support::normalize_sparql_bindings(&json)
    };

    let admin = fluree
        .query_with_options(
            &db,
            sparql,
            QueryExecutionOptions::new()
                .with_server_identity(VerifiedIdentity::new("did:key:admin")),
        )
        .await
        .expect("sparql as admin");
    assert!(
        rows(admin).is_empty(),
        "the allow-listed verified identity may turn reasoning off (SPARQL)"
    );

    let anon = fluree
        .query_with_options(&db, sparql, QueryExecutionOptions::new())
        .await
        .expect("sparql anonymous");
    assert_eq!(
        rows(anon).len(),
        1,
        "an anonymous SPARQL request is denied; config's rdfs wins"
    );
}

// =============================================================================
// f:IdentityRestricted override control, end to end on the datalog path
// =============================================================================

/// Query-time rules are the datalog override. Config keeps datalog on but
/// forbids query-time rules, so the rule survives only for the allow-listed
/// verified identity, and only it derives the grandparent.
#[tokio::test]
async fn datalog_override_identity_restricted_end_to_end() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-datalog-identity-e2e:main";
    let ledger = genesis_ledger(&fluree, ledger_id);
    let ledger = fluree
        .insert(
            ledger,
            &json!({
                "@context": {"ex": "http://example.org/"},
                "@graph": [
                    {"@id": "ex:alice", "ex:parent": {"@id": "ex:bob"}},
                    {"@id": "ex:bob", "ex:parent": {"@id": "ex:charlie"}}
                ]
            }),
        )
        .await
        .expect("seed")
        .ledger;

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

    let query = json!({
        "@context": {"ex": "http://example.org/"},
        "select": "?grandparent",
        "where": {"@id": "ex:alice", "ex:grandparent": "?grandparent"},
        "reasoning": "datalog",
        "rules": [{
            "@context": {"ex": "http://example.org/"},
            "where": {"@id": "?person", "ex:parent": {"ex:parent": "?grandparent"}},
            "insert": {"@id": "?person", "ex:grandparent": {"@id": "?grandparent"}}
        }]
    });
    async fn grandparents_as(
        fluree: &fluree_db_api::Fluree,
        ledger_id: &str,
        query: &serde_json::Value,
        server_identity: Option<&str>,
    ) -> serde_json::Value {
        let ledger_state = fluree.ledger(ledger_id).await.expect("load ledger");
        let db = fluree_db_api::GraphDb::from_ledger_state(&ledger_state);
        let mut options = QueryExecutionOptions::new();
        if let Some(id) = server_identity {
            options = options.with_server_identity(VerifiedIdentity::new(id));
        }
        fluree
            .query_with_options(&db, query, options)
            .await
            .expect("query")
            .to_jsonld(&ledger_state.snapshot)
            .expect("to_jsonld")
    }

    assert_eq!(
        grandparents_as(&fluree, ledger_id, &query, Some("did:key:admin")).await,
        json!(["ex:charlie"]),
        "the allow-listed verified identity may inject a query-time rule"
    );
    assert_eq!(
        grandparents_as(&fluree, ledger_id, &query, Some("did:key:other")).await,
        json!([]),
        "a verified identity outside the allow-list has its rule stripped"
    );
    assert_eq!(
        grandparents_as(&fluree, ledger_id, &query, None).await,
        json!([]),
        "an anonymous request has its rule stripped"
    );
}

// =============================================================================
// Documented config recipes and examples
// =============================================================================

/// The enable recipes (`docs/ledger-config/writing-config.md#enabling-shacl`):
/// one attaches `f:shaclDefaults` to the ledger's existing config subject, the
/// other writes the conventional one when there is none. Either way the shapes
/// the ledger holds are enforced after it, under one `f:LedgerConfig` subject.
#[cfg(feature = "shacl")]
#[tokio::test]
async fn enable_recipe_enables_shacl_with_or_without_an_existing_config() {
    for existing in [true, false] {
        let fluree = FlureeBuilder::memory().build_memory();
        let ledger_id = format!("it/enable-recipe-{existing}:main");
        let mut ledger = fluree
            .insert(genesis_ledger(&fluree, &ledger_id), &person_name_shape())
            .await
            .expect("shape insert")
            .ledger;
        if existing {
            ledger = fluree
                .insert(
                    ledger,
                    &json!({
                        "@context": {"f": "https://ns.flur.ee/db#"},
                        "@id": "urn:config:existing",
                        "@type": "f:LedgerConfig",
                        "@graph": "config",
                        "f:policyDefaults": {"f:defaultAllow": true}
                    }),
                )
                .await
                .expect("existing config")
                .ledger;
        }
        let cfg = config_graph_iri(&ledger_id);
        let recipe = if existing {
            format!(
                "PREFIX f: <https://ns.flur.ee/db#>
                 INSERT {{
                   GRAPH <{cfg}> {{
                     ?c f:shaclDefaults <urn:fluree:{ledger_id}:config:shacl> .
                     <urn:fluree:{ledger_id}:config:shacl> f:shaclEnabled true .
                   }}
                 }}
                 WHERE {{
                   GRAPH <{cfg}> {{ ?c a f:LedgerConfig }}
                 }}"
            )
        } else {
            format!(
                "PREFIX f: <https://ns.flur.ee/db#>
                 INSERT DATA {{
                   GRAPH <{cfg}> {{
                     <urn:fluree:{ledger_id}:config:ledger> a f:LedgerConfig ;
                       f:shaclDefaults <urn:fluree:{ledger_id}:config:shacl> .
                     <urn:fluree:{ledger_id}:config:shacl> f:shaclEnabled true .
                   }}
                 }}"
            )
        };
        let ledger = sparql_update(&fluree, ledger, &recipe)
            .await
            .unwrap_or_else(|e| panic!("existing={existing}: the enable recipe commits: {e}"))
            .ledger;

        let err = fluree
            .insert(
                ledger,
                &json!({"@context": {"ex": "http://example.org/"}, "@id": "ex:alice", "@type": "ex:Person"}),
            )
            .await
            .expect_err("SHACL is enabled");
        assert!(
            matches!(
                err,
                fluree_db_api::ApiError::Transact(
                    fluree_db_transact::TransactError::ShaclViolation(_)
                )
            ),
            "existing={existing}: {err:?}"
        );
        assert!(
            config_diagnostic_codes(&fluree, &ledger_id)
                .await
                .is_empty(),
            "existing={existing}: one config subject, SHACL configured"
        );
    }
}

/// Examples in `docs/ledger-config/writing-config.md` and
/// `docs/concepts/reasoning.md`: the JSON-LD named-graph form, the
/// `"graph": "config"` insert, and an update whose `graph` key scopes its
/// WHERE clause and templates to the config graph.
#[tokio::test]
async fn documented_json_ld_config_forms_write_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/config-doc-forms:main";
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": { "f": "https://ns.flur.ee/db#" },
                "@id": config_graph_iri(ledger_id),
                "@graph": [
                    {
                        "@id": format!("urn:fluree:{ledger_id}:config:ledger"),
                        "@type": "f:LedgerConfig",
                        "f:shaclDefaults": { "f:shaclEnabled": false }
                    }
                ]
            }),
        )
        .await
        .expect("named-graph form")
        .ledger;
    let ledger = fluree
        .update(
            ledger,
            &json!({
                "@context": {"f": "https://ns.flur.ee/db#"},
                "graph": "config",
                "insert": {
                    "@id": format!("urn:fluree:{ledger_id}:config:ledger"),
                    "f:reasoningDefaults": {
                        "f:reasoningModes": {"@id": "f:RDFS"},
                        "f:overrideControl": {"@id": "f:OverrideAll"}
                    }
                }
            }),
        )
        .await
        .expect("the reasoning.md insert")
        .ledger;
    fluree
        .update(
            ledger,
            &json!({
                "@context": { "f": "https://ns.flur.ee/db#" },
                "graph": "config",
                "where": {
                    "@id": format!("urn:fluree:{ledger_id}:config:ledger"),
                    "f:shaclDefaults": "?group"
                },
                "delete": { "@id": "?group", "f:shaclEnabled": false },
                "insert": { "@id": "?group", "f:shaclEnabled": true }
            }),
        )
        .await
        .expect("the graph-key update");

    let view = fluree.db(ledger_id).await.unwrap();
    let config = view.ledger_config().expect("config read");
    assert_eq!(
        config.shacl.as_ref().and_then(|s| s.enabled),
        Some(true),
        "the update flipped the setting in the config graph"
    );
    assert!(
        config
            .reasoning
            .as_ref()
            .and_then(|r| r.modes.as_ref())
            .is_some_and(|m| !m.is_empty()),
        "the reasoning defaults landed in the config graph"
    );
}

/// The verification query in `docs/indexing-and-search/fulltext.md`: a
/// node-level `@graph` in `where` scopes the pattern to the config graph.
#[tokio::test]
async fn fulltext_doc_verification_query_reads_the_config_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/fulltext-doc:main";
    let cfg = config_graph_iri(ledger_id);
    let ledger = fluree
        .insert(
            genesis_ledger(&fluree, ledger_id),
            &json!({
                "@context": { "f": "https://ns.flur.ee/db#", "ex": "http://example.org/" },
                "@graph": [
                    {
                        "@id": format!("urn:fluree:{ledger_id}:config:ledger"),
                        "@type": "f:LedgerConfig",
                        "@graph": cfg,
                        "f:fullTextDefaults": {
                            "@type": "f:FullTextDefaults",
                            "f:defaultLanguage": "en",
                            "f:property": [
                                { "@type": "f:FullTextProperty", "f:target": { "@id": "ex:title" } }
                            ]
                        }
                    }
                ]
            }),
        )
        .await
        .expect("the fulltext.md config")
        .ledger;
    let rows = fluree
        .query_connection(&json!({
            "@context": { "f": "https://ns.flur.ee/db#" },
            "from": ledger_id,
            "from-named": [cfg],
            "where": [{ "@graph": cfg, "@id": "?cfg", "f:fullTextDefaults": "?defaults" }],
            "select": ["?cfg", "?defaults"]
        }))
        .await
        .expect("query")
        .to_jsonld(&ledger.snapshot)
        .expect("jsonld");
    assert_eq!(
        rows.as_array().map(Vec::len),
        Some(1),
        "the config row: {rows}"
    );
}
