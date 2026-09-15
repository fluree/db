//! Reserved system graphs are not write targets (`#txn-meta`).
//!
//! `urn:fluree:{ledger}#txn-meta` (g_id 1) holds commit provenance, and the
//! commit-prefix resolvers (`ledger_view::resolve_commit_prefix`, which serves
//! `fluree show` and `branch create --at`; `time_resolve::commit_to_t`, which
//! serves `--at` / `@commit:` / `fluree history`) answer a user-typed prefix by
//! scanning exactly the `fluree:commit:sha256:<hex>` subjects in that graph.
//! They trust what they find there, so anything a user can write into it steers
//! commit lookup.
//!
//! The rule — reserved system graphs are Fluree-internal and never a write
//! target — was established for graph management in `1cb3e8cb2` and extended to
//! graph sync in `0bb792a97`. Neither reached ordinary data writes, so
//! `INSERT DATA { GRAPH <urn:fluree:L#txn-meta> { … } }` committed, and a forged
//! record sharing a real commit's 12-character prefix permanently shadowed that
//! commit on every commit-lookup surface.
//!
//! **`#config` (g_id 2) is deliberately still writable through an ordinary
//! transaction** — `docs/ledger-config/` documents maintaining ledger
//! configuration that way. `config_graph_stays_writable_by_an_ordinary_write`
//! pins that asymmetry so it is not "tidied" into symmetry without the
//! documentation decision that would require.

use crate::support::genesis_ledger;
use fluree_db_api::{CommitRef, FlureeBuilder};
use serde_json::json;

/// Stage a raw SPARQL UPDATE, returning the post-commit ledger on success and
/// the error string on refusal.
///
/// Mirrors the parse → lower → stage path the server's `/v1/fluree/update`
/// endpoint takes. A refused write leaves the caller's own `ledger` clone
/// usable for the next attempt; a successful one advances `t`, so its result
/// must be threaded forward or the next write hits a commit conflict.
async fn try_sparql(
    fluree: &fluree_db_api::Fluree,
    ledger: fluree_db_api::LedgerState,
    sparql: &str,
) -> std::result::Result<fluree_db_api::LedgerState, String> {
    let parsed = fluree_db_sparql::parse_sparql(sparql);
    assert!(
        !parsed.has_errors(),
        "SPARQL parse errors for `{sparql}`: {:?}",
        parsed.diagnostics
    );
    let ast = parsed.ast.expect("SPARQL AST");
    let mut ns = fluree_db_transact::NamespaceRegistry::from_db(&ledger.snapshot);
    let txn = fluree_db_transact::lower_sparql_update_ast(
        &ast,
        &mut ns,
        fluree_db_transact::TxnOpts::default(),
    )
    .expect("lower SPARQL UPDATE to Txn IR");
    fluree
        .stage_owned(ledger)
        .txn(txn)
        .execute()
        .await
        .map(|r| r.ledger)
        .map_err(|e| e.to_string())
}

/// Every ordinary data-write spelling that can name a graph must refuse
/// `#txn-meta`, and each must still accept an ordinary user graph — so the
/// guard is about the reserved graph, not about named-graph writes.
#[tokio::test]
async fn sparql_data_writes_refuse_the_txn_meta_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reserved-write-sparql:main";
    let txn_meta = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let user_graph = "http://example.org/g1";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Three spellings reach `Txn::graph_delta`: a GRAPH block, the WITH
    // default target, and CREATE (which lowers to an update carrying only a
    // graph registration).
    let refused = [
        format!(
            r"INSERT DATA {{ GRAPH <{txn_meta}> {{ <fluree:commit:sha256:dead> <https://ns.flur.ee/db#t> 5 }} }}"
        ),
        format!(
            r"WITH <{txn_meta}> INSERT {{ <http://example.org/s> <http://example.org/p> 1 }} WHERE {{ }}"
        ),
        format!("CREATE GRAPH <{txn_meta}>"),
    ];
    for sparql in &refused {
        let err = try_sparql(&fluree, ledger.clone(), sparql)
            .await
            .expect_err(&format!("write into #txn-meta must be refused: {sparql}"));
        assert!(
            err.contains("reserved system graph") && err.contains("#txn-meta"),
            "expected a reserved-graph refusal naming the graph for `{sparql}`, got: {err}"
        );
    }

    // Controls: the identical spellings against a user graph still commit.
    // Each success advances `t`, so the result is threaded into the next.
    let allowed = [
        format!(
            r"INSERT DATA {{ GRAPH <{user_graph}> {{ <http://example.org/s> <http://example.org/p> 1 }} }}"
        ),
        format!(
            r"WITH <{user_graph}> INSERT {{ <http://example.org/s> <http://example.org/p> 2 }} WHERE {{ }}"
        ),
        "CREATE GRAPH <http://example.org/g2>".to_string(),
    ];
    let mut ledger = ledger;
    for sparql in &allowed {
        ledger = try_sparql(&fluree, ledger, sparql)
            .await
            .unwrap_or_else(|e| panic!("user-graph write must still succeed: {sparql}: {e}"));
    }
}

/// TriG reaches `graph_delta` through a *different* parser — the named-graph
/// blocks in `fluree-db-api/src/tx.rs`, with their own IRI→id counter, rather
/// than `parse/jsonld.rs`'s `GraphIdAssigner`. It gets its own case rather than
/// being assumed covered by the shared chokepoint.
///
/// `upsert_turtle`, not `insert_turtle`: `insert` on TriG hits the documented
/// `expected subject, found 'GRAPH'` trap (`docs/transactions/turtle.md`),
/// which is unrelated to this guard.
#[tokio::test]
async fn trig_data_writes_refuse_the_txn_meta_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reserved-write-trig:main";
    let txn_meta = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let ledger = genesis_ledger(&fluree, ledger_id);

    let forged = format!(
        "@prefix f: <https://ns.flur.ee/db#> .\nGRAPH <{txn_meta}> {{ <fluree:commit:sha256:deadbeef> f:t 5 . }}\n"
    );
    let err = fluree
        .stage_owned(ledger.clone())
        .upsert_turtle(&forged)
        .execute()
        .await
        .expect_err("TriG write into #txn-meta must be refused")
        .to_string();
    assert!(
        err.contains("reserved system graph") && err.contains("#txn-meta"),
        "expected a reserved-graph refusal naming the graph, got: {err}"
    );

    // Control: the same TriG shape into a user graph still commits.
    let ok =
        "@prefix ex: <http://example.org/> .\nGRAPH <http://example.org/g1> { ex:s ex:p 1 . }\n";
    fluree
        .stage_owned(ledger)
        .upsert_turtle(ok)
        .execute()
        .await
        .expect("user-graph TriG write must still succeed");
}

/// SPARQL and JSON-LD share the transaction IR, so the JSON-LD named-graph
/// spelling (`"@graph": "<iri>"` as a node-level selector) reaches the same
/// `graph_delta` and must be refused identically.
#[tokio::test]
async fn jsonld_data_writes_refuse_the_txn_meta_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reserved-write-jsonld:main";
    let txn_meta = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let ledger = genesis_ledger(&fluree, ledger_id);

    let forged = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@graph": [{
            "@id": "fluree:commit:sha256:deadbeefdeadbeef",
            "@graph": txn_meta,
            "f:t": 5
        }]
    });
    let err = fluree
        .stage_owned(ledger.clone())
        .insert(&forged)
        .execute()
        .await
        .expect_err("JSON-LD write into #txn-meta must be refused")
        .to_string();
    assert!(
        err.contains("reserved system graph") && err.contains("#txn-meta"),
        "expected a reserved-graph refusal naming the graph, got: {err}"
    );

    // Control: the same shape into a user graph still commits.
    let ok = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{
            "@id": "ex:s",
            "@graph": "http://example.org/g1",
            "ex:p": 1
        }]
    });
    fluree
        .stage_owned(ledger)
        .insert(&ok)
        .execute()
        .await
        .expect("user-graph JSON-LD write must still succeed");
}

/// The defect this guard exists for, end to end.
///
/// `resolve_commit_prefix` scans indexed commit subjects in g_id 1 and trusts
/// them. Injecting a record whose digest shares a real commit's 12-character
/// prefix used to make that commit unresolvable — "Ambiguous commit prefix
/// '…': matches [...]" — on `fluree show`, `--at`, `@commit:`, `fluree history`
/// and `branch create --at`.
///
/// The resolution assertion comes FIRST deliberately: with the guard reverted,
/// this test fails on the *symptom* (the real commit stops resolving), not
/// merely on the guard's absence.
#[tokio::test]
async fn a_forged_commit_record_cannot_shadow_a_real_commit_prefix() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_name = "forge";
    let ledger_id = "forge:main";
    let txn_meta = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let ctx = json!({"ex": "http://example.org/"});

    let ledger = fluree.create_ledger(ledger_name).await.expect("create");
    let r1 = fluree
        .insert(
            ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:a", "ex:n": 1}]}),
        )
        .await
        .expect("commit 1");
    let r2 = fluree
        .insert(
            r1.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:b", "ex:n": 2}]}),
        )
        .await
        .expect("commit 2");
    let target_id = r2.receipt.commit_id.clone();
    let r3 = fluree
        .insert(
            r2.ledger,
            &json!({"@context": ctx, "@graph": [{"@id": "ex:c", "ex:n": 3}]}),
        )
        .await
        .expect("commit 3");

    // 12 hex characters is what `fluree show` / `--at` users actually type.
    let prefix = target_id.digest_hex()[..12].to_string();
    // A well-formed sibling digest: same 12-character prefix, then padding. It
    // is not a real commit — no blob exists for it — which is the point: only
    // the content store fails closed, resolution itself is steerable by data.
    let forged_subject = format!("fluree:commit:sha256:{prefix}{}", "f".repeat(52));

    let injection = json!({
        "@context": {"f": "https://ns.flur.ee/db#"},
        "@graph": [{
            "@id": forged_subject,
            "@graph": txn_meta,
            "f:t": 5,
            "f:size": 1,
            "f:time": 1
        }]
    });
    let injection_result = fluree.insert(r3.ledger, &injection).await.map(|_| ());

    // THE assertion: the real commit still resolves from its 12-char prefix.
    let view = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache ledger")
        .snapshot()
        .await;
    let resolved = view
        .resolve_commit(CommitRef::Prefix(prefix.clone()))
        .await
        .unwrap_or_else(|e| {
            panic!("real commit must still resolve from prefix `{prefix}`, got: {e}")
        });
    assert_eq!(
        resolved, target_id,
        "prefix `{prefix}` must resolve to the real commit"
    );

    // And the mechanism: the write never landed.
    let err = injection_result
        .expect_err("forged commit record must be refused at the write")
        .to_string();
    assert!(
        err.contains("reserved system graph") && err.contains("#txn-meta"),
        "expected a reserved-graph refusal naming the graph, got: {err}"
    );
}

/// The system's own txn-meta write is untouched: ordinary commits still
/// succeed, and each one's provenance still lands in g_id 1 where the
/// commit-prefix resolver reads it.
///
/// Commit metadata is generated in `finalize_state_with_base` and appended
/// after staging — it never builds a `Txn` and never passes the guard — but
/// assert it positively rather than inferring it.
#[tokio::test]
async fn commit_provenance_still_lands_in_the_txn_meta_graph() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_name = "provenance";
    let ledger_id = "provenance:main";
    let ctx = json!({"ex": "http://example.org/"});

    let mut ledger = fluree.create_ledger(ledger_name).await.expect("create");
    let mut commit_ids = Vec::new();
    for n in 1..=4 {
        let result = fluree
            .insert(
                ledger,
                &json!({"@context": ctx, "@graph": [{"@id": format!("ex:s{n}"), "ex:n": n}]}),
            )
            .await
            .unwrap_or_else(|e| panic!("ordinary commit {n} must still succeed: {e}"));
        assert_eq!(result.receipt.t, n, "commit {n} should advance t");
        commit_ids.push(result.receipt.commit_id.clone());
        ledger = result.ledger;
    }

    // `resolve_commit_prefix` reads g_id 1 exclusively, so resolving every
    // commit by prefix proves each one's provenance record is present there.
    let view = fluree
        .ledger_cached(ledger_id)
        .await
        .expect("cache ledger")
        .snapshot()
        .await;
    for id in &commit_ids {
        let prefix = id.digest_hex()[..12].to_string();
        let resolved = view
            .resolve_commit(CommitRef::Prefix(prefix.clone()))
            .await
            .unwrap_or_else(|e| panic!("commit provenance missing for prefix `{prefix}`: {e}"));
        assert_eq!(
            &resolved, id,
            "prefix `{prefix}` must resolve to its commit"
        );
    }
}

/// `#config` stays writable through an ordinary transaction.
///
/// `docs/ledger-config/README.md` and `docs/ledger-config/writing-config.md`
/// document maintaining ledger configuration through a transaction. The
/// asymmetry with `#txn-meta` is a deliberate scope decision, not an
/// oversight — closing it is a documentation change first.
#[tokio::test]
async fn config_graph_stays_writable_by_an_ordinary_write() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reserved-write-config:main";
    let config = fluree_db_core::config_graph_iri(ledger_id);
    let ledger = genesis_ledger(&fluree, ledger_id);

    let sparql = format!(
        r#"INSERT DATA {{ GRAPH <{config}> {{ <http://example.org/shape> <http://example.org/p> "v" }} }}"#
    );
    let ledger = try_sparql(&fluree, ledger, &sparql)
        .await
        .unwrap_or_else(|e| panic!("#config must remain writable by an ordinary write: {e}"));

    let jsonld = json!({
        "@context": {"ex": "http://example.org/"},
        "@graph": [{"@id": "ex:shape2", "@graph": config, "ex:p": "v2"}]
    });
    fluree
        .stage_owned(ledger)
        .insert(&jsonld)
        .execute()
        .await
        .expect("#config must remain writable through the JSON-LD spelling too");
}

/// The whole-graph verbs keep their own, wider contract: graph management and
/// graph sync refuse BOTH reserved graphs, because they destroy or re-home a
/// graph wholesale rather than adding facts to one.
///
/// This pins that the data-write guard did not disturb the four
/// `ReservedGraphTarget` sites that predate it — it stays green when that guard
/// is reverted, which is the point.
#[tokio::test]
async fn whole_graph_verbs_still_refuse_both_reserved_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let ledger_id = "it/reserved-write-verbs:main";
    let config = fluree_db_core::config_graph_iri(ledger_id);
    let txn_meta = fluree_db_core::txn_meta_graph_iri(ledger_id);
    let g1 = "http://example.org/g1";
    let ledger = genesis_ledger(&fluree, ledger_id);

    // Seed a user graph so the transfers have a valid non-reserved end.
    let seed = format!(
        r#"INSERT DATA {{ GRAPH <{g1}> {{ <http://example.org/s> <http://example.org/p> "v" }} }}"#
    );
    let ledger = try_sparql(&fluree, ledger, &seed)
        .await
        .expect("seed user graph");

    for sparql in [
        format!("CLEAR GRAPH <{config}>"),
        format!("DROP GRAPH <{config}>"),
        format!("CLEAR GRAPH <{txn_meta}>"),
        format!("DROP GRAPH <{txn_meta}>"),
        format!("COPY <{g1}> TO <{config}>"),
        format!("MOVE <{g1}> TO <{txn_meta}>"),
        format!("COPY <{config}> TO <{g1}>"),
        format!("ADD <{txn_meta}> TO <{g1}>"),
    ] {
        let err = try_sparql(&fluree, ledger.clone(), &sparql)
            .await
            .expect_err(&format!("reserved-graph verb must be refused: {sparql}"));
        assert!(
            err.contains("reserved system graph"),
            "expected a reserved-graph refusal for `{sparql}`, got: {err}"
        );
    }

    // Graph sync refuses both too — its own guard, by IRI shape.
    let payload = json!({"@context": {"ex": "http://example.org/"},
                         "@graph": [{"@id": "ex:s", "ex:p": 1}]});
    for graph in [&config, &txn_meta] {
        let err = fluree
            .stage_owned(ledger.clone())
            .sync_graph(graph, &payload)
            .execute()
            .await
            .expect_err(&format!("sync into {graph} must be refused"))
            .to_string();
        assert!(
            err.contains("reserved system graph"),
            "expected a reserved-graph refusal for sync into {graph}, got: {err}"
        );
    }
}
