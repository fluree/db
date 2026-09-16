//! The reserved-graph access contract, as executable tests.
//!
//! **Reachability is explicitness; policy is the access control.** A reserved
//! graph — `#txn-meta` (g_id 1) and `#config` (g_id 2) — may be READ when the
//! author names it in full; it may never appear implicitly, be enumerated, be
//! destroyed, or seed governance.
//!
//! | surface | `#config` | `#txn-meta` |
//! |---|---|---|
//! | ledger address (`L#config`, `--ledger`, `db()`) | reachable | reachable |
//! | SPARQL `FROM`/`FROM NAMED`, JSON-LD `from`/`from-named` | by full IRI only | by full IRI only |
//! | `GRAPH <iri>` with no `FROM NAMED` | blocked (empty) | blocked (empty) |
//! | `GRAPH ?g` enumeration | blocked | blocked |
//! | `GRAPH <iri>` named by an explicit `FROM NAMED` | reachable | reachable |
//! | `GRAPH <iri>` in a transaction | writable | writable |
//! | graph-management verbs | refused | refused |
//!
//! The `FROM`/`FROM NAMED` rows are covered by `it_query_dataset.rs`
//! (`sparql_from_admits_this_ledgers_reserved_graphs_by_full_iri` and its
//! negative twin); the graph-management row by the transact crate's
//! `ReservedGraphTarget` tests (`1cb3e8cb2`, `6df4381b3`). This file covers the
//! ledger-address row on `db_with_default_context` (the `db()` entry point is
//! covered by `it_named_graphs.rs::the_config_graph_is_addressable_by_its_full_iri`),
//! SPARQL↔JSON-LD parity for the `config` selector, and the `ledger-info` IRI
//! resolver.

#![cfg(feature = "native")]

use crate::support::{self, genesis_ledger};
use fluree_db_api::FlureeBuilder;
use serde_json::json;

fn config_iri(ledger_id: &str) -> String {
    fluree_db_core::config_graph_iri(ledger_id)
}

/// Default-graph data, one ordinary user graph, and a marked config graph.
async fn seed(fluree: &fluree_db_api::Fluree, ledger_id: &str) -> fluree_db_api::LedgerState {
    let ledger = genesis_ledger(fluree, ledger_id);
    let cfg = config_iri(ledger_id);
    let trig = format!(
        r#"
        @prefix ex: <http://example.org/ns/> .
        @prefix schema: <http://schema.org/> .
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        ex:alice schema:name "Alice" .

        GRAPH <urn:g1> {{ ex:bob schema:name "Bob" . }}

        GRAPH <{cfg}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> schema:name "CONFIG-MARKER" .
        }}
    "#
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed")
        .ledger
}

// ===========================================================================
// Ledger-address row
// ===========================================================================

/// The ledger-address row on the `db_with_default_context` entry point — the
/// one the server route and the CLI use, which also loads the default context
/// and re-resolves config after graph selection.
///
/// `it_named_graphs.rs::the_config_graph_is_addressable_by_its_full_iri` covers
/// the plain `db()` entry point and both spellings of `#config`; this covers
/// the other entry point plus the rows that test does not: `#txn-meta`, the
/// no-fragment default, and the negative. Deliberately not merged with it.
#[tokio::test]
async fn db_with_default_context_addresses_both_reserved_graphs() {
    let fluree = FlureeBuilder::memory().build_memory();
    let lid = "rg-addr:main";
    let _ledger = seed(&fluree, lid).await;

    let config_view = fluree
        .db_with_default_context(&format!("{lid}#config"))
        .await
        .expect("L#config must address the config graph");
    assert_eq!(config_view.graph_id, 2, "#config is g_id 2");

    let txn_meta_view = fluree
        .db_with_default_context(&format!("{lid}#txn-meta"))
        .await
        .expect("L#txn-meta must address the txn-meta graph");
    assert_eq!(txn_meta_view.graph_id, 1, "#txn-meta is g_id 1");

    let default_view = fluree.db_with_default_context(lid).await.expect("L");
    assert_eq!(default_view.graph_id, 0, "no fragment is the default graph");

    // The fragment arm only fires once a ledger has been named, so it cannot
    // be reached without naming this ledger.
    let err = fluree
        .db_with_default_context("no-such-ledger:main#config")
        .await
        .expect_err("a #config fragment on an unknown ledger must not resolve");
    assert!(
        err.is_not_found(),
        "expected not-found for an unknown ledger, got: {err}"
    );
}

// ===========================================================================
// SPARQL <-> JSON-LD parity for the `config` graph selector
// ===========================================================================

/// The JSON-LD `graph` selector accepts `"config"` as a well-known name, like
/// `"txn-meta"` — and every spelling agrees with the SPARQL twin.
///
/// Without `GraphSelector::Config`, `"config"` fell through to
/// `GraphSelector::Iri("config")` — an exact-IRI lookup for the bare word.
#[tokio::test]
async fn jsonld_config_graph_selector_matches_its_sparql_twin() {
    let fluree = FlureeBuilder::memory().build_memory();
    let lid = "rg-parity:main";
    let _ledger = seed(&fluree, lid).await;
    let cfg = config_iri(lid);

    let expected = json!([["CONFIG-MARKER"]]);

    for from in [
        json!(format!("{lid}#config")),
        json!({"@id": lid, "graph": "config"}),
        json!({"@id": lid, "@graph": "config"}),
        json!({"@id": lid, "graph": cfg.clone()}),
    ] {
        let q = json!({
            "@context": {"schema": "http://schema.org/"},
            "from": from,
            "select": ["?n"],
            "where": [{"@id": "?s", "schema:name": "?n"}]
        });
        let rows = fluree
            .query_from()
            .jsonld(&q)
            .execute_formatted()
            .await
            .unwrap_or_else(|e| panic!("JSON-LD from={from} must resolve, got: {e}"));
        assert_eq!(rows, expected, "JSON-LD from={from}");
    }

    // SPARQL twin on the same connection path.
    let sparql = format!(
        "PREFIX schema: <http://schema.org/> \
         SELECT ?n FROM <{cfg}> WHERE {{ ?s schema:name ?n }}"
    );
    let result = fluree
        .query_connection_sparql(&sparql)
        .await
        .expect("SPARQL FROM <#config> on the connection path");
    let rows = result
        .to_jsonld_async(fluree.db(lid).await.expect("db").as_graph_db_ref())
        .await
        .expect("to_jsonld");
    assert_eq!(rows, expected, "SPARQL twin must agree with JSON-LD");
}

// ===========================================================================
// ledger-info IRI resolution — the suffix-match bug
// ===========================================================================

/// **Shipping bug, present since the v4 baseline (`5985d0f01`), not caused by
/// this branch.** `ledger_info::resolve_graph_selector` matched the reserved
/// graphs with `iri.ends_with("#config")` / `("#txn-meta")`, so ANY graph whose
/// IRI happened to end that way resolved to this ledger's g_id 2 / 1. A user
/// graph named `http://evil.example/x#config` reported the governance graph's
/// stats in place of its own — and another ledger's reserved IRI silently
/// resolved to this ledger's.
///
/// The ledger is indexed so the store-backed lookup leg is live: the assertion
/// is that the evil graph resolves to ITSELF, not merely that it stops
/// resolving to g_id 2.
#[tokio::test]
async fn ledger_info_resolves_reserved_graphs_by_exact_iri_not_suffix() {
    let fluree = FlureeBuilder::memory().build_memory();
    let lid = "rg-suffix:main";
    let ledger = genesis_ledger(&fluree, lid);
    let cfg = config_iri(lid);
    let evil = "http://evil.example/x#config";
    let evil_tm = "http://evil.example/x#txn-meta";

    let trig = format!(
        r#"
        @prefix ex: <http://example.org/ns/> .
        @prefix schema: <http://schema.org/> .
        @prefix f: <https://ns.flur.ee/db#> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

        ex:alice schema:name "Alice" .

        GRAPH <{evil}> {{ ex:evil schema:name "EVIL-GRAPH-MARKER" . }}
        GRAPH <{evil_tm}> {{ ex:evil2 schema:name "EVIL-TM-MARKER" . }}

        GRAPH <{cfg}> {{
            <urn:config:main> rdf:type f:LedgerConfig .
            <urn:config:main> schema:name "CONFIG-MARKER" .
        }}
    "#
    );
    fluree
        .stage_owned(ledger)
        .upsert_turtle(&trig)
        .execute()
        .await
        .expect("seed");
    support::build_and_publish_index(&fluree, lid).await;

    let view = fluree.db(lid).await.expect("db");
    let g_of = |iri: &str| view.snapshot.graph_registry.graph_id_for_iri(iri);
    let evil_g = g_of(evil).expect("the evil graph is registered");
    let evil_tm_g = g_of(evil_tm).expect("the evil txn-meta graph is registered");
    assert!(
        evil_g >= 3 && evil_tm_g >= 3,
        "the decoys must be ordinary user graphs, got {evil_g} and {evil_tm_g}"
    );
    assert_eq!(g_of(&cfg), Some(2), "the real config graph is g_id 2");

    // `ledger-info` reports the resolved graph under "graph", as its IRI when
    // the store can name it. So "you get the graph you named" is directly
    // assertable — and it is exactly what failed before: asking for the evil
    // graph reported `urn:fluree:rg-suffix:main#config` and the config graph's
    // stats.
    let graph_of = |v: &serde_json::Value| -> Option<String> {
        v.get("graph").and_then(|g| g.as_str()).map(str::to_string)
    };

    for iri in [evil, evil_tm, cfg.as_str()] {
        let info = fluree
            .ledger_info(lid)
            .for_graph_iri(iri)
            .execute()
            .await
            .unwrap_or_else(|e| panic!("<{iri}> is a real graph of this ledger, got: {e}"));
        assert_eq!(
            graph_of(&info).as_deref(),
            Some(iri),
            "for_graph_iri(<{iri}>) must resolve to that graph, not a reserved slot"
        );
    }

    // The decoys' own flake counts confirm the stats came from the right
    // graph, not merely that the label is right.
    let evil_info = fluree
        .ledger_info(lid)
        .for_graph_iri(evil)
        .execute()
        .await
        .expect("evil graph info");
    let cfg_info = fluree
        .ledger_info(lid)
        .for_graph_iri(&cfg)
        .execute()
        .await
        .expect("config graph info");
    assert_ne!(
        evil_info.pointer("/stats"),
        cfg_info.pointer("/stats"),
        "the decoy graph must not report the config graph's stats"
    );

    // Another ledger's reserved IRI is not a graph of this ledger.
    let foreign = config_iri("some-other:main");
    assert!(
        fluree
            .ledger_info(lid)
            .for_graph_iri(&foreign)
            .execute()
            .await
            .is_err(),
        "another ledger's config IRI must not resolve to this ledger's g_id 2"
    );
}
