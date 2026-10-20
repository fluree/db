//! Multi-operation SPARQL UPDATE against **real storage**: binary-index
//! merges and commit-envelope reloads (PR-1454 review).
//!
//! The memory-backed multi-op tests in `it_transact_update.rs` prove the
//! sequential-staging semantics; these prove the two envelope-level
//! properties the fold relies on survive real persistence:
//!
//! 1. a dangling retract (`INSERT x ; DELETE x` netting to a retract of a
//!    never-existing fact) crossing a genuine novelty→index merge, and the
//!    cumulative namespace delta round-tripping a real commit envelope;
//! 2. the persisted `graph_delta` id↔IRI mapping agreeing with the ids the
//!    staged flakes actually carry, proven by reloading from disk (the live
//!    view self-heals from the in-memory registry; a reload trusts the
//!    envelope).

#![cfg(feature = "native")]

use crate::support::{
    query_sparql, start_background_indexer_local, trigger_index_and_wait_outcome,
};
use fluree_db_api::{FlureeBuilder, IndexConfig, LedgerState, ReindexOptions};
use fluree_db_core::comparator::IndexType;
use fluree_db_core::{range_with_overlay, FlakeValue, RangeMatch, RangeOptions, RangeTest, Sid};

/// The `(o, dt)` of every `ex:p` value of `ex:s` in `graph` (None = default)
/// that `ledger` holds as current facts.
async fn held_terms(ledger: &LedgerState, graph: Option<&str>) -> Vec<(FlakeValue, Sid)> {
    let g_id = match graph {
        None => 0,
        Some(iri) => ledger
            .snapshot
            .graph_registry
            .graph_id_for_iri(iri)
            .expect("graph"),
    };
    let s = ledger
        .snapshot
        .encode_iri("http://example.org/ns/s")
        .expect("subject");
    let p = ledger
        .snapshot
        .encode_iri("http://example.org/ns/p")
        .expect("predicate");
    range_with_overlay(
        &ledger.snapshot,
        g_id,
        ledger.novelty.as_ref(),
        IndexType::Spot,
        RangeTest::Eq,
        RangeMatch::subject_predicate(s, p),
        RangeOptions::new().with_to_t(ledger.t()),
    )
    .await
    .expect("range")
    .into_iter()
    .filter(|f| f.op && f.o != FlakeValue::String("keep".into()))
    .map(|f| (f.o, f.dt))
    .collect()
}

/// The `(o, dt)` of every `ex:p` retraction in `ledger`'s novelty.
fn retracted_terms(ledger: &LedgerState) -> Vec<(FlakeValue, Sid)> {
    let p = ledger
        .snapshot
        .encode_iri("http://example.org/ns/p")
        .expect("predicate");
    ledger
        .novelty
        .iter_flakes(IndexType::Spot)
        .filter(|f| !f.op && f.p == p)
        .map(|f| (f.o.clone(), f.dt.clone()))
        .collect()
}

/// The `INSERT x ; DELETE x` fold nets to a retract of a fact the base
/// never contained. The commit's novelty carries that dangling retract and
/// the index merge must cross it without resurrecting or corrupting
/// anything; the third operation's namespace (introduced only mid-request)
/// must round-trip the commit envelope and still resolve after a cold
/// reload from disk.
#[tokio::test]
async fn multi_op_dangling_retract_and_ns_delta_survive_index_merge_and_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let mut fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .publisher_arc()
            .expect("test setup requires ReadWrite nameservice mode"),
        fluree_db_indexer::IndexerConfig::small(),
    );
    fluree.set_indexing_mode(fluree_db_api::tx::IndexingMode::Background(handle.clone()));

    let index_cfg = IndexConfig {
        reindex_min_bytes: 0,
        reindex_max_bytes: 10_000_000,
    };
    let ledger_id = "it/multiop-indexed:main";

    local
        .run_until(async {
            fluree
                .create_ledger(ledger_id)
                .await
                .expect("create ledger");

            // Establish a binary-index base.
            let seeded = fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(
                    r#"PREFIX ex: <http://example.org/ns/>
                       INSERT DATA { ex:base ex:p "seed" }"#,
                )
                .index_config(index_cfg.clone())
                .commit()
                .await
                .expect("seed commit");
            trigger_index_and_wait_outcome(&handle, ledger_id, seeded.receipt.t).await;

            // The multi-op request: op 1+2 net to a dangling retract, op 3
            // introduces a namespace no earlier operation (or the base)
            // knows.
            let request = fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(
                    r#"PREFIX ex: <http://example.org/ns/>
                       PREFIX fresh: <http://fresh.example/only-in-op-3/>
                       INSERT DATA { ex:gone ex:p "dangling" } ;
                       DELETE DATA { ex:gone ex:p "dangling" } ;
                       INSERT DATA { fresh:kept fresh:q "kept" }"#,
                )
                .index_config(index_cfg.clone())
                .commit()
                .await
                .expect("multi-op request commits");

            // Merge the request's novelty — dangling retract included —
            // into the binary index.
            trigger_index_and_wait_outcome(&handle, ledger_id, request.receipt.t).await;
            request.receipt.t
        })
        .await;

    // Cold reload from disk: the commit envelope (namespace map included)
    // is all the new instance knows.
    drop(fluree);
    let fluree2 = FlureeBuilder::file(path)
        .build()
        .expect("rebuild file fluree");
    let ledger = fluree2.ledger(ledger_id).await.expect("reload ledger");

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://example.org/ns/gone> <http://example.org/ns/p> ?o }",
    )
    .await
    .expect("query dangling")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .is_empty(),
        "the dangling retract must net to absence across the index merge: {json}"
    );

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://fresh.example/only-in-op-3/kept> <http://fresh.example/only-in-op-3/q> ?o }",
    )
    .await
    .expect("query fresh ns")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert_eq!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .len(),
        1,
        "op 3's namespace must round-trip the commit envelope and reload: {json}"
    );

    let json = query_sparql(
        &fluree2,
        &ledger,
        r"SELECT ?o WHERE { <http://example.org/ns/base> <http://example.org/ns/p> ?o }",
    )
    .await
    .expect("query base")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    assert_eq!(
        json["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .len(),
        1,
        "pre-request data must survive untouched: {json}"
    );
}

/// PR-1454 review (bplatz): per-op staging assigns new GraphIds in arrival
/// order while `build_commit` may assign over a sorted union — if the
/// persisted `commit_record.graph_delta` mapping disagrees with the ids the
/// staged flakes carry, a reload routes named-graph data to the WRONG
/// graph. Arrival order (zzz before aaa) deliberately inverts sorted order
/// so any re-assignment is visible.
#[tokio::test]
async fn multi_op_graph_delta_mapping_survives_reload() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();

    let fluree = FlureeBuilder::file(path.clone())
        .build()
        .expect("build file fluree");
    let ledger_id = "it/multiop-graph-delta:main";
    fluree
        .create_ledger(ledger_id)
        .await
        .expect("create ledger");

    fluree
        .graph(ledger_id)
        .transact()
        .sparql_update(
            r#"PREFIX ex: <http://example.org/ns/>
               INSERT DATA { GRAPH <urn:g:zzz> { ex:a ex:p "in-zzz" } } ;
               INSERT DATA { GRAPH <urn:g:aaa> { ex:b ex:p "in-aaa" } }"#,
        )
        .commit()
        .await
        .expect("multi-op named-graph request commits");

    let assert_routing = |json_zzz: serde_json::Value, json_aaa: serde_json::Value, when: &str| {
        let zzz = json_zzz["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["o"]["value"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            zzz,
            vec!["in-zzz".to_string()],
            "{when}: <urn:g:zzz> must contain exactly its own data"
        );
        let aaa = json_aaa["results"]["bindings"]
            .as_array()
            .expect("bindings")
            .iter()
            .map(|b| b["o"]["value"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            aaa,
            vec!["in-aaa".to_string()],
            "{when}: <urn:g:aaa> must contain exactly its own data"
        );
    };

    let q_zzz = r"SELECT ?o WHERE { GRAPH <urn:g:zzz> { ?s ?p ?o } }";
    let q_aaa = r"SELECT ?o WHERE { GRAPH <urn:g:aaa> { ?s ?p ?o } }";

    // Live view (self-heals from the in-memory registry) — sanity.
    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let live_zzz = query_sparql(&fluree, &ledger, q_zzz)
        .await
        .expect("live zzz")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    let live_aaa = query_sparql(&fluree, &ledger, q_aaa)
        .await
        .expect("live aaa")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    assert_routing(live_zzz, live_aaa, "live view");

    // Cold reload: the persisted graph_delta is all the new instance has.
    drop(fluree);
    let fluree2 = FlureeBuilder::file(path)
        .build()
        .expect("rebuild file fluree");
    let ledger = fluree2.ledger(ledger_id).await.expect("reload ledger");
    let cold_zzz = query_sparql(&fluree2, &ledger, q_zzz)
        .await
        .expect("cold zzz")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    let cold_aaa = query_sparql(&fluree2, &ledger, q_aaa)
        .await
        .expect("cold aaa")
        .to_sparql_json(&ledger.snapshot)
        .expect("json");
    assert_routing(cold_zzz, cold_aaa, "cold reload");
}

/// A reversal is netted against what the base holds, and in a named graph
/// the base's values may live only in the index, whose rows carry no graph.
/// Read cold, so nothing is in novelty: deleting and re-inserting a held
/// value commits nothing, and inserting and deleting one still deletes it.
#[tokio::test]
async fn multi_op_reversal_nets_against_an_indexed_named_graph() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let path = tmp.path().to_string_lossy().to_string();
    let ledger_id = "it/multiop-named:main";
    {
        let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
        fluree
            .create_ledger(ledger_id)
            .await
            .expect("create ledger");
        fluree
            .graph(ledger_id)
            .transact()
            .sparql_update(
                r#"PREFIX ex: <http://example.org/ns/>
                   INSERT DATA { GRAPH ex:g { ex:s ex:p "x" , "y" } }"#,
            )
            .commit()
            .await
            .expect("seed");
        crate::support::rebuild_and_publish_index(&fluree, ledger_id).await;
    }
    let fluree = FlureeBuilder::file(path)
        .without_storage_lock()
        .build()
        .expect("cold reload");
    let update = |sparql: &'static str| {
        let fluree = fluree.clone();
        async move {
            fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(sparql)
                .commit()
                .await
                .expect("commit")
                .receipt
        }
    };

    let receipt = update(
        r#"PREFIX ex: <http://example.org/ns/>
           DELETE DATA { GRAPH ex:g { ex:s ex:p "y" } } ;
           INSERT DATA { GRAPH ex:g { ex:s ex:p "y" } }"#,
    )
    .await;
    assert_eq!(
        (receipt.t, receipt.flake_count),
        (1, 0),
        "deleting and re-inserting a held value commits nothing"
    );

    let receipt = update(
        r#"PREFIX ex: <http://example.org/ns/>
           INSERT DATA { GRAPH ex:g { ex:s ex:p "x" } } ;
           DELETE DATA { GRAPH ex:g { ex:s ex:p "x" } }"#,
    )
    .await;
    assert_eq!(
        (receipt.t, receipt.retract_count),
        (2, 1),
        "inserting and deleting a held value deletes it"
    );

    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
    let json = query_sparql(
        &fluree,
        &ledger,
        "PREFIX ex: <http://example.org/ns/> SELECT ?o WHERE { GRAPH ex:g { ex:s ex:p ?o } }",
    )
    .await
    .expect("query")
    .to_sparql_json(&ledger.snapshot)
    .expect("sparql json");
    let values: Vec<&str> = json["results"]["bindings"]
        .as_array()
        .expect("bindings")
        .iter()
        .map(|b| b["o"]["value"].as_str().expect("value"))
        .collect();
    assert_eq!(values, vec!["y"], "{json}");
}

/// A value the index keeps with less detail than written (a big integer's
/// XSD subtype; a `dateTime` written past the microsecond, read to it) is one
/// fact to storage
/// however it is written, and a reversal nets by that fact. Read cold, so
/// the base holds the value in the index alone: deleting and re-inserting it
/// commits nothing and keeps it, and inserting and deleting it deletes it,
/// in the default graph and in a named one, before and after the next
/// index build. The retraction names the value as the index stores it.
#[tokio::test]
async fn multi_op_reversal_nets_a_value_the_index_keeps_less_of() {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    let values = [
        "\"123456789012345678901234567890\"^^xsd:nonNegativeInteger",
        "\"2020-01-01T00:00:00.123456789Z\"^^xsd:dateTime",
    ];
    let mut failures = Vec::new();
    for value in values {
        for graph in [None, Some("ex:g")] {
            for reinsert in [true, false] {
                let wrap = |triples: String| match graph {
                    Some(g) => format!("GRAPH {g} {{ {triples} }}"),
                    None => triples,
                };
                let tmp = tempfile::TempDir::new().expect("tempdir");
                let path = tmp.path().to_string_lossy().to_string();
                let ledger_id = "it/multiop-lossy:main";
                {
                    let fluree = FlureeBuilder::file(path.clone()).build().expect("build");
                    fluree.create_ledger(ledger_id).await.expect("create");
                    let seed = format!(
                        "{PFX}INSERT DATA {{ {} }}",
                        wrap(format!("ex:s ex:p {value} , \"keep\" ."))
                    );
                    fluree
                        .graph(ledger_id)
                        .transact()
                        .sparql_update(&seed)
                        .commit()
                        .await
                        .expect("seed");
                    crate::support::rebuild_and_publish_index(&fluree, ledger_id).await;
                }
                let x = wrap(format!("ex:s ex:p {value} ."));
                let update = if reinsert {
                    format!("{PFX}DELETE DATA {{ {x} }} ;\nINSERT DATA {{ {x} }}")
                } else {
                    format!("{PFX}INSERT DATA {{ {x} }} ;\nDELETE DATA {{ {x} }}")
                };
                let read = |path: String| {
                    let q = format!(
                        "{PFX}SELECT (COUNT(?o) AS ?n) WHERE {{ {} }}",
                        wrap("ex:s ex:p ?o .".to_string())
                    );
                    async move {
                        let fluree = FlureeBuilder::file(path)
                            .without_storage_lock()
                            .build()
                            .expect("reload");
                        let ledger = fluree.ledger(ledger_id).await.expect("ledger");
                        let json = query_sparql(&fluree, &ledger, &q)
                            .await
                            .expect("query")
                            .to_sparql_json(&ledger.snapshot)
                            .expect("json");
                        json["results"]["bindings"][0]["n"]["value"]
                            .as_str()
                            .expect("count")
                            .to_string()
                    }
                };
                let graph_iri = graph.map(|_| "http://example.org/ns/g");
                let (receipt, stored, retracted) = {
                    let fluree = FlureeBuilder::file(path.clone())
                        .without_storage_lock()
                        .build()
                        .expect("cold reload");
                    let ledger = fluree.ledger(ledger_id).await.expect("ledger");
                    let stored = held_terms(&ledger, graph_iri).await;
                    let receipt = fluree
                        .graph(ledger_id)
                        .transact()
                        .sparql_update(&update)
                        .commit()
                        .await
                        .expect("commit")
                        .receipt;
                    let reloaded = FlureeBuilder::file(path.clone())
                        .without_storage_lock()
                        .build()
                        .expect("reload");
                    let ledger = reloaded.ledger(ledger_id).await.expect("ledger");
                    (receipt, stored, retracted_terms(&ledger))
                };
                let want_retracted = if reinsert { Vec::new() } else { stored.clone() };
                if stored.len() != 1 || retracted != want_retracted {
                    failures.push(format!(
                        "{value} in {} ({}): stored {stored:?}, retracted {retracted:?}",
                        graph.unwrap_or("the default graph"),
                        if reinsert {
                            "DELETE ; INSERT"
                        } else {
                            "INSERT ; DELETE"
                        },
                    ));
                }
                let after = read(path.clone()).await;
                {
                    let fluree = FlureeBuilder::file(path.clone())
                        .without_storage_lock()
                        .build()
                        .expect("reload");
                    crate::support::rebuild_and_publish_index(&fluree, ledger_id).await;
                }
                let reindexed = read(path.clone()).await;
                let (want_commit, want_count) = if reinsert {
                    ((1, 0, 0), "2")
                } else {
                    ((2, 1, 1), "1")
                };
                let got = (
                    (receipt.t, receipt.flake_count, receipt.retract_count),
                    after.as_str(),
                    reindexed.as_str(),
                );
                if got != (want_commit, want_count, want_count) {
                    failures.push(format!(
                        "{value} in {} ({}): (t, flakes, retracts), values, values after \
                         reindex = {got:?}; expected {:?}",
                        graph.unwrap_or("the default graph"),
                        if reinsert {
                            "DELETE ; INSERT"
                        } else {
                            "INSERT ; DELETE"
                        },
                        (want_commit, want_count, want_count)
                    ));
                }
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// The kind of value [`multi_op_last_op_matrix`] writes.
#[derive(Clone, Copy, Debug)]
enum Kind {
    /// An integer too large for 64 bits, under XSD integer subtypes.
    BigInteger = 0,
    /// A `dateTime` written with digits past the microsecond. Its spellings
    /// are one term once read, so a base holding a second spelling holds one
    /// term of it; only a big integer's spellings tell those bases apart.
    DateTime = 1,
}

/// The state a multi-operation update starts from, for
/// [`multi_op_last_op_matrix`].
#[derive(Clone, Copy, Debug)]
enum Base {
    Absent,
    Novelty,
    Indexed,
    /// Never indexed; novelty holds the value under two spellings, asserted
    /// by two requests (#2090).
    TwoTermsInNovelty,
    /// The index holds the value; novelty a later spelling of it.
    IndexedThenNovelty,
}

/// Every first and last op of a multi-operation update over a value the index
/// keeps less of, from `base`: the value is there afterwards exactly when the
/// last operation inserts it, whatever subtype (or digits past the
/// microsecond) each operation writes it with, read in process, after a
/// reload and after the next incremental and full index builds. A retraction
/// that survives the fold names the value as the base stores it, under every
/// term the base holds it by.
///
/// The sequences cover every one- and two-op sequence over the stored
/// spelling and another, so each first and last op is tried with the same
/// term and with another term of the value, either way round; every
/// three-op sequence whose first and last ops spell it differently; and
/// longer ones. The one- and two-op sequences also run as separate
/// requests, one commit per op: a `DELETE DATA` naming one spelling of a
/// value the ledger holds under another names that stored fact.
async fn multi_op_last_op_matrix(base: Base, kind: Kind) {
    const PFX: &str = "PREFIX ex: <http://example.org/ns/>\n\
                       PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n";
    // (stored, then three more spellings of the same value)
    let values = [
        [
            "\"123456789012345678901234567890\"^^xsd:integer",
            "\"123456789012345678901234567890\"^^xsd:nonNegativeInteger",
            "\"123456789012345678901234567890\"^^xsd:positiveInteger",
        ],
        [
            "\"2020-01-01T00:00:00.123456Z\"^^xsd:dateTime",
            "\"2020-01-01T00:00:00.123456789Z\"^^xsd:dateTime",
            "\"2020-01-01T00:00:00.1234561Z\"^^xsd:dateTime",
        ],
    ];
    // (true = INSERT DATA, false = DELETE DATA, spelling)
    let mut sequences: Vec<Vec<(bool, usize)>> = Vec::new();
    let mut push = |ops: Vec<(bool, usize)>| {
        if !sequences.contains(&ops) {
            sequences.push(ops);
        }
    };
    let ops = [true, false];
    for a in ops {
        for x in [0, 1] {
            push(vec![(a, x)]);
            for b in ops {
                for y in [0, 1] {
                    push(vec![(a, x), (b, y)]);
                }
            }
        }
    }
    for a in ops {
        for m in ops {
            for b in ops {
                for (x, y) in [(0, 1), (1, 0)] {
                    push(vec![(a, x), (m, 2), (b, y)]);
                }
            }
        }
    }
    for longer in [
        vec![(true, 2)],
        vec![(false, 0), (true, 1), (false, 1), (true, 2)],
        vec![(true, 1), (false, 1), (true, 2), (false, 2)],
    ] {
        push(longer);
    }
    let count = |fluree: fluree_db_api::Fluree, ledger_id: String| async move {
        let ledger = fluree.ledger(&ledger_id).await.expect("ledger");
        let q = format!("{PFX}SELECT (COUNT(?o) AS ?n) WHERE {{ ex:s ex:p ?o }}");
        let json = query_sparql(&fluree, &ledger, &q)
            .await
            .expect("query")
            .to_sparql_json(&ledger.snapshot)
            .expect("json");
        json["results"]["bindings"][0]["n"]["value"]
            .as_str()
            .expect("count")
            .to_string()
    };
    let mut failures = Vec::new();
    let mut case = 0;
    let spellings = &values[kind as usize];
    for (ops, split) in sequences
        .iter()
        .flat_map(|ops| [(ops, false), (ops, true)])
        .filter(|(ops, split)| !split || ops.len() <= 2)
    {
        case += 1;
        let ledger_id = &format!("it/multiop-last-op-{base:?}-{kind:?}-{case}:main");
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let path = tmp.path().to_string_lossy().to_string();
        let fluree = FlureeBuilder::file(path.clone())
            .without_indexing()
            .build()
            .expect("build");
        fluree.create_ledger(ledger_id).await.expect("create");
        let seed = match base {
            Base::Absent => format!("{PFX}INSERT DATA {{ ex:s ex:q \"keep\" }}"),
            _ => format!(
                "{PFX}INSERT DATA {{ ex:s ex:p {} ; ex:q \"keep\" }}",
                spellings[0]
            ),
        };
        fluree
            .graph(ledger_id)
            .transact()
            .sparql_update(&seed)
            .commit()
            .await
            .expect("seed");
        let fluree = if let Base::Indexed | Base::IndexedThenNovelty = base {
            crate::support::rebuild_and_publish_index(&fluree, ledger_id).await;
            FlureeBuilder::file(path.clone())
                .without_indexing()
                .without_storage_lock()
                .build()
                .expect("cold reload")
        } else {
            fluree
        };
        if let Base::TwoTermsInNovelty | Base::IndexedThenNovelty = base {
            fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(&format!(
                    "{PFX}INSERT DATA {{ ex:s ex:p {} }}",
                    spellings[1]
                ))
                .commit()
                .await
                .expect("second spelling");
        }
        let op_texts: Vec<String> = ops
            .iter()
            .map(|(insert, n)| {
                format!(
                    "{} DATA {{ ex:s ex:p {} }}",
                    if *insert { "INSERT" } else { "DELETE" },
                    spellings[*n]
                )
            })
            .collect();
        let update = op_texts.join(" ;\n");
        let requests = if split {
            op_texts.clone()
        } else {
            vec![update.clone()]
        };
        for request in requests {
            fluree
                .graph(ledger_id)
                .transact()
                .sparql_update(&format!("{PFX}{request}"))
                .commit()
                .await
                .expect("update");
        }
        let want = if ops.last().expect("an op").0 {
            "1"
        } else {
            "0"
        };
        let reload = || {
            FlureeBuilder::file(path.clone())
                .without_indexing()
                .without_storage_lock()
                .build()
                .expect("reload")
        };
        let in_process = count(fluree, ledger_id.clone()).await;
        let reloaded = count(reload(), ledger_id.clone()).await;
        let incremental = {
            crate::support::build_and_publish_index(&reload(), ledger_id).await;
            count(reload(), ledger_id.clone()).await
        };
        // `reindex` replaces the index of the same `t` the
        // incremental build published; `publish_index` would keep it.
        let rebuilt = {
            reload()
                .reindex(ledger_id, ReindexOptions::default())
                .await
                .expect("full rebuild");
            count(reload(), ledger_id.clone()).await
        };
        // Novelty holds each term of a value as its own fact until
        // the next build (#2090), so a value it holds and asserts
        // under another subtype reads twice before then, as after an
        // `INSERT DATA` of that subtype alone.
        // So can two spellings that separate requests assert.
        let asserted: std::collections::HashSet<usize> = ops
            .iter()
            .filter(|(insert, _)| *insert)
            .map(|(_, n)| *n)
            .collect();
        let two_terms_in_novelty = want == "1"
            && (matches!(base, Base::Novelty | Base::TwoTermsInNovelty)
                || (split && matches!(base, Base::Absent) && asserted.len() > 1));
        let unbuilt_ok = |n: &String| {
            n == want || (two_terms_in_novelty && n.parse::<u32>().is_ok_and(|n| n > 1))
        };
        let got = [in_process, reloaded, incremental, rebuilt];
        if !got[..2].iter().all(unbuilt_ok) || got[2..].iter().any(|n| n != want) {
            failures.push(format!(
                "{base:?} base, {}{}: values in process, reloaded, after an \
                         incremental build, after a rebuild = {got:?}; expected {want} each{}",
                update.replace('\n', " "),
                if split { " (one request per op)" } else { "" },
                if two_terms_in_novelty {
                    " (more than one before a build)"
                } else {
                    ""
                }
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_a_value() {
    multi_op_last_op_matrix(Base::Absent, Kind::BigInteger).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_a_value_held_in_novelty() {
    multi_op_last_op_matrix(Base::Novelty, Kind::BigInteger).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_an_indexed_value() {
    multi_op_last_op_matrix(Base::Indexed, Kind::BigInteger).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_a_value_held_under_two_subtypes() {
    multi_op_last_op_matrix(Base::TwoTermsInNovelty, Kind::BigInteger).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_an_indexed_value_with_a_later_subtype() {
    multi_op_last_op_matrix(Base::IndexedThenNovelty, Kind::BigInteger).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_a_datetime() {
    multi_op_last_op_matrix(Base::Absent, Kind::DateTime).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_a_datetime_held_in_novelty() {
    multi_op_last_op_matrix(Base::Novelty, Kind::DateTime).await;
}

#[tokio::test]
async fn a_multi_op_update_ends_where_its_last_op_leaves_an_indexed_datetime() {
    multi_op_last_op_matrix(Base::Indexed, Kind::DateTime).await;
}
