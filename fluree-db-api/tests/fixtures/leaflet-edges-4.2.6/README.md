Ledgers indexed by the v4.2.6 engine (`main` at f42ea3784) with tiny leaflets,
so that retractions sit at leaflet boundaries and every leaflet's routing keys
can be widened over the history it holds. A v4 leaf stores no live groups, so
a v5 binary serving these indexes judges leaflet boundaries from routing keys
and, where a leaflet holds history, from its lead column.

- `v4/edges-full:main`: the seed and retraction below, indexed once, after
  the retraction (a full build).
- `v4/edges-incr:main`: the same, indexed after the seed and again after the
  retraction (an incremental build).
- `v4/erase:main`: an email asserted and then retracted, indexed after the
  retraction, for a history-only erasure.

Each index was built with `leaflet_rows = 2` and `leaflets_per_leaf = 3`.
`storage/` is the file store the run below wrote, without its lock files.

The generator ran as an ignored test appended to
`fluree-db-api/tests/it_query_sparql_indexed.rs` on `main`, with
`FIXTURE_OUT=<dir>`:

```rust
fn fx_ctx() -> serde_json::Value {
    json!({ "ex": "http://example.org/ns/" })
}

fn fx_subjects_with(n: usize, values: &[i64]) -> serde_json::Value {
    let nodes: Vec<serde_json::Value> = (0..n)
        .map(|i| json!({ "@id": format!("ex:s{i:03}"), "ex:v": values }))
        .collect();
    json!({ "@context": fx_ctx(), "@graph": nodes })
}

fn fx_retract_v(value: i64) -> serde_json::Value {
    json!({
        "@context": fx_ctx(),
        "where": [{ "@id": "?s", "ex:v": value }],
        "delete": [{ "@id": "?s", "ex:v": value }]
    })
}

fn fx_cases() -> Vec<(&'static str, serde_json::Value, serde_json::Value, bool)> {
    let n = 8;
    let mut nodes = Vec::new();
    let mut del = Vec::new();
    for i in 0..n {
        nodes.push(json!({ "@id": format!("ex:s{i:02}"), "ex:v": [1, 2, 9] }));
        del.push(json!({ "@id": format!("ex:s{i:02}"), "ex:v": 9 }));
        nodes.push(json!({ "@id": format!("ex:x{i:02}"), "ex:u": [1, 2, 3, 5] }));
        del.push(json!({ "@id": format!("ex:x{i:02}"), "ex:u": 3 }));
        nodes.push(json!({ "@id": format!("ex:t{i:02}"), "ex:w": ["a", "b", "z"] }));
        del.push(json!({ "@id": format!("ex:t{i:02}"), "ex:w": "z" }));
        for suffix in ["a", "b", "z"] {
            nodes.push(json!({ "@id": format!("ex:o{i:02}{suffix}"), "ex:p": 1000 + i }));
        }
        del.push(json!({ "@id": format!("ex:o{i:02}z"), "ex:p": 1000 + i }));
        nodes.push(json!({ "@id": format!("ex:i{i:02}"), "ex:n": i }));
    }
    nodes.push(json!({ "@id": "ex:hi", "ex:n": 1000 }));
    nodes.push(json!({ "@id": "ex:lo", "ex:n": -5 }));
    del.push(json!({ "@id": "ex:hi", "ex:n": 1000 }));
    del.push(json!({ "@id": "ex:lo", "ex:n": -5 }));
    let seed = json!({ "@context": fx_ctx(), "@graph": nodes });
    let del = json!({ "@context": fx_ctx(), "delete": del });

    let erase = json!({ "@context": { "ex": "http://example.org/" }, "@graph": [
        { "@id": "ex:alice", "ex:email": "alice@secret.example", "ex:name": "Alice" },
        { "@id": "ex:bob", "ex:email": "bob@kept.example" }
    ]});
    let erase_del = json!({
        "@context": { "ex": "http://example.org/" },
        "delete": [{ "@id": "ex:alice", "ex:email": "alice@secret.example" }]
    });

    vec![
        ("v4/edges-full", seed.clone(), del.clone(), false),
        ("v4/edges-incr", seed, del, true),
        ("v4/erase", erase, erase_del, false),
    ]
}

#[tokio::test]
#[ignore]
async fn generate_leaflet_edges_fixture() {
    let out = std::env::var("FIXTURE_OUT").expect("FIXTURE_OUT");
    std::fs::create_dir_all(&out).unwrap();
    let fluree = FlureeBuilder::file(out.clone())
        .with_ledger_cache_config(LedgerManagerConfig::default())
        .build()
        .expect("build");
    let mut cfg = fluree_db_indexer::IndexerConfig::small();
    cfg.leaflet_rows = 2;
    cfg.leaflets_per_leaf = 3;
    let (local, handle) = start_background_indexer_local(
        fluree.backend().clone(),
        fluree
            .nameservice_mode()
            .as_arc_indexing_nameservice()
            .expect("ns"),
        cfg,
    );
    local
        .run_until(async move {
            let no_bg = IndexConfig {
                reindex_min_bytes: 1_000_000_000,
                reindex_max_bytes: 1_000_000_000,
            };
            for (name, seed, del, incremental) in fx_cases() {
                let id = format!("{name}:main");
                let l1 = fluree
                    .insert_with_opts(
                        genesis_ledger_for_fluree(&fluree, &id),
                        &seed,
                        TxnOpts::default(),
                        CommitOpts::default(),
                        &no_bg,
                    )
                    .await
                    .expect("seed")
                    .ledger;
                if incremental {
                    let _ = trigger_index_and_wait_outcome(&handle, &id, l1.t()).await;
                }
                let l1 = fluree.ledger(&id).await.expect("reload");
                let l2 = fluree
                    .update_with_opts(l1, &del, TxnOpts::default(), CommitOpts::default(), &no_bg)
                    .await
                    .expect("delete")
                    .ledger;
                let _ = trigger_index_and_wait_outcome(&handle, &id, l2.t()).await;
                let l = fluree.ledger(&id).await.unwrap();
                eprintln!("{id}: t={} index_t={}", l.t(), l.snapshot.t);
            }
        })
        .await;
}
```
