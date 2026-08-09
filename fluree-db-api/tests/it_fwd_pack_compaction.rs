//! Forward dictionary pack tail compaction, end to end through real
//! incremental index cycles.
//!
//! Every incremental build appends at least one forward pack per touched dict
//! stream, so without compaction the routing table grows once per build
//! forever — the growth that put five figures of packs in one table and
//! exhausted `vm.max_map_count`. These tests pin the three properties that
//! matter: the table stays bounded, every id still resolves across a merge
//! boundary, and consumed packs become collectable garbage rather than leaks.

#![cfg(feature = "native")]

use crate::support;
use fluree_db_api::tx::IndexingMode;
use fluree_db_api::{Fluree, IndexerConfig, NameServiceMode, TriggerIndexOptions};
use fluree_db_connection::config::ConnectionConfig;
use fluree_db_core::{ContentId, ContentStore, MemoryStorage};
use fluree_db_nameservice::memory::MemoryNameService;
use serde_json::json;
use std::sync::Arc;

/// Index cycles to run. Comfortably past the 8-pack merge width so at least
/// one compaction has to fire.
const CYCLES: usize = 12;

fn string_pack_cids(
    root: &fluree_db_binary_index::format::index_root::IndexRoot,
) -> Vec<ContentId> {
    root.dict_refs
        .forward_packs
        .string_fwd_packs
        .iter()
        .map(|e| e.pack_cid.clone())
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn incremental_cycles_compact_the_forward_pack_tail() {
    let storage = MemoryStorage::new();
    let nameservice = MemoryNameService::new();

    let mut fluree: Fluree = Fluree::new(
        ConnectionConfig::memory(),
        storage.clone(),
        NameServiceMode::ReadWrite(Arc::new(nameservice.clone())),
    );

    let indexer_cfg = IndexerConfig::small()
        .with_incremental_enabled(true)
        .with_incremental_max_commits(10_000);

    let (local, handle) = support::start_background_indexer_local(
        fluree_db_core::StorageBackend::Managed(Arc::new(storage.clone())),
        Arc::new(nameservice.clone()),
        indexer_cfg,
    );
    fluree.set_indexing_mode(IndexingMode::Background(handle.clone()));

    local
        .run_until(async move {
            let ledger_id = "it/fwd-pack-compaction:main";
            let mut ledger = support::genesis_ledger_for_fluree(&fluree, ledger_id);

            let mut roots: Vec<ContentId> = Vec::new();

            // Each cycle introduces brand new subjects and new string values, so
            // every cycle is guaranteed to append a forward pack to both streams.
            for cycle in 0..CYCLES {
                let tx = json!({
                    "@context": { "ex": "http://example.org/" },
                    "@graph": (0..3).map(|i| {
                        json!({
                            "@id": format!("ex:cycle{cycle}/entity{i}"),
                            "@type": "ex:Thing",
                            "ex:label": format!("label for cycle {cycle} entity {i}")
                        })
                    }).collect::<Vec<_>>()
                });

                let r = fluree.insert(ledger, &tx).await.expect("insert");
                ledger = r.ledger;

                let res = fluree
                    .trigger_index(ledger_id, TriggerIndexOptions::default())
                    .await
                    .expect("trigger_index");
                roots.push(res.root_id.expect("root id"));
            }

            let cs = fluree.content_store(ledger_id);
            let decode = |bytes: &[u8]| {
                fluree_db_binary_index::format::index_root::IndexRoot::decode(bytes)
                    .expect("decode root")
            };

            let mut per_cycle: Vec<Vec<ContentId>> = Vec::with_capacity(roots.len());
            for rc in &roots {
                per_cycle.push(string_pack_cids(&decode(
                    &cs.get(rc).await.expect("root bytes"),
                )));
            }
            let final_packs = per_cycle.last().unwrap().clone();

            // 1. The routing table is bounded by data, not by build count. Every
            //    cycle appended a pack, so an append-only table would hold at
            //    least CYCLES entries.
            assert!(
                final_packs.len() < CYCLES,
                "string routing table holds {} packs after {CYCLES} cycles — no compaction happened",
                final_packs.len()
            );

            // 2. The table actually shrank at some cycle, and the packs that
            //    disappeared are exactly the merge inputs.
            let merge_at = (1..per_cycle.len())
                .find(|&i| per_cycle[i].len() < per_cycle[i - 1].len())
                .expect("pack count never dropped; compaction never merged anything");
            let consumed: Vec<ContentId> = per_cycle[merge_at - 1]
                .iter()
                .filter(|cid| !per_cycle[merge_at].contains(cid))
                .cloned()
                .collect();
            assert!(
                consumed.len() >= 2,
                "a merge must consume at least two packs, saw {}",
                consumed.len()
            );

            // 3. Consumed packs are recorded as garbage, so GC can reclaim them.
            //    A merge that silently dropped its inputs would leak them forever.
            let mut found_in_garbage = false;
            for root_cid in &roots {
                let root = decode(&cs.get(root_cid).await.expect("root bytes"));
                let Some(garbage_ref) = root.garbage.as_ref() else {
                    continue;
                };
                let bytes = cs.get(&garbage_ref.id).await.expect("garbage bytes");
                let record: fluree_db_indexer::GarbageRecord =
                    serde_json::from_slice(&bytes).expect("parse garbage record");
                if consumed
                    .iter()
                    .any(|cid| record.garbage.contains(&cid.to_string()))
                {
                    found_in_garbage = true;
                    break;
                }
            }
            assert!(
                found_in_garbage,
                "compacted-away packs never reached a garbage manifest — they would leak"
            );

            // 4. Every id still resolves through the compacted dictionary. A
            //    mis-rebased page offset or a lost routing entry shows up here
            //    and nowhere else.
            let db = fluree.db(ledger_id).await.expect("db view");
            let q = json!({
                "@context": { "ex": "http://example.org/" },
                "select": ["?s", "?l"],
                "where": { "@id": "?s", "ex:label": "?l" }
            });
            let res = fluree.query(&db, &q).await.expect("label query");
            let rows = res.to_jsonld(&db.snapshot).expect("format jsonld");
            let text = serde_json::to_string(&rows).expect("serialize rows");
            for cycle in 0..CYCLES {
                for i in 0..3 {
                    let label = format!("label for cycle {cycle} entity {i}");
                    assert!(
                        text.contains(&label),
                        "\"{label}\" did not survive compaction"
                    );
                    // Subject IRIs resolve through the (separately compacted)
                    // subject forward packs. Match the suffix so this holds
                    // whether the formatter emits a compacted or absolute IRI.
                    let iri_suffix = format!("cycle{cycle}/entity{i}");
                    assert!(
                        text.contains(&iri_suffix),
                        "subject {iri_suffix} did not survive compaction"
                    );
                }
            }
        })
        .await;
}
