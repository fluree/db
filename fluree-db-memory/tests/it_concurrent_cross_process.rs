//! Concurrent cross-process memory writes must not lose updates.
//!
//! Models several processes (MCP servers and/or CLI invocations — e.g. Cursor
//! and Claude Code, or multiple Claude sessions) operating on the same
//! `.fluree-memory` directory in one project. Each process gets a
//! **process-private in-memory ledger** (matching `context::build_memory_fluree`);
//! the git-tracked `.ttl` file is the shared source of truth, guarded by a
//! cross-process file lock. Before the fix, a writer rewrote the file from its
//! own stale ledger and silently dropped a concurrent writer's memory.

use fluree_db_api::FlureeBuilder;
use fluree_db_memory::{MemoryFilter, MemoryInput, MemoryKind, MemoryStore, Scope};
use std::sync::Arc;

fn mk(store_idx: usize, i: usize) -> MemoryInput {
    MemoryInput {
        kind: MemoryKind::Fact,
        content: format!("cross-process memory from store {store_idx} item {i}"),
        tags: vec!["concurrency".to_string(), "cross-process".to_string()],
        scope: Scope::Repo,
        severity: None,
        artifact_refs: Vec::new(),
        branch: None,
        rationale: None,
        alternatives: None,
    }
}

fn build_store(mem_dir: &std::path::Path) -> MemoryStore {
    // Process-private in-memory ledger over the shared `.fluree-memory` files,
    // matching the production `mcp serve` configuration.
    let fluree = FlureeBuilder::memory().build_memory();
    MemoryStore::new_ephemeral_ledger(fluree, Some(mem_dir.to_path_buf()))
}

#[test]
fn concurrent_cross_process_adds_do_not_lose_updates() {
    // An 8 MB worker stack mirrors the CLI runtime (main::WORKER_STACK_SIZE):
    // the in-lock rebuild-from-file path is deep and overflows the default 2 MB
    // tokio worker stack under contention.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let tmp = tempfile::tempdir().unwrap();
        let mem_dir = tmp.path().join(".fluree-memory");
        std::fs::create_dir_all(&mem_dir).unwrap();

        const STORES: usize = 3;
        const PER_STORE: usize = 8;
        let expected = STORES * PER_STORE;

        let stores: Vec<Arc<MemoryStore>> = (0..STORES)
            .map(|_| Arc::new(build_store(&mem_dir)))
            .collect();

        // Create the shared directory structure / `.ttl` files once before the
        // storm so first-add file creation isn't itself a race.
        stores[0].initialize().await.expect("initialize");

        let mut handles = Vec::new();
        for (s, store) in stores.iter().enumerate() {
            for i in 0..PER_STORE {
                let store = Arc::clone(store);
                handles.push(tokio::spawn(async move { store.add(mk(s, i)).await }));
            }
        }
        for h in handles {
            h.await
                .expect("task join")
                .expect("add must succeed (no commit corruption)");
        }

        // File is truth: a fresh store rebuilds from the `.ttl` files, so its
        // ledger reflects exactly what persisted across all writers.
        let verifier = build_store(&mem_dir);
        verifier.ensure_synced().await.expect("final ensure_synced");
        let all = verifier
            .current_memories(&MemoryFilter::default())
            .await
            .expect("current_memories");

        assert_eq!(
            all.len(),
            expected,
            "lost updates: {} memories persisted, expected {expected}",
            all.len()
        );
    });
}
