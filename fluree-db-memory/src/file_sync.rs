//! File-based memory sync: watermark hashing, locking, and atomic rebuild.
//!
//! The Fluree `__memory` ledger is treated as a derived cache of the
//! `.ttl` source files. This module detects when files have changed
//! (via SHA-256 content hash) and rebuilds the ledger atomically.

use crate::error::{MemoryError, Result};
use crate::store::MemoryStore;
use crate::turtle_io;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::debug;

/// Schema version salt included in the build hash.
///
/// Bump this when the canonical Turtle format, predicate set, or `@fulltext`
/// injection rules change — forces a rebuild even if files haven't changed.
const SCHEMA_VERSION_SALT: &str = "MEMORY_SCHEMA_V1";

/// Maximum payload size (bytes) per transact batch during rebuild.
const MAX_BATCH_BYTES: usize = 512 * 1024; // 512 KB

// ---------------------------------------------------------------------------
// Hash computation
// ---------------------------------------------------------------------------

/// Compute a deterministic SHA-256 hash of the memory directory contents.
///
/// Includes: schema version salt + `repo.ttl` content + `user.ttl` content.
/// Missing files contribute empty bytes.
pub fn compute_build_hash(memory_dir: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_VERSION_SALT.as_bytes());

    let repo_path = turtle_io::repo_ttl_path(memory_dir);
    if repo_path.exists() {
        hasher.update(fs::read(&repo_path)?);
    }

    let user_path = turtle_io::user_ttl_path(memory_dir);
    if user_path.exists() {
        hasher.update(fs::read(&user_path)?);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Read the stored build hash from `.local/build-hash`.
pub fn read_stored_hash(memory_dir: &Path) -> Option<String> {
    let path = memory_dir.join(".local").join("build-hash");
    fs::read_to_string(path).ok().map(|s| s.trim().to_string())
}

/// Write the build hash to `.local/build-hash`.
pub fn write_stored_hash(memory_dir: &Path, hash: &str) -> Result<()> {
    let local_dir = memory_dir.join(".local");
    fs::create_dir_all(&local_dir)?;
    fs::write(local_dir.join("build-hash"), hash)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Sync check
// ---------------------------------------------------------------------------

/// Recompute and write the build hash, returning it.
///
/// Called after any mutation to `.ttl` files (add, update, forget). The
/// returned hash lets the caller advance its per-process sync watermark to
/// match the file it just wrote, without re-hashing.
pub fn update_hash(memory_dir: &Path) -> Result<String> {
    let hash = compute_build_hash(memory_dir)?;
    write_stored_hash(memory_dir, &hash)?;
    Ok(hash)
}

/// Check if **this process's** ledger needs rebuilding from files.
///
/// Returns true if:
/// - Ledger doesn't exist (deleted, corrupted, or fresh init)
/// - This process has not yet synced (no watermark — e.g. a fresh in-memory
///   ledger that has not ingested the files)
/// - The live file hash differs from the hash our ledger last ingested
///   (another process wrote the files, or `git pull` changed them)
///
/// The decision is made against the store's **per-process** watermark, not the
/// shared on-disk `build-hash`: that on-disk hash records whoever wrote the
/// files last, so a long-lived process whose own ledger never ingested those
/// writes would otherwise be fooled into thinking it is in sync. The watermark
/// is advanced only by our own rebuilds and mutations, so once set it rebuilds
/// whenever the files diverge from what our ledger holds.
///
/// On the first check the watermark is unset. For a persistent (file-backed)
/// ledger it is seeded from the on-disk hash — the just-loaded ledger is
/// consistent with it, avoiding a needless rebuild — but for an ephemeral
/// (in-memory) ledger seeding is unsafe (the fresh ledger is empty), so a
/// rebuild is forced. See [`MemoryStore::seed_watermark_from_disk`].
pub async fn needs_rebuild(store: &MemoryStore, memory_dir: &Path) -> Result<bool> {
    // Check if ledger exists
    if !store.is_initialized().await? {
        debug!("Memory ledger not initialized — rebuild needed");
        return Ok(true);
    }

    let synced = match store.synced_hash() {
        Some(h) => Some(h),
        None if store.seed_watermark_from_disk() => {
            let seed = read_stored_hash(memory_dir);
            store.set_synced_hash(seed.clone());
            seed
        }
        None => None,
    };

    match synced {
        None => {
            debug!("No watermark for this process — rebuild needed");
            Ok(true)
        }
        Some(synced) => {
            let current_hash = compute_build_hash(memory_dir)?;
            if synced != current_hash {
                debug!(
                    synced = %synced,
                    current = %current_hash,
                    "Files diverged from this process's ledger — rebuild needed"
                );
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sync entry point
// ---------------------------------------------------------------------------

/// Ensure the ledger is in sync with the `.ttl` files.
///
/// Called on every CLI/MCP invocation. Acquires an exclusive file lock
/// to prevent concurrent rebuilds.
pub async fn ensure_synced(store: &MemoryStore, memory_dir: &Path) -> Result<()> {
    // No memory dir → no file-based sync
    if !memory_dir.exists() {
        return Ok(());
    }

    // Check if any .ttl files exist (skip sync if memory dir is empty/uninitialized)
    let repo_path = turtle_io::repo_ttl_path(memory_dir);
    let user_path = turtle_io::user_ttl_path(memory_dir);
    if !repo_path.exists() && !user_path.exists() {
        return Ok(());
    }

    // Quick check before locking
    if !needs_rebuild(store, memory_dir).await? {
        return Ok(());
    }

    // Acquire the same exclusive lock used by file-backed mutations. `flock`
    // can block indefinitely behind another process, so run it on Tokio's
    // blocking pool rather than occupying an async worker thread.
    let rebuild_lock = acquire_memory_lock(memory_dir).await?;

    // Re-check after acquiring lock (another process may have rebuilt)
    if !needs_rebuild(store, memory_dir).await? {
        debug!("Another process already rebuilt — skipping");
        // Lock auto-released when lock_file is dropped
        return Ok(());
    }

    debug!("Rebuilding memory ledger from files");
    let result = rebuild_from_files(store, memory_dir).await;

    // Release the cross-process rebuild lock before returning.
    drop(rebuild_lock);
    result
}

/// Acquire the process-crossing lock for file-backed memory cache updates.
///
/// The returned file handle is the lock guard; dropping it releases the lock.
/// Callers may hold it across awaits because waiting for the OS lock itself is
/// offloaded to Tokio's blocking pool.
pub(crate) async fn acquire_memory_lock(memory_dir: &Path) -> Result<fs::File> {
    acquire_lock_file(memory_dir.join(".local").join("rebuild.lock")).await
}

async fn acquire_lock_file(lock_path: PathBuf) -> Result<fs::File> {
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;

        use fs2::FileExt;
        lock_file
            .lock_exclusive()
            .map_err(|e| MemoryError::FileSync(format!("failed to acquire rebuild lock: {e}")))?;
        Ok(lock_file)
    })
    .await
    .map_err(|e| MemoryError::FileSync(format!("failed to join rebuild lock task: {e}")))?
}

/// Rebuild the ledger from files if this process's ledger is stale, assuming
/// the caller **already holds** the cross-process file lock (and the store
/// mutation lock).
///
/// Unlike [`ensure_synced`], this acquires neither lock itself, so it is safe
/// to call from inside a mutation's locked critical section. `needs_rebuild`
/// and `rebuild_from_files` (via `drop_and_reinit_unlocked`) take no locks of
/// their own.
pub(crate) async fn rebuild_if_stale_unlocked(
    store: &MemoryStore,
    memory_dir: &Path,
) -> Result<()> {
    if !memory_dir.exists() {
        return Ok(());
    }
    if needs_rebuild(store, memory_dir).await? {
        debug!("In-lock rebuild: files diverged from this process's ledger");
        rebuild_from_files(store, memory_dir).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Atomic rebuild
// ---------------------------------------------------------------------------

/// Rebuild the `__memory` ledger from `.ttl` files.
///
/// **Atomic**: parses and validates all files first, then drops and recreates
/// the ledger. If parsing fails, the ledger is left untouched.
async fn rebuild_from_files(store: &MemoryStore, memory_dir: &Path) -> Result<()> {
    // Phase 1: Parse + validate (ledger untouched if this fails)
    let repo_path = turtle_io::repo_ttl_path(memory_dir);
    let user_path = turtle_io::user_ttl_path(memory_dir);

    let repo_jsonld = if repo_path.exists() {
        let content = fs::read_to_string(&repo_path)?;
        if has_memory_subjects(&content) {
            debug!("Parsing repo.ttl for rebuild");
            turtle_io::parse_and_inject_fulltext(&content)?
        } else {
            debug!("repo.ttl has no memory subjects, skipping");
            None
        }
    } else {
        None
    };

    let user_jsonld = if user_path.exists() {
        let content = fs::read_to_string(&user_path)?;
        if has_memory_subjects(&content) {
            debug!("Parsing user.ttl for rebuild");
            turtle_io::parse_and_inject_fulltext(&content)?
        } else {
            debug!("user.ttl has no memory subjects, skipping");
            None
        }
    } else {
        None
    };

    // Phase 2: Drop + reinit
    store.drop_and_reinit_unlocked().await?;

    // Phase 3: Batch transact
    if let Some(repo_data) = repo_jsonld {
        debug!("Importing repo memories from repo.ttl");
        transact_batch(store, repo_data).await?;
    }

    if let Some(user_data) = user_jsonld {
        debug!("Importing user memories from user.ttl");
        transact_batch(store, user_data).await?;
    }

    // Phase 4: Write hash and record it as this process's sync watermark —
    // our ledger now reflects exactly this file content.
    let hash = compute_build_hash(memory_dir)?;
    write_stored_hash(memory_dir, &hash)?;
    store.set_synced_hash(Some(hash.clone()));

    debug!(hash = %hash, "Memory ledger rebuilt from files");
    Ok(())
}

/// Transact a JSON-LD `@graph` payload, chunking if payload exceeds the byte limit.
async fn transact_batch(store: &MemoryStore, data: serde_json::Value) -> Result<()> {
    use serde_json::Value;

    // Extract @graph array and @context
    let (context, nodes) = match data {
        Value::Object(mut map) => {
            let ctx = map.remove("@context").unwrap_or(Value::Null);
            let graph = map.remove("@graph").unwrap_or(Value::Null);
            match graph {
                Value::Array(arr) => (ctx, arr),
                _ => return Ok(()), // no nodes
            }
        }
        _ => return Ok(()),
    };

    if nodes.is_empty() {
        return Ok(());
    }

    // Check total payload size
    let total_bytes: usize = nodes.iter().map(|n| n.to_string().len()).sum();

    if total_bytes <= MAX_BATCH_BYTES {
        // Single batch
        let doc = serde_json::json!({
            "@context": context,
            "@graph": nodes
        });
        store.transact_insert(&doc).await?;
    } else {
        // Chunk by byte size
        let mut chunk = Vec::new();
        let mut chunk_bytes = 0usize;

        for node in nodes {
            let node_bytes = node.to_string().len();
            if !chunk.is_empty() && chunk_bytes + node_bytes > MAX_BATCH_BYTES {
                let doc = serde_json::json!({
                    "@context": context,
                    "@graph": chunk
                });
                store.transact_insert(&doc).await?;
                chunk = Vec::new();
                chunk_bytes = 0;
            }
            chunk_bytes += node_bytes;
            chunk.push(node);
        }

        if !chunk.is_empty() {
            let doc = serde_json::json!({
                "@context": context,
                "@graph": chunk
            });
            store.transact_insert(&doc).await?;
        }
    }

    Ok(())
}

/// Check if a `.ttl` file has actual memory subject blocks (not just prefixes).
///
/// Looks for `mem:<kind>-` patterns which indicate real memory subjects
/// (e.g., `mem:fact-01JDXYZ...`). The old heuristic `content.contains("mem:")`
/// was too broad — it matches the `@prefix mem:` declaration in empty files.
fn has_memory_subjects(content: &str) -> bool {
    if content.trim().is_empty() {
        return false;
    }
    // Look for subject patterns: mem:<kind>-<ulid>
    // These patterns don't appear in prefix declarations or property references.
    for kind in &[
        "fact-",
        "decision-",
        "constraint-",
        "preference-",
        "artifact-",
    ] {
        let pattern = format!("mem:{kind}");
        if content.contains(&pattern) {
            return true;
        }
    }
    false
}

// We need hex encoding for sha2. Use a simple inline implementation
// to avoid pulling in the `hex` crate.
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_determinism() {
        let dir = tempfile::tempdir().unwrap();
        let memory_dir = dir.path();

        // Create repo.ttl
        let repo_path = turtle_io::repo_ttl_path(memory_dir);
        fs::create_dir_all(repo_path.parent().unwrap()).unwrap();
        fs::write(&repo_path, "test content").unwrap();

        let h1 = compute_build_hash(memory_dir).unwrap();
        let h2 = compute_build_hash(memory_dir).unwrap();
        assert_eq!(h1, h2, "hash should be deterministic");
    }

    #[test]
    fn hash_changes_with_content() {
        let dir = tempfile::tempdir().unwrap();
        let memory_dir = dir.path();

        let repo_path = turtle_io::repo_ttl_path(memory_dir);
        fs::create_dir_all(repo_path.parent().unwrap()).unwrap();
        fs::write(&repo_path, "content v1").unwrap();

        let h1 = compute_build_hash(memory_dir).unwrap();

        fs::write(&repo_path, "content v2").unwrap();
        let h2 = compute_build_hash(memory_dir).unwrap();

        assert_ne!(h1, h2, "hash should change when content changes");
    }

    #[test]
    fn hash_includes_user_file() {
        let dir = tempfile::tempdir().unwrap();
        let memory_dir = dir.path();

        let repo_path = turtle_io::repo_ttl_path(memory_dir);
        fs::create_dir_all(repo_path.parent().unwrap()).unwrap();
        fs::write(&repo_path, "repo content").unwrap();

        let h1 = compute_build_hash(memory_dir).unwrap();

        // Add user.ttl
        let user_path = turtle_io::user_ttl_path(memory_dir);
        fs::create_dir_all(user_path.parent().unwrap()).unwrap();
        fs::write(&user_path, "user content").unwrap();

        let h2 = compute_build_hash(memory_dir).unwrap();

        assert_ne!(h1, h2, "hash should include user.ttl");
    }

    #[test]
    fn stored_hash_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let memory_dir = dir.path();

        assert!(read_stored_hash(memory_dir).is_none());

        write_stored_hash(memory_dir, "abc123").unwrap();
        assert_eq!(read_stored_hash(memory_dir).unwrap(), "abc123");
    }

    #[test]
    fn has_memory_subjects_empty_file() {
        // Empty file with just prefixes — should return false
        let content = "\
# Fluree Memory — repo-scoped
# Auto-managed by `fluree memory`. Manual edits are supported.
@prefix mem: <https://ns.flur.ee/memory#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
";
        assert!(
            !has_memory_subjects(content),
            "empty file with only prefixes should not have memory subjects"
        );
    }

    #[test]
    fn has_memory_subjects_with_fact() {
        let content = "\
@prefix mem: <https://ns.flur.ee/memory#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

mem:fact-01jdxyz0000000000000000 a mem:Fact ;
    mem:content \"Test content\" .
";
        assert!(
            has_memory_subjects(content),
            "file with a fact subject should be detected"
        );
    }

    #[test]
    fn has_memory_subjects_with_decision() {
        let content = "\
@prefix mem: <https://ns.flur.ee/memory#> .
mem:decision-01jdxyz0000000000000000 a mem:Decision ;
    mem:content \"We decided X\" .
";
        assert!(
            has_memory_subjects(content),
            "file with a decision subject should be detected"
        );
    }

    #[test]
    fn has_memory_subjects_blank() {
        assert!(
            !has_memory_subjects(""),
            "blank content should not have subjects"
        );
        assert!(
            !has_memory_subjects("   \n\n  "),
            "whitespace-only should not have subjects"
        );
    }
}
