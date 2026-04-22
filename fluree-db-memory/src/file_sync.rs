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
use std::path::Path;
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

/// Recompute and write the build hash.
///
/// Called after any mutation to `.ttl` files (add, update, forget).
pub fn update_hash(memory_dir: &Path) -> Result<()> {
    let hash = compute_build_hash(memory_dir)?;
    write_stored_hash(memory_dir, &hash)
}

/// Check if the ledger needs rebuilding from files.
///
/// Returns true if:
/// - Stored hash is missing (fresh clone or first init)
/// - Hash mismatch (files changed externally — e.g., `git pull`)
/// - Ledger doesn't exist (deleted or corrupted)
pub async fn needs_rebuild(store: &MemoryStore, memory_dir: &Path) -> Result<bool> {
    // Check if ledger exists
    if !store.is_initialized().await? {
        debug!("Memory ledger not initialized — rebuild needed");
        return Ok(true);
    }

    let current_hash = compute_build_hash(memory_dir)?;
    let stored_hash = read_stored_hash(memory_dir);

    match stored_hash {
        None => {
            debug!("No stored build hash — rebuild needed");
            Ok(true)
        }
        Some(stored) if stored != current_hash => {
            debug!(
                stored = %stored,
                current = %current_hash,
                "Build hash mismatch — rebuild needed"
            );
            Ok(true)
        }
        _ => Ok(false),
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

    // Acquire exclusive lock
    let lock_path = memory_dir.join(".local").join("rebuild.lock");
    fs::create_dir_all(lock_path.parent().unwrap())?;
    let lock_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&lock_path)?;

    use fs2::FileExt;
    lock_file
        .lock_exclusive()
        .map_err(|e| MemoryError::FileSync(format!("failed to acquire rebuild lock: {e}")))?;

    // Re-check after acquiring lock (another process may have rebuilt)
    if !needs_rebuild(store, memory_dir).await? {
        debug!("Another process already rebuilt — skipping");
        // Lock auto-released when lock_file is dropped
        return Ok(());
    }

    debug!("Rebuilding memory ledger from files");
    let result = rebuild_from_files(store, memory_dir).await;

    // Lock auto-released when lock_file is dropped
    result
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
    store.drop_and_reinit().await?;

    // Phase 3: Batch transact
    if let Some(repo_data) = repo_jsonld {
        debug!("Importing repo memories from repo.ttl");
        transact_batch(store, repo_data).await?;
    }

    if let Some(user_data) = user_jsonld {
        debug!("Importing user memories from user.ttl");
        transact_batch(store, user_data).await?;
    }

    // Phase 4: Write hash
    let hash = compute_build_hash(memory_dir)?;
    write_stored_hash(memory_dir, &hash)?;

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
