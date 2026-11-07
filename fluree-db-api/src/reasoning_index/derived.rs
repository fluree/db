//! The derived-only set D of a generation, and the synthetic commit that
//! carries a fold's facts into the indexer.
//!
//! Both use the commit codec: it is Sid-direct, so the facts stay in
//! namespace-code space, independent of any dictionary. D is stored as
//! buckets of facts by subject, each its own content-addressed blob behind
//! [`DERIVED_FACTS_MAGIC`], listed by a [`DerivedIndex`]. Facts are stored at
//! `t` 0 and sorted, so a bucket whose facts did not change has the same
//! content id in the next generation and is not written again.

use fluree_db_core::commit::codec::{read_commit, write_commit};
use fluree_db_core::reasoning_index::{DerivedIndex, DERIVED_FACTS_MAGIC};
use fluree_db_core::{Commit, ContentId, ContentKind, ContentStore, Flake, IndexType};
use sha2::{Digest, Sha256};

use crate::{ApiError, Result};

/// Facts per bucket the set aims for; the bucket count is a power of two.
const FACTS_PER_BUCKET: usize = 16 * 1024;
const MAX_BUCKETS: usize = 4096;

/// Encode facts at `t` as a commit-codec blob.
pub(crate) fn encode_facts(t: i64, flakes: Vec<Flake>) -> Result<Vec<u8>> {
    let commit = Commit::new(t, flakes);
    write_commit(&commit, true, None)
        .map(|written| written.bytes)
        .map_err(|e| ApiError::internal(format!("encode reasoned facts: {e}")))
}

fn bucket_count(facts: usize) -> usize {
    facts
        .div_ceil(FACTS_PER_BUCKET)
        .max(1)
        .next_power_of_two()
        .min(MAX_BUCKETS)
}

/// A fact's bucket: a stable hash of its graph and subject.
fn bucket_of(flake: &Flake, buckets: usize) -> usize {
    let mut hasher = Sha256::new();
    if let Some(g) = &flake.g {
        hasher.update(g.namespace_code.to_be_bytes());
        hasher.update(g.name.as_bytes());
    }
    hasher.update([0xff]);
    hasher.update(flake.s.namespace_code.to_be_bytes());
    hasher.update(flake.s.name.as_bytes());
    let digest = hasher.finalize();
    let n = u64::from_be_bytes(digest[..8].try_into().expect("8 bytes"));
    (n % buckets as u64) as usize
}

/// Write a derived-only set to `store`, returning its index's content id.
/// Buckets already in the store are not written again.
pub(crate) async fn write_derived_only(
    store: &dyn ContentStore,
    flakes: Vec<Flake>,
) -> Result<ContentId> {
    let n = bucket_count(flakes.len());
    let facts = flakes.len() as u64;
    let mut buckets: Vec<Vec<Flake>> = (0..n).map(|_| Vec::new()).collect();
    for mut flake in flakes {
        flake.t = 0;
        flake.op = true;
        let b = bucket_of(&flake, n);
        buckets[b].push(flake);
    }
    let mut index = DerivedIndex {
        facts,
        buckets: Vec::with_capacity(n),
    };
    for mut bucket in buckets {
        bucket.sort_by(|a, b| IndexType::Spot.compare(a, b));
        bucket.dedup_by(|a, b| IndexType::Spot.compare(a, b).is_eq());
        let mut bytes = DERIVED_FACTS_MAGIC.to_vec();
        bytes.extend(encode_facts(0, bucket)?);
        let id = ContentId::new(ContentKind::DerivedFacts, &bytes);
        let present = store
            .has(&id)
            .await
            .map_err(|e| ApiError::internal(format!("derived-only set: {e}")))?;
        if !present {
            store
                .put_with_id(&id, &bytes)
                .await
                .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))?;
        }
        index.buckets.push(id);
    }
    store
        .put(ContentKind::DerivedFacts, &index.encode())
        .await
        .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))
}

/// Read a derived-only set written by [`write_derived_only`].
pub(crate) async fn read_derived_only(
    store: &dyn ContentStore,
    id: &ContentId,
) -> Result<Vec<Flake>> {
    let read = |id: ContentId| async move {
        store
            .get(&id)
            .await
            .map_err(|e| ApiError::internal(format!("read derived-only set {id}: {e}")))
    };
    let index = DerivedIndex::decode(&read(id.clone()).await?).map_err(ApiError::internal)?;
    let mut flakes = Vec::with_capacity(index.facts as usize);
    for bucket in index.buckets {
        let bytes = read(bucket).await?;
        let body = bytes
            .strip_prefix(DERIVED_FACTS_MAGIC.as_slice())
            .ok_or_else(|| ApiError::internal("not a derived-only fact set (bad magic)"))?;
        let commit = read_commit(body)
            .map_err(|e| ApiError::internal(format!("decode derived-only set: {e}")))?;
        flakes.extend(commit.flakes);
    }
    Ok(flakes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluree_db_core::{FlakeValue, MemoryContentStore, Sid};

    fn fact(s: &str, t: i64) -> Flake {
        Flake::new(
            Sid::new(8, s),
            Sid::new(3, "type"),
            FlakeValue::Ref(Sid::new(8, "Person")),
            Sid::new(1, "id"),
            t,
            true,
            None,
        )
    }

    #[tokio::test]
    async fn an_unchanged_bucket_keeps_its_content_id_across_generations() {
        let store = MemoryContentStore::new();
        let many: Vec<Flake> = (0..40_000).map(|i| fact(&format!("s{i}"), 5)).collect();
        let first = write_derived_only(&store, many.clone()).await.unwrap();
        let mut more = many.clone();
        more.push(fact("one-more", 9));
        let second = write_derived_only(&store, more).await.unwrap();
        let a = DerivedIndex::decode(&store.get(&first).await.unwrap()).unwrap();
        let b = DerivedIndex::decode(&store.get(&second).await.unwrap()).unwrap();
        assert_eq!(a.buckets.len(), b.buckets.len());
        let changed = a
            .buckets
            .iter()
            .zip(&b.buckets)
            .filter(|(x, y)| x != y)
            .count();
        assert_eq!(changed, 1, "one subject added changes one bucket");
        let back = read_derived_only(&store, &second).await.unwrap();
        assert_eq!(back.len(), 40_001);
    }
}
