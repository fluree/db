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
use std::collections::{BTreeMap, HashSet};

use fluree_db_core::{Commit, ContentId, ContentKind, ContentStore, Flake, IndexType, Sid};
use sha2::{Digest, Sha256};

use crate::{ApiError, Result};

/// Facts per bucket the set aims for; the bucket count is a power of two.
/// Small under test, so a few facts cross the partition thresholds.
const FACTS_PER_BUCKET: usize = if cfg!(test) { 4 } else { 16 * 1024 };
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

/// A bucket's facts, sorted and deduplicated, as its content-addressed blob.
fn encode_bucket(mut bucket: Vec<Flake>) -> Result<(ContentId, Vec<u8>)> {
    bucket.sort_by(|a, b| IndexType::Spot.compare(a, b));
    bucket.dedup_by(|a, b| IndexType::Spot.compare(a, b).is_eq());
    let mut bytes = DERIVED_FACTS_MAGIC.to_vec();
    bytes.extend(encode_facts(0, bucket)?);
    Ok((ContentId::new(ContentKind::DerivedFacts, &bytes), bytes))
}

/// Write a bucket unless the store already holds it.
async fn put_bucket(store: &dyn ContentStore, id: &ContentId, bytes: &[u8]) -> Result<()> {
    let present = store
        .has(id)
        .await
        .map_err(|e| ApiError::internal(format!("derived-only set: {e}")))?;
    if !present {
        store
            .put_with_id(id, bytes)
            .await
            .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))?;
    }
    Ok(())
}

async fn read_bucket(store: &dyn ContentStore, id: &ContentId) -> Result<Vec<Flake>> {
    let bytes = store
        .get(id)
        .await
        .map_err(|e| ApiError::internal(format!("read derived-only set {id}: {e}")))?;
    let body = bytes
        .strip_prefix(DERIVED_FACTS_MAGIC.as_slice())
        .ok_or_else(|| ApiError::internal("not a derived-only fact set (bad magic)"))?;
    let commit = read_commit(body)
        .map_err(|e| ApiError::internal(format!("decode derived-only set: {e}")))?;
    Ok(commit.flakes)
}

async fn read_index(store: &dyn ContentStore, id: &ContentId) -> Result<DerivedIndex> {
    let bytes = store
        .get(id)
        .await
        .map_err(|e| ApiError::internal(format!("read derived-only set {id}: {e}")))?;
    DerivedIndex::decode(&bytes).map_err(ApiError::internal)
}

/// A fact as a bucket holds it: at `t` 0, asserted.
fn stored(mut flake: Flake) -> Flake {
    flake.t = 0;
    flake.op = true;
    flake
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
    for flake in flakes {
        let flake = stored(flake);
        let b = bucket_of(&flake, n);
        buckets[b].push(flake);
    }
    let mut index = DerivedIndex {
        facts,
        buckets: Vec::with_capacity(n),
    };
    for bucket in buckets {
        let (id, bytes) = encode_bucket(bucket)?;
        put_bucket(store, &id, &bytes).await?;
        index.buckets.push(id);
    }
    store
        .put(ContentKind::DerivedFacts, &index.encode())
        .await
        .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))
}

/// The derived-only set `id` with `remove` taken out and `add` put in,
/// written by rewriting only the buckets those facts fall in; the rest keep
/// their content ids. Facts match by graph as well as content. Returns the
/// new set's index and its size.
///
/// The predecessor's partitioning is kept: a fact's bucket depends on the
/// bucket count, so it changes only by an explicit repartition, a full
/// rewrite, once the set outgrows its buckets.
pub(crate) async fn update_derived_only(
    store: &dyn ContentStore,
    id: &ContentId,
    remove: Vec<Flake>,
    add: Vec<Flake>,
) -> Result<(ContentId, u64)> {
    let mut index = read_index(store, id).await?;
    let n = index.buckets.len();
    let mut touched: BTreeMap<usize, (Vec<Flake>, Vec<Flake>)> = BTreeMap::new();
    for flake in remove {
        let flake = stored(flake);
        touched
            .entry(bucket_of(&flake, n))
            .or_default()
            .0
            .push(flake);
    }
    for flake in add {
        let flake = stored(flake);
        touched
            .entry(bucket_of(&flake, n))
            .or_default()
            .1
            .push(flake);
    }
    let mut facts = index.facts;
    for (b, (remove, add)) in touched {
        let mut bucket = read_bucket(store, &index.buckets[b]).await?;
        let before = bucket.len() as u64;
        let gone: HashSet<(Option<Sid>, Flake)> =
            remove.into_iter().map(|f| (f.g.clone(), f)).collect();
        bucket.retain(|f| !gone.contains(&(f.g.clone(), f.clone())));
        bucket.extend(add);
        let (new_id, bytes) = encode_bucket(bucket)?;
        // The count after sorting and deduplication.
        let after = read_commit(&bytes[DERIVED_FACTS_MAGIC.len()..])
            .map_err(|e| ApiError::internal(format!("decode derived-only set: {e}")))?
            .flakes
            .len() as u64;
        facts = facts + after - before;
        put_bucket(store, &new_id, &bytes).await?;
        index.buckets[b] = new_id;
    }
    index.facts = facts;
    if bucket_count(facts as usize) > n {
        let mut all = Vec::with_capacity(facts as usize);
        for bucket in &index.buckets {
            all.extend(read_bucket(store, bucket).await?);
        }
        return Ok((write_derived_only(store, all).await?, facts));
    }
    let new_id = store
        .put(ContentKind::DerivedFacts, &index.encode())
        .await
        .map_err(|e| ApiError::internal(format!("write derived-only set: {e}")))?;
    Ok((new_id, facts))
}

/// Read a derived-only set written by [`write_derived_only`].
pub(crate) async fn read_derived_only(
    store: &dyn ContentStore,
    id: &ContentId,
) -> Result<Vec<Flake>> {
    let index = read_index(store, id).await?;
    let mut flakes = Vec::with_capacity(index.facts as usize);
    for bucket in &index.buckets {
        flakes.extend(read_bucket(store, bucket).await?);
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

    fn in_graph(mut f: Flake, g: &str) -> Flake {
        f.g = Some(Sid::new(8, g));
        f
    }

    fn keyed(flakes: Vec<Flake>) -> HashSet<(Option<Sid>, Flake)> {
        flakes.into_iter().map(|f| (f.g.clone(), f)).collect()
    }

    async fn buckets(store: &MemoryContentStore, id: &ContentId) -> Vec<ContentId> {
        DerivedIndex::decode(&store.get(id).await.unwrap())
            .unwrap()
            .buckets
    }

    /// An update rewrites only the buckets its facts fall in, matches facts
    /// by graph, ignores a fact already held, and holds what a full write of
    /// the same facts holds.
    #[tokio::test]
    async fn an_update_rewrites_only_the_buckets_it_touches() {
        let store = MemoryContentStore::new();
        let base: Vec<Flake> = (0..20).map(|i| fact(&format!("s{i}"), 1)).collect();
        let first = write_derived_only(&store, base.clone()).await.unwrap();
        let before = buckets(&store, &first).await;

        // s3 leaves; the same fact in another graph is not touched; s4 is
        // added again; one new subject arrives.
        let other = in_graph(fact("s5", 1), "other");
        let with_other = write_derived_only(&store, [base.clone(), vec![other.clone()]].concat())
            .await
            .unwrap();
        let (updated, facts) = update_derived_only(
            &store,
            &with_other,
            vec![fact("s3", 2), in_graph(fact("s6", 2), "other")],
            vec![fact("s4", 2), fact("new", 2)],
        )
        .await
        .unwrap();
        assert_eq!(facts, 21);
        let held = read_derived_only(&store, &updated).await.unwrap();
        let mut expected: Vec<Flake> = base
            .iter()
            .filter(|f| f.s.name.as_ref() != "s3")
            .cloned()
            .collect();
        expected.push(other);
        expected.push(fact("new", 2));
        assert_eq!(keyed(held), keyed(expected));

        let after = buckets(&store, &updated).await;
        assert_eq!(after.len(), buckets(&store, &with_other).await.len());
        let changed = buckets(&store, &with_other)
            .await
            .iter()
            .zip(&after)
            .filter(|(x, y)| x != y)
            .count();
        assert!(
            changed <= 3,
            "{changed} buckets rewritten for 3 changed facts"
        );
        assert_eq!(before.len(), after.len());
    }

    /// Growing past its buckets repartitions the set, and nothing is lost.
    #[tokio::test]
    async fn an_update_that_outgrows_its_buckets_repartitions() {
        let store = MemoryContentStore::new();
        let base: Vec<Flake> = (0..8).map(|i| fact(&format!("s{i}"), 1)).collect();
        let first = write_derived_only(&store, base.clone()).await.unwrap();
        let n = buckets(&store, &first).await.len();
        let add: Vec<Flake> = (0..40).map(|i| fact(&format!("n{i}"), 2)).collect();
        let (updated, facts) = update_derived_only(&store, &first, Vec::new(), add.clone())
            .await
            .unwrap();
        assert_eq!(facts, 48);
        assert!(buckets(&store, &updated).await.len() > n);
        let held = read_derived_only(&store, &updated).await.unwrap();
        assert_eq!(keyed(held), keyed([base, add].concat()));
    }
}
