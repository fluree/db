# Serverless Storage Choices

This guide covers storage placement for cloud and serverless deployments, especially
AWS Lambda-style architectures where commit writes, background indexing, and query
execution may run in separate warm containers.

## Short Recommendation

Use **Standard S3 for commits**. Commits are the durable source of truth and should
use the strongest redundancy profile available. Indexes are derived from commits
and can be rebuilt, so index storage can be optimized separately.

For indexes:

- Choose **Standard S3** when cost, multi-AZ durability, and operational simplicity
  matter more than the lowest cold-read/indexing latency.
- Choose **S3 Express One Zone** when you need lower latency for cold queries,
  sustained incremental indexing, or workloads that touch many small index blobs.
- Keep a local disk artifact cache enabled for Lambda/query/indexer workers. It
  is what makes hot query performance largely independent of the remote index
  bucket.

## Commit Storage

Commit blobs are immutable and are the canonical history of the ledger. Indexes,
statistics, and derived query structures can always be reconstructed from commits.

For that reason, prefer Standard S3 for `commitStorage` even when using S3 Express
One Zone for `indexStorage`:

```json
{
  "@context": {
    "@base": "https://ns.flur.ee/config/connection/",
    "@vocab": "https://ns.flur.ee/system#"
  },
  "@graph": [
    {
      "@id": "commitStorage",
      "@type": "Storage",
      "s3Bucket": "fluree-commits",
      "s3Prefix": "commits/"
    },
    {
      "@id": "indexStorage",
      "@type": "Storage",
      "s3Bucket": "fluree-index--use1-az1--x-s3",
      "s3Prefix": "indexes/"
    },
    {
      "@id": "connection",
      "@type": "Connection",
      "commitStorage": { "@id": "commitStorage" },
      "indexStorage": { "@id": "indexStorage" }
    }
  ]
}
```

When using S3 Express One Zone, omit `s3Endpoint` and let the AWS SDK resolve the
directory-bucket endpoint.

## What To Expect

Every database and query shape is different. The ranges below come from internal
serverless benchmarks using identical Lambda binaries, one stack backed by
Standard S3 for indexes and one backed by S3 Express One Zone for indexes. Dataset
names and customer-specific identifiers are intentionally omitted.

### Transactions

Transactions showed no meaningful difference between Standard S3 and S3 Express
for index storage.

In the benchmark, transaction wall time was dominated by the synchronous commit
path through the transactor and queueing layer, not by index storage. Across a
medium staged JSON-LD workload, both backends landed in the same rough multi-second
range per commit, with differences lost in normal Lambda and queue variance.

### Queries

Hot queries should show little to no difference. Once the relevant root, branch,
leaf, dictionary, and sidecar artifacts are in local memory or the disk artifact
cache, the remote bucket is no longer on the critical path.

For cold or partially warm queries:

- Simple lookups that touch only a small number of index blobs are usually close.
  Expect roughly no measurable difference to about a 30% slowdown on Standard S3.
- Queries that touch many index blobs can show a larger cold-cache gap, because
  Standard S3 has higher per-object latency and the penalty compounds with the
  number of small reads.
- If background indexing falls behind, query latency can degrade for reasons
  unrelated to S3 Express vs Standard S3. The query engine may need to account
  for unindexed commits, and that work grows with the indexing gap.

In one small synthetic workload, Standard S3 query medians were about 10-35%
slower before cache effects dominated. In a medium staged workload with the
indexer keeping up, Standard and Express query medians were effectively
indistinguishable, with individual queries varying by normal runtime noise.

### Indexing

Indexing is the area where index bucket choice is most visible. Incremental
indexing reads and writes many small content-addressed artifacts: roots, branch
manifests, leaves, sidecars, dictionaries, sketches, and other derived blobs.
S3 Express One Zone is optimized for this small-object, low-latency access pattern.

In a medium staged workload of roughly 8.5 MB JSON-LD, about 10,000 subjects, and
five commits, Standard S3 indexing was consistently slower but completed cleanly:

| Index Event | Express Indexing | Standard S3 Indexing | Standard / Express |
|-------------|------------------|----------------------|--------------------|
| Initial build | ~0.5 s | ~1.2 s | ~2.5x |
| Incremental updates | ~0.7-1.1 s | ~1.5-1.9 s | ~1.7-2.5x |

Treat these as ballpark ranges, not guarantees. Larger ledgers, wider class and
property statistics, more named graphs, and colder caches can increase the gap.
On the other hand, hot repeated query traffic may see almost no difference.

## Choosing An Index Backend

Use Standard S3 for indexes when:

- You want multi-AZ S3 durability for both commits and indexes.
- You are cost-sensitive or want to avoid the S3 Express per-bucket cost floor.
- Your workload is mostly hot queries, modest indexing volume, or can tolerate
  indexing taking roughly twice as long.

Use S3 Express One Zone for indexes when:

- Cold query latency matters.
- Incremental indexing throughput is important.
- Queries touch many index segments before the local cache is warm.
- You can tolerate the single-AZ durability profile because indexes are
  reproducible from commits.

## Tuning Notes

- Size Lambda `/tmp` large enough for the disk artifact cache. The cache softens
  both query and indexing latency by avoiding repeated remote reads of immutable
  artifacts.
- Use `s3MaxConcurrentRequests` to cap per-process S3 SDK concurrency if a
  deployment shows signs of retry storms or HTTP-layer stalls.
- Use the indexer worker's own processing time logs for indexing measurements.
  Client-side polling includes nameservice propagation and scheduling delays.
- Watch `index_t` vs `commit_t`. If `index_t` lags behind `commit_t`, query
  latency may reflect catch-up work rather than the raw performance of the
  selected index bucket.
