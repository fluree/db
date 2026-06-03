# Hardware sizing: CPU vs disk (a worked benchmark)

This guide is a concrete, reproducible example of how cloud hardware choices affect
Fluree, using a large public dataset. The headline result is that **import speed and
query speed respond to different levers**: bulk import is dominated by **storage
throughput**, while steady-state query latency is dominated by **single-thread CPU
performance**. Sizing a deployment well means knowing which one you are optimizing for.

The numbers below come from the open Fluree benchmark suite:

- **Repo:** <https://github.com/fluree/benchmark-db>
- **Query set:** [SPARQLoscope](https://purl.org/ad-freiburg/sparqloscope) — a
  105-query suite spanning joins, OPTIONAL/MINUS/EXISTS, UNION, aggregates, numeric
  and date functions, string/REGEX, transitive paths, and result-size/export.
- **Dataset:** DBLP-core (the standard DBLP bibliography), DROPS monthly archive
  `2026-06-01` — **574.2 M** N-Triples lines (**561.5 M** distinct after dedup),
  90 predicates, ~73.5 GB uncompressed. Fluree builds a **27 GB** inline-indexed
  ledger from it.

Method: `fluree create dblp --from dblp.ttl` for import; for queries, 1 warmup + 3
timed runs per query, median per query, then aggregated across the 105 queries.
Each machine has 16 cores and 64 GB RAM, differing in CPU family and disk.

## Import: storage-bound

| Instance | CPU | Disk | Import | Throughput | Peak RSS | Index |
|---|---|---|--:|--:|--:|--:|
| `m7a.4xlarge` | AMD EPYC Zen 4 (~3.7 GHz) | gp3 EBS (500 MB/s) | 504 s | 1.14 M tr/s | 21.9 GB | 27 GB |
| `m7gd.4xlarge` | Graviton3 (~2.6 GHz) | local NVMe | 430 s | 1.33 M tr/s | 24.6 GB | 27 GB |
| `m8gd.4xlarge` | Graviton4 (~2.8 GHz) | local NVMe | **382 s** | **1.50 M tr/s** | 24.4 GB | 27 GB |

The two NVMe machines import **15–24% faster** than the EBS machine **even though
their CPUs are slower per core**. That is the tell-tale sign that import is
**I/O-bound, not CPU-bound**: a slower processor still finishes sooner once it is
fed by a faster disk. During the dominant parse + sorted-commit phase the CPU sits
around 25–30% busy on all three machines — the bottleneck is how fast triples can
be streamed off disk and indexed onto disk. If your workload is ingest-heavy
(large initial loads, frequent bulk imports), **prioritize local NVMe / high disk
throughput** over raw core speed.

## Query: CPU-bound

After warmup the working set (the 27 GB index) is resident in the 64 GB page cache,
so the disk barely participates and query latency tracks the **CPU**:

| Instance | CPU | Average (mean) | Geo mean | Median |
|---|---|--:|--:|--:|
| `m7a.4xlarge` | AMD EPYC Zen 4 (~3.7 GHz) | **936 ms** | **119 ms** | **67 ms** |
| `m8gd.4xlarge` | Graviton4 (~2.8 GHz) | 1,003 ms | 145 ms | 94 ms |
| `m7gd.4xlarge` | Graviton3 (~2.6 GHz) | 1,153 ms | 167 ms | 110 ms |

Here the ordering is the reverse of import: the highest-clock core (Zen 4) is
fastest, and the ranking lines up with single-thread performance, not disk. The
gap is widest on the cheapest queries — where fixed per-query overhead (planning,
parsing, serialization) dominates and there is little parallel work to hide a
slower core — and narrows on the heavy joins and aggregates. Newer cores help:
Graviton4 is ~13% faster than Graviton3 on the same suite. If your workload is
query-latency-sensitive (interactive APIs, many small reads), **prioritize a
high-clock / high-IPC CPU** and ensure RAM comfortably holds the working index.

## Putting it together

| If you optimize for… | Choose… | Why |
|---|---|---|
| Bulk import / ingest throughput | local NVMe, high disk bandwidth | import is I/O-bound |
| Query latency | fast single-thread CPU + enough RAM to cache the index | queries are CPU-bound and served from cache |
| Mixed | a modern core **with** NVMe (e.g. Graviton4 + NVMe) | best balance of both |

A practical rule of thumb: **size RAM to hold the active index** (so queries stay
in cache), pick the **fastest core** you can for query latency, and reach for
**local NVMe** when import time matters. The same-hardware effect is real — moving
from network block storage to local NVMe shortens import with no change to the CPU
— so storage class is a first-class sizing decision, not an afterthought.

To reproduce these numbers or run the suite against your own hardware and datasets,
see <https://github.com/fluree/benchmark-db>.
