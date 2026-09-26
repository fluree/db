# Performance architecture

Fluree is a temporal, verifiable graph database with triple-level access control,
reasoning, and integrated search. Systems with this feature set are commonly
expected to trade speed for capability. In the published benchmarks below,
Fluree outperforms engines that provide none of these features.

This document describes the design decisions behind that performance, with links
into the code. It also describes the trade-offs Fluree makes and its current
limits; see [Limits and deliberate trade-offs](#limits-and-deliberate-trade-offs).

## Measured results

Head-to-head benchmarks against other engines are maintained in a separate
repository: **[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db)**.
Each comparison uses the same hardware and datasets for every engine and can be
reproduced from pinned S3 snapshots.

**SPARQLoscope on DBLP-core** (561 M triples, 105 queries, m7a.4xlarge 16c/64 GB):

| | Fluree | QLever | Virtuoso | MillenniumDB | Jena | Oxigraph | Blazegraph |
|---|---|---|---|---|---|---|---|
| Queries passed | 105/105 | 105/105 | 103/105 | 103/105 | 34/105 | 39/105 | 3/105 |
| Geo mean | **17.5 ms** | 202.4 ms (11.5×) | 299.7 ms (17.1×) | 1,664 ms (95×) | 67.7 s | 87.0 s | 332.9 s |
| Median (passed) | **26.6 ms** | 310.3 ms (11.7×) | 326.0 ms (12.3×) | 3,894 ms (147×) | 4.5 s | 5.1 s | 23.2 s |

**Wikidata-truthy** (8.19 B triples, r7a.16xlarge 64c/512 GB): Fluree geometric
mean 367.4 ms. The next engine, QLever, is 10.4× slower.

**WGPB** (full 21.5 B-triple Wikidata dump, 850 basic graph pattern queries,
r7a.8xlarge 32c/256 GB): 850/850 passed, 43 ms geometric mean.

**Pokec / openCypher** (Memgraph's benchgraph suite, 1.6 M nodes / 30.6 M edges,
r8a.4xlarge 16c/128 GB):

| | Fluree | Memgraph | Neo4j | FalkorDB |
|---|---|---|---|---|
| Durable writes | **1.73 ms** | 4.46 ms | 4.07 ms | 4.57 ms |
| Read-only | **1.47 ms** | 4.41 ms | 6.80 ms | 4.57 ms |

Fluree's write latency is for durable commits, which are committed and
recoverable when the call returns. The other engines ran with their default
durability settings, which provide weaker guarantees.

> The internal criterion benchmarks documented in
> [BENCHMARKING.md](../../BENCHMARKING.md) serve a different purpose: they detect
> per-PR regressions and do not compare Fluree with other engines.

## Summary

Seven design choices account for most of Fluree's performance:

1. **Integer-ID execution.** Dictionary encoding means joins compare `u64`s, not
   IRIs or strings. Many query shapes never access a dictionary.
2. **Per-column compressed blocks.** Queries decompress only the columns they
   filter on or project, and in some cases none.
3. **Directory-only answers.** Leaflet headers carry enough metadata to answer
   many aggregates from the index directory in `O(leaflets)` rather than
   `O(rows)`.
4. **A cost model based on statistics.** HLL-derived per-predicate statistics
   drive selectivity estimates. Cost constants that depend on one another are
   covered by tests.
5. **Specialized physical operators.** Hash join, property join, semijoin, and
   cyclic BGP operators each replace a nested-loop pattern that degrades on a
   particular shape. Operators also limit their work to what the consuming
   operator requires.
6. **Fast-path operators** that fuse scan and aggregate. Each checks its
   preconditions at runtime and falls back to the generic operator tree.
7. **Writes do not wait on indexing.** Commits land in an in-memory overlay;
   indexing runs in the background, copy-on-write, and is threshold-driven.

## Design principles for optimizations

The layers below describe the individual mechanisms. The following principles
apply to all of them:

- **General mechanisms are preferred.** A change that improves a class of
  queries, such as a better estimate, a cheaper scan, or a streaming operator,
  is preferred to one that recognizes a single query. Shape-specific paths are
  added only where generic execution performs poorly by orders of magnitude on
  a recurring shape.
- **Correctness is enforced below the optimization.** Overlay merging, time
  travel, and graph scoping are handled in the scan layer (Layer 2). Operators
  built on the cursor inherit that behavior and do not reimplement it.
- **Specialized paths can decline.** Admission criteria are narrow and are
  checked again at runtime, and the generic plan is retained as a fallback. A
  declined optimization costs a precondition check and does not affect results.
- **Soundness conditions are explicit.** Each rewrite states the semantic
  condition that makes it valid, such as duplicate-insensitive aggregates,
  unchanged row counts, or preserved multiplicity, and is not applied when that
  condition does not hold.
- **Equivalence is tested.** Fast paths are tested against the generic
  pipeline. Routing stamps (`MustFire` / `MustNotFire`) ensure a test exercises
  the path it targets rather than passing on the generic path. Where both paths
  share a plan, tests use an independent oracle, because agreement between two
  paths that share a defect is not evidence of correctness.
- **Decisions are observable and reversible.** Plan choices and rejected
  alternatives appear in [EXPLAIN](../query/explain.md), fast-path outcomes are
  traced, and each class of optimizer rewrite has an
  [environment variable that disables it](../troubleshooting/debugging-queries.md#isolating-an-optimizer-change)
  for comparison.
- **Resource limits still apply.** Specialized operators check cancellation and
  fuel, and charge the memory they retain, such as hash tables and key sets,
  against the query's budget.
- **Changes are measured and gated.** Each change is justified with before/after
  measurements and a result check in its pull request. Criterion benchmarks in
  [BENCHMARKING.md](../../BENCHMARKING.md) guard against later regressions.

---

## Layer 1: Storage and encoding

See [Index format](index-format.md) for the wire format. The properties relevant
to performance are:

**Four covering permutations:** `SPOT`, `PSOT`, `POST`, and `OPST`
([`fluree-db-core/src/comparator.rs`](../../fluree-db-core/src/comparator.rs)).
Every triple pattern shape except one resolves to a contiguous range scan on one
of them (see [Limits](#limits-and-deliberate-trade-offs)).

`OPST` holds **all object types**, not only references. Because it leads with the
object, its leaflets are segmented by `o_type`: IRI references form one
contiguous partition and each literal type forms its own. This makes both reverse
traversal (`?s ?p <iri>`, with `o_type = IRI_REF` pinned) and bound-literal scans
inexpensive. For example, `fast_string_prefix_count_all` answers
`FILTER(STRSTARTS(?o,"Com"))` by scanning an OPST slice bounded by the string
dictionary ID range for that prefix. `BinaryScanOperator` therefore prefers OPST
for a constant object with an unbound subject, with one exception described under
[Limits](#limits-and-deliberate-trade-offs).

**Numeric IDs throughout.** Subjects, predicates, graphs, datatypes, languages,
and string literals are stored in dictionaries; the index stores `u64`/`u32`
keys. Joins, grouping, and deduplication operate on integers. A query that does
not project a value does not decode it.

**Order-preserving encodings.** Numeric, temporal, and boolean objects are
encoded so that `o_key` byte order matches value order. This allows
`ORDER BY DESC(?o) LIMIT k`, `MIN`, and `MAX` to be answered without a full scan
(Layer 5).

**Independently compressed per-column blocks.** A V3 leaflet stores one zstd
block per column (`SId`, `PId`, `OType`, `OKey`, `OI`, `T`), each with its own
`ColumnBlockRef`. A query decodes only the columns it filters on or projects; a
scan by key does not decode `T` or `OI`.

Columns that are constant within a leaflet are omitted from the block set.
POST/PSOT leaflets are predicate-homogeneous, so `p_id` is stored once as
`p_const`. OPST leaflets are type-homogeneous by segmentation, so `o_type` is
stored as `o_type_const`; other orders do the same when a leaflet is
single-typed. Each column uses the narrowest integer width that fits the
dictionary cardinality.

**History is stored outside the leaflet.** Time-travel data is a separate
content-addressed object, the per-leaf **history sidecar** (`FHS1`), located via
`LeafEntry.sidecar_cid` on the branch manifest. It holds per-leaflet segments of
31-byte `HistEntryV2` transition records sorted newest-first. A current-state
query does not fetch, decompress, or cache any history data, and the leaflet
cache excludes sidecar data. Time-travel support therefore adds no cost to
current-state queries.

**Leaflet directories.** Each leaf's uncompressed header carries a
`LeafletDirEntryV3` per leaflet: `row_count`, `lead_group_count`, 26-byte
`first_key` / `last_key` routing keys, the constant `p_const` / `o_type_const`
values, and the per-column block references. Several optimizations depend on
this directory. For example, when `first_key(i) == first_key(i+1)` in POST order,
leaflet `i` contains a single `(p, o)` group and can be counted without
decompression.

The same directory entry also carries the leaflet's history locator
(`history_offset`, `history_len`, `history_min_t`, `history_max_t`): an offset
range into the sidecar blob, not inline data. The `min_t`/`max_t` pair lets a
time-travel query skip a leaflet's history segment without reading it.

**Content addressing.** Leaves, branches, and dictionary blobs are addressed by
SHA-256 (local) or CIDv1 (remote). Because an address identifies its content,
cached blobs never need invalidation.

## Layer 2: Scan, decode, and cache

`BinaryCursor` yields `ColumnBatch` values, which are leaflet-at-a-time columnar
batches. A `ColumnProjection` / `ColumnSet` declares which columns the consumer
needs, and unrequested columns are not decoded
([`binary_scan.rs`](../../fluree-db-query/src/binary_scan.rs)).

**Late materialization.** When the persisted index is authoritative for decoding,
scans emit encoded bindings (`EncodedSid`, `EncodedPid`, `EncodedLit`) instead of
IRIs and values. Joins, filters on encoded keys, grouping, and deduplication
operate on these integers, and values are decoded once, at projection, for the
rows that remain.

**Overlay merging in the cursor.** The cursor skips base rows retracted by the
overlay, injects overlay assertions, and applies `to_t` before the operator above
receives a row. Correctness under uncommitted writes and time travel is
therefore a property of the scan layer and does not need to be reimplemented by
each operator.

Graph scoping is enforced at the same boundary. `BinaryGraphView` is a
graph-scoped decode handle, so leaflet decoding, predicate dictionaries, and
specialty arenas cannot read across named graphs.

**Caching.** Decoded leaflet regions and dictionary leaves share one
frequency-aware memory budget
([`LeafletCache`](../../fluree-db-binary-index/src/read/leaflet_cache.rs)), keyed
by content-addressed leaf identity, the query's effective `t`, and the overlay
epoch. Remote blobs are fetched through a single-flight disk cache
([`disk_cache.rs`](../../fluree-db-core/src/disk_cache.rs)), so concurrent
requests for the same blob share one fetch.

## Layer 3: The planner

The entry point is `reorder_patterns` in
[`planner.rs`](../../fluree-db-query/src/planner.rs), called from
`build_where_operators_seeded` in
[`execute/where_plan.rs`](../../fluree-db-query/src/execute/where_plan.rs).

### Placement algorithm

Placement is **greedy rather than dynamic-programming**. Patterns are placed one
at a time, cheapest eligible first, in three priority tiers:

1. **Reducers** first (lowest multiplier): FILTER and MINUS, to shrink the stream early
2. **Sources** next (lowest estimate): triples, searches, subqueries
3. **Expanders** last (lowest multiplier): OPTIONAL and UNION, to defer row growth

Ties are broken by the pattern's original position, so planning is deterministic.

Greedy placement keeps planning cost small relative to execution, even for large
WHERE clauses. Plan quality therefore depends primarily on the estimator rather
than on search.

### Estimation

Selectivity estimates come from HLL-derived per-predicate statistics
(`StatsView` / `PropertyStatData`): predicate row counts, distinct-subject
counts, distinct-value counts, and per-class counts for `rdf:type`. When
statistics are unavailable, the planner uses tiered heuristic constants rather
than a single default.

Patterns are classified **with respect to variables bound by earlier
placements**. `classify_pattern` treats a variable bound upstream as bound, so
`?s <p> ?o` is re-ranked as a bound-subject probe once `?s` is produced rather
than being scored as a full property scan.

Because the planner does not search alternative orders, a large estimation error
places a pattern in the wrong position with no later correction. The estimator
therefore handles specific shapes where generic RDF cardinality estimates are
known to be inaccurate:

- **Anchored transitive paths.** `<s> <p>+ ?o` enumerates a bounded closure from
  a fixed node rather than scanning the graph. It is estimated as small, so it
  drives the join instead of being costed as a join product.
- **Anchored `DISTINCT` subquery producers.** A subquery such as
  `MATCH (p {id: $x})-[:KNOWS*1..2]-(f) WITH DISTINCT f` is estimated at its
  projected distinct output rather than its body's join product, which can be
  larger by several orders of magnitude.
- **Branches containing only compound patterns.** A chained
  `{A} UNION {B} UNION {C}` nests one UNION inside a branch of another, so that
  branch contains no triples of its own. It takes its member's estimate instead
  of the default for an unknown scan. Because `UnionOperator` is correlated and
  re-runs once per input row, an overestimate that places a UNION behind a large
  driver has a disproportionate cost.

Correcting the estimate, rather than adding an operator, improves every query
that contains the shape, regardless of the surrounding patterns.

### Pre-planning rewrites

Several rewrites run before `reorder_patterns`, so that ordering and the scan
layer both operate on a narrower form. Each is a semantic identity under a
stated condition:

- **Redundant `rdf:type` elision.** `?s rdf:type <C>` is removed when statistics
  show that every subject of a co-occurring predicate is a `C`
  (`elide_redundant_type_filters`).
- **Single-row VALUES object folding.** `VALUES ?o { <iri> }` is equivalent to the
  constant `<iri>`, so `?s <p> ?o` in the same inner-join region is rewritten to
  `?s <p> <iri>` and the scan can seek on it (`inline_singleton_values_objects`).
  The VALUES pattern is retained, so the variable remains bound for projection
  and FILTERs. Folding does not cross compound patterns, where MINUS/EXISTS
  semantics could change, and literals are left to the scan layer's datatype
  matching.
- **Selective VALUES seeds for stars.** When predicate statistics predict that a
  small, fully bound object `VALUES` table substantially reduces the work of a
  same-subject star, the star is driven from that table instead of from a scan
  of its smallest predicate. The ordinary `ValuesOperator` joins are retained, so
  duplicates, `UNDEF`, and multi-column correlations keep their meaning. The
  thresholds are defined in `where_plan.rs`.
- **Algebraic aggregate rewrites.**
  [`aggregate_complement_fold`](../../fluree-db-query/src/aggregate_complement_fold.rs)
  relies on `SUM` and `COUNT` distributing over set difference. An average over
  entities that lack a key, written as a universe filtered by
  `FILTER NOT EXISTS`, is computed as a universe total minus a per-key positive
  join, without a cross product. When a sibling sub-SELECT already computes the
  matching per-key aggregate, both share one grouped scan. Recognition is narrow;
  see [Limits](#limits-and-deliberate-trade-offs).

### Coupled cost constants

Estimator constants are not independent. `DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY`
also seeds the driving-side estimate for a downstream hash join, so it must stay
large enough that `probe_count / driving_est` clears `HASH_JOIN_MAX_SCAN_RATIO`.
Otherwise the planner would choose an order that relies on a hash join the
hash-join gate then rejects. The test
`hash_join::tests::producer_seed_clears_scan_ratio_cap` asserts this relationship
and fails if either constant changes in a way that breaks it.

Relationships of this kind are maintained as tested invariants, so the cost
model can be changed without silently invalidating decisions elsewhere.

### Inspecting plans

Planner decisions are reported by [explain plans](../query/explain.md): the index
permutation chosen for each scan, whether statistics or fallbacks were used,
estimated row counts per node, hash-join selection and the reason an alternative
was rejected, and whether patterns were reordered. EXPLAIN applies the same
policy gate as execution, so it reports the plan that runs. To compare with the
plan an optimization replaced, use the
[environment variables that disable optimizer rewrites](../troubleshooting/debugging-queries.md#isolating-an-optimizer-change).

## Layer 4: Join operators

The default is `NestedLoopJoinOperator`, which probes the right side in batches
of distinct left-side keys. The planner selects a specialized operator when the
shape warrants it.

**`HashJoinOperator`** ([`hash_join.rs`](../../fluree-db-query/src/hash_join.rs))
addresses object-to-subject joins between a small selective side and a large
predicate scan. Driving from the selective side turns the large pattern into a
right scan with a bound object, which the nested-loop path resolves by seeking
the object-major OPST index once per distinct driving object. One predicate's
triples are distributed across the entire OPST keyspace, so this degrades
superlinearly: approximately 47 s at 100 M triples for approximately 61.8 K
driving objects. The hash join builds from the small side and probes with a
single scan of the large predicate's contiguous PSOT/POST partition, which takes
approximately 75 ms at 100 M triples.

**`PropertyJoinOperator`** evaluates same-subject multi-predicate stars anchored
by a bound object or a range filter (`?s a :Person ; :name ?n ; :email ?e`) as a
single operator rather than a join chain. One scan of the anchor produces the
subjects, and the remaining predicates are looked up for those subjects only,
by batched PSOT probes or a single SPOT walk.

**`SemijoinOperator`** evaluates `EXISTS` / `NOT EXISTS` with a single
uncorrelated build followed by hash probes, instead of evaluating a correlated
subquery per row. For inner bodies consisting only of triple patterns, outer
rows whose key variables are partly unbound, typically after an `OPTIONAL`, can
probe a lazily built projection of the inner keys onto the bound variables.
Compound inner patterns and poisoned bindings retain per-row evaluation.
Surviving outer rows retain their multiplicities; the keys are used only to
answer the existence test. Each execution caches at most four projection shapes,
with further shapes using per-row evaluation. Both the base and projected key
sets contribute to the query's retained-memory estimate and budget checks.

**`CyclicBgpOperator`** handles small cyclic fixed-predicate BGPs (triangles and
4-edge cycles over reference-valued joins) that would otherwise run as left-deep
nested loops. It is intentionally narrower than a general leapfrog triejoin;
unsupported cyclic shapes use the generic operator tree.

### Limiting work to what the consumer needs

Operators receive information about how their output will be consumed and avoid
work that cannot affect the result. These mechanisms change how much is
produced before the consumer is satisfied, not which rows the query returns.

**Row goals.** For a join block with no sort or aggregation, a small
`LIMIT + OFFSET` is treated as a hint about startup cost. The planner prefers a
streaming nested-loop join to building a large hash table first (EXPLAIN reports
the rejected hash join as `small-row-goal`), and probe windows start small and
grow geometrically. `PropertyJoinOperator` similarly reads a bound-object anchor
in growing chunks and emits each chunk's rows before reading the next, so an
outer `LIMIT` can stop it early. The hint affects operator choice and batch size
but never caps the number of rows read, so it can be passed through `DISTINCT`
and `FILTER`: if those operators consume a window, the join continues reading.
A query without `ORDER BY` may return a different, equally valid set of rows
when the join choice changes.

Startup cost matters less when the query must consume its entire input. If
statistics estimate that a `DISTINCT` projection has fewer rows than
`LIMIT + OFFSET`, the planner retains the throughput cost model. Unknown
estimates, including computed projections, keep the startup hint. Sparse filters
can still require a full drain, so probe windows grow to amortize repeated work.

**Count-only consumption.** An ungrouped `COUNT(*)` requests a count from its
input through `drain_count` instead of consuming rows. The nested-loop join
supports this on its batched subject-probe path: it counts matches after the
ordinary scan and inline filters, preserving left-row multiplicity, without
building output batches. It uses the same visibility, overlay, and history
handling as the normal scan, and shapes it cannot count directly fall back to
row execution. When every aggregate is `COUNT(*)` and all group keys come from
the driving side of a batched subject join, a grouped drain counts matches per
input row and folds those counts into normalized group keys. This avoids the
join's output expansion and hashes each surviving input row once, regardless of
its number of matches. Retained groups contribute to the query's memory estimate;
keys introduced on the right, joined-row `BIND`, and mixed aggregates retain row
consumption. The fast paths and count planner (Layer 5) apply the same idea more
broadly.

**Duplicate control.** Deep existential chains
(`?a p1 ?b . ?b p2 ?c . ?c p3 ?x`) accumulate duplicate rows: once `?a` is no
longer needed, each distinct `?b` repeats once per `?a`, and each hop multiplies
the duplication. After computing live-variable sets, the planner inserts
streaming `DistinctOperator`s between joins, but only when every aggregate is
duplicate-insensitive or the query is `SELECT DISTINCT`. Conversely, a
`DISTINCT` directly beneath a group whose aggregates are all `COUNT(DISTINCT …)`
is removed, because the aggregate already deduplicates its input.

## Layer 5: Fast-path operators

A set of operators recognizes specific query shapes and answers them by fusing
scan and aggregation, bypassing the generic operator tree.

Each is built as a `FastPathOperator` that **retains the generic tree as a
fallback** and returns `Ok(None)` from its `open()`-time closure when its runtime
preconditions do not hold. A declined fast path costs one precondition check.
Decisions are emitted as structured tracing events
([`fast_path_outcome.rs`](../../fluree-db-query/src/fast_path_outcome.rs)), so
planned and executed paths can be compared without a lock on the hot path.

### Directory-only aggregates: `O(leaflets)`, not `O(rows)`

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_min_max_string`](../../fluree-db-query/src/fast_min_max_string.rs) | `MIN(?o)` / `MAX(?o)` | POST leaflet boundary keys are the extremes when the leaflet is `o_type`-homogeneous; only leaflets spanning an `o_type` boundary are column-scanned |
| [`fast_group_count_firsts`](../../fluree-db-query/src/fast_group_count_firsts.rs) | `GROUP BY ?o COUNT(?s) ORDER BY DESC LIMIT k`, and `COUNT` of `?s <p> <o>` | Uncompressed per-leaflet FIRST headers: `FIRST(i)==FIRST(i+1)` shows the leaflet is one `(p,o)` group, which is counted without decoding. Single-datatype predicates skip the `OType` column |
| [`fast_whole_graph_agg`](../../fluree-db-query/src/fast_whole_graph_agg.rs) | Cypher `MATCH (n) RETURN count(n), count(n.age), …` | Replaces the whole-graph distinct-subject scan with directory reads: `count(*) = N + count(P) − subj(P)`, each term computed from the directory |

### Order-exploiting scans

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_post_order_limit`](../../fluree-db-query/src/fast_post_order_limit.rs) | `ORDER BY DESC(?o) LIMIT k`, optionally with `?s a <Class>` | POST is ordered `(p_id, o_type, o_key, o_i, s_id)`, so for an order-preserving `o_type` the end of the predicate range holds the top-k. Leaves are read backward, only surviving rows are decoded, and reading stops at `OFFSET+LIMIT`. Separate base and overlay-merging variants |
| [`fast_string_fold`](../../fluree-db-query/src/fast_string_fold.rs) | `COUNT(*)` with `REGEX`/`CONTAINS`; `SUM(STRLEN(?o))` and variants | POST places equal strings adjacently, so the function is evaluated once per distinct value (`O(distinct)` rather than `O(rows)`), reading the dictionary in ascending ID order |
| [`fast_string_prefix_count_all`](../../fluree-db-query/src/fast_string_prefix_count_all.rs) | `COUNT(*)` with `REGEX(?o,"^pfx")` / `STRSTARTS` | String IDs are assigned in lexical order, so a prefix maps to contiguous dictionary ID ranges and bounded OPST slices instead of a full partition scan |
| [`fast_star_const_order_topk`](../../fluree-db-query/src/fast_star_const_order_topk.rs) | Constant-object star + numeric filter + label `ORDER BY … LIMIT` | Intersects OPST subject lists for each constant constraint, applies the numeric filter to those subjects only, and fetches labels for the remaining rows |

### Fused aggregates

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_count`](../../fluree-db-query/src/fast_count.rs) | `COUNT` family | Consolidated count lanes |
| [`fast_predicate_scalar_agg`](../../fluree-db-query/src/fast_predicate_scalar_agg.rs) | `SUM` / `AVG` / `COUNT(DISTINCT ?o)` | Folded from encoded `(o_type, o_key)` without materializing per-row bindings |
| [`fast_exists_join_count_distinct_object`](../../fluree-db-query/src/fast_exists_join_count_distinct_object.rs) | `COUNT(DISTINCT ?o)` with an existence-only same-subject join | Builds a subject set from PSOT (SId column only) and streams sorted `(o_key, s_id)` from POST; no values are decoded |
| [`fast_union_star_count_all`](../../fluree-db-query/src/fast_union_star_count_all.rs) | `COUNT(*)` over a UNION of triples with same-subject star constraints | Computed from per-subject multiplicity streams without materializing the union |
| [`fast_sum_strlen_group_concat`](../../fluree-db-query/src/fast_sum_strlen_group_concat.rs) | `SUM(STRLEN(GROUP_CONCAT(…)))` | Reduces algebraically to `Σ strlen(o) + (N_rows − N_subjects)·strlen(sep)`, so group strings are not built |
| [`fast_path_plus_count_all`](../../fluree-db-query/src/fast_path_plus_count_all.rs) | `COUNT(*)` over `+` property paths with a fixed endpoint | Builds adjacency once and counts reachable nodes without repeated range scans |
| [`fast_label_regex_type`](../../fluree-db-query/src/fast_label_regex_type.rs) | Label scan + regex + `rdf:type` check | Scans the smaller label predicate and checks type only for regex matches, instead of a per-subject lookup for every member of a large class |
| [`fast_vector_topk`](../../fluree-db-query/src/fast_vector_topk.rs) | Vector similarity `ORDER BY DESC(score) LIMIT k` | Scores the packed f32 arena directly with the same SIMD kernel as the general evaluation path, so results are bit-identical; parallelized across subject-range partitions |

### The count planner

[`count_plan.rs`](../../fluree-db-query/src/count_plan.rs) and
[`count_plan_exec.rs`](../../fluree-db-query/src/count_plan_exec.rs) generalize
the per-shape `detect_*`/`fast_*` pairs into a single planner that analyzes the
WHERE join graph and composes a count-only plan. Its IR enforces **key domain
safety** (subject vs. object keys) and **output kind safety** (scalar vs. stream
vs. key set) in the type system, so invalid compositions, such as anti-joining a
subject stream against an object key set, fail to compile rather than producing
incorrect results.

New count optimizations are added to this planner in preference to new
per-shape detectors.

## Layer 6: Graph traversal

[`frontier.rs`](../../fluree-db-query/src/frontier.rs) provides the shared
raw-ID expansion path used by property paths and shortest path.

Expanding a BFS level node by node costs one index descent, a full `Flake`
materialization, and a dictionary-backed `Sid` **per neighbor, per node**. The
frontier path instead keys frontier nodes by persisted `s_id` (`u64`) and expands
each level with a small number of galloping batched-lookup sweeps, taking
neighbors as raw `o_key` IDs. For `IRI_REF` rows, `o_key` is the target's `s_id`,
so the expansion loop performs **no dictionary lookups**.

Overlay correctness is handled per node. `overlay_dirty_ids` records which
persisted subjects the overlay touches, by side: as a subject, its out-edges are
incomplete; as a reference object, its in-edges are incomplete; retractions mark
both. Only those nodes, plus subjects that exist only in novelty, use the slower
`Sid`-space path that merges novelty. The summary is cached in an LRU keyed by
overlay content version and store instance ID. If the overlay cannot be
summarized, the raw-ID path is not used for that query.

[`shortest_path.rs`](../../fluree-db-query/src/shortest_path.rs) runs
**bidirectional** BFS for `shortestPath`, alternating between two frontiers and
expanding the smaller one. This explores `O(b^(d/2))` nodes instead of `O(b^d)`,
which is significant on high-fanout graphs such as social networks.

## Layer 7: Parallelism

Parallelism is used where the work is large enough to benefit from it.

**Query side.** A shared, process-wide rayon pool (sized once to approximately
the number of logical cores) is used through `parallel_map_pooled`, which
preserves order so that results are deterministic. It is used by:

- partitioned base scans in fast-path folds ([`fast_path_common.rs`](../../fluree-db-query/src/fast_path_common.rs))
- the count planner's range partitions (`count_plan_exec.rs`)
- vector top-k subject-range partitions (`fast_vector_topk.rs`)
- cyclic BGP edge loading (`cyclic_bgp.rs`)

A single shared pool avoids oversubscribing cores under concurrent load, which
per-query pools would cause. Partial results are combined in chunk order, so a
parallel aggregate is bit-identical to its serial equivalent.

**Write and index side.** Dictionary building, leaf rebuilds, incremental branch
merges, and spatial index construction are parallelized
([`fluree-db-indexer`](../../fluree-db-indexer/)). Bulk import exceeds 2 M
facts per second.

**Not parallelized:** the general operator tree. A query that does not use a
fast path runs its scan/join pipeline on one core. See
[Limits](#limits-and-deliberate-trade-offs).

## Layer 8: The write path

Commits are written to an in-memory **novelty** overlay and are durable
immediately; they do not wait for index maintenance. Background indexing is
threshold-driven (`reindex-min-bytes` as a soft trigger, `reindex-max-bytes` for
backpressure). It resolves only the commits in the novelty window and merges them
into the affected leaf blobs **copy-on-write**, leaving the rest of the index
unchanged, and publishes the new root atomically. See
[Background indexing](../indexing-and-search/background-indexing.md).

Because unchanged blobs are reused by address, a reindex rewrites only what
changed and existing cache entries remain valid.

Queries merge the indexed base with novelty at scan time (Layer 2), so results
are complete regardless of indexing lag.

---

## Limits and deliberate trade-offs

**Four permutations, not six.** `SPOT`/`PSOT`/`POST`/`OPST` cover seven of the
eight triple-pattern shapes with a contiguous range scan. The exception is
`(s, ?p, o)`, with subject and object bound and predicate free. Engines that
maintain all six permutations serve it directly; Fluree resolves it as a bounded
SPOT scan on the subject with an object filter. Because a bound subject already
restricts the scan to one subject's rows, the residual filter is inexpensive.
The benefit is lower index build time and storage, with two fewer permutations
to write on every reindex.

**Undatatyped plain strings do not use the OPST preference.** A constant object
that is a plain string with no datatype constraint may be either `xsd:string` or
`rdf:langString`, so `(o_type, o_key)` may not be encodable when the scan opens.
`BinaryScanOperator` does not force OPST in that case, because doing so could
produce a wide scan rather than a bounded one. Specifying a datatype enables the
object-leading path.

**General operator trees are single-threaded.** Intra-query parallelism is
limited to the fast paths and the count planner. A complex analytical join that
does not use a fast path runs on one core. This is the largest remaining
opportunity for improvement in the engine, and the area where engines with
general intra-query parallelism can outperform Fluree on specific workloads.
Concurrent queries use all cores.

**`DistinctOperator` does not spill to disk.** It holds an unbounded in-memory
hash set of distinct rows
([`distinct.rs`](../../fluree-db-query/src/distinct.rs)), so a query that produces
a very large distinct set is bounded by available memory. This is one reason
automatic distinct injection (Layer 4) is gated: it trades memory for speed only
where correctness permits, and loosening the gate could make an aggregate query
that previously streamed memory-bound with no benefit.

**Planning is greedy.** There is no dynamic-programming join enumeration. On
very large WHERE clauses, a plan prefix chosen early is not revisited. In
exchange, planning time stays small and plans are deterministic. Because plan
quality depends on the estimator's handling of specific shapes, a shape the
estimator does not model may be ordered poorly. `EXPLAIN` shows the resulting
order.

**Shape-specific rewrites are narrow.** The algebraic aggregate rewrites and the
fast paths recognize particular query forms. A query that computes the same
result in another form, for example with an extra join, `GROUP BY` in place of
`DISTINCT`, or a different aggregate, uses the ordinary plan. No diagnostic
explains the difference beyond the plan shown by EXPLAIN. Admission is limited
to forms whose correctness has been established; the general mechanisms in
Layers 1–4 determine the performance of the ordinary plan.

**Large novelty degrades queries.** Late materialization requires the persisted
index to be authoritative, so scans over a ledger with unindexed novelty decode
values eagerly, and every scan performs the overlay merge. Below approximately
10 unindexed transactions the overhead is negligible; above approximately 100 it
is measurable in both latency and memory. Monitor `commit_t − index_t`; a lag
above approximately 50 indicates that indexing is not keeping up and
`reindex-min-bytes` should be lowered.

**Unanchored closures are computed but not optimized.** A property path with
both endpoints unbound (`?s <p>+ ?o`) materializes the predicate's full
transitive closure in memory, and composite paths (`?s (<a>/<b>)+ ?o`) run a
separate traversal from every possible start node. Paths with one endpoint bound
use the frontier path (Layer 6). Bind an endpoint where the query allows it.

**Fast paths have preconditions.** Most require single-ledger execution, no
`from_t`, root or no policy, and `to_t` at or after the persisted index point.
Time travel to a point before the index requires the history sidecar and uses
the generic pipeline, as do policy-enforced queries. The generic pipeline
returns the same results, without the fast-path speedup.

## Reproducing the benchmarks

The materials needed to run the comparisons are in
[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db):

- pinned datasets at `s3://fluree-benchmark-data/`
- per-engine setup guides in `common/engine-setup/`
- a generic SPARQL runner, `common/run_benchmark.sh`
- full per-engine results and run metadata in `benchmarks/*/reports/`

Competitor configurations are included so that their tuning can be reviewed.

## Where this lives in code

| Concern | Crate / file |
|---|---|
| Index permutations, comparators | [`fluree-db-core/src/comparator.rs`](../../fluree-db-core/src/comparator.rs) |
| Binary wire formats, cursors, decode, leaflet cache | [`fluree-db-binary-index`](../../fluree-db-binary-index/) |
| Planner, estimation, reordering | [`fluree-db-query/src/planner.rs`](../../fluree-db-query/src/planner.rs) |
| WHERE planning, operator tree build | [`fluree-db-query/src/execute/where_plan.rs`](../../fluree-db-query/src/execute/where_plan.rs) |
| Join operators | `join.rs`, `hash_join.rs`, `property_join.rs`, `semijoin.rs`, `cyclic_bgp.rs` |
| Aggregate rewrites | `aggregate_complement_fold.rs` |
| Fast paths | `fluree-db-query/src/fast_*.rs` |
| Count planner | `count_plan.rs`, `count_plan_exec.rs` |
| Traversal | `frontier.rs`, `property_path.rs`, `shortest_path.rs` |
| Background indexing | [`fluree-db-indexer`](../../fluree-db-indexer/) |
| Novelty overlay | [`fluree-db-novelty`](../../fluree-db-novelty/) |
| Operator timing probe | [`fluree-db-api/examples/query_operator_probe.rs`](../../fluree-db-api/examples/query_operator_probe.rs) |

## Related documentation

- [Index format](index-format.md): the binary wire format in detail
- [Query execution and overlay merge](query-execution.md): pipeline and overlay semantics
- [Explain plans](../query/explain.md): inspecting planner decisions
- [Debugging queries](../troubleshooting/debugging-queries.md): including environment variables that disable optimizer rewrites
- [Background indexing](../indexing-and-search/background-indexing.md): novelty and reindex thresholds
- [Hardware sizing: CPU vs disk](../operations/hardware-benchmarks.md): provisioning guidance
- [Performance investigation with distributed tracing](../troubleshooting/performance-tracing.md): diagnosing a slow query
- [BENCHMARKING.md](../../BENCHMARKING.md): internal regression-gating benchmarks
