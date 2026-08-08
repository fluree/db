# Performance architecture

Fluree is a temporal, verifiable graph database with triple-level access control,
reasoning, and integrated search. Systems with that feature surface are usually
assumed to be slow — capability traded for speed.

Fluree is faster than the specialist engines that have none of it.

This document explains why, layer by layer, with links into the code. It also
states plainly what Fluree does *not* do and where the current limits are — see
[Limits and deliberate trade-offs](#limits-and-deliberate-trade-offs).

## Measured results

Head-to-head benchmarks against other engines — same hardware, same datasets,
reproducible from pinned S3 snapshots — live in a separate repository:
**[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db)**.

**SPARQLoscope on DBLP-core** (561 M triples, 105 queries, m7a.4xlarge 16c/64 GB):

| | Fluree | QLever | Virtuoso | MillenniumDB | Jena | Oxigraph | Blazegraph |
|---|---|---|---|---|---|---|---|
| Queries passed | 105/105 | 105/105 | 103/105 | 103/105 | 34/105 | 39/105 | 3/105 |
| Geo mean | **17.5 ms** | 202.4 ms (11.5×) | 299.7 ms (17.1×) | 1,664 ms (95×) | 67.7 s | 87.0 s | 332.9 s |
| Median (passed) | **26.6 ms** | 310.3 ms (11.7×) | 326.0 ms (12.3×) | 3,894 ms (147×) | 4.5 s | 5.1 s | 23.2 s |

**Wikidata-truthy** (8.19 B triples, r7a.16xlarge 64c/512 GB): Fluree geo mean
367.4 ms — next engine (QLever) 10.4× slower.

**WGPB** (full 21.5 B-triple Wikidata dump, 850 basic graph pattern queries,
r7a.8xlarge 32c/256 GB): 850/850 passed, 43 ms geometric mean.

**Pokec / openCypher** (Memgraph's benchgraph suite, 1.6 M nodes / 30.6 M edges,
r8a.4xlarge 16c/128 GB):

| | Fluree | Memgraph | Neo4j | FalkorDB |
|---|---|---|---|---|
| Durable writes | **1.73 ms** | 4.46 ms | 4.07 ms | 4.57 ms |
| Read-only | **1.47 ms** | 4.41 ms | 6.80 ms | 4.57 ms |

Note that Fluree's write number is a *durable* write — committed and
recoverable — compared against engines whose defaults are weaker.

State the failure model when quoting these: durability here means the write
survives the process dying. Surviving power loss additionally requires the
storage backend to flush, which the file backend does by default
(`FLUREE_STORAGE_FSYNC`, see [Configuration](../operations/configuration.md))
and S3 does by acknowledging after replication. This table predates that
default and was measured without the flush, so it is not a like-for-like
comparison against an engine that fsyncs per commit; it needs a re-run before
it is quoted again.

> Internal criterion benchmarks that gate per-PR regressions are a different
> thing entirely and are documented in [BENCHMARKING.md](../../BENCHMARKING.md).
> They protect against drift; they do not measure competitors.

## The short version

Seven things account for most of it:

1. **Integer-ID execution.** Dictionary encoding means joins compare `u64`s, not
   IRIs or strings. Whole query shapes never touch a dictionary.
2. **Per-column compressed blocks.** Queries decompress only the columns they
   actually filter on or project — often none at all.
3. **Directory-only answers.** Leaflet headers carry enough metadata that many
   aggregates are answered from the index directory in `O(leaflets)` rather than
   `O(rows)`.
4. **A cost model with real statistics.** HLL-derived per-predicate stats drive
   selectivity estimates, and the cost constants are coupled and regression-tested
   against the operators that consume them.
5. **Physical operators for the shapes that matter.** Hash join, property join,
   semijoin, cyclic BGP — each replacing a nested-loop pattern that degrades.
6. **Sixteen fast-path operators** that fuse scan and aggregate, each with a
   runtime precondition check and a fallback to the generic tree.
7. **Writes never wait on indexing.** Commits land in an in-memory overlay;
   indexing is background, copy-on-write, and threshold-driven.

The rest of this document takes them in order, from storage upward.

---

## Layer 1: Storage and encoding

See [Index format](index-format.md) for the wire-format detail. What matters for
performance:

**Four covering permutations** — `SPOT`, `PSOT`, `POST`, `OPST`
([`fluree-db-core/src/comparator.rs`](../../fluree-db-core/src/comparator.rs)).
Every triple pattern shape resolves to a contiguous range scan on one of them.

`OPST` holds **all object types**, not just references. Because it leads with the
object, its leaflets are segmented by `o_type`, so IRI refs form one contiguous
partition and each literal type forms its own. That is what makes both reverse
traversal (`?s ?p <iri>` — pin `o_type = IRI_REF`) and bound-literal scans cheap:
`fast_string_prefix_count_all` answers `FILTER(STRSTARTS(?o,"Com"))` by scanning
an OPST slice bounded by the string dictionary ID range for that prefix.
`BinaryScanOperator` accordingly prefers OPST for *any* constant object with an
unbound subject, excluding only undatatyped plain strings — those are ambiguous
between `xsd:string` and `rdf:langString`, so `(o_type, o_key)` may not be
encodable at `open()` and OPST would devolve into a wide scan.

**Everything is a numeric ID.** Subjects, predicates, graphs, datatypes,
languages, and string literals all live in dictionaries; the index stores
`u64`/`u32` keys. Joins, grouping, and dedup happen in integer space. A query
that never projects a value never decodes one.

**Order-preserving encodings.** Numeric, temporal, and boolean objects are
encoded so that `o_key` byte order *is* value order. That single property is
what makes `ORDER BY DESC(?o) LIMIT k`, `MIN`, and `MAX` answerable without
scanning (Layer 5).

**Independently compressed per-column blocks.** A V3 leaflet stores one zstd
block per column (`SId`, `PId`, `OType`, `OKey`, `OI`, `T`), each with its own
`ColumnBlockRef`. A query decodes only the columns it filters on or projects —
scanning by key never pays to decode `T` or `OI`.

Columns that are constant for a leaflet are hoisted out of the block set
entirely: POST/PSOT leaflets are predicate-homogeneous so `p_id` becomes
`p_const`, and OPST leaflets are type-homogeneous by segmentation so `o_type`
becomes `o_type_const` (other orders hoist it too when single-typed). Element
width per column narrows to the smallest type that fits the dictionary
cardinality.

**History lives outside the leaflet.** Time-travel data is a separate
content-addressed object — the per-leaf **history sidecar** (`FHS1`), located via
`LeafEntry.sidecar_cid` on the branch manifest, holding per-leaflet segments of
31-byte `HistEntryV2` transition records sorted newest-first. A HEAD-only query
never fetches, decompresses, or caches a single history byte; the leaflet cache
deliberately excludes sidecar data as cold-path. This is why time travel costs
nothing when you aren't using it.

**Leaflet directories.** Each leaf's uncompressed header carries a
`LeafletDirEntryV3` per leaflet: `row_count`, `lead_group_count`, 26-byte
`first_key` / `last_key` routing keys, the hoisted `p_const` / `o_type_const`,
and the per-column block refs. This is the single highest-leverage layout
decision in the format — when `first_key(i) == first_key(i+1)` in POST order, the
entire leaflet `i` is provably one `(p, o)` group, so it can be counted without
decompressing anything.

The same directory entry also carries the leaflet's history locator
(`history_offset`, `history_len`, `history_min_t`, `history_max_t`) — an offset
range *into the sidecar blob*, never inline bytes. The `min_t`/`max_t` pair lets a
time-travel query skip a leaflet's history segment entirely without reading it.

**Content addressing.** Leaves, branches, and dictionary blobs are addressed by
SHA-256 (local) or CIDv1 (remote). Caches never need invalidation, because an
address uniquely identifies content.

## Layer 2: Scan and decode

`BinaryCursor` yields `ColumnBatch` — leaflet-at-a-time columnar batches — and a
`ColumnProjection` / `ColumnSet` declares which columns the consumer actually
needs, so unrequested columns are never decoded
([`binary_scan.rs`](../../fluree-db-query/src/binary_scan.rs)).

Overlay (novelty) merging happens *inside* the cursor: base rows retracted by the
overlay are skipped, overlay asserts are injected, and `to_t` is honored, all
before the operator above sees a row. Correctness under uncommitted writes and
time travel is therefore a property of the scan layer, not something every
operator has to re-implement.

Graph scoping is enforced at the same boundary — `BinaryGraphView` is a
graph-scoped decode handle, so leaflet decoding, predicate dictionaries, and
specialty arenas cannot leak across named graphs.

## Layer 3: The planner

Entry point: `reorder_patterns` in
[`planner.rs`](../../fluree-db-query/src/planner.rs) (~4.1k lines), called from
`build_where_operators_seeded` in
[`execute/where_plan.rs`](../../fluree-db-query/src/execute/where_plan.rs)
(~4.6k lines).

### Placement algorithm

Placement is **greedy, not dynamic-programming** — patterns are placed one at a
time, cheapest eligible first, in three priority tiers:

1. **Reducers** first (lowest multiplier) — FILTER, MINUS: shrink the stream ASAP
2. **Sources** next (lowest estimate) — triples, searches, subqueries
3. **Expanders** last (lowest multiplier) — OPTIONAL, UNION: defer row growth

Ties break on the pattern's original index, so planning is deterministic and a
query's plan doesn't drift between runs.

Greedy placement is a deliberate choice: planning cost stays negligible relative
to execution even on large WHERE clauses. The accuracy comes from the estimator,
not from search.

### Estimation

Selectivity estimates come from HLL-derived per-predicate statistics
(`StatsView` / `PropertyStatData`): predicate row counts, distinct-subject
counts, distinct-value counts, and per-class counts for `rdf:type`. When
statistics are unavailable the planner falls back to tiered heuristic constants
rather than a single default.

Patterns are classified **with respect to variables already bound by earlier
placements** — `classify_pattern` treats a variable bound upstream as bound, so
`?s <p> ?o` correctly re-ranks as a bound-subject probe once `?s` is produced,
instead of being scored as a full property scan forever.

Beyond the generic path, the estimator carries targeted knowledge of shapes that
generic RDF cardinality math gets badly wrong:

- **Anchored transitive paths.** `<s> <p>+ ?o` enumerates a bounded closure from
  a fixed node, not a world scan. Estimating it as a join product pushes it
  behind unrelated predicate scans; it is instead estimated small so it drives
  the join.
- **Anchored `DISTINCT` subquery producers.** A subquery like
  `MATCH (p {id: $x})-[:KNOWS*1..2]-(f) WITH DISTINCT f` emits its projected
  distinct rows, not its body's join product — the product overestimates by
  ~792 M on a 2-hop `KNOWS`.

### Cost constants are coupled and tested

Estimator constants are not free parameters. `DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY`
also seeds the driving-side estimate for a downstream hash join, so it has to
stay large enough that `probe_count / driving_est` clears
`HASH_JOIN_MAX_SCAN_RATIO` — otherwise the ordering unlocks a join that the
hash-join gate then rejects. That coupling is asserted by
`hash_join::tests::producer_seed_clears_scan_ratio_cap`, which fails if either
constant drifts.

This is the part of the planner that is hardest to see from outside and matters
most: the cost model is maintained as a system with tested invariants, not a bag
of tuned magic numbers.

### Inspecting plans

Every planner decision is visible via [explain plans](../query/explain.md) —
chosen index permutation per scan, whether statistics or fallbacks were used,
estimated row counts per node, hash-join selection and its reasoning, and
whether patterns were reordered.

## Layer 4: Join operators

The default is `NestedLoopJoinOperator`. The planner promotes to a specialized
operator when the shape warrants it.

**`HashJoinOperator`** ([`hash_join.rs`](../../fluree-db-query/src/hash_join.rs))
— the fix for "small selective side + large predicate scan" object→subject
joins. Driving from the selective side makes the large pattern a right scan with
a bound *object*, which the nested-loop path resolves by seeking the global
object-major OPST index once per distinct driving object. Since one predicate's
triples are scattered across the whole OPST keyspace, that degrades
superlinearly: ~47 s at 100 M triples for ~61.8 K driving objects. The hash join
builds from the small side and probes by scanning the large predicate's
*contiguous* PSOT/POST partition exactly once — that scan alone is ~75 ms at
100 M.

**`PropertyJoinOperator`** — fuses same-subject multi-predicate stars
(`?s :name ?n . ?s :age ?a . ?s :email ?e`) into per-predicate PSOT scans instead
of a join chain.

**`SemijoinOperator`** — turns `EXISTS` / `NOT EXISTS` from per-row correlated
subquery evaluation into a single uncorrelated build plus hash probes. Rows whose
key variables are unbound or poisoned fall back to per-row correlated evaluation,
preserving SPARQL substitution semantics.

**`CyclicBgpOperator`** — a targeted operator for small cyclic fixed-predicate
BGPs (triangles, 4-edge cycles over ref-valued joins) that otherwise fall through
to left-deep nested loops. Deliberately narrower than a general leapfrog
triejoin; unsupported cyclic shapes keep the generic tree.

**Streaming `DistinctOperator` injection** — deep existential chains
(`?a p1 ?b . ?b p2 ?c . ?c p3 ?x`) carry compounding duplicate multiplicity: once
`?a` is dead, every distinct `?b` repeats once per `?a`, and each hop multiplies
the redundancy. The planner inserts streaming distincts between joins after
computing live-variable sets. This is soundness-gated — only legal when every
aggregate is duplicate-insensitive or the query is `SELECT DISTINCT`.

## Layer 5: Fast-path operators

Sixteen operators recognize specific query shapes and answer them by fusing scan
and aggregate, bypassing the generic operator tree entirely.

The design contract matters as much as the operators: each is built as a
`FastPathOperator` that **captures the generic tree as a fallback** and returns
`Ok(None)` from its `open()`-time closure whenever its runtime preconditions
don't hold. A declined fast path costs one precondition check, not a cliff.
Decisions are emitted as structured tracing events
([`fast_path_outcome.rs`](../../fluree-db-query/src/fast_path_outcome.rs)) so
planned-vs-executed is observable without a lock on the hot path.

### Directory-only aggregates — `O(leaflets)`, not `O(rows)`

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_min_max_string`](../../fluree-db-query/src/fast_min_max_string.rs) | `MIN(?o)` / `MAX(?o)` | POST leaflet boundary keys are the extremes when the leaflet is `o_type`-homogeneous; only leaflets straddling an `o_type` boundary are column-scanned |
| [`fast_group_count_firsts`](../../fluree-db-query/src/fast_group_count_firsts.rs) | `GROUP BY ?o COUNT(?s) ORDER BY DESC LIMIT k`, and `COUNT` of `?s <p> <o>` | Uncompressed per-leaflet FIRST headers: `FIRST(i)==FIRST(i+1)` proves the whole leaflet is one `(p,o)` group, so it's counted without decoding. single-datatype predicates skip the `OType` column entirely |
| [`fast_whole_graph_agg`](../../fluree-db-query/src/fast_whole_graph_agg.rs) | Cypher `MATCH (n) RETURN count(n), count(n.age), …` | Rewrites the whole-graph distinct-subject scan into directory reads: `count(*) = N + count(P) − subj(P)`, all three terms directory-only |

### Order-exploiting scans

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_post_order_limit`](../../fluree-db-query/src/fast_post_order_limit.rs) | `ORDER BY DESC(?o) LIMIT k`, optionally `?s a <Class>` | POST is `(p_id, o_type, o_key, o_i, s_id)`, so for an order-preserving `o_type` the physical tail of the predicate range *is* the top-k. Walk leaves backward, decode only survivors, stop at `OFFSET+LIMIT`. Base lane and an overlay-merging lane |
| [`fast_string_fold`](../../fluree-db-query/src/fast_string_fold.rs) | `COUNT(*)` with `REGEX`/`CONTAINS`; `SUM(STRLEN(?o))` and variants | POST puts equal strings adjacent, so the function evaluates once per *distinct* value — `O(distinct)` instead of `O(rows)` — reading the dictionary in ascending ID order (sequential pack access) |
| [`fast_string_prefix_count_all`](../../fluree-db-query/src/fast_string_prefix_count_all.rs) | `COUNT(*)` with `REGEX(?o,"^pfx")` / `STRSTARTS` | On lex-sorted string IDs, a prefix maps to contiguous dictionary ID ranges → bounded OPST slices instead of a full partition scan |
| [`fast_star_const_order_topk`](../../fluree-db-query/src/fast_star_const_order_topk.rs) | Constant-object star + numeric filter + label `ORDER BY … LIMIT` | Intersect OPST subject lists per constant constraint, apply the numeric filter over just those subject ranges, fetch labels for survivors |

### Fused aggregates

| Operator | Shape |
|---|---|
| [`fast_count`](../../fluree-db-query/src/fast_count.rs) | consolidated `COUNT` family |
| [`fast_predicate_scalar_agg`](../../fluree-db-query/src/fast_predicate_scalar_agg.rs) | `SUM`/`AVG`/`COUNT(DISTINCT ?o)` folded from encoded `(o_type, o_key)` with no per-row binding materialization |
| [`fast_exists_join_count_distinct_object`](../../fluree-db-query/src/fast_exists_join_count_distinct_object.rs) | `COUNT(DISTINCT ?o)` with an existence-only same-subject join — builds a subject set from PSOT (SId column only), streams sorted `(o_key, s_id)` from POST, never decodes a value |
| [`fast_union_star_count_all`](../../fluree-db-query/src/fast_union_star_count_all.rs) | `COUNT(*)` over UNION-of-triples with same-subject star constraints, computed from per-subject multiplicity streams instead of materializing the union |
| [`fast_sum_strlen_group_concat`](../../fluree-db-query/src/fast_sum_strlen_group_concat.rs) | `SUM(STRLEN(GROUP_CONCAT(…)))` — the per-subject bookkeeping cancels algebraically to `Σ strlen(o) + (N_rows − N_subjects)·strlen(sep)`, so no group strings are ever built |
| [`fast_path_plus_count_all`](../../fluree-db-query/src/fast_path_plus_count_all.rs) | `COUNT(*)` over `+` property paths with a fixed endpoint — adjacency built once, reachability counted, no repeated range scans |
| [`fast_label_regex_type`](../../fluree-db-query/src/fast_label_regex_type.rs) | label scan + regex + `rdf:type` check — scans the small label predicate and checks type only for regex hits, instead of millions of per-subject lookups from a large class |
| [`fast_vector_topk`](../../fluree-db-query/src/fast_vector_topk.rs) | vector similarity `ORDER BY DESC(score) LIMIT k` — scores the packed f32 arena directly with the same SIMD kernel the eval path uses, so results are bit-identical; parallelized across subject-range partitions |

### The count planner

[`count_plan.rs`](../../fluree-db-query/src/count_plan.rs) +
[`count_plan_exec.rs`](../../fluree-db-query/src/count_plan_exec.rs) generalize
the per-shape `detect_*`/`fast_*` pairs into a single planner that analyzes the
WHERE join graph and composes a count-only plan. Its IR enforces **key domain
safety** (subject vs. object keys) and **output kind safety** (scalar vs. stream
vs. key set) at the type level, so invalid compositions like "anti-join a subject
stream against an object key set" are compile errors rather than wrong answers.

## Layer 6: Graph traversal

[`frontier.rs`](../../fluree-db-query/src/frontier.rs) is the shared raw-id
expansion lane behind property paths and shortest path.

BFS level expansion done node-by-node costs one index descent, a full `Flake`
materialization, and a dictionary-backed `Sid` **per neighbor, per node**. The
frontier lane instead keys frontier nodes by persisted `s_id` (`u64`) and expands
each level with a handful of galloping batched-lookup sweeps, taking neighbors as
raw `o_key` ids — for `IRI_REF` rows `o_key` *is* the target's `s_id`, so there
is **no dictionary in the loop**.

Overlay correctness is handled per-node rather than by giving up: `overlay_dirty_ids`
summarizes which persisted subjects the overlay touches, split by side (as
subject → out-edges incomplete; as ref-object → in-edges incomplete; retracts
stamp both). Only those nodes, plus novelty-only subjects, take the slower
`Sid`-space fallback that merges novelty. The summary is LRU-cached keyed on
overlay content version and store instance id. An overlay that can't be
summarized declines the raw-id lane entirely rather than risking a wrong answer.

On top of that, [`shortest_path.rs`](../../fluree-db-query/src/shortest_path.rs)
runs **bidirectional** BFS for `shortestPath` — two frontiers alternating on the
smaller side — exploring `O(b^(d/2))` instead of `O(b^d)`, which is decisive on
social-graph shapes.

## Layer 7: Parallelism

Parallelism is applied where it pays and skipped where it would cost more than
it returns.

**Query side.** A shared, process-wide rayon pool (sized once at ≈ logical cores)
is used via `parallel_map_pooled` — order-preserving, so results are
deterministic — by:

- partitioned base scans in fast-path folds ([`fast_path_common.rs`](../../fluree-db-query/src/fast_path_common.rs))
- the count planner's range partitions (`count_plan_exec.rs`)
- vector top-k subject-range partitions (`fast_vector_topk.rs`)
- cyclic BGP edge loading (`cyclic_bgp.rs`)

Sharing one pool matters: per-query pools would oversubscribe cores under
concurrent load. Partial results are folded in chunk order, so a parallel
aggregate is bit-identical to its serial equivalent.

**Write / index side.** Dictionary building, leaf rebuilds, incremental branch
merges, and spatial index construction all parallelize
([`fluree-db-indexer`](../../fluree-db-indexer/)). Bulk import exceeds 2 M
facts/second.

**Not parallelized:** the general operator tree. A single non-fast-path query
runs its scan/join pipeline on one core. See below.

## Layer 8: The write path

Commits land in an in-memory **novelty** overlay and are durable immediately;
they do not wait for index maintenance. Background indexing is threshold-driven
(`reindex-min-bytes` soft trigger, `reindex-max-bytes` backpressure), resolves
only the commits in the novelty window, and merges them into affected leaf blobs
**copy-on-write** — most of the index is untouched, and the new root is
published atomically. See [Background indexing](../indexing-and-search/background-indexing.md).

Because content addressing makes every unchanged blob reusable by address, a
reindex rewrites only what actually changed and every cache stays valid.

Queries merge indexed base with novelty at scan time (Layer 2), so reads are
always complete regardless of indexing lag.

---

## Limits and deliberate trade-offs

Every one of these is a real constraint, not a rough edge we're hiding. Knowing
where the walls are is how you evaluate whether the numbers above transfer to
your workload.

**Four permutations, not six.** `SPOT`/`PSOT`/`POST`/`OPST` cover seven of the
eight triple-pattern shapes with a contiguous range scan. The exception is
`(s, ?p, o)` — both subject and object bound, predicate free — which engines
keeping all six permutations serve directly and Fluree resolves as a bounded
SPOT scan on the subject with an object filter. Since a bound subject already
narrows to one subject's rows, the residual filter is cheap. The trade is index
build time and storage: two fewer permutations to write on every reindex.

**Undatatyped plain strings decline the OPST preference.** A constant object that
is a bare string with no datatype constraint is ambiguous between `xsd:string`
and `rdf:langString`, so `(o_type, o_key)` may not be encodable when the scan
opens. `BinaryScanOperator` therefore does not force OPST for that case —
forcing it would risk a wide scan rather than a bounded one. Supplying a
datatype gets the object-leading path.

**General operator trees are single-threaded.** Intra-query parallelism exists
only in the fast paths and the count planner. A complex non-fast-path analytical
join runs on one core. This is the clearest remaining headroom in the engine, and
it is the area where a multicore-parallel engine could contest specific
workloads. Concurrent *queries* use all cores.

**`DistinctOperator` does not spill.** It holds an unbounded in-memory hash set
of distinct rows
([`distinct.rs`](../../fluree-db-query/src/distinct.rs)). A query producing an
enormous distinct set is resident-memory-bound. Note the planner's automatic
distinct injection (Layer 4) is gated partly on this: it trades memory for speed
only where correctness permits, and an aggregate query that previously streamed
can become memory-bound for no gain if the gate is loosened.

**`BinaryScanOperator` materializes eagerly.** It decodes `ColumnBatch` rows to
`Binding` values up front rather than deferring. This costs allocation on scans
whose values are never projected. Deferred decoding is a known, unimplemented
optimization — the fast paths sidestep it by never materializing at all, which is
why the aggregate numbers are stronger than the general-scan numbers.

**Planning is greedy.** No dynamic-programming join enumeration. On very large
WHERE clauses, a plan prefix chosen early cannot be revisited. In exchange,
planning time stays negligible and plans are deterministic. Accuracy comes from
the estimator's shape-specific knowledge rather than from search — which means a
query shape the estimator doesn't know can be mis-ordered. `EXPLAIN` will show
you when that happens.

**Large novelty degrades queries.** Under ~10 unindexed transactions the overlay
merge is near-free; past ~100 it is measurable in both latency and memory. Track
`commit_t − index_t`; lag above ~50 means indexing is not keeping up and
`reindex-min-bytes` should come down.

**Unanchored full closure is refused, not attempted.** A property path with both
endpoints unbound returns an error rather than enumerating the transitive
closure of the graph. Bind one side explicitly.

**Fast paths have preconditions.** Most require single-ledger execution, no
`from_t`, root or no policy, and `to_t` at or after the persisted index point.
Time-travelling before the index point needs the history sidecar and takes the
generic pipeline. Policy-enforced queries take the generic pipeline. The fallback
is always correct — it is just not fast-path fast.

## Reproducing the benchmarks

Everything needed to run the comparisons yourself is in
[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db):

- pinned datasets at `s3://fluree-benchmark-data/`
- per-engine setup guides in `common/engine-setup/`
- a generic SPARQL runner, `common/run_benchmark.sh`
- full per-engine results and run metadata in `benchmarks/*/reports/`

Competitor configurations are included so their tuning is auditable rather than
asserted.

## Where this lives in code

| Concern | Crate / file |
|---|---|
| Index permutations, comparators | [`fluree-db-core/src/comparator.rs`](../../fluree-db-core/src/comparator.rs) |
| Binary wire formats, cursors, decode | [`fluree-db-binary-index`](../../fluree-db-binary-index/) |
| Planner, estimation, reordering | [`fluree-db-query/src/planner.rs`](../../fluree-db-query/src/planner.rs) |
| WHERE planning, operator tree build | [`fluree-db-query/src/execute/where_plan.rs`](../../fluree-db-query/src/execute/where_plan.rs) |
| Join operators | `hash_join.rs`, `property_join.rs`, `semijoin.rs`, `cyclic_bgp.rs` |
| Fast paths | `fluree-db-query/src/fast_*.rs` |
| Count planner | `count_plan.rs`, `count_plan_exec.rs` |
| Traversal | `frontier.rs`, `property_path.rs`, `shortest_path.rs` |
| Background indexing | [`fluree-db-indexer`](../../fluree-db-indexer/) |
| Novelty overlay | [`fluree-db-novelty`](../../fluree-db-novelty/) |

## Related documentation

- [Index format](index-format.md) — the binary wire format in detail
- [Query execution and overlay merge](query-execution.md) — pipeline and overlay semantics
- [Explain plans](../query/explain.md) — inspecting planner decisions
- [Background indexing](../indexing-and-search/background-indexing.md) — novelty and reindex thresholds
- [Hardware sizing: CPU vs disk](../operations/hardware-benchmarks.md) — provisioning guidance
- [Performance investigation with distributed tracing](../troubleshooting/performance-tracing.md) — diagnosing a slow query
- [BENCHMARKING.md](../../BENCHMARKING.md) — internal regression-gating benchmarks
