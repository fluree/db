# Performance architecture

Fluree is a temporal, verifiable graph database with triple-level access control,
reasoning, and integrated search. Systems with this feature set are commonly
expected to trade speed for capability; the benchmark results below show where
Fluree stands.

This document describes the design decisions behind that performance, with links
into the code. It also describes the trade-offs Fluree makes and its current
limits; see [Limits and deliberate trade-offs](#limits-and-deliberate-trade-offs).

## Measured results

Benchmark reports are maintained in a separate repository,
**[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db)**. Each
report records its hardware, dataset, query set and configuration, and can be
reproduced from pinned S3 snapshots. The figures below are for Fluree v4.2.8,
measured in November 2026, except WGPB, which was measured with v4.2.7.
Comparisons with other engines are summarized in the
[README](../../README.md#performance).

**SPARQLoscope on DBLP-core** (561 M triples, 105 queries, m7a.4xlarge 16c/64 GB,
180-second timeout): all 105 queries complete, with a 16.1 ms geometric mean and
a 25.1 ms median.

**SPARQLoscope on Wikidata-truthy** (8.18 B triples, 105 queries,
r7a.16xlarge 64c/512 GB, 300-second timeout): all 105 queries complete, with a
270.9 ms geometric mean and an 823.6 ms median.

No query fails in either run, so SPARQLoscope's P=2 aggregate, which counts a
failed query as twice the timeout, equals the plain geometric mean.

**WGPB** (full Wikidata dump, 21.1 B distinct triples, 850 basic graph pattern
queries, r7a.8xlarge 32c/256 GB): all 850 queries complete, with a 14.4 ms
geometric mean and a 10.2 ms median.

**Pokec / openCypher** (the benchgraph suite, m7a.4xlarge 16c/64 GB). Geometric
means of per-query medians, at three graph sizes:

| Nodes / edges | Durable writes (8 queries) | Reads (27 queries) |
|---|---|---|
| 10 K / 122 K | 2.63 ms | 0.71 ms |
| 100 K / 1.77 M | 2.40 ms | 1.29 ms |
| 1.63 M / 30.6 M | 2.54 ms | 1.81 ms |

Each write is committed durably before it is acknowledged, as syscall traces in
the report confirm. Write latency stays between 2.4 and 2.7 ms while the edge
count grows 250-fold.

**BSBM** (Berlin SPARQL Benchmark, m7a.4xlarge database with a separate
m7a.2xlarge driver). Peak throughput, in query mixes per hour:

| Mix | 1 M triples | 100 M | 200 M |
|---|---|---|---|
| Explore (25 queries) | 4,518,756 | 774,093 | 492,762 |
| Explore, SELECT only (20 queries) | 5,034,353 | 796,932 | 497,843 |
| Business Intelligence (15 queries) | 81,635 | 697.0 | 328.6 |

Explore throughput scales to 32 clients and plateaus there. In the
Explore-and-Update mix, where each write is fsynced before it is acknowledged,
throughput at 1 M triples peaks at 88,464 query mixes per hour, or 737 mixed
operations per second, with 16 clients.

> The internal criterion benchmarks documented in
> [BENCHMARKING.md](../../BENCHMARKING.md) serve a different purpose: they detect
> regressions from one pull request to the next.

## Summary

Seven design choices account for most of Fluree's performance:

1. **Integer-ID execution.** Dictionary encoding means joins compare `u64`s, not
   IRIs or strings. Many query shapes never access a dictionary.
2. **Per-column compressed blocks.** A scan decompresses only the columns it
   requests, and some directory-only answers decompress none.
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
  built on the cursor inherit that behavior. Probe lanes that read leaflets
  directly reconcile novelty through a shared helper (`ProbeOps`) that mirrors
  the cursor's merge, or decline when there is novelty.
- **Specialized paths can decline.** Admission criteria are narrow and are
  checked again at runtime, and the generic plan is retained as a fallback. A
  declined optimization usually costs one precondition check, never more than a
  bounded amount of work, and does not affect results.
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
  traced, and the fast paths and several rewrites have
  [environment variables that disable them](../troubleshooting/debugging-queries.md#isolating-an-optimizer-change)
  for comparison.
- **Resource limits still apply.** Specialized operators check cancellation and
  fuel. Several operators that hold intermediate state, including the hash-join
  build, the streaming `GROUP BY` aggregate, `MINUS`, the semijoin and buffered
  subquery rows, charge that memory against the query's budget.
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
inexpensive. For example, `fast_string_prefix_count_all` answers `COUNT(*)` with
`FILTER(STRSTARTS(?o,"Com"))` by scanning OPST slices bounded by the string
dictionary ID range for that prefix. `BinaryScanOperator` therefore prefers OPST
when only the object is bound (subject and predicate unbound), with one exception
described under [Limits](#limits-and-deliberate-trade-offs).

**Numeric IDs throughout.** Subjects, predicates, graphs, datatypes, languages,
and string literals are stored in dictionaries; the index stores `u64`/`u32`
keys. Joins, grouping, and deduplication operate on integers. A query that does
not project a value does not decode it.

**Order-preserving encodings.** Integer (within `i64`), floating-point, temporal,
and boolean objects are encoded so that `o_key` byte order matches value order
within each `o_type`. This allows `ORDER BY DESC(?o) LIMIT k`, `MIN`, and `MAX`
to be answered without a full scan (Layer 5). `xsd:decimal` values and integers
outside `i64` are stored as arena handles, which carry no value order.

**Independently compressed per-column blocks.** A V3 leaflet stores one zstd
block per column (`SId`, `PId`, `OType`, `OKey`, `OI`, `T`), each with its own
`ColumnBlockRef`. The cursor decodes only the columns in its projection. The
generic scan requests the four identity columns plus `OI`; fast paths and join
probes request narrower sets. `T` is skipped on a current-time read of a leaf
the overlay does not touch.

Three columns can be omitted from the block set. POST/PSOT leaflets are
predicate-homogeneous, so `p_id` is stored once as `p_const`. OPST leaflets are
type-homogeneous by segmentation, so `o_type` is stored as `o_type_const`, and
POST/PSOT leaflets do the same when they are single-typed; SPOT leaflets always
carry an `OType` column. The `OI` block is omitted when no row is a list member.
Each column has a fixed width (`u64` for `SId` and `OKey`, `u32` for `PId`, `T`
and `OI`, `u16` for `OType`), and zstd removes the redundancy.

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
this directory. For example, when the `(p, o)` prefixes of `first_key(i)` and
`first_key(i+1)` are equal in POST order, leaflet `i` contains a single `(p, o)`
group and can be counted without decompression.

The same directory entry also carries the leaflet's history locator
(`history_offset`, `history_len`, `history_min_t`, `history_max_t`): an offset
range into the sidecar blob, not inline data. The `min_t`/`max_t` pair lets a
time-travel query skip a leaflet's history segment without reading it.

**Content addressing.** Leaves, branches, and dictionary blobs are identified by
a CIDv1 over SHA-256 (`ContentId`) on every storage backend. Because an address
identifies its content, cached blobs never need invalidation.

## Layer 2: Scan, decode, and cache

`BinaryCursor` yields `ColumnBatch` values, which are leaflet-at-a-time columnar
batches. A `ColumnProjection` / `ColumnSet`
([`column_types.rs`](../../fluree-db-binary-index/src/read/column_types.rs))
declares which columns the consumer needs, and unrequested columns are not
decoded.

**Late materialization.** When the persisted index is authoritative for decoding,
scans emit encoded bindings (`EncodedSid`, `EncodedPid`, `EncodedLit`) instead of
IRIs and values. Joins, filters on encoded keys, grouping, and deduplication
operate on these integers, and values are decoded once, at projection, for the
rows that remain.

**Overlay merging in the cursor.** The cursor skips base rows retracted by the
overlay, injects overlay assertions, and applies `to_t` before the operator above
receives a row. Correctness under uncommitted writes and time travel is
therefore a property of the scan layer for every operator that reads through the
cursor. The batched join and cyclic-BGP probes read leaflets directly and
reconcile novelty through `ProbeOps`, which mirrors the cursor's merge.

Graph scoping is enforced at the same boundary. Each graph has its own branch
manifests, so a scan reads only that graph's leaves, and `BinaryGraphView`
decodes arena-backed values through that graph's arenas.

**Caching.** Decoded leaflet regions and dictionary leaves share one
frequency-aware memory budget
([`LeafletCache`](../../fluree-db-binary-index/src/read/leaflet_cache.rs)), keyed
by content-addressed leaf identity and leaflet index. Entries hold base columns
only; overlay merging and time-travel replay run after the cache, so one entry
serves every `t` and overlay. The budget is split by key hash into about one
shard per core (the core count rounded up to a power of two), capped so that each
shard holds at least 256 MiB (a small budget stays one pool), so cache hits on
different cores do not contend on one shared structure. Each shard enforces its share at its own housekeeping pass, so
between passes a burst of inserts can run the pool over budget by more as the
shard count grows. Remote blobs are fetched through a single-flight disk cache
([`disk_cache.rs`](../../fluree-db-core/src/disk_cache.rs)), so concurrent
requests for the same blob share one fetch.

## Layer 3: The planner

The entry point is `reorder_patterns_with_seed` in
[`planner.rs`](../../fluree-db-query/src/planner.rs) (`reorder_patterns` wraps
it), called from `build_where_operators_seeded_with_needed` in
[`execute/where_plan.rs`](../../fluree-db-query/src/execute/where_plan.rs).

### Placement algorithm

Placement is **greedy rather than dynamic-programming**. Patterns are placed one
at a time:

- **Sources** (triples, VALUES, UNION, uncorrelated subqueries, GRAPH, property
  paths, searches, SERVICE) are placed lowest estimate first. Once variables are
  bound, only sources that join them are candidates, unless none does.
- **Expanders** (OPTIONAL) are placed after the sources, to defer row growth.
- **FILTER and BIND** are inserted as soon as the variables they read are bound,
  so a filter shrinks the stream at the earliest point it can run.
- **MINUS, EXISTS and NOT EXISTS** keep their position after every pattern
  written before them, since their result depends on what those bind.

When two sources have the same estimate, the planner prefers one that lets a
pending selective FILTER run, then one that unlocks a larger hash-joinable scan,
and finally the pattern written first, so planning is deterministic.

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
  a fixed node, or from a node an earlier placement bound, rather than scanning
  the graph. It is estimated as small, so it drives the join instead of being
  costed as a full property scan.
- **Anchored `DISTINCT` subquery producers.** A subquery such as
  `MATCH (p {id: $x})-[:KNOWS*1..2]-(f) WITH DISTINCT f`, one projected variable
  with a constant anchor in its body, is estimated at a small fixed constant
  (`DISTINCT_SUBQUERY_PRODUCER_SELECTIVITY`) standing in for its distinct output,
  rather than at its body's join product, which can be larger by several orders
  of magnitude.
- **Branches containing only compound patterns.** A chained
  `{A} UNION {B} UNION {C}` nests one UNION inside a branch of another, so that
  branch contains no triples of its own. It takes its member's estimate instead
  of the default for an unknown scan. Because `UnionOperator` is correlated and
  re-runs once per input row, an overestimate that places a UNION behind a large
  driver has a disproportionate cost.

Correcting the estimate, rather than adding an operator, improves every query
that contains the shape, regardless of the surrounding patterns.

### Pre-planning rewrites

Several rewrites run around planning, so that ordering and the scan layer both
operate on a narrower form. Each is a semantic identity under a stated
condition. All but the VALUES star seeds run before the patterns are reordered:

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
  of its smallest predicate. This is decided when the reordered star is built
  into operators. The ordinary `ValuesOperator` joins are retained, so
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
large enough that `probe_count / driving_est` stays within `HASH_JOIN_MAX_SCAN_RATIO`.
Otherwise the planner would choose an order that relies on a hash join the
hash-join gate then rejects. The test
`hash_join::tests::producer_seed_clears_scan_ratio_cap` asserts this relationship
and fails if either constant changes in a way that breaks it.

Relationships of this kind are maintained as tested invariants, so the cost
model can be changed without silently invalidating decisions elsewhere.

### Inspecting plans

Planner decisions are reported by [explain plans](../query/explain.md): the
planned index hint where the planner sets one (the final permutation is chosen
when the scan is built and is not shown), whether statistics or fallbacks were
used, estimated row counts per node, hash-join selection and the reason an
alternative was rejected, and whether patterns were reordered. Under root policy,
EXPLAIN plans with the same statistics as execution. Under a non-root policy it
hides statistics, so the order and estimates it shows are heuristic and can
differ from the plan that runs. To compare with the
plan an optimization replaced, use the
[environment variables that disable optimizer rewrites](../troubleshooting/debugging-queries.md#isolating-an-optimizer-change).

## Layer 4: Join operators

The default is `NestedLoopJoinOperator`, which binds each left row into the right
pattern. For a fixed predicate correlated on its subject or its object, and for a
subject-bound existence check, it batches distinct left-side keys into one probe.
The planner selects a specialized operator when the shape warrants it.

**`HashJoinOperator`** ([`hash_join.rs`](../../fluree-db-query/src/hash_join.rs))
addresses object-to-subject joins between a small selective side and a large
predicate scan. Driving from the selective side turns the large pattern into a
right scan with a bound object. The nested-loop join answers that from the
predicate's POST partition, reading each leaf at most once and searching it for
each driving object. The hash join instead builds a table from the small side and
probes it with one scan of the large predicate's partition. It is chosen when
statistics show the probe predicate is no more than `HASH_JOIN_MAX_SCAN_RATIO`
times the driving side's estimate and has at least 50,000 rows, or at least 1,000
when the driving side is small (up to 20,000 rows) or medium (up to 250,000, and
no smaller than the probe).

**`PropertyJoinOperator`** evaluates same-subject multi-predicate stars anchored
by a bound object or a range filter (`?s a :Person ; :name ?n ; :email ?e`) as a
single operator rather than a join chain. One scan of the anchor produces the
subjects, and the remaining predicates are looked up for those subjects only,
by batched PSOT probes or a single SPOT walk. Where neither applies, such as a
predicate with a datatype constraint or a query over several ledgers, it falls
back to a full scan of that predicate.

**`MinusOperator`** builds its exclusion set once in a fresh scope, allowing the
inner patterns to use ordinary scan and star planning. A single shared encoded
reference uses a compact subject-ID set; other terms retain normalized binding
equality, and composite keys retain wildcard compatibility checks. Unbound keys
cannot eliminate a row without at least one shared bound variable. Retained keys
contribute to the query's memory estimate and budget checks.

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

**`OptionalOperator`** answers a single-triple `OPTIONAL` correlated on one end
of its triple with a batched probe over the driving side: a PSOT probe when the
subject comes from the required side (`OPTIONAL { ?s :email ?e }`), a POST
probe over at most 100,000 driving rows at a time when the object does
(`OPTIONAL { ?c :subjectOf ?s }`), the window the join's own bound-object lane
uses. A row a probe cannot key, such as a literal or an unbound value, is looked
up on its own, and the probe answers the rest of its window.
Other shapes look the triple up per driving row with every correlated value
bound into the scan, whichever form the row carries it in: an encoded subject,
predicate or literal is decoded into its slot, so no row scans the whole
predicate. With several graphs of one ledger active there is no graph view, and
the member scans bind every value decoded, so a row carries nothing to decode.
When the row binds the
subject, an encoded IRI, string or date object is left to the join check
instead of decoded, because the subject seek is already narrow. A numeric
object is still substituted: the substitution matches numerics by value across
numeric datatypes, and the join check compares terms.

**Term equality across forms.** On an indexed ledger the scan emits terms
encoded (`EncodedPid` for a predicate position, `EncodedSid` for a subject or
reference object, `EncodedLit` for a literal); decoded producers such as
`VALUES`, `BIND`, and scans over novelty carry the same terms decoded, and the
batched lanes bind a subject minted since the last index by its novelty ID.
The surfaces that key or compare terms (hash joins, `DISTINCT`, `GROUP BY` and
its aggregates, `MINUS`, `EXISTS`, `VALUES`, `OPTIONAL`'s join check, the
nested-loop join's check on a slot it could not bind, `BIND` onto a bound
variable, and `sameTerm`) resolve an IRI to one canonical form: its subject ID
when it has one, persisted or novelty, else its persisted predicate ID. A blank
node follows the same rule. A decoded term resolves through the persisted
dictionary first and the novelty dictionary second, the order every lane
assigns IDs in. The predicate-to-subject mapping is memoized per predicate per
index, so a key over predicate bindings costs a load per row. Property-path
endpoints decode an encoded IRI before traversal. An endpoint bound to a
literal is reached only by a zero-length match, and one that cannot be decoded
is an error. What stays outside the rule:
an IRI no dictionary holds keeps its decoded form, so its `Sid` and `Iri`
forms still compare apart. An arena-backed literal (a big number or a vector,
whose handle names a value within one graph) is never compared or keyed without
a graph view to decode it: `same_term` returns an internal error (and asserts in
debug builds), and the key normalizers fail with a decode error rather than key
by handle. Member scans of several graphs bind
those literals decoded, so no query reaches either.

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
outer `LIMIT` can stop it early. When a row budget reaches this subject-probe
path, its first window uses `LIMIT + OFFSET` without the general join's minimum
window; absent that budget, it keeps the default ramped schedule. The hint affects
operator choice and batch size
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
streaming `DistinctOperator`s between joins, but only when the result cannot
observe row multiplicity: every aggregate is duplicate-insensitive and no other
variable is read raw, or, without grouping, the query is `SELECT DISTINCT`, an
`ASK`, or a `CONSTRUCT` without blank-node templates or `LIMIT`/`OFFSET`. Conversely, a
`DISTINCT` directly beneath a group whose aggregates are all `COUNT(DISTINCT …)`
is removed, because the aggregate already deduplicates its input.

## Layer 5: Fast-path operators

A set of operators recognizes specific query shapes and answers them by fusing
scan and aggregation, bypassing the generic operator tree.

Each **retains the generic tree as a fallback**. Most are built as a
`FastPathOperator`, which returns `Ok(None)` from its `open()`-time closure when
its runtime preconditions do not hold; a few, such as the FIRST-header group
counts and the UNION-star count, are their own operators with the same fallback
contract. Most decline on a cheap precondition check; a few, such as the
POST-order `LIMIT` scan, decline only after a bounded amount of work.
Decisions are emitted as structured tracing events
([`fast_path_outcome.rs`](../../fluree-db-query/src/fast_path_outcome.rs)), so
planned and executed paths can be compared without a lock on the hot path.

### Directory-only aggregates: `O(leaflets)`, not `O(rows)`

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_min_max_string`](../../fluree-db-query/src/fast_min_max_string.rs) | `MIN(?o)` / `MAX(?o)` | POST leaflet boundary keys are the extremes when the leaflet is `o_type`-homogeneous; only leaflets spanning an `o_type` boundary are column-scanned |
| [`fast_group_count_firsts`](../../fluree-db-query/src/fast_group_count_firsts.rs) | `GROUP BY ?o COUNT(?s) ORDER BY DESC LIMIT k`, and `COUNT` of `?s <p> <o>` | Uncompressed per-leaflet FIRST headers: `FIRST(i)==FIRST(i+1)` shows the leaflet is one `(p,o)` group, which is counted without decoding. A leaflet whose directory records one `o_type` skips the `OType` column |
| [`fast_whole_graph_agg`](../../fluree-db-query/src/fast_whole_graph_agg.rs) | Cypher `MATCH (n) RETURN count(n), count(n.age), …` | Replaces the whole-graph distinct-subject scan with directory reads: `count(*) = N + count(P) − subj(P)`, each term computed from the directory |

### Order-exploiting scans

| Operator | Shape | Mechanism |
|---|---|---|
| [`fast_post_order_limit`](../../fluree-db-query/src/fast_post_order_limit.rs) | `ORDER BY DESC(?o) LIMIT k`, optionally with `?s a <Class>` | POST is ordered `(p_id, o_type, o_key, o_i, s_id)`, so for an order-preserving `o_type` the end of the predicate range holds the top-k. Leaves are read backward, only surviving rows are decoded, and reading stops at `OFFSET+LIMIT`. Separate base and overlay-merging variants |
| [`fast_string_fold`](../../fluree-db-query/src/fast_string_fold.rs) | `COUNT(*)` with `REGEX`/`CONTAINS`; `SUM(STRLEN(?o))` and variants | POST places equal strings adjacently, so the function is evaluated once per distinct value (`O(distinct)` rather than `O(rows)`), reading the dictionary in ascending ID order |
| [`fast_string_prefix_count_all`](../../fluree-db-query/src/fast_string_prefix_count_all.rs) | `COUNT(*)` with `REGEX(?o,"^pfx")` / `STRSTARTS` | Where string IDs are in lexical order (a bulk-imported index, until it is first reindexed), a prefix maps to contiguous dictionary ID ranges and bounded OPST slices instead of a full partition scan; elsewhere, or with unindexed novelty, it declines |
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
WHERE join graph and composes a count-only plan. Its IR gives each **output
kind** (scalar, stream, key set) its own type, so a stream cannot be used where a
key set is expected. **Key domains** (subject vs. object keys) are a planner
invariant documented on each node; the type checker does not enforce them.

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

**Write and index side.** Dictionary building, the per-order index builds, and
incremental branch merges are parallelized
([`fluree-db-indexer`](../../fluree-db-indexer/)). Bulk import exceeds 2 M
facts per second.

**Not parallelized:** the general operator tree. A query that does not use a
fast path runs its scan/join pipeline on one core. See
[Limits](#limits-and-deliberate-trade-offs).

## Layer 8: The write path

Commits are written to an in-memory **novelty** overlay and are durable
immediately; they do not wait for index maintenance. Background indexing is
threshold-driven (`reindex-min-bytes` as a soft trigger, `reindex-max-bytes` for
backpressure). Normally it resolves only the commits in the novelty window and
merges them into the affected leaf blobs **copy-on-write**, leaving the rest of
the index unchanged, and publishes the new root atomically. It falls back to a
full rebuild when there is no prior index, when the window spans more than
`incremental_max_commits` commits (10,000 by default), or when the incremental
pass aborts. See
[Background indexing](../indexing-and-search/background-indexing.md).

Because an incremental reindex reuses unchanged blobs by address, it rewrites
only what changed and existing cache entries remain valid.

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
written as a bare JSON-LD string carries no datatype constraint: it matches its text under any string datatype or language
tag, so `(o_type, o_key)` may not be encodable when the scan opens.
`BinaryScanOperator` does not force OPST in that case, because doing so could
produce a wide scan rather than a bounded one. A SPARQL string constant carries
its term (`xsd:string`, a language tag, or a datatype), as does a JSON-LD value
object with `@type` or `@language`, and these use the object-leading path.

**A pushed string comparison seeks the `xsd:string` slice.** `FILTER(?o = "a")`
and string ranges become object bounds on the scan, and a string bound admits
only `xsd:string` values. POST orders by `(p, o_type, o_key)`, so the scan seeks
the predicate's `xsd:string` slice and skips its language-tagged and other typed
values. An equality seeks the one string id, as the constant `?s :p "a"` does.
A range whose string parses as a date or other temporal also seeks those
temporal slices, since `<` and `>` order a string against a temporal value.

**General operator trees are single-threaded.** Intra-query parallelism is
limited to the fast paths, the count planner, cyclic-BGP edge loading, and R2RML
graph-source materialization. A complex analytical join that
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
above approximately 50 indicates that indexing is not keeping up. At the default
`reindex-min-bytes` (100 bytes) a reindex already starts after essentially every
commit, so lowering it helps only where it has been raised; otherwise indexing
throughput is the limit, and `reindex-max-bytes` bounds novelty through
backpressure.

**Unanchored closures are computed but not optimized.** A property path with
both endpoints unbound (`?s <p>+ ?o`) materializes the predicate's full
transitive closure in memory, and composite paths (`?s (<a>/<b>)+ ?o`) run a
separate traversal from every possible start node. A non-composite path with one
endpoint bound uses the frontier path (Layer 6) when it reads one graph of one
ledger without a non-root policy; composite paths and the other cases traverse
node by node, still from the bound endpoint but more slowly. Bind an endpoint
where the query allows it.

**Fast paths have preconditions.** All require single-ledger execution and no
`from_t`. Most require root or no policy; single-predicate fast paths also run
under a policy that provably does not cover their predicate. Fast paths that
read only the index directory need a current index, with no unindexed novelty.
Cursor-based ones tolerate novelty, and the count planner and `+`-path counts
also answer a `to_t` before the index point by replaying the history sidecar.
Where a precondition fails, the generic pipeline returns the same results,
without the fast-path speedup.

## Reproducing the benchmarks

The materials needed to reproduce these results are in
[github.com/fluree/benchmark-db](https://github.com/fluree/benchmark-db):

- pinned datasets at `s3://fluree-benchmark-data/`
- the Fluree setup guide, `common/engine-setup/fluree.md`
- a generic SPARQL runner, `common/run_benchmark.sh`
- full results and run metadata in `benchmarks/*/reports/`

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
