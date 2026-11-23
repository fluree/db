# Memory allocator: mimalloc v3 evaluation (November 2026)

Fluree's binaries use [mimalloc](https://github.com/microsoft/mimalloc) as their
global allocator (see [Memory Allocator](../operations/configuration.md#memory-allocator-mimalloc)).
In November 2026 we measured whether to move from mimalloc v2 to v3. This page
records what we tested, what we measured and what we concluded, so the next
evaluation can start from here.

## Decision

Stay on **mimalloc v2.2.4**, pinned exactly in the workspace `Cargo.toml`:

```toml
mimalloc = { version = "=0.1.48", default-features = false }
libmimalloc-sys = { version = "=0.1.44", default-features = false }
```

No v3 release we tested is both better across our workloads and available from
an official crate:

- **v3.7.0** (with or without upstream's later huge-page fix) serves 21–28% more
  reads under a write and indexing load, but is 10–19% slower on BSBM, a
  read-only benchmark. BSBM's product-detail query (Q2) runs twice as slowly.
- **v3.4.5** is 3% faster on BSBM and level on DBLP overall, with faster writes
  and lower peak memory. It doesn't get v3.7.0's read gain under writes, its
  regex queries on DBLP are 11–26% slower, and no crate has ever shipped it.
- **v3.3.2**, the only v3 on the crates.io `mimalloc` crate, has a known bug
  that affects processes using more than 4 GiB (see
  [Known upstream issues](#known-upstream-issues)). Not measured on Fluree.

### Why the pin is exact

`mimalloc` 0.1.49+ and `libmimalloc-sys` 0.1.49+ build mimalloc v3 by default.
With a caret requirement (`mimalloc = "0.1"`), a routine `cargo update` moves
every binary and the Python extension to v3.3.2 with no code change.
`mimalloc` 0.1.48 accepts any `libmimalloc-sys` 0.1.44 or later, so pinning it
alone isn't enough: `libmimalloc-sys` is pinned too, and declared beside
`mimalloc` in each crate that uses it so the pin holds whichever of them keeps
mimalloc.

## What we tested

| Arm | mimalloc | How it was built |
|---|---|---|
| v2.2.4 | 2.2.4 | crates.io `mimalloc` 0.1.48 + `libmimalloc-sys` 0.1.44 (the pin above) |
| v3.7.0 | 3.7.0 | upstream's own Rust crate, `git = "https://github.com/microsoft/mimalloc", tag = "v3.7.0"` |
| v3.7.0 + fix | 3.7.0 plus upstream's huge-page fix | the same crate at `rev = "aac4f5f47531cf7a22d3d4d85a7fb0b5ab206193"` (v3.7.0 plus only that fix) |
| v3.4.5 | 3.4.5 | `git = "https://github.com/xonatius/mimalloc_rust", rev = "6d4c41bb10c6d9da1d1b6f07b38c4cc051667f11"`: the branch of the open update to the crates.io crate (purpleprotocol/mimalloc_rust PR #169); its C sources are upstream's `v3.4.5` tag |
| v2.3.2 | 2.3.2 | crates.io `mimalloc` 0.1.52 with the `v2` feature (first round only) |

All arms in a round were built from the same Fluree commit with the same
profile (`release` in the first round, `dist` in the final round). To confirm
which mimalloc a stripped binary contains, look for option names:
v3 has `page_cross_thread_max_reclaim`, which v2 lacks; v3.6 and later also have
`arena_purge_min_size`, which v3.4.5 and v3.5 lack. `MIMALLOC_VERBOSE=1` prints the
options in effect at startup, which confirms that an environment setting took
hold.

### Workloads

- **BSBM explore**, 100M scale, server m7a.4xlarge (16 vCPU, 64 GB), separate
  m7a.2xlarge load generator. Per client count: a calibration pilot, then three
  repeats sized to about 180 s, each on a fresh server after a COUNT check and
  1,000 warm-up mixes. Reported as query mixes per hour (QMpH), median of the
  three repeats.
- **SPARQLoscope on DBLP-core**: 561M triples, 105 queries, m7a.4xlarge.
  Page cache dropped and re-warmed, fresh server, one warm-up and three timed
  runs per query (`-w 1 -r 3 -t 180`).
- **Write and indexing soak**: m7a.4xlarge, LUBM-25 imported fresh per run,
  default cache. Two writers at 4 transactions per second each, 1,000 triples
  per transaction, with background indexing at its defaults; four closed-loop
  readers over 14 LUBM queries (no reasoning). 10 minutes of load, then 2 minutes
  idle.
- **Synthetic retention probe**: 8 threads allocate about 5 GiB of objects, then
  free them all ("clean") or keep 1 object in 10 alive ("fragmented", about
  424 MiB live); resident memory is read 20 s later. Linux, transparent huge
  pages (THP) in `madvise` mode.

## Results

### Final round (Fluree 5.0.0-alpha.4 plus seven unrelated query commits)

**BSBM explore, 16 vCPU, QMpH:**

| Clients | v2.2.4 | v3.4.5 | v3.7.0 + fix | v3.7.0 |
|---:|---:|---:|---:|---:|
| 1 | 55,322 | **57,108 (+3%)** | 49,892 (−10%) | 49,133 (−11%) |
| 16 | 695,500 | **713,049 (+3%)** | 561,988 (−19%) | 580,680 (−17%) |
| 32 | 893,243 | **918,360 (+3%)** | 773,659 (−13%) | 760,945 (−15%) |

A repeat of v2.2.4 at 16 clients at the end of the session measured 698,211
(+0.4%). The v3.4.5 repeats stayed within 2% of each other at every client
count; the v3.7.0 repeats spread up to 10%.

Average query execution time in ms, median of the three repeats (explore Q6 is
not part of the mix):

| Query | 1 client: v2.2.4 | v3.4.5 | v3.7.0 + fix | v3.7.0 |
|---|---:|---:|---:|---:|
| Q1 | 4.554 | 4.510 | 4.551 | 4.567 |
| **Q2** | **1.184** | **0.991** | **2.359** | **2.403** |
| Q3 | 5.853 | 5.797 | 5.536 | 5.573 |
| Q4 | 8.216 | 8.119 | 7.743 | 7.846 |
| Q5 | 14.52 | 14.44 | 14.52 | 14.74 |
| Q7 | 1.076 | 1.016 | 1.217 | 1.252 |
| Q8 | 0.706 | 0.679 | 0.748 | 0.765 |
| Q9 | 0.346 | 0.333 | 0.377 | 0.387 |
| Q10 | 0.600 | 0.584 | 0.601 | 0.617 |
| Q11 | 0.395 | 0.386 | 0.416 | 0.424 |
| Q12 | 0.479 | 0.462 | 0.504 | 0.512 |

| Query | 16 clients: v2.2.4 | v3.4.5 | v3.7.0 + fix | v3.7.0 |
|---|---:|---:|---:|---:|
| Q1 | 5.306 | 5.220 | 6.023 | 5.880 |
| **Q2** | **1.808** | **1.576** | **3.645** | **3.311** |
| Q3 | 6.974 | 6.852 | 6.850 | 6.892 |
| Q4 | 9.526 | 9.414 | 9.362 | 9.508 |
| Q5 | 16.55 | 16.69 | 19.69 | 19.20 |
| Q7 | 1.612 | 1.546 | 1.908 | 1.855 |
| Q8 | 1.210 | 1.187 | 1.324 | 1.313 |
| Q9 | 0.775 | 0.766 | 0.872 | 0.863 |
| Q10 | 1.094 | 1.076 | 1.154 | 1.143 |
| Q11 | 0.766 | 0.752 | 0.827 | 0.819 |
| Q12 | 0.878 | 0.869 | 0.968 | 0.956 |

Q2 runs 6 times per mix, so at 1 client it accounts for nearly all of the
v3.7.0 loss. At 16 clients most other queries are also 4–19% slower in both
v3.7.0 arms (Q3 and Q4 are level), and v3.4.5 is within 1% or faster on every
query.

**DBLP-core, one sweep per arm (median ms):**

| | v2.2.4 | v3.4.5 | v3.7.0 + fix | v3.7.0 |
|---|---:|---:|---:|---:|
| Sum of 105 query medians | 21.41 s | 21.39 s | 21.45 s | 22.72 s |
| Geometric mean | 15.68 | 15.80 | 15.86 | 16.04 |
| `exists-join-3-chain-2` | 2259 | 2230 | 2297 | 2646 |
| `minus-join-3-chain-2` | 2264 | 2232 | 2320 | 2618 |
| `minus-join-3-star-1` | 1126 | 1093 | 1086 | 1286 |
| `result-size-xlarge` | 4381 | 4426 | 4259 | 4620 |
| `group-by-count-object-low-multiplicity` | 112.8 | 112.7 | 129.7 | 129.6 |
| `regex-3` | 521 | 577 | 598 | 578 |
| `regex-3-fixed` | 384 | 485 | 486 | 483 |
| Peak server memory | 17.6 GB | 16.3 GB | 15.7 GB | 15.7 GB |

All 105 queries passed in every arm.

**Write and indexing soak (two runs per arm):**

| | Reads/s | Median read | Median write | 99th-percentile write |
|---|---:|---:|---:|---:|
| v2.2.4 | 46.1 / 45.6 | 6.8 / 8.4 ms | 12.4 / 12.1 ms | 289 / 343 ms |
| v3.4.5 | 47.0 / 46.6 | 5.9 / 5.9 ms | 10.9 / 11.1 ms | 202 / 166 ms |
| v3.4.5, `MIMALLOC_ARENA_PURGE_MULT=4` (one run) | 50.6 | 5.5 ms | 10.8 ms | 189 ms |
| v3.7.0 + fix | 58.3 / 59.2 | 5.2 / 5.1 ms | 10.9 / 10.6 ms | 147 / 161 ms |
| v3.7.0 | 57.6 / 55.1 | 5.4 / 5.5 ms | 10.7 / 10.7 ms | 172 / 252 ms |

Peak memory under load was 26.8–29.2 GB in every arm, mostly the query cache
filling to its budget (35% of RAM by default).

### First round (Fluree 5.0.0-alpha.4)

**Write and indexing soak**, same settings, two rounds in rotated order:

| | Reads/s | Median read | Median write | 99th-percentile write |
|---|---:|---:|---:|---:|
| v2.2.4 | 45.9 / 47.0 | 7.9 / 7.7 ms | 12.2 / 12.5 ms | 213 / 223 ms |
| v2.3.2 | 45.1 / 45.3 | 6.9 / 7.0 ms | 12.6 / 12.5 ms | 357 / 249 ms |
| v3.7.0 | 56.2 / 56.2 | 5.3 / 5.0 ms | 10.7 / 10.7 ms | 128 / 181 ms |
| v3.7.0, `MIMALLOC_ALLOW_THP=1` | 52.0 / 51.3 | 5.6 / 5.9 ms | 10.9 / 10.9 ms | 181 / 255 ms |

The same soak with the cache capped at 4 GB (`FLUREE_CACHE_MAX_MB=4096`),
20 minutes of load and 3 minutes idle, one round:

| | Reads/s | Median write | Memory outside the cache, idle | Peak RSS |
|---|---:|---:|---:|---:|
| v2.2.4 | 49.3 | 16.4 ms | 6.5 GB | 12.5 GB |
| v2.3.2 | 48.2 | 16.8 ms | 6.1 GB | 11.9 GB |
| v3.7.0 | 54.2 | 13.7 ms | 7.0 GB | 11.0 GB |
| v3.7.0, `MIMALLOC_ALLOW_THP=1` | 50.5 | 13.7 ms | 5.3 GB | 10.6 GB |

LUBM-25 import (14.5–14.9 s, 1.2 GB peak) and single-client hot LUBM queries
(geometric mean within 1%) were the same in every arm. Those queries take a few
milliseconds each and allocate little, which is why this round did not reveal
the BSBM and DBLP losses.

**Synthetic retention probe**, resident memory 20 s after the free (MiB):

| | Clean free | Fragmented |
|---|---:|---:|
| v2.2.4 | 2,688 (of 5,251 peak) | 4,697 |
| v2.3.2 | 12 | 4,651 |
| v3.3.2 | 255 | 1,896 |
| v3.7.0 | 753 | 3,369 |
| v3.7.0, `MIMALLOC_ALLOW_THP=1` | about 50 | 1,183 |
| v3.7.0, `MIMALLOC_ALLOW_THP=0` | about 23 | 1,048 |

v2.2.4 kept about half of the clean-freed memory until a forced
`mi_collect(true)`. In the fragmented case, v3.7.0 at its defaults keeps more
than with `ALLOW_THP` at 0 or 1 because it purges only whole 2 MiB ranges, so as
not to split huge pages.

### DBLP diagnosis of v3.7.0

These runs compared v3.7.0 with v2.2.4 on DBLP-core before the upstream fix was
known.

- **The heavy-join gap depends on query order.** Run first on a fresh server,
  v3.7.0 is as fast as v2.2.4 on the multi-second joins, or faster. After a
  full 105-query sweep, v2.2.4 runs them 14–17% faster than it did on the fresh
  server, and v3.7.0 doesn't change.
- **Page faults explain the gap; TLB misses don't.** Per block of the six
  heaviest queries ×5, v2.2.4 took 3.06M page faults before the sweep and
  0.33–0.63M after it. v3.7.0 took about 3.6M in every block. dTLB misses did
  not fall for v2.2.4 after the sweep, and memory on 2 MB pages ended at similar
  levels in both.
- **Settings that didn't help v3.7.0:** `MIMALLOC_PURGE_DELAY=-1` (never return
  freed memory) changed neither latency nor faults. `MIMALLOC_ALLOW_THP=1`
  performed like the default. `MIMALLOC_ALLOW_THP=0` was the worst: 5–10%
  slower on the heavy joins with 1.5–2× the faults.
- **The `group-by-count-object-low-multiplicity` slowdown is in our code's
  cycles, not the allocator's.** A CPU profile put the extra 15% inside
  `group_count_v6` and `offer_topk`, with no mimalloc function above 0.3%.
  That points to data placement or code layout. v3.4.5 doesn't have it.

The upstream huge-page fix (next section) matches the fault pattern and removed
the heavy-join gap in the final round. It did not change BSBM.

## What is established and what isn't

Established:

- The BSBM loss and the `group-by-count` slowdown come from a change between
  v3.4.5 and v3.7.0 other than the huge-page bug. Both v3.7.0 arms show them;
  v3.4.5 doesn't.
- The DBLP heavy-join loss was the huge-page bug: the fix removed it.
- The `regex-3` and `regex-3-fixed` slowdowns are in every v3 arm, v3.4.5
  included. They come from one sweep per arm, but all three v3 arms agree.
- Part of v3.7.0's read gain under writes comes from returning freed memory
  less often: v3.4.5 with v3.7.0's arena purge multiplier (4000 ms instead of
  1000 ms) gained 10% instead of 2%.

Not established:

- Which upstream commit causes the BSBM Q2 regression. It lies between v3.4.5
  and v3.7.0.
- Whether the same change also gives v3.7.0 the rest of its read gain under
  writes.
- How `MIMALLOC_ARENA_PURGE_MULT=4` on v3.4.5 does on BSBM. It wasn't measured.
- What causes the regex slowdowns.
- How mimalloc v2.5.x, the newer v2 line, performs. It wasn't tested.

## Known upstream issues

| Issue | Affects | Fixed in |
|---|---|---|
| Uses OS memory instead of arenas once the process uses more than 4 GiB | v3 before 3.4.1, so the crates.io crate's v3.3.2 | v3.4.1 (release notes, 2026-07-14) |
| On macOS, every mimalloc v3 copy in a process shares fixed thread-local slots, so a Python extension using mimalloc v3 can crash under CPython 3.14, which ships its own copy ([#1327](https://github.com/microsoft/mimalloc/issues/1327)) | v3.3.2 | v3.4.4 |
| Some page allocations taken directly from the OS don't get the transparent-huge-page hint, so they fault in 4 KiB pages ([#1502](https://github.com/microsoft/mimalloc/issues/1502); introduced in commit `67b324e`) | v3.5.0 to v3.7.0 | commits `e2a1f17` and `aac4f5f`, 2026-11-18; not in a release as of 2026-11-23 |

Purge defaults, which govern how soon freed memory goes back to the OS:

| | `purge_delay` | `arena_purge_mult` | Effective arena delay | `allow_thp` |
|---|---:|---:|---:|---|
| v2.2.4 | 10 ms | 10 | 100 ms | (no such option) |
| v3.4.5 | 1000 ms | 1 | 1000 ms | 1 |
| v3.7.0 | 1000 ms | 4 | 4000 ms | 2 (purge only whole 2 MiB ranges) |

## Crate availability

No crate has ever shipped mimalloc v3.4.x.

| Crate | Publisher | mimalloc it bundles |
|---|---|---|
| `mimalloc` / `libmimalloc-sys` (crates.io) | purpleprotocol | 0.1.48: v2.2.4 (v3.1.5 behind a `v3` feature). From 0.1.49: v3 by default, v2 with the `v2` feature. The latest, 0.1.52 (May 2026), bundles v3.3.2 and v2.3.2. |
| `mimalloc` (upstream `contrib/rust`) | Microsoft, not on crates.io | from v3.6.0 (added 2026-10-17); usable as a git dependency at a release tag |
| `mimalloc3` / `libmimalloc-sys3` (crates.io) | third party | v3.5.0 (August 2026) to v3.7.0 |
| `mimalloc-safe` (crates.io) | napi-rs | v2 only, 2.3.2 to 2.5.2 |

## When to look again

Look again when one of these happens:

- An upstream v3 release fixes the BSBM Q2 regression. Bisecting v3.4.5..v3.7.0
  with a 1-client BSBM run would find the commit and could be reported upstream.
- A v3 release at or after v3.4.5 reaches an official crate, for example the
  crates.io `mimalloc` crate once PR #169 or a later update is published.
- mimalloc v2.5.x is worth a look as a newer v2.

A candidate must:

- come from an official crate (crates.io `mimalloc`, or upstream's own crate at
  a release tag);
- have no open issue affecting processes larger than 4 GiB.

### How to run the comparison

- Build every arm from the same Fluree commit, and run the v2.2.4 arm in the
  same session on the same machines. Repeat one v2.2.4 cell at the end to check
  for drift.
- Run all three workloads. The soak alone misses read-only losses, and BSBM
  alone misses the gain under writes.
- Use machines nothing else is running on. An overlapping job inflated
  tail-latency numbers once during this evaluation.
- On DBLP, compare full sweeps, not queries run in isolation. v2's heap state
  after earlier queries affects the heavy joins.
- Check `MIMALLOC_VERBOSE=1` output whenever a run sets a mimalloc environment
  variable.

### What to compare

- **BSBM:** QMpH at 1, 16 and 32 clients, and the Q2 and Q5 execution times.
- **DBLP:** the sum of query medians, the heavy joins (`exists-join-3-chain-2`,
  `minus-join-3-chain-2`, `minus-join-3-star-1`),
  `group-by-count-object-low-multiplicity`, `regex-3`, `regex-3-fixed`,
  `result-size-xlarge`, and peak memory.
- **Soak:** reads per second, median and 99th-percentile write latency, and
  idle memory outside the cache.
