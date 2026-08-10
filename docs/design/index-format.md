# Binary index format (leaf / leaflet / dictionaries)

This document describes the on-disk / blob-store formats used by Fluree’s binary indexes:
the **branch → leaf → leaflet** hierarchy for fact indexes, and the **dictionary artifacts**
used to translate between IRIs/strings and compact numeric IDs.

The intent is to make the formats easy to reason about (for debugging and tooling) and to
highlight why **leaf files contain multiple leaflets**: it materially improves performance and
cost characteristics on blob/object storage by reducing object counts and request rates while
preserving fine-grained decompression and caching at the leaflet level.

## Overview

A binary index build produces:

- **Per-graph, per-sort-order fact indexes**:
  - a content-addressed **branch manifest** (`FBR3`, file extension `.fbr`)
  - a set of content-addressed **leaf files** (`FLI3`, file extension `.fli`)
  - each leaf contains multiple **leaflets** (groups of independently compressed per-column blocks)
- **Shared dictionary artifacts**:
  - small dictionaries (predicates, graphs, datatypes, languages) embedded in the **index root** (CAS) and/or persisted as flat files in local builds
  - large dictionaries (subjects, strings): reverse as **CoW single-level B-tree-like trees**
    (a branch manifest `DTB1` + `DLR1` leaf blobs), forward as ID-range-routed **`FPK1` packs**
- **Manifests / roots** that describe how to load the above either from a local directory layout
  or from the content store via `IndexRoot` (FIR6 binary format, CID-based).

Fact indexes exist in up to four sort orders (see `RunSortOrder`):

- **SPOT**: \((g, s, p, o, dt, t, op)\)
- **PSOT**: \((g, p, s, o, dt, t, op)\)
- **POST**: \((g, p, o, dt, s, t, op)\)
- **OPST**: \((g, o, dt, p, s, t, op)\)

## Design goals

- **Blob-store efficiency**: keep object counts low and object sizes in a “healthy” range for
  S3/GCS/Azure-like stores, avoiding “many tiny objects” request overhead.
- **Fast routing**: branch manifest enables binary search routing to the relevant leaf range(s).
- **Cheap decompression**: leaflets are internally structured so query paths can decompress
  *only what they need* (e.g. the key columns to filter, before paying to decode `T` or `OI`).
- **Content-addressed immutability**: leaves/branches/dict leaves can be cached aggressively
  and safely, because their CAS address (or content hash filename) uniquely identifies content.
- **Simple versioning**: each binary artifact begins with a magic + version and can be rejected
  early if incompatible.

## Terminology

- **Leaflet**: a compressed block of rows (default build target: `leaflet_rows = 25_000`).
- **Leaf**: a container of multiple leaflets (default: `leaflets_per_leaf = 10`) plus a directory for
  random access to its leaflets.
- **Branch manifest**: maps key ranges to leaf files; used for routing.
- **Column block**: a separately compressed single-column section inside a leaflet.
- **History sidecar**: a separate CAS object holding a leaf's time-travel transition log.
- **Dictionary tree**: a `DTB1` branch + `DLR1` leaves, holding the reverse (value → ID) direction for large keyspaces (subjects/strings).
- **Forward pack**: an `FPK1` blob holding a contiguous ID range of a forward (ID → value) dictionary, routed from the index root.
- **ContentId**: a CIDv1 value that uniquely identifies a content-addressed artifact by its hash and type. See [ContentId and ContentStore](content-id-and-contentstore.md).

## Physical layout (local build output)

When built to a filesystem directory (see `IndexBuildConfig`), the output layout is:

```text
index/
  index_manifest_spot.json
  index_manifest_psot.json
  index_manifest_post.json
  index_manifest_opst.json
  graph_<g_id>/
    spot/
      <branch_hash>.fbr
      <leaf_hash_0>.fli
      <leaf_hash_1>.fli
      ...
    psot/
      ...
    post/
      ...
    opst/
      ...
```

The `.fbr` and `.fli` files are content-addressed by **SHA-256 hex** of their bytes (the filename is the hash).
`index_manifest_<order>.json` is a small routing manifest that points to the per-graph directory and branch hash.

### Per-order index manifest (`index_manifest_<order>.json`)

The per-order manifest is JSON and summarizes all graphs for a sort order:

- `total_rows`: total indexed asserted facts for that order
- `max_t`: max transaction `t` in the indexed snapshot
- `graphs[]`: `g_id`, `leaf_count`, `total_rows`, `branch_hash`, and `directory` (relative path)

## Root descriptor (CAS): `IndexRoot` (FIR6)

When publishing an index to nameservice / CAS, the canonical entrypoint is the **FIR6 root**
(`IndexRoot`, binary wire format, magic bytes `FIR6`).

Key properties:

- **CID references** for all artifacts (dicts, branches, leaves).
- Deterministic binary encoding so the root itself is suitable for content hashing to derive its own ContentId.
- Tracks `index_t` (max transaction covered) and `base_t` (earliest time for which history-sidecar replay is valid).
- Embeds **predicate ID mapping** and **namespace prefix table** inline, so query-time predicate IRI → `p_id` translation does not require fetching a redundant predicate dictionary blob.
- Embeds small dictionaries (**graphs**, **datatypes**, **languages**) inline, so query-time graph/dt/lang resolution does not require fetching tiny dict blobs (important for S3 cold starts).
- **Default graph routing is inline**: leaf entries (first/last key, row count, leaf CID) are embedded directly, avoiding an extra branch fetch for the common single-graph case.
- **Named graph routing uses branch CID pointers**: larger multi-graph setups reference branch manifests by CID.
- Optional binary sections for **stats**, **schema**, **prev_index** (GC chain), **garbage** manifest, and **sketch** (HLL).
- Import-only performance hint: `IndexRoot.lex_sorted_string_ids` indicates whether `StringId` assignment preserves
  lexicographic UTF-8 byte order of strings (true for bulk imports). Query execution can use this to avoid
  materializing simple string values during `ORDER BY` comparisons. This flag must be cleared on the first
  post-import write because incremental dictionary appends break the invariant.
  When the flag is absent (older roots) or false, query execution must assume no lexical ordering.

At a high level the root contains:

- **Inline small dictionaries** (embedded in the binary root):
  - `graph_iris[]` (dict_index → graph IRI; `g_id = dict_index + 1`)
  - `datatype_iris[]` (dt_id → datatype IRI)
  - `language_tags[]` (lang_id-1 → tag string; `lang_id = index + 1`, 0 = "no tag")
- **Dictionary ContentIds** (CAS artifacts):
  - tree blobs: subject/string reverse (`DTB1` branch + `DLR1` leaves)
  - forward packs: subject (per namespace) & string (`FPK1`), routed by ID range
  - optional per-predicate numbig arenas
  - optional per-predicate vector arenas (manifest + shards)
- **Default graph routing** (inline leaf entries per sort order)
- **Named graph routing** (branch CIDs per sort order per graph)

## Branch manifest (`FBR3`, `.fbr`)

A branch manifest is a single-level index mapping key ranges to leaf files. It is written per graph
per order and read via binary search to route a lookup/range scan.

### File format

```text
[BranchHeader: 16 bytes]
  magic: "FBR3" (4B)
  version: u8
  _pad: [u8; 3]
  leaf_count: u32
  _reserved: u32
[LeafEntries: leaf_count × 104 bytes]
  first_key: key bytes (44B, little-endian)  [1]
  last_key:  key bytes (44B, little-endian)  [1]
  row_count: u64
  path_offset: u32
  path_len: u16
  _pad: u16
[PathTable]
  Concatenated UTF-8 relative paths (typically "<leaf_hash>.fli")
```

Notes:

- `first_key` and `last_key` use the same 44-byte key wire encoding produced by the index builder (see footnote [1]).
- The path table stores **relative filenames**; on read, paths are resolved against the `.fbr`’s directory.
- In local builds, paths are `<leaf_hash>.fli` to match the content-addressed leaf filenames.

**[1] Key encoding note (internal)**: the 44-byte key is the `RunRecord` wire layout used by the import/index-build
pipeline and stored here only for routing. It is an internal build artifact detail (not a core runtime fact type).

## Leaf file (`FLI3`, `.fli`)

A leaf file groups multiple leaflets into a single blob, and includes a small directory so leaflets can
be accessed without scanning the entire file.

### File format

```text
[LeafHeaderV3: fixed 72 bytes]
  magic: "FLI3" (4B)
  version: u8
  order: u8              (RunSortOrder wire id)
  _pad: [u8; 2]
  leaflet_count: u32
  total_rows: u64
  first_key: [u8; 26]    (ORDERED_KEY_V2)
  last_key:  [u8; 26]
[LeafletDirectory: leaflet_count × variable]
  row_count: u32
  lead_group_count: u32
  first_key: [u8; 26]
  last_key:  [u8; 26]
  p_const: u32           (u32::MAX = not present)
  o_type_const: u16      (u16::MAX = not present)
  flags: u32             (HAS_O_I | HAS_O_TYPE_COL)
  payload_offset: u32    (relative to payload section start)
  payload_len: u32
  column_count: u16
  [ColumnBlockRef × column_count]   (16 bytes each)
  history_offset: u64    ─┐ locator into the history SIDECAR blob,
  history_len: u32        │ not into this leaf
  history_min_t: u32      │
  history_max_t: u32     ─┘
[LeafletData: concatenated compressed column blocks]
```

Directory entries are variable-length because `column_count` varies with the sort
order and which values were hoisted to constants.

**Leaflet-boundary skip-decoding.** Because each entry carries `first_key` /
`last_key` uncompressed, adjacent entries can be compared without touching the
payload. If two adjacent entries share the same `(p_id, o_type, o_key)` prefix in
POST order, the earlier leaflet is guaranteed to contain only that `(p, o)`
combination — so fast-path COUNT and GROUP BY operators count rows straight from
`row_count` without decompressing any column block. `lead_group_count` similarly
gives the number of distinct leading-key values per leaflet without a decode.
This is what makes the directory-only aggregates in
[Performance architecture](performance.md) `O(leaflets)` rather than `O(rows)`.

**History locators.** The four `history_*` fields address a segment inside the
leaf's [history sidecar](#history-sidecar-fhs1) — the leaf itself never contains
history bytes. `history_min_t` / `history_max_t` let a time-travel query skip a
leaflet's history segment without reading it, and a HEAD-only query ignores these
fields entirely.

### Why “leaf contains leaflets” (blob-store optimization)

If every leaflet were its own object:

- range scans and joins would issue **many more GETs** (request overhead dominates)
- caches would be pressured by **object metadata overhead** and higher churn

By grouping N leaflets into one leaf object:

- we reduce object count and request rate roughly by a factor of N
- we still keep leaflet-sized “micro-partitions” internally for:
  - selective decompression (column-by-column)
  - caching hot leaflets (decoded) independent of unrelated ones
  - future optimizations like ranged reads (leaflet offsets are explicit)

The default build targets (`leaflet_rows = 25_000`, `leaflets_per_leaf = 10`) yield a leaf that is
large enough to amortize object-store overhead but still small enough to cache and move efficiently.

## Leaflet format (compressed block inside a leaf)

A V3 leaflet is a set of **independently zstd-compressed per-column blocks**. There is no
fixed multi-region header: the leaflet's directory entry carries a `ColumnBlockRef` per
column present, and the payload is the concatenation of those compressed blocks. Which
columns are present depends on the sort order and on which values can be hoisted to
constants.

> **History is not stored in the leaflet.** It lives in a separate CAS object — the
> per-leaf **history sidecar** (`FHS1`). See
> [History sidecar](#history-sidecar-fhs1) below.

### Leaflet directory entry

Per leaflet (see `EncodedLeaflet` in `fluree-db-binary-index/src/format/leaflet.rs`):

```text
row_count: u32
lead_group_count: u32          // distinct values of the leading sort key
first_key: [u8; 26]            // ORDERED_KEY_V2_SIZE, order-specific routing key
last_key:  [u8; 26]
p_const:      Option<u32>      // POST/PSOT: the leaflet's constant predicate
o_type_const: Option<u16>      // OPST always; other orders when single-typed
flags: u32                     // HAS_O_I | HAS_O_TYPE_COL
column_refs: [ColumnBlockRef]  // one per column actually stored
```

`ColumnBlockRef`:

```text
col_id: u16          // SId=0, PId=1, OType=2, OKey=3, OI=4, T=5
codec: u8            // currently always Zstd
elem_width: u8       // 1, 2, 4, or 8 bytes
offset: u32          // relative to leaflet payload start
compressed_len: u32
uncompressed_len: u32
```

### Columns

The full column set is `SId`, `PId`, `OType`, `OKey`, `OI`, `T`. A column is omitted
entirely when its value is constant for the leaflet or absent for every row:

- **POST / PSOT** leaflets are predicate-homogeneous, so `p_id` is hoisted to `p_const`.
- **OPST** leaflets are type-homogeneous by segmentation design, so `o_type` is always
  hoisted to `o_type_const`. Other orders hoist it too when the leaflet is single-typed
  (common for a single-datatype predicate); `HAS_O_TYPE_COL` marks the mixed case.
- **`o_i`** (list index) is only materialized when at least one row is a list member —
  `HAS_O_I`.

Readers use the constants instead of decoding a block, and decode only the columns a
given query actually projects or filters on.

> **Note:** V3 uses a **unified `o_type` tag** (`RunRecordV2.o_type: u16`) that carries
> datatype identity. There are no separate `dt` / `lang_id` columns — earlier revisions of
> this document described a V2 layout that had them, along with `o_kind`.

#### Column presence by sort order

The leading sort key compresses best and is the natural RLE candidate; the remaining
columns are stored densely. Constants are hoisted out of the block set entirely:

| Order | Hoisted constant | Columns stored |
|-------|------------------|----------------|
| SPOT  | (none)           | `SId`, `PId`, `OKey`, `T` (+ `OType` when mixed, `OI` when present) |
| PSOT  | `p_const`        | `SId`, `OKey`, `T` (+ `OType` when mixed, `OI` when present) |
| POST  | `p_const`        | `OKey`, `SId`, `T` (+ `OType` when mixed, `OI` when present) |
| OPST  | `o_type_const`   | `OKey`, `PId`, `SId`, `T` (+ `OI` when present) |

Element width per column is carried in `ColumnBlockRef.elem_width`, so a column narrows
to the smallest type that fits its dictionary cardinality rather than paying a fixed width.

## History sidecar (FHS1)

Time-travel history is **not** stored inside the leaflet. Each leaf may have a companion
**history sidecar**: a separate content-addressed object whose CID is carried on the
branch manifest's leaf entry (`LeafEntry.sidecar_cid`, `Option<ContentId>` — the single
source of truth for locating it). Leaves with no history have no sidecar.

Keeping history out of the leaflet means a HEAD-only query never fetches, decompresses, or
caches history bytes at all — the leaflet cache deliberately does not hold sidecar data,
since it is cold-path.

### Sidecar blob layout

```text
magic:   [u8; 4]   "FHS1"
version: u8        1
padding: [u8; 3]   reserved
[HistorySegment for leaflet 0]
[HistorySegment for leaflet 1]
...
[HistorySegment for leaflet N]
```

Each segment corresponds positionally to one leaflet of the leaf:

```text
entry_count: u32
[HistEntryV2 × entry_count]     // sorted by t DESCENDING (newest first)
```

`HistEntryV2` wire layout (31 bytes, little-endian):

```text
s_id:   u64
p_id:   u32
o_type: u16
o_key:  u64
o_i:    u32
t:      u32
op:     u8      // 0 = retract, 1 = assert
```

### Semantics

Valid from `base_t` onward (see the index root). The sidecar is a **transition log**, not a
commit log:

- Entries record only presence *changes*. An assert entry means the fact was absent
  immediately before its `t`; a retract entry means it was present immediately before.
  No-op events (re-asserting a present fact, retracting an absent one) are never recorded.
- **Row-assert conservation**: a fact's materialized assert lives either as its base row or
  as a sidecar entry — never both, never neither. Replay synthesizes the assert event from
  the base row.
- **Segment co-location**: a fact's transition entries must land in the segment matching the
  leaflet holding its materialized row. Replay is per-leaflet, so entries stranded in a
  neighbouring segment would silently drop the fact from time-travel results. The writer
  buffers pushed history until the row's leaflet is known, so entries follow their row
  across a predicate/`o_type` segment boundary — see `LeafWriter::commit_all_pending` in
  `fluree-db-binary-index/src/format/leaf.rs`.
- Index-served history is **transition-grade**: it reports state changes, not every commit
  event.

Authoritative semantics: the module docs of
`fluree-db-binary-index/src/format/transitions.rs` and
`fluree-db-binary-index/src/format/history_sidecar.rs`.

## Dictionary artifacts

Binary indexes store facts in numeric-ID form. Dictionaries are required to:

- translate query inputs (IRIs, strings) to numeric IDs for scans
- decode numeric IDs back to user-visible values when returning flakes

### Small flat dictionaries (`FRD1`)

Several dictionaries use a simple “count + length-prefixed UTF-8” format:

```text
magic: "FRD1" (4B)
count: u32
for each entry:
  len: u32
  utf8_bytes: [u8; len]
```

This format is used for predicate-like dictionaries. In local builds these are written
as flat files (e.g., `graphs.dict`, `datatypes.dict`, `languages.dict`), but in CAS
publishes (FIR6 root) these small dictionaries are embedded inline in the binary root.

### Legacy forward files + index (`FSI1`) (primarily build-time)

Some build paths still write a forward file (`*.fwd`) plus a separate index (`*.idx`):

`FSI1` index format:

```text
magic: "FSI1" (4B)
count: u32
offsets: [u64] × count
lens:    [u32] × count
```

The forward file itself is a raw concatenation of bytes; access is via `(offset,len)` from the index.

### Large dictionaries

Subjects and strings are large enough to need their own artifacts, and the two
directions use different structures:

- **Reverse** (value → ID) is a single-level **CoW tree**: a `DTB1` branch
  mapping key ranges to `DLR1` leaf ContentIds. An incremental update rewrites
  only the leaves whose key range it touches; untouched leaves keep their CIDs.
- **Forward** (ID → value) is a set of **`FPK1` packs** routed by ID range
  directly from the index root — not a tree. See below.

#### Dictionary branch (`DTB1`)

```text
[magic: 4B "DTB1"]
[leaf_count: u32]
[offset_table: u32 × leaf_count]  // byte offset of each leaf entry
[leaf entries...]
  entry :=
    [first_key_len: u32] [first_key_bytes]
    [last_key_len: u32]  [last_key_bytes]
    [entry_count: u32]
    [content_id_len: u16]   [content_id_bytes]
```

Keys are treated as raw bytes and compared lexicographically. For forward trees keyed by numeric ID,
the branch uses **8-byte big-endian** keys (so lexical order matches numeric order).

#### Forward dict pack (`FPK1`)

Forward dictionaries are paged packs, each covering a contiguous ID range:

```text
Header (40 bytes):
  [magic: 4B "FPK1"] [version: u8=1] [kind: u8] [ns_code: u16 LE]
  [first_id: u64 LE] [last_id: u64 LE]
  [page_count: u32 LE] [page_dir_offset: u64 LE] [reserved: u32=0]

Pages (concatenated):
  page := [entry_count: u32 LE] [offsets: u32 LE × (entry_count+1)] [value bytes]

Page directory (at page_dir_offset, 20 bytes per entry):
  [page_first_id: u64 LE] [entry_count: u32 LE]
  [page_offset: u32 LE] [page_len: u32 LE]
```

IDs within a page are contiguous, so a lookup binary-searches the page
directory and then indexes directly: `value = data[offsets[local]..offsets[local+1]]`.
`kind` is `0` for the single global string stream and `1` for subject packs,
which are per-namespace (`ns_code`). Targets are **512 KiB** per page and
**16 MiB** per pack.

Because the encoded length is `page_dir_offset + page_count * 20`, a pack can
be sized from its 40-byte header alone — a range read, not a full fetch.

**Routing** lives inline in the index root (`DictPackRefs`), as
`{first_id, last_id, pack_cid}` per pack. Ranges must be ascending and
non-overlapping; gaps between packs are legal.

#### Forward pack compaction

Each incremental build appends at least one pack per dict stream it touched,
including a small trailing pack for a handful of new entries. Left alone that
grows the routing table once per build forever — unbounded objects, unbounded
mappings, and a root that grows on every publish.

So after appending, the indexer merges qualifying **runs** in each touched
stream: scanning left to right for the longest contiguous run that fits the
16 MiB target and whose members are either all small (≤ 64 KiB) or within a 4×
size ratio of each other, requiring either 8 packs or a group already at half
the target. Merging preserves input pages verbatim, rebasing their offsets —
no dictionary value is decoded.

Runs rather than suffixes, because a merge output is larger than the packs
around it. Anchoring candidates at the newest pack means that once a merge
lands, every later candidate contains it, it fails the size ratio against its
smaller neighbours, and any fragmentation *behind* it is unreachable forever —
which would leave a table inherited from before compaction permanently stuck.

Nothing records which packs were previously compacted: the rule is scale-free,
decided from sizes and ID ranges alone, which is why no compaction state is
carried in the root. Sizes come from an in-cycle memo, local file metadata, or
a 40-byte header range read — never a full fetch.

Each stream's **tail is examined first and unconditionally**, because that is
where new packs land and so what keeps the table bounded. Only then are older
regions swept for inherited fragmentation. Sweeping head-first instead lets a
table larger than the scan budget hide its own tail: the budget is spent
walking mature packs, the newest packs are never reached, and growth becomes
unbounded again for exactly the largest dictionaries.

Compaction also runs for streams that received **no novelty** this cycle. An
active stream keeps itself tidy, but one that goes quiet — or whose work was
cut short when a budget ran out — would otherwise keep its fragmentation
forever while its packs are still loaded with the index. Coverage rotates with
`index_t`, so a ledger with more streams than one cycle can service still
reaches all of them over time without persisting a cursor.

Work per cycle is bounded two ways, both shared across every stream: 64 MiB of
merge input, and a count of storage operations. The byte budget alone does not
bound *requests* — a backlog of 113-byte packs consumes almost none of it while
still costing a probe and a GET per pack — and a per-stream counter would be
multiplied by the namespace count. Every probe and fetch that reaches storage
is charged against one cycle-wide budget, so a large backlog drains over
several cycles instead of stalling one publish behind thousands of round trips.
Probes and fetches issue concurrently.

Superseded packs are diffed out of the routing table by
`IncrementalRootBuilder::set_dict_refs` and land in the garbage manifest, so
they are reclaimed by the normal retention-aware GC — older roots keep
referencing them until those roots age out. Packs *created and consumed inside
one cycle* — a freshly appended pack that compaction immediately absorbed, or a
cascade's own intermediate output — appear in neither the base root nor the
published one, so that diff cannot see them; the indexer records those
explicitly as garbage instead.

#### Reverse dict leaf (`DLR1`)

```text
[magic: 4B "DLR1"]
[entry_count: u32]
[offset_table: u32 × entry_count]
[data section]
  entry := [key_len: u32] [key_bytes] [id: u64 LE]
```

Subject reverse key format is:

```text
[ns_code: u16 BE][suffix bytes]
```

The `u16` big-endian prefix ensures that lexicographic byte comparisons match logical `(ns_code, suffix)` ordering.

## Endianness and encoding conventions

- Numeric fields in file formats are **little-endian**, unless explicitly stated otherwise.
- Subject reverse keys embed `ns_code` in **big-endian** for byte-sort correctness.
- Compression is currently **zstd**, applied independently per column block within a leaflet.
- Fact keys are keyed by numeric IDs; ID assignment is provided by dictionary artifacts and/or the root.

## Integrity, caching, and lifecycle

- Leaf and branch filenames (local) are derived from **SHA-256** content hashes; remote references use ContentId (CIDv1).
- Content-addressed artifacts are immutable; caches can key by ContentId.
- `IndexRoot` (FIR6) provides a GC chain (`prev_index`) and an optional garbage manifest pointer to
  support retention-based cleanup of replaced artifacts.

## Versioning notes

- Fact artifacts:
  - branch: magic `FBR3`, version `1`
  - leaf: magic `FLI3`, version `1`
- Dictionary tree artifacts:
  - branch: magic `DTB1`
  - reverse leaves: magic `DLR1`
  - forward packs: magic `FPK1`
- Small dict blobs: magic `FRD1`

When adding new fields, prefer:

- bumping the per-file `version` byte (when present), and
- keeping old readers strict (fail fast on unsupported versions)
  to avoid silent corruption.

