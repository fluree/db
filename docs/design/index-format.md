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
  - each leaf contains multiple **leaflets** (compressed blocks with independently compressed regions)
- **Shared dictionary artifacts**:
  - small dictionaries (predicates, graphs, datatypes, languages) embedded in the **index root** (CAS) and/or persisted as flat files in local builds
  - large dictionaries (subjects, strings) stored as **CoW single-level B-tree-like trees**
    (a branch manifest `DTB1` + multiple leaf blobs `DLF1`/`DLR1`)
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
  *only what they need* (e.g., Region 1 to filter before paying for Region 2).
- **Content-addressed immutability**: leaves/branches/dict leaves can be cached aggressively
  and safely, because their CAS address (or content hash filename) uniquely identifies content.
- **Simple versioning**: each binary artifact begins with a magic + version and can be rejected
  early if incompatible.

## Terminology

- **Leaflet**: a compressed block of rows (default build target: `leaflet_rows = 25_000`).
- **Leaf**: a container of multiple leaflets (default: `leaflets_per_leaf = 10`) plus a directory for
  random access to its leaflets.
- **Branch manifest**: maps key ranges to leaf files; used for routing.
- **Region**: a separately compressed section inside a leaflet.
- **Dictionary tree**: a `DTB1` branch + `DLF1`/`DLR1` leaves for large keyspaces (subjects/strings).
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
- Tracks `index_t` (max transaction covered) and `base_t` (earliest time for which Region 3 history is valid).
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
  - tree blobs: subject/string forward & reverse (`DTB1` branch + `DLF1`/`DLR1` leaves)
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
[LeafHeader: variable size]
  magic: "FLI3" (4B)
  version: u8          (currently 1)
  order: u8
  dt_width: u8         (currently 1; may widen to 2)
  p_width: u8          (2=u16, 4=u32)
  total_rows: u64
  first_key: SortKey (28B)
  last_key:  SortKey (28B)
  [LeafletDirectory: leaflet_count × 40B]    (v2: 28B, lacks first_o_*)
    offset: u64
    compressed_len: u32
    row_count: u32
    first_s_id: u64
    first_p_id: u32
    first_o_kind: u8   (v3+)
    _pad: [u8; 3]      (v3+)
    first_o_key: u64   (v3+)
[LeafletData: concatenated encoded leaflets]
```

The v3 leaflet directory adds `first_o_kind` and `first_o_key` to each entry.
These fields enable **leaflet-boundary skip-decoding**: if two adjacent leaflet
directory entries share the same `(p_id, o_kind, o_key)`, the entire earlier
leaflet is guaranteed to contain only that `(p, o)` combination. Fast-path
COUNT + GROUP BY operators use this property to count rows by `row_count`
without decompressing Region 1, which significantly reduces CPU and I/O for
large predicate scans. v2 leaves (which lack these fields) are still readable
but always require full leaflet decoding.

### `SortKey` (leaf routing key)

`SortKey` is a compact 28-byte key stored in leaf headers:

```text
g_id: u32
s_id: u64
p_id: u32
dt:  u16
o_kind: u8
_pad: u8
o_key: u64
```

`SortKey` exists to reduce leaf header overhead; the branch manifest uses full `RunRecord` boundaries.
It also intentionally omits `t`, `op`, `lang_id`, and `i` — leaf header keys are useful for coarse
metadata and diagnostics, while precise routing is done via the branch’s full `RunRecord` ranges.

### Why “leaf contains leaflets” (blob-store optimization)

If every leaflet were its own object:

- range scans and joins would issue **many more GETs** (request overhead dominates)
- caches would be pressured by **object metadata overhead** and higher churn

By grouping N leaflets into one leaf object:

- we reduce object count and request rate roughly by a factor of N
- we still keep leaflet-sized “micro-partitions” internally for:
  - selective decompression (region-by-region)
  - caching hot leaflets (decoded) independent of unrelated ones
  - future optimizations like ranged reads (leaflet offsets are explicit)

The default build targets (`leaflet_rows = 25_000`, `leaflets_per_leaf = 10`) yield a leaf that is
large enough to amortize object-store overhead but still small enough to cache and move efficiently.

## Leaflet format (compressed block inside a leaf)

A leaflet is a compressed block of rows containing three regions. Each region is independently zstd-compressed.

### Leaflet header (fixed 61 bytes)

```text
row_count: u32
region1_offset: u32
region1_compressed_len: u32
region1_uncompressed_len: u32
region2_offset: u32
region2_compressed_len: u32
region2_uncompressed_len: u32
region3_offset: u32
region3_compressed_len: u32
region3_uncompressed_len: u32
first_s_id: u64
first_p_id: u32
first_o_kind: u8
first_o_key: u64
```

### Regions

- **Region 1 (core columns)**: order-dependent layout optimized for scan/join filtering.
  - includes an RLE-encoded “primary” column (e.g., `s_id` in SPOT)
  - stores the other core columns as dense arrays
  - `p_id` may be stored as `u16` or `u32` depending on dictionary cardinality (`p_width`)
- **Region 2 (metadata columns)**: values needed to reconstruct full flakes (datatype, transaction time, etc.).
  - stored in a layout that supports sparse `lang_id` and `i` without per-row overhead
  - `dt` is stored as `u8` today (`dt_width = 1`) and may widen to `u16`
- **Region 3 (history journal)**: optional operation log to support time-travel semantics from `base_t` onward.
  - stored as a sequence of fixed-size entries in **reverse chronological order** (newest first)

#### Region 1 layouts (uncompressed)

Region 1’s uncompressed bytes vary by sort order:

- **SPOT**: `RLE(s_id:u64)`, `p_id[p_width]`, `o_kind[u8]`, `o_key[u64]`
- **PSOT**: `RLE(p_id:u32)`, `s_id[u64]`, `o_kind[u8]`, `o_key[u64]`
- **POST**: `RLE(p_id:u32)`, `o_kind[u8]`, `o_key[u64]`, `s_id[u64]`
- **OPST**: `RLE(o_key:u64)`, `p_id[p_width]`, `s_id[u64]`
  - OPST leaflets are **type-homogeneous** (segmented by `o_type`), so the per-row object type
    column can be omitted and stored as a constant in the leaflet directory entry. When a leaflet
    contains mixed types in other orders, `o_type` is stored as a per-row column.

RLE encoding is:

```text
run_count: u32
[(key, run_len)] × run_count
```

with `(key=u64, run_len=u32)` or `(key=u32, run_len=u32)` depending on the field.

#### Region 2 layout (uncompressed)

```text
dt: [dt_width bytes] × row_count
t:  [i64] × row_count
lang_bitmap:  u8 × ceil(row_count/8)
lang_values:  u16 × popcount(lang_bitmap)
i_bitmap:     u8 × ceil(row_count/8)
i_values:     i32 × popcount(i_bitmap)
```

- `lang_id` is 0 when absent; otherwise stored in `lang_values` keyed by bitmap position.
- `i` uses `ListIndex::none()` (sentinel) when absent; otherwise stored sparsely.

#### Region 3 layout (uncompressed)

Region 3 is an operation journal stored newest-first:

```text
entry_count: u32
[Region3Entry; entry_count]    // 37 bytes per entry
```

`Region3Entry` wire layout (37 bytes):

```text
s_id: u64
p_id: u32
o_kind: u8
o_key: u64
t_signed: i64      // positive = assert, negative = retract, abs() = t
dt: u16
lang_id: u16
i: i32
```

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

### Large dictionaries as CoW trees (`DTB1` + leaf blobs)

Subjects and strings are large enough that we represent them as single-level CoW trees:

- **Branch**: `DTB1` mapping key ranges to leaf ContentIds
- **Leaves**:
  - forward leaf (`DLF1`): numeric ID → value bytes
  - reverse leaf (`DLR1`): key bytes → numeric ID

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

#### Forward dict leaf (`DLF1`)

```text
[magic: 4B "DLF1"]
[entry_count: u32]
[offset_table: u32 × entry_count]
[data section]
  entry := [id: u64 LE] [value_len: u32] [value_bytes]
```

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
- Compression is currently **zstd** via independent region compression within a leaflet.
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
  - leaves: magic `DLF1` / `DLR1`
- Small dict blobs: magic `FRD1`

When adding new fields, prefer:

- bumping the per-file `version` byte (when present), and
- keeping old readers strict (fail fast on unsupported versions)
  to avoid silent corruption.

