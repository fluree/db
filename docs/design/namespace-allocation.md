# Namespace allocation and fallback modes

Fluree encodes IRIs as compact **SIDs**: a `(ns_code, local)` pair where:

- `ns_code` is a `u16` namespace code that identifies an IRI prefix
- `local` is the remaining suffix (bytes) after removing the matched prefix

The database maintains a **namespace table** (`LedgerSnapshot.namespace_codes`: `ns_code -> prefix string`).
That table is embedded in the published index root and is loaded whenever a `LedgerSnapshot` is opened.

This document describes how Fluree chooses a namespace prefix for an IRI, and how it mitigates
datasets that would otherwise allocate an excessive number of distinct namespace prefixes.

## Goals

- **Keep declared namespaces intact**: if a dataset declares `@prefix foo: <...>`, we want IRIs in
  that namespace to use that exact prefix, not a derived/split prefix.
- **Stable behavior across writes**: after importing an “outlier” dataset, subsequent transactions
  should continue using the same fallback rules for *previously unseen* IRIs (e.g. new hosts),
  avoiding regression back to finer-grained splitting.
- **Contain namespace explosion**: avoid allocating one namespace code per highly-specific leaf
  (e.g. splitting on the last `/` for IRIs whose paths are effectively unique).

## Core rule: declared-prefix trie match wins

Namespace resolution is **trie-first**:

1. Load all known prefixes (predefined defaults + DB namespace table) into a byte-level trie.
2. For each IRI, perform a **longest-prefix match**.
3. If a match is found, emit `Sid(ns_code, iri[prefix_len..])` and do **not** run fallback logic.

Only IRIs with **no** matching prefix fall through to the fallback splitter.

Implementation: `fluree-db-transact/src/namespace.rs`

- `NamespaceRegistry::sid_for_iri` (transactions, serial paths)
- `SharedNamespaceAllocator::sid_for_iri` (parallel bulk import)

## Fallback split modes (only for unmatched IRIs)

For IRIs that match no known prefix, the split is derived by `canonical_split(iri, mode)`, where
`mode` is an **`NsSplitMode`** — a **ledger-level property**, not a per-call flag.

```rust
pub enum NsSplitMode {
    #[default]
    MostGranular,      // finest-grained; the default for new ledgers
    HostPlusN(u8),     // scheme://host/ plus n additional path segments
}
```

- **`MostGranular`** (default): split at the last `/` or `#` for hierarchical IRIs, or the last
  `/ | # | :` for opaque IRIs.
- **`HostPlusN(n)`**: split at `scheme://host/` plus up to *n* additional non-empty path segments.
  For opaque IRIs, split at `scheme:` plus the first segment plus up to *n* further
  colon-delimited segments.
  - `HostPlusN(0)` ≈ host-only splitting
  - `HostPlusN(1)` ≈ host + one path segment

The mode persists as a single byte on the ledger: `0x00` = `MostGranular`,
`0x01..=0xFF` = `HostPlusN(n-1)`. `HOST_PLUS_N_MAX = 254`, because `HostPlusN(255)` would wrap to
`MostGranular` on decode.

Implementation: `fluree-db-core/src/ns_encoding.rs` (`NsSplitMode`, `canonical_split`).
The registry reads it via `LedgerSnapshot::ns_split_mode()` and applies it in
`fluree-db-transact/src/namespace.rs`.

## Bulk import: streaming preflight + dynamic mitigation

For large Turtle streaming imports, Fluree attempts to detect “namespace explosion” early without
an extra I/O pass:

1. `StreamingTurtleReader` samples bounded byte windows (`NS_PREFLIGHT_WINDOW_SIZE`, 8 MiB) within
   the first chunk region and counts distinct prefixes under the default `MostGranular` split.
2. If the sample exceeds `NS_PREFLIGHT_BUDGET` (currently 255), the reader publishes a
   `NamespaceSuggestion::CoarseHeuristic` preflight result recommending mitigation.
3. The import forwarder sets `NsSplitMode::HostPlusN(1)` on the shared allocator **before parsing
   begins**, so the earliest allocations are already coarse.

Implementation:

- Preflight detector: `fluree-graph-turtle/src/splitter.rs`
- Policy application: `fluree-db-api/src/import.rs`
- Shared allocator: `SharedNamespaceAllocator::set_split_mode` in `fluree-db-transact/src/namespace.rs`

> Note: `NamespaceSuggestion::CoarseHeuristic` is an *import-side signal*, not a split mode. It is
> the preflight's recommendation; the mode it selects is `HostPlusN(1)`.

## The mode is a ledger property, not a per-open derivation

The split mode chosen at import time persists as part of the ledger, so subsequent **normal
transactions** keep splitting unmatched IRIs the same way. There is no re-derivation from the size
of the namespace table and no runtime escalation between modes.

- `LedgerSnapshot::ns_split_mode()` returns the persisted mode.
- `NamespaceRegistry::from_db(snapshot)` seeds the registry with it.
- `LedgerSnapshot::set_ns_split_mode(mode, commit_t)` is **immutable after user namespace
  allocation**: if user namespaces already exist under a different mode, it returns an error
  (`ns_split_mode conflict: commit t=… declares … but ledger already has user namespaces under …`)
  rather than silently re-splitting.

That immutability is the point — a ledger whose IRIs were encoded under one split cannot have later
IRIs encoded under another, or the same logical IRI would resolve to two different SIDs.

So for a ledger imported under `HostPlusN(0)`, a later unseen IRI:

`http://some-unseen-host/blah/123/456`

allocates (if needed) at:

`http://some-unseen-host/`

Implementation: `NamespaceRegistry::from_db` and `NamespaceRegistry::sid_for_iri` in
`fluree-db-transact/src/namespace.rs`; `set_ns_split_mode` in `fluree-db-core/src/db.rs`

## Notes and trade-offs

- `HostPlusN(0)` can still result in many namespaces if a dataset genuinely contains many distinct hosts
  (one per host), but it prevents deeper fragmentation that is common in path-heavy IRIs.
- The `OVERFLOW` namespace code is a sentinel used when `u16` codes are exhausted; it is not a
  fallback mode. Overflow SIDs store the **full IRI** as the SID name.
- **High-cardinality trailing segments allocate one namespace per subject.** Under `MostGranular`,
  an opaque IRI splits at its *last* `/`, `#` or `:`, so a shape like
  `urn:example:record:<id>:rev:<hash>` yields the prefix `urn:example:record:<id>:rev:` — unique to
  that one subject. A dataset built this way allocates a namespace code per node rather than per
  family, which has two consequences: the user code space (`USER_START`..`OVERFLOW`, ~65.5k codes)
  can be exhausted by a few hundred thousand subjects, and per-transaction work that keys off
  namespace codes degrades. The streaming preflight above only covers **bulk import** — ordinary
  transactions get no such detection, so this is a data-modelling concern for write workloads.
  Prefer moving the high-cardinality part into the *suffix* (`urn:example:record:<id>-rev-<hash>`)
  or setting `HostPlusN(n)` on the ledger.

