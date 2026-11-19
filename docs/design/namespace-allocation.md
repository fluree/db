# Namespace allocation and fallback modes

Fluree encodes IRIs as compact **SIDs**: a `(ns_code, local)` pair where:

- `ns_code` is a `u16` namespace code that identifies an IRI prefix
- `local` is the remaining suffix (bytes) after removing the matched prefix

The database maintains a **namespace table** (`LedgerSnapshot.namespace_codes`: `ns_code -> prefix string`).
That table is embedded in the published index root and is loaded whenever a `LedgerSnapshot` is opened.

This document describes how Fluree chooses a namespace prefix for an IRI, and how it mitigates
datasets that would otherwise allocate an excessive number of distinct namespace prefixes.

## Goals

- **One SID per IRI**: every encoder in a ledger splits an IRI the same way, so the same IRI never
  resolves to two SIDs.
- **Stable behavior across writes**: after importing an “outlier” dataset, subsequent transactions
  should continue using the same fallback rules for *previously unseen* IRIs (e.g. new hosts),
  avoiding regression back to finer-grained splitting.
- **Contain namespace explosion**: avoid allocating one namespace code per highly-specific leaf
  (e.g. splitting on the last `/` for IRIs whose paths are effectively unique).

## Core rule: canonical split, then the exact prefix

Every IRI splits into `(prefix, suffix)` by `canonical_split(iri, mode)`, a pure function of the IRI
and the ledger's split mode — independent of which prefixes are registered. The prefix is then
looked up (or allocated) **exactly**; there is no longest-match against other registered prefixes.
An IRI whose prefix has no code, and none can be allocated, is `Sid(OVERFLOW, iri)` once the table
is full.

Implementation: `fluree-db-core/src/ns_encoding.rs` (`canonical_split`, `encode_registered`);
`fluree-db-transact/src/namespace.rs`:

- `NamespaceRegistry::sid_for_iri` (transactions, serial paths)
- `SharedNamespaceAllocator::sid_for_iri` (parallel bulk import)

## Split modes

`mode` is an **`NsSplitMode`** — a **ledger-level property**, not a per-call flag.

```rust
pub enum NsSplitMode {
    #[default]
    MostGranular,      // what a ledger declaring no mode splits under
    HostPlusN(u8),     // scheme://host/ plus n additional path segments
    NamedLocal,        // MostGranular, but no local name is empty; new ledgers
}
```

- **`MostGranular`**: split at the last `/` or `#` for hierarchical IRIs, or the last
  `/ | # | :` for opaque IRIs. An IRI that ends in one of those is the whole prefix of a namespace of
  its own, with an empty local name.
- **`HostPlusN(n)`**: split at `scheme://host/` plus up to *n* additional non-empty path segments.
  For opaque IRIs, split at `scheme:` plus the first segment plus up to *n* further
  colon-delimited segments.
  - `HostPlusN(0)` ≈ host-only splitting
  - `HostPlusN(1)` ≈ host + one path segment
- **`NamedLocal`** (`NsSplitMode::FOR_NEW_LEDGERS`): as `MostGranular`, except that an IRI ending in
  a delimiter splits at the delimiter before it, so no local name is empty:

  | IRI | `MostGranular` | `NamedLocal` |
  |---|---|---|
  | `http://ex.com/v7/Offer12` | `http://ex.com/v7/` + `Offer12` | same |
  | `http://ex.com/v7/Offer12/` | `http://ex.com/v7/Offer12/` + (empty) | `http://ex.com/v7/` + `Offer12/` |
  | `http://ex.com/onto#` | `http://ex.com/onto#` + (empty) | `http://ex.com/` + `onto#` |
  | `http://ex.com/` | `http://ex.com/` + (empty) | `http://` + `ex.com/` |
  | `urn:a:b:` | `urn:a:b:` + (empty) | `urn:a:` + `b:` |

  A built-in namespace's own IRI (`http://www.w3.org/2001/XMLSchema#`) keeps its code, and an IRI
  with no earlier delimiter (`urn:`) keeps the last one.

The mode persists as a single byte, in the first commit's envelope and in every index root:
`0x00` = `MostGranular`, `0x01..=0xFE` = `HostPlusN(n-1)`, `0xFF` = `NamedLocal`.
`HOST_PLUS_N_MAX = 253`. Byte `0xFF` once meant `HostPlusN(254)`; no release wrote a `HostPlusN`
above 1, which is the only value the bulk-import preflight selects.

Implementation: `fluree-db-core/src/ns_encoding.rs` (`NsSplitMode`, `canonical_split`).
The registry reads it via `LedgerSnapshot::ns_split_mode()` and applies it in
`fluree-db-transact/src/namespace.rs`.

## Why new ledgers split under `NamedLocal`

Under `MostGranular`, a page address per entity (`…/dataFromVendor7/Offer12/`, the value of
`bsbm:offerWebpage`) is a namespace per entity. In BSBM 100M those took about 54,200 of the 65,521
user codes during import, so the entities that share real prefixes — producers, vendors, rating
sites — found the table full: 95.6% of subjects were stored under `OVERFLOW`, each with its whole
IRI as its name (#2135). Without the page-address prefixes, the dataset has about 17,300 prefixes,
which fit.

A new ledger takes `NsSplitMode::FOR_NEW_LEDGERS`: `LedgerSnapshot::genesis` and
`NamespaceRegistry::new` (a bulk import's registry) start under it, and the ledger's first commit
declares the mode it was written under, as every first commit has since 4.0. An existing ledger
keeps the mode its first commit declared, whether it is loaded from its commits or from an index
root. A snapshot that stands in for an existing ledger's (inline SHACL shapes) takes that ledger's
mode (`LedgerSnapshot::genesis_with_split_mode`).

## Bulk import: streaming preflight + dynamic mitigation

For large Turtle streaming imports, Fluree attempts to detect “namespace explosion” early without
an extra I/O pass:

1. `StreamingTurtleReader` samples bounded byte windows (`NS_PREFLIGHT_WINDOW_SIZE`, 8 MiB) within
   the first chunk region and counts distinct prefixes under the `NamedLocal` split: the preflight
   chooses only a new ledger's mode, and a new ledger splits under `NamedLocal`.
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
  The remedy is to move the high-cardinality part into the *suffix*
  (`urn:example:record:<id>-rev-<hash>`), so the whole family shares one prefix. There is no
  user-facing knob for the split mode — a new ledger takes `NamedLocal`, `HostPlusN(n)` is selected
  only by the bulk-import preflight above (`fluree-db-api/src/import.rs`), and the mode is immutable
  once the ledger's genesis commit is written, so it cannot be changed on an existing ledger.
  `NamedLocal` does not help this shape: its last segment is not empty.

