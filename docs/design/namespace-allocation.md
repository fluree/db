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

Fluree uses a small set of fallback “splitters” that derive `(prefix, local)` for IRIs that do not
match any known prefix.

The active fallback behavior is represented by `NsFallbackMode`:

- `LastSlashOrHash` (default): split on the last `/` or `#` (prefix is inclusive)
- `CoarseHeuristic` (outlier mitigation):
  - http(s): usually `scheme://host/<seg1>/`
  - special-case: DBLP-style `.../pid/<digits>/` buckets may keep 2 segments
  - non-http(s) with `:` but no `/` or `#`: split at the **2nd** `:` when present (e.g. `urn:isbn:`),
    else the 1st `:`
- `HostOnly` (“fallback to the fallback”):
  - http(s): `scheme://host/`
  - non-http(s) with `:` but no `/` or `#`: split at the **1st** `:`
  - else: last-slash-or-hash

Implementation: `fluree-db-transact/src/namespace.rs`

## Bulk import: streaming preflight + dynamic mitigation

For large Turtle streaming imports, Fluree attempts to detect “namespace explosion” early without
an extra I/O pass:

1. `StreamingTurtleReader` samples bounded byte windows within the first chunk region and counts
   distinct prefixes under `LastSlashOrHash`.
2. If the sample exceeds a budget (`NS_PREFLIGHT_BUDGET`, currently 255), the reader publishes a
   preflight result recommending mitigation.
3. The import forwarder enables `CoarseHeuristic` on the shared allocator **before parsing begins**
   (so the earliest allocations are already coarse).
4. If allocations under `CoarseHeuristic` still grow beyond the u8-ish threshold (>255), the shared
   allocator switches to `HostOnly` so new, unseen hosts do not allocate deeper-than-host namespaces.

Implementation:

- Preflight detector: `fluree-graph-turtle/src/splitter.rs`
- Policy application: `fluree-db-api/src/import.rs`
- Runtime switch: `SharedNamespaceAllocator::get_or_allocate` in `fluree-db-transact/src/namespace.rs`

## Transactions after import: preventing regression for unseen IRIs

Bulk import can upgrade fallback behavior at runtime (shared allocator). For subsequent **normal
transactions**, we also need “outlier mode” to persist so new IRIs do not regress to `LastSlashOrHash`.

Fluree derives this from the DB’s namespace table at open time:

- When a `LedgerSnapshot` is opened, `NamespaceRegistry::from_db(db)` loads `db.namespace_codes`.
- If the DB has already allocated namespace codes beyond the u8-ish threshold (>255), the registry
  sets its fallback mode to `HostOnly`.

That means a new IRI like:

`http://some-unseen-host/blah/123/456`

will allocate (if needed) at:

`http://some-unseen-host/`

instead of falling back to a finer last-slash split.

Implementation: `NamespaceRegistry::from_db` and `NamespaceRegistry::sid_for_iri` in
`fluree-db-transact/src/namespace.rs`

## Notes and trade-offs

- `HostOnly` can still result in many namespaces if a dataset genuinely contains many distinct hosts
  (one per host), but it prevents deeper fragmentation that is common in path-heavy IRIs.
- The `OVERFLOW` namespace code is a sentinel used when `u16` codes are exhausted; it is not a
  fallback mode. Overflow SIDs store the **full IRI** as the SID name.

