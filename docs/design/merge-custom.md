# Custom merge commits

Status: design — not yet implemented. Replaces the strategy-only merge surface introduced in `feature/merge` (#1191) and the read-only preview in `feature/merge-preview` (#1204, #1205).

## Goal

Let callers drive a merge through a four-step flow — **preview → query → validate → commit** — using a single declarative `MergePlan` payload. Each step is keyed off the same plan, so a client can iterate on conflict resolutions, run queries against the proposed merge state, validate it (e.g., SHACL), and finally publish a real multi-parent merge commit.

## Non-goals

- Merging a non-HEAD historical commit of the source branch. The plan shape leaves room for it (`source.at`) but v1 always merges HEAD.
- Persistent server-side merge sessions. The flow is stateless; the server may opportunistically cache a staged merge state by deterministic hash but correctness must not depend on cache presence.
- Replacing transact-time SHACL or building a merge-specific validator. Validation reuses the existing SHACL pipeline.
- N-way merges. Always one source, one target.

## Today

Today's `POST /merge/{ledger}` (and `Fluree::merge_branch`) takes a single global `ConflictStrategy` and applies it uniformly. Diverged merges already produce a multi-parent commit. The missing pieces are:

- per-conflict resolutions
- arbitrary plan-level edits (asserted/retracted flakes outside the conflict set)
- a way to query the proposed merge state without committing
- a way to validate (SHACL) the proposed merge state before commit

This proposal extends the existing `/merge` rather than introducing a parallel `/merge-custom` endpoint. Since `/merge` has no external users yet, taking a breaking change is cheap and the unified surface is conceptually cleaner.

## `MergePlan`

```jsonc
{
  "source": {
    "branch": "feature-x",
    "expected": "<commitId>"          // required: staleness guard
    // "at": "<CommitRef>"             // reserved for future use; v1 must be absent
  },
  "target": {
    "branch": "main",
    "expected": "<commitId>"          // required: staleness guard
  },
  "baseStrategy": "abort",            // fallback for conflicts not addressed by `resolutions`
  "resolutions": [                    // optional
    {
      "key": { "subject": "<iri>", "predicate": "<iri>", "graph": "<iri>" },
      "action": "take-source" | "take-target" | "take-both" | "custom",
      "customPatch": {                 // required iff action == "custom"
        "@context": { /* optional, JSON-LD context */ },
        "insert": [ /* JSON-LD assertion */ ],
        "delete": [ /* JSON-LD retraction */ ]
      }
    }
  ],
  "additionalPatch": {                // optional plan-level free-form edits
    "@context": { /* optional, JSON-LD context */ },
    "insert": [],
    "delete": []
  }
}
```

### Field semantics

The `ledger` is intentionally **not** a field of `MergePlan`. The HTTP path (`/merge/{ledger}/...`) is the single source of truth for which ledger this plan operates on. A request body that includes a top-level `ledger` field is rejected with `400 Bad Request` to prevent ambiguity. (The Rust `MergePlan` type likewise has no `ledger` field; the merge engine takes ledger as a separate argument.)

#### `source.expected`, `target.expected`

Canonical `CommitId`s captured by the client at preview time. Every operation that touches branch state (preview with plan, query, validate, commit) checks that the current branch HEADs still match. If either has moved, the operation fails with `409 Conflict` and a structured `StaleHead` payload pointing at the new HEAD.

The `expected` fields are **not** optional in v1. Forcing the client to commit to a specific HEAD pair makes the four-step flow internally consistent — you preview against (S, T), query against (S, T), validate against (S, T), and commit against (S, T). If S or T moves between steps, the client sees it and re-plans.

#### `source.at` (reserved)

Once #1199 ("Branching from historical commits") lands, the `CommitRef` primitive is available for naming non-HEAD commits. A future revision can let `source.at` specify a historical commit to merge from. v1 rejects requests where `source.at` is present.

#### `baseStrategy`

What to do with conflicts not addressed by `resolutions`. One of:

- `take-source` — replace target's value at the conflict key with source's
- `take-target` — keep target's value (merge becomes a no-op for that key)
- `take-both` — union of source's and target's values
- `abort` — fail the operation; conflict must be explicitly resolved

`baseStrategy` is mandatory. There is no implicit default; the client must declare its fallback.

#### `resolutions`

Optional list of per-conflict instructions. Each entry's `key` identifies the conflict at `(subject, predicate, graph)` granularity — the same shape as `merge_preview` reports. The merge engine validates that:

1. Every `resolutions[i].key` corresponds to an actual detected conflict between source and target deltas. Resolutions for non-conflicts are rejected (`400 Bad Request`); use `additionalPatch` for arbitrary edits.
2. No two entries share the same `key`. Duplicates are rejected.
3. Conflicts not named in `resolutions` fall through to `baseStrategy`. If `baseStrategy == "abort"` and any conflict is unresolved, the operation fails.

Actions:

- `take-source` / `take-target` / `take-both` — same semantics as `baseStrategy` but scoped to one conflict key
- `custom` — apply the `customPatch` instead. See [`customPatch` is a raw patch](#custompatch-is-a-raw-patch) below.

There is intentionally no `skip` action. The `take-target` action already covers "no merge-level change at this key," and conflicts not listed in `resolutions` fall through to `baseStrategy`, which gives full control without a redundant action verb.

#### `customPatch` is a raw patch

`customPatch` is a JSON-LD transaction (`insert` / `delete`) **applied on top of** the target's current object set at `(s, p, g)`. It is **not** a declaration of the desired final value at that key.

This means: to express "after the merge, this property should hold values X and Y," the caller must explicitly retract any current target values they want gone:

```jsonc
{
  "key": { "subject": "ex:doc1", "predicate": "ex:tags", "graph": "ex:default" },
  "action": "custom",
  "customPatch": {
    "delete": [
      { "@id": "ex:doc1", "ex:tags": "draft" },
      { "@id": "ex:doc1", "ex:tags": "old-name" }
    ],
    "insert": [
      { "@id": "ex:doc1", "ex:tags": "x" },
      { "@id": "ex:doc1", "ex:tags": "y" }
    ]
  }
}
```

Raw-patch was chosen over a declarative "final object set" form because (a) it composes with the existing transact JSON-LD pipeline with zero new code paths, and (b) declarative final-state form is awkward for typed literals, language tags, and IRIs vs strings, all of which have natural JSON-LD assertion shapes already.

Scope check: every produced flake's `(s, p, g)` must equal the resolution's `key`. A `customPatch` that touches other subjects/predicates/graphs is rejected with `400`. Use `additionalPatch` for cross-key edits.

Future shorthand (not v1): a `customValue` field that takes a JSON-LD object form for the conflict key and the engine computes the implied retract-current + assert-customValue. Worth adding once the UI grows complex enough to want it; not on the critical path for v1.

#### `additionalPatch`

Plan-level asserts/retracts applied on top of the conflict-resolution result. Use cases:

- "Bump a version property to mark this as a merge result"
- "Add an `f:mergedBy <user>` triple"
- Workarounds where resolving conflict A requires editing key B

`additionalPatch` has no scope restrictions. It is applied **after** conflict resolution, so it can override anything the resolutions produced.

#### Composition and cancellation semantics

All plan effects — every conflict resolution and the plan-level `additionalPatch` — are materialized into **one** logical flake set, then committed as a single transaction. The composition rule is:

1. Compile resolution-derived flakes (per the chosen action for each conflict key).
2. Compile `additionalPatch` flakes.
3. Concatenate in that order: `[...resolutionFlakes, ...additionalFlakes]`.
4. Apply transaction-style cancellation: walk the list in order, building a working set keyed by `(s, p, o, g)`. For each flake, if a flake with the same `(s, p, o, g)` already exists in the working set with the **opposite** `op` flag, both flakes annihilate (remove the existing entry, do not add the new one). If a flake with the same `(s, p, o, g)` and same `op` flag exists, dedupe (keep one). Otherwise, add to the working set.
5. The working set is the final transaction.

This guarantees that a `customPatch.delete` of `(s, p, o, g)` followed by an `additionalPatch.insert` of the same `(s, p, o, g)` produces **no** flake — the same outcome a single transaction with both clauses would produce in the existing transact pipeline. Without this step, separately-compiled patches would emit both the assert and the retract as independent flakes, and the resulting commit would be inconsistent.

Order matters when the working set is built (later wins on dedup, opposite-op annihilates), so `additionalPatch` semantically "applies last." Today's transact `stage()` already runs an equivalent cancellation pass over a single transaction's clauses; the merge engine reuses the same primitive over the concatenated list.

#### Patch wire format

Both `customPatch` and `additionalPatch` use **JSON-LD transaction shape** — `{ "@context": ..., "insert": [...], "delete": [...] }` — the same body shape accepted by `/v1/fluree/insert`, `/v1/fluree/upsert`, and `/v1/fluree/update`. This is what every UI and CLI client already produces; reusing it means no new serializer/parser, and the transact module's existing JSON-LD → flake pipeline applies.

Context resolution rules:

- If the patch carries a `@context`, that context is used.
- Otherwise, the ledger's stored default context (if any) is used. (See [Default-context boundary](../../CLAUDE.md) — we explicitly load it for this opt-in path.)
- Otherwise, all IRIs in the patch must be fully expanded.

The merge engine compiles each patch to flakes once at the start of an operation, then operates on flakes throughout. Scope rules differ:

- `customPatch` flakes must all be at the resolution's `key` `(s, p, g)`. See [`customPatch` is a raw patch](#custompatch-is-a-raw-patch) for the rationale.
- `additionalPatch` flakes have no scope restriction — any subject, predicate, or graph.

## The four operations

All four take the same `MergePlan`. They differ in what they return and whether they mutate state.

### 1. Preview — `POST /merge/{ledger}/preview`

Read-only. Returns:

- `ancestor` — common ancestor (`commit_id`, `t`)
- `ahead`, `behind` — commit summaries (newest-first, capped, with truncation flag)
- `currentConflicts` — per-conflict details computed against the current state of both branches
- `resolutionSummary` — per-conflict selected resolution after applying `resolutions` and `baseStrategy` (only present when `resolutionsApplied: true`)
- `patchSummary` — counts of asserts/retracts the plan would produce (no full flake dump; only present when `resolutionsApplied: true`)
- `commitReady: bool` — true only when `resolutionsApplied` and every conflict has a resolution and validation policy is satisfiable
- `resolutionsApplied: bool` — true when `staleness == null` and the plan was applied; false otherwise.
- `submittedResolutions` — the caller's `resolutions` echoed verbatim, present whenever `resolutionsApplied: false` so clients can diff against `currentConflicts`
- `staleness` — null, or `{ source: <newHead>, target: <newHead> }` if either expected HEAD doesn't match

**Preview's stale-head behavior is intentionally soft.** Unlike query/validate/commit (which return `409 Conflict` on stale heads), preview always returns `200 OK` and surfaces staleness in the body. Preview is the UX refresh path — clients call it to discover that heads moved and update their plan; failing it hard would force two round trips for the common case.

When stale, preview **recomputes against current heads** (not the expected ones) so the client sees the live state of the world. `staleness` is non-null, `resolutionsApplied` is `false`, `commitReady` is `false`, and `resolutionSummary` / `patchSummary` are omitted. `currentConflicts` reflects the live conflict set; the caller's submitted `resolutions` are echoed back at the top level under `submittedResolutions` for client-side diffing.

This means "preview a stale plan" is functionally a fresh read with the user's old plan attached for comparison, not a half-applied plan. Clients should treat staleness as "your plan is invalid; here's the current state — recompute and re-issue."

A `MergePlan` with empty `resolutions` and `additionalPatch` is the simple-merge case; preview reports what `baseStrategy` would do globally.

### 2. Query — `POST /merge/{ledger}/query`

Body: `{ plan: MergePlan, query: <Fluree query> }`.

Stages the merge result in memory and runs the query against that staged state. The staged state is:

```
target_committed_db
  ⊕ resolved_merge_patch(plan)
  ⊕ additionalPatch
```

The query body's `from` (or equivalent ledger selector) **must be omitted or equal to the path's `{ledger}` ledger ID**. Cross-ledger queries are not supported here — the staging context is the path-identified target ledger and only that ledger has the merge applied. A query with a non-matching `from` is rejected with `400`.

No commit is produced. The server may cache the staged state by `plan_hash` (see [Plan canonicalization](#plan-canonicalization)) with a short TTL, but correctness must not depend on cache presence.

**Stale-head behavior is hard.** If either expected HEAD doesn't match the current branch HEAD, returns `409 Conflict` with `{ source: <newHead>, target: <newHead> }`. Querying a stale plan would return results that don't reflect either current branch state — silently surfacing those would mislead the caller. The client should call `/preview` to refresh and re-issue.

### 3. Validate — `POST /merge/{ledger}/validate`

Body: `{ plan: MergePlan, validators?: { shacl?: bool, failOnSeverity?: "warning" | "violation" } }`.

Stages the same merge state as `/merge/{ledger}/query` and runs the configured validators against it. v1 supports SHACL only.

Returns:

```jsonc
{
  "valid": false,
  "violations": [
    {
      "severity": "violation",
      "focusNode": "<iri>",
      "path": "<iri>",
      "message": "...",
      "sourceShape": "<iri>",
      "value": "..."
    }
  ]
}
```

Validation is independent of commit. A client may validate, present violations to the user, get them addressed via `additionalPatch` or revised `resolutions`, and then validate again before committing.

Stale-head behavior matches `/query`: `409 Conflict` if either expected HEAD has moved.

### 4. Commit — `POST /merge/{ledger}` (extended)

Today's `/merge` becomes the commit endpoint for the new flow. Body:

```jsonc
{
  "plan": MergePlan,
  "validate": { "shacl": true, "failOnSeverity": "violation" },
  "message": "Merge feature-x into main"
}
```

Behavior:

1. Compute the request hash. Check current HEADs:
   - If `source.expected` doesn't match current source HEAD → `409 Conflict` (source moves are never idempotent for this endpoint, since only target HEAD is mutated here).
   - If `target.expected` doesn't match current target HEAD, load the commit at the current target HEAD and inspect its `f:mergeRequestHash`. If it equals this request's hash → `200 OK` with the original `MergeReport` (idempotent retry success). Otherwise → `409 Conflict` with new HEADs.
   - The idempotency check **must** run before the generic stale rejection, otherwise the documented retry path can never return `200`.
2. Compute resolved merge patch.
3. If `validate` is set, run validators on staged state. If `valid == false` and severity meets threshold → `422 Unprocessable Entity` with violations payload.
4. **Copy source ancestry into the target namespace.** Before publishing the merge commit, copy any commit envelopes from the source's commit chain that are reachable from `source_head` and not already present in the target namespace, stopping at the merge base. This makes the source-parent edge of the merge commit's DAG locally resolvable in the target namespace — push validation, DAG walks, and `CommitRef::T` resolution on the target branch all expect non-prior parents to be locally readable. Today's `general_merge` already does this (the `commits_copied` field in `MergeReport` reflects the count); the new flow inherits that behavior.
   
   This is a copy of envelope blobs only; flake bodies for the copied commits aren't pulled in. The target's `BranchedContentStore` falls back to source for any older history, but the merge-parent envelope must be local for chain validation to succeed.
5. Write a multi-parent merge commit to the target's namespace:
   - `parents = [target_head, source_head]` (target first — primary parent)
   - `flakes = resolved_merge_patch ⊎ additionalPatch`
   - `t = max(source_t, target_t) + 1` (see [Commit `t` semantics](#commit-t-semantics) below)
   - `txn_meta` includes `f:message` if provided, plus the request hash (see [Idempotent commit retry](#idempotent-commit-retry))
6. Advance target HEAD via the nameservice.

If a network failure interrupts the call, the client retries with the same body. See [Idempotent commit retry](#idempotent-commit-retry) for how the server distinguishes a pre-publish drop (target HEAD unchanged → retry runs normally) from a post-publish drop (target HEAD already advanced to the merge commit → return the original `MergeReport`).

## Conflict definition

A conflict at key `(s, p, g)` exists when:

- both source and target deltas (each measured against the merge base) modify the object set at that key, **and**
- the resulting post-delta object sets differ.

"Modify" means at least one assert/retract on `(s, p, g)`. "Differ" means the post-delta object sets are not equal as sets.

**This is a refinement of today's behavior.** The current `merge_preview` code (`fluree-db-api/src/merge_preview.rs`) treats any intersection of source/target delta keys as a conflict — it intersects `compute_delta_keys` outputs without comparing the resulting object sets. Under that rule, two branches that independently assert the exact same triple would be reported as conflicting, which is poor UX and adds noise to plans the user must resolve.

Implementation impact: conflict detection becomes "intersect delta keys, then for each intersected key load source's and target's resolved object sets and report only those that differ." This requires:

- the existing delta-key intersection (already there), plus
- per-conflict-key object-set lookups against the two branch states (one extra range read per candidate key).

The cost is bounded by candidate count, which is already small (only delta-key intersections). For all four operations (preview/query/validate/commit) the conflict detection runs once per call, so this is acceptable.

The merge engine and preview share this refined detector. Updating it is part of the merge-custom implementation, not a separate change.

### Cardinality

Resolutions operate at `(s, p, g)` granularity, replacing the entire object set:

- `take-source` → object set is what source ended up with
- `take-target` → object set is what target ended up with
- `take-both` → union of the two object sets

For finer-grained control (e.g., "keep both my new objects but only one of theirs"), use `custom` with an explicit `customPatch`.

## Commit `t` semantics

The merge commit gets `t = max(source_t, target_t) + 1`.

This breaks today's invariant that `new_t = base.t() + 1` (`fluree-db-transact/src/commit.rs`). Today, every new commit is `target_t + 1`. For a merge commit where `source_t > target_t`, that rule produces a commit whose `t` is **lower** than its source parent's `t` — breaking any "t is monotonically increasing along ancestor paths" assumption (used for stop conditions in DAG walks, and for `CommitRef::T` resolution).

The fix is small in `fluree-db-transact` but it has push/sync ripple effects that are easy to miss. Implementation impact:

**`merge_t` flows end-to-end.** Setting just the commit-header `t` is not enough. Today's `stage()` computes flake `t` values from `base.t() + 1`, and query views over `StagedLedger` expose `staged_t` derived from the same. If the commit header says `max(source_t, target_t) + 1` while the staged flakes still say `target_t + 1`, query/validate during the merge flow will see inconsistent `t`s on the very flakes the commit is about to publish.

The fix threads a single `merge_t` value through:

1. **Merge engine.** Computes `merge_t = max(source_t, target_t) + 1` once when the operation starts.
2. **Patch compilation → flake generation.** Flakes derived from resolutions and `additionalPatch` are stamped with `t = merge_t`. (Today's transact pipeline takes its `t` from a context object; that context gains an explicit override.)
3. **`StagedLedger`.** Construction takes `merge_t` and exposes it via `staged_t()`. Query/validate paths consume it the same way they consume any other staged-transaction `t`.
4. **Commit writer (`fluree-db-transact::commit`).** Grows a `CommitOpts::merge_t: Option<i64>`. When set, skips `base.t() + 1` and uses the supplied value. Monotonicity check (commit `t` must be strictly greater than the latest indexed `t` on the target) still applies. The pre-stamped flakes already match.

This keeps a single source of truth: one `merge_t` computed up front and passed through. The override path doesn't fork the transact pipeline — it just changes one input.

**Push validation (`fluree-db-api/src/commit_transfer.rs`).** Three changes:

*(a) Multi-parent `t` check.* Currently rejects any commit where `commit.t != prev_t + 1`. This is correct for single-parent commits but breaks for multi-parent merge commits (where the prior on this chain is the target parent at `target_t`, but the commit's `t` is `max(target_t, source_t) + 1`). When `commit.parents.len() > 1`, load all parent envelopes from the receiving namespace and require `commit.t == max(parents.map(|p| p.t)) + 1` — same invariant the writer enforces, verified at receive time. Looser "any `commit.t > prev_t`" was considered and rejected: it accepts arbitrary t gaps, drifting away from the design invariant. Single-parent commits keep the existing strict `+1` check (no extra envelope read).

*(b) Verify non-prior parents exist.* Today's parent-hash check only verifies that **at least one** parent matches the prior commit's hash. It doesn't prove the other parent envelopes exist in the receiving namespace. For multi-parent commits, push validation must additionally load every non-prior parent's envelope and reject if any are missing — otherwise the merge commit could land in a namespace where its source-parent edge is unresolvable. The merge engine's pre-publish ancestry copy (commit step 4) is what makes this check satisfiable.

*(c) Allow empty multi-parent commits.* Push currently rejects commits with zero data flakes. A `take-target`/no-op merge — every conflict resolved to `take-target`, no `additionalPatch` — legitimately produces an empty merge commit that records only the DAG join. Relax the empty-commit rule for `parents.len() > 1`: empty multi-parent commits are valid, single-parent empty commits are still rejected.

The same envelope loads cover both (a) and (b), so the cost on multi-parent commits is one round of parent envelope reads regardless.

This is the only site that enforces strict `+1` chain contiguity in the codebase. `pack.rs` and `import.rs` use commit-DAG iteration but do not re-check the `+1` rule — they trust the source's chain validation. No changes needed there.

**`T`-based resolvers (`CommitRef::T`).** Already DAG-aware via #1199. Merge fits cleanly: merge commit's `t` is still unique on the target branch (rebase-induced duplicates are the only ambiguity).

**Tests.** Cover (a) a merge where `source_t > target_t + 1`, push the merge commit's chain to a fresh server, and verify it accepts; (b) the same commit fails with the strict check disabled by feature flag (regression guard).

Alternative considered: `target_t + 1` (target-branch-local contiguous). Rejected because it produces non-monotonic `t` along the source-parent edge of the DAG, which complicates every commit walker. The `+1` writer change plus the contiguity relaxation is a smaller surface than the alternative.

## Validation pipeline

The current SHACL implementation runs as a transact-time validator scoped to **touched nodes** — the subjects whose flakes the transaction modifies. Merge validation reuses that exact scope, not full-database re-validation:

- The "touched node set" for a merge is `{ s | (s, p, o, g) ∈ plan_flakes }` — every subject named in the resolution-derived flakes or `additionalPatch`. This is exactly the same predicate the transact pipeline uses for a single transaction.
- For each touched node, run the SHACL shapes that target it (via `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`, etc.) against the **staged merge state**.
- Shapes whose target nodes weren't touched by the merge are not re-evaluated. Pre-existing violations on untouched data are out of scope; the merge is only responsible for what it changes.

Plan: extract a `validate_candidate(db: &dyn GraphDb, touched: &[Sid], validators: &[Validator]) -> Result<ValidationReport>` service in `fluree-db-shacl` (or a new `fluree-db-validation` shim crate if other validators show up). Both transact and merge call it. Transact passes the transaction's touched-node set; merge passes the plan's touched-node set.

This is consistent with how `fluree-db-transact` validates today and avoids the cost of re-running SHACL over the entire ledger on every merge. If a future use case calls for full-DB validation (rare), it can be added as an explicit `validators.shacl.scope: "full" | "touched"` option without changing the default.

Out of scope for v1: pluggable validators beyond SHACL. The validator list is hardcoded to `[shacl]` (feature-gated). If `shacl` is requested but the feature is disabled, `/merge/{ledger}/validate` returns `501 Not Implemented` with body `{ "error": "validator_unavailable", "validator": "shacl" }`. Silently returning `valid: true` would mask validation failures and is unsafe — the caller asked for validation and didn't get it.

If `validators` is omitted entirely (no validator requested), `/validate` is a no-op and returns `{ valid: true, violations: [] }` regardless of which validators are compiled in. The error case is specifically "asked for X, X is unavailable."

## Branch state precondition

`MergePlan` operates on **committed** branch state — specifically, on the commits identified by `commit_head_id` for source and target. The expected-HEAD pair pins the operation to a precise pair of commit identities; any operation that mutates the staged state (query/validate/commit) checks current HEADs against expected and fails on mismatch.

In this codebase, `Novelty` refers to flakes that are committed but not yet indexed — they are part of the durable commit chain, just not in the columnar index yet. Merge does **not** need to block on novelty in this sense: the operation reads from the commit chain (via the `BranchedContentStore`) and is unaffected by indexing state. A branch with novelty is a normal recently-committed branch and can be merged just like any other.

There is no separate "uncommitted staged transactions" state attached to `LedgerState` itself — staged-but-uncommitted writes live in a `StagedLedger` value scoped to a single transaction. Merges target the published `LedgerState`, so the staged-transaction surface is irrelevant to this contract.

Net: no precondition beyond expected-HEAD matching. The earlier-drafted "block on `LedgerState.novelty.has_unindexed_data()`" rule was based on a misreading of `Novelty` semantics and is removed.

## Plan canonicalization

`/merge/{ledger}/query` may cache staged merge states by `plan_hash`. The hash is derived from the **lexical** plan, not a semantic one — two plans whose JSON-LD patches differ only in `@context` shorthand will hash differently and be treated as separate cache entries. Semantic-equivalence canonicalization for JSON-LD requires context resolution, vocabulary expansion, and namespace-code stability across calls (which the per-ledger namespace allocator does not guarantee mid-flight). Lexical hashing is simpler and correct; the cache cost of occasional shorthand-induced misses is acceptable.

Canonicalization rules:

- `resolutions` is sorted lexicographically by `(key.graph, key.subject, key.predicate)`.
- Inside each patch (`customPatch`, `additionalPatch`), `insert` and `delete` arrays are emitted in their caller-supplied order — JSON-LD assertion order is meaningless for transaction shape but reordering risks breaking arrays that contain `@list` semantics. Callers seeking cache hits across structurally-identical bodies should sort their patch arrays before submission.
- Canonical JSON (sorted keys, no whitespace, RFC 8785) is applied **recursively to every object** in the plan, including each `@context` blob. We don't expand or normalize JSON-LD semantically (no context resolution, no IRI compaction reversal), but we do guarantee that two contexts whose JSON differs only in key ordering or whitespace will hash the same.

`plan_hash = sha256(canonical_json(plan))`. The hash includes `source.expected` and `target.expected`, so a stale plan never collides with a fresh one.

Note: `request_hash` (used for idempotent commit retry) wraps `plan_hash` and is computed the same way over the broader request envelope (see [Idempotent commit retry](#idempotent-commit-retry)).

## Idempotent commit retry

After `/merge/{ledger}` advances target HEAD via the nameservice but before the response reaches the client, the network may drop. The client retries with the same body.

If the network drop happens **before** the nameservice update, target HEAD didn't move; the retry's `target.expected` still matches and the operation runs normally — no idempotency check needed.

If the drop happens **after** the nameservice update, target HEAD has moved to the just-published merge commit. On retry the server compares the request's `request_hash` against the request hash recorded on the current target HEAD's commit. If they match, return `200 OK` with the same `MergeReport`. Otherwise (HEAD moved for a different reason, or matches a different request), return `409 Conflict` with the new HEADs.

`request_hash` covers the whole semantic request — not just the plan — so two distinct calls that happen to share a plan can't be confused for retries of each other:

```
request_hash = sha256(canonical_json({
  plan: <canonical MergePlan>,
  message: <body.message | null>,
  validate: <body.validate | null>,
  principal: <auth identity stable id>
}))
```

`principal` comes from the auth context (typically the JWT `sub` or DID). Including it means a retry from a different identity does **not** match — even if the plan and message are identical, two different users posting the same merge are two different commits at the semantic layer.

The hash is recorded in the merge commit's `txn_meta` as `f:mergeRequestHash`. The check is local; no separate state tracking.

## Authorship

The merge commit's identity (signature/author) comes from the auth context that posted to `/merge/{ledger}`. The `message` field on the request body is copied into the commit's `txn_meta` as `f:message`. v1 does not support overriding author; if needed later, gate it on admin and add a single optional `author` field.

## Auth brackets

- `/merge/{ledger}/preview`, `/merge/{ledger}/query`, `/merge/{ledger}/validate` — read-only, but require **read access to both the source and the target branch**. The staged merge state combines target's committed db with source's contributions; a caller who can read target but not source could otherwise observe source's facts through the staged view. The middleware verifies both `ledger:source_branch` and `ledger:target_branch` are readable under the caller's policy/scope before staging anything.
- `/merge/{ledger}` (commit) — admin-protected, same bracket as today's `/merge`. Admin token implies read of both branches.

## Breaking change to `/merge`

The current request body:

```jsonc
{ "ledger": "mydb", "source": "feature-x", "target": "main", "strategy": "take-source" }
```

becomes:

```jsonc
{
  "plan": {
    "source": { "branch": "feature-x", "expected": "<commitId>" },
    "target": { "branch": "main",      "expected": "<commitId>" },
    "baseStrategy": "take-source"
  }
}
```

The CLI's `fluree merge --strategy take-source` retains its UX: it does a preview internally to capture expected HEADs, then posts the plan. The `--strategy` flag becomes shorthand for `baseStrategy` with no `resolutions`.

**Strategy renames.** Today's strategy values use `take-branch` (meaning "take source branch's value"). The new design uses the more explicit `take-source` / `take-target` pair. Mapping:

| Old (`take-branch` style) | New                |
|---------------------------|--------------------|
| `take-branch`             | `take-source`      |
| `take-target` *(implicit)* | `take-target`     |
| `take-both`               | `take-both`        |
| `abort`                   | `abort`            |

Old names are not accepted in the new request body (request validator rejects with `400` and a hint). CLI's `--strategy` flag accepts the new names; we don't aliasing old names since (a) there's effectively one user (us), and (b) keeping old names alive across the rename is the kind of compat shim CLAUDE.md tells us to avoid.

## CLI surface

- `fluree merge preview <source> [--into <target>] [--plan plan.json]` — runs `/preview`. With `--plan`, uses an explicit plan file; without, generates a default plan (`baseStrategy: abort`, no resolutions) so the user can see conflicts before writing a plan.
- `fluree merge query <source> [--into <target>] [--plan plan.json] -q query.json` — runs `/query`.
- `fluree merge validate <source> [--into <target>] [--plan plan.json]` — runs `/validate`.
- `fluree merge <source> [--into <target>] [--plan plan.json] [--strategy <s>] [--message "..."]` — commits.

Plan files are JSON matching the wire shape. The CLI fills in `expected` HEADs from a fresh nameservice lookup unless they're explicitly set in the file.

## Implementation phasing

1. Spec doc round-trip (this document).
2. Add `MergePlan` types and `validate_candidate` extraction in `fluree-db-api` and `fluree-db-shacl`.
3. Implement preview/query/validate/commit pipeline in `fluree-db-api::merge` (extend the existing module, share conflict detection with `merge_preview`).
4. Wire HTTP routes in `fluree-db-server`. Update the existing `/merge` handler to take the new body. Add `/merge/{ledger}/{preview,query,validate}` routes with appropriate auth brackets.
5. CLI commands in `fluree-db-cli` reusing `--remote` infrastructure.
6. Integration tests covering each acceptance criterion plus stale-head, custom-patch-out-of-scope, missing-resolution + abort, novelty-present-but-not-blocking, idempotent-retry, take-target-empty-commit, source-ancestry-copy, and SHACL-violation paths.
7. Doc updates: `docs/cli/server-integration.md` (mirror the merge-preview/branch contract pattern), `docs/api/endpoints.md`, `docs/cli/branch.md` (cross-link from merge-related sections).

## Acceptance criteria

- Different conflicts can use different actions in one merge.
- A custom `customPatch` resolves a conflict scoped to its `(s, p, g)`.
- Plan-level `additionalPatch` applies on top of resolutions.
- A query against a staged merge runs against `target ⊕ resolved_patch ⊕ additionalPatch` without producing a commit.
- SHACL violations on the staged state are reported by `/validate`.
- A successful commit produces a multi-parent commit with `parents = [target_head, source_head]`.
- A stale `source.expected` or `target.expected` blocks commit (and preview/query/validate signal it).
- Retrying a commit that already succeeded (network drop) returns the original `MergeReport`.
- A branch with unindexed novelty is mergeable (no precondition beyond expected-HEAD matching).
- A `take-target` no-op merge produces a valid empty multi-parent commit.
- Source ancestry envelopes are copied into the target namespace before publish, so push/sync of the merge commit's chain succeeds against a fresh server.
- The implementation is generic to all ledgers; SKOS/OWL rules are application-provided SHACL shapes.

## Open questions

- **Should `/merge/{ledger}/preview` accept GET (current) and POST (with plan body)?** Likely yes — keep GET for the no-plan case (default `baseStrategy: abort`, surfaces conflicts), POST when the plan is non-trivial.
- **Cap on `additionalPatch` size.** Plan transport over HTTP suggests an enforced max body size (e.g., 16MB) with an explicit error rather than letting axum's default kick in.
- **Where exactly does both-branch read auth check live?** The natural place is the route handler before staging starts; needs a small extraction since today's read-auth middleware operates on a single `*ledger` capture, not a (source, target) pair derived from the body.
