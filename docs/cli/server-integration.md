# Implementing Server Support For Fluree CLI

This document is for implementers building a custom server (for example in `../solo3/`) that wants to support the Fluree CLI end-to-end.

The CLI supports two broad categories of remote operations:

- **Data API**: query/update/insert/upsert/info/exists/show (normal ledger operations).
- **Replication / sync**: clone/pull/fetch (content-addressed replication by CID, via pack + storage proxy).

## Base URL And Discovery

The CLI prefers to be configured with a server origin URL (scheme/host/port) and then uses discovery:

- `GET /.well-known/fluree.json` returns `api_base_url` (usually `/v1/fluree`)

The CLI stores the discovered base as the remote's `base_url` and constructs all other endpoints relative to it.

If you do not implement discovery, users must configure the CLI remote URL to already include the API base (for example `http://localhost:8090/v1`), and the CLI will append `/fluree` as needed.

## Minimum Endpoints By CLI Feature

### `fluree remote add`, `fluree auth login`

- `GET /.well-known/fluree.json`

### `fluree fetch` (nameservice refs only)

- `GET {api_base_url}/nameservice/snapshot`
- `POST {api_base_url}/nameservice/refs/:ledger-id/commit`
- `POST {api_base_url}/nameservice/refs/:ledger-id/index`
- `POST {api_base_url}/nameservice/refs/:ledger-id/init`

### `fluree clone`, `fluree pull` (pack-first replication)

Required:

- `GET {api_base_url}/info/*ledger` (existence + remote `t` preflight; see `/info` minimum fields below)
- `GET {api_base_url}/storage/ns/:ledger-id` (remote NsRecord, includes `commit_head_id`, optional `index_head_id`, and optional `config_id`)
- `POST {api_base_url}/pack/*ledger` (binary `fluree-pack-v1` stream)

The CLI sends pack requests with **index artifacts** by default (`include_indexes: true`, `want_index_root_id` from the NsRecord) when the remote advertises an `index_head_id`. Use `--no-indexes` on clone/pull to request commits and txns only. Use `--no-txns` on clone to request commits without original transaction payloads (the commit chain still transfers and remains verifiable). Servers that support pack MUST honor the following request fields:

- `include_indexes: bool` — when `false`, skip index artifact frames.
- `include_txns: bool` — when `false`, skip transaction blob frames. Commits are still streamed; the server must decode each commit's envelope and simply omit the referenced `txn` blob from the stream. The emitted `PackHeader.capabilities` should reflect this (drop `"txns"` from the list).

Servers that support pack should support all combinations of these flags.

Fallbacks (strongly recommended):

- `GET {api_base_url}/commits/*ledger` (paginated export of commit + txn blobs)
- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id` (per-object fetch by CID)

### `fluree push` (commit ingestion)

- `POST {api_base_url}/push/*ledger`

This is not storage-proxy replication; it is a transaction operation and should be authorized like normal transactions.

The CLI sends an `Idempotency-Key` header derived from the pushed commit bytes so servers can safely replay a successful push result if the client retries after a timeout.

### `fluree show --remote`

- `GET {api_base_url}/show/*ledger?commit=<ref>`

The `commit` query parameter accepts the same identifiers as the local `fluree show` command: `t:<N>` for transaction number, hex-digest prefix (min 6 chars), or full CID.

**Policy filtering:** The returned flakes are filtered by the caller's data-auth identity (extracted from the Bearer token) and the server's configured `default_policy_class`. When neither is present, all flakes are returned (root/admin access). Flakes the caller cannot read are silently omitted — the `asserts` and `retracts` counts reflect only the visible flakes. Unlike the query endpoints, show does not accept per-request policy overrides via headers or request body.

**Response:** A JSON object with fields: `id`, `t`, `time`, `size`, `previous`, `signer`, `asserts`, `retracts`, `@context`, `flakes`. Each flake is a tuple: `[subject, predicate, object, datatype, operation]`.

**Error responses:**
- `400 Bad Request` — missing or invalid `commit` parameter
- `404 Not Found` — ledger or commit not found
- `501 Not Implemented` — proxy storage mode (no local index available for decoding)

### `fluree publish <remote> [ledger]` (create + push)

Creates a ledger on the remote and pushes all local commits in a single operation.

Required endpoints:

- `GET {api_base_url}/exists/*ledger` (check if ledger already exists)
- `POST {api_base_url}/create` (create empty ledger if not exists)
- `GET {api_base_url}/info/*ledger` (check remote head when ledger exists)
- `POST {api_base_url}/push/*ledger` (push all commits)

**Workflow:**

1. CLI calls `GET /exists?ledger=mydb:main`
2. If `exists: false`, CLI calls `POST /create` with `{"ledger": "mydb:main"}`
3. If `exists: true`, CLI calls `GET /info/mydb:main` and rejects if `t > 0` (remote already has data)
4. CLI walks the full local commit chain (oldest → newest) and sends all commits via `POST /push/mydb:main`
5. CLI configures upstream tracking locally

The `--remote-name` flag allows publishing under a different name on the remote (e.g., `fluree publish origin mydb --remote-name production-db`).

### `fluree create <name> --from <file>.flpack` (native ledger import)

- No server endpoint required (local-only operation)

Imports a `.flpack` file (native ledger pack) into a new local ledger. The `.flpack` format uses the same `fluree-pack-v1` wire format as `POST /pack`. See [Ledger portability](#ledger-portability-flpack-files) below.

### `fluree export --format ledger` (native ledger export)

- No server endpoint required (local-only operation)

Exports a full local ledger (all commits, indexes, dictionaries) as a `.flpack` file. See [Ledger portability](#ledger-portability-flpack-files) below.

### `fluree query`, `fluree insert`, `fluree upsert`, `fluree update`, `fluree track`, `fluree info`, `fluree exists`

- `POST {api_base_url}/query/*ledger`
- `POST {api_base_url}/insert/*ledger`
- `POST {api_base_url}/upsert/*ledger`
- `POST {api_base_url}/update/*ledger`
- `GET {api_base_url}/info/*ledger`
- `GET {api_base_url}/exists/*ledger`

When the CLI is invoked with policy flags (`--as`, `--policy-class`,
`--policy`, `--policy-file`, `--policy-values`, `--policy-values-file`,
`--default-allow`), it carries them on every data API request via the headers
listed below and, for JSON-LD bodies, also injects them into `opts`. To be
CLI-compatible, your server must implement the contract in
[Policy Enforcement Contract](#policy-enforcement-contract).

### `fluree branch list` (read-only)

- `GET {api_base_url}/branch/{ledger}` — note **singular** `branch`, ledger is a
  greedy tail segment (`*ledger` in axum), so `mydb` and `org/mydb` both work.

Returns all non-retracted branches for the ledger. Same auth bracket as other
read endpoints (`GET /branch/*ledger` enforces Bearer when
`data_auth.mode == required` and `can_read(ledger)`; returns `404` not `403`
when the bearer cannot read it). See
[Branch List Contract](#branch-list-contract).

### `fluree branch create --remote <name>` (admin-protected)

- `POST {api_base_url}/branch` with `{ ledger, branch, source? }`

Same admin auth bracket as `/create`, `/drop`, `/reindex`. See
[Branch Create Contract](#branch-create-contract).

### `fluree branch drop --remote <name>` (admin-protected)

- `POST {api_base_url}/drop-branch` with `{ ledger, branch }`

Same admin auth bracket as `/create`, `/drop`, `/reindex`. See
[Branch Drop Contract](#branch-drop-contract).

### `fluree branch rebase --remote <name>` (admin-protected)

- `POST {api_base_url}/rebase` with `{ ledger, branch, strategy? }`

Same admin auth bracket as `/create`, `/drop`, `/reindex`. See
[Rebase Contract](#rebase-contract).

### `fluree branch merge --remote <name>` (admin-protected)

- `POST {api_base_url}/merge/*ledger` with `{ "plan": MergePlan }`

Same admin auth bracket as `/create`, `/drop`, `/reindex`. The CLI looks up
source and target heads via `/info/{ledger}:{branch}` first and pins them in
`plan.source.expected` / `plan.target.expected` so the server's CAS-protected
publish can detect a concurrent advance and return `409 Conflict`. See
[Merge Contract](#merge-contract) for the full plan shape, status codes, and
error mapping.

### `fluree branch diff` (read-only merge preview)

- `GET {api_base_url}/merge-preview/*ledger?source=&target=&max_commits=&max_conflict_keys=&include_conflicts=`

Returns the rich diff between two branches — ahead/behind commit summaries,
common ancestor, conflict keys, fast-forward eligibility — without mutating
any nameservice or content-store state. See
[Merge Preview Contract](#merge-preview-contract) for the full semantic and
response-shape spec.

## Policy Enforcement Contract

CLI policy flags ride on every data API request as both HTTP headers and (for
JSON-LD bodies) body-level `opts` fields. Servers wanting full CLI parity must
honor both transports and apply the **root-impersonation gate** described
below.

### Headers the CLI may send

| Header | CLI flag | Type | Notes |
|---|---|---|---|
| `fluree-identity` | `--as <iri>` | string | Identity IRI to execute as. |
| `fluree-policy-class` | `--policy-class <iri>` | string, repeatable | Send one header per class, OR a single header with comma-separated IRIs. Both forms must accumulate into a single list. |
| `fluree-policy` | `--policy <json>` / `--policy-file` | JSON string | Inline JSON-LD policy document(s). Reject with `400` on parse failure. |
| `fluree-policy-values` | `--policy-values <json>` / `--policy-values-file` | JSON object string | Variable bindings for parameterized policies (keys begin with `?$`). Reject with `400` on parse failure or non-object value. |
| `fluree-default-allow` | `--default-allow` | `"true"` (presence-truthy) | Permit access when no matching policy rules exist. |

For JSON-LD requests (`POST /query/*`, `POST /insert/*`, `POST /upsert/*`,
`POST /update/*` with `Content-Type: application/json`), the CLI **also**
injects each field into the request body's `opts` object using the same names
(`opts.identity`, `opts.policy-class` as a JSON array, `opts.policy`,
`opts.policy-values` as an object, `opts.default-allow` as a bool). Servers
should treat header values as defaults that body values override.

For SPARQL requests (`Content-Type: application/sparql-query`,
`application/sparql-update`), headers are the only transport — the SPARQL body
has no opts block.

### Required server behavior

1. **Build a `PolicyContext`** from the merged opts (header defaults + body
   overrides) and apply it to every query and transaction execution path.
   Without policy fields the request runs under root (no enforcement). With
   any policy field, the policies must be enforced — including for unsigned
   bearer-only transactions, which historically bypassed enforcement.

2. **Force the bearer's identity into `opts.identity`** by default (the
   bearer is the authenticated principal; clients cannot spoof identity by
   setting `opts.identity`). The exception is the impersonation gate below.

3. **Implement the impersonation gate** for JSON-LD `opts.identity`,
   `opts.policy-class`, `opts.policy`, and `opts.policy-values`, plus the
   `fluree-identity` header on SPARQL requests:

   - Resolve the bearer's identity in the target ledger's policy graph.
   - If the lookup returns "subject exists with no `f:policyClass`"
     (the `FoundNoPolicies` outcome — the bearer is unrestricted on this
     ledger), respect the client-supplied identity / policy fields.
   - If the lookup returns "subject has `f:policyClass` assignments"
     (`FoundWithPolicies`) **or** "subject not found" (`NotFound`), force the
     bearer identity into `opts.identity` and ignore the client-supplied
     policy fields — the request runs under the bearer's own policies.
   - `opts.default-allow` is **not** an impersonation field — it only governs
     the absence of matching rules and should not trigger the gate's lookup.

4. **Audit-log impersonations**. When the gate honors a client-supplied
   identity, log at `info` level with the bearer, target, and ledger:

   ```
   policy impersonation: bearer=<bearer-id> target=<as-iri> ledger=<name>
   ```

5. **Set commit `author` to the impersonated identity** for write operations.
   The original bearer is captured in the audit log; the commit's author
   field tracks who the operation was executed *as*.

6. **In proxy/forwarding mode**, defer the gate to the upstream server:
   forward the request as-is and let the upstream resolve the gate against
   its own ledger state.

### Reference behavior

The Fluree reference server implements the gate via
`fluree_db_api::identity_has_no_policies(snapshot, overlay, t, identity_iri)`,
which wraps the three-state `IdentityLookupResult` enum and returns `true`
only for `FoundNoPolicies`. Source: `fluree-db-api/src/policy_builder.rs`.
The route-level wiring (header merge, gate, force-override, audit log,
PolicyContext construction) lives in
`fluree-db-server/src/routes/policy_auth.rs` — useful as a concrete
implementation reference if you're porting the contract to another server.

## Merge Preview Contract

`fluree branch diff` issues a single read-only request:

```
GET {api_base_url}/merge-preview/{ledger}?source={source}&target={target}
   &max_commits={n}&max_conflict_keys={n}&include_conflicts={bool}
   &include_conflict_details={bool}&strategy={strategy}
```

| Parameter | Type | Required | Server default | Description |
|-----------|------|----------|----------------|-------------|
| `ledger` (path) | string | Yes | — | Ledger name without branch suffix |
| `source` | string | Yes | — | Source branch to merge **from** |
| `target` | string | No | source's parent branch | Target branch to merge **into** |
| `max_commits` | integer | No | `500` | Per-side cap on `ahead.commits` / `behind.commits` |
| `max_conflict_keys` | integer | No | `200` | Cap on `conflicts.keys` |
| `include_conflicts` | bool | No | `true` | When `false`, the conflict computation is skipped |
| `include_conflict_details` | bool | No | `false` | When `true`, include source/target flake values for the returned conflict keys |
| `strategy` | string | No | `take-both` | Strategy used for resolution labels in `conflicts.details[].resolution`; one of `take-both`, `abort`, `take-source`, `take-branch` |

Auth follows the same pattern as `GET /branch/*ledger` (read-only): require
a Bearer when `data_auth.mode == required`; gate on `can_read(ledger)`;
return `404` (not `403`) when the bearer cannot read it.

### Required semantics

These rules are not negotiable; the CLI and other clients depend on them:

1. **Source resolution.** `source` must be a branch — its nameservice record
   must have `source_branch != null`. Otherwise respond `400` with a message
   containing `"no source branch"` so the CLI's error matcher works.
2. **Target defaulting.** When `target` is omitted, resolve to
   `source.source_branch`.
3. **Self-merge.** If `source == resolved_target`, respond `400` with a
   message containing `"itself"`.
4. **Cross-branch ancestor lookup.** `ancestor` is the most recent common
   commit between `source` HEAD and `target` HEAD. The walk **must** be able
   to load commit envelopes from both branches' namespaces — sibling
   branches off `main` must work. The reference implementation builds a
   union view that fans out through both `BranchedContentStore` ancestries;
   equivalents are fine.
5. **Fast-forward predicate.**
   `fast_forward = (ancestor.commit_id == target_head)` when both heads
   exist; `true` when both heads are absent; `false` otherwise.
6. **Per-side walks.** `ahead.count` is the total number of commits on
   `source` since `ancestor.t` (uncapped). `ahead.commits` is the same set,
   capped at `max_commits`, **strictly newest-first by `t`**.
   `truncated = count > commits.len()`. Same shape for `behind`.
7. **Conflict computation.** When
   `include_conflicts == true && !fast_forward` and both heads exist:
   - Walk both deltas: `(s, p, g)` tuples touched on each side since
     `ancestor.t`.
   - `conflicts.keys` is the intersection.
   - **Sort the intersection before truncating** — `HashSet::intersection`
     order is unspecified, and stable ordering matters for paginated UIs.
     Lexicographic by `(s, p, g)` is fine; what matters is that two
     requests against the same state return the same prefix.
   - `count` is the unbounded intersection size; `truncated = count > cap`.
8. **Conflict details.** When `include_conflict_details == true`, populate
   `conflicts.details` for the keys returned in `conflicts.keys` after
   truncation. Each detail includes `key`, `source_values`, `target_values`,
   and a `resolution` annotation for the requested `strategy`. The values are
   the current asserted values for that key at each branch HEAD; preview must
   not apply the strategy. Use the same
   resolved flake tuple shape as `/show` (`[s, p, o, dt, op]`, optional
   metadata as a 6th item).
9. **No mutations.** Implementations must not write to the nameservice,
   advance any HEAD, copy commits between namespaces, or update any cache
   that downstream operations depend on.
10. **Server-side cap is mandatory.** Even if a client sends
   `max_commits=10000000`, clamp to a defensive limit. The reference
   server applies two layers: when no query param is present, it falls
   back to the recommended defaults (`500` for commits, `200` for
   conflict keys); when a param **is** present, the server clamps the
   caller's value with `min(value, hard_max)` where the reference hard
   maxes are `5_000` for commits and `5_000` for conflict keys
   (constants `MERGE_PREVIEW_HARD_MAX_COMMITS` and
   `MERGE_PREVIEW_HARD_MAX_CONFLICT_KEYS` in
   `fluree-db-server/src/routes/ledger.rs`). The CLI assumes the server
   enforces a cap, and unbounded responses must not be reachable over
   HTTP regardless of what the client requests.

   **Scope of the cap.** This bounds the **size of the returned lists**
   and the per-summary `load_commit_by_id` reads (one full commit blob
   per summary). It does *not* bound the underlying divergence walk:
   `count` on each side reflects the unbounded divergence and is computed
   by walking every commit envelope between HEAD and the ancestor.
   Likewise, conflict computation walks the full per-side delta when
   `include_conflicts=true`. If you need to refuse expensive previews,
   add a separate operational guard before invoking the walk (for
   example, reject when `target.t - ancestor.t` exceeds some threshold)
   or document that clients should pass `include_conflicts=false` for a
   cheaper preview.

### Response (`200 OK`)

```jsonc
{
  "source": "feature-x",
  "target": "main",
  "ancestor": { "commit_id": "bafy...", "t": 5 },
  "ahead": {
    "count": 3,
    "commits": [
      {
        "t": 8,
        "commit_id": "bafy...",
        "time": "2026-04-25T12:00:00Z",
        "asserts": 2,
        "retracts": 0,
        "flake_count": 2,
        "message": null
      }
      // ... newest-first
    ],
    "truncated": false
  },
  "behind": { "count": 1, "commits": [], "truncated": false },
  "fast_forward": false,
  "mergeable": true,
  "conflicts": {
    "count": 1,
    "keys": [{ "s": [100, "alice"], "p": [100, "status"], "g": null }],
    "truncated": false,
    "strategy": "take-source",
    "details": [
      {
        "key": { "s": [100, "alice"], "p": [100, "status"], "g": null },
        "source_values": [["ex:alice", "ex:status", "active", "xsd:string", true]],
        "target_values": [["ex:alice", "ex:status", "archived", "xsd:string", true]],
        "resolution": {
          "source_action": "kept",
          "target_action": "retracted",
          "outcome": "source-wins"
        }
      }
    ]
  }
}
```

`ancestor` is `null` only when both heads are absent. Each `CommitSummary`
sets `time` to `null` for legacy commits without a timestamp; `message` is
extracted from `txn_meta` when an entry with predicate `f:message` (Fluree
DB system namespace, local name `"message"`) and a string value is present.
Other conventions are not recognized — return `null`.

`ConflictKey` encodes a `(s, p, g)` tuple. The wire shape mirrors
`fluree_db_core::ConflictKey`:

```jsonc
{
  "s": [<namespace_code: u16>, "<local_name>"],
  "p": [<namespace_code: u16>, "<local_name>"],
  "g": [<namespace_code: u16>, "<local_name>"]   // or null for the default graph
}
```

`Sid`s serialize as `[ns_code, name]` tuples. Changing the encoding will
break the CLI.

When `include_conflict_details=false`, `conflicts.details` is omitted. When it
is true, `source_values` and `target_values` are resolved flake tuples for the
current asserted values in the same shape returned by `GET /show/*ledger`;
`resolution` is a label only. `mergeable` is `false` when the chosen strategy
would abort (currently `strategy=abort` with one or more conflicts). It is not
full transaction validation for constraints that might fail during the real
merge commit. `mergeable=true` does not guarantee a subsequent
`POST /merge/{ledger}` will succeed; it only reflects the conflict/strategy
interaction at preview time.

### Error responses

| Status | When |
|--------|------|
| `400` | Source has no parent (e.g., `main`); `source == target`; unknown strategy; unsupported strategy; `include_conflict_details=true` with `include_conflicts=false`; `strategy=abort` with `include_conflicts=false`. Body must include `"no source branch"` or `"itself"` for the first two cases so the CLI's matcher works. |
| `401` | Bearer required and absent/invalid. |
| `404` | Ledger or branch does not exist; or the bearer cannot `can_read`. |
| `5xx` | Storage / nameservice errors. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::merge_preview` |
| Orchestration | `fluree-db-api/src/merge_preview.rs::merge_preview_with` |
| Per-commit summary + DAG walk | `fluree-db-core/src/commit.rs::walk_commit_summaries` |
| Common ancestor (dual-frontier BFS) | `fluree-db-core/src/commit.rs::find_common_ancestor` |
| Delta-key computation | `fluree-db-novelty/src/delta.rs::compute_delta_keys` |

Validate compatibility by running `fluree branch diff dev --target feature
--remote your-remote --json` against your server and diffing the response
against output from the reference server on the same ledger state.

## Branch List Contract

`fluree branch list <ledger> --remote <name>` issues:

```
GET {api_base_url}/branch/{ledger}
```

The path segment is **singular** `branch` (not `branches`) and uses axum's
greedy `*ledger` tail capture, so a ledger named `org/mydb` is matched by
`/branch/org/mydb`. The endpoint takes no query parameters and no body.

### Auth

Read-only. Requires a Bearer when `data_auth.mode == required`; gates on
`can_read(ledger)`; returns `404` (not `403`) when the bearer cannot read it
to avoid existence leaks. Admin tokens are NOT required.

### Response (`200 OK`)

A JSON array of `BranchInfo`. Empty array when the ledger has no
non-retracted branches.

```jsonc
[
  {
    "branch": "main",
    "ledger_id": "mydb:main",
    "t": 12,
    "source": null
  },
  {
    "branch": "feature-x",
    "ledger_id": "mydb:feature-x",
    "t": 15,
    "source": "main"
  }
]
```

| Field | Type | Notes |
|-------|------|-------|
| `branch` | string | Branch name. |
| `ledger_id` | string | Full `ledger:branch` identifier. |
| `t` | integer | Current commit `t` on this branch. |
| `source` | string \| null | Parent branch, or `null` for root branches like `main`. Omitted via `skip_serializing_if = "Option::is_none"` when null. |

### Error responses

| Status | When |
|--------|------|
| `401` | Bearer required and absent/invalid. |
| `404` | Ledger does not exist; or the bearer cannot `can_read`. |
| `5xx` | Storage / nameservice errors. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::list_branches` |
| Response shape | `fluree-db-server/src/routes/ledger.rs::BranchInfo` |
| Underlying API | `fluree_db_api::Fluree::list_branches` |

## Branch Create Contract

`fluree branch create <name> --remote <name>` issues:

```
POST {api_base_url}/branch
Content-Type: application/json

{
  "ledger": "mydb",
  "branch": "feature-x",
  "source": "main"
}
```

The body type mirrors `fluree-db-server::routes::ledger::CreateBranchRequest`.

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `ledger` | string | Yes | — | Ledger name without branch suffix. |
| `branch` | string | Yes | — | New branch name. Must pass `validate_branch_name`. |
| `source` | string | No | `"main"` | Parent branch to fork from. The source must already exist and have at least one commit. |

### Auth

Admin-protected. Same middleware as `POST /create`, `POST /drop`,
`POST /reindex`, and `POST /iceberg/map` — registered through
`v1_admin_protected_routes` in `fluree-db-server/src/routes/mod.rs`.

### Response (`201 Created`)

```jsonc
{
  "ledger_id": "mydb:feature-x",
  "branch": "feature-x",
  "source": "main",
  "t": 12
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ledger_id` | string | Full `ledger:branch` identifier of the new branch. |
| `branch` | string | New branch name (echoed). |
| `source` | string | Resolved parent branch. Empty string if the new record's `source_branch` is unexpectedly null. |
| `t` | integer | Commit `t` at the branch point (inherited from the source's HEAD). |

The CLI's pretty-printer (`print_branch_created` in
`fluree-db-cli/src/commands/branch.rs`) reads `branch`, `source`, `t`, and
`ledger_id` from the response — keep all four populated.

### Error responses

| Status | When |
|--------|------|
| `400` | Invalid branch name (per `validate_branch_name`); malformed JSON body. |
| `401` / `403` | Admin token required and absent/invalid (see admin-auth middleware). |
| `404` | Source branch does not exist. |
| `409` | A branch with this name already exists (`ApiError::LedgerExists` → 409). |
| `5xx` | Nameservice / storage / index-copy errors. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::create_branch` |
| Request / response shapes | `CreateBranchRequest`, `CreateBranchResponse` (same file) |
| Underlying API | `fluree_db_api::Fluree::create_branch` (`fluree-db-api/src/ledger/loading.rs`) |

## Branch Drop Contract

`fluree branch drop <name> --remote <name>` issues:

```
POST {api_base_url}/drop-branch
Content-Type: application/json

{
  "ledger": "mydb",
  "branch": "feature-x"
}
```

Note the endpoint is `/drop-branch` (hyphenated) — separate from the
ledger-level `POST /drop` endpoint.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ledger` | string | Yes | Ledger name without branch suffix. |
| `branch` | string | Yes | Branch to drop. Cannot be `"main"`. |

### Auth

Admin-protected (same bracket as `/branch`, `/rebase`, `/merge`,
`/create`, `/drop`, `/reindex`).

### Behavior

The reference server's `Fluree::drop_branch`:

1. Refuses to drop `"main"` with `400`.
2. If the branch is **retracted** already → returns status `already_retracted`.
3. If the branch has children (`branches > 0`) → **soft-retracts** it (preserves
   storage so children can still resolve), returns `deferred: true`.
4. If the branch is a leaf → cancels indexing, deletes all storage artifacts
   (commits, txns, index roots, leaves, branches, dicts, garbage records,
   config, context), purges the nameservice record, and **cascades upward**
   to any retracted ancestors that now have zero children.

### Response (`200 OK`)

```jsonc
{
  "ledger_id": "mydb:feature-x",
  "status": "dropped",
  "deferred": false,
  "files_deleted": 14,
  "cascaded": ["mydb:retired-parent"],
  "warnings": []
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ledger_id` | string | Full `ledger:branch` identifier of the dropped branch. |
| `status` | string | `"dropped"`, `"already_retracted"`, or `"not_found"`. |
| `deferred` | bool | `true` when the branch was retracted but storage preserved (had children). |
| `files_deleted` | integer | Omitted when `0`. |
| `cascaded` | string[] | Ancestor `ledger_id`s that were cascade-dropped because they were retracted with zero remaining children. Omitted when empty. |
| `warnings` | string[] | Non-fatal warnings (e.g. partial artifact deletion). Omitted when empty. |

The CLI's `print_branch_dropped` reads `ledger_id`, `deferred`,
`files_deleted`, `cascaded`, and `warnings` — populate them all.

### Error responses

| Status | When |
|--------|------|
| `400` | Attempting to drop `"main"`; malformed JSON body. |
| `401` / `403` | Admin token required and absent/invalid. |
| `404` | Branch not found (the underlying lookup miss surfaces as `ApiError::NotFound` → 404). |
| `5xx` | Storage / nameservice errors during purge. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::drop_branch` |
| Request / response shapes | `DropBranchRequest`, `DropBranchResponse` (same file) |
| Underlying API | `fluree_db_api::Fluree::drop_branch` (`fluree-db-api/src/admin.rs`) |
| Report struct | `fluree_db_api::BranchDropReport` |

## Rebase Contract

`fluree branch rebase <branch> --remote <name>` issues:

```
POST {api_base_url}/rebase
Content-Type: application/json

{
  "ledger": "mydb",
  "branch": "feature-x",
  "strategy": "take-both"
}
```

| Field | Type | Required | Server default | Description |
|-------|------|----------|----------------|-------------|
| `ledger` | string | Yes | — | Ledger name without branch suffix. |
| `branch` | string | Yes | — | Branch to rebase. Cannot be `"main"`. |
| `strategy` | string | No | `"take-both"` | One of `take-both`, `abort`, `take-source`, `take-branch`, `skip`. Parsed by `ConflictStrategy::from_str_name`; unknown values respond `400`. |

### Auth

Admin-protected (same bracket as `/branch`, `/drop-branch`, `/merge`,
`/create`, `/drop`, `/reindex`).

### Behavior

Replays the branch's unique commits on top of its source branch's current
HEAD, detecting and resolving conflicts according to `strategy`. The branch's
own `source_branch` (from its nameservice record) is the rebase target — there
is no `target` field in the request.

- If the branch is already up-to-date with its source (`branch_head == ancestor`),
  the operation is a fast-forward: the branch's HEAD is advanced to the source
  HEAD with no replay, and `fast_forward: true` is returned.
- If `strategy == "abort"` and **any** branch commit conflicts with the source
  delta, the rebase aborts up-front with `409 BranchConflict`. No commits are
  written.
- Otherwise, the branch's commits are replayed sequentially on top of the
  source HEAD using the chosen strategy for conflict resolution.

### Response (`200 OK`)

```jsonc
{
  "ledger_id": "mydb:feature-x",
  "branch": "feature-x",
  "fast_forward": false,
  "replayed": 3,
  "skipped": 0,
  "conflicts": 1,
  "failures": 0,
  "total_commits": 3,
  "source_head_t": 18
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ledger_id` | string | Full `ledger:branch` identifier of the rebased branch. |
| `branch` | string | Branch name (echoed). |
| `fast_forward` | bool | `true` when the branch had no unique commits and was just advanced. |
| `replayed` | integer | Commits successfully replayed onto source HEAD. |
| `skipped` | integer | Commits skipped (e.g. via `skip` strategy on conflicts). |
| `conflicts` | integer | Total commits that contained conflicts. Note this is a count, not a list — the underlying `RebaseReport` carries `Vec<RebaseConflict>` and `Vec<RebaseFailure>`, but the HTTP response surfaces only the lengths. |
| `failures` | integer | Commits that failed to replay (transactional / validation errors). |
| `total_commits` | integer | Total branch commits considered for replay. |
| `source_head_t` | integer | Source branch HEAD `t` after rebase. |

The CLI's `print_rebase_result` reads `fast_forward`, `branch`, `source_head_t`,
`replayed`, `skipped`, `conflicts`, and `failures`.

### Error responses

| Status | When |
|--------|------|
| `400` | Rebasing `"main"` (`InvalidBranch`); branch has no `source_branch` (root branch); unknown / unsupported strategy; malformed JSON body. |
| `401` / `403` | Admin token required and absent/invalid. |
| `404` | Branch or its source not found. |
| `409` | `BranchConflict` — currently raised when `strategy=abort` and any commit conflicts with the source delta. |
| `5xx` | Storage / nameservice / index-build errors during replay. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::rebase` |
| Request / response shapes | `RebaseBranchRequest`, `RebaseBranchResponse` (same file) |
| Underlying API | `fluree_db_api::Fluree::rebase_branch` (`fluree-db-api/src/rebase.rs`) |
| Report struct | `fluree_db_api::RebaseReport` |
| Strategy enum | `fluree_db_api::ConflictStrategy` |

## Merge Contract

`fluree branch merge <source> --remote <name>` issues:

```
POST {api_base_url}/merge/*ledger
Content-Type: application/json

{
  "plan": {
    "source": { "branch": "feature-x", "expected": "<source-head-cid>" },
    "target": { "branch": "main",      "expected": "<target-head-cid>" },
    "baseStrategy": "take-both"
  }
}
```

The full plan schema, semantics, and the four-step `preview → query →
validate → commit` flow it's part of are specified in
[`docs/design/merge-custom.md`](../design/merge-custom.md). What follows is
the wire contract for the **commit** operation that's wired today; the
remaining three operations are staged.

### Path

| Segment | Description |
|---------|-------------|
| `*ledger` | Ledger name without branch suffix (e.g., `"mydb"`). Greedy tail capture, identical to other ledger-tailed routes. |

A stray top-level `ledger` field in the body is rejected with `400` so
callers using the retired `{ ledger, source, target, strategy }` shape fail
loudly.

### Body

```jsonc
{
  "plan": {
    "source": {
      "branch": "feature-x",
      "expected": "<source-head-cid>"          // required staleness guard
      // "at": "<CommitRef>"                    // reserved; must be absent in v1
    },
    "target": {
      "branch": "main",
      "expected": "<target-head-cid>"
    },
    "baseStrategy": "take-source",             // take-source | take-target | take-both | abort
    "resolutions": [],                         // v1: must be empty
    "additionalPatch": null                    // v1: must be empty/absent
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `plan.source.branch` | string | Yes | Branch to merge **from**. |
| `plan.source.expected` | string (CID) | Yes | Source HEAD captured by the caller. Stale value → `409`. |
| `plan.target.branch` | string | Yes | Branch to merge **into**. Must differ from `source.branch`. |
| `plan.target.expected` | string (CID) | Yes | Target HEAD captured by the caller. Stale value → `409`. |
| `plan.baseStrategy` | enum | Yes | Fallback strategy for conflicts not addressed by `resolutions`. |
| `plan.resolutions` | array | No | v1: must be empty (rejected with `400` otherwise). Per-conflict actions land in a follow-up. |
| `plan.additionalPatch` | object | No | v1: must be empty/absent (rejected with `400` otherwise). Plan-level edits land in a follow-up. |

### Auth

Admin-protected (same bracket as `/branch`, `/drop-branch`, `/rebase`,
`/create`, `/drop`, `/reindex`).

### Behavior

- Computes the common ancestor between `source` HEAD and `target` HEAD using
  a `BranchedContentStore` so sibling branches off `main` work.
- If `target` HEAD == ancestor, performs a **fast-forward merge**: copies
  the source's unique commit blobs into the target's namespace and advances
  the target HEAD via a CAS-protected publish against the pre-merge target
  ref. A concurrent advance between the nameservice read and this publish
  surfaces as `409` with `err:db/CommitConflict`. `fast_forward: true` is
  reported.
- Otherwise, performs a **general merge**: detects real conflicts via
  delta-key intersection plus object-set comparison (convergent edits — both
  branches asserting identical values — are **not** reported as conflicts),
  applies `baseStrategy` to each, restamps every resulting flake at
  `merge_t = max(source_t, target_t) + 1`, and writes a multi-parent commit
  on the target. The commit publish is also CAS-protected. If
  `baseStrategy == "abort"` and conflicts exist, fails with `409`.
- The merge engine no longer rolls back the target nameservice on error.
  CAS-only publishes mean any error path either advanced HEAD atomically or
  did nothing; the legacy `reset_head` rollback was force-write and would
  clobber a winning concurrent writer.

### Response (`200 OK`)

```jsonc
{
  "ledger_id": "mydb:main",
  "target": "main",
  "source": "feature-x",
  "fast_forward": false,
  "new_head_t": 22,
  "new_head_id": "bagaybqabciq...",
  "commits_copied": 4,
  "conflict_count": 1,
  "strategy": "take-both"
}
```

| Field | Type | Notes |
|-------|------|-------|
| `ledger_id` | string | Full `ledger:branch` identifier of the **target** after merge. |
| `target` | string | Resolved target branch. |
| `source` | string | Source branch name. |
| `fast_forward` | bool | `true` for a fast-forward merge. |
| `new_head_t` | integer | New commit `t` of the target after merge. For multi-parent merge commits this is `max(source_t, target_t) + 1`, which can exceed `previous_target_t + 1`. |
| `new_head_id` | string (CID) | New commit HEAD CID. Useful as the `expected` value on a subsequent retry or follow-up merge. |
| `commits_copied` | integer | Number of commit blobs copied into the target's namespace. |
| `conflict_count` | integer | Number of real conflicts resolved (refined detection). `0` for fast-forward. |
| `strategy` | string \| omitted | Strategy used. Omitted (via `skip_serializing_if`) for fast-forward merges where strategy doesn't apply. |

### Error responses

| Status | `@type` | When |
|--------|---------|------|
| `400` | `err:db/BadRequest` | Plan shape invalid (legacy `{ ledger, source, ... }` body, unknown `baseStrategy`, non-empty staged-feature fields, source has no parent branch and no target supplied, source == target, source has no commits). |
| `401` / `403` | `err:api/Unauthorized` / `err:api/AccessDenied` | Admin token required and absent/invalid. |
| `404` | `err:db/LedgerNotFound` | Source or target branch not found. |
| `409` | `err:db/CommitConflict` | Stale `source.expected` or `target.expected`; fast-forward CAS race with a concurrent writer; `baseStrategy=abort` with conflicts present. |
| `5xx` | `err:db/Internal` | Storage / nameservice / commit-write errors. |

### Reference implementation

| Concern | Canonical location |
|---------|-------------------|
| HTTP route + auth | `fluree-db-server/src/routes/ledger.rs::merge` |
| Request / response shapes | `MergeBranchRequest`, `MergeBranchResponse` (same file) |
| Plan-driven entry point | `fluree_db_api::Fluree::merge` (`fluree-db-api/src/merge.rs`) |
| Plan types (wire shape) | `fluree_db_api::merge_plan` (`fluree-db-api/src/merge_plan.rs`) |
| Legacy strategy entry point | `fluree_db_api::Fluree::merge_branch` (same shared backend) |
| Report struct | `fluree_db_api::MergeReport` |
| Conflict detection | `fluree_db_api::merge::detect_conflicts` (delta-key intersection + object-set comparison) |
| `merge_t` plumbing | `fluree_db_transact::CommitOpts::merge_t`; `fluree_db_ledger::StagedLedger::new_with_t` |

## Replication Auth Contract

Replication endpoints are intentionally protected more strictly than data reads:

- Pack + commit export + storage proxy endpoints require a Bearer token with `fluree.storage.*` permissions.
- Unauthorized requests should return `404 Not Found` (no existence leak) for these endpoints.

Data API endpoints use normal read/transaction auth (`fluree.ledger.read.*`, `fluree.ledger.write.*`) and should return `401/403/404` as appropriate for your product.

## Pack Protocol Contract

- Endpoint: `POST {api_base_url}/pack/*ledger`
- Request: JSON `PackRequest` with `"protocol":"fluree-pack-v1"`. Includes `include_indexes: bool` (default `true` for clone/pull; `false` with `--no-indexes`), `include_txns: bool` (default `true`; `false` with `--no-txns` on clone), and optional `want_index_root_id` / `have_index_root_id` when the CLI requests index data.
- Response: `Content-Type: application/x-fluree-pack`, streaming frames:
  - Preamble `FPK1` + version byte
  - Header frame (mandatory, first)
  - Data frames: CID binary + raw object bytes
  - Optional Manifest frames (phase transitions)
  - End frame (mandatory termination)

Clients verify integrity:

- Commit-v2 blobs (`FCV2` magic): sub-range hash verification.
- All other objects: full-bytes hash verification by CID.

**Graceful fallback:** If you do not implement pack yet, return `404 Not Found`, `405 Method Not Allowed`, `406 Not Acceptable`, or `501 Not Implemented`. The CLI treats those as "pack not supported" and falls back to `GET /commits` plus `GET /storage/objects/:cid`.

## Storage Proxy Contract

These endpoints exist so a client can fetch bytes by CID without knowing storage layout:

- `GET {api_base_url}/storage/ns/:ledger-id` returns `NsRecord` JSON with CID identity fields:
  - `commit_head_id`, `commit_t`, `index_head_id`, `index_t`, optional `config_id`
- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id` returns raw bytes for the CID after verifying integrity.

`/storage/block` is only required for query peers that need server-mediated index-leaf access.

## `/create` Contract

- Endpoint: `POST {api_base_url}/create`
- Request body: `{"ledger": "mydb:main"}`
- Response (201 Created): `{"ledger": "mydb:main", "t": 0}`
- Response (409 Conflict): ledger already exists

If no branch suffix is provided (e.g., `"mydb"`), the server MUST normalize to `"mydb:main"`.

Used by `fluree publish` (and potentially future `fluree create --remote`) to create a ledger on a remote server before pushing commits.

## `/reindex` Contract

- Endpoint: `POST {api_base_url}/reindex`
- Auth: admin-protected (same middleware as `/create`, `/drop`).
- Request body:
  ```json
  {
    "ledger": "mydb:main",
    "opts": { }
  }
  ```
  `opts` is optional and reserved for future per-request overrides (e.g. indexer tuning). Servers MUST accept it and MAY ignore it — today the reference server always reindexes using its own configured indexer settings.
- Response (200 OK):
  ```json
  {
    "ledger_id": "mydb:main",
    "index_t": 42,
    "root_id": "fluree:index:sha256:...",
    "stats": {
      "flake_count": 0,
      "leaf_count": 0,
      "branch_count": 0,
      "total_bytes": 0
    }
  }
  ```
- Response (4xx/5xx): standard `ApiError` envelope on failure (e.g. ledger not found).

The response shape mirrors `fluree_db_api::ReindexResult` — implementers should treat that Rust struct as the source of truth and add new fields only additively. Used by `fluree reindex --remote <name>` and by the CLI's auto-routing when a local server is running.

## `/exists` Response Contract

- Endpoint: `GET {api_base_url}/exists?ledger=mydb:main` (or via `fluree-ledger` header)
- Response (200 OK, always): `{"ledger": "mydb:main", "exists": true|false}`

MUST return 200 regardless of whether the ledger exists (the `exists` field carries the result). Should query the nameservice only — no ledger data loading.

## `/info` Response Contract (CLI Minimum)

The CLI currently treats `GET {api_base_url}/info/*ledger` as an opaque JSON object, but it requires these fields:

- `t` (integer): required for `fluree clone` and `fluree pull` preflight and for `fluree push` conflict checks.
- `commitId` (string CID): required for `fluree push` when `t > 0` so it can detect divergence.

Other fields are optional and may be used only for display.

## Origin-Based Replication (LedgerConfig)

The CLI can do origin-based `clone --origin` and `pull` fallback without a named remote by fetching objects via:

- `GET {api_base_url}/storage/objects/:cid?ledger=:ledger-id`

If your nameservice advertises `config_id` on the NsRecord, the CLI will attempt to fetch that `LedgerConfig` blob (by CID) and then use it to try additional origins.

## Graph Source Endpoints (Iceberg, R2RML, BM25, etc.)

The CLI routes graph source operations through the server when one is running. This uses the same auto-routing mechanism as query/insert/etc.: the CLI checks for `server.meta.json` (written by `fluree server start`), verifies the PID is alive, and routes through `http://{listen_addr}/v1/fluree`. Users can bypass with `--direct`.

### `fluree list` (includes graph sources)

- `GET {api_base_url}/ledgers`

Returns a JSON array of **both** ledger records and graph source records. Retracted records are excluded.

**Response fields (required for each entry):**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Ledger or graph source name |
| `branch` | string | Branch name (e.g., `"main"`) |
| `type` | string | One of: `"Ledger"`, `"Iceberg"`, `"R2RML"`, `"BM25"`, `"Vector"`, `"Geo"` |
| `t` | integer | `commit_t` for ledgers, `index_t` for graph sources (0 if not indexed) |

**Example response:**

```json
[
  { "name": "mydb", "branch": "main", "type": "Ledger", "t": 5 },
  { "name": "warehouse-orders", "branch": "main", "type": "Iceberg", "t": 0 },
  { "name": "my-search", "branch": "main", "type": "BM25", "t": 5 }
]
```

The CLI shows a TYPE column only when the response contains non-Ledger entries.

**Error responses:** `500` on internal failure. Empty array `[]` when no records exist.

### `fluree info <name>` (graph source fallback)

- `GET {api_base_url}/info/*name`

Existing endpoint, extended with graph source fallback. Resolution order:

1. Look up `name` as a **ledger** — if found, return the standard ledger info response (unchanged)
2. Look up `name` as a **graph source** (append `:main` if no branch suffix) — if found, return the graph source response below
3. Return `404 Not Found`

**Graph source response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Graph source name |
| `branch` | string | Branch name |
| `type` | string | Source type (e.g., `"Iceberg"`) |
| `graph_source_id` | string | Canonical ID (e.g., `"warehouse-orders:main"`) |
| `retracted` | boolean | Whether retracted |
| `index_t` | integer | Index watermark |
| `index_id` | string? | Index ContentId (omitted if none) |
| `dependencies` | string[]? | Source ledger IDs (omitted if empty) |
| `config` | object? | Parsed configuration JSON (omitted if empty/`{}`) |

**Example:**

```json
{
  "name": "warehouse-orders",
  "branch": "main",
  "type": "Iceberg",
  "graph_source_id": "warehouse-orders:main",
  "retracted": false,
  "index_t": 0,
  "config": {
    "catalog": {
      "type": "rest",
      "uri": "https://polaris.example.com/api/catalog",
      "warehouse": "my-warehouse"
    },
    "table": "sales.orders",
    "io": {
      "vended_credentials": true,
      "s3_region": "us-east-1"
    }
  }
}
```

**CLI detection:** The CLI distinguishes graph source responses from ledger responses by checking for the `graph_source_id` field in the JSON.

### `fluree drop <name>` (graph source fallback)

- `POST {api_base_url}/drop`

Existing endpoint, extended with graph source fallback. Request body is unchanged: `{ "ledger": "<name>", "hard": true }`.

**Resolution order:**

1. Try dropping `name` as a **ledger** — if the drop report has `status: "dropped"` or `status: "already_retracted"`, return that
2. If the ledger drop report has `status: "not_found"`, try dropping as a **graph source** (default branch `"main"`)
3. If both return not found, return the not-found response

**Response:** Same schema as ledger drop: `{ "ledger_id": "name:branch", "status": "dropped"|"already_retracted"|"not_found", "warnings": [...] }`. For graph sources, `ledger_id` contains the graph source ID (e.g., `"warehouse-orders:main"`).

### `fluree iceberg map` (Iceberg graph source creation)

- `POST {api_base_url}/iceberg/map` (admin-protected)

Creates an Iceberg graph source with an R2RML mapping that defines how table rows become RDF triples. This is a write operation and should be admin-protected (same middleware as `/create` and `/drop`).

**Request body fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Graph source name (no colons) |
| `mode` | string | No | `"rest"` (default) or `"direct"` |
| `catalog_uri` | string | REST mode | REST catalog URI |
| `table` | string | No | Table identifier (`namespace.table`); required for REST mode if not specified in R2RML mapping |
| `table_location` | string | Direct mode | S3 URI (`s3://bucket/path/to/table`) |
| `r2rml` | string | Yes | R2RML mapping source (storage address or path) |
| `r2rml_type` | string | No | Mapping media type (e.g., `"text/turtle"`); inferred from extension |
| `branch` | string | No | Branch name (default: `"main"`) |
| `auth_bearer` | string | No | Bearer token for REST catalog auth |
| `oauth2_token_url` | string | No | OAuth2 token endpoint |
| `oauth2_client_id` | string | No | OAuth2 client ID |
| `oauth2_client_secret` | string | No | OAuth2 client secret |
| `warehouse` | string | No | Warehouse identifier (REST mode) |
| `no_vended_credentials` | boolean | No | Disable vended credentials (default: `false`) |
| `s3_region` | string | No | S3 region override |
| `s3_endpoint` | string | No | S3 endpoint override (MinIO, LocalStack) |
| `s3_path_style` | boolean | No | Use path-style S3 URLs (default: `false`) |

**Validation rules:**
- `name` must not be empty or contain `:`
- `r2rml` is required (defines how table rows become RDF triples)
- REST mode requires `catalog_uri`; requires `table` unless specified in R2RML mapping's `rr:tableName`
- Direct mode requires `table_location` (must start with `s3://` or `s3a://`)
- OAuth2 fields must all be provided together (url + id + secret)

**Example — REST catalog with R2RML:**

```json
{
  "name": "warehouse-orders",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "table": "sales.orders",
  "r2rml": "mappings/orders.ttl",
  "auth_bearer": "my-token",
  "warehouse": "my-warehouse"
}
```

**Example — REST catalog (table inferred from R2RML `rr:tableName`):**

```json
{
  "name": "airlines",
  "mode": "rest",
  "catalog_uri": "https://polaris.example.com/api/catalog",
  "r2rml": "mappings/airlines.ttl",
  "auth_bearer": "my-token"
}
```

**Example — Direct S3 (no catalog):**

```json
{
  "name": "execution-log",
  "mode": "direct",
  "table_location": "s3://bucket/warehouse/logs/execution_log",
  "r2rml": "mappings/execution_log.ttl",
  "s3_region": "us-east-1"
}
```

**Response (`201 Created`):**

| Field | Type | Present | Description |
|-------|------|---------|-------------|
| `graph_source_id` | string | Always | Created ID (e.g., `"warehouse-orders:main"`) |
| `table_identifier` | string | Always | Table identifier or derived from location |
| `catalog_uri` | string | Always | Catalog URI or S3 location |
| `connection_tested` | boolean | Always | Whether catalog connection was verified (always `false` for direct mode) |
| `mapping_source` | string | Always | R2RML mapping source |
| `triples_map_count` | integer | Always | Number of TriplesMap definitions found |
| `mapping_validated` | boolean | Always | Whether mapping was parsed and compiled successfully |

**Error responses:**
- `400 Bad Request` — validation failures (missing fields, invalid mode, bad table identifier)
- `409 Conflict` — graph source with this name already exists (if your nameservice enforces uniqueness)
- `500 Internal Server Error` — catalog connection failure, mapping load failure, nameservice write failure

### Querying graph sources

Graph source queries work through normal query endpoints. No separate endpoint is needed, but the Rust API has an important distinction:

- Use `query_from()` when the query body carries the dataset (`"from"` in JSON-LD, `FROM` / `FROM NAMED` in SPARQL), or when you are composing multiple sources.
- Use `graph(alias).query()` for a single lazy query target that may be either a native ledger or a mapped graph source.
- Do not use the raw materialized-snapshot path (`fluree.db(&alias)` → `fluree.query(&view, ...)`) for graph source aliases.

> **Important:** The unsupported path is specifically the raw `GraphDb` snapshot flow (`fluree.db(&alias)` → `fluree.query(&view, ...)`). That API assumes you already loaded a native ledger snapshot. Graph source resolution happens in the lazy builder paths (`graph().query()` and `query_from()`), which wire in the R2RML provider and can fall back from "ledger not found" to "mapped graph source".

**Supported query paths:**

```rust
// Connection-level — graph sources resolve transparently
// When compiled with the `iceberg` feature, query_from() automatically
// enables R2RML provider support via .with_r2rml().
f.query_from().sparql(sparql).execute_formatted().await
f.query_from().jsonld(&query_json).execute_formatted().await

// Single-target lazy query — works for ledgers and mapped graph sources
f.graph(alias).query().sparql(sparql).execute_formatted().await

// Ledger-scoped query that may reference graph sources in GRAPH patterns
f.graph(ledger_id).query().sparql(sparql).execute_formatted().await
```

**Do NOT use:**

```rust
// Raw materialized snapshot path — native ledgers only
let view = f.db(&alias).await?;
f.query(&view, query_input).await?  // ❌ No R2RML, no graph source resolution
```

**Query patterns that reference graph sources:**

Graph sources can be queried directly, just like ledgers:

- `POST {api_base_url}/query/execution-log:main` with a SPARQL or JSON-LD query body

Via `FROM` / `FROM NAMED` clauses:

```sparql
SELECT * FROM <execution-log:main> WHERE { ?s ?p ?o } LIMIT 10
```

Via `GRAPH` patterns (joining with ledger data):

```sparql
SELECT ?name ?orderId ?total
FROM <mydb:main>
WHERE {
  ?customer schema:name ?name .
  ?customer ex:customerId ?custId .
  GRAPH <warehouse-orders:main> {
    ?order ex:customerId ?custId .
    ?order ex:orderId ?orderId .
    ?order ex:total ?total .
  }
}
```

**How it works:** When the `iceberg` feature is compiled, `query_from()` and `graph().query()` automatically call `.with_r2rml()`, which constructs a `FlureeR2rmlProvider` that can resolve graph source names to R2RML mappings and route triple patterns through the Iceberg scan engine. The `NameService` trait requires `GraphSourceLookup` (read-only graph source discovery), so graph source resolution is always available at the nameservice layer.

**Known limitation:** `FROM <ledger>, <graph-source>` with bare WHERE patterns (no GRAPH wrapper) — the graph source participates in the dataset but bare triple patterns only scan native indexes. Use explicit `GRAPH <gs:main> { ... }` for the graph source part in mixed-source queries.

### Authentication

- **`POST /iceberg/map`** and **`POST /drop`** are admin-protected (same middleware as `/create`)
- **`GET /ledgers`** and **`GET /info/*name`** are read-only (same auth as other read endpoints)
- **`POST /query/*ledger`** with graph source GRAPH patterns uses normal query auth

## Ledger Portability (.flpack Files)

The CLI supports exporting and importing full native ledgers as `.flpack` files using the `fluree-pack-v1` wire format. This enables ledger portability without a running server.

```bash
# Export a ledger (all commits + indexes + dictionaries)
fluree export mydb --format ledger -o mydb.flpack

# Import into a new instance (can use a different ledger name)
fluree create imported-db --from mydb.flpack
```

The `.flpack` format is identical to the binary stream served by `POST /pack/{ledger}`, with the addition of a **nameservice manifest frame** that carries the metadata needed to reconstruct the nameservice record on import:

```json
{
  "phase": "nameservice",
  "ledger_id": "original-name:main",
  "name": "original-name",
  "branch": "main",
  "commit_head_id": "bafybeig...commitHead",
  "commit_t": 42,
  "index_head_id": "bafybeig...indexRoot",
  "index_t": 40
}
```

**Aliasing on import:** The ledger name provided to `fluree create` determines the local storage path. The data itself is content-addressed (CIDs), so a ledger can be imported under any name. The `ledger_id` inside the index root binary is informational and does not affect CAS resolution.

**Combined with publish:** A typical workflow for moving a ledger from one environment to another:

```bash
# On source machine: export
fluree export mydb --format ledger -o mydb.flpack

# On target machine: import and publish to server
fluree create mydb --from mydb.flpack
fluree remote add prod https://prod.example.com
fluree auth login --remote prod
fluree publish prod mydb
```

## Quick Validation Script

From a clean project directory:

```bash
fluree init
fluree remote add origin http://localhost:8090
fluree auth login --remote origin --token @token.txt

# Ledger operations
fluree fetch origin
fluree clone origin mydb:main
fluree pull mydb:main
fluree push mydb:main

# Publish a local ledger to remote
fluree create local-db
fluree insert local-db -e '{"@id": "ex:test", "ex:val": 1}'
fluree publish origin local-db

# Export / import round-trip
fluree export mydb --format ledger -o mydb.flpack
fluree create imported --from mydb.flpack

# Iceberg operations (requires iceberg feature on server)
fluree iceberg map my-gs \
  --catalog-uri https://polaris.example.com/api/catalog \
  --r2rml mappings/orders.ttl \
  --auth-bearer $POLARIS_TOKEN

fluree list                    # should show mydb (Ledger) + my-gs (Iceberg)
fluree info my-gs              # should show Iceberg config + R2RML mapping
fluree show t:1 --remote origin  # should show decoded commit with resolved IRIs
fluree drop my-gs --force      # should drop the graph source
```
