# Policy in Transactions

Transaction-time enforcement uses the same [policy model](policy-model.md) as queries, switched on by `f:action: f:modify`. Where query-time enforcement *filters* flakes from results, transaction-time enforcement *rejects* the transaction when a write would touch flakes the identity isn't allowed to modify.

This page documents how write-time enforcement integrates with the transaction lifecycle, the failure shape, and the patterns that come up most often. For the policy node shape and combining algorithm, see the [policy model reference](policy-model.md). For the conceptual frame, see [Policy enforcement](../concepts/policy-enforcement.md).

## How transaction-time enforcement works

When a transaction is staged against a `PolicyContext`:

1. The engine resolves the request's policy set: identity-driven `f:policyClass` lookups + any inline `opts.policy` array, restricted to policies whose `f:action` includes `f:modify`.
2. The transaction is staged into novelty (assertions and retractions are computed from `insert` / `delete` / `where` clauses).
3. Each staged flake is checked against the matching policies.
4. If any required policy denies a flake (or any non-required allow is missing where one would be needed), the **entire transaction is rejected**. Transactions are atomic — a partial write is never persisted.
5. On rejection, the response carries the policy's `f:exMessage` (when supplied), the offending flake, and the policy's `@id`.

The result: the requester gets a clear authorization failure rather than a silently incomplete write.

## Worked example

```bash
fluree insert '{
  "@context": {"f": "https://ns.flur.ee/db#", "ex": "http://example.org/"},
  "@graph": [
    {
      "@id": "ex:email-restriction",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:required": true,
      "f:onProperty": [{"@id": "http://schema.org/email"}],
      "f:action": [{"@id": "f:modify"}],
      "f:exMessage": "Users can only update their own email.",
      "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/user\": {\"@id\": \"?$this\"}}}"
    },
    {
      "@id": "ex:default-rw",
      "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
      "f:action": [{"@id": "f:view"}, {"@id": "f:modify"}],
      "f:allow": true
    },
    {"@id": "ex:johnIdentity",  "ex:user": {"@id": "ex:john"},  "f:policyClass": [{"@id": "ex:CorpPolicy"}]},
    {"@id": "ex:janeIdentity",  "ex:user": {"@id": "ex:jane"},  "f:policyClass": [{"@id": "ex:CorpPolicy"}]}
  ]
}'
```

Now John attempts to update his own email — succeeds:

```bash
fluree update --as ex:johnIdentity --policy-class ex:CorpPolicy '
  PREFIX ex: <http://example.org/>
  PREFIX schema: <http://schema.org/>
  WHERE  { ex:john schema:email ?email }
  DELETE { ex:john schema:email ?email }
  INSERT { ex:john schema:email "new-john@flur.ee" }
'
```

John attempts to update Jane's email — rejected:

```bash
fluree update --as ex:johnIdentity --policy-class ex:CorpPolicy '
  PREFIX ex: <http://example.org/>
  PREFIX schema: <http://schema.org/>
  WHERE  { ex:jane schema:email ?email }
  DELETE { ex:jane schema:email ?email }
  INSERT { ex:jane schema:email "hacked@flur.ee" }
'
# Error: policy denied: Users can only update their own email. (ex:email-restriction)
```

## What gets enforced

Every modification path runs the same `f:modify` policy check on its staged flakes:

| Operation | Flakes checked |
|-----------|----------------|
| **Insert** | All asserted flakes. |
| **Upsert** | Asserted flakes + retractions for any pre-existing values being replaced. |
| **Update** (WHERE/DELETE/INSERT) | Both retracted flakes (DELETE) and asserted flakes (INSERT). |
| **Retraction** (`@type: f:Retraction`) | Retracted flakes. |

Crucially, the policy is checked against the **flakes**, not the operation type. A transaction that retracts a flake the identity can't modify is rejected just like an insert that asserts one.

Enforcement is also independent of the **wire format**: the check runs on the staged flakes, so JSON-LD, SPARQL UPDATE, and Turtle / TriG / N-Triples writes are all governed by the same `f:modify` policy. Sending data as Turtle is not a way to bypass write policy.

Class targeting on the write side is **exact**: an `f:onClass` modify policy governs *every* flake whose subject is an instance of the class — including properties the class has never carried before. (Read-side class targeting uses the committed class→property index; write-side targeting matches by the subject's classes directly, since a write may introduce class/property combinations no committed data predicts.)

## Write verbs

`f:action` accepts three write verbs — `f:create`, `f:update`, `f:delete` — that refine `f:modify` by the subject's **lifecycle** within the transaction:

| subject exists pre-state | exists post-state | lifecycle |
|--------------------------|-------------------|-----------|
| no                       | yes               | `f:create` |
| yes                      | yes               | `f:update` |
| yes                      | no                | `f:delete` |

Every staged flake inherits its subject's lifecycle verb. There is no in-place mutation in RDF — changing a value stages a retract of the old value plus an assert of the new one — so the verbs are deliberately *entity-lifecycle* verbs, not flake-op verbs:

- **Changing** a value on an existing subject is entirely `f:update` (both the retract and the assert).
- **Clearing** a value while the subject persists is `f:update`.
- **Removing** the subject outright (all of its flakes retracted) is `f:delete`.
- **Inserting** a subject that didn't exist is `f:create` — all of its flakes.

This matches the SQL intuition (`UPDATE` can `SET x = NULL`; `DELETE` removes the row) and makes the common grants direct to express.

### Verb semantics vs bare `f:modify`

Verb policies get **exact** semantics on two axes where bare `f:modify` keeps its legacy behavior:

1. **Class targeting is pre ∪ post.** A verb policy with `f:onClass ex:Lead` matches flakes whose subject is a Lead before *or after* the transaction. "May create new Leads" is therefore one policy — the created subject's post-state class is visible to targeting. A deny cannot be escaped by un-typing the subject in the same transaction. (Bare `f:modify` class targeting matches pre-state classes only, so it never applies to brand-new subjects — the [immutable-records pattern](#immutable-records) below relies on this.)
2. **`rdf:type` flakes match by the class they mint or remove.** Asserting `rdf:type ex:Lead` is an operation *on `ex:Lead`* — it requires a grant whose class target covers Lead, and a grant scoped to Lead can never mint `ex:Contract`, even smuggled alongside a legitimate Lead typing. Under `default-allow: false`, class-scoped create grants are therefore also class-mint constraints, exactly like SQL's `GRANT INSERT ON lead`.

### "May create new X"

```json
{
  "@id": "ex:leadCreators",
  "@type": ["f:AccessPolicy", "ex:ApiKeyPolicy"],
  "f:onClass": [{"@id": "http://example.org/ns/Lead"}],
  "f:action": {"@id": "f:create"},
  "f:allow": true
}
```

With `default-allow: false`, the holder can insert new `ex:Lead` subjects (any of their properties — typing the new subject as `ex:Lead` in the same transaction is what brings it under the grant) and nothing else: no other class can be minted, existing Leads can't be edited or removed. Pair with a SHACL shape (`sh:closed`, required properties) to constrain *what a valid Lead looks like* — the shape is identity-invariant; the policy is who may create.

### Lifecycle edge cases

- A subject can be hollowed out by an `f:update` holder — values removed one by one — but never fully removed: retracting its last flakes classifies as `f:delete`. Use SHACL `sh:minCount` on required properties to prevent degrading below the shape's minimum.
- Retract-only writes against a nonexistent subject net to nothing and classify as `f:update` (no create/delete grant is consumed by a no-op).
- Property-level append-only policies ("may add tags but never remove them") are below the verbs' resolution; express them as a condition on `?$op`: a required `f:onProperty` policy with `ASK { FILTER($op = "assert") }`.

## Config-driven write enforcement

The ledger's `#config` graph governs writes the same way it governs reads:

- **Policy defaults apply without request inputs.** When `f:policyDefaults` declares `f:policyClass` (and optionally `f:defaultAllow`), transactions build a policy context from those defaults even when the request carries no `fluree-identity` / `fluree-policy-class` headers or inline `opts.policy`. A ledger configured with a modify-deny rule rejects violating writes from anonymous requests, matching read-side behavior.
- **`f:policySource` redirects the rule lookup.** Policy rules relocated into a named graph (or a cross-ledger model ledger via `f:ledger`) are loaded from the configured source at transaction time — never silently from the default graph. Unknown graph selectors fail closed.
- **Cross-ledger sources always engage.** A cross-ledger `f:policySource` builds a policy context unconditionally (mirroring the read path): the model ledger's `f:modify` rules apply to every transaction against the data ledger. See [Cross-ledger policy](cross-ledger-policy.md).
- **Identities are bind-only under cross-ledger.** A request identity (header, `opts.identity`, or a verified credential's DID) resolves against the data ledger and populates `?$identity` for the model ledger's `f:query` rules — it never selects rules the way same-ledger identity-mode does. Rule selection is the policy-class chain (request `policy_class` → config `f:policyClass` → `{f:AccessPolicy}` for anonymous requests). An identity-carrying request with no policy class anywhere fails closed; declaring `f:policyClass` in the config makes authenticated writes work with no per-request changes. See [Cross-ledger policy → Identity binding](cross-ledger-policy.md#identity-binding-under-cross-ledger-policy).
- **Override control gates request-time overrides.** A request that supplies its own policy inputs replaces the config defaults only when the config's `f:overrideControl` permits it — see [Override control](../ledger-config/override-control.md).

This applies uniformly across the server transact routes (local and Raft consensus), push replication, credentialed transactions, and the CLI's local mode with policy flags.

> **Trusted-admin exception.** Config-driven write enforcement is applied at the consensus commit boundary. Direct in-process library writes — the embedded `Fluree::insert_turtle` / `insert_turtle_with_opts` methods, and `fluree insert`/`upsert`/`update` in local mode invoked without policy flags — are a **trusted-admin path**: they stage under root and do **not** resolve config-declared `f:policySource` / `f:modify` defaults. An embedded integrator that needs config policy enforced on writes must go through the server transact routes (or supply an explicit `PolicyContext`). This is deliberate: a process with direct storage access is already inside the trust boundary.

## Targeting patterns

### Whitelist a property to a role

```json
{
  "@id": "ex:salary-write",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "http://example.org/salary"}],
  "f:action": [{"@id": "f:modify"}],
  "f:exMessage": "Only HR may write salary.",
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/role\": \"hr\"}}"
}
```

Combined with `default-allow: true` (or a permissive default `f:modify` policy), every other property remains writable.

### Owner-only edits

```json
{
  "@id": "ex:owner-edit",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:action": [{"@id": "f:modify"}],
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"http://example.org/user\": {\"@id\": \"?$user\"}}, \"$where\": {\"@id\": \"?$this\", \"http://example.org/owner\": {\"@id\": \"?$user\"}}}"
}
```

The `f:query` resolves the identity's user and verifies that `?$this` (the entity being modified) has that user as its owner.

### Status-based gates

Prevent edits to records past a workflow gate:

```json
{
  "@id": "ex:no-edit-after-approval",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onClass": [{"@id": "http://example.org/Order"}],
  "f:action": [{"@id": "f:modify"}],
  "f:exMessage": "Approved orders cannot be modified.",
  "f:query": "{\"where\": [{\"@id\": \"?$this\", \"http://example.org/status\": \"?status\"}, [\"filter\", \"(!= ?status \\\"approved\\\")\"]]}"
}
```

Approved orders fail the gate — their flakes can't be retracted or modified.

### Workflow service exception

Combine targeting + identity-typed checks to limit a write to a single service:

```json
{
  "@id": "ex:approved-by-workflow-only",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onProperty": [{"@id": "http://example.org/approved"}],
  "f:action": [{"@id": "f:modify"}],
  "f:exMessage": "ex:approved is set by the workflow service only.",
  "f:query": "{\"where\": {\"@id\": \"?$identity\", \"@type\": \"http://example.org/WorkflowService\"}}"
}
```

End-user identities can read `ex:approved`, but only the workflow service can write it.

### Immutable records

The verb form states the intent directly — insertable, never editable or removable:

```json
{
  "@id": "ex:audit-log-immutable",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:onClass": [{"@id": "http://example.org/AuditEvent"}],
  "f:action": [{"@id": "f:update"}, {"@id": "f:delete"}],
  "f:exMessage": "Audit events are immutable.",
  "f:allow": false
}
```

Notice the absence of `f:query` — `f:allow: false` is a flat deny, applied to every update or delete of `ex:AuditEvent` instances. New events insert freely (pair with a create grant under `default-allow: false`). Because verb-policy `rdf:type` targeting matches the class being retracted, the deny also blocks *un-typing* an audit event — the type flake can't be stripped to escape the class.

The legacy spelling — `f:action: [{"@id": "f:modify"}]` — still works: bare `f:modify` class targeting matches pre-state classes only, so a fresh `@type: ex:AuditEvent` insertion creates a new subject the targeting doesn't match. Prefer the verb form for new policies; it says what it means and closes the un-typing edge.

## Failure shape

When a transaction is rejected, the API returns:

```json
{
  "error": "policy_denied",
  "message": "Users can only update their own email.",
  "policy": "http://example.org/email-restriction",
  "subject": "http://example.org/jane",
  "property": "http://schema.org/email"
}
```

`f:exMessage` is the user-visible string. The policy `@id`, the offending subject, and the property are reported for diagnostics.

When no `f:exMessage` is set, a generic message is returned (`"policy denied"`); the structured fields are still present so a client can surface the right error to a user.

## WHERE/DELETE/INSERT semantics with policy

A WHERE/DELETE/INSERT transaction proceeds in three phases — match → retract → assert. The two policy actions apply at different phases:

- **`f:view` filters the match phase.** The WHERE clause is a *read*, so it binds only the flakes the requesting identity may view — exactly as the same patterns would in a query. An identity therefore cannot conditionally match on data it can't see: `INSERT { ?s ex:flag true } WHERE { ?s ex:salary ?v . FILTER(?v > 100000) }` binds nothing when `ex:salary` isn't viewable, so a conditional write can't be used to probe hidden values.
- **`f:modify` rejects the write.** Enforcement runs on the staged flakes from the retract/assert phases; any flake the identity can't modify rejects the whole transaction.

```sparql
PREFIX ex:     <http://example.org/>
PREFIX schema: <http://schema.org/>

WHERE  { ?u schema:email ?old . FILTER(?u = ex:jane) }
DELETE { ?u schema:email ?old }
INSERT { ?u schema:email "new@flur.ee" }
```

Run by an identity that **can view** Jane's email but lacks **modify** rights on it:

- The WHERE binds Jane's email (it is viewable), so the DELETE/INSERT are staged.
- The DELETE retraction stages a flake the identity can't modify — **rejected**.

Run by an identity that **cannot view** Jane's email:

- The WHERE binds nothing — the hidden value is never read, so it can't be inferred from whether the write succeeded. A WHERE/DELETE/INSERT that matches no rows produces no flakes and is reported as an empty transaction.

So pairing an `f:modify` policy with a same-shape `f:view` policy turns a would-be modify *rejection* into a clean no-op: the WHERE stops matching the protected rows, so nothing is staged to reject.

## Signed transactions and impersonation

When a transaction is signed (JWS or VC-wrapped), the signing key's identity replaces the bearer identity for policy purposes. The signed credential becomes the source of truth: the server verifies the signature, resolves the signer's identity entity, and applies that identity's `f:policyClass` policies.

For the impersonation rules — when `--as <iri>` is honored vs force-overridden — see [Policy in queries → Remote impersonation](policy-in-queries.md#remote-impersonation-how-its-authorized). The same gate applies to transactions.

See [Signed / credentialed transactions](../transactions/signed-transactions.md) for the wire format.

## Provenance

Every committed transaction carries the asserting identity in its commit metadata. Combined with policy enforcement, this gives a clean audit trail:

- The identity is recorded on the commit.
- The policies in effect at commit time are themselves time-travelable.
- Replay-from-commit produces the same policy decisions.

## Performance considerations

- **Stage cost dominates.** Most of the work is staging the transaction (computing assertions/retractions, building the novelty layer). Policy checks add a small per-flake cost on top.
- **Required policies short-circuit.** A failure rejects the transaction immediately without checking remaining flakes.
- **Batch transactions amortize loading.** Loading the policy set is per-transaction, not per-flake — large batched transactions pay the load cost once.
- **Cache identity properties.** The identity's `@type`, `f:policyClass`, and any role tags used in `f:query` are loaded once per transaction.
- **Config resolution is memoized.** Learning whether a ledger declares policy requires reading its `#config` graph, which the write path now does on every transaction. That read is cached per-ledger and invalidated only when a commit actually writes the config graph, so a configured-but-static ledger under sustained writes resolves its config once per config change — not once per write (nor once per stage/commit retry). Unconfigured ledgers short-circuit before any scan. The cache is a fail-safe fast path: it is consulted only at head with a live handle, and any miss or ambiguity resolves fresh, so it can never serve stale (fail-open) policy.

## Testing policies from the CLI

The same `--as`, `--policy-class`, and `--default-allow` flags used on `fluree query` are available on `fluree insert`, `fluree upsert`, and `fluree update` so you can verify write-time enforcement without any client code:

```bash
# Attempt a write as an identity that lacks the f:modify policy — expect failure
fluree insert --as ex:readOnlyIdentity --policy-class ex:CorpPolicy -f new-data.ttl

# Same write as an authorized identity — expect success
fluree insert --as ex:writerIdentity --policy-class ex:CorpPolicy -f new-data.ttl
```

The flags work locally and against remote servers. On remote, the CLI sends the policy options as HTTP headers (`fluree-identity`, `fluree-policy-class`, `fluree-default-allow`) and, for JSON-LD bodies, also injects them into `opts`. The server applies the **root-impersonation gate**: your bearer identity may delegate to `--as <iri>` only when the bearer identity itself has no `f:policyClass` on the target ledger. Restricted bearers have `--as` force-overridden back to their own identity (and writes only what their own policies permit).

This is the standard service-account pattern — see [Policy in queries → Remote impersonation](policy-in-queries.md#remote-impersonation-how-its-authorized) for the full authorization rules and audit-log format.

### Transaction enforcement is end-to-end

Unsigned bearer-authenticated transactions build a `PolicyContext` from the (post-header-merge) opts and route through the policy-enforcing `transact_tracked_with_policy` path. A non-root bearer's `f:modify` constraints apply to their writes, matching the long-standing query-side behavior. SPARQL UPDATE inherits the same enforcement, with identity sourced from either the bearer or the `fluree-identity` header (impersonation-gated).

## Related documentation

- [Policy model and inputs](policy-model.md) — node shape, combining algorithm, request-time options
- [Policy enforcement (concepts)](../concepts/policy-enforcement.md) — model overview
- [Policy in queries](policy-in-queries.md) — read-time enforcement
- [Cross-ledger policy](cross-ledger-policy.md) — transaction-time enforcement under cross-ledger `f:policySource`
- [Cookbook: Access control policies](../guides/cookbook-policies.md) — worked patterns
- [Programmatic policy API (Rust)](programmatic-policy.md) — building `PolicyContext` and using `transact_tracked_with_policy`
- [Signed / credentialed transactions](../transactions/signed-transactions.md) — JWS / VC transaction wrapping
- [Transaction overview](../transactions/overview.md) — transaction lifecycle
