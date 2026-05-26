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

```json
{
  "@id": "ex:audit-log-immutable",
  "@type": ["f:AccessPolicy", "ex:CorpPolicy"],
  "f:required": true,
  "f:onClass": [{"@id": "http://example.org/AuditEvent"}],
  "f:action": [{"@id": "f:modify"}],
  "f:exMessage": "Audit events are immutable.",
  "f:allow": false
}
```

Notice the absence of `f:query` — `f:allow: false` is a flat deny, applied to every modification of `ex:AuditEvent` instances. New events can still be inserted because the policy targets only existing-instance flakes; a fresh `@type: ex:AuditEvent` insertion creates a new subject and a new `rdf:type` flake, neither of which the targeting matches.

(For a hard "append-only" guarantee that forbids anything but new insertions, model the constraint with a SHACL shape that requires the property to be unset on prior commits — SHACL is a better fit for that pattern than policy.)

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

A WHERE/DELETE/INSERT transaction proceeds in three phases — match → retract → assert. Policy enforcement is on the staged flakes from phases 2 and 3:

```sparql
PREFIX ex:     <http://example.org/>
PREFIX schema: <http://schema.org/>

WHERE  { ?u schema:email ?old . FILTER(?u = ex:jane) }
DELETE { ?u schema:email ?old }
INSERT { ?u schema:email "new@flur.ee" }
```

When run by an identity that lacks modify rights on `?u`'s email:

- The WHERE pattern still binds normally — policy doesn't filter the *match phase*.
- The DELETE retraction stages a flake the identity can't modify — **rejected**.

To prevent accidental no-op rejections (the WHERE matches but the DELETE/INSERT can't proceed), pair transaction-time `f:modify` policies with the same shape `f:view` policies, so the WHERE itself sees a filtered view.

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
