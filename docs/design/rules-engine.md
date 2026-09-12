# Datalog rules engine: bodies through the query executor

Status: implemented in the "rules that see claims" change set (2026-09). This
page records the design, the decisions it forces, and the one decision that is
deliberately left open (reifier minting in rule heads).

## Why

The original datalog engine evaluated rule bodies with its own matcher: a
restricted pattern language (`RuleTriplePattern`, comparison-only filters), a
nested-loop join that issued one `db.range` call per binding row per pattern,
and its own parsers for JSON-LD and SPARQL rule sources. Measured on a 100k-edge
ledger (2026-09-11), a one-pattern rule deriving 100k facts took 19–25 s in every
storage state (novelty-only, indexed, indexed plus novelty), while OWL 2 RL
derived the same facts in 0.3 s. The cost was not data access: the head
instantiator deduplicated by scanning its whole accumulating result vector for
every candidate flake, and the fixpoint rebuilt the derived overlay from scratch
each round.

The restricted language had a second consequence: a rule body could not read
edge annotations. A JSON-LD `@annotation` key fell through the parser as an
ordinary predicate IRI and matched nothing, with no diagnostic; a SPARQL
`{| … |}` tail was rejected by the CONSTRUCT-rule lowering, and the rejection
was a log line the caller never saw. The canonical claims-graph rule — "knows
with confidence above 0.85 implies trustedKnows" — silently derived nothing.

## Architecture

A rule is now a body plus heads:

```text
DatalogRule {
    id, name,
    body:  Query patterns lowered by the normal query parser (JSON-LD or SPARQL),
           with their VarRegistry,
    heads: Vec<RuleHead>  — (subject, predicate, object) terms that are either
           constants or VarIds bound by the body,
    depends_on / generates — predicate Sids, for the ordering heuristic
}
```

Rule bodies are executed by the planned query executor
(`prepare_execution_with_config` + `execute_prepared`) against the same
`GraphDbRef` the query runs on, with the base overlay wrapped in a
`ReasoningOverlay` carrying the facts derived so far. Every construct the
executor supports is therefore available in a rule body, subject to the
monotonicity rule below: edge annotations (`@annotation`, `{| |}`, `~ ?r`),
claim-first patterns (`@reifies`, `rdf:reifies <<( … )>>`), full FILTER
expressions, BIND, VALUES, UNION, property paths, and subqueries.

Rule bodies run with reasoning disabled (no re-entrant materialization), as
root (derived facts are policy-filtered after materialization, exactly as
before), with eager materialization so every binding is a decoded `Sid` or
literal, and without fuel or cancellation limits of their own (the reasoning
budget bounds them).

### Fixpoint

```text
loop:
    budget check (time, facts, memory)
    overlay = base ⊕ derived-so-far           (rebuilt only when derived grew)
    for rule in rules (ordered by dependency count):
        rows = execute(rule.body over overlay)
        for row in rows, head in rule.heads:
            flake = instantiate(head, row)      (skip rows that cannot instantiate)
            if seen.insert(key(flake)):         (HashSet, O(1))
                derived.push(flake); new += 1
                budget check (facts, memory)    (inside the round)
    if new == 0: fixpoint
```

Deduplication is a hash set keyed on `(s, p, o, dt, m)`. The derived overlay
is rebuilt between rounds only when the round produced facts; most rule sets
converge in two rounds. Semi-naive evaluation (restricting one body pattern to
the previous round's delta) is a follow-up; it is straightforward to add as a
VALUES seed on the delta once needed.

### Parsing

- **JSON-LD rules** `{ "@context", "where", "insert" }`: `where` is parsed by
  the JSON-LD query `where` parser (`parse_where_with_counters`) with a
  `JsonLdParseCtx` built from the rule's own `@context`, then lowered with the
  ledger snapshot as IRI encoder. `insert` is parsed by a dedicated head parser
  that accepts node maps with variables in subject, predicate and object
  position and typed literals via `@value`/`@type`.
- **SPARQL rules** `CONSTRUCT { … } WHERE { … }`: the WHERE is the lowered
  query's pattern list, unchanged; the CONSTRUCT template triples are the heads.
- **Stored rules** (`f:rule`) and **query-time rules** (`rules: [...]`) share
  both parsers.

### Validation, and the loud-rejection rule

A rule that cannot run is rejected with an error that names the rule and the
construct, and the query fails. This replaces the previous behaviour of
skipping the rule with a warning that reached only the process log. It applies
to stored rules too: a ledger whose rule set contains a broken rule fails its
reasoning queries until the rule is fixed, rather than silently answering over
an incomplete derivation.

Rejected constructs are the non-monotonic ones, which a fixpoint cannot
evaluate soundly without stratification: OPTIONAL, MINUS, NOT EXISTS
(`["not-exists", …]`), aggregates and GROUP BY, and SERVICE. Everything else the
executor accepts is allowed.

Range restriction is enforced per rule: every head variable must be bound by
the body. A head that references an unbound variable now rejects the rule (it
used to skip that head with a warning).

Filter operands keep the fail-closed semantics introduced for #1556: an
unquoted operand naming a prefix the rule's context does not define, or a
namespace the ledger has never seen, rejects the rule; a bare word compared
against a variable that only occurs in IRI position is rejected with the two
rewrites (quote it for a string, prefix it for an IRI).

Two more fail-closed checks close the silent shapes #1558 catalogued. A
`@`-prefixed key the query parser gives no meaning to in that position
(`@reverse`, `@list`, `@nest`, … on a node pattern; anything but `@value`,
`@type`, `@language`, `@annotation` on a value object) is rejected before
parsing — the query parser ignores such keys, which in a rule means "derive
nothing, silently". And a stored `f:rule` value whose datatype is neither
`@json` nor `f:sparql` (the documented-but-broken nested-object shape) rejects
with the two accepted shapes spelled out, instead of being expanded as
ordinary nodes.

The fixpoint's round limit is reported like a budget cap: stopping at
`max_iterations` while the last round still derived facts sets
`capped_reason: "iterations"` and logs a warning, so a truncated closure is
never indistinguishable from a converged one (#1559).

### IRI operands in JSON-LD filters

The JSON-LD query filter language could not compare a variable to an IRI:
`(= ?p ex:knows)` treated `ex:knows` as the string `"ex:knows"`, so `=` never
matched and `!=` kept every row. That is the fail-open shape #1556 fixed inside
the old rule parser, present at the query level. Moving rules onto the query
parser fixes it there: an unquoted atom that expands to an IRI through the
query's `@context` (a compact IRI with a known prefix, or an absolute IRI in
`<…>`) lowers to an IRI constant, and the executor compares it by term identity
as SPARQL does. Quoted strings are unchanged. An unquoted word with no prefix is
still a string, as before.

### Cache

`compute_derived_facts` now consults the reasoning LRU for the combined outcome
(OWL 2 RL plus datalog) under a key that extends the OWL key with the datalog
inputs: the enabled flag, the rules-source graph, a hash of the query-time rule
documents, and a hash of the stored rule sources it loaded. Stored rules are
hashed by content rather than trusted to the ledger epoch because a
cross-ledger `f:rulesSource` can change without the primary ledger moving. The
OWL-only result stays cached under its own key as before.

### Budget

The fact cap is checked inside a round, not only between rounds. Memory is now
accounted: each derived flake adds an estimate of its heap footprint, and
exceeding `max_memory_bytes` (the documented 100 MB default) caps the
materialization with `capped_reason: "memory"`. The derived overlay no longer
keeps four sorted copies of every flake; it keeps one sorted array and three
index permutations.

## Reifier minting in rule heads: options

Left open in this change set. The engine lands with heads that emit plain
triples only; a head containing `@annotation` (JSON-LD) or an annotation tail
(SPARQL) is rejected with an error naming the construct, so the gap is loud
until a decision is made.

The use case: a walker that explains its steps needs "this edge exists because
rule R fired over claims c1 and c2". Today derived facts are anonymous and
cannot carry provenance.

| Option | Sketch | Identity across re-materializations | Cost | Risk |
|---|---|---|---|---|
| A. No minting (status quo) | Provenance modelled as extra derived triples on the endpoints | n/a | none | Cannot attach provenance to the edge itself; a second rule per fact |
| B. Deterministic anonymous reifier | Head `@annotation` block mints one reifier per derived triple with id = skolem hash of (rule id, instantiated s p o); body from constants and body vars | Stable: same inputs, same id | Head instantiation emits the `f:reifies*` bundle plus body facts into the overlay; the annotation lanes already read overlays | Bundle size (4–6 flakes per derived edge) multiplies derived-fact counts; budgets must count them |
| C. Reuse a body-bound reifier | `@annotation: {"@id": "?claim"}` re-attaches the supporting claim to the derived edge | Stable | Cheapest | Violates the single-target invariant (one reifier, one live edge); rejected on that ground |
| D. System provenance metadata | Each derived flake carries the rule id in flake metadata; a query-side function exposes it | Stable | Smallest storage | New query surface, invisible to RDF tooling; cannot carry rule-supplied properties like confidence |

Recommendation: B, as a follow-up once this engine has landed, with the head
syntax mirroring the write surfaces:

```json
"insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b",
           "@annotation": {"ex:derivedFrom": {"@id": "?claim"}, "ex:confidence": "?c"}}}
```

```sparql
CONSTRUCT { ?a :trustedKnows ?b {| :derivedFrom ?claim ; :confidence ?c |} }
```

Two things need deciding before B ships: whether minted reifiers count against
`max_facts` individually or per derived edge, and whether a commit-driven
materialization sidecar (if built) persists them with the same deterministic
ids, which would make derived provenance durable and diffable across commits.

## Known limit: rule bodies run under the budget, not the query's tracker

Materialization happens while the query is *prepared*, before its execution
context — fuel tracker and cancellation handle — exists, exactly as OWL 2 RL
materialization always has. A rule body is therefore bounded by the reasoning
budget (facts, memory, time — checked between rules and rounds) but cannot be
cancelled mid-body by the query's cancellation token, and its scans are not
charged as query fuel. Threading the tracker and cancellation into
prepare-time materialization is the follow-up that closes this for both
engines.

## Why not keep the old matcher and extend it

The matcher's cost was its own bookkeeping, and extending its pattern language
to annotations would have meant re-implementing the planner's annotation
expansion (`expand_edge_annotation_patterns`), the three annotation read lanes,
and join ordering, inside a second engine. Two real costs of the executor
approach were weighed and accepted: per-rule planning overhead of a few
milliseconds per round, and the loss of a cheap semi-naive delta restriction
(the old engine did not have one either; it re-ran every rule against the full
overlay each round).
