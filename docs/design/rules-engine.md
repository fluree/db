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
    footprint: graph-qualified predicates/classes the body may read and the
           heads may write, plus whether re-running the body is repeatable
}
```

Rule bodies are executed by the planned query executor
(`prepare_execution_with_config` + `execute_prepared_streaming`) against the
same `GraphDbRef` the query runs on, with the base overlay wrapped in a
`ReasoningOverlay` carrying the facts derived so far. Derived facts carry
the query's `t`, and at equal `t` an assertion holds over a retraction, so
when the latest commit retracted the stored copy of a fact the rules derive
again, the fact still holds. Every construct the
executor supports is therefore available in a rule body, subject to the
monotonicity rule below: edge annotations (`@annotation`, `{| |}`, `~ ?r`),
claim-first patterns (`@reifies`, `rdf:reifies <<( … )>>`), full FILTER
expressions, BIND, VALUES, UNION, property paths, and subqueries.

Rule bodies run with reasoning disabled (no re-entrant materialization), as
root (derived facts are policy-filtered after materialization, exactly as
before), with eager materialization so every binding is a decoded `Sid` or
literal, and without fuel limits of their own. Each body runs under a
cancellation handle that times out at the reasoning budget's deadline, so a
long body stops at its next checkpoint instead of running to completion.

### Fixpoint

Rules are scheduled by dependency (`datalog_rules/dependency.rs`). Rule B
depends on rule A when one of B's reads may observe one of A's writes. Reads
and writes are qualified by graph and by predicate, and `rdf:type` by class.
Anything the walk cannot pin down reads or writes everything: a variable
predicate, a wildcard or zero-length path (a zero-hop match pairs every term
in the graph with itself), a variable-predicate head, an unmodelled construct.
Strongly connected components of the dependency graph are the recursive
groups; each group's stage is one past the latest stage of any group it reads
from.

```text
for stage in stages (producers first):
    active = stage's groups, each with delta = none
    while active is non-empty:                   (≤ max_iterations passes)
        budget check (time, facts, memory)
        overlay = base ⊕ derived-so-far
        for group in active, rule in group:
            bodies = [rule.body]                      if group.delta is none
                                                      or the rule re-runs whole
                   = [rule.body with atom seeded      otherwise
                      by group.delta, for each recursive atom]
            for body in bodies:
                for row in execute(body over overlay):   (streamed)
                    budget check (time, every 1024 rows)
                    for head in rule.heads:
                        flake = instantiate(head, row)  (skip rows that cannot instantiate)
                        if seen.insert(key(flake)):     (HashSet, O(1))
                            derived.push(flake)
                            budget check (facts, memory)
        active = recursive groups that derived something this pass,
                 each with delta = what it derived
        rebuild the derived overlay if anything was derived and work remains
```

A non-recursive group runs once, against the complete output of the stages
before it. Groups in one stage never read each other's writes, so they share
an overlay. Deduplication is a hash set keyed on `(s, p, o, dt, m)`.

Inside a recursive group, evaluation is semi-naive (`datalog_rules/delta.rs`).
A match a rule finds in pass k+1 but not in pass k uses at least one fact the
group derived in pass k: nothing else changes while the group iterates, since
no group in the same stage writes what it reads. A rule's *recursive atoms*
are its top-level triple patterns whose read may observe one of the group's
writes. After the group's first pass, the rule runs once per recursive atom,
with a `VALUES` seed in front of the atom binding its variables to the
distinct matches among the previous pass's facts. The seed replaces the
pattern when it matches exactly as a scan would (variables and SID constants,
a constant predicate, no datatype constraint); otherwise the pattern stays
behind the seed to do the matching. The rule's other atoms read the full
overlay, so a match using new facts in two positions is found twice and
deduplicated; splitting old and new facts per position would need two
overlays per pass. A rule that reads its group's writes anywhere else — a
UNION branch, EXISTS, a subquery, a property path, the trailing VALUES block —
re-runs its whole body every pass. So does a rule whose footprint is not
repeatable: a volatile function gives new rows for old matches every pass,
and a seed carries no transaction metadata for `t`/`op` to read.

A recursive group of one rule shaped as a transitive closure skips
iteration (`datalog_rules/closure.rs`). With `R₀` the `R` facts visible when
the group starts and `T` a triple pattern with another constant predicate,
in either direction, the rule's fixpoint derives exactly:

| Rule | Derives | Run once as |
|---|---|---|
| `T(x,y) . R(y,z) → R(x,z)` | `T⁺ ∘ R₀` | `T+(x,y) . R(y,z)` |
| `R(x,y) . T(y,z) → R(x,z)` | `R₀ ∘ T⁺` | `R(x,y) . T+(y,z)` |
| `R(x,y) . R(y,z) → R(x,z)` | pairs ≥ 2 hops apart in `R₀` | `R+(x,y) . R(y,z)` |

The nonlinear row keeps its last hop as a triple pattern: `R+(x,z)` would
also emit the one-hop pairs, re-deriving asserted facts the rule never
derives. The path operator follows only IRI-valued objects. In the first and
third rows every node on the path is the subject of a fact; in the second the
path ends at the head's object, so that shape compiles only after a scan
finds no literal object of `T`'s predicate (which the group cannot write).
A path with both ends free computes all its pairs before it returns a row,
so the closure body runs with a pair limit of one more than the facts the
budget still has room for (or the memory left, at `size_of::<Flake>()` a
fact, if smaller). A path that exceeds it, or a traversal that hits the path
operator's visited-node limit, falls back to iteration, which the budget
bounds fact by fact. The wasted work is about one budget's worth of pairs.

`RuleSchedule::WholeProgram` treats the whole program as one recursive group
and runs every rule's whole body every round, the evaluation used before
dependency scheduling. It is kept as the reference differential tests compare
against.

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

Rejected constructs: OPTIONAL, MINUS (NOT EXISTS is the supported negation),
GROUP BY at the top of a body (aggregate in a subquery instead), and SERVICE.
Everything else the executor accepts is allowed, including stratified
negation and aggregation (below).

Range restriction is enforced per rule: every head variable must be bound by
the body. A head that references an unbound variable now rejects the rule (it
used to skip that head with a warning).

Filter operands keep the fail-closed semantics introduced for #1556: an
unquoted operand naming a prefix the rule's context does not define, or a
namespace the ledger has never seen, rejects the rule; a bare word is rejected outright, in any position,
with the two rewrites (quote it for a string, prefix it for an IRI) — the
linter does not try to infer whether an IRI was plausible there.

Two more fail-closed checks close the silent shapes #1558 catalogued. A
`@`-prefixed key the query parser gives no meaning to in that position
(`@reverse`, `@list`, `@nest`, … on a node pattern; anything but `@value`,
`@type`, `@language`, `@annotation` on a value object) is rejected before
parsing — the query parser ignores such keys, which in a rule means "derive
nothing, silently". And a stored `f:rule` value whose datatype is neither
`@json` nor `f:sparql` (the documented-but-broken nested-object shape) rejects
with the two accepted shapes spelled out, instead of being expanded as
ordinary nodes.

The fixpoint's pass limit is reported like a budget cap: stopping at
`max_iterations` passes in one stage while its last pass still derived facts
sets `capped_reason: "iterations"` and logs a warning, so a truncated closure
is never indistinguishable from a converged one (#1559).

### Stratified negation and aggregation

Reads under `NOT EXISTS` or `!` are recorded as `negated_reads` in the
footprint, and reads inside a grouped or aggregating subquery as
`aggregated_reads`, as well as ordinary reads, so they create dependency
edges like any other read: a negating or aggregating rule's stage comes after
every rule that can write what it reads. `RuleDependencies::unstratified`
rejects a program in which such a read may observe a write of the reading
rule's own recursive group, reporting a cycle found by a breadth-first search
inside that group (`-not->` or `-agg->` on the first arrow) and the facts
the offending read names, with IRIs spelled out. Double negation counts as negated, which keeps the check
conservative. The validator requires every variable a negation shares with
its group to be bound by a positive pattern of that group (or of the group the
negation is nested in).

Because a cap stops all later stages, a negating or aggregating rule never
runs over incomplete inputs within datalog. A capped run that leaves such a
rule unfinished fails with `ResourceLimit` instead of returning partial
conclusions, and so does an OWL 2 RL cap or failure ahead of such a program,
since the OWL closure is datalog's input (a positive program still runs over
what OWL produced, or without it). `RuleSchedule::WholeProgram` refuses these
programs: it runs every rule from the first round. A subquery's solutions are
a bag, as in queries, and `owl:sameAs` aliases are counted as stated.

### Named graphs

`RuleHead::graph` holds a head's expanded graph IRI: from a CONSTRUCT
template's `GRAPH <iri>` block, or a JSON-LD insert node's `"@graph": "<iri>"`
(a named graph object `{"@id": <iri>, "@graph": [ … ]}` assigns its nodes).
The footprint writes such a head into `GraphScope::Named`. At run start
`HeadGraphs::resolve` maps each IRI to a graph id through the ledger's graph
registry, refusing a graph the ledger lacks, a system graph, or the graph the
rules run against. A derived flake carries its graph's SID in `g`, so the
dedup key and routing both see it.

`DerivedFactsOverlay` partitions facts by graph id (`new_routed` /
`DerivedFactsBuilder::build_routed`); a flake with no `g` belongs to the
graph the rules ran against, and OWL 2 RL facts stay in the default graph.
The executor splices the reasoning overlay into every dataset graph of the
ledger that holds derived facts, not only the primary view. A semi-naive
seed holds only facts without `g`, since a seeded pattern is a top-level
pattern of the graph the rules run against; named-graph reads sit inside
`GRAPH` and re-run whole. A transitive closure head with a graph does not
compile to a path.

### Run contract

- **Evaluation time.** `NOW()` and `TODAY()` read
  `ExecutionContext::evaluation_time` when it is set, and rule bodies always
  set it. A run's time is the query's `reasoningTime` /
  `# PRAGMA reasoning-time:`, else the commit time (`db:time` in the
  txn-meta graph) of the commit at the queried `t`. A snapshot with no commit
  at that `t` fails a program whose footprint `reads_clock`, rather than
  falling back to the wall clock. With one time per run, both functions are
  repeatable for scheduling and semi-naive purposes. A requested time is part
  of the cache key; a defaulted one is already pinned by `to_t`.
- **Deadline.** The reasoning budget's deadline becomes the body's
  `QueryCancellation::with_deadline`; a cancelled body caps the run on time.
- **Attribution.** A capped run records `capped_rules` (the groups still
  running when the cap fired) and `pending_rules` (the stages after it), and
  both reach the tracked response. The Raft-recorded tally mirror does not
  carry them, so its positional wire form is unchanged.

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
exceeding `max_memory_bytes` (derived from the fact ceiling) caps the
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

## Rule bodies run on the query's behalf

Materialization happens while the query is *prepared*, before its execution
context exists, so preparation takes the request's tracker and cancellation
handle (`PrepareConfig::for_request`, carried as `QueryRequest` into
`compute_derived_facts` and `RuleExecutionEnv`). Each rule body runs under a
handle from `QueryCancellation::with_deadline_inside`: it reports the query's
cancellation (client disconnect, query timeout) and also times out at the
reasoning budget's deadline without cancelling the query. A body stopped by
the query's cancellation fails the run with `Cancelled`; one stopped by the
deadline caps it, as before. Rule bodies' scans charge the query's tracker.
OWL 2 RL takes the same handle and tracker in `ReasoningOptions`: it checks
for cancellation at each round and every 1,024 facts within one, and charges
one micro-fuel per fact a round processes. Its cancellation and fuel errors
fail the query instead of letting it continue without OWL entailments.

A materialization is cached and shared, so its cost lands on the query that
computes it; queries that reuse it pay nothing for it. A run that is
cancelled or runs out of fuel returns an error and caches nothing.

## Explanations

`explain_derived_facts` (behind `Fluree::explain_derived`) materializes as a
query with the same reasoning modes would, from the reasoning cache when it
can, and takes the facts matching the target from the derived facts. It then
re-runs the body of every repeatable rule whose head can produce the target,
over asserted plus derived facts, and keeps the rows whose head instantiates
to one of those facts, compared by full term identity (value, datatype,
language tag) (`datalog_rules::explain`). Rules that are not repeatable are
reported, not replayed. Each row becomes a derivation: the body's triple
patterns instantiated as premises (looked up in the asserted facts to mark
them derived or not), its property paths as premises marked `path`, its
`NOT EXISTS` patterns instantiated as absences with their graph scope, and,
for a grouped
subquery, the subquery's patterns re-run without grouping, inside the
`GRAPH` block that encloses the subquery, and filtered to the row's group
key. Patterns inside a `UNION` count for each choice of branches the row
satisfies: every triple bound by the row and holding over asserted and
derived facts, every path's ends bound (up to 64 choices; past that, only
what lies outside the unions is reported). Replays run only when the run derived a matching fact, under the
reasoning budget's deadline measured from the start of the explanation and
under the request's cancellation and tracker: the deadline marks the
explanation incomplete, a cancellation fails it. Explanations are one step
and are not cached. They expose premises regardless of policy, so the API
refuses a non-root view, checked after the ledger's default policy is
applied.

## Why not keep the old matcher and extend it

The matcher's cost was its own bookkeeping, and extending its pattern language
to annotations would have meant re-implementing the planner's annotation
expansion (`expand_edge_annotation_patterns`), the three annotation read lanes,
and join ordering, inside a second engine. Two real costs of the executor
approach were weighed and accepted: per-rule planning overhead of a few
milliseconds per pass, and a delta restriction that seeds a body with a
`VALUES` block (see Fixpoint) rather than living inside a dedicated join
operator.
