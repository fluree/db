# Datalog Rules

Datalog rules let you define custom inference logic that goes beyond what
OWL and RDFS provide. Rules are expressed in a familiar JSON-LD pattern syntax
with `where` (conditions) and `insert` (conclusions) clauses, and execute in a
fixpoint loop that can chain rules together.

For background concepts see [Reasoning and inference](../concepts/reasoning.md);
for enabling reasoning in queries see
[Query-time reasoning](reasoning.md).

## Quick example

Infer a `grandparent` relationship from two `parent` hops:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?gp"],
  "where": {"@id": "ex:alice", "ex:grandparent": "?gp"},
  "reasoning": "datalog",
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?person", "ex:parent": {"ex:parent": "?gp"}},
      "insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
    }
  ]
}
```

The rule says: *"For any `?person` whose `parent` has a `parent` `?gp`, insert
that `?person` has a `grandparent` `?gp`."* The query then finds Alice's
grandparents using the inferred facts.

## Rule format

Each rule is a JSON object with three parts:

| Key | Required | Description |
|-----|----------|-------------|
| `@context` | Yes | JSON-LD context for expanding compact IRIs |
| `where` | Yes | Pattern(s) that must match for the rule to fire |
| `insert` | Yes | Pattern(s) of new facts to derive when the rule fires |
| `@id` | No | Optional name/IRI for the rule (for documentation/debugging) |

### Where clause

The `where` clause defines the conditions under which the rule fires. It
follows the same pattern syntax as JSON-LD queries.

**Single pattern:**
```json
"where": {"@id": "?person", "ex:parent": "?parent"}
```

**Multiple patterns (implicit join on shared variables):**
```json
"where": [
  {"@id": "?person", "ex:parent": "?parent"},
  {"@id": "?parent", "ex:name": "?parentName"}
]
```

**Nested patterns (shorthand for multi-hop traversal):**
```json
"where": {"@id": "?person", "ex:parent": {"ex:parent": "?gp"}}
```
This is equivalent to two patterns joined on an intermediate variable.

**Property-position variables (the predicate is a variable):**
```json
"where": [
  {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
  {"@id": "?other", "?prop": "?val"}
]
```
A variable in predicate position matches any predicate and binds it — here
`?prop` / `?val` range over every property of `?other`. The bound predicate can
then be reused in the `insert` clause. (A leading pattern whose subject **and**
predicate are both unbound, e.g. `{"@id": "?s", "?p": "?o"}`, scans the whole
ledger — see the reasoning budget under [Fixpoint evaluation](#fixpoint-evaluation).)

**With filters:**
```json
"where": [
  {"@id": "?person", "ex:age": "?age"},
  ["filter", "(>= ?age 65)"]
]
```

Filters can also compare against an **IRI**, which is how you constrain a
variable-position predicate or an entity-valued object:

```json
"where": [
  {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
  {"@id": "?other", "?prop": "?val"},
  ["filter", "(!= ?prop ex:ssn)"]
]
```

How a filter operand is classified:

| Operand | Read as |
|---------|---------|
| `?name` | Variable |
| `62`, `1.5`, `true` | Number / boolean literal |
| `ex:ssn`, `http://example.org/ssn` | **IRI** — expanded via the rule's `@context` and resolved against the ledger |
| `"senior"`, `"John Smith"` | String literal (quote it — quoted operands may contain spaces) |
| `senior` | **Rejected** — a bare unquoted token is ambiguous; quote it for a string, prefix it for an IRI |

Three rules worth knowing:

- **IRI comparison is namespace-aware.** `(= ?p ex:knows)` matches `ex:knows`
  and not `foaf:knows`. Only `=` and `!=` are defined for IRIs; ordering
  operators against an IRI are rejected.
- **A bare name is rejected, not guessed.** `(= ?p knows)` is a parse error
  naming the operand: write `"knows"` (quoted) to compare against the string,
  or `ex:knows` to compare against the IRI. A bare token cannot be safely read
  as a string, because against an IRI-bound variable a string comparison fails
  invisibly in both directions — `=` derives nothing, and `!=` keeps every row,
  so an exclusion filter derives exactly the facts it was written to exclude.
  (Before Fluree resolved IRIs in filters, the bare form was the only one that
  appeared to work — but it matched namespace-blindly, so `ex:knows`,
  `foaf:knows` and any other `knows` were treated as equal. A rule still using
  the bare form is now rejected and skipped with a logged error naming the
  operand and both rewrites.)
- **An unresolvable IRI operand is an error, not a fallback.** If a filter
  names a prefix the rule's `@context` does not define, or a namespace the
  ledger has never seen, the rule is rejected and skipped with a logged error
  rather than quietly comparing the operand as a string. Quote the operand if
  you did mean a literal.

### Insert clause

The `insert` clause defines what facts to produce for each set of matching
variable bindings.

```json
"insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
```

- Variables in any position — subject, **predicate**, or object — are replaced
  with the bound values from `where`. A predicate variable lets a rule write a
  property whose name is computed: e.g. `"insert": {"@id": "?s", "?prop": "?val"}`
  copies every `?prop`/`?val` bound in the `where` clause onto `?s`.
- Use `{"@id": "?var"}` for IRI/entity values; use `"?var"` directly for
  literal values.
- Multiple triples can be generated from a single insert pattern.
- **Every variable used in `insert` must also appear in `where`.** A variable
  the `where` clause never binds cannot produce a fact. Each insert pattern is
  checked independently: a pattern that references an unbound variable is
  **skipped with a logged warning naming the variable**, while the rule's other
  insert patterns keep deriving — so one typo'd head in a multi-head rule does
  not silence the rest. Only a rule in which **no** insert pattern can ever
  produce a fact is rejected outright at parse time (with the same
  variable-naming error), rather than running and deriving nothing. A
  `where`/`insert` typo is the usual cause:

  ```json
  "where":  {"@id": "?s", "ex:relType": {"@id": "?relation"}},
  "insert": {"@id": "?s", "?rel": {"@id": "?s"}}
  ```

  `?rel` is never bound — the `where` clause binds `?relation`.
- Every node in an `insert` pattern needs an `@id`. An anonymous node has no
  subject to derive facts about, and is reported the same way.

## Providing rules

Rules can be provided in two ways:

### 1. Query-time rules

Pass rules directly in the query via the `rules` array. This is the simplest
approach and doesn't require any prior setup:

```json
{
  "select": ["?result"],
  "where": {"@id": "?s", "ex:derived": "?result"},
  "reasoning": "datalog",
  "rules": [ ... ]
}
```

> **Note:** Providing a `rules` array automatically enables datalog reasoning —
> you don't strictly need `"reasoning": "datalog"`, though including it is
> recommended for clarity.

### 2. Database-stored rules

Rules can be stored in the database as `f:rule` assertions and referenced via
ledger configuration. This is useful for rules that should apply consistently
across all queries.

**Store a rule:**
```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/"
  },
  "insert": {
    "@id": "ex:grandparentRule",
    "f:rule": {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?person", "ex:parent": {"ex:parent": "?gp"}},
      "insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
    }
  }
}
```

**Configure the ledger to use stored rules:**
```json
{
  "insert": {
    "@id": "urn:fluree:mydb:main:config:ledger",
    "@type": "f:LedgerConfig",
    "f:datalogDefaults": {
      "f:datalogEnabled": true,
      "f:rulesSource": {
        "@type": "f:GraphRef",
        "f:graphSource": {"f:graphSelector": {"@id": "f:defaultGraph"}}
      },
      "f:allowQueryTimeRules": true
    }
  }
}
```

See [Setting groups — datalogDefaults](../ledger-config/setting-groups.md) for
full configuration options.

`f:rulesSource` also supports cross-ledger references — set
`f:ledger` on the inner `f:graphSource` to pull `f:rule` JSON
bodies from another ledger at query time. See
[Cross-ledger governance — Cross-ledger datalog rules](../security/cross-ledger-policy.md#cross-ledger-datalog-rules)
for the end-to-end pattern and failure modes.

When stored rules, cross-ledger rules, and query-time rules are
present, they are all **merged** and execute together in the
same fixpoint loop.

## SPARQL rules

Rules can alternatively be written as SPARQL `CONSTRUCT ... WHERE ...`
queries: the CONSTRUCT template is the rule head (`insert`) and the WHERE
clause is the rule body. The language of a stored `f:rule` literal is
selected by its RDF datatype — `@json` means the JSON-LD rule format above;
the `f:sparql` datatype (`https://ns.flur.ee/db#sparql`) means SPARQL.

**Store a SPARQL rule:**
```json
{
  "@context": {"f": "https://ns.flur.ee/db#"},
  "insert": {
    "@id": "http://example.org/grandparentRule",
    "f:rule": {
      "@type": "f:sparql",
      "@value": "PREFIX ex: <http://example.org/> CONSTRUCT { ?person ex:grandparent ?gp } WHERE { ?person ex:parent ?p . ?p ex:parent ?gp }"
    }
  }
}
```

The same typed-value form works inside a query-time `rules` array entry
(directly, or as the `f:rule` value of a stored-rule-shaped entry).

SPARQL rules compile into the same restricted rule language as JSON-LD
rules, so the body supports **basic graph patterns and comparison FILTERs**
(`=`, `!=`, `<`, `<=`, `>`, `>=`, combined with `&&`, `||`, `!`). Anything
else — OPTIONAL, UNION, property paths, BIND, subqueries, aggregates — is
rejected: the rule is skipped with a warning and derives nothing (it never
partially applies). Include `PREFIX` declarations in the rule text; the
request `@context` is not applied to rule sources.

## Examples

### Sibling inference

Infer siblings from shared parents:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?sibling"],
  "where": {"@id": "ex:alice", "ex:sibling": "?sibling"},
  "reasoning": "datalog",
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": [
        {"@id": "?person", "ex:parent": "?parent"},
        {"@id": "?sibling", "ex:parent": "?parent"}
      ],
      "insert": {"@id": "?person", "ex:sibling": {"@id": "?sibling"}}
    }
  ]
}
```

> **Note:** This rule will also infer that a person is their own sibling. You
> could add a filter `["filter", "(!= ?person ?sibling)"]` to exclude
> self-references.

### Chained rules (uncle + aunt)

Multiple rules that build on each other:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?aunt"],
  "where": {"@id": "ex:alice", "ex:aunt": "?aunt"},
  "reasoning": "datalog",
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": {"@id": "?person", "ex:parent": {"ex:brother": "?uncle"}},
      "insert": {"@id": "?person", "ex:uncle": {"@id": "?uncle"}}
    },
    {
      "@context": {"ex": "http://example.org/"},
      "where": {
        "@id": "?person",
        "ex:uncle": {
          "ex:spouse": {"@id": "?aunt", "ex:gender": {"@id": "ex:Female"}}
        }
      },
      "insert": {"@id": "?person", "ex:aunt": {"@id": "?aunt"}}
    }
  ]
}
```

The second rule (aunt) depends on facts derived by the first rule (uncle). The
fixpoint loop handles this automatically — it keeps iterating until no new facts
are produced.

### Rules with filters

Classify people by age:

```json
{
  "@context": {"ex": "http://example.org/"},
  "select": ["?person"],
  "where": {"@id": "?person", "ex:status": "senior"},
  "reasoning": "datalog",
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": [
        {"@id": "?person", "ex:age": "?age"},
        ["filter", "(>= ?age 65)"]
      ],
      "insert": {"@id": "?person", "ex:status": "senior"}
    }
  ]
}
```

### Combining with OWL reasoning

Datalog rules can build on OWL-derived facts. For example, use OWL 2 RL to
materialize transitive and symmetric properties, then use Datalog for custom
business logic:

```json
{
  "select": ["?recommendation"],
  "where": {"@id": "ex:alice", "ex:recommended": "?recommendation"},
  "reasoning": ["owl2rl", "datalog"],
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": [
        {"@id": "?person", "ex:friend": "?friend"},
        {"@id": "?friend", "ex:likes": "?item"},
        {"@id": "?person", "ex:likes": "?item"}
      ],
      "insert": {"@id": "?person", "ex:recommended": {"@id": "?item"}}
    }
  ]
}
```

If `ex:friend` is declared as a `owl:SymmetricProperty`, OWL 2 RL
materializes the reverse friendship links, and then the Datalog rule can
find items liked by mutual friends.

## Execution model

### Fixpoint evaluation

Rules execute in a **fixpoint loop**:

1. All rules are applied against the current data (base + previously derived
   facts).
2. New facts produced in this iteration are collected.
3. If any new facts were produced, go back to step 1 with the expanded fact set.
4. When no new facts are produced (fixpoint reached), the loop terminates.

This means:
- **Recursive rules work.** A rule can produce facts that trigger itself again.
- **Rule chaining works.** Rule A can produce facts that trigger Rule B, and
  vice versa.
- **Termination is guaranteed** by a maximum fixpoint-iteration bound and by
  the shared reasoning budget — a maximum derived-fact count and a maximum
  wall-clock time, the same budget OWL2-RL uses. Hitting the budget stops
  early and marks the result `capped` in the tracked response's `reasoning`
  block. Configure it with `f:reasoningMaxFacts` / `f:reasoningMaxSeconds`
  (ledger config), `"reasoningBudget"` (query), or
  `FLUREE_REASONING_MAX_FACTS` / `FLUREE_REASONING_MAX_SECONDS` (server).

### Execution order

Rules are ordered before the fixpoint by a lightweight heuristic (fewest
predicate dependencies first). Because the fixpoint re-runs every rule each
iteration until no new facts are produced, this ordering only affects how
quickly the fixpoint converges — never the final set of derived facts. Rule
chaining (rule A's output feeding rule B) works regardless of the order.

### Interaction with OWL 2 RL

When both OWL 2 RL and Datalog are enabled:

1. OWL 2 RL materialization runs first.
2. Datalog rules run over the combined base data + OWL-derived facts.
3. Both result sets are merged into a single overlay for query execution.

## Filter expressions

Filters use S-expression syntax within the `where` array:

```json
["filter", "(expression)"]
```

### Available operators

A JSON-LD rule filter is a single comparison between two operands:

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `!=` (also `not=`), `<`, `>`, `<=`, `>=` |

Logical combinators (`and` / `or` / `not`), arithmetic, and string or
type-checking functions are **not** available in JSON-LD rule filters — an
unrecognized operator is a parse error and the rule is skipped. Use several
`["filter", ...]` entries in the `where` array to require more than one
condition; they are combined with AND. For richer expressions, write the rule
in SPARQL (see [SPARQL rules](#sparql-rules)), where `FILTER(… && …)` and
`FILTER(!(…))` are supported.

Ordering operators (`<`, `<=`, `>`, `>=`) are defined for numbers and strings
only; using one against an IRI operand is rejected.

### Examples

```json
["filter", "(> ?age 21)"]
["filter", "(!= ?person ?other)"]
["filter", "(!= ?prop ex:ssn)"]
["filter", "(= ?name \"John Smith\")"]
```

Two conditions, ANDed:

```json
"where": [
  {"@id": "?person", "ex:age": "?age"},
  ["filter", "(>= ?age 18)"],
  ["filter", "(< ?age 65)"]
]
```

## Performance considerations

- **Keep rules focused.** Broad rules that match many patterns produce more
  derived facts and require more iterations.
- **Budget limits apply.** The same time/fact/memory budgets as OWL 2 RL
  materialization apply to Datalog execution (default: 30s, 1M facts, 100MB).
- **Results are cached.** The same rule set + database state returns instantly
  from cache on subsequent queries.
- **Query-time rules disable caching** across queries with different rule sets,
  since the cache key includes a hash of the rules.

## Related pages

| Topic | Page |
|-------|------|
| Conceptual introduction | [Reasoning and inference](../concepts/reasoning.md) |
| Enabling reasoning in queries | [Query-time reasoning](reasoning.md) |
| OWL & RDFS constructs | [OWL & RDFS reference](../reference/owl-rdfs-support.md) |
| Ledger-wide config | [Setting groups](../ledger-config/setting-groups.md) |
