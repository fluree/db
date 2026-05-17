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

**With filters:**
```json
"where": [
  {"@id": "?person", "ex:age": "?age"},
  ["filter", "(>= ?age 65)"]
]
```

### Insert clause

The `insert` clause defines what facts to produce for each set of matching
variable bindings.

```json
"insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
```

- Variables (`?person`, `?gp`) are replaced with the bound values from `where`.
- Use `{"@id": "?var"}` for IRI/entity values; use `"?var"` directly for
  literal values.
- Multiple triples can be generated from a single insert pattern.

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
- **Termination is guaranteed** by the budget controls (max iterations, max
  facts, max time, max memory).

### Execution order

Rules are topologically sorted by their predicate dependencies: a rule that
generates `ex:uncle` triples runs before a rule that consumes `ex:uncle` in its
`where` clause. This minimizes the number of fixpoint iterations needed.

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

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `!=`, `<`, `>`, `<=`, `>=` |
| Logical | `and`, `or`, `not` |
| Arithmetic | `+`, `-`, `*`, `/` |
| String | `str`, `strlen`, `contains`, `strstarts`, `strends` |
| Type checking | `isIRI`, `isBlank`, `isLiteral`, `bound` |

### Examples

```json
["filter", "(> ?age 21)"]
["filter", "(and (>= ?age 18) (< ?age 65))"]
["filter", "(contains ?name \"Smith\")"]
["filter", "(!= ?person ?other)"]
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
