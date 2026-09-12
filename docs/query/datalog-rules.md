# Datalog Rules

Datalog rules let you define custom inference logic that goes beyond what
OWL and RDFS provide. A rule is an ordinary query `where` clause — the
**body** — plus an `insert` clause naming the triples to derive for every
solution — the **heads**. Rules run in a fixpoint loop that can chain rules
together and recurse.

Rule bodies are evaluated by the same query engine as any other query, so a
rule can use everything a `where` clause can: edge annotations (claims and
other relationship metadata), the full FILTER expression language, BIND,
VALUES, UNION, property paths and subqueries. The only constructs a rule
body may not use are the non-monotonic ones — OPTIONAL, MINUS, `not-exists`,
aggregates and SERVICE — which a fixpoint cannot evaluate soundly; those are
rejected with an error naming the construct (see [Validation](#validation-a-rule-that-cannot-run-is-an-error)).

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
| `where` | Yes | The rule body — any query `where` clause (see [Body](#body-the-where-clause)) |
| `insert` | Yes | Pattern(s) of new facts to derive when the body matches |
| `@id` | No | Optional name/IRI for the rule (used in diagnostics) |

### Body: the `where` clause

The body is parsed and lowered exactly like a query's `where` clause, using
the rule's own `@context`.

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

**Property-position variables (the predicate is a variable):**
```json
"where": [
  {"@id": "?s", "ex:sameAs": {"@id": "?other"}},
  {"@id": "?other", "?prop": "?val"}
]
```
A variable in predicate position matches any predicate and binds it; the
bound predicate can be reused in the `insert` clause.

**Edge annotations (claims and relationship metadata):**
```json
"where": [
  {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"ex:confidence": "?c"}}},
  ["filter", "(> ?c 0.85)"]
]
```
Binding an annotation returns one solution per annotation, so a fact asserted
by two sources with different confidence values is considered once per claim.
The claim-first form works too — start from the metadata and walk back to
the edge with `@reifies`:
```json
"where": [
  {"ex:confidence": "?c", "ex:source": {"@id": "ex:sourceB"},
   "@reifies": {"@id": "?a", "ex:knows": {"@id": "?b"}}},
  ["filter", "(> ?c 0.85)"]
]
```
See [Edge annotations](../concepts/edge-annotations.md) for the model.

**Filters, BIND, VALUES, UNION, paths:** the same syntax as in queries —
`["filter", "(and (> ?age 18) (< ?age 65))"]`, `["bind", "?pct", "(* ?c 100)"]`,
`["values", ["?src", [{"@id": "ex:sourceA"}, {"@id": "ex:sourceB"}]]]`,
`["union", {...}, {...}]`, and `@path` aliases for transitive traversal. See
[JSON-LD query](jsonld-query.md).

#### Filter operands

A filter operand is classified by how it is written:

| Operand | Read as |
|---------|---------|
| `?name` | Variable |
| `62`, `1.5`, `true` | Number / boolean literal |
| `ex:ssn` (prefix defined in the rule's `@context`), `<http://example.org/ssn>`, `http://example.org/ssn` | **IRI** — compared by term identity, as in SPARQL |
| `"senior"`, `"John Smith"` | String literal (quoted; may contain spaces) |
| `foo:ssn` (prefix **not** defined in the `@context`) | **Rejected** — the rule is invalid (see below) |
| `senior` (unquoted bare word) | **Rejected** — quote it for a string, prefix it for an IRI |

Three rules worth knowing:

- **IRI comparison is by identity.** `(= ?p ex:knows)` matches `ex:knows` and
  not `foaf:knows`. `=` and `!=` are the meaningful comparisons for IRIs.
- **A bare word is rejected, not guessed.** `(= ?p knows)` is an error naming
  the operand: write `"knows"` (quoted) to compare against the string, or
  `ex:knows` to compare against the IRI. Against an IRI-bound variable a
  string comparison fails invisibly in both directions — `=` derives nothing,
  and `!=` keeps every row, so an exclusion filter derives exactly the facts it
  was written to exclude.
- **An unresolvable IRI operand is an error, not a fallback.** If a filter
  names a prefix the rule's `@context` does not define, or a namespace the
  ledger has never seen, the rule is rejected rather than run with a filter
  that cannot match.

### Insert clause

The `insert` clause defines the triples to produce for each solution of the
body.

```json
"insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
```

- Variables in any position — subject, **predicate**, or object — are replaced
  with the bound values from `where`. A predicate variable lets a rule write a
  property whose name is computed: `"insert": {"@id": "?s", "?prop": "?val"}`
  copies every `?prop`/`?val` bound in the `where` clause onto `?s`.
- Use `{"@id": "?var"}` or `{"@id": "ex:iri"}` for IRI/entity values; use
  `"?var"` directly for literal values. Typed literals use the value-object
  form: `{"@value": "2024-01-01", "@type": "xsd:date"}`; language-tagged
  strings use `{"@value": "chat", "@language": "fr"}`.
- `@type` derives `rdf:type` triples; an array of objects (or an `@graph`)
  derives several heads from one rule.
- **Every variable used in `insert` must also appear in `where`.** A head that
  references a variable the body never binds rejects the whole rule with an
  error naming the variable (a `where`/`insert` typo is the usual cause):

  ```json
  "where":  {"@id": "?s", "ex:relType": {"@id": "?relation"}},
  "insert": {"@id": "?s", "?rel": {"@id": "?s"}}
  ```

  `?rel` is never bound — the `where` clause binds `?relation`.
- Every node in an `insert` pattern needs an `@id`. An anonymous node has no
  subject to derive facts about.
- Heads derive plain triples. An `@annotation` block in a head (deriving an
  edge annotation, for example to record which claims a derived edge rests on)
  is not supported yet and is rejected by name; see
  [Reifier minting in rule heads](../design/rules-engine.md#reifier-minting-in-rule-heads-options).

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

Rules can be stored in the database as `f:rule` assertions. A JSON-LD rule is
stored as a `@json` literal — the `@type: "@json"` wrapper is required, so
the rule body's `?variables` are kept as data rather than read as transaction
template variables:

```json
{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/"
  },
  "insert": {
    "@id": "ex:grandparentRule",
    "f:rule": {
      "@type": "@json",
      "@value": {
        "@context": {"ex": "http://example.org/"},
        "where": {"@id": "?person", "ex:parent": {"ex:parent": "?gp"}},
        "insert": {"@id": "?person", "ex:grandparent": {"@id": "?gp"}}
      }
    }
  }
}
```

Stored rules apply to any query that enables datalog reasoning
(`"reasoning": "datalog"`). To apply them to every query without asking,
set a ledger default in the config graph:

```sparql
PREFIX f: <https://ns.flur.ee/db#>
INSERT DATA {
  GRAPH <urn:fluree:mydb:main#config> {
    <urn:fluree:mydb:main:config:ledger> a f:LedgerConfig ;
      f:reasoningDefaults [ f:reasoningModes f:Datalog ] ;
      f:datalogDefaults [
        f:datalogEnabled true ;
        f:rulesSource [ a f:GraphRef ; f:graphSource [ f:graphSelector f:defaultGraph ] ] ;
        f:allowQueryTimeRules true
      ] .
  }
}
```

See [Setting groups — datalogDefaults](../ledger-config/setting-groups.md) for
full configuration options and [Writing config data](../ledger-config/writing-config.md)
for the other ways to write the config graph.

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

A SPARQL rule body may use anything a SPARQL WHERE clause may, including the
RDF 1.2 annotation surface, subject to the same monotonicity rule as JSON-LD
rules:

```sparql
PREFIX ex: <http://example.org/>
CONSTRUCT { ?a ex:trustedKnows ?b }
WHERE { ?a ex:knows ?b {| ex:confidence ?c |} FILTER(?c > 0.85) }
```

```sparql
PREFIX ex:  <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
CONSTRUCT { ?a ex:trustedKnows ?b }
WHERE { ?claim rdf:reifies <<( ?a ex:knows ?b )>> ; ex:confidence ?c FILTER(?c > 0.85) }
```

Include `PREFIX` declarations in the rule text; the request `@context` is not
applied to rule sources. A CONSTRUCT template may not contain blank nodes (a
head cannot mint fresh nodes — every round would mint new ones) and, like a
JSON-LD head, may not carry an annotation tail yet.

## Validation: a rule that cannot run is an error

A rule that fails to parse or validate **fails the query** with an error
naming the rule (its `@id`, or its position in the `rules` array) and the
problem. This applies to stored rules too: a ledger whose rule set contains a
broken rule fails its reasoning queries until the rule is fixed. Answering
over an incomplete rule set would be silently wrong, which is worse.

Rejected:

- **Non-monotonic constructs in the body**: OPTIONAL (`["optional", …]`),
  MINUS, NOT EXISTS (`["not-exists", …]`), GROUP BY / aggregates, SERVICE.
  A fixpoint cannot evaluate negation or left joins soundly without
  stratification.
- **A head variable the body never binds** (range restriction).
- **Filter operands that cannot match**: a bare unquoted word, an undefined
  prefix, or a namespace the ledger has never seen (see
  [Filter operands](#filter-operands)).
- **A head that would mint an annotation or a blank node.**
- Malformed rule documents: a missing `where` or `insert`, a filter element
  that is not a string or array expression.

Positive existence checks (`["exists", …]`), UNION, BIND, VALUES, property
paths and subqueries without aggregates are all allowed.

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
        {"@id": "?sibling", "ex:parent": "?parent"},
        ["filter", "(!= ?person ?sibling)"]
      ],
      "insert": {"@id": "?person", "ex:sibling": {"@id": "?sibling"}}
    }
  ]
}
```

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

### Trusted edges from claims

Two sources make claims about the same fact; derive a `trustedKnows` edge
from any claim above a threshold, then walk only trusted edges:

```json
{
  "@context": {"ex": "http://example.org/", "trusted+": {"@path": ["+", "ex:trustedKnows"]}},
  "select": ["?reachable"],
  "where": {"@id": "ex:alice", "trusted+": {"@id": "?reachable"}},
  "reasoning": "datalog",
  "rules": [
    {
      "@context": {"ex": "http://example.org/"},
      "where": [
        {"@id": "?a", "ex:knows": {"@id": "?b", "@annotation": {"ex:confidence": "?c"}}},
        ["filter", "(> ?c 0.85)"]
      ],
      "insert": {"@id": "?a", "ex:trustedKnows": {"@id": "?b"}}
    }
  ]
}
```

Derived predicates are ordinary predicates to the rest of the query, so a
property path over `ex:trustedKnows` traverses the derived edges.

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

1. Every rule body is evaluated by the query engine against the current data
   (base + previously derived facts).
2. The heads are instantiated for each solution; new facts are collected.
3. If any new facts were produced, go back to step 1 with the expanded fact set.
4. When no new facts are produced (fixpoint reached), the loop terminates.

This means:
- **Recursive rules work.** A rule can produce facts that trigger itself again.
- **Rule chaining works.** Rule A can produce facts that trigger Rule B, and
  vice versa.
- **Termination is guaranteed** by a maximum fixpoint-iteration bound and by
  the shared reasoning budget — a maximum derived-fact count, a maximum
  memory estimate for the derived facts, and a maximum wall-clock time, the
  same budget OWL2-RL uses. The budget is checked after every derived fact,
  so a single round cannot overshoot it. Hitting the budget stops early and
  marks the result `capped` in the tracked response's `reasoning` block.
  Configure it with `f:reasoningMaxFacts` / `f:reasoningMaxSeconds` (ledger
  config), `"reasoningBudget"` (query), or `FLUREE_REASONING_MAX_FACTS` /
  `FLUREE_REASONING_MAX_SECONDS` (server).

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

## Performance considerations

- **Rule bodies are planned queries.** They take the same index lanes and join
  ordering as any query, so the usual query advice applies: put a selective
  pattern in the body, and prefer bound predicates.
- **Budget limits apply.** The same time / fact / memory budgets as OWL 2 RL
  materialization apply to Datalog execution (default: 30s, 1M facts, 100MB).
- **Results are cached.** A materialization is cached per ledger state and
  rule set: the same rules (stored or query-time, hashed by content) against
  an unchanged ledger return instantly from the reasoning cache. Any commit
  invalidates the entry.

## Related pages

| Topic | Page |
|-------|------|
| Conceptual introduction | [Reasoning and inference](../concepts/reasoning.md) |
| Enabling reasoning in queries | [Query-time reasoning](reasoning.md) |
| Edge annotations (claims) in rule bodies | [Edge annotations](../concepts/edge-annotations.md) |
| Engine design and the head-annotation decision | [Rules engine design](../design/rules-engine.md) |
| OWL & RDFS constructs | [OWL & RDFS reference](../reference/owl-rdfs-support.md) |
| Ledger-wide config | [Setting groups](../ledger-config/setting-groups.md) |
