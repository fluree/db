## Query Optimization and Explain API

This guide explains how Fluree reorders query patterns to run faster, and how you can use the Explain API to see what the optimizer did and why.

### What is being optimized?

Fluree queries are made of patterns (triples and clauses). Some patterns are much more selective than others. The optimizer rearranges patterns within safe boundaries to reduce intermediate results and total work.

High‑level rules the optimizer follows:
- Prefer patterns that return fewer rows first (more selective).
- Avoid reordering across boundaries like FILTER, OPTIONAL, UNION, VALUES, etc.
- Keep overall query semantics the same – this is a cost-based ordering only.

### What signals does the optimizer use?

Fluree maintains lightweight statistics during indexing:
- Property counts: how many triples exist for a property.
- Class counts: how many entities exist for a class.
- Distinct counts (NDV): approximate number of distinct values (per property) and distinct subjects (per property).

These are used to estimate the number of rows each pattern would produce. Lower estimates run first.

Examples of estimates used:
- Bound object (e.g., `?s :email "alice@..."`): estimated rows ≈ `count(:email) / NDV(values|:email)`.
- Bound subject (e.g., `?person :friend ?f` after `?person` is known): ≈ `count(:friend) / NDV(subjects|:friend)`.
- Predicate‑only scan (e.g., `?s :status ?o`): ≈ `count(:status)`.
- Class (e.g., `?s a :Person`): ≈ `classCount(:Person)`.

About NDV: NDV stands for “Number of Distinct Values.” Fluree estimates NDV efficiently using a standard probabilistic sketch (HyperLogLog, precision 8). It’s compact and fast, and accurate enough to guide ordering decisions.

### What does the optimizer reorder?

Within each segment of optimizable patterns (triples and classes), the optimizer sorts patterns by estimated selectivity. It does not reorder across boundaries such as FILTER, OPTIONAL, UNION, VALUES, GRAPH, etc. Sub‑queries are optimized independently.

### Using the Explain API

Explain returns a plan showing the original order, the optimized order, and the inputs used to make that decision.

#### Clojure example

```clojure
(require '[fluree.db.api :as fluree])

(let [conn   @(fluree/connect-memory)
      _      @(fluree/create conn "opt-demo")
      query  {:context {"ex" "http://example.org/"}
              :select ["?name" "?email"]
              :where [{"@id" "?p", "@type" "ex:Person"}
                      {"@id" "?p", "ex:email" "alice@example.org"}
                      {"@id" "?p", "ex:name"  "?name"}]}
      plan   @(fluree/query conn (assoc-in query [:opts :explain?] true))]
  plan)
```

Explain output includes:
- `[:plan :statsSummary]` (i.e., `(get-in plan [:plan :statsSummary])`): counts and NDV availability summary.
- `[:plan :segments]`: for each optimizable segment, both original and optimized order with:
  - `:score` (lower runs first)
  - `:inputs` (e.g., `:count`, `:ndvValues`, `:ndvSubjects`, and flags like `:usedValuesNDV?`, `:fallback?`, `:clampedToOne?`).

This helps you understand why a pattern was moved earlier or later.

#### Notes
- Explain is read‑only: it does not execute the query.
- The optimizer uses only statistics kept in the index; it does not scan data at explain/query time.
- If statistics are missing for a property, the optimizer falls back to conservative defaults – explain will show a `:fallback?` flag.

### Practical tips

- Use unique or near‑unique properties (like emails, IDs) early – the optimizer will usually do this for you.
- When joining from a known subject to other properties, properties with small average fan‑out (NDV‑aware) will run earlier.
- Use VALUES for parameterization; the optimizer recognizes the shape without baking the literal values into the cache key.

### FAQ

**Does reordering change query results?**
No. Reordering only changes the evaluation order, not the semantics.

**How accurate are the estimates?**
They are “good enough” to choose better orders. NDV is approximate but compact and fast. Small inaccuracies rarely harm performance; correctness is unaffected.

**Can I see which stats were used?**
Yes – the Explain plan’s per‑pattern `:inputs` show exactly what was used to compute the estimate.

**Do I need to configure anything?**
No. Statistics are maintained during indexing. Explain works out of the box.


