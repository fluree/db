# Cookbook: Query Patterns

Practical recipes built on Fluree's list-value and path operators — `unwind`,
`range`, `collect`, the list functions, and `shortestPath`. These are generic
query constructs (they work on any data, not just RDF 1.2 / property-graph
edges). For the full reference, see [JSON-LD Query](../query/jsonld-query.md).

All examples assume `"@context": { "ex": "http://example.org/" }`.

## Dense / gap-filled series

**Problem.** You want a value for *every* point on an axis — every month, every
bucket, every status — including the points that have **no data**. A plain
`groupBy` can't do this: it only ever produces keys that occur in the data, so
empty periods silently vanish and a chart or report has to guess the gaps.

**Pattern.** Generate the axis with `range` + `unwind`, then LEFT JOIN the data
with `optional`. The driving rows come from the generated axis, so empty buckets
survive as zero.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?year", "(as (count ?o) ?orders)"],
  "where": [
    ["unwind", "?year", "(range 2019 2023)"],
    ["optional", { "@id": "?o", "@type": "ex:Order", "ex:orderYear": "?year" }]
  ],
  "groupBy": ["?year"],
  "orderBy": ["?year"]
}
```

```text
[[2019, 1], [2020, 2], [2021, 0], [2022, 1], [2023, 0]]
```

`2021` and `2023` appear with `0` even though no order has those years —
something neither `values` (constants only) nor a triple pattern (stored data
only) can produce. If the bounds should come from the data, compute the
min/max in a sub-select and feed them to `range`.

## Collecting values into a list

**Problem.** Instead of one row per (author, paper), you want one row per author
with the list of their papers.

**Pattern.** `collect` folds a group's values into a single list value (a JSON
array).

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?name", "(as (collect ?title) ?papers)"],
  "where": [
    { "@id": "?a", "ex:name": "?name", "ex:authored": "?paper" },
    { "@id": "?paper", "ex:title": "?title" }
  ],
  "groupBy": ["?a", "?name"]
}
```

```text
[["Alice", ["Graphs", "Indexes", "Graphs"]], ["Bob", ["Joins"]]]
```

Use `collect-distinct` to drop duplicates that arrive via a join (e.g. the same
subject reached through two papers):

```json
{ "select": ["?name", "(as (collect-distinct ?subject) ?subjects)"] }
```

> RDF stores identical triples once, so `collect` and `collect-distinct` differ
> only when a value reaches the aggregate on multiple solution rows (typically
> through a join), not when a predicate is merely repeated in the source data.

## Round-trip: collect, transform, re-expand

**Problem.** You need to operate on a group *as a list* — sort it, take the
first few, dedup it — and then go back to one row per element.

**Pattern.** `collect` in a sub-select, then `unwind` the result in the outer
query. `unwind` is the inverse of `collect`.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?subject"],
  "where": [
    ["query", {
      "@context": { "ex": "http://example.org/" },
      "select": ["(as (collect-distinct ?s) ?subjects)"],
      "where": [
        { "@id": "ex:alice", "ex:authored": "?paper" },
        { "@id": "?paper", "ex:subject": "?s" }
      ]
    }],
    ["unwind", "?subject", "?subjects"]
  ],
  "orderBy": ["?subject"]
}
```

This yields Alice's distinct subjects, one per row — deduped in the list stage,
then re-expanded.

## Working with list values

`range`, `list`, and `collect` produce lists; the list functions inspect and
transform them. They compose, and pair naturally with `unwind`.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["?count", "?first", "?last"],
  "where": [
    ["bind", "?nums", "(range 1 100)"],
    ["bind", "?count", "(size ?nums)"],
    ["bind", "?first", "(head ?nums)"],
    ["bind", "?last", "(nth ?nums -1)"]
  ]
}
```

```text
[[100, 1, 100]]
```

- `(size ?l)`, `(head ?l)`, `(last ?l)`, `(nth ?l ?i)` (0-based; negatives from
  the end), `(tail ?l)`, `(reverse ?l)`.
- `tail` / `reverse` return lists, so compose them: `(head (reverse ?l))` is the
  last element.

The same functions apply to a list produced by `collect` (in a sub-select) or by
`nodes` on a `shortestPath` result.

## Generating a sequence

`range` + `unwind` is a row generator — useful for pagination windows, numeric
buckets, or any fixed sequence the data doesn't contain.

```json
{
  "select": ["?page"],
  "where": [["unwind", "?page", "(range 1 10)"]]
}
```

For a constant *set* of values, prefer [`values`](../query/jsonld-query.md#values-patterns)
— it's clearer and cheaper. Reach for `unwind` when the list is **computed**
(`range`, `collect`, or a bound list).

## Shortest path between two entities

**Problem.** How are two people connected, and how many hops apart are they?

**Pattern.** `shortestPath` binds the path to a variable; the path functions
read it.

```json
{
  "@context": { "ex": "http://example.org/" },
  "select": ["(as (size (path-pairs ?p)) ?hops)"],
  "where": [
    ["shortestPath", {
      "from": "ex:alice", "to": "ex:dan",
      "via": "ex:knows", "direction": "out",
      "maxHops": 6, "bind": "?p"
    }]
  ]
}
```

- `(path-pairs ?p)` is the list of consecutive node pairs, so its `size` is the
  hop count; `(nodes ?p)` is the ordered node list.
- `["allShortestPaths", { … }]` returns one row per minimal-length path when
  several tie.
- List the actual nodes along the path by unwinding `nodes`:

```json
{
  "select": ["?node"],
  "where": [
    ["shortestPath", { "from": "ex:alice", "to": "ex:dan", "via": "ex:knows", "bind": "?p" }],
    ["unwind", "?node", "(nodes ?p)"]
  ]
}
```

## See also

- [JSON-LD Query](../query/jsonld-query.md) — full reference for `unwind`,
  aggregation functions, list/path functions, and `shortestPath`.
- [Property paths](../query/jsonld-query.md#property-paths) — transitive
  traversal that matches across many hops but binds only the endpoint (vs.
  `shortestPath`, which binds the whole path).
- [Edge annotations](../concepts/edge-annotations.md) — attaching metadata to
  relationships (RDF 1.2 / property-graph edges).
