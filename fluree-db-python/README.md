# fluree

Python package for [Fluree](https://flur.ee), a graph database with time travel,
history, and fine-grained policy. The engine runs in your Python process — there
is no server to start.

```python
import fluree

with fluree.connect("./data") as conn:          # or fluree.connect(":memory:")
    people = conn.create("people")

    people.insert("""
        @prefix ex: <http://example.org/> .
        ex:alice ex:name "Alice" ; ex:age 42 .
    """)
    commit = people.insert({
        "@context": {"ex": "http://example.org/"},
        "@id": "ex:bob", "ex:name": "Bob", "ex:age": 37,
    })

    rows = people.select("""
        PREFIX ex: <http://example.org/>
        SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age } ORDER BY ?name
    """)
    for name, age in rows:
        print(name, age)              # Alice 42, then Bob 37 — native str and int
    df = rows.to_pandas()             # with pandas installed: pip install "fluree[pandas]"

    # Every past state stays queryable.
    before_bob = people.at(t=commit.t - 1)
    print(before_bob.query("PREFIX ex: <http://example.org/> ASK { ex:bob ?p ?o }"))  # False
```

## Writing

`insert`, `upsert` and `update` each make one commit and return a `Commit`
(`t`, `id`, `digest`, `asserts`, `retracts`).

- `insert(data)` and `upsert(data)` take JSON-LD (a dict, list, or JSON text),
  Turtle or TriG text, or a path to a file.
- `update(txn)` takes SPARQL UPDATE, a Cypher write (`CREATE`, `MERGE`,
  `SET`, `DELETE`, or a `;` script of them, committed all or nothing), or
  JSON-LD `where`/`delete`/`insert`. The language is told from the text;
  `language=` overrides it.
- `message=` on any of them is recorded with the commit; `ledger.log()` shows
  it.
- `sync(data, graph=None)` makes the default graph, or a named graph, hold
  exactly `data`, committing only the difference; `dry_run=True` counts what
  would change. Handy for mirroring an export from another system.

A property value in a JSON-LD dict may be any value a query returns, and
reads back as it went in: a `fluree.IRI` or `BlankNode` is a reference to that
node, a `LangString` keeps its tag, a `Literal` its datatype, and a `Decimal`,
`datetime`, `date` or `time` its XSD type. `fluree.Vector` and numpy arrays
are embedding vectors. The same values work as query parameters and in JSON-LD
`where` patterns. Keyword entries (`@id`, `@type`, `@context`) take plain
strings.

To make several writes one commit, use a transaction. Each write applies over
the ones before it and is checked as it is staged; queries on the transaction
read the staged state, which no one else sees until it commits:

```python
with people.transaction(message="onboard bob") as txn:
    txn.insert({...})
    txn.update("DELETE { ... } INSERT { ... } WHERE { ... }")   # sees the insert
    txn.query("SELECT ...")
print(txn.committed)     # the Commit; an exception in the block rolls back
```

A write that fails (bad syntax, denied by policy, a SHACL violation) raises at
once and is left out, all of it: a Cypher script that fails part way leaves
none of its statements staged. The commit holds only the net change.

If another commit lands first, a transaction that only wrote is staged again
on top of it (each update's `WHERE` matches the new data). One that was also
read raises `fluree.ConflictError` instead, since what it read may have
decided what it wrote. To read and then write safely, hand the work to
`transact`, which runs it again on a conflict:

```python
def birthday(txn):
    (age,) = txn.query("SELECT ?age WHERE { ex:alice ex:age ?age }")[0]
    txn.upsert({"@id": "ex:alice", "ex:age": age + 1})

people.transact(birthday)
```

## Querying

`query()` takes SPARQL, Cypher, or JSON-LD — told apart by how the query
opens, or set with `language=` — and reads the latest state.
`ledger.snapshot()` and `ledger.at(t=..., time=..., commit=...)` return a
frozen view that every query sees identically.

- SPARQL `SELECT` and every Cypher query return a `Result` of `Record`s in
  the query's column order. A record unpacks like a tuple and reads by
  position, key, or attribute (`r[0]`, `r["name"]`, `r.name`); the result has
  `keys()`, `single()`, `value(key)`, `values()`, `data()` and
  `to_pandas()` (alias `to_df()`) — the Neo4j driver's vocabulary.
- Literals are Python values (`int`, `float`, `Decimal`, `datetime`, `str`,
  ...); IRIs are `fluree.IRI` and language-tagged strings
  `fluree.LangString`, both `str` subclasses. A literal with no lossless
  Python type stays a `fluree.Literal`.
- SPARQL `ASK` returns a `bool`, `CONSTRUCT` a JSON-LD document; JSON-LD
  queries (a `dict`) return their JSON result as Python objects.
- `select()` is `query()` for tables: it takes a SPARQL `SELECT` or a Cypher
  query and is typed to return a `Result`, so editors and type checkers know
  what comes back. Any other query is refused before it runs.
- `ledger.stream(query)` reads a large `SELECT` row by row in flat memory.
  Open it with `with ledger.stream(query) as rows:` when the loop may stop
  early: leaving the block stops the query.
- `query(..., max_fuel=..., timeout=...)` bounds a query's work and time;
  `profile(query)` reports what it cost, and `explain(query)` shows the plan.
  Ctrl-C cancels a running query.
- `conn.query(...)` runs a query whose `FROM` names the ledgers, so it can span
  several.
- `ledger.set_context({...})` sets the default JSON-LD context: queries that
  omit `PREFIX` or `@context` resolve prefixes against it.
- Parameters bind values by name, from a dict or keyword arguments, in
  `query()`, `update()`, `stream()`, `explain()` and `profile()`. In SPARQL a
  parameter replaces its variable (`?name` or `$name`) wherever it appears, as
  if the value were written there:

  ```python
  people.query("SELECT ?s WHERE { ?s ex:name $name ; ex:age ?age FILTER(?age > $min) }",
               name="Alice", min=21)
  ```

  A value is an `IRI`, `BlankNode`, `LangString` or `Literal`, a Python
  `str`, `int`, `float`, `bool`, `Decimal`, `datetime`, `date` or `time`, or
  a Cypher `Node` (its `element_id`). A parameter the query never mentions
  raises `InvalidRequestError` rather than leaving a misspelt variable
  unbound. JSON-LD queries take none.

### Cypher

```python
result = people.query(
    "MATCH (p:Person {name: $name})-[r:KNOWS]->(friend) RETURN p, r, friend.name AS friend",
    name="Alice",
)
for record in result:
    record.friend, record["p"]["age"], record["r"].type

commit = people.update("CREATE (p:Person {name: $name}) RETURN p", name="Eve")
commit.result.single()["p"]
```

- `$name` parameters come from a dict or keyword arguments, as for SPARQL.
- Nodes, relationships and paths come back as `fluree.Node` (properties read
  like a dict, plus `labels` and `element_id` — the same `IRI` SPARQL returns
  for it), `fluree.Relationship` (`type`, `start_node`, `end_node`) and
  `fluree.Path`.
- A Cypher write runs through `update()`; the records its `RETURN` produces
  are the commit's `result`. Inside `with people.transaction() as txn:`,
  `txn.update(...)` stages Cypher alongside SPARQL and JSON-LD writes, each
  seeing the others, in one commit.
- Cypher and SPARQL see the same data: Cypher's names are bare IRIs (`Person`
  is `<Person>`).
- Not yet for Cypher: `max_fuel`, `profile()` and `stream()`.

## Search

Full-text and vector search run inside queries, so a search joins with any
other pattern and sees the same data: staged writes in a transaction, a past
`t`, a branch.

```python
people.set_full_text(["ex:title", "ex:body"], language="en")  # reindexes by default
people.query("""
    SELECT ?doc ?score WHERE {
      ?doc ex:body ?body .
      BIND(fulltext(?body, $q) AS ?score) FILTER(?score > 0)
    } ORDER BY DESC(?score) LIMIT 10""", q="graph databases")

people.insert({"@context": ctx, "@id": "ex:doc1", "ex:embedding": fluree.Vector(model.encode(text))})
people.query("""
    SELECT ?doc ?score WHERE {
      ?doc ex:embedding ?v .
      BIND(dotProduct(?v, $q) AS ?score)
    } ORDER BY DESC(?score) LIMIT 5""", q=query_embedding)
```

- `set_full_text(properties)` makes the plain-string values of those
  properties searchable with `fulltext()`, analyzed in `language`;
  language-tagged values use their own. `full_text()` reads the setting back.
  A property becomes searchable once an index build has seen values of it, so
  configure after loading data, or call `reindex()` after the first load.
- `fluree.Vector` holds an embedding (stored as float32); a one-dimensional
  numpy array works anywhere a `Vector` does, as a value or a parameter.
  Vectors come back from queries as `Vector`, and `numpy.asarray(v)` gives
  a float32 array.
- `cosineSimilarity`, `dotProduct` and `euclideanDistance` score vectors;
  `dotProduct` over normalized embeddings is the fastest.

## History

- `ledger.history(subject, predicate=None, from_t=1)` lists every assertion and
  retraction of a subject, oldest first.
- `ledger.log()` lists commits, newest first; `ledger.changes(commit)` lists
  what one commit asserted and retracted.

## Branches

A branch is a ledger that shares another's history up to the point it was
created, then changes on its own. Branch ids are `ledger:branch`; a ledger's
first branch is `main`.

```python
dev = people.branch("dev")                  # or people.at(t=5).branch("fix")
dev.insert({...})

preview = people.merge_preview("dev")       # ahead/behind commits, conflicts
if preview.mergeable:
    people.merge("dev")                     # bring dev's commits into main
```

- `ledger.branches()` lists a ledger's branches; `conn.drop("people:dev")`
  drops one.
- `ledger.merge(source, strategy=...)` settles properties both branches changed:
  `"take-both"` (default) keeps both values, `"take-source"` or
  `"take-branch"` picks a side, `"abort"` raises `fluree.ConflictError`.
  `merge_preview(...)` reports what a merge would do, including the net
  changes (`changes=True`) and what each side wrote to each conflict
  (`details=True`).
- `branch.rebase()` replays a branch's own commits on top of the latest
  commit of the branch it came from.
- `ledger.revert(commits)` undoes one or more commits in a new commit;
  `revert_preview(...)` checks first.

## Policy

`ledger.with_policy(identity=..., policy_class=..., policy=..., values=...,
default_allow=...)` returns a governed handle: reads through it are filtered by
policy, and a write the policy does not allow raises
`fluree.PermissionDeniedError`. History and commit contents are filtered the
same way.

Policy belongs to the handle. A query or write on a ledger that names its own
policy — JSON-LD `"opts": {"identity": ...}` or a SPARQL `# PRAGMA identity` —
raises `fluree.InvalidRequestError`, so a query passed in from elsewhere can
never widen what a governed handle sees.

## Maintenance

- `ledger.validate()` checks the data against the ledger's SHACL shapes — or
  other shapes passed in — and returns a report, changing nothing.
- `ledger.index_status()`, `ledger.index()` (index now and wait) and
  `ledger.reindex()` manage indexing; queries never need it, but indexed data
  reads faster.
- `ledger.verify()` checks that the commit chain and index are intact, and
  `ledger.sweep()` deletes index files no index references any more.

## Backup and export

- `ledger.export(path, format=...)` writes Turtle, TriG, N-Triples, N-Quads or
  JSON-LD (or returns text when no path is given); `ledger.at(...).export()`
  exports a past state.
- `ledger.archive(path)` writes a `.flpack` archive of the whole ledger, and
  `conn.restore(path, name)` loads one back.

## Storage

`fluree.connect(path)` stores data in a directory. For S3 or split
commit/index storage, a DynamoDB nameservice, or encryption at rest, pass a
JSON-LD connection document instead: `fluree.connect(config={...})` — the same
document `fluree server` takes with `--connection-config`.

## Errors

Every error is a `fluree.FlureeError`, and also the matching builtin where one
fits: `NotFoundError` is a `LookupError`, `InvalidRequestError` a `ValueError`,
`PermissionDeniedError` a `PermissionError`, `QueryTimeoutError` a
`TimeoutError`.

## asyncio

`fluree.aio` is the same API with coroutines, for FastAPI and other asyncio
applications:

```python
import fluree.aio

async with fluree.aio.connect("./data") as conn:
    people = await conn.ledger("people")
    rows = await people.query("SELECT ...")
    async for row in people.stream("SELECT ..."):
        ...
    async with people.transaction() as txn:
        await txn.insert({...})
```

Each call runs on a worker thread while the event loop carries on.
Cancelling a task that awaits a query (`asyncio.timeout`, a client that
disconnects) stops the query in the engine; a write already under way still
completes. Cancelled inside `async with ledger.transaction()`, the block waits
for that write to finish, rolls the transaction back, and re-raises the
cancellation.

## Concurrency

Engine calls release the GIL, so threads can query in parallel. Several
processes may share one database directory; a commit that loses a race to
another process is retried against the new state.

Forked processes (a `multiprocessing` pool with the `fork` start method,
gunicorn workers) open their own connection: one opened before the fork
raises `FlureeError` in the child rather than half-working, and is left
alone when the child exits. On Linux a child connects as usual, ideally
forked while the parent is not mid-query. On macOS the system refuses
threaded work in a child forked after Fluree started, so use the `spawn`
start method there (the macOS default) or `forkserver`; a child forked
before Fluree was first used is fine on either.

## Development

```sh
uv venv && uv pip install maturin pytest pandas
maturin develop          # build the extension into the active environment
pytest
```

## License

Business Source License 1.1. Embedding Fluree as a component of your own
application or service is permitted; offering it to third parties as a hosted
database service is not. Each version converts to the Apache License 2.0 four
years after its release. See the LICENSE file in the repository root.
