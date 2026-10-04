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

    rows = people.query("""
        PREFIX ex: <http://example.org/>
        SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age } ORDER BY ?name
    """)
    for name, age in rows:
        print(name, age)              # Alice 42, then Bob 37 — native str and int
    df = rows.to_pandas()

    # Every past state stays queryable.
    before_bob = people.at(t=commit.t - 1)
    print(before_bob.query("PREFIX ex: <http://example.org/> ASK { ex:bob ?p ?o }"))  # False
```

## Writing

`insert`, `upsert` and `update` each make one commit and return a `Commit`
(`t`, `id`, `digest`, `asserts`, `retracts`).

- `insert(data)` and `upsert(data)` take JSON-LD (a dict, list, or JSON text),
  Turtle or TriG text, or a path to a file.
- `update(txn)` takes SPARQL UPDATE, or JSON-LD `where`/`delete`/`insert`.
- `message=` on any of them is recorded with the commit; `ledger.log()` shows
  it.
- `sync(data, graph=None)` makes the default graph, or a named graph, hold
  exactly `data`, committing only the difference; `dry_run=True` counts what
  would change. Handy for mirroring an export from another system.

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
once and is left out. The commit holds only the net change, and if another
commit lands first, the writes are staged again on top of it.

## Querying

- **SPARQL** `SELECT` returns `Rows`: named tuples in the query's column order,
  with `to_dicts()` and `to_pandas()`. Literals are Python values (`int`,
  `float`, `Decimal`, `datetime`, `str`, ...); IRIs are `fluree.IRI` and
  language-tagged strings `fluree.LangString`, both `str` subclasses. A literal
  with no lossless Python type stays a `fluree.Literal`. `ASK` returns a `bool`;
  `CONSTRUCT` returns a JSON-LD document.
- **JSON-LD** queries (a `dict`) return their JSON result as Python objects.
- `ledger.stream(query)` reads a large `SELECT` row by row in flat memory;
  leaving the loop early stops the query.
- `query(..., max_fuel=..., timeout=...)` bounds a query's work and time;
  `profile(query)` reports what it cost, and `explain(query)` shows the plan.
  Ctrl-C cancels a running query.
- `conn.query(...)` runs a query whose `FROM` names the ledgers, so it can span
  several.
- `ledger.set_context({...})` sets the default JSON-LD context: queries that
  omit `PREFIX` or `@context` resolve prefixes against it.

`ledger.query(...)` reads the latest state. `ledger.snapshot()` and
`ledger.at(t=..., time=..., commit=...)` return a frozen view that every query
sees identically.

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


`ledger.with_policy(identity=..., policy_class=..., policy=..., values=...,
default_allow=...)` returns a governed handle: reads through it are filtered by
policy, and a write the policy does not allow raises
`fluree.PermissionDeniedError`. History and commit contents are filtered the
same way.

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
completes.

## Concurrency

Engine calls release the GIL, so threads can query in parallel. Several
processes may share one database directory; a commit that loses a race to
another process is retried against the new state. Do not fork a process after
it has used Fluree — use the `spawn` or `forkserver` multiprocessing start
method.

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
