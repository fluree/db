# Using Fluree from Python

The `fluree` package runs the Fluree engine inside your Python process: no
server to start, and the same engine the server and CLI run. This page gets
you started and shows how the rest of this book reads from Python. The
[Python documentation](https://fluree.github.io/db/python/) has a guide to the
whole package and its API reference, and each class and method carries its
documentation for `help()` and your editor.

## Install

```sh
pip install fluree                # or "fluree[pandas]", "fluree[polars]"
```

Wheels are published for Linux (x86-64 and arm64, glibc 2.28 or later), macOS
on Apple silicon, and Windows (x86-64), for CPython 3.10 and later. The
package version is the version of the engine inside it.

## Quick start

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
        print(name, age)              # Alice 42, then Bob 37: native str and int

    # Every past state stays queryable.
    before_bob = people.at(t=commit.t - 1)
    print(before_bob.query("PREFIX ex: <http://example.org/> ASK { ex:bob ?p ?o }"))  # False
```

`conn.ledger("people")` opens an existing ledger. Results come back as Python
values: `int`, `Decimal`, `datetime`, `str`, and `fluree.IRI` for IRIs. A
result converts to a DataFrame with `to_pandas()` or `to_polars()`.

## Reading this book from Python

The queries and transactions in this book work as written. A SPARQL, JSON-LD
or Cypher query that a page sends to the server goes to `ledger.query()`, and a
transaction body goes to `insert()`, `upsert()` or `update()`:

| In this book | From Python |
|--------------|-------------|
| [Query](../query/): JSON-LD, SPARQL or Cypher | `ledger.query(query)`, or `ledger.select(query)` for a table |
| A query whose `FROM` names ledgers ([datasets](../query/datasets.md)) | `conn.query(query)` |
| [Insert](../transactions/insert.md), [upsert](../transactions/upsert.md), [update](../transactions/update-where-delete-insert.md) | `ledger.insert(data)`, `ledger.upsert(data)`, `ledger.update(txn)` |
| [Sync](../transactions/sync.md) a graph to a payload | `ledger.sync(data, graph=...)` |
| [Time travel](../concepts/time-travel.md) (`t`, ISO time, commit) | `ledger.at(t=..., time=..., commit=...)` |
| [Branching](../guides/cookbook-branching.md) | `ledger.branch(name)`, `merge()`, `rebase()`, `revert()` |
| [Policy](../security/policy-model.md) options (`identity`, `policyClass`, ...) | `ledger.with_policy(identity=..., policy_class=...)` |
| [Ledger configuration](../ledger-config/writing-config.md) | `ledger.update(...)` writing the config graph |
| [Full-text](../indexing-and-search/fulltext.md) and [vector](../indexing-and-search/vector-search.md) search | `ledger.set_full_text(...)`, `fluree.Vector`, and the query functions |
| [Graph sources](../graph-sources/overview.md): Iceberg, Delta, SQL | `conn.map_iceberg(...)`, `conn.map_delta(...)`, `conn.map_sql(...)` |
| Export, and [archives](../operations/pack-archive-restore.md) | `ledger.export(...)`, `ledger.archive(path)`, `conn.restore(path, name)` |
| [Connection configuration](../reference/connection-config-jsonld.md) (S3, DynamoDB, encryption) | `fluree.connect(config={...})` |

A few things differ from the server:

- **Policy belongs to the handle.** Choose it with `ledger.with_policy(...)`.
  A query or write that names its own policy (JSON-LD `opts`, a SPARQL
  `# PRAGMA`) is refused, so a query passed in from elsewhere cannot widen
  what the handle sees.
- **A ledger's query reads that ledger.** A query that names other ledgers
  goes to `conn.query()`.
- **Several writes can be one commit.** `with ledger.transaction() as txn:`
  stages writes in any language together, and `ledger.transact(fn)` runs
  read-then-write work again when another commit lands first.
- **Parameters.** `query()`, `update()` and `stream()` take values by name,
  from a dict or keyword arguments: `ledger.query("... $name ...", name="Alice")`.

## asyncio

`fluree.aio` is the same API with coroutines, for FastAPI and other asyncio
applications. Cancelling a task stops its query in the engine:

```python
import fluree.aio

async with fluree.aio.connect("./data") as conn:
    people = await conn.ledger("people")
    rows = await people.query("SELECT ...")
```

## Next steps

- The [Python guide](https://fluree.github.io/db/python/guide.html) for the
  whole package (results, transactions, Cypher, search, graph sources,
  branches, errors, logging and concurrency) and the
  [API reference](https://fluree.github.io/db/python/api/fluree/).
- [Concepts](../concepts/) for how Fluree works.
- [Guides](../guides/) for task-oriented recipes; their queries run
  unchanged through `ledger.query()`.
