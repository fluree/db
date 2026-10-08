# fluree

Python package for [Fluree](https://flur.ee), a graph database with time travel,
history, and fine-grained policy. The engine runs in your Python process — there
is no server to start. The [API reference](https://fluree.github.io/db/python/api/fluree/)
documents every class and method, and the
[Fluree documentation](https://fluree.github.io/db/getting-started/python.html)
covers concepts, query languages and configuration; its queries run unchanged
through `ledger.query()`.

```sh
pip install fluree                # or "fluree[pandas]", "fluree[polars]"
```

Wheels are published for Linux (x86-64 and arm64, glibc 2.28 or later),
macOS on Apple silicon, and Windows (x86-64), for CPython 3.10 and later —
the platforms the Fluree CLI ships on. The package version is the version of
the Fluree engine inside it.

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
- `insert_rows(rows, id="ex:person/{id}", type="ex:Person")` loads a pandas
  or polars DataFrame (or a list of dicts), one node per row: columns become
  properties, missing values (`None`, `NaN`, `NaT`) add nothing, and `refs=`
  turns columns into links to other nodes. `upsert_rows` replaces existing
  values, to load a changed table again. A pandas integer column with gaps is
  a float column unless it uses the nullable `Int64` dtype.
- Named graphs are created by writing to them (TriG, SPARQL `GRAPH`, or a
  JSON-LD node's `"@graph"`). `ledger.graphs()` lists them, and
  `ledger.drop_graph(iri)` retracts one's contents in a single commit.

A property value in a JSON-LD dict may be any value a query returns, and
reads back as it went in: a `fluree.IRI` or `BlankNode` is a reference to that
node, a `LangString` keeps its tag, a `Literal` its datatype, and a `Decimal`,
`datetime`, `date` or `time` its XSD type. A `fluree.Triple` is an RDF 1.2
triple term, and `fluree.Vector` and numpy arrays are embedding vectors. The
same values work as query parameters and in JSON-LD `where` patterns. Keyword
entries (`@id`, `@type`, `@context`) take plain strings, except `"@reifies"`,
which takes the `Triple` a node is about; that records a claim without
asserting the triple:

```python
ex = "http://example.org/"
ledger.insert({"@id": ex + "claim1", ex + "source": fluree.IRI(ex + "wiki"),
               "@reifies": fluree.Triple(ex + "carol", ex + "age", 30)})
```

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
  `keys()`, `value(key)`, `values()`, `data()`, `to_pandas()` (alias
  `to_df()`) and `to_polars()`, the Neo4j driver's vocabulary. In Jupyter a
  result shows as a table. `single()` returns the one
  record and raises unless there is exactly one; `first()` returns the first
  record, or `None`.
- Literals are Python values (`int`, `float`, `Decimal`, `datetime`, `str`,
  ...); IRIs are `fluree.IRI` and language-tagged strings
  `fluree.LangString`, both `str` subclasses. A literal with no lossless
  Python type stays a `fluree.Literal`. An RDF 1.2 triple term, such as the
  `?t` of `?r rdf:reifies ?t`, is a `fluree.Triple(subject, predicate,
  object)`, which unpacks like a tuple.
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

  A value is an `IRI`, `LangString`, `Literal`, `Triple` or `Vector`, a
  Python `str`, `int`, `float`, `bool`, `Decimal`, `datetime`, `date` or
  `time`, a Cypher `Node` (its `element_id`), or a `BlankNode` a query
  returned. A `BlankNode` built from any other label raises
  `InvalidRequestError`: written in the query, it would match every node. So
  does a parameter the query never mentions, rather than leaving a misspelt
  variable unbound. JSON-LD queries take none.

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
- Not yet for Cypher: `stream()`; read a large Cypher result with `query()`.

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

## Graph sources

Tables in Iceberg, Delta Lake, or a SQL engine can be queried in place, mapped
to RDF by an [R2RML](https://www.w3.org/TR/r2rml/) mapping, without copying
them into a ledger:

```python
from pathlib import Path

people = conn.map_iceberg("people", Path("people.ttl"),
                          table_location="s3://lake/silver/people")
sales = conn.map_delta("sales", Path("sales.ttl"), root="s3://lake/Tables")
crm = conn.map_sql("crm", "https://trino.example.com", Path("crm.ttl"),
                   catalog="pg", schema="public",
                   auth=fluree.OAuth2(token_url, client_id,
                                      fluree.EnvVar("TRINO_SECRET")))

people.select("PREFIX ex: <http://example.org/> SELECT ?name WHERE { ?p ex:name ?name }")
```

- The mapping is Turtle text or a path to a Turtle file. Give each object map
  an `rr:datatype`: a column without one reads back as a string.
- An Iceberg table is read directly from `table_location`, or through a REST
  catalog (`catalog_uri=`, `warehouse=`, `auth=`). Delta tables are found under
  `root`, at locations in `tables`, or through a `fluree.Unity` catalog. A SQL
  source pushes queries to any Trino-protocol endpoint.
- Secrets can be `fluree.EnvVar("NAME")`, read where the tables are read,
  rather than stored with the source.
- `conn.graph_sources()` lists them, `conn.graph_source(name)` finds one, and
  `source.drop()` removes it (the tables are untouched).
- To join a source with a ledger, name the source with `FROM NAMED` and read it
  in a `GRAPH` block (JSON-LD: `"fromNamed"` and `["graph", ...]`):
  `SELECT ... FROM <crm:main> FROM NAMED <people:main> WHERE { ... GRAPH <people:main> { ... } }`.
- `people.materialize("people-copy")` copies an Iceberg source into a ledger,
  reading only what is new on each later pass.
- Local tables (`file://...`) must lie under a directory named in the
  `FLUREE_ICEBERG_LOCAL_ROOTS` environment variable, set before the process
  first reads one; they are not yet supported on Windows.

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

## RDF documents

`fluree.parse()` and `fluree.serialize()` read and write Turtle, TriG,
N-Triples, N-Quads and JSON-LD with no ledger involved, as lists of `fluree.Quad`
(`subject, predicate, object, graph`; the graph is `None` for the default
graph). Here a document's IRIs move to a new namespace:

```python
from pathlib import Path

OLD, NEW = "http://old.example/", "http://new.example/"

def move(term):
    if isinstance(term, fluree.IRI) and term.startswith(OLD):
        return fluree.IRI(NEW + term[len(OLD):])
    if isinstance(term, fluree.Triple):
        return fluree.Triple(*map(move, term))
    return term

quads = [fluree.Quad(*map(move, q)) for q in fluree.parse(Path("data.trig"))]
text = fluree.serialize(quads, "trig", prefixes={"ex": NEW})
ledger.insert(quads)                      # or write them straight to a ledger
```

JSON-LD reads from a dict or list as well as from text:

```python
quads = fluree.parse({
    "@context": {"ex": "http://example.org/"},
    "@id": "ex:alice",
    "ex:knows": {"@id": "ex:bob", "@annotation": {"ex:since": 2020}},
})
# alice knows bob, plus (_:b1, rdf:reifies, Triple(alice, knows, bob))
# and (_:b1, ex:since, 2020)
text = fluree.serialize(quads, "jsonld", prefixes={"ex": "http://example.org/"})
```

- A path (a `Path`, not a `str`, which is the document's text) gives its
  format by extension (`.ttl`, `.trig`, `.nt`, `.nq`, `.jsonld`, `.json`);
  text needs `format=`, and a dict or list is JSON-LD. Turtle, TriG and
  JSON-LD resolve relative IRIs against `base=`.
- RDF 1.2 is read whole. A triple term is a `fluree.Triple`, and an
  annotation or a reified triple is the quad `(reifier, rdf:reifies,
  Triple(...))`, the annotated triple being a quad of its own. `serialize`
  writes such quads back as annotations where the format has them. JSON-LD
  writes these as `@annotation` on a value, a node's `@reifies`, and
  `{"@id": {"@id": s, p: o}}`, and a named graph as `{"@id": g, "@graph":
  [...]}`.
- Literals are Python values, as in query results, so neither a number's
  spelling nor a narrower datatype is kept (`"01"^^xsd:long` reads as `1`
  and is written back as `xsd:integer`); a float is written in its shortest
  form (`0.9957`). `literals="lexical"` keeps every typed literal but a plain
  string as a `fluree.Literal` with its spelling and datatype, so a document
  read and written back keeps its literals exactly.
- Blank nodes keep the document's labels; an anonymous one gets a fresh
  label.
- JSON-LD contexts must be given inline: a `@context` URL is not fetched,
  and a string context is taken as the vocabulary IRI (`"https://schema.org/"`
  makes `name` `https://schema.org/name`). A property with no context entry is
  an error rather than dropped.
- `ledger.insert(quads)` and `upsert(quads)` write quads, named graphs
  included.

[RDF documents](https://fluree.github.io/db/reference/rdf-documents.html) in
the Fluree documentation shows how each syntax writes named graphs,
annotations, reified triples and triple terms.

## rdflib

[rdflib](https://rdflib.readthedocs.io) reads formats Fluree does not, such as
RDF/XML and N3, and is what tools like pySHACL take and return. N-Triples
moves a graph between the two without loss:

```python
import rdflib

g = rdflib.Graph().parse("ontology.owl", format="xml")
ledger.insert(g.serialize(format="nt"), format="turtle")       # rdflib → Fluree

g = rdflib.Graph().parse(data=ledger.export(format="ntriples"), format="nt")
```

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

Some errors carry what the engine knows, so an application can act on them
without reading the message:

- `ShaclViolationError` (an `InvalidRequestError`): a write the ledger's SHACL
  shapes reject. `violations` lists each failure as the same
  `ValidationResult` that `ledger.validate()` reports — focus node, path,
  value, constraint component and message.
- `UniqueConstraintError` (an `InvalidRequestError`): a value of an
  `f:enforceUnique` property that another subject already holds; `property`,
  `value`, `graph`, `existing_subject` and `new_subject`.
- `ConflictError`: when a commit lost a race, `expected_t` and `head_t` say
  where the ledger was and where it moved to.

```python
try:
    people.insert(person)
except fluree.ShaclViolationError as e:
    for v in e.violations:
        print(v.focus, v.path, v.message)
```

Writes are checked against the ledger's SHACL shapes where its config turns
SHACL on, ledger-wide as here or per graph (see
[SHACL defaults](https://github.com/fluree/db/blob/main/docs/ledger-config/setting-groups.md#shacl-defaults)):

```python
people.update("""
PREFIX f: <https://ns.flur.ee/db#>
INSERT DATA { GRAPH <urn:fluree:people:main#config> {
    <urn:fluree:people:main:config:ledger> a f:LedgerConfig ;
        f:shaclDefaults <urn:fluree:people:main:config:shacl> .
    <urn:fluree:people:main:config:shacl> f:shaclEnabled true .
} }""")
```

## Logging

The engine's log goes to Python's `logging` as the `fluree.engine` logger,
each record carrying the engine module that wrote it as `record.target`.
Warnings and errors are sent by default; `fluree.set_log_level("INFO")` (or
`"DEBUG"`, `"TRACE"`) sends more, and `"OFF"` none. Below the level the engine
skips the events altogether. As with any library, what is shown is the
application's choice: nothing reaches the console until it configures
`logging` (`logging.basicConfig()`, say).

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

Each call runs on a worker thread while the event loop carries on, as do
`fluree.aio.parse()` and `fluree.aio.serialize()`.
Cancelling a task that awaits a query (`asyncio.timeout`, a client that
disconnects) stops the query in the engine; a write already under way still
completes. Cancelled inside `async with ledger.transaction()`, the block waits
for that write to finish, rolls the transaction back, and re-raises the
cancellation, unless that write was the commit itself, which completes;
`txn.committed` says whether it did.

## Concurrency

Engine calls release the GIL, so threads can query in parallel. Several
processes may share one database directory; a commit that loses a race to
another process is retried against the new state, a bounded number of times
before it raises `ConflictError`. Hand writes that contend heavily to
`transact`, which runs them again on a conflict.

A process forked after Fluree started in its parent cannot use it: the
engine's threads do not survive a fork, and the lock state they leave behind
is unsafe to reuse, so the child raises `FlureeError` rather than risk a
crash. Use the `spawn` or `forkserver` start method for a `multiprocessing`
pool (`multiprocessing.get_context("spawn")`; Python before 3.14 defaults to
`fork` on Linux), or fork before Fluree is first used, as gunicorn does
unless the app is preloaded. A connection opened before the fork raises
`FlureeError` in the child too, and is left alone when the child exits.

## Development

Building from source needs a Rust toolchain:

```sh
uv venv && uv pip install maturin pytest pandas
maturin develop          # build the extension into the active environment
pytest
```

Releases are built and published by `.github/workflows/python-release.yml`
from the same version tags as the CLI; see `docs/contributing/releasing.md`.

## License

Licensed under the
[Business Source License 1.1](https://github.com/fluree/db/blob/main/LICENSE),
with a Change Date to Apache License 2.0 as specified in that file.

In short: you may use Fluree in production for anything except offering
Fluree itself to others as a hosted or managed database. Building your own
applications and data products on Fluree is permitted, including publishing a
dataset you curate with a public, read-only query endpoint. See the
[License FAQ](https://github.com/fluree/db/blob/main/docs/reference/license-faq.md)
for details.
