"""Graph sources: Iceberg, Delta and SQL tables mapped to RDF by R2RML,
registered, queried in place, materialized into a ledger, and dropped.

Local Iceberg and Delta tables are the engine's committed test fixtures
(conftest.py allowlists them); the SQL source talks to a small
Trino-protocol endpoint backed by sqlite3."""

import asyncio
import json
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import fluree
import fluree.aio
from conftest import DELTA_FIXTURES, ICEBERG_FIXTURES
from fluree import Bearer, EnvVar, GraphSource, IRI, InvalidRequestError, NotFoundError

EX = "http://example.org/"
P = f"PREFIX ex: <{EX}> "

local_tables = pytest.mark.skipif(
    sys.platform == "win32", reason="local Iceberg and Delta tables are read on Unix-style paths only"
)

PEOPLE_MAPPING = f"""
@prefix rr: <http://www.w3.org/ns/r2rml#> . @prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<{EX}mapping#People> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "silver.people" ] ;
    rr:subjectMap [ rr:template "{EX}person/{{id}}" ; rr:class ex:Person ] ;
    rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:score ; rr:objectMap [ rr:column "score" ; rr:datatype xsd:double ] ] .
"""

SALES_MAPPING = f"""
@prefix rr: <http://www.w3.org/ns/r2rml#> . @prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<{EX}mapping#Store> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "dim_store" ] ;
    rr:subjectMap [ rr:template "{EX}store/{{store_id}}" ; rr:class ex:Store ] ;
    rr:predicateObjectMap [ rr:predicate ex:storeName ; rr:objectMap [ rr:column "name" ] ] .
<{EX}mapping#Order> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "fact_order" ] ;
    rr:subjectMap [ rr:template "{EX}order/{{order_id}}" ; rr:class ex:Order ] ;
    rr:predicateObjectMap [ rr:predicate ex:total ; rr:objectMap [ rr:column "amount" ; rr:datatype xsd:integer ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:store ; rr:objectMap [
        rr:parentTriplesMap <{EX}mapping#Store> ;
        rr:joinCondition [ rr:child "store_id" ; rr:parent "store_id" ] ] ] .
"""

NAMES = P + "SELECT ?name WHERE { ?p ex:name ?name } ORDER BY ?name"
EVERYONE = ["alice", "bob", "carol", "dave", "erin"]


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def people(conn):
    location = (ICEBERG_FIXTURES / "silver" / "people").resolve().as_uri()
    return conn.map_iceberg("people", PEOPLE_MAPPING, table_location=location)


# -- Iceberg -------------------------------------------------------------------


@local_tables
def test_an_iceberg_table_is_queried_in_place(conn, people):
    assert isinstance(people, GraphSource)
    assert (people.id, people.kind) == ("people:main", "iceberg")
    assert people.select(NAMES).value("name") == EVERYONE
    top = people.query(P + "SELECT ?name WHERE { ?p ex:name ?name ; ex:score ?s FILTER(?s > $min) } ORDER BY ?name", min=90.0)
    assert top.value("name") == ["alice", "erin"]
    jsonld = people.query({"@context": {"ex": EX}, "select": "?n", "where": {"@id": "?p", "ex:name": "?n"}, "orderBy": "?n"})
    assert jsonld == EVERYONE
    with pytest.raises(InvalidRequestError, match="Cypher"):
        people.query("MATCH (n) RETURN n")


@local_tables
def test_a_graph_source_joins_a_ledger_as_a_named_graph(conn, people):
    crm = conn.create("crm")
    crm.insert({"@context": {"ex": EX}, "@id": "ex:person/1", "ex:tier": "gold"})
    joined = conn.query(
        P + "SELECT ?name ?tier FROM <crm:main> FROM NAMED <people:main> "
        "WHERE { ?p ex:tier ?tier GRAPH <people:main> { ?p ex:name ?name } }"
    )
    assert joined.values() == [["alice", "gold"]]
    jsonld = conn.query({
        "@context": {"ex": EX},
        "from": "crm:main",
        "fromNamed": "people:main",
        "select": ["?name", "?tier"],
        "where": [{"@id": "?p", "ex:tier": "?tier"}, ["graph", "people:main", {"@id": "?p", "ex:name": "?name"}]],
    })
    assert jsonld == [["alice", "gold"]]
    # In the default graph alongside the ledger, it would not be read: refused.
    with pytest.raises(InvalidRequestError, match="FROM NAMED"):
        conn.query(P + "SELECT ?name FROM <crm:main> FROM <people:main> WHERE { ?p ex:name ?name }")


@local_tables
def test_sources_are_listed_found_and_dropped(conn, people):
    assert conn.graph_sources() == [people]
    assert conn.graph_source("people") == people == conn.graph_source("people:main")
    people.drop()
    assert conn.graph_sources() == []
    with pytest.raises(NotFoundError):
        conn.graph_source("people")
    with pytest.raises(fluree.FlureeError):
        people.select(NAMES)


@local_tables
def test_materialize_copies_rows_into_a_ledger_and_then_reads_only_what_is_new(conn, people):
    first = people.materialize("people-copy")
    assert (first.rows_read, first.subjects_upserted, first.committed) == (5, 5, True)
    copy = conn.ledger("people-copy")
    assert copy.select(NAMES).value("name") == EVERYONE
    assert copy.select(P + "SELECT ?s WHERE { ex:person\\/1 ex:score ?s }").single().s == 91.5
    again = people.materialize("people-copy")
    assert (again.incremental, again.rows_read, again.committed) == (True, 0, False)
    assert people.materialize("people-copy", full=True).rows_read == 5


@local_tables
def test_registration_mistakes_are_refused(conn):
    with pytest.raises(InvalidRequestError, match="table_location"):
        conn.map_iceberg("x", PEOPLE_MAPPING)
    with pytest.raises(InvalidRequestError, match="table_location"):
        conn.map_iceberg("x", PEOPLE_MAPPING, table_location="file:///tmp/t", catalog_uri="http://c")
    with pytest.raises(fluree.FlureeError, match="(?i)local|allow"):
        conn.map_iceberg("x", PEOPLE_MAPPING, table_location="file:///not/allowed/people")
    with pytest.raises(TypeError):
        conn.map_iceberg("x", PEOPLE_MAPPING, table_location="file:///tmp/t", auth="token")


@local_tables
def test_a_mapping_can_be_a_file(conn, tmp_path):
    mapping = tmp_path / "people.ttl"
    mapping.write_text(PEOPLE_MAPPING)
    location = (ICEBERG_FIXTURES / "silver" / "people").resolve().as_uri()
    source = conn.map_iceberg("people", mapping, table_location=location)
    assert len(source.select(NAMES)) == 5


# -- Delta ---------------------------------------------------------------------


@local_tables
def test_delta_tables_beneath_a_root(conn):
    sales = conn.map_delta("sales", SALES_MAPPING, root=str(DELTA_FIXTURES.resolve()))
    assert sales.kind == "delta"
    totals = sales.select(
        P + "SELECT ?name (SUM(?t) AS ?total) WHERE { ?o ex:total ?t ; ex:store ?s . ?s ex:storeName ?name } "
        "GROUP BY ?name ORDER BY ?name"
    )
    assert totals.values() == [["East shop", 1300], ["Unassigned shop", 400], ["West shop", 750]]


@local_tables
def test_a_delta_table_that_cannot_be_read_is_a_warning(conn):
    with pytest.warns(UserWarning, match="not_there"):
        conn.map_delta("partial", SALES_MAPPING, tables={"dim_store": str(DELTA_FIXTURES / "not_there")},
                       root=str(DELTA_FIXTURES.resolve()))


# -- SQL -----------------------------------------------------------------------


class TrinoOverSqlite(BaseHTTPRequestHandler):
    """The Trino statement protocol, answered by sqlite3: each statement is
    run as sent and its rows returned in one page."""

    database: sqlite3.Connection
    seen_auth: list[str | None]

    def do_POST(self) -> None:
        sql = self.rfile.read(int(self.headers["Content-Length"])).decode()
        self.seen_auth.append(self.headers.get("Authorization"))
        try:
            cursor = self.database.execute(sql)
            rows = cursor.fetchall()
            names = [d[0] for d in cursor.description or []]
            body = {
                "id": "q",
                "columns": [{"name": n, "type": _trino_type(rows, i)} for i, n in enumerate(names)],
                "data": [list(r) for r in rows],
                "stats": {"state": "FINISHED"},
            }
        except sqlite3.Error as e:
            body = {"id": "q", "stats": {"state": "FAILED"}, "error": {"message": f"{e}: {sql}"}}
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args: object) -> None:
        pass


def _trino_type(rows: list[tuple], column: int) -> str:
    sample = next((r[column] for r in rows if r[column] is not None), None)
    return {int: "bigint", float: "double"}.get(type(sample), "varchar")


@pytest.fixture
def endpoint():
    database = sqlite3.connect(":memory:", check_same_thread=False)
    database.executescript("""
        CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
        INSERT INTO people VALUES (1, 'ann', 31), (2, 'ben', 45), (3, 'cy', 27);
    """)
    seen_auth: list[str | None] = []
    handler = type("Handler", (TrinoOverSqlite,), {"database": database, "seen_auth": seen_auth})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}", seen_auth
    server.shutdown()


SQL_MAPPING = f"""
@prefix rr: <http://www.w3.org/ns/r2rml#> . @prefix ex: <{EX}> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
<{EX}mapping#People> a rr:TriplesMap ;
    rr:logicalTable [ rr:tableName "people" ] ;
    rr:subjectMap [ rr:template "{EX}person/{{id}}" ; rr:class ex:Person ] ;
    rr:predicateObjectMap [ rr:predicate ex:name ; rr:objectMap [ rr:column "name" ] ] ;
    rr:predicateObjectMap [ rr:predicate ex:age ; rr:objectMap [ rr:column "age" ; rr:datatype xsd:integer ] ] .
"""


def test_a_sql_source_is_queried_through_its_endpoint(conn, endpoint, monkeypatch):
    url, seen_auth = endpoint
    monkeypatch.setenv("FLUREE_TEST_SQL_TOKEN", "s3cret")
    crm = conn.map_sql("crm", url, SQL_MAPPING, dialect="sqlite", auth=Bearer(EnvVar("FLUREE_TEST_SQL_TOKEN")))
    assert crm.kind == "sql"
    older = crm.select(P + "SELECT ?name WHERE { ?p ex:name ?name ; ex:age ?age FILTER(?age > 30) } ORDER BY ?name")
    assert older.value("name") == ["ann", "ben"]
    assert "Bearer s3cret" in seen_auth


def test_an_unreachable_sql_endpoint_is_a_warning(conn):
    with pytest.warns(UserWarning) as caught:
        source = conn.map_sql("gone", "http://127.0.0.1:9", SQL_MAPPING, dialect="sqlite")
    messages = [str(w.message) for w in caught]
    assert any("could not reach" in m for m in messages)
    assert any("uniqueness was not probed" in m for m in messages)
    assert source.id == "gone:main"
    with pytest.raises(InvalidRequestError, match="dialect"):
        conn.map_sql("bad", "http://127.0.0.1:9", SQL_MAPPING, dialect="cobol")  # type: ignore[arg-type]


# -- asyncio -------------------------------------------------------------------


@local_tables
def test_asyncio():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            location = (ICEBERG_FIXTURES / "silver" / "people").resolve().as_uri()
            people = await conn.map_iceberg("people", PEOPLE_MAPPING, table_location=location)
            names = (await people.select(NAMES)).value("name")
            sources = await conn.graph_sources()
            result = await people.materialize("copy")
            await people.drop()
            return names, sources, result, await conn.graph_sources()

    names, sources, result, after = asyncio.run(main())
    assert names == EVERYONE
    assert [s.id for s in sources] == ["people:main"]
    assert result.rows_read == 5 and after == []
