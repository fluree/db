"""Graph sources: Iceberg, Delta and SQL tables mapped to RDF by R2RML,
registered, queried in place, materialized into a ledger, and dropped.

Local Iceberg and Delta tables are the engine's committed test fixtures
(conftest.py allowlists them); the SQL source talks to a small
Trino-protocol endpoint backed by sqlite3."""

import asyncio
import json
import sqlite3
import subprocess
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


ORDERS_MAPPING = f"""
@prefix rr: <http://www.w3.org/ns/r2rml#> . @prefix ex: <{EX}> .
<{EX}mapping#Orders> a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "sales.orders" ] ;
  rr:subjectMap [ rr:template "{EX}order/{{order_id}}" ; rr:class ex:Order ] .
"""


def test_glue_and_s3_tables_sources_register(conn):
    # Registering reads no catalog (only a REST catalog is connection-tested),
    # so these register offline; the mapping's rr:tableName names each table.
    # Reading them end to end is covered by fluree-db-api's it_iceberg_glue_moto.
    glue = conn.map_iceberg(
        "glue-orders", ORDERS_MAPPING, glue=fluree.Glue(region="us-east-1", catalog_id="123456789012")
    )
    assert (glue.id, glue.kind) == ("glue-orders:main", "iceberg")
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/analytics"
    tables = conn.map_iceberg("s3t-orders", ORDERS_MAPPING, s3_tables=fluree.S3Tables(arn))
    assert (tables.id, tables.kind) == ("s3t-orders:main", "iceberg")


def test_glue_and_s3_tables_mistakes_are_refused(conn):
    with pytest.raises(InvalidRequestError, match="table_location"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(), catalog_uri="http://c")
    with pytest.raises(TypeError):
        conn.map_iceberg("x", ORDERS_MAPPING, glue="us-east-1")
    with pytest.raises(fluree.FlureeError, match="ARN"):
        conn.map_iceberg("x", ORDERS_MAPPING, s3_tables=fluree.S3Tables("not-an-arn"))
    with pytest.raises(fluree.FlureeError, match="not an AWS region"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(region="evil.com"))
    # REST-only settings would be ignored by Glue / S3 Tables: refused instead.
    with pytest.raises(InvalidRequestError, match="auth applies to rest mode only"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(), auth=Bearer("t"))
    with pytest.raises(InvalidRequestError, match="vended_credentials applies to rest mode only"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(), vended_credentials=True)
    with pytest.raises(InvalidRequestError, match="warehouse applies to rest mode only"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(), warehouse="w")
    with pytest.raises(InvalidRequestError, match="auth applies to rest mode only"):
        conn.map_iceberg(
            "x",
            ORDERS_MAPPING,
            s3_tables=fluree.S3Tables("arn:aws:s3tables:us-east-1:123456789012:bucket/b"),
            auth=Bearer("t"),
        )
    # Turning vending off agrees with every mode.
    conn.map_iceberg("glue-no-vend", ORDERS_MAPPING, glue=fluree.Glue(), vended_credentials=False)


# A service-account key named by a variable that is never set: registration
# cannot mint a token, so nothing connects, and the source registers with a
# warning as an unreachable catalog does.
UNSET_KEY = fluree.EnvVar("FLUREE_TEST_PY_SA_KEY_NEVER_SET")
CATALOG = "http://127.0.0.1:9/catalog"


def test_a_google_identity_authenticates_a_rest_catalog_and_reads_its_files(conn, monkeypatch):
    monkeypatch.delenv(UNSET_KEY.name, raising=False)
    with pytest.warns(UserWarning, match="could not reach the catalog"):
        source = conn.map_iceberg(
            "biglake",
            ORDERS_MAPPING,
            catalog_uri=CATALOG,
            warehouse="bq://projects/lake/locations/us/catalogs/sales",
            auth=fluree.Google(UNSET_KEY, quota_project="lake"),
        )
    assert source.kind == "iceberg"
    with pytest.warns(UserWarning, match="could not reach the catalog"):
        conn.map_iceberg(
            "biglake-explicit",
            ORDERS_MAPPING,
            catalog_uri=CATALOG,
            auth=fluree.Google(UNSET_KEY),
            storage="catalog",
        )
    # A Direct source on gs:// reads with an identity of its own.
    conn.map_iceberg(
        "direct-gcs",
        ORDERS_MAPPING,
        table_location="gs://bucket/warehouse/sales/orders",
        storage=fluree.Google(UNSET_KEY),
    )
    # A Google identity reads gs:// only: one on s3:// could never be read,
    # so it does not register.
    with pytest.raises(fluree.FlureeError, match="serves only gs://"):
        conn.map_iceberg(
            "direct-gcs-on-s3",
            ORDERS_MAPPING,
            table_location="s3://bucket/warehouse/sales/orders",
            storage=fluree.Google(UNSET_KEY),
        )


# A child process with a mock of Google's metadata server and of a BigLake
# catalog, which records the `x-goog-user-project` each catalog request names.
# A child, because the engine reads the metadata-server and token-host
# overrides once per process.
QUOTA_CHILD = r"""
import http.server, json, os, sys, threading

seen = []

class Mock(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        if self.path.startswith("/computeMetadata/"):
            body = {"access_token": "ya29.host", "expires_in": 3600, "token_type": "Bearer"}
        else:
            seen.append(self.headers.get("x-goog-user-project"))
            body = {} if self.path.startswith("/v1/config") else {
                "metadata-location": "gs://lake/wh/orders/metadata/v1.metadata.json"
            }
        data = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Mock)
threading.Thread(target=server.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{server.server_address[1]}"
os.environ["FLUREE_GOOGLE_METADATA_URL"] = (
    base + "/computeMetadata/v1/instance/service-accounts/default/token"
)
os.environ["FLUREE_GOOGLE_TOKEN_HOSTS"] = "127.0.0.1"

import fluree

mapping, warehouse, quota_project = json.loads(sys.argv[1])
with fluree.connect(":memory:") as conn:
    conn.map_iceberg(
        "biglake",
        mapping,
        catalog_uri=base,
        warehouse=warehouse,
        table="sales.orders",
        auth=fluree.Google(quota_project=quota_project),
        storage="catalog",
    )
print(json.dumps(seen))
"""


def catalog_billed_to(warehouse, quota_project=None):
    """The `x-goog-user-project` each of the registration's catalog requests
    named, for a source on `warehouse`."""
    child = subprocess.run(
        [sys.executable, "-c", QUOTA_CHILD, json.dumps([ORDERS_MAPPING, warehouse, quota_project])],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert child.returncode == 0, child.stderr
    seen = json.loads(child.stdout.strip().splitlines()[-1])
    assert seen, "the registration's connection test reached the catalog"
    return set(seen)


def test_a_biglake_warehouse_names_the_catalogs_quota_project():
    # BigLake refuses a request naming no quota project, whatever the identity.
    assert catalog_billed_to("bq://projects/lake-project/locations/us-central1") == {"lake-project"}
    assert catalog_billed_to("bl://projects/123456789012/catalogs/c") == {"123456789012"}
    # The caller's own project wins; another warehouse names none.
    assert catalog_billed_to(
        "bq://projects/lake-project/locations/us-central1", "billing-project"
    ) == {"billing-project"}
    assert catalog_billed_to("wh") == {None}


def test_a_storage_identity_the_catalog_cannot_serve_is_refused(conn):
    # `catalog` reads with the catalog's Google identity; a bearer has none.
    with pytest.raises(InvalidRequestError, match="storage_auth `catalog`"):
        conn.map_iceberg("x", ORDERS_MAPPING, catalog_uri=CATALOG, auth=Bearer("t"), storage="catalog")
    # A bq:// warehouse defaults to `catalog`, so it needs a Google identity too.
    with pytest.raises(InvalidRequestError, match="storage_auth `catalog`"):
        conn.map_iceberg(
            "x",
            ORDERS_MAPPING,
            catalog_uri=CATALOG,
            warehouse="bq://projects/lake/locations/us/catalogs/sales",
            auth=Bearer("t"),
        )
    # Only a REST catalog vends.
    with pytest.raises(InvalidRequestError, match="REST catalog"):
        conn.map_iceberg("x", ORDERS_MAPPING, glue=fluree.Glue(), storage="vended")
    with pytest.raises(InvalidRequestError, match="not both"):
        conn.map_iceberg(
            "x", ORDERS_MAPPING, catalog_uri=CATALOG, storage="ambient", vended_credentials=False
        )
    with pytest.raises(InvalidRequestError, match="storage is"):
        conn.map_iceberg("x", ORDERS_MAPPING, catalog_uri=CATALOG, storage="hmac")


def test_a_google_identity_is_never_a_literal_key_and_its_config_is_checked(conn, tmp_path):
    with pytest.raises(InvalidRequestError, match="never given literally"):
        conn.map_iceberg(
            "x", ORDERS_MAPPING, catalog_uri=CATALOG, auth=fluree.Google('{"private_key": "k"}')
        )
    # A WIF configuration reaches the engine, which refuses a credential
    # source outside the accepted subset (a local file here).
    wif = {
        "type": "external_account",
        "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/p/providers/aws",
        "subject_token_type": "urn:ietf:params:aws:token-type:aws4_request",
        "token_url": "https://sts.googleapis.com/v1/token",
        "credential_source": {"file": "/etc/passwd"},
    }
    with pytest.raises(InvalidRequestError, match="credential_source"):
        conn.map_iceberg("x", ORDERS_MAPPING, catalog_uri=CATALOG, auth=fluree.Google(wif))
    path = tmp_path / "wif.json"
    path.write_text("[]")
    with pytest.raises(InvalidRequestError, match="Workload Identity Federation"):
        conn.map_iceberg("x", ORDERS_MAPPING, catalog_uri=CATALOG, auth=fluree.Google(path))
    with pytest.raises(InvalidRequestError, match="storage scope"):
        conn.map_iceberg(
            "x",
            ORDERS_MAPPING,
            table_location="s3://bucket/warehouse/sales/orders",
            storage=fluree.Google(UNSET_KEY, scopes="https://www.googleapis.com/auth/bigquery"),
        )
    # Unity Catalog takes a token or a service principal, not a Google identity.
    with pytest.raises(TypeError):
        conn.map_delta("x", ORDERS_MAPPING, unity=fluree.Unity("https://ws.example.com", auth=fluree.Google()))


def test_a_rest_catalog_source_can_take_its_tables_from_the_mapping(conn):
    # No `table`: the mapping's rr:tableName names it, as on the CLI and server.
    # The catalog is unreachable (a refused loopback address), so registration
    # warns rather than fails.
    with pytest.warns(UserWarning, match="could not reach the catalog"):
        rest = conn.map_iceberg("rest-orders", ORDERS_MAPPING, catalog_uri="http://127.0.0.1:9/catalog")
    assert rest.kind == "iceberg"


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
