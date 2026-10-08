import datetime as dt
import json
import threading
import time
from decimal import Decimal

import pytest

import fluree
from fluree import IRI, BlankNode, LangString, Literal

EX = "http://example.org/"
PREFIX = f"PREFIX ex: <{EX}> "
CONTEXT = {"ex": EX}

TURTLE = """
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:alice ex:name "Alice" ;
    ex:age 42 ;
    ex:score 1.5 ;
    ex:ratio "2.5E0"^^xsd:double ;
    ex:active true ;
    ex:label "Bonjour"@fr ;
    ex:born "2020-01-02T03:04:05Z"^^xsd:dateTime ;
    ex:day "2020-01-02"^^xsd:date ;
    ex:wait "P1D"^^xsd:duration ;
    ex:knows _:anon .
_:anon ex:name "Anon" .
ex:bob ex:name "Bob" .
"""


@pytest.fixture(params=["memory", "file"])
def conn(request, tmp_path):
    path = ":memory:" if request.param == "memory" else tmp_path / "db"
    with fluree.connect(path) as conn:
        yield conn


@pytest.fixture
def people(conn):
    ledger = conn.create("people")
    ledger.insert(TURTLE)
    return ledger


def test_ledger_lifecycle(conn):
    assert conn.ledgers() == []
    ledger = conn.create("people")
    assert ledger.id == "people:main"
    assert conn.ledgers() == ["people:main"]
    assert "people" in conn
    assert "people:main" in conn
    assert "other" not in conn
    assert conn.ledger("people").id == "people:main"
    conn.drop("people")
    assert "people" not in conn


def test_missing_ledger_is_a_lookup_error(conn):
    with pytest.raises(LookupError) as err:
        conn.ledger("nope")
    assert isinstance(err.value, fluree.NotFoundError)


def test_select_rows_follow_projection_order(people):
    rows = people.query(
        PREFIX + "SELECT ?name ?s ?age WHERE { ?s ex:name ?name OPTIONAL { ?s ex:age ?age } } ORDER BY ?name"
    )
    assert rows.columns == ["name", "s", "age"]
    assert len(rows) == 3
    name, s, age = rows[0]
    assert (name, s, age) == ("Alice", IRI(EX + "alice"), 42)
    assert rows[0].name == "Alice"
    assert rows[1].age is None
    assert isinstance(rows[1].s, BlankNode)
    assert rows.data()[2] == {"name": "Bob", "s": EX + "bob", "age": None}


def test_literals_become_python_values(people):
    (row,) = people.query(
        PREFIX
        + """SELECT ?age ?score ?ratio ?active ?label ?born ?day ?wait WHERE {
               ex:alice ex:age ?age ; ex:score ?score ; ex:ratio ?ratio ; ex:active ?active ;
                 ex:label ?label ; ex:born ?born ; ex:day ?day ; ex:wait ?wait }"""
    )
    assert row.age == 42 and type(row.age) is int
    assert row.score == Decimal("1.5")
    assert row.ratio == 2.5 and type(row.ratio) is float
    assert row.active is True
    assert row.label == "Bonjour" and row.label.language == "fr"
    assert isinstance(row.label, LangString)
    assert row.born == dt.datetime(2020, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
    assert row.day == dt.date(2020, 1, 2)
    assert row.wait == Literal("P1D", "http://www.w3.org/2001/XMLSchema#duration")


def test_ask_and_construct(people):
    assert people.query(PREFIX + "ASK { ex:alice ex:age 42 }") is True
    assert people.query(PREFIX + "ASK { ex:bob ex:age ?a }") is False
    graph = people.query(PREFIX + "CONSTRUCT { ?s ex:called ?n } WHERE { ?s ex:name ?n }")
    assert len(graph["@graph"]) == 3


def test_jsonld_query_returns_python_objects(people):
    result = people.query({"@context": CONTEXT, "select": {"ex:bob": ["*"]}})
    assert result == [{"@id": "ex:bob", "ex:name": "Bob"}]
    as_text = people.query(json.dumps({"@context": CONTEXT, "select": "?n", "where": {"@id": "ex:bob", "ex:name": "?n"}}))
    assert as_text == ["Bob"]


def test_insert_formats_and_commit_receipt(conn, tmp_path):
    ledger = conn.create("formats")
    c1 = ledger.insert({"@context": CONTEXT, "@id": "ex:a", "ex:n": 1})
    assert (c1.t, c1.asserts, c1.retracts) == (1, 1, 0)
    assert c1.id
    c2 = ledger.insert('{"@context": {"ex": "http://example.org/"}, "@id": "ex:b", "ex:n": 2}')
    assert c2.t == 2
    ttl = tmp_path / "c.ttl"
    ttl.write_text("@prefix ex: <http://example.org/> . ex:c ex:n 3 .")
    ledger.insert(ttl)
    jsonld = tmp_path / "d.jsonld"
    jsonld.write_text(json.dumps({"@context": CONTEXT, "@id": "ex:d", "ex:n": 4}))
    ledger.insert(jsonld)
    total = ledger.query(PREFIX + "SELECT (SUM(?n) AS ?total) WHERE { ?s ex:n ?n }")
    assert total[0].total == 10


def test_unknown_file_format_needs_format(conn, tmp_path):
    ledger = conn.create("formats")
    data = tmp_path / "data.txt"
    data.write_text("@prefix ex: <http://example.org/> . ex:a ex:n 1 .")
    with pytest.raises(ValueError):
        ledger.insert(data)
    assert ledger.insert(data, format="turtle").asserts == 1


def test_upsert_and_update(people):
    people.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": 43})
    assert people.query(PREFIX + "SELECT ?a WHERE { ex:alice ex:age ?a }")[0].a == 43

    commit = people.update(
        PREFIX + "DELETE { ex:alice ex:age ?a } INSERT { ex:alice ex:age 44 } WHERE { ex:alice ex:age ?a }"
    )
    assert (commit.asserts, commit.retracts) == (1, 1)

    people.update(
        {
            "@context": CONTEXT,
            "where": {"@id": "ex:alice", "ex:age": "?a"},
            "delete": {"@id": "ex:alice", "ex:age": "?a"},
            "insert": {"@id": "ex:alice", "ex:age": 45},
        }
    )
    assert people.query(PREFIX + "SELECT ?a WHERE { ex:alice ex:age ?a }")[0].a == 45


def test_time_travel_by_t_commit_and_time(conn):
    ledger = conn.create("history")
    first = ledger.insert({"@context": CONTEXT, "@id": "ex:a", "ex:n": 1})
    time.sleep(0.05)
    between = dt.datetime.now(dt.timezone.utc)
    time.sleep(0.05)
    ledger.insert({"@context": CONTEXT, "@id": "ex:b", "ex:n": 2})

    count = PREFIX + "SELECT (COUNT(?s) AS ?c) WHERE { ?s ex:n ?n }"
    assert ledger.query(count)[0].c == 2
    assert ledger.at(t=first.t).query(count)[0].c == 1
    assert ledger.at(commit=first.id).query(count)[0].c == 1
    assert ledger.at(commit=first.digest).query(count)[0].c == 1
    assert ledger.at(commit=first.short_id).query(count)[0].c == 1
    assert ledger.at(time=between).query(count)[0].c == 1
    assert ledger.at(t=first.t).t == first.t


def test_at_requires_exactly_one_point(people):
    with pytest.raises(ValueError):
        people.at()
    with pytest.raises(ValueError):
        people.at(t=1, commit="abc")
    with pytest.raises(ValueError):
        people.at(time=dt.datetime(2020, 1, 1))  # naive


def test_snapshot_is_frozen(people):
    snapshot = people.snapshot()
    names = PREFIX + "SELECT ?n WHERE { ?s ex:name ?n }"
    before = len(snapshot.query(names))
    people.insert({"@context": CONTEXT, "@id": "ex:carol", "ex:name": "Carol"})
    assert len(snapshot.query(names)) == before
    assert len(people.query(names)) == before + 1


def test_invalid_query_is_a_value_error(people):
    with pytest.raises(ValueError) as err:
        people.query("SELEC nonsense")
    assert isinstance(err.value, fluree.InvalidRequestError)
    assert err.value.status == 400


def test_to_pandas(people):
    pandas = pytest.importorskip("pandas")
    df = people.query(PREFIX + "SELECT ?name WHERE { ?s ex:name ?name } ORDER BY ?name").to_pandas()
    assert isinstance(df, pandas.DataFrame)
    assert list(df["name"]) == ["Alice", "Anon", "Bob"]


def test_threads_query_concurrently(people):
    errors = []

    def work():
        try:
            for _ in range(20):
                assert len(people.query(PREFIX + "SELECT ?n WHERE { ?s ex:name ?n }")) == 3
        except Exception as e:  # noqa: BLE001 - surfaced through the assertion below
            errors.append(e)

    threads = [threading.Thread(target=work) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


SMALL_STACK = """
import threading, fluree
threading.stack_size(512 * 1024)
def work():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("t")
        ledger.insert({"@id": "http://example.org/a", "http://example.org/n": 1})
        assert len(ledger.query("SELECT ?n WHERE { ?s <http://example.org/n> ?n }")) == 1
        ledger.query("SELECT ?n WHERE { ?s <http://example.org/n> ?n }", timeout=60)
thread = threading.Thread(target=work)
thread.start()
thread.join()
print("done")
"""


def test_engine_calls_from_a_thread_with_a_small_stack():
    # A Python thread's stack can be smaller than the engine needs (2 MB on
    # Windows); engine calls must not run on it. A subprocess, since an
    # overflow aborts the process.
    import subprocess
    import sys

    run = subprocess.run([sys.executable, "-c", SMALL_STACK], capture_output=True, text=True, timeout=120)
    assert run.returncode == 0 and run.stdout.strip() == "done", run.stderr[-2000:]


def test_bulk_import(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "data.ttl").write_text(TURTLE)
    with fluree.connect(tmp_path / "db") as conn:
        ledger = conn.create("imported", source=source)
        rows = ledger.query(PREFIX + "SELECT ?name WHERE { ?s ex:name ?name } ORDER BY ?name")
        assert [r.name for r in rows] == ["Alice", "Anon", "Bob"]


def test_file_database_persists(tmp_path):
    with fluree.connect(tmp_path / "db") as conn:
        conn.create("kept").insert({"@context": CONTEXT, "@id": "ex:a", "ex:n": 1})
    with fluree.connect(tmp_path / "db") as conn:
        assert conn.ledgers() == ["kept:main"]
        assert conn.ledger("kept").query(PREFIX + "ASK { ex:a ex:n 1 }") is True


def test_a_refresh_takes_in_another_connections_commits(tmp_path):
    # Two connections on one directory stand in for two processes sharing it:
    # each keeps the state of the ledgers it has read.
    with fluree.connect(tmp_path / "db") as writer, fluree.connect(tmp_path / "db") as reader:
        written = writer.create("shared")
        first = written.insert({"@context": CONTEXT, "@id": "ex:a", "ex:n": 1})
        # Not yet read through this connection, it refreshes to the head too.
        assert reader.ledger("shared").refresh() == first.t
        ledger = reader.ledger("shared")
        assert ledger.query(PREFIX + "ASK { ex:a ex:n 1 }") is True

        second = written.insert({"@context": CONTEXT, "@id": "ex:a", "ex:n": 2})
        assert ledger.query(PREFIX + "ASK { ex:a ex:n 2 }") is False
        assert ledger.refresh() == second.t
        assert ledger.query(PREFIX + "ASK { ex:a ex:n 2 }") is True


def test_a_closed_connection_closes_what_was_opened_through_it():
    conn = fluree.connect(":memory:")
    ledger = conn.create("people")
    ledger.insert({"@context": {"ex": EX}, "@id": "ex:a", "ex:name": "A"})
    snapshot = ledger.snapshot()
    stream = ledger.stream(f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ?s ex:name ?n }}")
    txn = ledger.transaction()
    txn.insert({"@context": {"ex": EX}, "@id": "ex:b", "ex:name": "B"})
    conn.close()
    for call in (
        lambda: ledger.insert({"@context": {"ex": EX}, "@id": "ex:z", "ex:name": "Z"}),
        lambda: ledger.query(f"PREFIX ex: <{EX}> ASK {{ ?s ?p ?o }}"),
        lambda: conn.ledgers(),
        lambda: conn.ledger("people"),
        lambda: snapshot.query(f"PREFIX ex: <{EX}> ASK {{ ?s ?p ?o }}"),
        lambda: next(stream),
        lambda: txn.insert({"@context": {"ex": EX}, "@id": "ex:c", "ex:name": "C"}),
        lambda: txn.commit(),
    ):
        with pytest.raises(fluree.InvalidRequestError, match="closed"):
            call()
    txn.rollback()
    conn.close()
