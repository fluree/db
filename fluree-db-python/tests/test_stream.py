import threading
import time

import pytest

import fluree
from fluree import InvalidRequestError

EX = "http://example.org/"
NUMS = f"PREFIX ex: <{EX}> SELECT ?n ?s WHERE {{ ?s ex:n ?n }} ORDER BY ?n"
CROSS = f"PREFIX ex: <{EX}> SELECT ?x ?y ?z WHERE {{ ?a ex:n ?x . ?b ex:n ?y . ?c ex:n ?z }}"


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("nums")
        ledger.insert({"@context": {"ex": EX}, "@graph": [{"@id": f"ex:s{i}", "ex:n": i} for i in range(2500)]})
        yield ledger


def test_stream_yields_every_row_in_projection_order(ledger):
    stream = ledger.stream(NUMS, batch_size=100)
    rows = list(stream)
    assert len(rows) == 2500
    assert stream.columns == ["n", "s"]
    assert rows[0] == (0, fluree.IRI(EX + "s0"))
    assert rows[-1].n == 2499
    assert [r.n for r in rows] == list(range(2500))


def test_stream_matches_query(ledger):
    assert list(ledger.stream(NUMS)) == list(ledger.query(NUMS))


def test_stream_jsonld(ledger):
    query = {"@context": {"ex": EX}, "select": ["?s", "?n"], "where": {"@id": "?s", "ex:n": "?n"}}
    rows = list(ledger.stream(query))
    assert len(rows) == 2500
    assert rows[0].keys() == ["s", "n"]


def test_stream_select_star(ledger):
    rows = list(ledger.snapshot().stream(f"PREFIX ex: <{EX}> SELECT * WHERE {{ ?s ex:n ?n }}"))
    assert len(rows) == 2500 and set(rows[0].keys()) == {"s", "n"}


def test_breaking_early_stops_a_huge_query(ledger):
    started = time.monotonic()
    with ledger.stream(CROSS) as stream:
        first = [next(stream) for _ in range(10)]
    assert len(first) == 10
    assert time.monotonic() - started < 10
    assert len(ledger.query(NUMS)) == 2500


def test_a_second_reader_is_refused(ledger):
    # No row ever passes the filter, so the first reader waits until the stream is closed.
    endless = f"PREFIX ex: <{EX}> SELECT ?x WHERE {{ ?a ex:n ?x . ?b ex:n ?y . ?c ex:n ?z FILTER(?x + ?y + ?z < 0) }}"
    stream = ledger.stream(endless)
    outcomes = {}

    def read(name):
        try:
            outcomes[name] = next(stream, None)
        except fluree.FlureeError as e:
            outcomes[name] = e

    first = threading.Thread(target=read, args=("first",))
    first.start()
    time.sleep(0.5)
    second = threading.Thread(target=read, args=("second",))
    second.start()
    second.join(timeout=10)
    assert not second.is_alive()
    assert isinstance(outcomes["second"], InvalidRequestError)
    assert "being read by another thread" in str(outcomes["second"])
    stream.close()
    first.join(timeout=30)
    assert not first.is_alive()


def test_stream_timeout(ledger):
    stream = ledger.stream(CROSS, timeout=0.3)
    with pytest.raises(fluree.QueryTimeoutError):
        for _ in stream:
            pass


def test_stream_fuel_limit(ledger):
    with pytest.raises(fluree.ResourceLimitError):
        list(ledger.stream(NUMS, max_fuel=1))


def test_only_select_streams(ledger):
    with pytest.raises(ValueError):
        ledger.stream(f"PREFIX ex: <{EX}> ASK {{ ?s ex:n 1 }}")


def test_stream_respects_policy(ledger):
    deny_all = ledger.with_policy(policy=[{"@id": "none", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": False}])
    assert list(deny_all.stream(NUMS)) == []


DENY_ALL = [{"@id": "none", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": False}]


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        conn.create("people").update(f"PREFIX ex: <{EX}> INSERT DATA {{ ex:a ex:n 1 . GRAPH <urn:g1> {{ ex:a ex:n 2 }} }}")
        conn.create("other").update(f"PREFIX ex: <{EX}> INSERT DATA {{ ex:a ex:n 3 }}")
        yield conn


def test_a_stream_reads_the_graphs_its_from_names(conn):
    people = conn.ledger("people")
    from_g1 = f"PREFIX ex: <{EX}> SELECT ?n FROM <urn:g1> WHERE {{ ?s ex:n ?n }}"
    named_g1 = f"PREFIX ex: <{EX}> SELECT ?n FROM NAMED <urn:g1> WHERE {{ GRAPH <urn:g1> {{ ?s ex:n ?n }} }}"
    for query in (from_g1, named_g1):
        assert [r.n for r in people.stream(query)] == people.query(query).value("n") == [2]
        assert [r.n for r in people.snapshot().stream(query)] == [2]
    assert list(people.with_policy(policy=DENY_ALL).stream(from_g1)) == []
    other = f"PREFIX ex: <{EX}> SELECT ?n FROM <other:main> WHERE {{ ?s ex:n ?n }}"
    for verb in (people.query, people.stream):
        with pytest.raises(InvalidRequestError, match="not in this ledger"):
            verb(other)


def test_a_jsonld_query_on_a_ledger_reads_only_that_ledger(conn):
    people = conn.ledger("people")
    query = {"@context": {"ex": EX}, "select": ["?n"], "where": {"@id": "?s", "ex:n": "?n"}}
    assert people.query({**query, "from": "people"}) == [[1]]
    assert [r.n for r in people.stream({**query, "from": ["people:main"]})] == [1]
    txn = people.transaction()
    verbs = (people.query, people.stream, people.explain, people.snapshot().query, txn.query)
    datasets = (
        {"from": "other"},
        {"from": ["people", "other"]},
        {"from": "people@t:1"},
        {"fromNamed": "people"},
        {"opts": {"ledger": "other"}},
    )
    for dataset in datasets:
        for verb in verbs:
            with pytest.raises(InvalidRequestError, match="cannot name another dataset"):
                verb({**query, **dataset})
    txn.rollback()
