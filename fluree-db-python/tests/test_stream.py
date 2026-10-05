import time

import pytest

import fluree

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
