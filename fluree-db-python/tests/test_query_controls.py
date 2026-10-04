import datetime as dt
import time

import pytest

import fluree

EX = "http://example.org/"
NAMES = f"PREFIX ex: <{EX}> SELECT ?s ?n WHERE {{ ?s ex:n ?n }}"
NAMES_JSONLD = {"@context": {"ex": EX}, "select": ["?s", "?n"], "where": {"@id": "?s", "ex:n": "?n"}}
# A three-way cross product over 300 subjects: 27 million rows to count.
CROSS = f"PREFIX ex: <{EX}> SELECT (COUNT(*) AS ?count) WHERE {{ ?a ex:n ?x . ?b ex:n ?y . ?c ex:n ?z }}"


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def ledger(conn):
    ledger = conn.create("nums")
    ledger.insert({"@context": {"ex": EX}, "@graph": [{"@id": f"ex:s{i}", "ex:n": i} for i in range(300)]})
    return ledger


@pytest.mark.parametrize("query", [NAMES, NAMES_JSONLD], ids=["sparql", "jsonld"])
def test_profile_reports_fuel_and_time(ledger, query):
    profile = ledger.profile(query)
    assert len(profile.result) == 300
    assert profile.fuel > 0
    assert isinstance(profile.time, dt.timedelta)


@pytest.mark.parametrize("query", [NAMES, NAMES_JSONLD], ids=["sparql", "jsonld"])
def test_max_fuel_stops_a_query(ledger, query):
    needed = ledger.profile(query).fuel
    assert len(ledger.query(query, max_fuel=needed * 2)) == 300
    with pytest.raises(fluree.ResourceLimitError):
        ledger.query(query, max_fuel=needed / 10)


def test_max_fuel_on_snapshot_and_connection(conn, ledger):
    with pytest.raises(fluree.ResourceLimitError):
        ledger.snapshot().query(NAMES, max_fuel=1)
    from_query = f"PREFIX ex: <{EX}> SELECT ?n FROM <nums:main> WHERE {{ ?s ex:n ?n }}"
    assert len(conn.query(from_query)) == 300
    with pytest.raises(fluree.ResourceLimitError):
        conn.query(from_query, max_fuel=1)


def test_timeout_cancels_a_slow_query(ledger):
    started = time.monotonic()
    with pytest.raises(TimeoutError) as err:
        ledger.query(CROSS, timeout=0.2)
    assert isinstance(err.value, fluree.QueryTimeoutError)
    assert time.monotonic() - started < 5
    # The connection is still usable afterwards.
    assert len(ledger.query(NAMES)) == 300


def test_timeout_must_be_positive(ledger):
    with pytest.raises(ValueError):
        ledger.query(NAMES, timeout=0)


@pytest.mark.parametrize("query", [NAMES, NAMES_JSONLD], ids=["sparql", "jsonld"])
def test_explain(ledger, query):
    plan = ledger.explain(query)
    assert isinstance(plan, dict) and plan
    assert isinstance(ledger.snapshot().explain(query), dict)


class Interrupted(Exception):
    pass


def test_signal_interrupts_a_running_query(ledger):
    import os
    import signal
    import threading

    def on_sigint(signum, frame):
        raise Interrupted

    previous = signal.signal(signal.SIGINT, on_sigint)
    timer = threading.Timer(0.2, os.kill, (os.getpid(), signal.SIGINT))
    try:
        started = time.monotonic()
        timer.start()
        with pytest.raises(Interrupted):
            ledger.query(CROSS)
        assert time.monotonic() - started < 5
    finally:
        timer.cancel()
        signal.signal(signal.SIGINT, previous)
    assert len(ledger.query(NAMES)) == 300
