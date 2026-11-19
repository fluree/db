import datetime as dt
import sys
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


# 1e19 seconds fits a Duration but overflows a Unix deadline; Windows'
# clock reaches that far.
UNIX_ONLY = pytest.mark.skipif(sys.platform == "win32", reason="a deadline that far fits Windows' clock")


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), 1e300, pytest.param(1e19, marks=UNIX_ONLY)])
def test_a_timeout_must_be_positive_and_finite(ledger, timeout):
    for call in (
        lambda: ledger.query(NAMES, timeout=timeout),
        lambda: ledger.query("MATCH (n) RETURN n", timeout=timeout),
        lambda: ledger.stream(NAMES, timeout=timeout),
        lambda: ledger.validate(timeout=timeout),
        lambda: ledger.index(timeout=timeout),
    ):
        with pytest.raises(fluree.InvalidRequestError, match="timeout"):
            call()


@pytest.mark.parametrize("max_fuel", [0, -1, float("nan"), float("inf")])
def test_max_fuel_must_be_positive_and_finite(ledger, max_fuel):
    for call in (
        lambda: ledger.query(NAMES, max_fuel=max_fuel),
        lambda: ledger.validate(max_fuel=max_fuel),
    ):
        with pytest.raises(fluree.InvalidRequestError, match="max_fuel"):
            call()


@pytest.mark.parametrize("query", [NAMES, NAMES_JSONLD], ids=["sparql", "jsonld"])
def test_explain(ledger, query):
    plan = ledger.explain(query)
    assert isinstance(plan, dict) and plan
    assert isinstance(ledger.snapshot().explain(query), dict)


class Interrupted(Exception):
    pass


def test_signal_interrupts_a_running_query(ledger):
    import signal
    import threading

    def on_sigint(signum, frame):
        raise Interrupted

    previous = signal.signal(signal.SIGINT, on_sigint)
    # raise_signal, not os.kill: on Windows os.kill terminates the process.
    timer = threading.Timer(0.2, signal.raise_signal, (signal.SIGINT,))
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


CYPHER_CROSS = "MATCH (a:N), (b:N), (c:N) RETURN count(*) AS count"


@pytest.fixture
def nodes(conn):
    ledger = conn.create("nodes")
    ledger.update("UNWIND range(1, 300) AS i CREATE (:N {i: i})")
    return ledger


def test_cypher_profile_and_max_fuel(nodes):
    query = "MATCH (n:N) WHERE n.i <= 10 RETURN n.i AS i ORDER BY i"
    profile = nodes.profile(query)
    assert profile.result.value("i") == list(range(1, 11))
    assert profile.fuel > 0 and isinstance(profile.time, dt.timedelta)
    assert len(nodes.query(query, max_fuel=profile.fuel * 2)) == 10
    with pytest.raises(fluree.ResourceLimitError):
        nodes.query(query, max_fuel=profile.fuel / 10)
    assert nodes.snapshot().profile(query).fuel > 0


def test_cypher_timeout_stops_the_query(nodes):
    # 27 million rows; uncancelled it would run for a long while.
    started = time.monotonic()
    with pytest.raises(fluree.QueryTimeoutError):
        nodes.query(CYPHER_CROSS, timeout=0.3)
    assert time.monotonic() - started < 5
    assert nodes.query("MATCH (n:N) RETURN count(n) AS c").single().c == 300
