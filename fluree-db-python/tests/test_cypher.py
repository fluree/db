import asyncio
import datetime as dt
from decimal import Decimal

import pytest

import fluree
import fluree.aio
from fluree import (
    ConflictError,
    CypherResult,
    InvalidRequestError,
    Node,
    Path,
    PermissionDeniedError,
    Record,
    Relationship,
)

GRAPH = """
CREATE (a:Person {name: "Alice", age: 30, born: date("1990-01-02"), score: 1.5})
       -[:KNOWS {since: 2020}]->(b:Person {name: "Bob", age: 40}),
       (b)-[:KNOWS {since: 2021}]->(c:Person:Admin {name: "Carol"})
"""


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def ledger(conn):
    ledger = conn.create("graph")
    result = ledger.cypher(GRAPH)
    assert result.commit is not None and result.commit.t == 1
    return ledger


def names(result):
    return [name for (name,) in result]


def test_records_read_by_key_or_position(ledger):
    result = ledger.cypher(
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"
    )
    assert isinstance(result, CypherResult) and result.commit is None
    assert result.keys() == ["name", "age"]
    alice = result[0]
    assert isinstance(alice, Record)
    assert alice["name"] == alice[0] == alice.get("name") == "Alice"
    assert alice.get("missing", "x") == "x"
    assert alice.index("age") == 1
    assert alice.items() == [("name", "Alice"), ("age", 30)]
    assert alice.data() == {"name": "Alice", "age": 30}
    name, age = alice
    assert (name, age) == ("Alice", 30)
    assert result.value("name") == ["Alice", "Bob", "Carol"]
    assert result.value(1) == [30, 40, None]
    assert result.values("age") == [[30], [40], [None]]
    assert result.data()[1] == {"name": "Bob", "age": 40}
    with pytest.raises(KeyError):
        alice["nope"]


def test_native_values(ledger):
    record = ledger.cypher(
        'MATCH (p:Person {name: "Alice"}) '
        "RETURN p.born AS born, p.score AS score, [1, 2] AS list, {x: 1} AS map, true AS flag"
    ).single()
    assert record["born"] == dt.date(1990, 1, 2)
    assert record["score"] == 1.5
    assert record["list"] == [1, 2]
    assert record["map"] == {"x": 1}
    assert record["flag"] is True


def test_parameters(ledger):
    query = "MATCH (p:Person {name: $name}) RETURN p.age AS age"
    assert ledger.cypher(query, {"name": "Bob"}).single()["age"] == 40
    assert ledger.cypher(query, name="Bob").single()["age"] == 40
    assert ledger.cypher(query, {"name": "Alice"}, name="Bob").single()["age"] == 40
    assert ledger.cypher(query, name="Nobody").single() is None


def test_nodes_relationships_and_paths(ledger):
    record = ledger.cypher(
        'MATCH (a:Person {name: "Alice"})-[r:KNOWS]->(b) RETURN a, r, b'
    ).single()
    alice, knows, bob = record
    assert isinstance(alice, Node)
    assert alice.labels == frozenset({"Person"})
    assert alice["name"] == "Alice" and dict(alice)["age"] == 30
    assert isinstance(alice.element_id, str)
    assert isinstance(knows, Relationship)
    assert knows.type == "KNOWS" and knows["since"] == 2020
    # The relationship's endpoints are the nodes the record returned.
    assert knows.start_node is alice and knows.end_node is bob
    assert knows.start_node["name"] == "Alice"
    assert record.data()["r"] == (
        {"name": "Alice", "age": 30, "born": dt.date(1990, 1, 2), "score": 1.5},
        "KNOWS",
        {"name": "Bob", "age": 40},
    )

    carol = ledger.cypher('MATCH (c {name: "Carol"}) RETURN c').single()["c"]
    assert carol.labels == frozenset({"Person", "Admin"})

    path = ledger.cypher(
        'MATCH p = (a {name: "Alice"})-[:KNOWS*]->(c {name: "Carol"}) RETURN p'
    ).single()["p"]
    assert isinstance(path, Path)
    assert len(path) == 2
    assert [n["name"] for n in path.nodes] == ["Alice", "Bob", "Carol"]
    # A variable-length walk follows the base edges, so its relationships
    # carry no annotation identity or properties.
    assert [r.type for r in path] == ["KNOWS", "KNOWS"]
    assert [(r.start_node["name"], r.end_node["name"]) for r in path] == [("Alice", "Bob"), ("Bob", "Carol")]
    assert path.start_node["name"] == "Alice" and path.end_node["name"] == "Carol"


def test_single(ledger):
    assert ledger.cypher('MATCH (p {name: "Bob"}) RETURN p.name').single()[0] == "Bob"
    with pytest.warns(UserWarning):
        ledger.cypher("MATCH (p:Person) RETURN p.name").single()
    with pytest.raises(InvalidRequestError):
        ledger.cypher("MATCH (p:Person) RETURN p.name").single(strict=True)


def test_writes_commit(ledger):
    result = ledger.cypher('MATCH (p:Person {name: "Bob"}) SET p.age = 41')
    assert result.commit.t == 2 and result.commit.asserts >= 1
    assert result.keys() == [] and len(result) == 0
    assert ledger.cypher('MATCH (p {name: "Bob"}) RETURN p.age').single()[0] == 41
    assert ledger.log()[0].id == result.commit.id

    created = ledger.cypher('CREATE (d:Person {name: "Dave"}) RETURN d').single()["d"]
    assert isinstance(created, Node) and created["name"] == "Dave"


def test_a_script_commits_all_or_nothing(ledger):
    before = ledger.log()[0].t
    result = ledger.cypher(
        'CREATE (:Person {name: "Erin"}); '
        'MATCH (e:Person {name: "Erin"}) SET e.age = 25; '
        'MATCH (e:Person {name: "Erin"}) RETURN e.age AS age'
    )
    assert result.single()["age"] == 25
    assert result.commit.t > before

    with pytest.raises(fluree.FlureeError):
        ledger.cypher('CREATE (:Person {name: "Frank"}); THIS IS NOT CYPHER')
    assert ledger.cypher('MATCH (p {name: "Frank"}) RETURN p').single() is None


def test_snapshot_and_history(ledger):
    ledger.cypher('MATCH (p:Person {name: "Bob"}) SET p.age = 41')
    past = ledger.at(t=1)
    assert past.cypher('MATCH (p {name: "Bob"}) RETURN p.age').single()[0] == 40
    with pytest.raises(InvalidRequestError, match="read-only"):
        past.cypher('CREATE (:Person {name: "Zed"})')


def test_cypher_transaction(ledger):
    with ledger.cypher_transaction() as tx:
        tx.run('CREATE (:Person {name: "Gus"})')
        assert tx.run('MATCH (p {name: "Gus"}) RETURN p.name').single()[0] == "Gus"
        assert ledger.cypher('MATCH (p {name: "Gus"}) RETURN p').single() is None
        tx.run("MATCH (p {name: $n}) SET p.age = $age", n="Gus", age=50)
    assert tx.committed.t == ledger.log()[0].t
    assert ledger.cypher('MATCH (p {name: "Gus"}) RETURN p.age').single()[0] == 50

    with pytest.raises(RuntimeError):
        with ledger.cypher_transaction() as tx:
            tx.run('CREATE (:Person {name: "Hal"})')
            raise RuntimeError
    assert ledger.cypher('MATCH (p {name: "Hal"}) RETURN p').single() is None
    with pytest.raises(InvalidRequestError):
        tx.run("MATCH (p) RETURN p")


def test_cypher_transaction_conflicts_when_the_ledger_moved(ledger):
    tx = ledger.cypher_transaction()
    tx.run('CREATE (:Person {name: "Ida"})')
    ledger.cypher('CREATE (:Person {name: "Jo"})')
    with pytest.raises(ConflictError):
        tx.commit()
    assert ledger.cypher('MATCH (p {name: "Ida"}) RETURN p').single() is None


def test_cypher_on_a_fresh_ledger(conn):
    ledger = conn.create("fresh")
    assert ledger.cypher('CREATE (:Thing {n: 1})').commit.t == 1


def test_cypher_reads_a_transactions_staged_state(ledger):
    with ledger.transaction() as txn:
        txn.update('PREFIX ex: <http://example.org/> INSERT DATA { ex:k ex:name "Kim" }')
        assert len(txn.cypher("MATCH (p:Person) RETURN p")) == 3


def test_governed_cypher(ledger):
    read_only = ledger.with_policy(
        policy=[{"@id": "view-only", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": True}]
    )
    assert len(read_only.cypher("MATCH (p:Person) RETURN p")) == 3
    with pytest.raises(PermissionDeniedError):
        read_only.cypher('CREATE (:Person {name: "Nope"})')


def test_cypher_errors(ledger):
    with pytest.raises(InvalidRequestError):
        ledger.cypher("MATCH (p RETURN p")


def test_to_df(ledger):
    pandas = pytest.importorskip("pandas")
    df = ledger.cypher("MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name").to_df()
    assert isinstance(df, pandas.DataFrame)
    assert list(df["name"]) == ["Alice", "Bob", "Carol"]


def test_decimal_and_datetime_values(conn):
    # Cypher names are bare IRIs: `balance` is `<balance>`.
    ledger = conn.create("money")
    ledger.update(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> INSERT DATA { <acct> a <Account> ; "
        '<balance> "10.25"^^xsd:decimal ; <opened> "2024-01-02T03:04:05Z"^^xsd:dateTime }'
    )
    record = ledger.cypher("MATCH (a:Account) RETURN a.balance AS b, a.opened AS o").single()
    assert record["b"] == Decimal("10.25")
    assert record["o"] == dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)


def test_aio_cypher():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("graph")
            await ledger.cypher(GRAPH)
            result = await ledger.cypher("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            assert names(result) == ["Alice", "Bob", "Carol"]
            async with ledger.cypher_transaction() as tx:
                await tx.run('CREATE (:Person {name: "Dee"})')
            assert tx.committed is not None
            past = await ledger.at(t=1)
            assert len(await past.cypher("MATCH (p:Person) RETURN p")) == 3

    asyncio.run(main())
