import asyncio
import datetime as dt
from decimal import Decimal

import pytest

import fluree
import fluree.aio
from fluree import (
    ConflictError,
    InvalidRequestError,
    Node,
    Path,
    PermissionDeniedError,
    Record,
    Relationship,
    Result,
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
    commit = ledger.update(GRAPH)
    assert commit.t == 1 and commit.result is None
    return ledger


def names(result):
    return [name for (name,) in result]


def test_records_read_by_key_or_position(ledger):
    result = ledger.query(
        "MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name"
    )
    assert isinstance(result, Result)
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
    record = ledger.query(
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
    assert ledger.query(query, {"name": "Bob"}).single()["age"] == 40
    assert ledger.query(query, name="Bob").single()["age"] == 40
    assert ledger.query(query, {"name": "Alice"}, name="Bob").single()["age"] == 40
    assert ledger.query(query, name="Nobody").single() is None


def test_nodes_relationships_and_paths(ledger):
    record = ledger.query(
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

    carol = ledger.query('MATCH (c {name: "Carol"}) RETURN c').single()["c"]
    assert carol.labels == frozenset({"Person", "Admin"})

    path = ledger.query(
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
    assert ledger.query('MATCH (p {name: "Bob"}) RETURN p.name').single()[0] == "Bob"
    with pytest.warns(UserWarning):
        ledger.query("MATCH (p:Person) RETURN p.name").single()
    with pytest.raises(InvalidRequestError):
        ledger.query("MATCH (p:Person) RETURN p.name").single(strict=True)


def test_writes_commit(ledger):
    commit = ledger.update('MATCH (p:Person {name: "Bob"}) SET p.age = 41')
    assert commit.t == 2 and commit.asserts >= 1
    assert commit.result is None
    assert ledger.query('MATCH (p {name: "Bob"}) RETURN p.age').single()[0] == 41
    assert ledger.log()[0].id == commit.id

    created = ledger.update('CREATE (d:Person {name: "Dave"}) RETURN d').result.single()["d"]
    assert isinstance(created, Node) and created["name"] == "Dave"


def test_a_script_commits_all_or_nothing(ledger):
    before = ledger.log()[0].t
    commit = ledger.update(
        'CREATE (:Person {name: "Erin"}); '
        'MATCH (e:Person {name: "Erin"}) SET e.age = 25; '
        'MATCH (e:Person {name: "Erin"}) RETURN e.age AS age'
    )
    assert commit.result.single()["age"] == 25
    assert commit.t > before

    with pytest.raises(fluree.FlureeError):
        ledger.update('CREATE (:Person {name: "Frank"}); THIS IS NOT CYPHER')
    assert ledger.query('MATCH (p {name: "Frank"}) RETURN p').single() is None


def test_a_failed_script_in_a_transaction_stages_nothing(ledger):
    with ledger.transaction() as txn:
        txn.update('CREATE (:Person {name: "Gail"})')
        for script in (
            'CREATE (:Person {name: "Frank"}); THIS IS NOT CYPHER',
            'CREATE (:Person {name: "Frank"}); MATCH (p:Person) SET p.x = $missing',
            'CREATE (:Person {name: "Frank"}); MATCH (p:Person) RETURN $missing',
        ):
            with pytest.raises(fluree.FlureeError):
                txn.update(script)
            assert txn.query('MATCH (p {name: "Frank"}) RETURN p').single() is None
    names = ledger.query('MATCH (p:Person) WHERE p.name IN ["Frank", "Gail"] RETURN p.name')
    assert names.value(0) == ["Gail"]


def test_snapshot_and_history(ledger):
    ledger.update('MATCH (p:Person {name: "Bob"}) SET p.age = 41')
    past = ledger.at(t=1)
    assert past.query('MATCH (p {name: "Bob"}) RETURN p.age').single()[0] == 40
    with pytest.raises(InvalidRequestError, match="update()"):
        past.query('CREATE (:Person {name: "Zed"})')


def test_cypher_in_a_transaction(ledger):
    with ledger.transaction(message="gus") as txn:
        assert txn.update('CREATE (:Person {name: "Gus"})') is None
        txn.update("MATCH (p {name: $n}) SET p.age = $age", n="Gus", age=50)
        # Cypher and SPARQL see each other's staged writes.
        txn.update('INSERT { ?p <nick> "G" } WHERE { ?p <name> "Gus" ; <age> 50 }')
        assert ledger.query('MATCH (p {name: "Gus"}) RETURN p').single() is None
    assert txn.committed.t == ledger.log()[0].t and ledger.log()[0].message == "gus"
    record = ledger.query('MATCH (p {name: "Gus"}) RETURN p.age AS age, p.nick AS nick').single()
    assert (record.age, record.nick) == (50, "G")

    with pytest.raises(RuntimeError):
        with ledger.transaction() as txn:
            txn.update('CREATE (:Person {name: "Hal"})')
            raise RuntimeError
    assert ledger.query('MATCH (p {name: "Hal"}) RETURN p').single() is None


def test_cypher_return_in_a_transaction_is_a_read(ledger):
    txn = ledger.transaction()
    created = txn.update('CREATE (p:Person {name: "Ida"}) RETURN p').single()["p"]
    assert created["name"] == "Ida"
    ledger.update('CREATE (:Person {name: "Jo"})')
    with pytest.raises(ConflictError):
        txn.commit()
    assert ledger.query('MATCH (p {name: "Ida"}) RETURN p').single() is None


def test_cypher_without_return_is_staged_again(ledger):
    txn = ledger.transaction()
    txn.update('MATCH (p:Person {name: "Bob"}) SET p.age = 41')
    ledger.update('CREATE (:Person {name: "Jo"})')
    txn.commit()
    assert ledger.query('MATCH (p {name: "Bob"}) RETURN p.age').single()[0] == 41
    assert ledger.query('MATCH (p {name: "Jo"}) RETURN p').single() is not None


def test_cypher_update_message(ledger):
    commit = ledger.update('CREATE (:Person {name: "Kim"})', message="add Kim")
    assert ledger.log()[0].message == "add Kim" and commit.t == ledger.log()[0].t


def test_cypher_on_a_fresh_ledger(conn):
    ledger = conn.create("fresh")
    assert ledger.update('CREATE (:Thing {n: 1})').t == 1


def test_cypher_reads_a_transactions_staged_state(ledger):
    with ledger.transaction() as txn:
        txn.update('PREFIX ex: <http://example.org/> INSERT DATA { ex:k ex:name "Kim" }')
        assert len(txn.query("MATCH (p:Person) RETURN p")) == 3


def test_governed_cypher(ledger):
    read_only = ledger.with_policy(
        policy=[{"@id": "view-only", "f:action": {"@id": "https://ns.flur.ee/db#view"}, "f:allow": True}]
    )
    assert len(read_only.query("MATCH (p:Person) RETURN p")) == 3
    with pytest.raises(PermissionDeniedError):
        read_only.update('CREATE (:Person {name: "Nope"})')


def test_cypher_errors(ledger):
    with pytest.raises(InvalidRequestError):
        ledger.query("MATCH (p RETURN p")


def test_to_df(ledger):
    pandas = pytest.importorskip("pandas")
    df = ledger.query("MATCH (p:Person) RETURN p.name AS name, p.age AS age ORDER BY name").to_df()
    assert isinstance(df, pandas.DataFrame)
    assert list(df["name"]) == ["Alice", "Bob", "Carol"]


def test_decimal_and_datetime_values(conn):
    # Cypher names are bare IRIs: `balance` is `<balance>`.
    ledger = conn.create("money")
    ledger.update(
        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> INSERT DATA { <acct> a <Account> ; "
        '<balance> "10.25"^^xsd:decimal ; <opened> "2024-01-02T03:04:05Z"^^xsd:dateTime }'
    )
    record = ledger.query("MATCH (a:Account) RETURN a.balance AS b, a.opened AS o").single()
    assert record["b"] == Decimal("10.25")
    assert record["o"] == dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)


def test_aio_cypher():
    async def main():
        async with fluree.aio.connect(":memory:") as conn:
            ledger = await conn.create("graph")
            await ledger.update(GRAPH)
            result = await ledger.query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
            assert names(result) == ["Alice", "Bob", "Carol"]
            async with ledger.transaction() as txn:
                await txn.update('CREATE (:Person {name: "Dee"})')
            assert txn.committed is not None
            past = await ledger.at(t=1)
            assert len(await past.query("MATCH (p:Person) RETURN p")) == 3

    asyncio.run(main())
