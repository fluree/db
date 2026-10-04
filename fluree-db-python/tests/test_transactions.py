import threading

import pytest

import fluree
from fluree import IRI, Change, ConflictError, InvalidRequestError, PermissionDeniedError

EX = "http://example.org/"
F = "https://ns.flur.ee/db#"
CONTEXT = {"ex": EX}
PEOPLE = f"PREFIX ex: <{EX}> SELECT ?name ?age WHERE {{ ?s ex:name ?name OPTIONAL {{ ?s ex:age ?age }} }} ORDER BY ?name"
BIRTHDAY = (
    f"PREFIX ex: <{EX}> DELETE {{ ex:alice ex:age ?age }} INSERT {{ ex:alice ex:age ?next }} "
    "WHERE { ex:alice ex:age ?age BIND(?age + 1 AS ?next) }"
)


def people(source):
    return [tuple(row) for row in source.query(PEOPLE)]


def person(id, name, age=None):
    doc = {"@context": CONTEXT, "@id": f"ex:{id}", "ex:name": name}
    if age is not None:
        doc["ex:age"] = age
    return doc


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def ledger(conn):
    return conn.create("people")


def test_writes_commit_together(ledger):
    with ledger.transaction(message="onboard") as txn:
        txn.insert(person("alice", "Alice", 30))
        txn.update(BIRTHDAY)  # sees the insert above
        txn.insert(f"@prefix ex: <{EX}> . ex:bob ex:name \"Bob\" .")
        assert people(txn) == [("Alice", 31), ("Bob", None)]
        assert people(ledger) == []

    commit = txn.committed
    assert (commit.t, commit.asserts, commit.retracts) == (1, 3, 0)
    assert people(ledger) == [("Alice", 31), ("Bob", None)]
    [logged] = ledger.log()
    assert (logged.t, logged.id, logged.message) == (1, commit.id, "onboard")


def test_an_exception_rolls_back(ledger):
    with pytest.raises(RuntimeError):
        with ledger.transaction() as txn:
            txn.insert(person("alice", "Alice"))
            raise RuntimeError("abandon")
    assert txn.committed is None
    assert people(ledger) == []
    assert ledger.log() == []


def test_a_rejected_write_is_left_out(ledger):
    with ledger.transaction() as txn:
        txn.insert(person("alice", "Alice"))
        with pytest.raises(InvalidRequestError):
            txn.update("INSERT DATA { this is not sparql")
        txn.insert(person("bob", "Bob"))
    assert people(ledger) == [("Alice", None), ("Bob", None)]


def test_reversed_writes_net_out(ledger):
    ledger.insert(person("alice", "Alice"))
    with ledger.transaction() as txn:
        txn.insert(person("tmp", "Temporary"))
        txn.update(f'PREFIX ex: <{EX}> DELETE DATA {{ ex:tmp ex:name "Temporary" }}')
        txn.insert(person("bob", "Bob"))
    assert (txn.committed.asserts, txn.committed.retracts) == (1, 0)
    assert ledger.history(EX + "tmp") == []


def test_nothing_to_commit(ledger):
    with ledger.transaction() as txn:
        pass
    assert txn.committed.id is None and txn.committed.t == 0
    assert ledger.log() == []


def test_explicit_commit_and_rollback(ledger):
    txn = ledger.transaction()
    txn.insert(person("alice", "Alice"))
    commit = txn.commit(message="by hand")
    assert commit == txn.committed
    assert ledger.log()[0].message == "by hand"
    with pytest.raises(InvalidRequestError, match="already been committed"):
        txn.insert(person("bob", "Bob"))
    with pytest.raises(InvalidRequestError):
        txn.commit()

    txn = ledger.transaction()
    txn.insert(person("bob", "Bob"))
    txn.rollback()
    with pytest.raises(InvalidRequestError):
        txn.query(PEOPLE)
    assert people(ledger) == [("Alice", None)]
    assert "closed" in repr(txn)


def test_a_concurrent_commit_is_built_on(ledger):
    ledger.insert(person("alice", "Alice", 30))
    txn = ledger.transaction()
    txn.update(BIRTHDAY)
    ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": 50})
    ledger.insert(person("carol", "Carol"))
    txn.commit()
    # Never read, so staged again over the commits that landed first: 50 + 1.
    assert people(ledger) == [("Alice", 51), ("Carol", None)]


def age_of_alice(source):
    return source.query(AGE)[0].age


AGE = f"PREFIX ex: <{EX}> SELECT ?age WHERE {{ ex:alice ex:age ?age }}"


def set_age(ledger_or_txn, age):
    ledger_or_txn.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": age})


def test_a_read_transaction_refuses_a_moved_ledger(ledger):
    ledger.insert(person("alice", "Alice", 30))
    txn = ledger.transaction()
    set_age(txn, age_of_alice(txn) + 1)  # decided from what it read
    set_age(ledger, 50)  # another writer, meanwhile
    with pytest.raises(ConflictError):
        txn.commit()
    assert age_of_alice(ledger) == 50  # their update is not lost


def test_transact_reruns_on_conflict(ledger):
    ledger.insert(person("alice", "Alice", 30))
    attempts = []

    def birthday(txn):
        attempts.append(age_of_alice(txn))
        if len(attempts) == 1:
            set_age(ledger, 50)  # another writer lands mid-transaction
        set_age(txn, attempts[-1] + 1)
        return attempts[-1] + 1

    assert ledger.transact(birthday) == 51
    assert attempts == [30, 50]
    assert age_of_alice(ledger) == 51


def test_transact_arguments_and_self_commit(ledger):
    def add(txn, name, *, age):
        txn.insert(person(name.lower(), name, age))
        return txn.commit(message=f"add {name}")

    commit = ledger.transact(add, "Alice", age=30)
    assert ledger.log()[0].message == "add Alice" and commit.t == 1


def test_transact_rolls_back_on_error(ledger):
    def broken(txn):
        txn.insert(person("alice", "Alice"))
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        ledger.transact(broken)
    assert people(ledger) == []


def test_transact_gives_up(ledger):
    ledger.insert(person("alice", "Alice", 30))
    attempts = []

    def always_raced(txn):
        attempts.append(1)
        set_age(txn, age_of_alice(txn) + 1)
        set_age(ledger, 100 + len(attempts))

    with pytest.raises(ConflictError):
        ledger.transact(always_raced)
    assert len(attempts) == 10


def test_transact_retries_only_its_own_commit(ledger):
    attempts = []

    def unrelated(txn):
        attempts.append(1)
        raise ConflictError("not from this transaction's commit")

    with pytest.raises(ConflictError):
        ledger.transact(unrelated)
    assert attempts == [1]


def test_message_on_single_writes(ledger):
    ledger.insert(person("alice", "Alice"), message="first")
    ledger.update(f'PREFIX ex: <{EX}> INSERT DATA {{ ex:alice ex:age 30 }}', message="second")
    ledger.upsert(f"@prefix ex: <{EX}> . ex:alice ex:age 31 .", message="third")
    assert [c.message for c in ledger.log()] == ["third", "second", "first"]


def test_governed_transaction(ledger):
    ledger.insert(person("alice", "Alice"))
    governed = ledger.with_policy(
        policy=[
            {"@id": "view-all", "f:action": {"@id": F + "view"}, "f:allow": True},
            {
                "@id": "names-only",
                "f:action": {"@id": F + "modify"},
                "f:onProperty": [{"@id": EX + "name"}],
                "f:allow": True,
            },
        ],
        default_allow=False,
    )
    with governed.transaction() as txn:
        txn.insert(person("bob", "Bob"))
        with pytest.raises(PermissionDeniedError):
            txn.insert(person("carol", "Carol", 40))
        assert people(txn) == [("Alice", None), ("Bob", None)]
    assert people(ledger) == [("Alice", None), ("Bob", None)]


def test_one_transaction_used_from_two_threads(ledger):
    txn = ledger.transaction()
    errors = []

    def write(n):
        try:
            txn.insert(person(f"p{n}", f"P{n}"))
        except InvalidRequestError as e:
            errors.append(e)

    threads = [threading.Thread(target=write, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads)
    # Writes that found the transaction busy were refused, not lost silently.
    assert all("in use by another thread" in str(e) for e in errors)
    txn.commit()
    assert len(people(ledger)) == 8 - len(errors)


def test_transaction_on_a_branch(ledger):
    ledger.insert(person("alice", "Alice"))
    dev = ledger.branch("dev")
    with dev.transaction() as txn:
        txn.insert(person("bob", "Bob"))
    assert people(dev) == [("Alice", None), ("Bob", None)]
    assert people(ledger) == [("Alice", None)]


def test_changes_of_a_transaction_commit(ledger):
    with ledger.transaction() as txn:
        txn.insert(person("alice", "Alice"))
        txn.insert(person("bob", "Bob"))
    assert sorted(c.subject for c in ledger.changes(txn.committed)) == [
        IRI(EX + "alice"),
        IRI(EX + "bob"),
    ]
    assert all(isinstance(c, Change) and c.t == 1 for c in ledger.changes(1))
