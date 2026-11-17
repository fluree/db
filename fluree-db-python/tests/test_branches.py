import pytest

import fluree
from fluree import (
    IRI,
    Branch,
    Change,
    Commit,
    Conflict,
    ConflictError,
    InvalidRequestError,
    NotFoundError,
    PermissionDeniedError,
)

EX = "http://example.org/"
CONTEXT = {"ex": EX}
ALICE = IRI(EX + "alice")
AGE = IRI(EX + "age")
AGES = f"PREFIX ex: <{EX}> SELECT ?age WHERE {{ ex:alice ex:age ?age }} ORDER BY ?age"
NAMES = f"PREFIX ex: <{EX}> SELECT ?name WHERE {{ ?s ex:name ?name }} ORDER BY ?name"


def ages(ledger):
    return [age for (age,) in ledger.query(AGES)]


def names(ledger):
    return [name for (name,) in ledger.query(NAMES)]


def set_age(ledger, age):
    return ledger.upsert({"@context": CONTEXT, "@id": "ex:alice", "ex:age": age})


def add_person(ledger, name):
    return ledger.insert({"@context": CONTEXT, "@id": f"ex:{name.lower()}", "ex:name": name})


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def main(conn):
    ledger = conn.create("people")
    ledger.insert({"@context": CONTEXT, "@id": "ex:alice", "ex:name": "Alice", "ex:age": 30})  # t=1
    return ledger


@pytest.fixture
def dev(main):
    return main.branch("dev")


@pytest.fixture
def diverged(main, dev):
    """main and dev both changed alice's age since dev was created."""
    set_age(main, 40)
    set_age(dev, 50)
    return main, dev


def test_branch_is_a_ledger_on_its_own_line(conn, main, dev):
    assert dev.id == "people:dev"
    add_person(dev, "Bob")
    add_person(main, "Carol")
    assert names(dev) == ["Alice", "Bob"]
    assert names(main) == ["Alice", "Carol"]
    assert names(conn.ledger("people:dev")) == ["Alice", "Bob"]


def test_branches(main, dev):
    head = main.log()[0].id
    assert main.branches() == [
        Branch(name="dev", id="people:dev", source="main", t=1, head=head),
        Branch(name="main", id="people:main", source=None, t=1, head=head),
    ]
    assert dev.branches() == main.branches()


def test_branch_from_the_past(main):
    set_age(main, 31)
    set_age(main, 32)
    old = main.at(t=2).branch("old")
    assert ages(old) == [31]
    set_age(old, 99)
    assert ages(old) == [99]
    assert ages(main) == [32]


def test_branch_errors(conn, main, dev):
    with pytest.raises(ConflictError):
        main.branch("dev")
    with pytest.raises(InvalidRequestError):
        main.branch("a:b")
    with pytest.raises(InvalidRequestError):
        conn.create("empty").branch("dev")


def test_fast_forward_merge(main, dev):
    set_age(dev, 31)
    add_person(dev, "Bob")

    preview = main.merge_preview("dev", changes=True)
    assert preview.fast_forward and preview.mergeable
    assert (preview.ahead_count, preview.behind_count) == (2, 0)
    assert [c.t for c in preview.ahead] == [3, 2]
    assert all(isinstance(c, Commit) and c.time is not None for c in preview.ahead)
    assert preview.ancestor_t == 1
    assert preview.conflicts == [] and preview.conflict_count == 0
    assert sorted(preview.changes, key=lambda c: (c.subject, c.op)) == [
        Change(None, "assert", ALICE, AGE, 31),
        Change(None, "retract", ALICE, AGE, 30),
        Change(None, "assert", IRI(EX + "bob"), IRI(EX + "name"), "Bob"),
    ]

    result = main.merge("dev")
    assert result.fast_forward
    assert (result.source, result.target, result.t) == ("dev", "main", 3)
    assert result.id == dev.log()[0].id
    assert names(main) == ["Alice", "Bob"]
    assert ages(main) == [31]


def test_merge_conflicts_by_strategy(conn, diverged):
    main, dev = diverged
    preview = main.merge_preview(dev, details=True)
    assert not preview.fast_forward
    assert (preview.ahead_count, preview.behind_count, preview.conflict_count) == (1, 1, 1)
    assert preview.conflicts == [
        Conflict(
            subject=ALICE,
            predicate=AGE,
            graph=None,
            source=[Change(None, "assert", ALICE, AGE, 50)],
            target=[Change(None, "assert", ALICE, AGE, 40)],
        )
    ]
    assert preview.mergeable
    assert not main.merge_preview(dev, strategy="abort").mergeable
    assert main.merge_preview(dev).conflicts[0].source is None

    with pytest.raises(ConflictError):
        main.merge(dev, strategy="abort")
    assert ages(main) == [40]

    for target, strategy, expected in [
        ("both", "take-both", [40, 50]),
        ("source", "take-source", [50]),
        ("branch", "take-branch", [40]),
    ]:
        ledger = main.at(t=main.log()[0].t).branch(target)
        result = ledger.merge(dev, strategy=strategy)
        assert not result.fast_forward
        assert (result.conflicts, result.strategy) == (1, strategy)
        assert ages(ledger) == expected, strategy
        assert ledger.log()[0].id == result.id


def test_merge_names_the_source_several_ways(conn, main, dev):
    set_age(dev, 31)
    assert main.merge_preview("people:dev").ahead_count == 1
    assert main.merge_preview(conn.ledger("people:dev")).ahead_count == 1
    other = conn.create("other")
    other.insert({"@id": "ex:x", "ex:p": 1, "@context": CONTEXT})
    other_dev = other.branch("dev")
    with pytest.raises(InvalidRequestError):
        main.merge(other_dev)
    with pytest.raises(NotFoundError):
        main.merge("nope")
    with pytest.raises(InvalidRequestError):
        main.merge("main")
    with pytest.raises(InvalidRequestError):
        main.merge("dev", strategy="theirs")


def test_merge_preview_pages_changes(main, dev):
    for name in ["Bob", "Carol", "Dave"]:
        add_person(dev, name)
    seen = []
    after = None
    while True:
        page = main.merge_preview("dev", changes=True, max_changes=1, changes_after=after)
        seen += [c.value for c in page.changes]
        after = page.changes_after
        if after is None:
            break
    assert sorted(seen) == ["Bob", "Carol", "Dave"]
    with pytest.raises(InvalidRequestError):
        main.merge_preview("dev", changes_after=after or "x")


def test_rebase(diverged):
    main, dev = diverged
    add_person(dev, "Bob")
    add_person(main, "Carol")

    result = dev.rebase()
    assert (result.fast_forward, result.replayed, result.total) == (False, 2, 2)
    assert result.source_t == main.log()[0].t
    assert result.conflicts == [(2, 1, "take-both")]
    assert names(dev) == ["Alice", "Bob", "Carol"]
    assert ages(dev) == [40, 50]
    # The branch now sits on main's head, so merging it back fast-forwards.
    assert main.merge_preview(dev).fast_forward


def test_rebase_strategies(conn, diverged):
    main, dev = diverged
    with pytest.raises(ConflictError):
        dev.rebase(strategy="abort")
    assert ages(dev) == [50]

    result = dev.rebase(strategy="skip")
    assert (result.replayed, result.skipped, result.total) == (0, 1, 1)
    assert ages(dev) == [40]

    with pytest.raises(InvalidRequestError):
        main.rebase()


def test_rebase_with_nothing_of_its_own_fast_forwards(main, dev):
    set_age(main, 41)
    result = dev.rebase()
    assert result.fast_forward and result.replayed == 0
    assert ages(dev) == [41]


def test_revert(main):
    set_age(main, 31)  # t=2
    bob = add_person(main, "Bob")  # t=3

    preview = main.revert_preview(bob)
    assert preview.revertable
    assert [c.t for c in preview.commits] == [3] and preview.commit_count == 1

    result = main.revert(bob)
    assert result.committed and result.t == 4
    assert result.reverted == [bob.id]
    assert result.strategy == "abort"
    assert names(main) == ["Alice"]
    assert main.log()[0].id == result.id

    main.revert(2)
    assert ages(main) == [30]


def test_revert_names_commits_several_ways(main):
    first = set_age(main, 31)  # t=2
    second = add_person(main, "Bob")  # t=3
    for ref in (2, first, first.id, first.short_id):
        assert [c.t for c in main.revert_preview(ref).commits] == [2]
    assert [c.t for c in main.revert_preview([first, second.short_id]).commits] == [3, 2]
    with pytest.raises(NotFoundError):
        main.revert("deadbeefdeadbeef")
    with pytest.raises(InvalidRequestError):
        main.revert([])
    with pytest.raises(TypeError):
        main.revert(2.0)


def test_revert_conflicts(main):
    set_age(main, 31)  # t=2
    set_age(main, 32)  # t=3: changes what reverting t=2 would restore

    preview = main.revert_preview(2)
    assert not preview.revertable
    assert preview.conflict_count == 1
    assert preview.conflicts == [Conflict(subject=ALICE, predicate=AGE, graph=None)]
    with pytest.raises(ConflictError):
        main.revert(2)
    assert ages(main) == [32]

    assert main.revert_preview(2, strategy="take-branch").revertable
    main.revert(2, strategy="take-branch")
    assert ages(main) == [32]


def test_revert_on_a_branch_leaves_its_source_alone(main, dev):
    commit = set_age(dev, 31)
    dev.revert(commit)
    assert ages(dev) == [30]
    assert main.log()[0].t == 1


def test_drop_branch(conn, main, dev):
    set_age(dev, 31)
    conn.drop("people:dev")
    assert [b.name for b in main.branches()] == ["main"]
    assert "people:dev" not in conn
    with pytest.raises(NotFoundError):
        conn.ledger("people:dev")
    with pytest.raises(NotFoundError):
        conn.drop("people:dev")
    with pytest.raises(InvalidRequestError, match="first branch"):
        conn.drop("people:main")
    assert ages(main) == [30]


def test_drop_ledger_drops_its_branches(conn, main, dev):
    conn.drop("people")
    assert "people:dev" not in conn
    assert conn.ledgers() == []


def test_governed_handles_cannot_branch_or_merge(main, dev):
    governed = main.with_policy(default_allow=True)
    for attempt in (
        lambda: governed.branch("x"),
        lambda: governed.at(t=1).branch("x"),
        lambda: governed.merge("dev"),
        lambda: governed.merge_preview("dev"),
        lambda: governed.revert(1),
        lambda: governed.revert_preview(1),
        lambda: dev.with_policy(default_allow=True).rebase(),
    ):
        with pytest.raises(PermissionDeniedError):
            attempt()
    assert [b.name for b in governed.branches()] == ["dev", "main"]


def test_branches_persist(tmp_path):
    with fluree.connect(tmp_path) as conn:
        main = conn.create("people")
        set_age(main, 30)
        dev = main.branch("dev")
        set_age(dev, 31)
        set_age(main, 40)
        main.merge(dev, strategy="take-source")
    with fluree.connect(tmp_path) as conn:
        assert [b.name for b in conn.ledger("people").branches()] == ["dev", "main"]
        assert ages(conn.ledger("people")) == [31]
        assert ages(conn.ledger("people:dev")) == [31]
