"""Engine errors carry what the engine knows about them, so an application
can act on a rejected write without parsing its message."""

import pytest

import fluree
from fluree import (
    IRI,
    ConflictError,
    InvalidRequestError,
    ShaclViolationError,
    UniqueConstraintError,
    ValidationResult,
)

EX = "http://example.org/"
SH = "http://www.w3.org/ns/shacl#"
F = "https://ns.flur.ee/db#"
CONTEXT = {"ex": EX}
SHAPES = {
    "@context": {"sh": SH, "ex": EX, "xsd": "http://www.w3.org/2001/XMLSchema#"},
    "@id": "ex:PersonShape",
    "@type": "sh:NodeShape",
    "sh:targetClass": {"@id": "ex:Person"},
    "sh:property": [
        {"sh:path": {"@id": "ex:age"}, "sh:datatype": {"@id": "xsd:integer"}},
        {"sh:path": {"@id": "ex:name"}, "sh:minCount": 1},
    ],
}
BOB = {"@context": CONTEXT, "@id": "ex:bob", "@type": "ex:Person", "ex:age": "old"}


@pytest.fixture
def conn():
    with fluree.connect(":memory:") as conn:
        yield conn


@pytest.fixture
def shaped(conn):
    ledger = conn.create("people")
    ledger.insert(SHAPES)
    return ledger


def test_a_shacl_rejection_lists_its_violations(shaped):
    with pytest.raises(ShaclViolationError) as err:
        shaped.insert(BOB)
    assert isinstance(err.value, InvalidRequestError) and err.value.status == 400
    by_path = {v.path: v for v in err.value.violations}
    assert by_path[IRI(EX + "age")] == ValidationResult(
        focus=IRI(EX + "bob"),
        path=IRI(EX + "age"),
        message=by_path[IRI(EX + "age")].message,
        severity="violation",
        value="old",
        shape=IRI(EX + "PersonShape"),
        component=IRI(SH + "DatatypeConstraintComponent"),
    )
    assert by_path[IRI(EX + "name")].component == IRI(SH + "MinCountConstraintComponent")
    # The message still reads as it did, compacted against the write's context.
    assert "ex:bob" in str(err.value)


def test_violations_match_what_validate_reports(shaped, conn):
    with pytest.raises(ShaclViolationError) as err:
        shaped.insert(BOB)
    other = conn.create("unshaped")
    other.insert(BOB)
    report = other.validate(SHAPES)
    assert sorted(err.value.violations, key=repr) == sorted(report.results, key=repr)


def test_a_staged_write_raises_it_too(shaped):
    with shaped.transaction() as txn:
        with pytest.raises(ShaclViolationError) as err:
            txn.insert(BOB)
        assert err.value.violations


def test_a_merge_the_shapes_reject_raises_it_too(conn):
    ledger = conn.create("people")
    ledger.insert({"@context": CONTEXT, "@id": "ex:alice", "ex:name": "Alice"})
    dev = ledger.branch("dev")
    dev.insert(BOB)  # no shapes on this branch
    ledger.insert(SHAPES)
    with pytest.raises(ShaclViolationError) as err:
        ledger.merge(dev)
    assert IRI(EX + "bob") in {v.focus for v in err.value.violations}


def test_a_unique_clash_names_both_subjects(conn):
    ledger = conn.create("accounts")
    ledger.insert({
        "@context": {"ex": EX, "f": F},
        "@graph": [
            {"@id": "ex:email", "f:enforceUnique": True},
            {"@id": "ex:alice", "ex:email": "alice@example.com"},
        ],
    })
    ledger.upsert(
        f"""@prefix f: <{F}> .
        GRAPH <urn:fluree:accounts:main#config> {{
          <urn:config:main> a f:LedgerConfig ; f:transactDefaults <urn:config:transact> .
          <urn:config:transact> f:uniqueEnabled true .
        }}""",
        format="trig",
    )
    with pytest.raises(UniqueConstraintError) as err:
        ledger.insert({"@context": CONTEXT, "@id": "ex:bob", "ex:email": "alice@example.com"})
    clash = err.value
    assert isinstance(clash, InvalidRequestError)
    assert (clash.property, clash.value, clash.graph) == (IRI(EX + "email"), "alice@example.com", None)
    assert {clash.existing_subject, clash.new_subject} == {IRI(EX + "alice"), IRI(EX + "bob")}
    assert isinstance(clash.property, IRI)


def test_a_commit_conflict_says_where_the_ledger_moved(conn):
    ledger = conn.create("counter")
    ledger.insert({"@context": CONTEXT, "@id": "ex:c", "ex:n": 1})
    txn = ledger.transaction()
    txn.query(f"PREFIX ex: <{EX}> SELECT ?n WHERE {{ ex:c ex:n ?n }}")
    txn.upsert({"@context": CONTEXT, "@id": "ex:c", "ex:n": 2})
    ledger.upsert({"@context": CONTEXT, "@id": "ex:c", "ex:n": 3})
    with pytest.raises(ConflictError) as err:
        txn.commit()
    assert (err.value.expected_t, err.value.head_t) == (1, 2)
    assert ConflictError("other").expected_t is None
