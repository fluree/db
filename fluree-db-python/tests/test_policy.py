import json

import pytest

import fluree

EX = "http://example.org/ns/"
SCHEMA = "http://schema.org/"
F = "https://ns.flur.ee/db#"
ALICE_IDENTITY = EX + "aliceIdentity"

SSNS = f"PREFIX ex: <{EX}> PREFIX schema: <{SCHEMA}> SELECT ?s ?ssn WHERE {{ ?s a ex:User ; schema:ssn ?ssn }} ORDER BY ?s"
SSNS_JSONLD = {
    "@context": {"ex": EX, "schema": SCHEMA},
    "select": ["?s", "?ssn"],
    "where": {"@id": "?s", "@type": "ex:User", "schema:ssn": "?ssn"},
}

# Only the user an identity is linked to may see their own SSN.
OWN_SSN_ONLY = {
    "f:required": True,
    "f:onProperty": [{"@id": SCHEMA + "ssn"}],
    "f:action": {"@id": F + "view"},
    "f:query": json.dumps({"where": {"@id": "?$identity", EX + "user": {"@id": "?$this"}}}),
}


@pytest.fixture
def ledger():
    with fluree.connect(":memory:") as conn:
        ledger = conn.create("people")
        ledger.insert(
            {
                "@context": {"ex": EX, "schema": SCHEMA, "f": F},
                "@graph": [
                    {"@id": "ex:alice", "@type": "ex:User", "schema:name": "Alice", "schema:ssn": "111-11-1111"},
                    {"@id": "ex:john", "@type": "ex:User", "schema:name": "John", "schema:ssn": "888-88-8888"},
                    {
                        "@id": ALICE_IDENTITY,
                        "f:policyClass": [{"@id": "ex:EmployeePolicy"}],
                        "ex:user": {"@id": "ex:alice"},
                    },
                    {
                        "@id": "ex:ownSsnOnly",
                        "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                        **OWN_SSN_ONLY,
                    },
                    {
                        "@id": "ex:viewEverythingElse",
                        "@type": ["f:AccessPolicy", "ex:EmployeePolicy"],
                        "f:action": {"@id": F + "view"},
                        "f:query": json.dumps({}),
                    },
                ],
            }
        )
        yield ledger


def ssns(rows):
    return [(str(r.s), r.ssn) for r in rows]


def test_ungoverned_ledger_sees_everything(ledger):
    assert ssns(ledger.query(SSNS)) == [(EX + "alice", "111-11-1111"), (EX + "john", "888-88-8888")]


def test_identity_applies_its_policy_classes(ledger):
    alice = ledger.with_policy(identity=ALICE_IDENTITY)
    assert ssns(alice.query(SSNS)) == [(EX + "alice", "111-11-1111")]
    assert alice.query(SSNS_JSONLD) == [["ex:alice", "111-11-1111"]]
    assert ssns(alice.snapshot().query(SSNS)) == [(EX + "alice", "111-11-1111")]


def test_policy_class_with_values(ledger):
    governed = ledger.with_policy(
        policy_class=EX + "EmployeePolicy",
        values={"?$identity": {"@id": ALICE_IDENTITY}},
    )
    assert ssns(governed.query(SSNS)) == [(EX + "alice", "111-11-1111")]


def test_inline_policy(ledger):
    governed = ledger.with_policy(
        policy=[{"@id": "inline-ssn", **OWN_SSN_ONLY}],
        values={"?$identity": {"@id": ALICE_IDENTITY}},
        default_allow=True,
    )
    assert ssns(governed.query(SSNS)) == [(EX + "alice", "111-11-1111")]


def test_policy_applies_to_past_states(ledger):
    t = ledger.snapshot().t
    ledger.insert({"@context": {"ex": EX, "schema": SCHEMA}, "@id": "ex:zed", "@type": "ex:User", "schema:ssn": "999"})
    alice = ledger.with_policy(identity=ALICE_IDENTITY)
    assert ssns(alice.at(t=t).query(SSNS)) == [(EX + "alice", "111-11-1111")]


def test_unknown_identity_sees_nothing_by_default(ledger):
    stranger = ledger.with_policy(identity=EX + "nobody")
    assert len(stranger.query(SSNS)) == 0


def test_denied_write_raises_permission_error(ledger):
    read_only = ledger.with_policy(
        policy=[{"@id": "view-only", "f:action": {"@id": F + "view"}, "f:allow": True}],
    )
    assert len(read_only.query(SSNS)) == 2
    with pytest.raises(PermissionError) as err:
        read_only.insert({"@context": {"ex": EX}, "@id": "ex:x", "ex:n": 1})
    assert isinstance(err.value, fluree.PermissionDeniedError)
    assert err.value.status == 403
    assert ledger.query(f"PREFIX ex: <{EX}> ASK {{ ex:x ?p ?o }}") is False


def test_with_policy_needs_an_option(ledger):
    with pytest.raises(ValueError):
        ledger.with_policy()
