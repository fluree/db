"""Fluree: a graph database with time travel, history, and fine-grained policy.

Open a database with :func:`connect`, create or open a ledger, then write
JSON-LD or Turtle and query it with SPARQL or JSON-LD::

    import fluree

    with fluree.connect(":memory:") as conn:
        people = conn.create("people")
        people.insert({
            "@context": {"ex": "http://example.org/"},
            "@id": "ex:alice", "ex:name": "Alice", "ex:age": 42,
        })
        for name, age in people.query(
            "PREFIX ex: <http://example.org/> "
            "SELECT ?name ?age WHERE { ?s ex:name ?name ; ex:age ?age }"
        ):
            print(name, age)
"""

from fluree._connection import Connection, Ledger, QueryProfile, Snapshot, Transaction, connect
from fluree._cypher import CypherResult, CypherTransaction, Node, Path, Record, Relationship
from fluree._records import (
    Branch,
    Change,
    Commit,
    Conflict,
    IndexStatus,
    MergePreview,
    MergeResult,
    RebaseResult,
    RevertPreview,
    RevertResult,
    SweepResult,
    ValidationReport,
    ValidationResult,
    VerifyReport,
)
from fluree._fluree import __version__
from fluree._results import Rows, RowStream
from fluree._terms import IRI, BlankNode, LangString, Literal
from fluree.errors import (
    ConflictError,
    FlureeError,
    InvalidRequestError,
    NotFoundError,
    PermissionDeniedError,
    QueryTimeoutError,
    ResourceLimitError,
)

__all__ = [
    "IRI",
    "BlankNode",
    "Branch",
    "Change",
    "Commit",
    "Conflict",
    "ConflictError",
    "Connection",
    "CypherResult",
    "CypherTransaction",
    "FlureeError",
    "IndexStatus",
    "InvalidRequestError",
    "LangString",
    "Ledger",
    "Literal",
    "MergePreview",
    "MergeResult",
    "Node",
    "NotFoundError",
    "Path",
    "PermissionDeniedError",
    "QueryProfile",
    "QueryTimeoutError",
    "RebaseResult",
    "Record",
    "Relationship",
    "ResourceLimitError",
    "RevertPreview",
    "RevertResult",
    "RowStream",
    "Rows",
    "Snapshot",
    "SweepResult",
    "Transaction",
    "ValidationReport",
    "ValidationResult",
    "VerifyReport",
    "__version__",
    "connect",
]
