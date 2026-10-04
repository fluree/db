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

from fluree._connection import Change, Commit, Connection, Ledger, QueryProfile, Snapshot, connect
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
    "Change",
    "Commit",
    "ConflictError",
    "Connection",
    "FlureeError",
    "InvalidRequestError",
    "LangString",
    "Ledger",
    "Literal",
    "NotFoundError",
    "PermissionDeniedError",
    "QueryProfile",
    "QueryTimeoutError",
    "ResourceLimitError",
    "RowStream",
    "Rows",
    "Snapshot",
    "__version__",
    "connect",
]
