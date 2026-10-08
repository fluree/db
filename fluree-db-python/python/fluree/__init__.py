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

from importlib.metadata import version as _version

from fluree._connection import Connection, Ledger, QueryProfile, Snapshot, Transaction, connect
from fluree._graph import Node, Path, Relationship
from fluree._records import (
    Branch,
    Change,
    Commit,
    Conflict,
    FullText,
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
from fluree._logging import set_log_level
from fluree._results import Record, Result, RowStream
from fluree._sources import (
    AzureServicePrincipal,
    Bearer,
    EnvVar,
    GraphSource,
    MaterializeResult,
    OAuth2,
    Unity,
)
from fluree._terms import IRI, BlankNode, LangString, Literal, Vector
from fluree.errors import (
    ConflictError,
    FlureeError,
    InvalidRequestError,
    NotFoundError,
    PermissionDeniedError,
    QueryTimeoutError,
    ResourceLimitError,
    ShaclViolationError,
    UniqueConstraintError,
)

# The installed package's version, which is the engine's.
__version__ = _version("fluree")

__all__ = [
    "AzureServicePrincipal",
    "Bearer",
    "BlankNode",
    "Branch",
    "Change",
    "Commit",
    "Conflict",
    "ConflictError",
    "Connection",
    "EnvVar",
    "FlureeError",
    "FullText",
    "GraphSource",
    "IRI",
    "IndexStatus",
    "InvalidRequestError",
    "LangString",
    "Ledger",
    "Literal",
    "MaterializeResult",
    "MergePreview",
    "MergeResult",
    "Node",
    "NotFoundError",
    "OAuth2",
    "Path",
    "PermissionDeniedError",
    "QueryProfile",
    "QueryTimeoutError",
    "RebaseResult",
    "Record",
    "Relationship",
    "ResourceLimitError",
    "Result",
    "RevertPreview",
    "RevertResult",
    "RowStream",
    "ShaclViolationError",
    "Snapshot",
    "SweepResult",
    "Transaction",
    "UniqueConstraintError",
    "Unity",
    "ValidationReport",
    "ValidationResult",
    "Vector",
    "VerifyReport",
    "connect",
    "set_log_level",
    "__version__",
]
