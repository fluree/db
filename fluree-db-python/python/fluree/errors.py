"""Exceptions raised by Fluree.

Every error derives from :class:`FlureeError`. Where a Python builtin
exception means the same thing, the Fluree error derives from it too, so
``except LookupError`` catches a missing ledger and ``except ValueError``
catches a malformed query.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fluree._records import ValidationResult
    from fluree._terms import IRI


class FlureeError(Exception):
    """Base class for all Fluree errors.

    ``status`` is the HTTP-style status code the engine assigned the error,
    or ``None`` when the error did not come from the engine.
    """

    status: int | None = None


class InvalidRequestError(FlureeError, ValueError):
    """The query, transaction, or argument is malformed or invalid."""


class ShaclViolationError(InvalidRequestError):
    """A write that would leave data breaking the ledger's SHACL shapes.

    ``violations`` lists each way it would, as the same
    :class:`ValidationResult` objects :meth:`Ledger.validate` reports, so a
    rejected write is handled like a failed validation."""

    _results: list[dict[str, Any]] = []

    @property
    def violations(self) -> list[ValidationResult]:
        from fluree._records import _validation_results

        return _validation_results(self._results)


class UniqueConstraintError(InvalidRequestError):
    """A write that would give two subjects the same value of a property the
    ledger requires to be unique (``f:enforceUnique``).

    ``value`` is the value as text; ``graph`` is the named graph's IRI, or
    ``None`` for the default graph."""

    property: IRI
    value: str
    graph: IRI | None
    existing_subject: IRI
    new_subject: IRI


class NotFoundError(FlureeError, LookupError):
    """A ledger, branch, commit, or graph does not exist."""


class PermissionDeniedError(FlureeError, PermissionError):
    """Policy denied the operation."""


class ConflictError(FlureeError):
    """A concurrent change won; retrying against the new state may succeed.

    When a commit lost a race, ``expected_t`` is the ledger ``t`` it was
    built on and ``head_t`` the ``t`` another commit had moved it to; both
    are ``None`` for other conflicts."""

    expected_t: int | None = None
    head_t: int | None = None


class QueryTimeoutError(FlureeError, TimeoutError):
    """The operation ran past its time limit."""


class ResourceLimitError(FlureeError):
    """The operation exceeded a fuel, memory, or size limit."""


__all__ = [
    "ConflictError",
    "FlureeError",
    "InvalidRequestError",
    "NotFoundError",
    "PermissionDeniedError",
    "QueryTimeoutError",
    "ResourceLimitError",
    "ShaclViolationError",
    "UniqueConstraintError",
]
