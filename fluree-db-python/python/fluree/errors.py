"""Exceptions raised by Fluree.

Every error derives from :class:`FlureeError`. Where a Python builtin
exception means the same thing, the Fluree error derives from it too, so
``except LookupError`` catches a missing ledger and ``except ValueError``
catches a malformed query.
"""

from __future__ import annotations


class FlureeError(Exception):
    """Base class for all Fluree errors.

    ``status`` is the HTTP-style status code the engine assigned the error,
    or ``None`` when the error did not come from the engine.
    """

    status: int | None = None


class InvalidRequestError(FlureeError, ValueError):
    """The query, transaction, or argument is malformed or invalid."""


class NotFoundError(FlureeError, LookupError):
    """A ledger, branch, commit, or graph does not exist."""


class PermissionDeniedError(FlureeError, PermissionError):
    """Policy denied the operation."""


class ConflictError(FlureeError):
    """A concurrent change won; retrying against the new state may succeed."""


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
]
