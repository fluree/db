"""Connections, ledgers and snapshots."""

from __future__ import annotations

import datetime as _dt
from dataclasses import replace as _dc_replace
import json
import os
import re
import random as _random
import time as _time
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Iterable, Mapping
from typing import Any, Literal as _Literal, TypeVar, Union

from fluree import _fluree
from fluree._cypher import _table
from fluree._params import _cypher_params, _params, _sparql_params
from fluree._records import (
    Branch,
    Change,
    Commit,
    FullText,
    IndexStatus,
    MergePreview,
    MergeResult,
    RebaseResult,
    RevertPreview,
    RevertResult,
    SweepResult,
    ValidationReport,
    VerifyReport,
    _change,
    _commit,
    _merge_preview,
    _revert_preview,
    _validation_report,
)
from fluree._results import Result, RowStream
from fluree._terms import IRI, to_python
from fluree.errors import ConflictError, InvalidRequestError, PermissionDeniedError

MEMORY = ":memory:"

_T = TypeVar("_T")
_TRANSACT_ATTEMPTS = 10


def _backoff(attempt: int) -> float:
    """Seconds to wait before retry ``attempt + 1``: exponential, jittered,
    capped at a second."""
    return min(1.0, 0.005 * 2**attempt) * (0.5 + _random.random() / 2)


def _close(txn: Any) -> None:
    if txn._native.is_open:
        txn.rollback()

Query = Union[str, dict[str, Any]]
Data = Union[str, dict[str, Any], list[Any], "os.PathLike[str]"]
Format = _Literal["jsonld", "turtle", "trig"]
ExportFormat = _Literal["turtle", "trig", "ntriples", "nquads", "jsonld"]
Language = _Literal["sparql", "cypher", "jsonld"]
SelectLanguage = _Literal["sparql", "cypher"]
MergeStrategy = _Literal["take-both", "abort", "take-source", "take-branch"]
RebaseStrategy = _Literal["take-both", "abort", "take-source", "take-branch", "skip"]
RevertStrategy = _Literal["abort", "take-source", "take-branch"]
CommitRef = Union[int, str, Commit]

_EXPORT_SUFFIXES: dict[str, ExportFormat] = {
    ".ttl": "turtle",
    ".trig": "trig",
    ".nt": "ntriples",
    ".nq": "nquads",
    ".jsonld": "jsonld",
    ".json": "jsonld",
}

_TURTLE_SUFFIXES = {".ttl", ".nt", ".trig"}
_JSON_SUFFIXES = {".jsonld", ".json"}
_SPARQL_SUFFIXES = {".rq", ".ru", ".sparql"}
_CYPHER_SUFFIXES = {".cypher", ".cyp", ".cql"}


def connect(
    path: str | os.PathLike[str] | None = None,
    *,
    config: dict[str, Any] | None = None,
    indexing: bool = True,
) -> Connection:
    """Open a Fluree database, creating it if needed.

    Give a directory ``path``, or ``":memory:"`` for a database that lives
    only as long as the connection. ``indexing=False`` turns off background
    indexing, for a process that only queries while another one indexes.

    For anything else — S3 or split commit/index storage, a DynamoDB
    nameservice, encryption at rest, values read from environment variables —
    pass a JSON-LD connection ``config`` instead of a path; it is the same
    document ``fluree server`` takes with ``--connection-config``.

    The connection is a context manager that closes it on exit::

        with fluree.connect("./data") as conn:
            ledger = conn.create("people")
    """
    if (path is None) == (config is None):
        raise InvalidRequestError("give exactly one of path or config")
    if config is not None:
        return Connection(_fluree.Connection.from_config(config))
    if os.fspath(path) == MEMORY:  # type: ignore[arg-type]
        return Connection(_fluree.Connection.memory())
    return Connection(_fluree.Connection.file(Path(path), indexing=indexing))  # type: ignore[arg-type]


class Connection:
    """A connection to a Fluree database. Create one with :func:`connect`."""

    __slots__ = ("_native",)

    def __init__(self, native: _fluree.Connection) -> None:
        self._native = native

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __contains__(self, ledger: object) -> bool:
        return isinstance(ledger, str) and self._native.exists(ledger)

    def close(self) -> None:
        """Flush pending writes and release the database.

        Afterwards the connection, and every ledger, snapshot, transaction
        and stream opened through it, raises :class:`InvalidRequestError`;
        an open transaction can still be rolled back. Closing again does
        nothing."""
        self._native.close()

    def create(self, ledger: str, *, source: str | os.PathLike[str] | None = None) -> Ledger:
        """Create a ledger, optionally bulk-loading RDF files into it.

        ``source`` is a file or a directory of Turtle, TriG, N-Triples,
        N-Quads, or JSON-LD files. Bulk loading needs a file-backed connection.
        """
        src = None if source is None else Path(source)
        return Ledger(self, self._native.create(ledger, src))

    def ledger(self, ledger: str) -> Ledger:
        """An existing ledger. Raises :class:`NotFoundError` if there is none."""
        return Ledger(self, self._native.ledger(ledger))

    def ledgers(self) -> list[str]:
        """The ids of every ledger, as ``name:branch``."""
        return self._native.ledgers()

    def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a query whose ``FROM`` names the ledgers to read.

        SPARQL ``FROM <ledger>`` (and ``FROM NAMED``) or a JSON-LD ``"from"``
        picks the ledgers, so one query can span several; a ledger address can
        carry a time (``<people@t:5>``), and SPARQL ``FROM ... TO ...`` reads a
        range of history. Results, parameters, ``max_fuel`` and ``timeout`` are
        as for :meth:`Snapshot.query`. Cypher has no ``FROM``; run it on a ledger.
        """
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters),
        )

    def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """A SPARQL ``SELECT`` whose ``FROM`` names the ledgers, as for
        :meth:`query`, returning its :class:`Result`; see
        :meth:`Snapshot.select`."""
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters), select=True,
        )

    def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(
            self._run, query, max_fuel, timeout,
            language=language, params=_params(parameters, kwparameters),
        )

    def _run(self, query: Any, language: str, controls: dict[str, Any] | None, params: Any) -> Any:
        if language == "cypher":
            raise InvalidRequestError("a Cypher query names no ledgers; run it with ledger.query()")
        if language == "sparql":
            return self._native.query_sparql_from(query, None, controls, params)
        return self._native.query_jsonld_from(query, controls)

    def restore(self, path: str | os.PathLike[str], ledger: str) -> Ledger:
        """Create ``ledger`` from a ``.flpack`` archive (see :meth:`Ledger.archive`)."""
        return Ledger(self, self._native.restore(Path(path), ledger))

    def drop(self, ledger: str) -> None:
        """Delete a ledger, every branch and all history (``"people"``), or one
        branch (``"people:dev"``).

        A ledger's first branch cannot be dropped on its own; drop the ledger.
        A branch that other branches were created from is hidden at once but
        its storage is kept until they are dropped too.
        """
        if ":" in ledger:
            self._native.drop_branch(ledger)
        else:
            self._native.drop(ledger)


class Ledger:
    """A ledger: write to it, and query its latest state or any past one."""

    __slots__ = ("_connection", "_id", "_policy")

    def __init__(
        self, connection: Connection, ledger_id: str, policy: dict[str, Any] | None = None
    ) -> None:
        self._connection = connection
        self._id = ledger_id
        self._policy = policy

    @property
    def id(self) -> str:
        return self._id

    def __repr__(self) -> str:
        governed = " governed" if self._policy is not None else ""
        return f"<Ledger {self._id!r}{governed}>"

    def with_policy(
        self,
        *,
        identity: str | None = None,
        policy_class: str | list[str] | None = None,
        policy: dict[str, Any] | list[Any] | None = None,
        values: dict[str, Any] | None = None,
        default_allow: bool | None = None,
    ) -> Ledger:
        """This ledger, governed: every read and write through the returned
        handle is subject to policy.

        - ``identity``: the IRI (often a DID) to evaluate policy as; the
          policies attached to it in the ledger apply.
        - ``policy_class``: apply the ledger's policies of these classes.
        - ``policy``: inline JSON-LD policy documents to apply.
        - ``values``: bindings for variables the policies reference,
          e.g. ``{"?$dept": "eng"}``.
        - ``default_allow``: whether data no policy covers is visible and
          writable. ``None`` defers to the ledger's configured default, which
          is to deny.

        Reads filter out what policy does not allow; a write that policy does
        not allow raises :class:`PermissionDeniedError`.
        """
        opts: dict[str, Any] = {}
        if identity is not None:
            opts["identity"] = identity
        if policy_class is not None:
            opts["policyClass"] = [policy_class] if isinstance(policy_class, str) else list(policy_class)
        if policy is not None:
            opts["policy"] = policy
        if values is not None:
            opts["policyValues"] = values
        if default_allow is not None:
            opts["defaultAllow"] = default_allow
        if not opts:
            raise InvalidRequestError("with_policy needs at least one policy option")
        return Ledger(self._connection, self._id, {**(self._policy or {}), **opts})

    def insert(self, data: Data, *, format: Format | None = None, message: str | None = None) -> Commit:
        """Add data: JSON-LD (a dict, list, or JSON text), Turtle, or TriG.

        A path is read as a file, its format taken from the extension unless
        ``format`` is given. ``message`` is recorded with the commit, and
        :meth:`log` shows it.
        """
        return self._transact("insert", *_rdf_payload(data, format), message)

    def upsert(self, data: Data, *, format: Format | None = None, message: str | None = None) -> Commit:
        """Add data, replacing existing values of the properties it sets."""
        return self._transact("upsert", *_rdf_payload(data, format), message)

    def update(
        self,
        transaction: str | dict[str, Any] | os.PathLike[str],
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        message: str | None = None,
        **kwparameters: Any,
    ) -> Commit:
        """Apply a SPARQL UPDATE, a Cypher write, or a JSON-LD
        ``where``/``delete``/``insert``.

        The language is told from the text (``language`` overrides it); a
        path is read as a file, ``.rq``/``.ru`` as SPARQL and
        ``.cypher``/``.cyp``/``.cql`` as Cypher. A Cypher write may be a ``;``
        script, committed all or nothing — and mixes with the other languages
        inside a :meth:`transaction`; its ``$name`` parameters come from
        ``parameters`` and keyword arguments, and the records a ``RETURN``
        produces are the commit's ``result``.

        ``parameters`` and keyword arguments bind values by name: Cypher
        ``$name``, and in SPARQL the variable ``?name`` (or ``$name``) wherever
        it appears, as if the value were written in its place; see
        :meth:`Snapshot.query`.
        """
        kind, payload = _update_payload(transaction, language)
        params = _params(parameters, kwparameters)
        if kind == "cypher":
            # A Cypher write stages like any other, in a transaction of its
            # own; one whose RETURN was read is run again on a conflict.
            result, commit = self._retrying(
                lambda txn: txn.update(payload, params, language="cypher"), message
            )
            return _dc_replace(commit, result=result)
        return self._transact("update", kind, payload, message, _update_params(kind, params))

    def sync(
        self,
        data: Data,
        *,
        graph: str | None = None,
        format: Format | None = None,
        allow_empty: bool = False,
        dry_run: bool = False,
        message: str | None = None,
    ) -> Commit:
        """Make a graph hold exactly ``data``, committing only the difference:
        facts missing from ``data`` are retracted, new ones asserted, and
        unchanged ones left alone. History keeps every earlier state.

        ``graph`` is a named graph's IRI; by default the default graph is
        synced. ``data`` is as for :meth:`insert`, and Turtle, N-Triples or
        TriG text.

        Empty ``data`` would clear the graph, so it is refused unless
        ``allow_empty`` is set. With ``dry_run`` nothing is committed and the
        returned :class:`Commit` (``id`` ``None``) counts what would change.
        When ``data`` already matches, no commit is written either.
        """
        kind, payload = _rdf_payload(data, format)
        native = self._connection._native
        commit = native.sync(
            self._id, kind, payload, graph, allow_empty, dry_run, self._policy, message
        )
        return Commit(**commit)

    def transact(self, fn: Callable[..., _T], /, *args: Any, **kwargs: Any) -> _T:
        """Run ``fn(txn, *args, **kwargs)`` in a :class:`Transaction` and
        commit it, running ``fn`` again in a fresh transaction if another
        commit lands first. Returns what ``fn`` returns.

        The way to read and then write safely: values ``fn`` reads from
        ``txn`` are re-read on every attempt, so what it writes never rests
        on data another writer has since changed. ``fn`` may commit ``txn``
        itself (to pass a ``message``); otherwise it is committed when ``fn``
        returns. An exception from ``fn`` rolls the transaction back and
        propagates.
        """
        return self._retrying(lambda txn: fn(txn, *args, **kwargs), None)[0]

    def _retrying(self, fn: Callable[[Transaction], _T], message: str | None) -> tuple[_T, Commit]:
        """``fn(txn)`` and its committed transaction's :class:`Commit`, run
        again on a conflict at commit."""
        for attempt in range(_TRANSACT_ATTEMPTS):
            txn = self.transaction(message=message)
            try:
                result = fn(txn)
                if txn._native.is_open:
                    txn.commit()
                assert txn.committed is not None
                return result, txn.committed
            except ConflictError:
                # Only a conflict on this transaction's own commit is retried.
                if txn._native.is_open or attempt + 1 == _TRANSACT_ATTEMPTS:
                    _close(txn)
                    raise
            except BaseException:
                _close(txn)
                raise
            _time.sleep(_backoff(attempt))
        raise AssertionError("unreachable")

    def transaction(self, *, message: str | None = None) -> Transaction:
        """Open a :class:`Transaction`: several writes, each seeing the ones
        before it, committed together as one commit, with ``message``.

        Use it as a context manager to commit on a clean exit and roll back
        on an exception::

            with ledger.transaction(message="onboard bob") as txn:
                txn.insert(...)
                txn.update(...)
            txn.committed  # the Commit
        """
        native = self._connection._native.begin(self._id, self._policy)
        return Transaction(self, native, message)

    def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Query the latest state. See :meth:`Snapshot.query`."""
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters),
        )

    def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a ``SELECT`` on the latest state; see :meth:`Snapshot.select`."""
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters), select=True,
        )

    def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(
            self._run, query, max_fuel, timeout,
            language=language, params=_params(parameters, kwparameters),
        )

    def stream(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
        **kwparameters: Any,
    ) -> RowStream:
        """Run a SELECT and read its rows as they are produced; see
        :class:`RowStream`. ``max_fuel`` and ``timeout`` cover the whole stream."""
        controls = _controls(max_fuel, timeout, stats=False)
        query, params = _streamable(query, _params(parameters, kwparameters))
        native = self._connection._native.stream(self._id, query, None, self._policy, controls, params)
        return RowStream(native, batch_size)

    def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it."""
        native = self._connection._native
        params = _params(parameters, kwparameters)
        if _query_language(query, language) == "cypher":
            return native.explain_cypher(self._id, _cypher_text(query), _cypher_params(params), None, self._policy)
        query, params = _explainable(query, language, params)
        return native.explain(self._id, query, None, self._policy, params)

    def _run(self, query: Any, language: str, controls: dict[str, Any] | None, params: Any) -> Any:
        native = self._connection._native
        if language == "cypher":
            return _table(native.cypher_query(self._id, query, params, None, self._policy, controls))
        if language == "sparql":
            return native.query_sparql(self._id, query, None, self._policy, controls, params)
        return native.query_jsonld(self._id, query, None, self._policy, controls)

    def snapshot(self) -> Snapshot:
        """The latest state, frozen: every query on it sees the same data."""
        return Snapshot(self._connection._native.snapshot(self._id, None, self._policy), self)

    def at(
        self,
        t: int | None = None,
        *,
        time: _dt.datetime | str | None = None,
        commit: str | None = None,
    ) -> Snapshot:
        """The ledger as it was at transaction ``t``, a wall-clock ``time``, or
        a ``commit``. Give exactly one.

        ``commit`` is a full commit id (:attr:`Commit.id`), or its hex
        :attr:`Commit.digest` or a unique prefix of it (:attr:`Commit.short_id`).
        """
        given = [(k, v) for k, v in (("t", t), ("time", time), ("commit", commit)) if v is not None]
        if len(given) != 1:
            raise InvalidRequestError("give exactly one of t, time, or commit")
        kind, value = given[0]
        if isinstance(value, _dt.datetime):
            if value.tzinfo is None:
                raise InvalidRequestError("time must be timezone-aware")
            value = value.isoformat()
        return Snapshot(
            self._connection._native.snapshot(self._id, (kind, value), self._policy), self
        )

    def history(
        self,
        subject: str,
        predicate: str | None = None,
        *,
        from_t: int = 1,
        to_t: int | None = None,
    ) -> list[Change]:
        """Every change to ``subject`` between transactions ``from_t`` and
        ``to_t`` (default: the latest), oldest first; within one transaction,
        retractions come before assertions.

        ``subject`` and ``predicate`` are full IRIs. Give ``predicate`` to
        follow one property.
        """
        s = _iri_ref(subject)
        p = _iri_ref(predicate) if predicate is not None else "?p"
        to = "latest" if to_t is None else int(to_t)
        sparql = (
            f"PREFIX f: <{_F}> SELECT ?p ?v ?t ?op "
            f"FROM <{self._id}@t:{int(from_t)}> TO <{self._id}@t:{to}> "
            f"WHERE {{ << {s} {p} ?v >> f:t ?t . << {s} {p} ?v >> f:op ?op . }} "
            "ORDER BY ?t ?op"
        )
        rows = _sparql_result(self._connection._native.query_sparql_from(sparql, self._policy))
        return [
            Change(
                t=row.t,
                op="assert" if row.op else "retract",
                subject=IRI(subject),
                predicate=row.p if predicate is None else IRI(predicate),
                value=row.v,
            )
            for row in rows
        ]

    def log(self, limit: int | None = None) -> list[Commit]:
        """The ledger's commits, newest first; at most ``limit`` of them."""
        commits, _total = self._connection._native.log(self._id, limit)
        return [_commit(c) for c in commits]

    def changes(self, commit: CommitRef) -> list[Change]:
        """The facts one commit asserted and retracted.

        ``commit`` is its ``t``, its id, a prefix of its hex digest
        (:attr:`Commit.short_id`), or a :class:`Commit`. Under :meth:`with_policy`, facts the policy
        hides are left out.
        """
        detail = self._connection._native.commit_detail(self._id, _commit_ref(commit), self._policy)
        return [_change(detail["t"], flake) for flake in detail["flakes"]]

    def branch(self, name: str) -> Ledger:
        """Create branch ``name`` from this ledger's latest state and return
        it. Branch from a past state with ``ledger.at(...).branch(name)``.

        The new branch shares this one's history up to that point; from then
        on each changes independently, until :meth:`merge` brings one's
        commits into the other.
        """
        return self._create_branch(name, None)

    def branches(self) -> list[Branch]:
        """Every branch of this ledger, by name."""
        return [Branch(**b) for b in self._connection._native.branches(self._id)]

    def merge(self, source: str | Ledger, *, strategy: MergeStrategy = "take-both") -> MergeResult:
        """Bring the commits of branch ``source`` (a name like ``"dev"``, or
        its :class:`Ledger`) into this branch.

        If this branch has no commits of its own since ``source`` was created
        from it, it moves forward to ``source``'s latest commit. Otherwise one
        merge commit applies ``source``'s changes, and ``strategy`` settles
        properties both branches changed:

        - ``"take-both"``: keep both values.
        - ``"abort"``: change nothing and raise :class:`ConflictError`.
        - ``"take-source"``: ``source``'s value wins.
        - ``"take-branch"``: this branch's value wins.

        Preview a merge with :meth:`merge_preview`.
        """
        self._require_unrestricted("merge")
        raw = self._connection._native.merge(self._id, self._source_branch(source), strategy)
        return MergeResult(**raw)

    def merge_preview(
        self,
        source: str | Ledger,
        *,
        strategy: MergeStrategy = "take-both",
        details: bool = False,
        changes: bool = False,
        changes_after: str | None = None,
        validate: bool = True,
        conflicts: bool = True,
        max_commits: int | None = 500,
        max_conflicts: int | None = 200,
        max_changes: int | None = 500,
    ) -> MergePreview:
        """What :meth:`merge` of ``source`` would do, without doing it.

        - ``strategy``: the one to judge ``mergeable`` by.
        - ``details``: include what each side wrote to each conflict.
        - ``changes``: include the net facts the merge would bring in, at most
          ``max_changes`` of them per call; pass the result's
          ``changes_after`` back to read the next page.
        - ``validate=False`` skips checking the merged state against the
          ledger's SHACL shapes.
        - ``conflicts=False`` skips finding conflicts, the costly part on
          branches that have drifted far apart.
        - ``max_commits`` and ``max_conflicts`` cap the lists returned, not the
          work; ``None`` lifts a cap.
        """
        self._require_unrestricted("merge_preview")
        if changes_after is not None and not changes:
            raise InvalidRequestError("changes_after needs changes=True")
        options = {
            "strategy": strategy,
            "conflicts": conflicts,
            "details": details,
            "changes": changes,
            "changes_after": changes_after,
            "validate": validate,
            "max_commits": max_commits,
            "max_conflicts": max_conflicts,
            "max_changes": max_changes,
        }
        native = self._connection._native
        return _merge_preview(native.merge_preview(self._id, self._source_branch(source), options))

    def rebase(self, *, strategy: RebaseStrategy = "take-both") -> RebaseResult:
        """Replay this branch's own commits on top of the latest commit of the
        branch it was created from, as if it had been created from there.

        ``strategy`` settles properties both branches changed: ``"take-both"``
        keeps both values, ``"abort"`` changes nothing and raises
        :class:`ConflictError`, ``"take-source"`` lets the source branch's
        value win, ``"take-branch"`` lets this branch's win, and ``"skip"``
        drops each of this branch's commits that conflicts.
        """
        self._require_unrestricted("rebase")
        return RebaseResult(**self._connection._native.rebase(self._id, strategy))

    def revert(
        self,
        commits: CommitRef | list[CommitRef],
        *,
        strategy: RevertStrategy = "abort",
    ) -> RevertResult:
        """Undo one commit or several, in one new commit.

        A commit is named by its ``t``, its id, a prefix of its hex digest, or
        a :class:`Commit`. ``strategy`` settles properties that later commits
        changed again: ``"abort"`` changes nothing and raises
        :class:`ConflictError`, ``"take-source"`` undoes them anyway, and
        ``"take-branch"`` keeps the later values. Merge commits cannot be
        reverted.

        Preview a revert with :meth:`revert_preview`.
        """
        self._require_unrestricted("revert")
        native = self._connection._native
        return RevertResult(**native.revert(self._id, _commit_refs(commits), strategy))

    def revert_preview(
        self,
        commits: CommitRef | list[CommitRef],
        *,
        strategy: RevertStrategy = "abort",
        validate: bool = True,
        conflicts: bool = True,
        max_commits: int | None = 500,
        max_conflicts: int | None = 200,
    ) -> RevertPreview:
        """What :meth:`revert` would do, without doing it. ``strategy`` is the
        one to judge ``revertable`` by; the other options are as for
        :meth:`merge_preview`."""
        self._require_unrestricted("revert_preview")
        options = {
            "strategy": strategy,
            "conflicts": conflicts,
            "validate": validate,
            "max_commits": max_commits,
            "max_conflicts": max_conflicts,
        }
        native = self._connection._native
        return _revert_preview(native.revert_preview(self._id, _commit_refs(commits), options))

    def validate(
        self,
        shapes: Data | None = None,
        *,
        shapes_graph: str | None = None,
        graph: str | None = None,
        include_attached: bool = False,
        format: Format | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
    ) -> ValidationReport:
        """Check the data against SHACL shapes, without changing anything.

        By default the shapes are the ledger's own — the ones its writes are
        checked against. Give ``shapes`` (JSON-LD, Turtle, or a path, as for
        :meth:`insert`) to check against other shapes, or ``shapes_graph``,
        the IRI of a named graph that holds them; ``include_attached`` adds
        the ledger's own shapes to either. ``graph`` validates a named graph
        rather than the default graph. ``max_fuel`` and ``timeout`` bound the
        work, as for :meth:`query`.
        """
        self._require_unrestricted("validate")
        if shapes is not None and shapes_graph is not None:
            raise InvalidRequestError("give shapes or shapes_graph, not both")
        if shapes is not None:
            kind, payload = _rdf_payload(shapes, format)
        elif shapes_graph is not None:
            kind, payload = "graph", shapes_graph
        else:
            kind, payload = "attached", None
        options = {
            "shapes_kind": kind,
            "shapes": payload,
            "graph": graph,
            "include_attached": include_attached,
            "max_fuel": None if max_fuel is None else float(max_fuel),
            "timeout": None if timeout is None else float(timeout),
        }
        return _validation_report(self._connection._native.validate(self._id, options))

    def index_status(self) -> IndexStatus:
        """How far indexing has caught up with the ledger's commits."""
        return IndexStatus(**self._connection._native.index_status(self._id))

    def index(self, *, timeout: float | None = None) -> int:
        """Index everything committed so far, waiting up to ``timeout``
        seconds; returns the indexed ``t``. Queries do not need this — they
        read unindexed commits too — but it makes them faster sooner.
        Needs a connection with background indexing."""
        self._require_unrestricted("index")
        return self._connection._native.index(self._id, timeout)

    def reindex(self) -> int:
        """Rebuild the index from scratch from the commit history; returns the
        indexed ``t``."""
        self._require_unrestricted("reindex")
        return self._connection._native.reindex(self._id)

    def verify(self, *, max_commits: int | None = None) -> VerifyReport:
        """Check that every commit, back to the first (or the last
        ``max_commits``), and the index root are present and readable."""
        self._require_unrestricted("verify")
        return VerifyReport(**self._connection._native.verify(self._id, max_commits))

    def sweep(self, *, dry_run: bool = False) -> SweepResult:
        """Delete index files that no index of any branch of this ledger
        references any more — left behind as indexing replaces old index
        files. ``dry_run`` counts them without deleting."""
        self._require_unrestricted("sweep")
        return SweepResult(**self._connection._native.sweep(self._id, dry_run))

    def _create_branch(self, name: str, at: tuple[str, Any] | None) -> Ledger:
        self._require_unrestricted("branch")
        return Ledger(self._connection, self._connection._native.create_branch(self._id, name, at))

    def _source_branch(self, source: str | Ledger) -> str:
        """The branch name of ``source``, which must be a branch of this ledger."""
        ledger_id = source.id if isinstance(source, Ledger) else source
        name, sep, branch = ledger_id.partition(":")
        if not sep:
            return ledger_id
        if name != self._id.partition(":")[0]:
            raise InvalidRequestError(f"{ledger_id!r} is not a branch of {self._id!r}")
        return branch

    def export(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        format: ExportFormat | None = None,
        graph: str | None = None,
        all_graphs: bool = False,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        """Export the ledger's current data as RDF.

        Writes to ``path`` and returns ``None``, or returns the text when no
        path is given. ``format`` is ``"turtle"``, ``"trig"``, ``"ntriples"``,
        ``"nquads"``, or ``"jsonld"``; by default it follows the path's
        extension, else Turtle. The default graph is exported unless ``graph``
        names one or ``all_graphs`` asks for every named graph too (use TriG
        or N-Quads for that). ``context`` supplies prefixes for Turtle and
        JSON-LD. Export the past with ``ledger.at(...).export(...)``.
        """
        return self._export(path, format, graph, all_graphs, context, None)

    def archive(self, path: str | os.PathLike[str], *, include_indexes: bool = True) -> None:
        """Write the whole ledger, every commit, to a ``.flpack`` archive that
        :meth:`Connection.restore` loads back. Without indexes the archive is
        smaller but the restored ledger must reindex before it is fast."""
        self._require_unrestricted("archive")
        self._connection._native.archive(self._id, Path(path), include_indexes)

    def _export(
        self,
        path: str | os.PathLike[str] | None,
        format: ExportFormat | None,
        graph: str | None,
        all_graphs: bool,
        context: dict[str, Any] | None,
        at: tuple[str, Any] | None,
    ) -> str | None:
        self._require_unrestricted("export")
        if format is None:
            suffix = Path(path).suffix.lower() if path is not None else ""
            format = _EXPORT_SUFFIXES.get(suffix, "turtle")
        target = None if path is None else Path(path)
        text, _ = self._connection._native.export(
            self._id, format, target, graph, all_graphs, context, at
        )
        return text

    def _require_unrestricted(self, operation: str) -> None:
        # These operations bypass policy enforcement entirely.
        if self._policy is not None:
            raise PermissionDeniedError(
                f"{operation} is not subject to policy, so it is not available on a "
                "policy-governed ledger"
            )

    @property
    def context(self) -> dict[str, Any] | None:
        """The ledger's default JSON-LD context, or ``None``.

        Queries that omit ``@context`` (JSON-LD) or ``PREFIX`` (SPARQL) resolve
        their prefixes against it. Replace it with :meth:`set_context`.
        """
        return self._connection._native.context(self._id)

    def set_context(self, context: dict[str, Any]) -> None:
        """Replace the ledger's default JSON-LD context: a ``{prefix: IRI}`` map."""
        self._require_unrestricted("set_context")
        self._connection._native.set_context(self._id, context)

    def info(self) -> dict[str, Any]:
        """Ledger metadata and statistics."""
        return self._connection._native.info(self._id)

    def graphs(self) -> list[IRI]:
        """The IRIs of the ledger's named graphs, in the order they were first
        written to. The default graph and Fluree's own graphs (commit
        metadata, configuration) are left out.

        A graph is created by writing to it — TriG, SPARQL ``GRAPH``, or a
        JSON-LD node's ``"@graph"`` — and queried with SPARQL ``GRAPH`` or
        JSON-LD ``["graph", iri, pattern]``."""
        system = {"urn:default", f"urn:fluree:{self._id}#txn-meta", self._config_graph}
        named = self.info()["ledger"]["named-graphs"]
        return [IRI(g["iri"]) for g in named if g["iri"] not in system]

    def drop_graph(self, graph: str) -> Commit:
        """Retract everything in named graph ``graph`` (its full IRI) in one
        commit. History keeps it, and the graph can be written to again. The
        default graph and Fluree's own graphs cannot be dropped."""
        self._require_unrestricted("drop_graph")
        return Commit(**self._connection._native.drop_graph(self._id, graph))

    def set_full_text(
        self, properties: Iterable[str], *, language: str = "en", reindex: bool = True
    ) -> None:
        """Make the plain-string values of ``properties`` searchable with
        ``fulltext()``, analyzed in ``language`` (a BCP-47 tag; values tagged
        with their own language use it instead).

        ``properties`` are IRIs, full or compact against the ledger's default
        context; they replace the ones configured before, and an empty list
        turns the configuration off. The data itself is not changed.

        ``reindex`` rebuilds the index so values already in the ledger are
        searchable. From then on, a property is searchable as values commit
        once an index build has seen values of it: a property configured
        before the ledger holds any of its values is searchable after the
        next index build (background indexing, or :meth:`reindex`). Pass
        ``reindex=False`` to defer the rebuild, for instance before a bulk
        load, and call :meth:`reindex` when ready.
        """
        self._require_unrestricted("set_full_text")
        targets = list(properties)
        config_iri = self._config_subject() or f"urn:fluree:{self._id}:config:ledger"
        if targets:
            group = f"urn:fluree:{self._id}:config:fullText"
            doc: dict[str, Any] = {
                "@id": config_iri,
                "@type": _F + "LedgerConfig",
                "@graph": self._config_graph,
                _F + "fullTextDefaults": {
                    "@id": group,
                    "@type": _F + "FullTextDefaults",
                    _F + "defaultLanguage": language,
                    _F + "property": [
                        {
                            "@id": f"{group}:{i}",
                            "@type": _F + "FullTextProperty",
                            _F + "target": {"@id": str(target)},
                        }
                        for i, target in enumerate(targets)
                    ],
                },
            }
            # The node names its graph, so it goes in an envelope: a top-level
            # "@graph" would be the envelope's own.
            self.upsert({"@context": self.context or {}, "@graph": [doc]})
        else:
            self.update(
                f"PREFIX f: <{_F}> WITH <{self._config_graph}> "
                f"DELETE {{ <{config_iri}> f:fullTextDefaults ?g }} "
                f"WHERE {{ <{config_iri}> f:fullTextDefaults ?g }}"
            )
        if reindex:
            self.reindex()

    def full_text(self) -> FullText | None:
        """The ledger's full-text configuration, or ``None`` without one."""
        self._require_unrestricted("full_text")
        config_iri = self._config_subject()
        if config_iri is None:
            return None
        rows = self._config_query(
            f"SELECT ?target ?language WHERE {{ <{config_iri}> f:fullTextDefaults ?g . "
            "OPTIONAL { ?g f:property ?p . ?p f:target ?target } "
            "OPTIONAL { ?g f:defaultLanguage ?language } } ORDER BY ?target"
        )
        if not rows:
            return None
        targets = tuple(dict.fromkeys(r.target for r in rows if r.target is not None))
        return FullText(targets, rows[0].language)

    @property
    def _config_graph(self) -> str:
        return f"urn:fluree:{self._id}#config"

    def _config_subject(self) -> str | None:
        """The ledger's ``f:LedgerConfig``, the first by IRI as the engine reads it."""
        rows = self._config_query("SELECT ?c WHERE { ?c a f:LedgerConfig } ORDER BY ?c LIMIT 1")
        return str(rows[0].c) if rows else None

    def _config_query(self, select: str) -> Result:
        sparql = f"PREFIX f: <{_F}> " + select.replace(" WHERE ", f" FROM <{self._config_graph}> WHERE ", 1)
        return _sparql_result(self._connection._native.query_sparql_from(sparql))

    def _transact(
        self, op: str, kind: str, payload: Any, message: str | None, params: Any = None
    ) -> Commit:
        native = self._connection._native
        return Commit(**native.transact(self._id, op, kind, payload, self._policy, message, params))


class Snapshot:
    """A ledger frozen at one point in time.

    Get one from :meth:`Ledger.snapshot` or :meth:`Ledger.at`.
    """

    __slots__ = ("_ledger", "_native")

    def __init__(self, native: _fluree.Snapshot, ledger: Ledger) -> None:
        self._native = native
        self._ledger = ledger

    def export(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        format: ExportFormat | None = None,
        graph: str | None = None,
        all_graphs: bool = False,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        """Export the data as of this snapshot; see :meth:`Ledger.export`."""
        return self._ledger._export(path, format, graph, all_graphs, context, ("t", self.t))

    def branch(self, name: str) -> Ledger:
        """Create branch ``name`` from this past state; see :meth:`Ledger.branch`."""
        return self._ledger._create_branch(name, ("t", self.t))

    @property
    def ledger(self) -> str:
        return self._native.ledger

    @property
    def t(self) -> int:
        """The transaction this snapshot reflects."""
        return self._native.t

    def __repr__(self) -> str:
        return f"<Snapshot {self.ledger!r} t={self.t}>"

    def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a SPARQL, Cypher, or JSON-LD query.

        The language is told from the query — a dict or JSON text is
        JSON-LD, text that opens like SPARQL (``PREFIX``, ``SELECT``, ...) is
        SPARQL, and text that opens like Cypher (``MATCH``, ``UNWIND``, ...)
        is Cypher — or set with ``language``.

        - SPARQL ``SELECT`` and every Cypher query return a :class:`Result`;
          Cypher nodes, relationships and paths come back as :class:`Node`,
          :class:`Relationship` and :class:`Path`.
        - SPARQL ``ASK`` returns a ``bool``; ``CONSTRUCT``/``DESCRIBE`` the
          constructed graph as a JSON-LD document.
        - A JSON-LD query returns its JSON result as Python objects.

        ``parameters`` and keyword arguments bind values by name. In Cypher
        they are the ``$name`` parameters. In SPARQL each names a variable —
        ``?name`` or ``$name``, the same variable — and the value stands in
        for it wherever it appears, as if written there: ``query("SELECT ?s
        WHERE { ?s ex:name $name }", name="Alice")``. A value is an
        :class:`IRI`, :class:`BlankNode`, :class:`LangString` or
        :class:`Literal`, or a Python ``str``, ``int``, ``float``, ``bool``,
        ``Decimal``, ``datetime``, ``date`` or ``time``; a :class:`Node` stands
        for its ``element_id``. A SPARQL parameter the query never mentions is
        an error, since its misspelt variable would otherwise match anything;
        a JSON-LD query takes no parameters.

        A Cypher write is refused here; run it with :meth:`Ledger.update`.

        ``max_fuel`` caps the work the query may do (see :meth:`profile` for
        what a query costs); past it the query stops with
        :class:`ResourceLimitError`. ``timeout`` is in seconds; past it the
        query is cancelled with :class:`QueryTimeoutError`. Cypher takes
        ``timeout`` but not yet ``max_fuel``.
        """
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters),
        )

    def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a SPARQL ``SELECT`` or a Cypher query and return its
        :class:`Result` — :meth:`query` for the queries whose result is a
        table, typed as one. Anything else (``ASK``, ``CONSTRUCT``, JSON-LD)
        raises :class:`InvalidRequestError` without running. Parameters,
        ``max_fuel`` and ``timeout`` are as for :meth:`query`."""
        return _execute(
            self._run, query, max_fuel, timeout, False,
            language=language, params=_params(parameters, kwparameters), select=True,
        )

    def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(
            self._run, query, max_fuel, timeout,
            language=language, params=_params(parameters, kwparameters),
        )

    def stream(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
        **kwparameters: Any,
    ) -> RowStream:
        """Run a SELECT and read its rows as they are produced; see
        :class:`RowStream`."""
        controls = _controls(max_fuel, timeout, stats=False)
        query, params = _streamable(query, _params(parameters, kwparameters))
        return RowStream(self._native.stream(query, controls, params), batch_size)

    def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it."""
        params = _params(parameters, kwparameters)
        if _query_language(query, language) == "cypher":
            return self._native.explain_cypher(_cypher_text(query), _cypher_params(params))
        query, params = _explainable(query, language, params)
        return self._native.explain(query, params)

    def _run(self, query: Any, language: str, controls: dict[str, Any] | None, params: Any) -> Any:
        if language == "cypher":
            return _table(self._native.cypher_query(query, params, controls))
        if language == "sparql":
            return self._native.query_sparql(query, controls, params)
        return self._native.query_jsonld(query, controls)


class Transaction:
    """Writes staged one at a time and committed together as one commit.

    Get one from :meth:`Ledger.transaction`. Each write applies over the ones
    before it — an ``update``'s ``WHERE`` sees an earlier ``insert`` — and is
    checked as it is staged, so a write that is invalid or that policy does
    not allow raises at once and is left out; the transaction carries on
    without it. Queries on the transaction read the staged state; nothing is
    visible on the ledger until :meth:`commit`.

    A fact that one write adds and a later one removes (or the reverse) is
    left out of the commit, so the commit holds only the net change.

    The writes stage against the ledger as it was when the transaction
    began. If another commit lands first:

    - a transaction that was only written to is staged again on top of it —
      each update's ``WHERE`` matches the new data;
    - a transaction that was also *read* (``query``, ``explain``, or the
      ``RETURN`` records of a Cypher write) raises :class:`ConflictError` on
      :meth:`commit`, since what was read may have decided what was written. Run it again, or use
      :meth:`Ledger.transact`, which does.

    As a context manager it commits on a clean exit and rolls back on an
    exception; :attr:`committed` holds the resulting :class:`Commit`.
    """

    __slots__ = ("_ledger", "_message", "_native", "committed")

    def __init__(self, ledger: Ledger, native: _fluree.Transaction, message: str | None) -> None:
        self._ledger = ledger
        self._native = native
        self._message = message
        self.committed: Commit | None = None

    def __repr__(self) -> str:
        state = "open" if self._native.is_open else "closed"
        return f"<Transaction {self._native.ledger!r} {state}>"

    def __enter__(self) -> Transaction:
        return self

    def __exit__(self, exc_type: object, *exc: object) -> None:
        if not self._native.is_open:
            return
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

    def insert(self, data: Data, *, format: Format | None = None) -> None:
        """Stage an insert; see :meth:`Ledger.insert`."""
        self._native.stage("insert", *_rdf_payload(data, format))

    def upsert(self, data: Data, *, format: Format | None = None) -> None:
        """Stage an upsert; see :meth:`Ledger.upsert`."""
        self._native.stage("upsert", *_rdf_payload(data, format))

    def update(
        self,
        transaction: str | dict[str, Any] | os.PathLike[str],
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> Result | None:
        """Stage an update — SPARQL UPDATE, a Cypher write, or JSON-LD; see
        :meth:`Ledger.update`. A Cypher write returns the records of its
        ``RETURN`` (``None`` without one); receiving them counts as reading
        the transaction."""
        kind, payload = _update_payload(transaction, language)
        params = _params(parameters, kwparameters)
        if kind == "cypher":
            table = self._native.stage_cypher(payload, _cypher_params(params))
            return None if table is None else _table(table)
        self._native.stage("update", kind, payload, _update_params(kind, params))
        return None

    def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Query the staged state; see :meth:`Snapshot.query`."""
        return self._view().query(
            query, parameters, language=language, max_fuel=max_fuel, timeout=timeout, **kwparameters
        )

    def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a ``SELECT`` on the staged state; see :meth:`Snapshot.select`."""
        return self._view().select(
            query, parameters, language=language, max_fuel=max_fuel, timeout=timeout, **kwparameters
        )

    def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan ``query`` would run with over the staged state."""
        return self._view().explain(query, parameters, language=language, **kwparameters)

    def commit(self, *, message: str | None = None) -> Commit:
        """Commit the staged writes as one commit, recording ``message`` (by
        default the one the transaction was opened with). The transaction is
        closed afterwards, even if the commit fails."""
        self.committed = Commit(**self._native.commit(message or self._message))
        return self.committed

    def rollback(self) -> None:
        """Discard the staged writes and close the transaction."""
        self._native.rollback()

    def _view(self) -> Snapshot:
        return Snapshot(self._native.snapshot(), self._ledger)


@dataclass(frozen=True, slots=True)
class QueryProfile:
    """A query's result with what it cost: ``fuel`` (the engine's unit of
    work, the unit ``max_fuel`` limits) and wall-clock ``time``."""

    result: Any
    fuel: float | None
    time: _dt.timedelta | None


def _controls(
    max_fuel: float | None, timeout: float | None, stats: bool, cancel: _fluree.Canceller | None = None
) -> dict[str, Any] | None:
    if not stats and max_fuel is None and timeout is None and cancel is None:
        return None
    return {
        "max_fuel": None if max_fuel is None else float(max_fuel),
        "timeout": None if timeout is None else float(timeout),
        "stats": stats,
        "cancel": cancel,
    }


def _execute(
    run: Any,
    query: Query,
    max_fuel: float | None,
    timeout: float | None,
    stats: bool,
    cancel: _fluree.Canceller | None = None,
    *,
    language: Language | None = None,
    params: dict[str, Any] | None = None,
    select: bool = False,
) -> Any:
    kind = _query_language(query, language)
    if select:
        _require_table(query, kind)
    if kind == "cypher":
        if stats or max_fuel is not None:
            raise InvalidRequestError("max_fuel and profile() are not yet supported for Cypher")
        return run(query, "cypher", _controls(None, timeout, False, cancel), _cypher_params(params))
    controls = _controls(max_fuel, timeout, stats, cancel)
    sparql = kind == "sparql"
    if sparql:
        raw = run(query, kind, controls, _sparql_params(params))
    else:
        _no_jsonld_parameters(params)
        raw = run(_json_query(query), kind, controls, None)
    result, measured = raw if stats else (raw, None)
    if sparql:
        result = _sparql_result(result)
    return (result, measured) if stats else result


def _require_table(query: Any, kind: str) -> None:
    """Refuse, before running it, a query whose result is not a table."""
    if kind == "jsonld":
        raise InvalidRequestError(
            "select() takes a SPARQL SELECT or a Cypher query; run a JSON-LD query with query()"
        )
    if kind == "sparql":
        form = _fluree.sparql_form(query)
        if form == "update":
            raise InvalidRequestError("this is a SPARQL update; run it with update()")
        if form not in (None, "select"):
            raise InvalidRequestError(
                f"select() takes a SPARQL SELECT, not {form.upper()}; run it with query()"
            )


def _profile(
    run: Any,
    query: Query,
    max_fuel: float | None,
    timeout: float | None,
    cancel: _fluree.Canceller | None = None,
    *,
    language: Language | None = None,
    params: dict[str, Any] | None = None,
) -> QueryProfile:
    result, stats = _execute(run, query, max_fuel, timeout, True, cancel, language=language, params=params)
    time = stats["time"]
    elapsed = None
    if time is not None and time.endswith("ms"):
        elapsed = _dt.timedelta(milliseconds=float(time[:-2]))
    return QueryProfile(result, stats["fuel"], elapsed)


def _explainable(
    query: Query, language: Language | None, params: dict[str, Any] | None
) -> tuple[Any, dict[str, Any] | None]:
    """A SPARQL or JSON-LD query as the native layer takes it, with its parameters."""
    if _query_language(query, language) == "sparql":
        return query, _sparql_params(params)
    _no_jsonld_parameters(params)
    return _json_query(query), None


def _streamable(query: Query, params: dict[str, Any] | None) -> tuple[Any, dict[str, Any] | None]:
    if _query_language(query, None) == "cypher":
        raise InvalidRequestError("streaming is not yet supported for Cypher; use query()")
    return _explainable(query, None, params)


def _sparql_result(result: tuple[Any, ...]) -> Any:
    kind = result[0]
    if kind == "select":
        return Result._from_cells(result[1], result[2], to_python)
    return result[1]


def _no_jsonld_parameters(params: dict[str, Any] | None) -> None:
    if params:
        raise InvalidRequestError(
            "parameters apply to SPARQL and Cypher; a JSON-LD query takes its values in the query"
        )


def _update_params(kind: str, params: dict[str, Any] | None) -> dict[str, Any] | None:
    """The parameters of a SPARQL or JSON-LD (``kind``) update."""
    if kind == "sparql":
        return _sparql_params(params)
    _no_jsonld_parameters(params)
    return None


_LANGUAGES = ("sparql", "cypher", "jsonld")
_LEADING_NOISE = re.compile(r"(?:\s+|#[^\n]*|//[^\n]*|/\*.*?\*/)*", re.S)
_SPARQL_READ = {"PREFIX", "BASE", "SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}
_SPARQL_WRITE = _SPARQL_READ | {"INSERT", "LOAD", "CLEAR", "DROP", "COPY", "MOVE", "ADD"}
_CYPHER_LEAD = {
    "MATCH", "OPTIONAL", "MERGE", "UNWIND", "CREATE", "DETACH", "DELETE", "SET", "REMOVE",
    "WITH", "RETURN", "CALL", "FOREACH", "USE", "EXPLAIN", "PROFILE",
}


def _query_language(query: Any, language: Language | None) -> str:
    if language is not None:
        if language not in _LANGUAGES:
            raise InvalidRequestError(f"language is one of {', '.join(_LANGUAGES)}, not {language!r}")
        return language
    if not isinstance(query, str) or _looks_like_json(query):
        return "jsonld"
    return _text_language(query, write=False)


def _text_language(text: str, *, write: bool) -> str:
    """``"sparql"`` or ``"cypher"``, from the statement's opening keyword.

    The keywords both languages open with are told apart by what follows:
    SPARQL ``CREATE GRAPH``, ``DELETE DATA``/``WHERE``/``{`` and
    ``WITH <iri>`` are updates; Cypher's ``CREATE (``, ``DELETE n`` and
    ``WITH n`` are not.
    """
    rest = text[_LEADING_NOISE.match(text).end() :]  # type: ignore[union-attr]
    match = re.match(r"[A-Za-z]+", rest)
    if match is None:
        return "sparql"
    word = match.group(0).upper()
    following = rest[match.end() :].lstrip()
    next_word = re.match(r"[A-Za-z]*", following).group(0).upper()  # type: ignore[union-attr]
    if word in (_SPARQL_WRITE if write else _SPARQL_READ):
        return "sparql"
    if write and (
        (word == "CREATE" and next_word in ("GRAPH", "SILENT"))
        or (word == "DELETE" and (next_word in ("DATA", "WHERE") or following.startswith("{")))
        or (word == "WITH" and following.startswith("<"))
    ):
        return "sparql"
    return "cypher" if word in _CYPHER_LEAD else "sparql"


def _commit_ref(commit: CommitRef) -> int | str:
    if isinstance(commit, Commit):
        if commit.id is None:
            raise InvalidRequestError("that transaction changed nothing, so it has no commit")
        return commit.id
    if isinstance(commit, bool) or not isinstance(commit, (int, str)):
        raise TypeError(f"a commit is a t, an id, a digest prefix, or a Commit, not {type(commit).__name__}")
    return commit


def _commit_refs(commits: CommitRef | list[CommitRef]) -> list[int | str]:
    if isinstance(commits, (int, str, Commit)):
        return [_commit_ref(commits)]
    refs = [_commit_ref(c) for c in commits]
    if not refs:
        raise InvalidRequestError("name at least one commit")
    return refs


_F = "https://ns.flur.ee/db#"
_IRI_FORBIDDEN = set('<>"{}|^`\\') | {chr(c) for c in range(0x21)}


def _iri_ref(iri: str) -> str:
    """``<iri>`` for splicing into SPARQL; rejects text that is not an IRI."""
    if not iri or any(ch in _IRI_FORBIDDEN for ch in iri):
        raise InvalidRequestError(f"not an IRI: {iri!r}")
    return f"<{iri}>"


def _cypher_text(query: Query) -> str:
    if not isinstance(query, str):
        raise TypeError(f"a Cypher query is text, not {type(query).__name__}")
    return query


def _looks_like_json(text: str) -> bool:
    return text.lstrip()[:1] in ("{", "[")


def _json_query(query: Query) -> Any:
    if isinstance(query, str):
        return json.loads(query)
    if isinstance(query, dict):
        return query
    raise TypeError(f"a query is SPARQL text or a JSON-LD dict, not {type(query).__name__}")


def _rdf_payload(data: Data, format: Format | None) -> tuple[str, Any]:
    if isinstance(data, os.PathLike):
        path = Path(data)
        if format is None:
            suffix = path.suffix.lower()
            if suffix in _JSON_SUFFIXES:
                format = "jsonld"
            elif suffix in _TURTLE_SUFFIXES:
                format = "turtle"
            else:
                raise InvalidRequestError(f"cannot tell the format of {path.name}; pass format=")
        data = path.read_text(encoding="utf-8")
    if isinstance(data, (dict, list)):
        if format not in (None, "jsonld"):
            raise InvalidRequestError(f"a dict or list is JSON-LD, not {format}")
        return "jsonld", data
    if isinstance(data, str):
        if format == "jsonld" or (format is None and _looks_like_json(data)):
            return "jsonld", json.loads(data)
        return "turtle", data
    raise TypeError(f"cannot insert {type(data).__name__}; pass JSON-LD, Turtle, or a path")


def _update_payload(
    txn: str | dict[str, Any] | os.PathLike[str], language: Language | None = None
) -> tuple[str, Any]:
    """``(kind, payload)``: kind ``"sparql"``, ``"cypher"``, or ``"jsonld"``."""
    if language is not None and language not in _LANGUAGES:
        raise InvalidRequestError(f"language is one of {', '.join(_LANGUAGES)}, not {language!r}")
    if isinstance(txn, os.PathLike):
        path = Path(txn)
        text = path.read_text(encoding="utf-8")
        suffix = path.suffix.lower()
        if language is None and suffix in _SPARQL_SUFFIXES:
            language = "sparql"
        elif language is None and suffix in _CYPHER_SUFFIXES:
            language = "cypher"
        txn = text
    if isinstance(txn, dict):
        if language not in (None, "jsonld"):
            raise InvalidRequestError(f"a dict is a JSON-LD update, not {language}")
        return "jsonld", txn
    if isinstance(txn, str):
        if language == "jsonld" or (language is None and _looks_like_json(txn)):
            return "jsonld", json.loads(txn)
        return language or _text_language(txn, write=True), txn
    raise TypeError(f"an update is SPARQL or Cypher text or a JSON-LD dict, not {type(txn).__name__}")