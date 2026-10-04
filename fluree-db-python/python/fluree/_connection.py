"""Connections, ledgers and snapshots."""

from __future__ import annotations

import datetime as _dt
import json
import os
import random as _random
import time as _time
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Callable, Mapping
from typing import Any, Literal as _Literal, TypeVar, Union

from fluree import _fluree
from fluree._cypher import CypherResult, CypherTransaction, _params, _result
from fluree._records import (
    Branch,
    Change,
    Commit,
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
from fluree._results import Rows, RowStream
from fluree._terms import IRI
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
        """Flush pending writes and release the database."""
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

    def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        """Run a query whose ``FROM`` names the ledgers to read.

        SPARQL ``FROM <ledger>`` (and ``FROM NAMED``) or a JSON-LD ``"from"``
        picks the ledgers, so one query can span several; a ledger address can
        carry a time (``<people@t:5>``), and SPARQL ``FROM ... TO ...`` reads a
        range of history. Results, ``max_fuel`` and ``timeout`` are as for
        :meth:`Snapshot.query`.
        """
        return _execute(self._run, query, max_fuel, timeout, stats=False)

    def profile(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(self._run, query, max_fuel, timeout)

    def _run(self, query: Any, sparql: bool, controls: dict[str, Any] | None) -> Any:
        if sparql:
            return self._native.query_sparql_from(query, None, controls)
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
        self, transaction: str | dict[str, Any] | os.PathLike[str], *, message: str | None = None
    ) -> Commit:
        """Apply a SPARQL UPDATE, or a JSON-LD ``where``/``delete``/``insert``."""
        return self._transact("update", *_update_payload(transaction), message)

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
        for attempt in range(_TRANSACT_ATTEMPTS):
            txn = self.transaction()
            try:
                result = fn(txn, *args, **kwargs)
                if txn._native.is_open:
                    txn.commit()
                return result
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

    def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        """Query the latest state. See :meth:`Snapshot.query` for results,
        ``max_fuel`` and ``timeout``."""
        return _execute(self._run, query, max_fuel, timeout, stats=False)

    def profile(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(self._run, query, max_fuel, timeout)

    def cypher(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> CypherResult:
        """Run a Cypher statement, as ``session.run`` does in the Neo4j driver.

        A read returns its records; a write — or a ``;``-separated script of
        them — commits, all or nothing, and the result's ``commit`` says
        what it wrote. Parameters (``$name``) come from ``parameters`` and
        keyword arguments. ``timeout`` (seconds) bounds a read.

        Records are :class:`Record` tuples, also indexable by column name;
        nodes, relationships and paths come back as :class:`Node`,
        :class:`Relationship` and :class:`Path`. Group several statements
        into one transaction with :meth:`cypher_transaction`.
        """
        native = self._connection._native
        commit, table = native.cypher(
            self._id, query, _params(parameters, kwparameters), None, self._policy, timeout
        )
        return _result(commit, table)

    def cypher_transaction(self) -> CypherTransaction:
        """Open a :class:`CypherTransaction` for several Cypher statements
        committed together."""
        return CypherTransaction(self._connection._native.begin_cypher(self._id, self._policy))

    def stream(
        self,
        query: Query,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
    ) -> RowStream:
        """Run a SELECT and read its rows as they are produced; see
        :class:`RowStream`. ``max_fuel`` and ``timeout`` cover the whole stream."""
        controls = _controls(max_fuel, timeout, stats=False)
        native = self._connection._native.stream(self._id, _explainable(query), None, self._policy, controls)
        return RowStream(native, batch_size)

    def explain(self, query: Query) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it."""
        return self._connection._native.explain(self._id, _explainable(query), None, self._policy)

    def _run(self, query: Any, sparql: bool, controls: dict[str, Any] | None) -> Any:
        native = self._connection._native
        if sparql:
            return native.query_sparql(self._id, query, None, self._policy, controls)
        return native.query_jsonld(self._id, self._govern(query), None, controls)

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
        if timeout is not None and timeout <= 0:
            raise InvalidRequestError("timeout must be positive")
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

    def _transact(self, op: str, kind: str, payload: Any, message: str | None) -> Commit:
        native = self._connection._native
        return Commit(**native.transact(self._id, op, kind, payload, self._policy, message))

    def _govern(self, query: Any) -> Any:
        """Fold this handle's policy into a JSON-LD query's ``opts``. Options the
        query sets itself win, as they do over request headers on the server."""
        if self._policy is None or not isinstance(query, dict):
            return query
        opts = dict(query.get("opts") or {})
        for key, value in self._policy.items():
            opts.setdefault(key, value)
        return {**query, "opts": opts}


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

    def cypher(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> CypherResult:
        """Run a Cypher read against this snapshot; see :meth:`Ledger.cypher`."""
        table = self._native.cypher(query, _params(parameters, kwparameters), timeout)
        return _result(None, table)

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

    def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        """Run a SPARQL or JSON-LD query.

        SPARQL ``SELECT`` returns :class:`Rows`, ``ASK`` a ``bool``, and
        ``CONSTRUCT``/``DESCRIBE`` the constructed graph as a JSON-LD document.
        A JSON-LD query (a dict, or JSON text) returns its JSON result as
        Python objects.

        ``max_fuel`` caps the work the query may do (see :meth:`profile` for
        what a query costs); past it the query stops with
        :class:`ResourceLimitError`. ``timeout`` is in seconds; past it the
        query is cancelled with :class:`QueryTimeoutError`.
        """
        return _execute(self._run, query, max_fuel, timeout, stats=False)

    def profile(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took."""
        return _profile(self._run, query, max_fuel, timeout)

    def stream(
        self,
        query: Query,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
    ) -> RowStream:
        """Run a SELECT and read its rows as they are produced; see
        :class:`RowStream`."""
        controls = _controls(max_fuel, timeout, stats=False)
        return RowStream(self._native.stream(_explainable(query), controls), batch_size)

    def explain(self, query: Query) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it."""
        return self._native.explain(_explainable(query))

    def _run(self, query: Any, sparql: bool, controls: dict[str, Any] | None) -> Any:
        if sparql:
            return self._native.query_sparql(query, controls)
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
    - a transaction that was also *read* (``query``, ``cypher``, ``explain``)
      raises :class:`ConflictError` on :meth:`commit`, since what was read may
      have decided what was written. Run it again, or use
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

    def update(self, transaction: str | dict[str, Any] | os.PathLike[str]) -> None:
        """Stage an update; see :meth:`Ledger.update`."""
        self._native.stage("update", *_update_payload(transaction))

    def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        """Query the staged state; see :meth:`Snapshot.query`."""
        return self._view().query(query, max_fuel=max_fuel, timeout=timeout)

    def explain(self, query: Query) -> dict[str, Any]:
        """The plan ``query`` would run with over the staged state."""
        return self._view().explain(query)

    def cypher(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> CypherResult:
        """Run a Cypher read over the staged state. Cypher writes go through
        :meth:`Ledger.cypher_transaction`."""
        return self._view().cypher(query, parameters, timeout=timeout, **kwparameters)

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
    if timeout is not None and timeout <= 0:
        raise InvalidRequestError("timeout must be positive")
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
) -> Any:
    controls = _controls(max_fuel, timeout, stats, cancel)
    sparql = isinstance(query, str) and not _looks_like_json(query)
    raw = run(query if sparql else _json_query(query), sparql, controls)
    result, measured = raw if stats else (raw, None)
    if sparql:
        result = _sparql_result(result)
    return (result, measured) if stats else result


def _profile(
    run: Any,
    query: Query,
    max_fuel: float | None,
    timeout: float | None,
    cancel: _fluree.Canceller | None = None,
) -> QueryProfile:
    result, stats = _execute(run, query, max_fuel, timeout, True, cancel)
    time = stats["time"]
    elapsed = None
    if time is not None and time.endswith("ms"):
        elapsed = _dt.timedelta(milliseconds=float(time[:-2]))
    return QueryProfile(result, stats["fuel"], elapsed)


def _explainable(query: Query) -> Any:
    if isinstance(query, str) and not _looks_like_json(query):
        return query
    return _json_query(query)


def _sparql_result(result: tuple[Any, ...]) -> Any:
    kind = result[0]
    if kind == "select":
        return Rows(result[1], result[2])
    return result[1]


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


def _update_payload(txn: str | dict[str, Any] | os.PathLike[str]) -> tuple[str, Any]:
    if isinstance(txn, os.PathLike):
        path = Path(txn)
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() in _SPARQL_SUFFIXES:
            return "sparql", text
        txn = text
    if isinstance(txn, dict):
        return "jsonld", txn
    if isinstance(txn, str):
        if _looks_like_json(txn):
            return "jsonld", json.loads(txn)
        return "sparql", txn
    raise TypeError(f"an update is SPARQL text or a JSON-LD dict, not {type(txn).__name__}")
