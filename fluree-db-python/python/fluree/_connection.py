"""Connections, ledgers and snapshots."""

from __future__ import annotations

import datetime as _dt
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal as _Literal, Union

from fluree import _fluree
from fluree._results import Rows, RowStream
from fluree._terms import IRI, BlankNode, _datetime, to_python
from fluree.errors import InvalidRequestError, PermissionDeniedError

MEMORY = ":memory:"

Query = Union[str, dict[str, Any]]
Data = Union[str, dict[str, Any], list[Any], "os.PathLike[str]"]
Format = _Literal["jsonld", "turtle", "trig"]
ExportFormat = _Literal["turtle", "trig", "ntriples", "nquads", "jsonld"]

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


@dataclass(frozen=True, slots=True)
class Commit:
    """The outcome of a transaction.

    ``id`` is the commit's content id (a CID). ``digest`` is its hex hash, the
    form ``fluree log`` shows: like a git SHA, a unique prefix of it
    (``short_id``, say) identifies the commit. A prefix of ``id`` does not,
    since every CID starts with the same header characters.

    ``id`` and ``digest`` are ``None`` when the transaction changed nothing, in
    which case no commit was written and ``t`` is the ledger's unchanged ``t``.

    ``time`` and ``message`` are filled in for commits read from
    :meth:`Ledger.log`.
    """

    t: int
    id: str | None
    digest: str | None
    asserts: int
    retracts: int
    time: _dt.datetime | None = None
    message: str | None = None

    @property
    def short_id(self) -> str | None:
        """The first 12 hex digits of ``digest``, as ``fluree log`` prints them."""
        return None if self.digest is None else self.digest[:12]


@dataclass(frozen=True, slots=True)
class Change:
    """One fact asserted or retracted at transaction ``t``: ``subject``'s
    ``predicate`` gained (``op == "assert"``) or lost (``op == "retract"``)
    ``value``, in the default graph or the named ``graph``."""

    t: int
    op: _Literal["assert", "retract"]
    subject: IRI | BlankNode
    predicate: IRI
    value: Any
    graph: IRI | None = None


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
        """Delete a ledger, every branch and all history. Takes the ledger name
        without a branch (``"people"``, not ``"people:main"``)."""
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

    def insert(self, data: Data, *, format: Format | None = None) -> Commit:
        """Add data: JSON-LD (a dict, list, or JSON text), Turtle, or TriG.

        A path is read as a file, its format taken from the extension unless
        ``format`` is given.
        """
        return self._transact("insert", *_rdf_payload(data, format))

    def upsert(self, data: Data, *, format: Format | None = None) -> Commit:
        """Add data, replacing existing values of the properties it sets."""
        return self._transact("upsert", *_rdf_payload(data, format))

    def update(self, transaction: str | dict[str, Any] | os.PathLike[str]) -> Commit:
        """Apply a SPARQL UPDATE, or a JSON-LD ``where``/``delete``/``insert``."""
        return self._transact("update", *_update_payload(transaction))

    def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        """Query the latest state. See :meth:`Snapshot.query` for results,
        ``max_fuel`` and ``timeout``."""
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
        return [
            Commit(**{**c, "time": None if c["time"] is None else _datetime(c["time"])})
            for c in commits
        ]

    def changes(self, commit: int | str) -> list[Change]:
        """The facts one commit asserted and retracted.

        ``commit`` is its ``t``, its id, or a prefix of its hex digest
        (:attr:`Commit.short_id`). Under :meth:`with_policy`, facts the policy
        hides are left out.
        """
        detail = self._connection._native.commit_detail(self._id, commit, self._policy)
        return [
            Change(
                t=detail["t"],
                op="assert" if op else "retract",
                subject=_node(s),
                predicate=IRI(p),
                value=_node(o[1]) if o[0] == "iri" else to_python(o),
                graph=None if g is None else IRI(g),
            )
            for s, p, o, op, g in detail["flakes"]
        ]

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
        # Export, archive, and set_context bypass policy enforcement entirely.
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

    def _transact(self, op: str, kind: str, payload: Any) -> Commit:
        commit = self._connection._native.transact(self._id, op, kind, payload, self._policy)
        return Commit(**commit)

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


@dataclass(frozen=True, slots=True)
class QueryProfile:
    """A query's result with what it cost: ``fuel`` (the engine's unit of
    work, the unit ``max_fuel`` limits) and wall-clock ``time``."""

    result: Any
    fuel: float | None
    time: _dt.timedelta | None


def _controls(max_fuel: float | None, timeout: float | None, stats: bool) -> dict[str, Any] | None:
    if timeout is not None and timeout <= 0:
        raise InvalidRequestError("timeout must be positive")
    if not stats and max_fuel is None and timeout is None:
        return None
    return {
        "max_fuel": None if max_fuel is None else float(max_fuel),
        "timeout": None if timeout is None else float(timeout),
        "stats": stats,
    }


def _execute(run: Any, query: Query, max_fuel: float | None, timeout: float | None, stats: bool) -> Any:
    controls = _controls(max_fuel, timeout, stats)
    sparql = isinstance(query, str) and not _looks_like_json(query)
    raw = run(query if sparql else _json_query(query), sparql, controls)
    result, measured = raw if stats else (raw, None)
    if sparql:
        result = _sparql_result(result)
    return (result, measured) if stats else result


def _profile(run: Any, query: Query, max_fuel: float | None, timeout: float | None) -> QueryProfile:
    result, stats = _execute(run, query, max_fuel, timeout, stats=True)
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


_F = "https://ns.flur.ee/db#"
_IRI_FORBIDDEN = set('<>"{}|^`\\') | {chr(c) for c in range(0x21)}


def _node(iri: str) -> IRI | BlankNode:
    return BlankNode(iri[2:]) if iri.startswith("_:") else IRI(iri)


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
