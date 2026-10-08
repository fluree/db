"""The :mod:`fluree` API for asyncio.

The same classes and methods as :mod:`fluree`, with every call that reaches
the engine a coroutine::

    import fluree.aio

    async with fluree.aio.connect("./data") as conn:
        people = await conn.create("people")
        await people.insert({...})
        rows = await people.query("SELECT ...")
        async for row in people.stream("SELECT ..."):
            ...
        async with people.transaction() as txn:
            await txn.insert({...})

Each call runs on a worker thread from the event loop's default executor while
the loop carries on. The engine releases the GIL, so calls from many tasks run
in parallel.

Cancelling a task that is waiting on a query — ``asyncio.timeout``, a client
that disconnects — stops the query in the engine too. A write cannot be
stopped part way: cancelling the task stops the wait, and the write still
completes or fails on its own.
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Iterable, Mapping
from typing import Any, Generic, TypeVar

import fluree
from fluree import _connection as _sync
from fluree import _fluree
from fluree._connection import (
    CommitRef,
    Data,
    ExportFormat,
    Format,
    Language,
    MergeStrategy,
    Query,
    QueryProfile,
    RebaseStrategy,
    RevertStrategy,
    SelectLanguage,
)
from fluree._params import _params
from fluree._rdf import RdfFormat
from fluree._terms import Quad
from fluree._results import Record, Result
from fluree._sources import MaterializeResult
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
)

__all__ = [
    "Connection",
    "GraphSource",
    "Ledger",
    "RowStream",
    "Snapshot",
    "Transaction",
    "connect",
    "parse",
    "serialize",
]

T = TypeVar("T")


async def _call(fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    return await asyncio.to_thread(fn, *args, **kwargs)


async def _query(run: Callable[[_fluree.Canceller], T]) -> T:
    """Run ``run`` on a worker thread, cancelling its query if the awaiting
    task is cancelled."""
    canceller = _fluree.Canceller()
    try:
        return await asyncio.to_thread(run, canceller)
    except asyncio.CancelledError:
        canceller.cancel()
        raise


class _Opening(Generic[T]):
    """An object that is opened by awaiting it or by ``async with``, which
    also closes it."""

    def __init__(self, open: Callable[[], Awaitable[T]]) -> None:
        self._open = open
        self._value: Any = None

    def __await__(self) -> Generator[Any, None, T]:
        return self._open().__await__()

    async def __aenter__(self) -> T:
        self._value = await self._open()
        return self._value

    async def __aexit__(self, *exc: Any) -> None:
        await self._value.__aexit__(*exc)


def connect(
    path: str | os.PathLike[str] | None = None,
    *,
    config: dict[str, Any] | None = None,
    indexing: bool = True,
) -> _Opening[Connection]:
    """Open a database; see :func:`fluree.connect`. Await it, or use it with
    ``async with`` to close the connection at the end of the block."""

    async def open() -> Connection:
        return Connection(await _call(fluree.connect, path, config=config, indexing=indexing))

    return _Opening(open)


async def parse(
    data: str | os.PathLike[str],
    format: RdfFormat | None = None,
    *,
    base: str | None = None,
) -> list[Quad]:
    """The quads of an RDF document; see :func:`fluree.parse`. The file is
    read and the document parsed on a worker thread."""
    return await _call(fluree.parse, data, format, base=base)


async def serialize(
    quads: Iterable[Quad | tuple[Any, ...]],
    format: RdfFormat,
    *,
    prefixes: dict[str, str] | None = None,
) -> str:
    """Quads written as an RDF document; see :func:`fluree.serialize`. The
    document is written on a worker thread."""
    return await _call(fluree.serialize, quads, format, prefixes=prefixes)


class Connection:
    """A connection to a Fluree database; see :class:`fluree.Connection`."""

    __slots__ = ("_sync",)

    def __init__(self, sync: fluree.Connection) -> None:
        self._sync = sync

    async def __aenter__(self) -> Connection:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Flush pending writes and release the database; see :meth:`fluree.Connection.close`."""
        await _call(self._sync.close)

    async def exists(self, ledger: str) -> bool:
        """Whether ``ledger`` exists (``ledger in conn`` for the sync API)."""
        return await _call(self._sync.__contains__, ledger)

    async def create(self, ledger: str, *, source: str | os.PathLike[str] | None = None) -> Ledger:
        """Create a ledger, optionally bulk-loading RDF files into it; see
        :meth:`fluree.Connection.create`."""
        return Ledger(self, await _call(self._sync.create, ledger, source=source))

    async def ledger(self, ledger: str) -> Ledger:
        """An existing ledger; see :meth:`fluree.Connection.ledger`."""
        return Ledger(self, await _call(self._sync.ledger, ledger))

    async def ledgers(self) -> list[str]:
        """The ids of every ledger, as ``name:branch``; see :meth:`fluree.Connection.ledgers`."""
        return await _call(self._sync.ledgers)

    async def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a query whose ``FROM`` names the ledgers to read; see
        :meth:`fluree.Connection.query`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c, language=language, params=params
            )
        )

    async def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """A SPARQL ``SELECT`` whose ``FROM`` names the ledgers, as for :meth:`query`, returning
        its :class:`Result`; see :meth:`fluree.Connection.select`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c,
                language=language, params=params, select=True,
            )
        )

    async def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took; see
        :meth:`fluree.Connection.profile`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._profile(
                self._sync._run, query, max_fuel, timeout, c, language=language, params=params
            )
        )

    async def map_iceberg(self, name: str, mapping: str | os.PathLike[str], **options: Any) -> GraphSource:
        """Register Iceberg tables; see :meth:`fluree.Connection.map_iceberg`."""
        return GraphSource(await _call(self._sync.map_iceberg, name, mapping, **options))

    async def map_delta(self, name: str, mapping: str | os.PathLike[str], **options: Any) -> GraphSource:
        """Register Delta tables; see :meth:`fluree.Connection.map_delta`."""
        return GraphSource(await _call(self._sync.map_delta, name, mapping, **options))

    async def map_sql(
        self, name: str, endpoint: str, mapping: str | os.PathLike[str], **options: Any
    ) -> GraphSource:
        """Register SQL tables; see :meth:`fluree.Connection.map_sql`."""
        return GraphSource(await _call(self._sync.map_sql, name, endpoint, mapping, **options))

    async def graph_sources(self) -> list[GraphSource]:
        """Every graph source, by id; see :meth:`fluree.Connection.graph_sources`."""
        return [GraphSource(s) for s in await _call(self._sync.graph_sources)]

    async def graph_source(self, name: str) -> GraphSource:
        """Graph source ``name`` (``"name"`` or ``"name:branch"``); see
        :meth:`fluree.Connection.graph_source`."""
        return GraphSource(await _call(self._sync.graph_source, name))

    async def restore(self, path: str | os.PathLike[str], ledger: str) -> Ledger:
        """Create ``ledger`` from a ``.flpack`` archive; see :meth:`fluree.Connection.restore`."""
        return Ledger(self, await _call(self._sync.restore, path, ledger))

    async def drop(self, ledger: str) -> None:
        """Delete a ledger, every branch and all history (``"people"``), or one branch
        (``"people:dev"``); see :meth:`fluree.Connection.drop`."""
        await _call(self._sync.drop, ledger)


class Ledger:
    """A ledger; see :class:`fluree.Ledger`."""

    __slots__ = ("_connection", "_sync")

    def __init__(self, connection: Connection, sync: fluree.Ledger) -> None:
        self._connection = connection
        self._sync = sync

    @property
    def id(self) -> str:
        """The ledger's id, ``name:branch``."""
        return self._sync.id

    def __repr__(self) -> str:
        return repr(self._sync).replace("<Ledger", "<aio.Ledger", 1)

    def _wrap(self, sync: fluree.Ledger) -> Ledger:
        return Ledger(self._connection, sync)

    def with_policy(
        self,
        *,
        identity: str | None = None,
        policy_class: str | list[str] | None = None,
        policy: dict[str, Any] | list[Any] | None = None,
        values: dict[str, Any] | None = None,
        default_allow: bool | None = None,
    ) -> Ledger:
        """This ledger, governed; see :meth:`fluree.Ledger.with_policy`."""
        return self._wrap(
            self._sync.with_policy(
                identity=identity,
                policy_class=policy_class,
                policy=policy,
                values=values,
                default_allow=default_allow,
            )
        )

    async def insert(self, data: Data, *, format: Format | None = None, message: str | None = None) -> Commit:
        """Add data; see :meth:`fluree.Ledger.insert`."""
        return await _call(self._sync.insert, data, format=format, message=message)

    async def upsert(self, data: Data, *, format: Format | None = None, message: str | None = None) -> Commit:
        """Add data, replacing existing values of the properties it sets; see
        :meth:`fluree.Ledger.upsert`."""
        return await _call(self._sync.upsert, data, format=format, message=message)

    async def insert_rows(self, rows: Any, **options: Any) -> Commit:
        """See :meth:`fluree.Ledger.insert_rows`."""
        return await _call(self._sync.insert_rows, rows, **options)

    async def upsert_rows(self, rows: Any, **options: Any) -> Commit:
        """See :meth:`fluree.Ledger.upsert_rows`."""
        return await _call(self._sync.upsert_rows, rows, **options)

    async def update(
        self,
        transaction: str | dict[str, Any] | os.PathLike[str],
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        message: str | None = None,
        **kwparameters: Any,
    ) -> Commit:
        """Apply a SPARQL UPDATE, a Cypher write, or a JSON-LD ``where``/``delete``/``insert``;
        see :meth:`fluree.Ledger.update`."""
        return await _call(
            self._sync.update, transaction, parameters, language=language, message=message, **kwparameters
        )

    async def sync(
        self,
        data: Data,
        *,
        graph: str | None = None,
        format: Format | None = None,
        allow_empty: bool = False,
        dry_run: bool = False,
        message: str | None = None,
    ) -> Commit:
        """Make a graph hold exactly ``data``, committing only the difference; see
        :meth:`fluree.Ledger.sync`."""
        return await _call(
            self._sync.sync,
            data,
            graph=graph,
            format=format,
            allow_empty=allow_empty,
            dry_run=dry_run,
            message=message,
        )

    async def transact(self, fn: Callable[..., Awaitable[T]], /, *args: Any, **kwargs: Any) -> T:
        """Run ``await fn(txn, *args, **kwargs)`` in a :class:`Transaction`
        and commit it, running ``fn`` again if another commit lands first;
        see :meth:`fluree.Ledger.transact`."""
        for attempt in range(_sync._TRANSACT_ATTEMPTS):
            txn = await self.transaction()
            try:
                result = await fn(txn, *args, **kwargs)
                if txn._sync._native.is_open:
                    await txn.commit()
                return result
            except fluree.ConflictError:
                if txn._sync._native.is_open or attempt + 1 == _sync._TRANSACT_ATTEMPTS:
                    await txn._close()
                    raise
            except BaseException:
                await txn._close()
                raise
            await asyncio.sleep(_sync._backoff(attempt))
        raise AssertionError("unreachable")

    def transaction(self, *, message: str | None = None) -> _Opening[Transaction]:
        """Open a :class:`Transaction`. Await it, or use it with ``async with``
        to commit on a clean exit and roll back on an exception."""

        async def open() -> Transaction:
            return Transaction(await _call(self._sync.transaction, message=message))

        return _Opening(open)

    async def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Query the latest state; see :meth:`fluree.Ledger.query`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c, language=language, params=params
            )
        )

    async def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a ``SELECT`` on the latest state; see :meth:`fluree.Ledger.select`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c,
                language=language, params=params, select=True,
            )
        )

    async def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took; see
        :meth:`fluree.Ledger.profile`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._profile(
                self._sync._run, query, max_fuel, timeout, c, language=language, params=params
            )
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
        """Read a SELECT's rows as they are produced: ``async for row in
        ledger.stream(...)``. The query starts on the first read."""
        return RowStream(
            lambda: self._sync.stream(
                query, parameters, max_fuel=max_fuel, timeout=timeout, batch_size=batch_size, **kwparameters
            )
        )

    async def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it; see
        :meth:`fluree.Ledger.explain`."""
        return await _call(self._sync.explain, query, parameters, language=language, **kwparameters)

    async def snapshot(self) -> Snapshot:
        """The latest state, frozen; see :meth:`fluree.Ledger.snapshot`."""
        return Snapshot(self, await _call(self._sync.snapshot))

    async def at(
        self,
        t: int | None = None,
        *,
        time: _dt.datetime | str | None = None,
        commit: str | None = None,
    ) -> Snapshot:
        """The ledger as it was at transaction ``t``, a wall-clock ``time``, or a ``commit``;
        see :meth:`fluree.Ledger.at`."""
        return Snapshot(self, await _call(self._sync.at, t, time=time, commit=commit))

    async def history(
        self,
        subject: str,
        predicate: str | None = None,
        *,
        from_t: int = 1,
        to_t: int | None = None,
    ) -> list[Change]:
        """Every change to ``subject``, oldest first; see :meth:`fluree.Ledger.history`."""
        return await _call(self._sync.history, subject, predicate, from_t=from_t, to_t=to_t)

    async def log(self, limit: int | None = None) -> list[Commit]:
        """The ledger's commits, newest first; see :meth:`fluree.Ledger.log`."""
        return await _call(self._sync.log, limit)

    async def changes(self, commit: CommitRef) -> list[Change]:
        """The facts one commit asserted and retracted; see :meth:`fluree.Ledger.changes`."""
        return await _call(self._sync.changes, commit)

    async def branch(self, name: str) -> Ledger:
        """Create branch ``name`` from this ledger's latest state and return it; see
        :meth:`fluree.Ledger.branch`."""
        return self._wrap(await _call(self._sync.branch, name))

    async def branches(self) -> list[Branch]:
        """Every branch of this ledger, by name; see :meth:`fluree.Ledger.branches`."""
        return await _call(self._sync.branches)

    async def merge(self, source: str | Ledger, *, strategy: MergeStrategy = "take-both") -> MergeResult:
        """Bring the commits of branch ``source`` (a name like ``"dev"``, or its
        :class:`Ledger`) into this branch; see :meth:`fluree.Ledger.merge`."""
        return await _call(self._sync.merge, _source(source), strategy=strategy)

    async def merge_preview(self, source: str | Ledger, **options: Any) -> MergePreview:
        """See :meth:`fluree.Ledger.merge_preview` for ``options``."""
        return await _call(self._sync.merge_preview, _source(source), **options)

    async def rebase(self, *, strategy: RebaseStrategy = "take-both") -> RebaseResult:
        """Replay this branch's own commits on top of the latest commit of the branch it was
        created from, as if it had been created from there; see :meth:`fluree.Ledger.rebase`."""
        return await _call(self._sync.rebase, strategy=strategy)

    async def revert(
        self, commits: CommitRef | list[CommitRef], *, strategy: RevertStrategy = "abort"
    ) -> RevertResult:
        """Undo one commit or several, in one new commit; see :meth:`fluree.Ledger.revert`."""
        return await _call(self._sync.revert, commits, strategy=strategy)

    async def revert_preview(self, commits: CommitRef | list[CommitRef], **options: Any) -> RevertPreview:
        """See :meth:`fluree.Ledger.revert_preview` for ``options``."""
        return await _call(self._sync.revert_preview, commits, **options)

    async def validate(self, shapes: Data | None = None, **options: Any) -> ValidationReport:
        """See :meth:`fluree.Ledger.validate` for ``options``."""
        return await _call(self._sync.validate, shapes, **options)

    async def index_status(self) -> IndexStatus:
        """How far indexing has caught up with the ledger's commits; see
        :meth:`fluree.Ledger.index_status`."""
        return await _call(self._sync.index_status)

    async def index(self, *, timeout: float | None = None) -> int:
        """Index everything committed so far, waiting up to ``timeout`` seconds; see
        :meth:`fluree.Ledger.index`."""
        return await _call(self._sync.index, timeout=timeout)

    async def reindex(self) -> int:
        """Rebuild the index from scratch from the commit history; see
        :meth:`fluree.Ledger.reindex`."""
        return await _call(self._sync.reindex)

    async def verify(self, *, max_commits: int | None = None) -> VerifyReport:
        """Check that every commit, back to the first (or the last ``max_commits``), and the
        index root are present and readable; see :meth:`fluree.Ledger.verify`."""
        return await _call(self._sync.verify, max_commits=max_commits)

    async def sweep(self, *, dry_run: bool = False) -> SweepResult:
        """Delete index files that no index of any branch of this ledger references any more —
        left behind as indexing replaces old index files; see :meth:`fluree.Ledger.sweep`."""
        return await _call(self._sync.sweep, dry_run=dry_run)

    async def export(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        format: ExportFormat | None = None,
        graph: str | None = None,
        all_graphs: bool = False,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        """Export the ledger's current data as RDF; see :meth:`fluree.Ledger.export`."""
        return await _call(
            self._sync.export, path, format=format, graph=graph, all_graphs=all_graphs, context=context
        )

    async def archive(self, path: str | os.PathLike[str], *, include_indexes: bool = True) -> None:
        """Write the whole ledger, every commit, to a ``.flpack`` archive that
        :meth:`Connection.restore` loads back; see :meth:`fluree.Ledger.archive`."""
        await _call(self._sync.archive, path, include_indexes=include_indexes)

    async def context(self) -> dict[str, Any] | None:
        """The ledger's default JSON-LD context (a property in the sync API)."""
        return await _call(lambda: self._sync.context)

    async def set_context(self, context: dict[str, Any]) -> None:
        """Replace the ledger's default JSON-LD context; see :meth:`fluree.Ledger.set_context`."""
        await _call(self._sync.set_context, context)

    async def set_full_text(
        self, properties: Iterable[str], *, language: str = "en", reindex: bool = True
    ) -> None:
        """Make the plain-string values of ``properties`` searchable with ``fulltext()``; see
        :meth:`fluree.Ledger.set_full_text`."""
        await _call(self._sync.set_full_text, list(properties), language=language, reindex=reindex)

    async def full_text(self) -> FullText | None:
        """The ledger's full-text configuration, or ``None`` without one; see
        :meth:`fluree.Ledger.full_text`."""
        return await _call(self._sync.full_text)

    async def info(self) -> dict[str, Any]:
        """Ledger metadata and statistics; see :meth:`fluree.Ledger.info`."""
        return await _call(self._sync.info)

    async def graphs(self) -> list[fluree.IRI]:
        """The IRIs of the ledger's named graphs, in the order they were first written to; see
        :meth:`fluree.Ledger.graphs`."""
        return await _call(self._sync.graphs)

    async def drop_graph(self, graph: str) -> Commit:
        """Retract everything in named graph ``graph`` (its full IRI) in one commit; see
        :meth:`fluree.Ledger.drop_graph`."""
        return await _call(self._sync.drop_graph, graph)


def _source(source: str | Ledger) -> str | fluree.Ledger:
    return source._sync if isinstance(source, Ledger) else source


class Snapshot:
    """A ledger frozen at one point in time; see :class:`fluree.Snapshot`."""

    __slots__ = ("_ledger", "_sync")

    def __init__(self, ledger: Ledger, sync: fluree.Snapshot) -> None:
        self._ledger = ledger
        self._sync = sync

    @property
    def ledger(self) -> str:
        """The id of the ledger this is a snapshot of, ``name:branch``."""
        return self._sync.ledger

    @property
    def t(self) -> int:
        """The transaction this snapshot reflects; see :attr:`fluree.Snapshot.t`."""
        return self._sync.t

    def __repr__(self) -> str:
        return repr(self._sync).replace("<Snapshot", "<aio.Snapshot", 1)

    async def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a SPARQL, Cypher, or JSON-LD query; see :meth:`fluree.Snapshot.query`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c, language=language, params=params
            )
        )

    async def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a SPARQL ``SELECT`` or a Cypher query and return its :class:`Result` —
        :meth:`query` for the queries whose result is a table, typed as one; see
        :meth:`fluree.Snapshot.select`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c,
                language=language, params=params, select=True,
            )
        )

    async def profile(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> QueryProfile:
        """Run :meth:`query` and report the fuel and time it took; see
        :meth:`fluree.Snapshot.profile`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._profile(
                self._sync._run, query, max_fuel, timeout, c, language=language, params=params
            )
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
        :meth:`fluree.Snapshot.stream`."""
        return RowStream(
            lambda: self._sync.stream(
                query, parameters, max_fuel=max_fuel, timeout=timeout, batch_size=batch_size, **kwparameters
            )
        )

    async def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan the engine would run ``query`` with, without running it; see
        :meth:`fluree.Snapshot.explain`."""
        return await _call(self._sync.explain, query, parameters, language=language, **kwparameters)

    async def export(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        format: ExportFormat | None = None,
        graph: str | None = None,
        all_graphs: bool = False,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        """Export the data as of this snapshot; see :meth:`fluree.Snapshot.export`."""
        return await _call(
            self._sync.export, path, format=format, graph=graph, all_graphs=all_graphs, context=context
        )

    async def branch(self, name: str) -> Ledger:
        """Create branch ``name`` from this past state; see :meth:`fluree.Snapshot.branch`."""
        return self._ledger._wrap(await _call(self._sync.branch, name))


class Transaction:
    """Writes committed together as one commit; see :class:`fluree.Transaction`.

    A write cannot be stopped part way, so cancelling a task that is waiting
    on one leaves it running on its worker thread. Leaving ``async with``
    (and :meth:`Ledger.transact` giving up) waits for that work to finish
    before rolling back, and the cancellation then propagates.
    """

    __slots__ = ("_pending", "_sync")

    def __init__(self, sync: fluree.Transaction) -> None:
        self._sync = sync
        self._pending: set[asyncio.Future[Any]] = set()

    @property
    def committed(self) -> Commit | None:
        """The :class:`Commit` once the transaction has committed, else ``None``."""
        return self._sync.committed

    def __repr__(self) -> str:
        return repr(self._sync).replace("<Transaction", "<aio.Transaction", 1)

    async def __aenter__(self) -> Transaction:
        return self

    async def __aexit__(self, exc_type: object, *exc: object) -> None:
        await asyncio.shield(self._finish(exc_type, *exc))

    async def insert(self, data: Data, *, format: Format | None = None) -> None:
        """Stage an insert; see :meth:`fluree.Transaction.insert`."""
        await self._call(self._sync.insert, data, format=format)

    async def upsert(self, data: Data, *, format: Format | None = None) -> None:
        """Stage an upsert; see :meth:`fluree.Transaction.upsert`."""
        await self._call(self._sync.upsert, data, format=format)

    async def insert_rows(self, rows: Any, **options: Any) -> None:
        """Stage :meth:`Ledger.insert_rows`; see :meth:`fluree.Transaction.insert_rows`."""
        await self._call(self._sync.insert_rows, rows, **options)

    async def upsert_rows(self, rows: Any, **options: Any) -> None:
        """Stage :meth:`Ledger.upsert_rows`; see :meth:`fluree.Transaction.upsert_rows`."""
        await self._call(self._sync.upsert_rows, rows, **options)

    async def update(
        self,
        transaction: str | dict[str, Any] | os.PathLike[str],
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> Result | None:
        """Stage an update — SPARQL UPDATE, a Cypher write, or JSON-LD; see
        :meth:`fluree.Transaction.update`."""
        return await self._call(self._sync.update, transaction, parameters, language=language, **kwparameters)

    async def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Query the staged state; see :meth:`fluree.Transaction.query`."""
        return await self._query(query, parameters, language, max_fuel, timeout, kwparameters, False)

    async def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a ``SELECT`` on the staged state; see :meth:`fluree.Transaction.select`."""
        return await self._query(query, parameters, language, max_fuel, timeout, kwparameters, True)

    async def _query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None,
        language: Language | None,
        max_fuel: float | None,
        timeout: float | None,
        kwparameters: dict[str, Any],
        select: bool,
    ) -> Any:
        params = _params(parameters, kwparameters)
        canceller = _fluree.Canceller()
        try:
            return await self._call(
                lambda: _sync._execute(
                    self._sync._view()._run, query, max_fuel, timeout, False, canceller,
                    language=language, params=params, select=select,
                )
            )
        except asyncio.CancelledError:
            canceller.cancel()
            raise

    async def explain(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        **kwparameters: Any,
    ) -> dict[str, Any]:
        """The plan ``query`` would run with over the staged state; see
        :meth:`fluree.Transaction.explain`."""
        return await self._call(self._sync.explain, query, parameters, language=language, **kwparameters)

    async def commit(self, *, message: str | None = None) -> Commit:
        """Commit the staged writes as one commit, recording ``message`` (by default the one the
        transaction was opened with); see :meth:`fluree.Transaction.commit`."""
        return await self._call(self._sync.commit, message=message)

    async def rollback(self) -> None:
        """Discard the staged writes and close the transaction; see
        :meth:`fluree.Transaction.rollback`."""
        await self._call(self._sync.rollback)

    async def _call(self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        """``fn`` on a worker thread, which a cancelled caller leaves running
        and :meth:`_settle` waits for."""
        work = asyncio.ensure_future(asyncio.to_thread(fn, *args, **kwargs))
        self._pending.add(work)
        work.add_done_callback(self._pending.discard)
        return await asyncio.shield(work)

    async def _settle(self) -> None:
        while self._pending:
            await asyncio.gather(*self._pending, return_exceptions=True)

    async def _finish(self, exc_type: object, *exc: object) -> None:
        await self._settle()
        await asyncio.to_thread(self._sync.__exit__, exc_type, *exc)

    async def _close(self) -> None:
        await asyncio.shield(self._finish(asyncio.CancelledError))


class GraphSource:
    """A graph source; see :class:`fluree.GraphSource`."""

    __slots__ = ("_sync",)

    def __init__(self, sync: fluree.GraphSource) -> None:
        self._sync = sync

    @property
    def id(self) -> str:
        """The source's id, ``name:branch``."""
        return self._sync.id

    @property
    def name(self) -> str:
        """The source's name."""
        return self._sync.name

    @property
    def branch(self) -> str:
        """The source's branch."""
        return self._sync.branch

    @property
    def kind(self) -> str:
        """The kind of source, such as ``"iceberg"``, ``"delta"`` or ``"sql"``."""
        return self._sync.kind

    def __repr__(self) -> str:
        return repr(self._sync).replace("<GraphSource", "<aio.GraphSource", 1)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, GraphSource) and other.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)

    async def query(
        self,
        query: Query,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: Language | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Any:
        """Run a SPARQL or JSON-LD query of this source; see :meth:`fluree.GraphSource.query`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c, language=language, params=params
            )
        )

    async def select(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        language: SelectLanguage | None = None,
        max_fuel: float | None = None,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> Result:
        """Run a SPARQL ``SELECT`` of this source; see :meth:`fluree.GraphSource.select`."""
        params = _params(parameters, kwparameters)
        return await _query(
            lambda c: _sync._execute(
                self._sync._run, query, max_fuel, timeout, False, c,
                language=language, params=params, select=True,
            )
        )

    async def materialize(self, into: str, *, full: bool = False) -> MaterializeResult:
        """Copy this source's rows into ledger ``into`` (created if missing) as ordinary ledger
        data, so they gain history, policy and indexes; see
        :meth:`fluree.GraphSource.materialize`."""
        return await _call(self._sync.materialize, into, full=full)

    async def drop(self) -> None:
        """Remove this graph source; see :meth:`fluree.GraphSource.drop`."""
        await _call(self._sync.drop)


_END = object()


class RowStream(AsyncIterator[Record]):
    """Rows of a SELECT read as the query produces them; see
    :class:`fluree.RowStream`. ``async with`` closes it at the end of the
    block, and so does cancelling the task reading it."""

    __slots__ = ("_start", "_stream")

    def __init__(self, start: Callable[[], fluree.RowStream]) -> None:
        self._start: Callable[[], fluree.RowStream] | None = start
        self._stream: fluree.RowStream | None = None

    @property
    def columns(self) -> list[str] | None:
        """The same as :meth:`keys`; see :attr:`fluree.RowStream.columns`."""
        return None if self._stream is None else self._stream.columns

    def __aiter__(self) -> RowStream:
        return self

    async def __anext__(self) -> Record:
        stream = await self._open()
        if stream is None:
            raise StopAsyncIteration
        try:
            while not stream._buffer:
                if not await _call(stream._fill):
                    raise StopAsyncIteration
        except asyncio.CancelledError:
            stream.close()
            raise
        return stream._take()

    async def _open(self) -> fluree.RowStream | None:
        if self._stream is None and self._start is not None:
            start, self._start = self._start, None
            self._stream = await _call(start)
        return self._stream

    async def aclose(self) -> None:
        """Stop the query; the stream yields nothing more."""
        self._start = None
        if self._stream is not None:
            self._stream.close()

    async def __aenter__(self) -> RowStream:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
