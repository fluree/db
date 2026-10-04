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
from collections.abc import AsyncIterator, Awaitable, Callable, Generator, Mapping
from typing import Any, Generic, TypeVar

import fluree
from fluree import _connection as _sync
from fluree import _fluree
from fluree._connection import (
    CommitRef,
    Data,
    ExportFormat,
    Format,
    MergeStrategy,
    Query,
    QueryProfile,
    RebaseStrategy,
    RevertStrategy,
)
from fluree._cypher import CypherResult
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
)

__all__ = ["Connection", "CypherTransaction", "Ledger", "RowStream", "Snapshot", "Transaction", "connect"]

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
        await _call(self._sync.close)

    async def exists(self, ledger: str) -> bool:
        """Whether ``ledger`` exists (``ledger in conn`` for the sync API)."""
        return await _call(self._sync.__contains__, ledger)

    async def create(self, ledger: str, *, source: str | os.PathLike[str] | None = None) -> Ledger:
        return Ledger(self, await _call(self._sync.create, ledger, source=source))

    async def ledger(self, ledger: str) -> Ledger:
        return Ledger(self, await _call(self._sync.ledger, ledger))

    async def ledgers(self) -> list[str]:
        return await _call(self._sync.ledgers)

    async def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        return await _query(lambda c: _sync._execute(self._sync._run, query, max_fuel, timeout, False, c))

    async def profile(
        self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None
    ) -> QueryProfile:
        return await _query(lambda c: _sync._profile(self._sync._run, query, max_fuel, timeout, c))

    async def restore(self, path: str | os.PathLike[str], ledger: str) -> Ledger:
        return Ledger(self, await _call(self._sync.restore, path, ledger))

    async def drop(self, ledger: str) -> None:
        await _call(self._sync.drop, ledger)


class Ledger:
    """A ledger; see :class:`fluree.Ledger`."""

    __slots__ = ("_connection", "_sync")

    def __init__(self, connection: Connection, sync: fluree.Ledger) -> None:
        self._connection = connection
        self._sync = sync

    @property
    def id(self) -> str:
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
        return await _call(self._sync.insert, data, format=format, message=message)

    async def upsert(self, data: Data, *, format: Format | None = None, message: str | None = None) -> Commit:
        return await _call(self._sync.upsert, data, format=format, message=message)

    async def update(
        self, transaction: str | dict[str, Any] | os.PathLike[str], *, message: str | None = None
    ) -> Commit:
        return await _call(self._sync.update, transaction, message=message)

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

    async def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        return await _query(lambda c: _sync._execute(self._sync._run, query, max_fuel, timeout, False, c))

    async def profile(
        self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None
    ) -> QueryProfile:
        return await _query(lambda c: _sync._profile(self._sync._run, query, max_fuel, timeout, c))

    async def cypher(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> CypherResult:
        """See :meth:`fluree.Ledger.cypher`."""
        return await _call(self._sync.cypher, query, parameters, timeout=timeout, **kwparameters)

    def cypher_transaction(self) -> _Opening[CypherTransaction]:
        """Open a :class:`CypherTransaction`. Await it, or use it with
        ``async with`` to commit on a clean exit and roll back on an
        exception."""

        async def open() -> CypherTransaction:
            return CypherTransaction(await _call(self._sync.cypher_transaction))

        return _Opening(open)

    def stream(
        self,
        query: Query,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
    ) -> RowStream:
        """Read a SELECT's rows as they are produced: ``async for row in
        ledger.stream(...)``. The query starts on the first read."""
        return RowStream(
            lambda: self._sync.stream(query, max_fuel=max_fuel, timeout=timeout, batch_size=batch_size)
        )

    async def explain(self, query: Query) -> dict[str, Any]:
        return await _call(self._sync.explain, query)

    async def snapshot(self) -> Snapshot:
        return Snapshot(self, await _call(self._sync.snapshot))

    async def at(
        self,
        t: int | None = None,
        *,
        time: _dt.datetime | str | None = None,
        commit: str | None = None,
    ) -> Snapshot:
        return Snapshot(self, await _call(self._sync.at, t, time=time, commit=commit))

    async def history(
        self,
        subject: str,
        predicate: str | None = None,
        *,
        from_t: int = 1,
        to_t: int | None = None,
    ) -> list[Change]:
        return await _call(self._sync.history, subject, predicate, from_t=from_t, to_t=to_t)

    async def log(self, limit: int | None = None) -> list[Commit]:
        return await _call(self._sync.log, limit)

    async def changes(self, commit: CommitRef) -> list[Change]:
        return await _call(self._sync.changes, commit)

    async def branch(self, name: str) -> Ledger:
        return self._wrap(await _call(self._sync.branch, name))

    async def branches(self) -> list[Branch]:
        return await _call(self._sync.branches)

    async def merge(self, source: str | Ledger, *, strategy: MergeStrategy = "take-both") -> MergeResult:
        return await _call(self._sync.merge, _source(source), strategy=strategy)

    async def merge_preview(self, source: str | Ledger, **options: Any) -> MergePreview:
        """See :meth:`fluree.Ledger.merge_preview` for ``options``."""
        return await _call(self._sync.merge_preview, _source(source), **options)

    async def rebase(self, *, strategy: RebaseStrategy = "take-both") -> RebaseResult:
        return await _call(self._sync.rebase, strategy=strategy)

    async def revert(
        self, commits: CommitRef | list[CommitRef], *, strategy: RevertStrategy = "abort"
    ) -> RevertResult:
        return await _call(self._sync.revert, commits, strategy=strategy)

    async def revert_preview(self, commits: CommitRef | list[CommitRef], **options: Any) -> RevertPreview:
        """See :meth:`fluree.Ledger.revert_preview` for ``options``."""
        return await _call(self._sync.revert_preview, commits, **options)

    async def validate(self, shapes: Data | None = None, **options: Any) -> ValidationReport:
        """See :meth:`fluree.Ledger.validate` for ``options``."""
        return await _call(self._sync.validate, shapes, **options)

    async def index_status(self) -> IndexStatus:
        return await _call(self._sync.index_status)

    async def index(self, *, timeout: float | None = None) -> int:
        return await _call(self._sync.index, timeout=timeout)

    async def reindex(self) -> int:
        return await _call(self._sync.reindex)

    async def verify(self, *, max_commits: int | None = None) -> VerifyReport:
        return await _call(self._sync.verify, max_commits=max_commits)

    async def sweep(self, *, dry_run: bool = False) -> SweepResult:
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
        return await _call(
            self._sync.export, path, format=format, graph=graph, all_graphs=all_graphs, context=context
        )

    async def archive(self, path: str | os.PathLike[str], *, include_indexes: bool = True) -> None:
        await _call(self._sync.archive, path, include_indexes=include_indexes)

    async def context(self) -> dict[str, Any] | None:
        """The ledger's default JSON-LD context (a property in the sync API)."""
        return await _call(lambda: self._sync.context)

    async def set_context(self, context: dict[str, Any]) -> None:
        await _call(self._sync.set_context, context)

    async def info(self) -> dict[str, Any]:
        return await _call(self._sync.info)


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
        return self._sync.ledger

    @property
    def t(self) -> int:
        return self._sync.t

    def __repr__(self) -> str:
        return repr(self._sync).replace("<Snapshot", "<aio.Snapshot", 1)

    async def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        return await _query(lambda c: _sync._execute(self._sync._run, query, max_fuel, timeout, False, c))

    async def profile(
        self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None
    ) -> QueryProfile:
        return await _query(lambda c: _sync._profile(self._sync._run, query, max_fuel, timeout, c))

    def stream(
        self,
        query: Query,
        *,
        max_fuel: float | None = None,
        timeout: float | None = None,
        batch_size: int = 1000,
    ) -> RowStream:
        return RowStream(
            lambda: self._sync.stream(query, max_fuel=max_fuel, timeout=timeout, batch_size=batch_size)
        )

    async def explain(self, query: Query) -> dict[str, Any]:
        return await _call(self._sync.explain, query)

    async def export(
        self,
        path: str | os.PathLike[str] | None = None,
        *,
        format: ExportFormat | None = None,
        graph: str | None = None,
        all_graphs: bool = False,
        context: dict[str, Any] | None = None,
    ) -> str | None:
        return await _call(
            self._sync.export, path, format=format, graph=graph, all_graphs=all_graphs, context=context
        )

    async def cypher(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
        **kwparameters: Any,
    ) -> CypherResult:
        return await _call(self._sync.cypher, query, parameters, timeout=timeout, **kwparameters)

    async def branch(self, name: str) -> Ledger:
        return self._ledger._wrap(await _call(self._sync.branch, name))


class Transaction:
    """Writes committed together as one commit; see :class:`fluree.Transaction`."""

    __slots__ = ("_sync",)

    def __init__(self, sync: fluree.Transaction) -> None:
        self._sync = sync

    @property
    def committed(self) -> Commit | None:
        return self._sync.committed

    def __repr__(self) -> str:
        return repr(self._sync).replace("<Transaction", "<aio.Transaction", 1)

    async def __aenter__(self) -> Transaction:
        return self

    async def __aexit__(self, exc_type: object, *exc: object) -> None:
        await _call(self._sync.__exit__, exc_type, *exc)

    async def insert(self, data: Data, *, format: Format | None = None) -> None:
        await _call(self._sync.insert, data, format=format)

    async def upsert(self, data: Data, *, format: Format | None = None) -> None:
        await _call(self._sync.upsert, data, format=format)

    async def update(self, transaction: str | dict[str, Any] | os.PathLike[str]) -> None:
        await _call(self._sync.update, transaction)

    async def query(self, query: Query, *, max_fuel: float | None = None, timeout: float | None = None) -> Any:
        return await _query(
            lambda c: _sync._execute(self._sync._view()._run, query, max_fuel, timeout, False, c)
        )

    async def explain(self, query: Query) -> dict[str, Any]:
        return await _call(self._sync.explain, query)

    async def commit(self, *, message: str | None = None) -> Commit:
        return await _call(self._sync.commit, message=message)

    async def rollback(self) -> None:
        await _call(self._sync.rollback)

    async def _close(self) -> None:
        if self._sync._native.is_open:
            await self.rollback()


class CypherTransaction:
    """Cypher statements committed together; see
    :class:`fluree.CypherTransaction`."""

    __slots__ = ("_sync",)

    def __init__(self, sync: fluree.CypherTransaction) -> None:
        self._sync = sync

    @property
    def committed(self) -> Commit | None:
        return self._sync.committed

    async def __aenter__(self) -> CypherTransaction:
        return self

    async def __aexit__(self, exc_type: object, *exc: object) -> None:
        await _call(self._sync.__exit__, exc_type, *exc)

    async def run(
        self, query: str, parameters: Mapping[str, Any] | None = None, **kwparameters: Any
    ) -> CypherResult:
        return await _call(self._sync.run, query, parameters, **kwparameters)

    async def commit(self) -> Commit:
        return await _call(self._sync.commit)

    async def rollback(self) -> None:
        await _call(self._sync.rollback)


_END = object()


class RowStream(AsyncIterator[tuple[Any, ...]]):
    """Rows of a SELECT read as the query produces them; see
    :class:`fluree.RowStream`. ``async with`` closes it at the end of the
    block, and so does cancelling the task reading it."""

    __slots__ = ("_start", "_stream")

    def __init__(self, start: Callable[[], fluree.RowStream]) -> None:
        self._start: Callable[[], fluree.RowStream] | None = start
        self._stream: fluree.RowStream | None = None

    @property
    def columns(self) -> list[str] | None:
        return None if self._stream is None else self._stream.columns

    def __aiter__(self) -> RowStream:
        return self

    async def __anext__(self) -> tuple[Any, ...]:
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
