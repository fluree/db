"""SELECT results."""

from __future__ import annotations

from collections import deque, namedtuple
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any, overload

from fluree._terms import to_python

if TYPE_CHECKING:
    import pandas

    from fluree import _fluree


def _row_type(columns: list[str]) -> Any:
    # `rename=True` turns names that are not identifiers into _0, _1, ...;
    # the original names stay available through `columns`.
    return namedtuple("Row", columns, rename=True)  # type: ignore[misc]


class Rows(Sequence[tuple[Any, ...]]):
    """The rows of a SELECT query.

    Each row is a named tuple in the query's projection order: unpack it
    (``for name, age in rows``), read a column by attribute (``row.name``), or
    get a dict with ``row._asdict()``. Unbound values are ``None``.
    """

    __slots__ = ("_columns", "_rows")

    def __init__(self, columns: list[str], cells: list[tuple[Any, ...]]) -> None:
        self._columns = columns
        row = _row_type(columns)
        self._rows = [row._make(map(to_python, cells)) for cells in cells]

    @property
    def columns(self) -> list[str]:
        """Variable names, without ``?``, in projection order."""
        return list(self._columns)

    def __len__(self) -> int:
        return len(self._rows)

    @overload
    def __getitem__(self, index: int) -> tuple[Any, ...]: ...
    @overload
    def __getitem__(self, index: slice) -> list[tuple[Any, ...]]: ...
    def __getitem__(self, index: int | slice) -> Any:
        return self._rows[index]

    def __iter__(self) -> Iterator[tuple[Any, ...]]:
        return iter(self._rows)

    def __repr__(self) -> str:
        return f"<Rows columns={self._columns} len={len(self._rows)}>"

    def to_dicts(self) -> list[dict[str, Any]]:
        """Rows as dicts keyed by variable name."""
        return [dict(zip(self._columns, row)) for row in self._rows]

    def to_pandas(self) -> pandas.DataFrame:
        """Rows as a pandas DataFrame, one column per variable."""
        import pandas

        return pandas.DataFrame.from_records(self._rows, columns=self._columns)


class RowStream(Iterator[tuple[Any, ...]]):
    """Rows of a SELECT query, read as the query produces them.

    Iterate it like :class:`Rows`; memory stays flat however many rows the
    query returns. Leaving it before the end — ``break``, :meth:`close`, or the
    end of a ``with`` block — stops the query.
    """

    __slots__ = ("_batch_size", "_buffer", "_columns", "_native", "_row")

    def __init__(self, native: _fluree.RowStream, batch_size: int) -> None:
        self._native: _fluree.RowStream | None = native
        self._batch_size = batch_size
        self._buffer: deque[tuple[Any, ...]] = deque()
        self._columns: list[str] | None = native.columns
        self._row: Any = None

    @property
    def columns(self) -> list[str] | None:
        """Variable names in projection order (for ``SELECT *``, known once
        the first row is read)."""
        return None if self._columns is None else list(self._columns)

    def __iter__(self) -> RowStream:
        return self

    def __next__(self) -> tuple[Any, ...]:
        while not self._buffer:
            if self._native is None:
                raise StopIteration
            batch = self._native.next_batch(self._batch_size)
            if self._columns is None:
                self._columns = self._native.columns
            if batch is None:
                self._native = None
                raise StopIteration
            self._buffer.extend(batch)
        if self._row is None:
            self._row = _row_type(self._columns or [])
        return self._row._make(map(to_python, self._buffer.popleft()))

    def close(self) -> None:
        """Stop the query; the stream yields nothing more."""
        if self._native is not None:
            self._native.close()
            self._native = None
        self._buffer.clear()

    def __enter__(self) -> RowStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        self.close()
