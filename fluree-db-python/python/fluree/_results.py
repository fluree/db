"""Query results: the records of a SPARQL SELECT or a Cypher statement,
eagerly as a :class:`Result` or streamed as a :class:`RowStream`."""

from __future__ import annotations

import warnings
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, overload

from fluree._graph import _plain
from fluree._terms import to_python
from fluree.errors import InvalidRequestError

if TYPE_CHECKING:
    import pandas

    from fluree import _fluree
    from fluree._records import Commit


class Record(tuple):  # type: ignore[type-arg]
    """One result row: a tuple in the query's column order that can also be
    read by column name — ``record["name"]`` or ``record.name`` — with
    ``get``, ``keys``, ``values``, ``items`` and ``data``.

    Attribute access gives way to the methods: a column named ``data`` or
    ``count`` is read as ``record["data"]``. Unbound values are ``None``.
    """

    __slots__ = ()
    _keys: tuple[str, ...] = ()

    @overload
    def __getitem__(self, key: int | str) -> Any: ...
    @overload
    def __getitem__(self, key: slice) -> tuple[Any, ...]: ...
    def __getitem__(self, key: int | str | slice) -> Any:
        if isinstance(key, str):
            return super().__getitem__(self.index(key))
        return super().__getitem__(key)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_") or name not in self._keys:
            raise AttributeError(name)
        return self[name]

    def get(self, key: str, default: Any = None) -> Any:
        return self[key] if key in self._keys else default

    def index(self, key: str | int) -> int:  # type: ignore[override]
        """The position of column ``key``."""
        if isinstance(key, int):
            if not -len(self) <= key < len(self):
                raise IndexError(key)
            return key % len(self)
        try:
            return self._keys.index(key)
        except ValueError:
            raise KeyError(key) from None

    def keys(self) -> list[str]:
        return list(self._keys)

    def values(self, *keys: str | int) -> list[Any]:
        return [self[k] for k in keys] if keys else list(self)

    def items(self, *keys: str | int) -> list[tuple[str, Any]]:
        positions = [self.index(k) for k in keys] if keys else range(len(self))
        return [(self._keys[i], super(Record, self).__getitem__(i)) for i in positions]

    def data(self, *keys: str | int) -> dict[str, Any]:
        """The record as a dict of plain values: nodes become their property
        dicts, relationships ``(start properties, type, end properties)``."""
        return {k: _plain(v) for k, v in self.items(*keys)}

    def __repr__(self) -> str:
        fields = " ".join(f"{k}={v!r}" for k, v in zip(self._keys, self))
        return f"<Record {fields}>"


def _record_type(keys: Iterable[str]) -> type[Record]:
    """A :class:`Record` subclass for one result's columns, so each record
    is a bare tuple."""
    return type("Record", (Record,), {"__slots__": (), "_keys": tuple(keys)})


class Result(Sequence[Record]):
    """The records a SPARQL ``SELECT`` or a Cypher statement returned, in
    order. Iterate it, index it, or read it as a whole: ``single()`` for
    one record, ``value()`` for one column, ``data()`` for dicts,
    ``to_pandas()`` for a DataFrame."""

    __slots__ = ("_keys", "_records")

    def __init__(self, keys: list[str], records: list[Record]) -> None:
        self._keys = keys
        self._records = records

    @classmethod
    def _from_cells(
        cls, keys: list[str], rows: Iterable[Iterable[Any]], decode: Callable[[Any], Any]
    ) -> Result:
        record = _record_type(keys)
        return cls(list(keys), [record(map(decode, row)) for row in rows])

    def keys(self) -> list[str]:
        """Column names in projection order (SPARQL variables without ``?``)."""
        return list(self._keys)

    @property
    def columns(self) -> list[str]:
        """The same as :meth:`keys`."""
        return list(self._keys)

    def __len__(self) -> int:
        return len(self._records)

    @overload
    def __getitem__(self, index: int) -> Record: ...
    @overload
    def __getitem__(self, index: slice) -> list[Record]: ...
    def __getitem__(self, index: int | slice) -> Record | list[Record]:
        return self._records[index]

    def __iter__(self) -> Iterator[Record]:
        return iter(self._records)

    def __repr__(self) -> str:
        return f"<Result keys={self._keys!r} len={len(self)}>"

    def single(self, strict: bool = False) -> Record | None:
        """The one record. With none, ``None``; with more than one, the
        first, with a warning. ``strict`` raises
        :class:`InvalidRequestError` instead of either."""
        if len(self._records) == 1:
            return self._records[0]
        if strict:
            raise InvalidRequestError(f"expected exactly one record, got {len(self._records)}")
        if not self._records:
            return None
        warnings.warn("expected a result with a single record, but it contains more", stacklevel=2)
        return self._records[0]

    def value(self, key: str | int = 0, default: Any = None) -> list[Any]:
        """One column's values, ``default`` where a record lacks it."""
        if isinstance(key, str) and key not in self._keys:
            return [default] * len(self._records)
        return [r[key] for r in self._records]

    def values(self, *keys: str | int) -> list[list[Any]]:
        return [r.values(*keys) for r in self._records]

    def data(self, *keys: str | int) -> list[dict[str, Any]]:
        """Every record as a dict of plain values; see :meth:`Record.data`."""
        return [r.data(*keys) for r in self._records]

    def to_pandas(self) -> pandas.DataFrame:
        """The records as a pandas DataFrame, one column per key. Nodes and
        relationships stay objects."""
        import pandas

        return pandas.DataFrame.from_records(self._records, columns=self._keys)

    to_df = to_pandas


class RowStream(Iterator[Record]):
    """The records of a SELECT, read as the query produces them.

    Iterate it like a :class:`Result`; memory stays flat however many records
    the query returns. Leaving it before the end — ``break``, :meth:`close`, or
    the end of a ``with`` block — stops the query.
    """

    __slots__ = ("_batch_size", "_buffer", "_keys", "_native", "_record")

    def __init__(self, native: _fluree.RowStream, batch_size: int) -> None:
        self._native: _fluree.RowStream | None = native
        self._batch_size = batch_size
        self._buffer: deque[tuple[Any, ...]] = deque()
        self._keys: list[str] | None = native.columns
        self._record: type[Record] | None = None

    def keys(self) -> list[str] | None:
        """Column names in projection order (for ``SELECT *``, known once the
        first record is read)."""
        return None if self._keys is None else list(self._keys)

    @property
    def columns(self) -> list[str] | None:
        """The same as :meth:`keys`."""
        return self.keys()

    def __iter__(self) -> RowStream:
        return self

    def __next__(self) -> Record:
        while not self._buffer:
            if not self._fill():
                raise StopIteration
        return self._take()

    def _fill(self) -> bool:
        """Read the next batch into the buffer; ``False`` once the stream ends."""
        native = self._native
        if native is None:
            return False
        batch = native.next_batch(self._batch_size)
        if self._keys is None:
            self._keys = native.columns
        if batch is None:
            self._native = None
            return False
        self._buffer.extend(batch)
        return True

    def _take(self) -> Record:
        if self._record is None:
            self._record = _record_type(self._keys or [])
        return self._record(map(to_python, self._buffer.popleft()))

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
