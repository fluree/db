"""Read documents with an extraction model reached over HTTP.

:func:`read` parses a file (PDF, Word, PowerPoint, HTML, Markdown, text,
spreadsheets, transcripts, email) the way ``fluree doc parse`` does, sends
its text and structure with your ontology and catalog to a reader, and
returns its reading: a Fluree DB transaction holding the run (the document
read, the release that read it), the facts in your ontology's terms, the
claims behind them and the mentions, each anchored to character offsets in
the parser's text (what ``fluree doc parse --format text`` prints)::

    import fluree

    reading = fluree.doc.read("invoice.pdf", ontology="invoices.ttl",
                              catalog="suppliers.ttl", model="reader-small")
    for m in reading.mentions():
        print(m["text"], m["entity"])

    with fluree.connect("./data") as conn:
        conn.create("invoices").insert(reading.to_jsonld())

``include=`` says what comes with the facts, which always come: ``"claims"``,
``"mentions"``, both (the default) or ``"none"``.

The read is the ``fluree`` CLI's own, so a document is cut into calls, sent,
resent when the reader has no capacity yet, and kept the same way. Waits for
capacity (a model still starting can take minutes) are logged as warnings
on the ``fluree.engine`` logger. Python does no vision escalation: scanned
pages and images are read with the parser's deterministic tier only, and
:attr:`Reading.notes` says what it couldn't read.

**Signing in.**

1. ``api_key`` with ``auth_header``: that key, sent as is in that header,
   to ``url``.
2. ``api_key`` alone: a Fluree.ai API key (``flk_…``) is traded for an
   access token (reused for the hour it lasts), or, when the service won't
   trade it (a developer key, which calls the reader itself), sent as it
   is; any other key is sent as ``Authorization: Bearer``, as the CLI's
   ``[doc.reader] api_key`` is.
3. No ``api_key``: this machine's ``fluree login``, else the
   ``FLUREE_AI_API_KEY`` environment variable (as in 2). Each is sent only
   to its own service; a ``url`` elsewhere is called with no credential.

With no ``url``, a Fluree.ai sign-in reads through the account.

**Rows and terms.** The reading is kept as the reader wrote it, in the
extraction vocabulary (``xt:``, ``https://ns.flur.ee/extract#``):
:attr:`Reading.transactions` holds one transaction per call (a document
longer than one call's limit takes several, each its own run, its offsets
moved into the whole document's text). :meth:`Reading.mentions` and
:meth:`Reading.claims` are rows read out of it.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from fluree import _fluree

__all__ = ["Reading", "Usage", "read"]

_RDF_FILE = (".ttl", ".nt", ".jsonld", ".json")
_INCLUDE = ("claims", "mentions")


@dataclass(frozen=True)
class Usage:
    """What a read was billed, as the reader reported it."""

    input_tokens: int
    windows: int
    cache_read_tokens: int
    cache_write_tokens: int
    fuel: float | None
    note: str
    """The same, as one line: ``1,032 tokens, cache 900 read / 0 written``."""


@dataclass(frozen=True)
class Reading:
    """One document's reading, as the reader wrote it."""

    transactions: list[dict[str, Any]]
    """One Fluree DB transaction (JSON-LD) per call: the run in its
    ``txn-meta``, the facts and the claims annotating them in its
    ``@graph``, the mentions in a named graph of their own."""
    fact_count: int
    """Statements in the facts, the claims aside."""
    new_entities: list[str]
    """Entities the runs minted for things the catalog doesn't hold."""
    linked: list[str]
    """Catalog records the mentions link to."""
    usage: Usage
    calls: int
    """Calls the document took; :attr:`cached` of them were answered from the cache."""
    cached: int
    release: str | None
    runs: list[dict[str, Any]] = field(default_factory=list)
    """One per call: ``run`` (the run's IRI), ``release``, ``request_id``
    (the endpoint's ID for the call) and ``cached``."""
    ontology_notes: list[str] = field(default_factory=list)
    """What the reader set aside or adapted in the ontology."""
    notes: list[str] = field(default_factory=list)
    """What the parse left out: pages it couldn't read (no vision escalation
    from Python), an email's attachments, and the like."""
    _mentions: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _claims: list[dict[str, Any]] = field(default_factory=list, repr=False)

    @property
    def mention_count(self) -> int:
        return len(self._mentions)

    @property
    def claim_count(self) -> int:
        return len(self._claims)

    def to_jsonld(self) -> dict[str, Any]:
        """The reading as a transaction, ready for ``Ledger.insert``: the
        run becomes the commit's metadata. A document read in several calls
        has a transaction per call; insert each of :attr:`transactions`."""
        if len(self.transactions) != 1:
            raise ValueError(
                f"read in {len(self.transactions)} calls: insert each of .transactions, one per run"
            )
        return self.transactions[0]

    def to_trig(self) -> str:
        """The reading as TriG: the run in ``<#txn-meta>``, each claim behind
        a fact reifying it."""
        return _fluree.reading_text(json.dumps(self.transactions), "trig")

    def to_turtle(self) -> str:
        """The reading's statements as Turtle, without the run."""
        return _fluree.reading_text(json.dumps(self.transactions), "turtle")

    def mentions(self) -> list[dict[str, Any]]:
        """One row per mention (``xt:Occurrence``), in text order: ``id``,
        ``text`` (``nif:anchorOf``), ``begin`` and ``end`` (offsets in the
        parser's text), ``class`` (``xt:occurrenceClass``), ``entity`` (the
        catalog record it links to, ``xt:entity``, or the entity the run
        minted, ``xt:localEntity``, and then ``new_entity``), ``confidence``
        (``xt:linkConfidence`` for a link, else ``xt:score``) and
        ``element`` (the structure element it sits in)."""
        return [dict(m) for m in self._mentions]

    def claims(self) -> list[dict[str, Any]]:
        """One row per claim (``xt:Claim``): ``id``, ``subject``,
        ``predicate``, ``object`` (an IRI, or a literal's text), ``status``
        (``xt:assertionStatus``), ``accepted``, ``score``, and ``asserted``
        (the claim annotates its fact, which is in the facts)."""
        return [dict(c) for c in self._claims]


def read(
    path: str | os.PathLike[str] | None = None,
    *,
    text: str | None = None,
    ontology: str | os.PathLike[str],
    catalog: str | os.PathLike[str] | Sequence[str | os.PathLike[str] | tuple[str | os.PathLike[str], str]] | None = None,
    model: str,
    cache: str | None = None,
    include: str | Sequence[str] = _INCLUDE,
    url: str | None = None,
    api_key: str | None = None,
    auth_header: str | None = None,
    doc_iri: str | None = None,
    base_iri: str = "urn:fluree:doc:",
    cache_dir: str | os.PathLike[str] | None = None,
) -> Reading:
    """Read one document with ``model`` and return its :class:`Reading`.

    Give a file as ``path``, or plain text as ``text``. A text is read under
    ``doc_iri``, by default ``<base_iri>text/<first 16 hex of its SHA-256>``,
    so two different texts never share a document's nodes. ``ontology`` is
    a ``.ttl``, ``.nt``, ``.jsonld`` or ``.json`` file. ``catalog`` is one
    such file, or a list of them, each optionally a ``(file, class IRI)``
    pair to take only that class's records; a bare 2-tuple is read as such
    a pair unless its second item is itself an RDF file. ``cache``
    (``"ontology"``, ``"catalog"`` or ``"off"``) lets the reader keep that
    much for the next request sharing it. ``include`` says what comes with
    the facts: ``"claims"``, ``"mentions"``, both (the default, as
    ``("claims", "mentions")`` or ``"claims,mentions"``), or ``"none"``.
    With ``cache_dir``, parses and answers are kept there, so reading the
    same request again costs nothing.

    Raises :class:`fluree.errors.InvalidRequestError` for arguments that
    can't make a read (a bad ``cache``, ``auth_header`` without
    ``api_key``, no way to sign in),
    :class:`fluree.errors.PermissionDeniedError` when a credential is
    refused, and :class:`fluree.errors.FlureeError` for a reader's other
    refusals, with its message.
    """
    if (path is None) == (text is None):
        raise TypeError("read() takes a path or text=, not both")
    catalogs: list[tuple[str, str | None]] = []
    for entry in _as_list(catalog):
        if isinstance(entry, tuple):
            if len(entry) != 2:
                raise TypeError(
                    f"a catalog entry is a file or a (file, class IRI) pair, not {entry!r}"
                )
            catalogs.append((os.fspath(entry[0]), str(entry[1])))
        else:
            catalogs.append((os.fspath(entry), None))
    kwargs: dict[str, Any] = dict(
        ontology=os.fspath(ontology),
        catalog=catalogs,
        model=model,
        url=url,
        api_key=api_key,
        auth_header=auth_header,
        cache=cache,
        include=include if isinstance(include, str) else ",".join(include) or "none",
        doc_iri=doc_iri,
        base_iri=base_iri,
        cache_dir=os.fspath(cache_dir) if cache_dir is not None else None,
    )
    if text is not None:
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
        if doc_iri is None:
            kwargs["doc_iri"] = f"{base_iri}text/{digest}"
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp) / f"{digest}.txt"
            file.write_text(text, encoding="utf-8")
            raw = _fluree.read_document(os.fspath(file), **kwargs)
    else:
        raw = _fluree.read_document(os.fspath(path), **kwargs)  # type: ignore[arg-type]
    return _reading(raw)


def _reading(raw: Mapping[str, Any]) -> Reading:
    u = raw["usage"]
    return Reading(
        transactions=json.loads(raw["transactions"]),
        fact_count=raw["facts"],
        new_entities=list(raw["new_entities"]),
        linked=list(raw["linked"]),
        usage=Usage(
            input_tokens=u["input_tokens"],
            windows=u["windows"],
            cache_read_tokens=u["cache_read_tokens"],
            cache_write_tokens=u["cache_write_tokens"],
            fuel=u["fuel"],
            note=u["note"],
        ),
        calls=raw["calls"],
        cached=raw["cached"],
        release=raw["release"],
        runs=json.loads(raw["runs"]),
        ontology_notes=list(raw["ontology_notes"]),
        notes=list(raw.get("notes", [])),
        _mentions=json.loads(raw["mentions"]),
        _claims=json.loads(raw["claims"]),
    )


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike)):
        return [value]
    if isinstance(value, tuple):
        # A (file, class) pair, unless both items are RDF files.
        if len(value) == 2 and not os.fspath(value[1]).lower().endswith(_RDF_FILE):
            return [value]
        return list(value)
    return list(value)
