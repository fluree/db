"""A reference worker for custom stages that run outside the engine.

A stage reads pinned inputs from a ledger, computes in ordinary Python, and
publishes its complete result into a named graph it owns, replacing the
previous result in one commit together with a generation record. Two stages
make up the example program, a claims review:

``extract``
    Runs a model over each document's text and publishes the extracted
    vendor and amount. A retained extraction for the same model and the same
    text is reused, so the model runs only on new or changed text.

``gate``
    A deterministic, versioned decision per case over the documents, their
    retained extractions and reference data, at an explicit evaluation time.
    It never calls the model, so a new gate version replays over the same
    pinned inputs. Evidence that is missing (no text, no extraction, fewer
    documents than the case expects), ambiguous (two texts for one document)
    or partial (a vendor without an amount) makes a case ``indeterminate``;
    it is never read as the absence of a problem.

Every run records what it read and how: the ledger and transaction its
inputs were pinned to, the evaluation time, the program, code, engine and
gate versions, the model, and a hash of its output. A run that fails
publishes nothing, so the previous generation stays in place. Publication is
a compare-and-swap on the generation number: a run that finds a newer
generation than the one it started from refuses to publish over it.

The manifest format is experimental.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import fluree
from fluree import IRI

EX = "http://example.org/"
W = "http://example.org/worker#"
MANIFEST_FORMAT = "fluree-reference-worker/experimental-1"
CODE_VERSION = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12]

Extraction = Mapping[str, Any]
Model = Callable[[str], Extraction | None]


class StaleRun(Exception):
    """Another run published to the stage's graph after this one started."""


@dataclass(frozen=True)
class Manifest:
    program: str
    program_version: str
    ledger: str
    clock: datetime
    extraction_graph: str
    model: str
    decision_graph: str
    gate_version: str
    input_t: int | None = None

    @classmethod
    def from_json(cls, doc: Mapping[str, Any]) -> Manifest:
        if doc.get("format") != MANIFEST_FORMAT:
            raise ValueError(f"expected a {MANIFEST_FORMAT!r} manifest")
        clock = datetime.fromisoformat(doc["clock"].replace("Z", "+00:00"))
        if clock.tzinfo is None:
            raise ValueError("the clock needs a timezone")
        if doc["gate"]["version"] not in GATES:
            raise ValueError(f"unknown gate version {doc['gate']['version']!r}")
        return cls(
            program=doc["program"],
            program_version=doc["programVersion"],
            ledger=doc["ledger"],
            clock=clock,
            extraction_graph=doc["extract"]["graph"],
            model=doc["extract"]["model"],
            decision_graph=doc["gate"]["graph"],
            gate_version=doc["gate"]["version"],
            input_t=doc.get("inputT"),
        )


@dataclass(frozen=True)
class Published:
    """A completed generation."""

    generation: int
    t: int
    input_t: int
    output_hash: str


# -- reading ------------------------------------------------------------------

PREFIXES = f"PREFIX ex: <{EX}> PREFIX w: <{W}> "


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def pin(ledger: fluree.Ledger, manifest: Manifest) -> fluree.Snapshot:
    """The snapshot every input of the run is read from."""
    if manifest.input_t is None:
        return ledger.snapshot()
    return ledger.at(t=manifest.input_t)


@dataclass(frozen=True)
class Document:
    """A document with every received time and text it has. Each should have
    one; a document with two is ambiguous evidence, not two documents."""

    iri: str
    received: tuple[datetime, ...]
    texts: tuple[str, ...]

    @property
    def text(self) -> str | None:
        """The document's text, if it has exactly one."""
        return self.texts[0] if len(self.texts) == 1 else None


@dataclass(frozen=True)
class Case:
    iri: str
    expected: tuple[int, ...]
    documents: tuple[Document, ...]


def cases(snapshot: fluree.Snapshot) -> list[Case]:
    """Every case, with the number of documents it expects and those it has.

    The query returns a row for each combination of a case's and a
    document's values, so the rows are grouped by case and by document."""
    rows = snapshot.query(
        PREFIXES + "SELECT ?case ?expected ?doc ?received ?text WHERE { "
        "?case a ex:Case ; ex:expectedDocuments ?expected . "
        "OPTIONAL { ?doc ex:case ?case "
        "  OPTIONAL { ?doc ex:receivedAt ?received } OPTIONAL { ?doc ex:text ?text } } }"
    )
    expected: dict[str, set[int]] = {}
    docs: dict[str, dict[str, tuple[set[datetime], set[str]]]] = {}
    for case, count, doc, received, text in rows:
        expected.setdefault(case, set()).add(count)
        found = docs.setdefault(case, {})
        if doc is not None:
            times, texts = found.setdefault(doc, (set(), set()))
            if received is not None:
                times.add(received)
            if text is not None:
                texts.add(text)
    return [
        Case(
            iri,
            tuple(sorted(expected[iri])),
            tuple(
                Document(doc, tuple(sorted(times)), tuple(sorted(texts)))
                for doc, (times, texts) in sorted(docs[iri].items())
            ),
        )
        for iri in sorted(expected)
    ]


def retained_extractions(snapshot: fluree.Snapshot, graph: str) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Published extractions, by document, model and text hash."""
    rows = snapshot.query(
        PREFIXES + f"SELECT ?doc ?model ?hash ?vendor ?amount WHERE {{ GRAPH <{graph}> {{ "
        "?x w:document ?doc ; w:model ?model ; w:textHash ?hash "
        "OPTIONAL { ?x w:vendor ?vendor } OPTIONAL { ?x w:amount ?amount } } }"
    )
    retained: dict[tuple[str, str, str], dict[str, Any]] = {}
    for doc, model, digest, vendor, amount in rows:
        extraction = retained.setdefault((doc, model, digest), {})
        if vendor is not None:
            extraction["vendor"] = vendor
        if amount is not None:
            extraction["amount"] = Decimal(str(amount))
    return retained


def current_generation(view: Any, graph: str) -> int:
    """The generation published in ``graph`` as ``view`` sees it, 0 for none."""
    rows = view.query(
        PREFIXES + f"SELECT ?g WHERE {{ GRAPH <{graph}> {{ <{graph}#generation> w:generation ?g }} }}"
    )
    return rows[0][0] if len(rows) else 0


# -- the gate -------------------------------------------------------------------


@dataclass(frozen=True)
class Reference:
    approved_vendors: frozenset[str]
    threshold: Decimal


def reference_data(snapshot: fluree.Snapshot) -> Reference:
    vendors = snapshot.query(PREFIXES + "SELECT ?name WHERE { ?v a ex:ApprovedVendor ; ex:name ?name }")
    threshold = snapshot.query(PREFIXES + "SELECT ?limit WHERE { ex:policy ex:reviewThreshold ?limit }")
    if len(threshold) != 1:
        raise ValueError("the reference data has no single ex:reviewThreshold")
    return Reference(frozenset(name for (name,) in vendors), Decimal(str(threshold[0][0])))


FIELDS = ("vendor", "amount")


def gaps(case: Case, extraction_of: Callable[[Document], Extraction | None], clock: datetime) -> list[str]:
    """Why the evidence for a case is incomplete or ambiguous at ``clock``."""
    found: list[str] = []
    for doc in case.documents:
        if not doc.received:
            found.append(f"missing-received-time:{doc.iri}")
        elif len(doc.received) > 1:
            found.append(f"ambiguous-received-time:{doc.iri}")
    received = received_by(case, clock)
    if len(case.expected) > 1:
        found.append("ambiguous-expected-documents")
    elif len(received) < case.expected[0]:
        found.append(f"incomplete-coverage:{len(received)}/{case.expected[0]}")
    for doc in received:
        if not doc.texts:
            found.append(f"missing-text:{doc.iri}")
        elif len(doc.texts) > 1:
            found.append(f"ambiguous-text:{doc.iri}")
        elif (extraction := extraction_of(doc)) is None:
            found.append(f"missing-extraction:{doc.iri}")
        elif 0 < sum(extraction.get(key) is not None for key in FIELDS) < len(FIELDS):
            # The model read some fields and not others; a missing vendor is
            # not an approved one, and a missing amount is not zero.
            found.append(f"partial-extraction:{doc.iri}")
    return found


def received_by(case: Case, clock: datetime) -> list[Document]:
    """The documents known to have arrived at ``clock``; later ones do not count yet."""
    return [d for d in case.documents if len(d.received) == 1 and d.received[0] <= clock]


def per_document_limit(amounts: list[Decimal], limit: Decimal) -> bool:
    return any(amount > limit for amount in amounts)


def case_total_limit(amounts: list[Decimal], limit: Decimal) -> bool:
    return sum(amounts, Decimal(0)) > limit


GATES: dict[str, Callable[[list[Decimal], Decimal], bool]] = {
    "v1": per_document_limit,
    "v2": case_total_limit,
}


def decide(
    case: Case,
    extraction_of: Callable[[Document], Extraction | None],
    reference: Reference,
    clock: datetime,
    gate_version: str,
) -> tuple[str, list[str]]:
    """A case's outcome and the reasons for it."""
    missing = gaps(case, extraction_of, clock)
    if missing:
        return "indeterminate", missing
    findings: list[str] = []
    amounts: list[Decimal] = []
    for doc in received_by(case, clock):
        extraction = extraction_of(doc) or {}
        vendor = extraction.get("vendor")
        if vendor is not None and vendor not in reference.approved_vendors:
            findings.append(f"unapproved-vendor:{vendor}")
        if extraction.get("amount") is not None:
            amounts.append(extraction["amount"])
    if GATES[gate_version](amounts, reference.threshold):
        findings.append("over-threshold")
    return ("flagged" if findings else "cleared"), sorted(findings)


# -- publishing -----------------------------------------------------------------


def output_hash(nodes: list[dict[str, Any]]) -> str:
    canonical = json.dumps(sorted(nodes, key=lambda n: n["@id"]), sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def publish(
    ledger: fluree.Ledger,
    graph: str,
    nodes: list[dict[str, Any]],
    record: dict[str, Any],
    started_from: int,
    input_t: int,
) -> Published:
    """Replace ``graph`` with ``nodes`` and a generation record, in one commit.

    The graph is cleared and rewritten inside one transaction, which nets to
    the difference: unchanged facts produce no flakes. The generation read
    inside the same transaction must still be ``started_from``; if another
    commit lands first, the transaction runs again and re-reads it.
    """
    digest = output_hash(nodes)
    generation = started_from + 1
    marker = {
        "@id": f"{graph}#generation",
        W + "generation": generation,
        W + "inputT": input_t,
        W + "outputHash": digest,
        W + "outputCount": len(nodes),
        W + "codeVersion": CODE_VERSION,
        W + "engineVersion": fluree.__version__,
        W + "status": "complete",
        **record,
    }

    def write(txn: fluree.Transaction) -> fluree.Commit:
        current = current_generation(txn, graph)
        if current != started_from:
            raise StaleRun(f"{graph} is at generation {current}; this run started from {started_from}")
        txn.update(f"DELETE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }} WHERE {{ GRAPH <{graph}> {{ ?s ?p ?o }} }}")
        txn.insert([{**node, "@graph": graph} for node in [*nodes, marker]])
        return txn.commit(message=f"{record[W + 'stage']} generation {generation} of <{graph}>")

    commit = ledger.transact(write)
    return Published(generation, commit.t, input_t, digest)


def run_record(manifest: Manifest, stage: str, snapshot: fluree.Snapshot) -> dict[str, Any]:
    return {
        W + "stage": stage,
        W + "program": IRI(manifest.program),
        W + "programVersion": manifest.program_version,
        W + "inputLedger": snapshot.ledger,
        W + "evaluationTime": manifest.clock,
        W + "model": manifest.model,
    }


# -- the stages -----------------------------------------------------------------


def run_extract(ledger: fluree.Ledger, manifest: Manifest, model: Model) -> Published:
    """Extract every document's text that has no retained extraction."""
    graph = manifest.extraction_graph
    started_from = current_generation(ledger, graph)
    snapshot = pin(ledger, manifest)
    retained = retained_extractions(snapshot, graph)
    nodes = []
    for doc in (d for case in cases(snapshot) for d in case.documents):
        if doc.text is None:
            continue  # no text, or several: the gate reports it
        digest = text_hash(doc.text)
        extraction: Extraction | None = retained.get((doc.iri, manifest.model, digest))
        if extraction is None:
            extraction = model(doc.text)
        if extraction is None:
            continue  # unavailable: the gate reports it, and the next run retries
        node = {
            "@id": f"{graph}/{text_hash(doc.iri + manifest.model)[:16]}",
            W + "document": IRI(doc.iri),
            W + "model": manifest.model,
            W + "textHash": digest,
        }
        for key in FIELDS:
            if extraction.get(key) is not None:
                node[W + key] = extraction[key]
        nodes.append(node)
    return publish(ledger, graph, nodes, run_record(manifest, "extract", snapshot), started_from, snapshot.t)


def run_gate(ledger: fluree.Ledger, manifest: Manifest) -> Published:
    """Decide every case from pinned evidence and retained extractions."""
    graph = manifest.decision_graph
    started_from = current_generation(ledger, graph)
    snapshot = pin(ledger, manifest)
    reference = reference_data(snapshot)
    retained = retained_extractions(snapshot, manifest.extraction_graph)

    def extraction_of(doc: Document) -> Extraction | None:
        if doc.text is None:
            return None
        return retained.get((doc.iri, manifest.model, text_hash(doc.text)))

    nodes = []
    for case in cases(snapshot):
        outcome, reasons = decide(case, extraction_of, reference, manifest.clock, manifest.gate_version)
        node: dict[str, Any] = {
            "@id": f"{graph}/{text_hash(case.iri)[:16]}",
            W + "case": IRI(case.iri),
            W + "outcome": outcome,
            W + "gateVersion": manifest.gate_version,
        }
        if reasons:
            node[W + "reason"] = reasons
        nodes.append(node)
    record = {**run_record(manifest, "gate", snapshot), W + "gateVersion": manifest.gate_version}
    return publish(ledger, graph, nodes, record, started_from, snapshot.t)


# -- a toy model and the command line -----------------------------------------------

INVOICE = re.compile(r"from (?P<vendor>[A-Z][\w ]*?) for (?P<amount>\d+(?:\.\d+)?)")


def toy_model(text: str) -> Extraction | None:
    """Reads ``from <Vendor> for <amount>``. Text that is not an invoice has
    nothing to extract; an invoice it cannot read gets no answer."""
    match = INVOICE.search(text)
    if match is not None:
        return {"vendor": match["vendor"], "amount": Decimal(match["amount"])}
    return None if "invoice" in text.lower() else {}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--storage", required=True, help="the Fluree storage directory")
    parser.add_argument("--stage", choices=["extract", "gate", "all"], default="all")
    args = parser.parse_args(argv)
    manifest = Manifest.from_json(json.loads(args.manifest.read_text()))
    with fluree.connect(args.storage) as conn:
        ledger = conn.ledger(manifest.ledger)
        if args.stage in ("extract", "all"):
            print("extract:", run_extract(ledger, manifest, toy_model))
        if args.stage in ("gate", "all"):
            print("gate:", run_gate(ledger, manifest))


if __name__ == "__main__":
    main()
