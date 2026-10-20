# Custom stages in Python

Some steps in a reasoning program are not rules: a model reads documents, or
a versioned decision procedure weighs evidence. Run those steps in Python
next to the ledger, and publish each step's result into a named graph it
owns. Queries, datalog rules and later steps then read that graph like any
other data, and every published result stays queryable as of the commit
that published it.

The [reference worker](https://github.com/fluree/db/tree/main/fluree-db-python/examples/reference_worker)
is a working example of the pattern, built only on the
[Python package](../getting-started/python.md)'s ordinary query and
transaction calls. Its manifest format is experimental.

## The contract

A stage that follows it can be rerun, replayed and audited:

- **Pinned inputs.** Every read in a run comes from one snapshot,
  `ledger.at(t=…)`, and the run records that `t`. Rerunning with the same
  pin reads the same facts, however much the ledger has changed since.
- **An explicit clock.** The run takes its evaluation time from the
  manifest and never reads the wall clock, so "received by" and "within 30
  days" mean the same thing on every rerun.
- **One owned graph per stage.** Nothing else writes to it. A run replaces
  the graph's whole contents in one commit, so readers see either the
  previous result or the new one, never a mix.
- **A generation record in the graph.** Next to the results, a node
  `<graph>#generation` records the generation number, the input ledger and
  `t`, the evaluation time, the program, code, engine and gate versions, the
  model, a hash and count of the results, and `"complete"`. It is published
  in the same commit as the results, so the two always agree. The record is
  there even when the result is empty, and it keeps the graph in existence.
- **Failures publish nothing.** A run that raises before it publishes
  leaves the previous generation in place.
- **One writer per graph.** Publication is a compare-and-swap on the
  generation number: a run that finds a newer generation than the one it
  started from refuses to publish over it.
- **Missing evidence is not an answer.** A document with no text, an
  extraction the model could not produce, or fewer documents than a case
  expects makes the case `indeterminate`. So does evidence that is
  ambiguous or partial: a document with two texts or two received times, or
  an extraction with a vendor and no amount, or an amount and no vendor.
  Only a completed search that finds nothing counts as nothing found.

## The example program

The worker reviews claims in two stages.

`extract` runs a model over each document's text and publishes the vendor
and amount it finds. Each extraction records the model and a hash of the
text it read, and a later run reuses it while both still match, so the model
runs only on new or changed text. A model that cannot answer for a document
returns nothing; the document gets no extraction, and the next run asks
again.

`gate` decides each case from the documents that had arrived by the
evaluation time, their extractions and reference data: `flagged` with the
reasons (an unapproved vendor, an amount over the threshold), `cleared`, or
`indeterminate` with the evidence that is missing. Gate `v1` applies the
threshold to each document and `v2` to the case total. The gate never calls
the model, so a new gate version replays over the same pinned inputs:

```python
import dataclasses, json
import fluree
from worker import Manifest, run_extract, run_gate, toy_model

manifest = Manifest.from_json(json.load(open("manifest.json")))
with fluree.connect("./data") as conn:
    ledger = conn.ledger("claims")
    run_extract(ledger, manifest, toy_model)
    first = run_gate(ledger, manifest)

    # The same inputs under the next gate version; no model calls.
    run_gate(ledger, dataclasses.replace(manifest, gate_version="v2", input_t=first.input_t))
```

`toy_model` stands in for a real model. Pass any function from text to a
dict of extracted fields, or `None` when it cannot answer. The toy model
returns `{}` for text that is not an invoice, and `None` for an invoice it
cannot read.

The example directory has the worker, a manifest and sample data. To run it
from the command line:

```python
from pathlib import Path
import fluree

with fluree.connect("./data") as conn:
    conn.create("claims").insert(Path("claims.ttl"))
```

```sh
python worker.py manifest.json --storage ./data
```

## Reading the results

The decisions are ordinary facts in the stage's graph:

```sparql
PREFIX w: <http://example.org/worker#>
SELECT ?case ?outcome ?reason WHERE {
  GRAPH <http://example.org/claims-review/decisions> {
    ?d w:case ?case ; w:outcome ?outcome OPTIONAL { ?d w:reason ?reason }
  }
}
```

An earlier generation is a query of the commit that published it,
`ledger.at(t=first.t)`, and the commit log names each generation in its
message. Datalog rules can build on a published graph: this stored rule
routes every undecided case to an analyst.

```sparql
PREFIX ex: <http://example.org/>
PREFIX w: <http://example.org/worker#>
CONSTRUCT { ?case a ex:NeedsAnalyst }
WHERE { GRAPH <http://example.org/claims-review/decisions> {
  ?d w:case ?case ; w:outcome "indeterminate" } }
```

See [Datalog rules](../query/datalog-rules.md#named-graphs) for rules that
read named graphs.

## Workers in several processes

One process owns a storage directory. Within that process, run workers as
threads or tasks sharing its connection; the compare-and-swap keeps two runs
from publishing over each other. Workers in separate processes, or on
separate machines, should share a [Fluree server](../getting-started/quickstart-server.md)
rather than the directory.

The Python package does not connect to a server yet, so a worker that shares
one uses the [HTTP API](../api/endpoints.md). Its pinned reads are queries
`from` `claims:main@t:<t>`. It publishes with one guarded update: the
`WHERE` requires the generation the run started from, and the update deletes
the graph's contents and inserts the new ones and the next generation
record.

```sparql
PREFIX w: <http://example.org/worker#>
DELETE { GRAPH <http://example.org/claims-review/decisions> { ?s ?p ?o } }
INSERT { GRAPH <http://example.org/claims-review/decisions> {
  # … the new results …
  <http://example.org/claims-review/decisions#generation> w:generation 4 .
} }
WHERE { GRAPH <http://example.org/claims-review/decisions> {
  <http://example.org/claims-review/decisions#generation> w:generation 3 .
  ?s ?p ?o
} }
```

The server applies the update atomically. A run that lost the race matches
nothing and commits nothing; it confirms its own publication by reading the
generation record back and finding its generation and output hash there. The
first publication guards with `FILTER NOT EXISTS` on any generation record
instead.

## Limits

- The worker does not schedule, retry or isolate runs. Start it from
  whatever runs your other jobs.
- A rule cannot call Python. Rules read what a stage published; a stage
  reads what rules derived only through a query with reasoning enabled.
