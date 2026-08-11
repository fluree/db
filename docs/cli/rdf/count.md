# fluree rdf count

Count the statements in an RDF document.

```bash
fluree rdf count [<FILE>] [options]
```

```bash
fluree rdf count dump.ttl
fluree rdf count dump.nt.gz --time
TRIPLES=$(fluree rdf count -q dump.ttl)
cat dump.ttl | fluree rdf count --syntax turtle
```

## Output

```console
$ fluree rdf count dump.ttl
triples: 240000
terms: 480001 (iri 120001, blank 0, literal 360000)
grammar statements: 120001
prefixes: 1
```

`triples` is the RDF edge count — the number other tools report for the same
file. `grammar statements` is Turtle's `statement` production, which counts
directives and counts a whole predicate-object list as one; the two differ by
design and are never both called "statements".

Under `-q` the output is the bare total and nothing else, for capturing in a
shell variable:

```console
$ fluree rdf count -q dump.ttl
240000
```

Lines for quads, list items, and reified triples appear only when the count
is non-zero. Quads stay at zero until the N-Quads and TriG readers land; the
counting sink already accepts them, so the line appears the day they do.

## Options

| Option | Description |
|--------|-------------|
| `--syntax <SYNTAX>` | Input syntax, overriding extension and sniffing |
| `--base <IRI>` | Base IRI for resolving relative references |
| `--time` | Print elapsed time and throughput to stderr |
| `--profile[=FORMAT]` | Per-phase timing breakdown (`human` or `json`) |
| `--no-hash` | Skip the corpus SHA-256 in `--profile` output |
| `-q, --quiet` | Print the bare total only (global flag) |

See [the `rdf` overview](README.md) for input handling, syntax resolution,
and profiling, which are shared by every verb.

## What counts as a statement

Triples, as RDF defines them — which is not always what Fluree's ingest path
stores.

A collection is counted as the `rdf:first`/`rdf:rest` blank-node spine it
denotes. `<s> <p> ( "a" "b" "c" ) .` is **seven** triples: three `rdf:first`,
three `rdf:rest`, and the statement itself. Fluree's transaction path
flattens the same collection into three indexed list items on one edge,
because that is what a flake stores; if `count` reported that shape it would
disagree with every other RDF tool on the same file.

For the same reason an object-position `()` counts as the one `rdf:nil`
triple it denotes, and numeric literals keep the lexical form they were
written with rather than being canonicalized.

## Timing

`--time` adds one line on stderr:

```console
$ fluree rdf count dump.ttl --time
triples: 240000
...
  240,000 triples in 2.49s (96,514 triples/s, 2.2 MiB/s)
```

For a breakdown by phase — how much of that was I/O, decompression, and
parsing — use `--profile`, documented in the [overview](README.md).

Counting is a parse whose results are discarded, which makes it the cheapest
honest measurement of parser throughput available: no sink work, no
serialization, no output I/O. That is why the benchmark lane leans on it.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | The document did not parse |
| `2` | The invocation was wrong: no such file, unreadable, unknown syntax |

A document that fails to parse partway through does not print a count — a
partial number that looks whole is worse than no number. The error names how
far it got instead:

```console
$ fluree rdf count broken.ttl
error: broken.ttl:3:16: unexpected character '?'
  counted 1 triple(s) before the document stopped parsing
```
