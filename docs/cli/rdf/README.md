# fluree rdf

RDF syntax tooling that never touches a ledger. `fluree rdf` reads files and
reports on them — no `fluree init`, no ledger, no connection, no `.fluree/`
directory anywhere on the path.

```bash
fluree rdf <verb> [<FILE>] [options]
```

| Verb | Description |
|------|-------------|
| [`check`](check.md) | Parse a document and report syntax errors |
| [`count`](count.md) | Count the statements in a document |
| [`convert`](convert.md) | Convert between RDF syntaxes (not yet implemented) |

## Input

Every verb takes the same input handling.

```bash
fluree rdf count dump.ttl            # a file
fluree rdf count dump.nt.gz          # transparently decompressed
cat dump.ttl | fluree rdf count      # stdin
fluree rdf count - < dump.ttl        # stdin, named explicitly
```

`.gz` and `.zst` inputs decompress transparently. gzip is decoded with a
multi-member decoder, so the concatenated streams `pigz` and `bgzip` produce
decode in full rather than stopping at the first member. A piped input is
detected from its magic bytes, since a pipe has no filename to read a suffix
from — and so is a compressed file that nobody gave a `.gz` suffix.

## Syntax resolution

The input syntax is resolved in this order, and the first rule that answers
wins:

1. `--syntax <SYNTAX>` on the command line.
2. The file extension, after any compression suffix is stripped
   (`data.ttl.gz` → `ttl`).
3. A look at the first bytes of the decompressed document.

| Syntax | `--syntax` value | Extensions | Read | Write |
|--------|------------------|------------|------|-------|
| Turtle | `turtle` (`ttl`, `n3`) | `.ttl`, `.n3` | yes | not yet |
| N-Triples | `ntriples` (`nt`) | `.nt` | yes | not yet |
| N-Quads | `nquads` (`nq`) | `.nq` | not yet | not yet |
| TriG | `trig` | `.trig` | not yet | not yet |
| JSON-LD | `jsonld` (`json-ld`) | `.jsonld`, `.json` | not yet | not yet |
| RDF/XML | `rdfxml` (`xml`) | `.rdf`, `.owl` | not yet | not yet |
| RDF/JSON | `rdfjson` | `.rj` | not yet | not yet |
| Jelly | `jelly` | `.jelly` | not yet | not yet |

N-Triples is a subset of the Turtle grammar and is read by the same parser,
so it is not an approximation. The syntaxes marked "not yet" are named so
that passing one produces a clear refusal naming what it is waiting on,
rather than a parse error partway through a file that was sniffed as
something else.

Content sniffing distinguishes the XML, JSON, and Turtle families and
deliberately does not try to tell Turtle from TriG, or N-Triples from
N-Quads: those distinctions live in the middle of a statement, and a sniffer
that guesses at them is a sniffer that is confidently wrong on the file that
matters. Use `--syntax` for those.

## Shared options

| Option | Description |
|--------|-------------|
| `--syntax <SYNTAX>` | Input syntax, overriding extension and sniffing |
| `--base <IRI>` | Base IRI for resolving relative references |
| `--time` | Print elapsed time and throughput to stderr |
| `--profile[=FORMAT]` | Per-phase timing breakdown: `human` (default) or `json` |
| `--no-hash` | Skip the corpus SHA-256 in `--profile` output |
| `-q, --quiet` | Suppress non-essential output (global flag) |

`--time` and `--profile` both write to **stderr**, so piping a verb's output
stays clean:

```bash
fluree rdf count dump.ttl --time | tee counts.txt
```

## Profiling

`--profile` reports where the time went, by phase.

```console
$ fluree rdf count dbpedia.ttl --profile
  ┌────────────┬──────────────┬──────────────┐
  │ phase      │           ms │       % wall │
  ├────────────┼──────────────┼──────────────┤
  │ read       │        41.20 │         4.10 │
  │ parse      │       952.60 │        94.80 │
  │ sink (est) │        11.30 │         1.12 │
  └────────────┴──────────────┴──────────────┘

  count dbpedia.ttl · turtle · 512.00 MiB
  wall 1.00s · 11.0ms unattributed · 4823119 statements · 4.82M/s
  profiler overhead 0.001% of wall (8 clock reads @ 42ns/pair)
  sink phase is estimated from 0.78% of calls; shares are of wall clock, and
  sink time runs inside parse
```

Three things in that report are worth understanding:

- **The `sink` phase is an estimate.** Timing every sink call would take two
  clock reads per event, and on these verbs a clock read costs more than the
  work it measures — the profile would be reporting its own overhead. So
  events are counted exactly and timed on a sample of statements, then
  scaled. The row is labelled `(est)` and the sample rate is printed.
- **Shares are of wall clock, not of the phase sum.** Time nobody claimed —
  process startup, hashing under `--profile` without `--no-hash` — shows up
  as the `unattributed` figure instead of being redistributed into the
  phases that did run.
- **The profiler prices its own instrument.** It measures the cost of a clock
  read on the host, multiplies by the number it took, and prints the result
  as a share of the run. Above 2% the report is marked `UNTRUSTED` and should
  not be used as a baseline.

`--profile=json` emits the same run as one JSON document on stderr, carrying
the tool version, host and thread count, corpus fingerprint, per-phase
nanoseconds and shares, event counts, rates, and the self-calibration block.
The `schema` field is `fluree.rdf.profile.v1`.

```bash
fluree rdf count dump.ttl --profile=json --no-hash 2> run.json
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | The document is bad — syntax errors found |
| `2` | The invocation is bad — no such file, unreadable, unknown syntax |

The 1/2 split is what makes these verbs scriptable: a wrapper can react
differently to "your RDF is broken" and "your path was typo'd" without
parsing error text.

## Limits

Input is read into memory in one pass and is capped at 4 GiB, because the
parser addresses token spans with 32-bit offsets. Chunked reading of larger
inputs, and of streams, lands with the parallel conversion pipeline.
