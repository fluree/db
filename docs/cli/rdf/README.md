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

`--profile` reports where the time went, by phase. Note the `=`:
`--profile=json`, not `--profile json` — the flag's value must be attached,
because a space-separated optional value could not be told apart from the
input filename.

```console
$ fluree rdf count corpus.ttl --profile
  ┌──────────┬──────────────┬──────────────┐
  │ phase    │           ms │       % wall │
  ├──────────┼──────────────┼──────────────┤
  │ read     │         1.73 │         0.07 │
  │ parse    │      2481.16 │        99.81 │
  └──────────┴──────────────┴──────────────┘

  count corpus.ttl · turtle · 5.38 MiB · macos-aarch64
  wall 2.49s (input → parse; excludes startup and fingerprinting) · 3.1ms unattributed
  240000 triples · 96.5K/s · 120001 grammar statements · peak RSS 58.05 MiB
  sink: below the measurement floor — across 720004 calls its per-event cost is
  under 79.9ms, which is where the clock's own 26.6ms stops being separable from it
  profiler cost 0.009% of wall (11481 clock reads @ 37ns/pair)
```

Four things in that report are worth understanding.

**The sink usually cannot be measured, and says so.** Timing every sink call
would take two clock reads per event, and on these verbs a clock read costs
*more than the work it measures* — a bracketed discard-sink call is about
20 ns, of which 19 ns is the clock. So events are counted exactly and timed
on a sample of statements, the clock's own cost is subtracted from that
sample before it is scaled up, and if what remains does not clear three times
the extrapolated clock artifact the tool reports "below the measurement
floor" rather than a number. That is not the same as zero: the sink's cost is
unresolved, not absent. Resolving it needs a differential run (time the same
corpus with and without the sink's work) — or a sink expensive enough to
clear the floor, which a real serializer is.

**A costly `finish()` is measured exactly.** A writer's flush runs once, so it
is timed directly and never passed through the sample scaling, and it appears
on its own line.

**Shares are of wall clock, not of the phase sum,** so time nobody claimed
shows up as `unattributed` instead of being redistributed into the phases
that did run. The measured window runs from the first byte of input handling
to the end of parsing: it excludes process startup, and it excludes the
SHA-256 fingerprint, which is computed after the clock stops. Pass
`--no-hash` to skip that pass entirely.

**The profiler prices its own instrument.** It measures what a clock read
costs on this host and reports two figures: what the reads it actually took
cost the run, and what the clock artifact carried by a *scaled* sink estimate
would come to. If either exceeds 2% of wall the report is marked `UNTRUSTED`,
and the line says which one — an artifact-driven warning leaves the read,
decompress and parse phases perfectly sound.

`--profile=json` emits the same run as one JSON document on stderr. Beyond the
phase array it carries the tool version and the git SHA it was built from, the
host class, thread count and peak RSS, the corpus fingerprint, event counts,
rates, a `sink` block (including `body_ns: null` when unresolved), and the
self-calibration block. The `schema` field is `fluree.rdf.profile.v1`.

```bash
fluree rdf count dump.ttl --profile=json --no-hash 2> run.json
```

Set `FLUREE_BENCH_HOST_CLASS` to name the machine class for baseline
comparison; it defaults to `{os}-{arch}`, which does not distinguish two
different cloud instance types. Absolute timings may only be diffed between
runs whose `host_class` matches.

## Memory

Input is read into memory in one pass, so peak RSS scales with the input —
but not by a fixed multiple, because most of the excess is the parser's IRI
cache, and that grows with the number of *distinct* IRIs rather than with
bytes.

Measured on this CLI over a 20 MiB Turtle corpus:

| Corpus shape | Peak RSS / input bytes |
|---|---|
| Every subject distinct | ~6.2× |
| 100 subjects, reused | ~2.4× |

Plus a fixed ~25 MiB for the binary and runtime. At the 4 GiB input cap that
is roughly 10–25 GB depending on shape, which is worth knowing before pointing
this at a large dump on a small machine. Streaming and chunked reading land
with the parallel conversion pipeline; until then, split large inputs.

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
inputs, and of streams, lands with the parallel conversion pipeline. See
[Memory](#memory) for what that costs in RSS.
