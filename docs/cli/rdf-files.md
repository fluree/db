# RDF file tools

Three verbs that never touch a ledger. They read files and report on them —
no `fluree init`, no ledger, no connection, no `.fluree/` directory anywhere
on the path.

| Verb | Description |
|------|-------------|
| [`parse`](parse.md) | Parse a document and report syntax errors |
| [`count`](count.md) | Count the statements in a document |
| [`convert`](convert.md) | Convert between RDF syntaxes |

This page is the shared half: input handling, syntax resolution, the flags all
three take, profiling, memory, and exit codes. Each verb's own page covers what
only it does.

They are top-level commands rather than an `fluree rdf <verb>` group. Nothing
else in the CLI converts or counts a file, so the group prefix bought
disambiguation nobody needed and cost every invocation a word. `parse` was
briefly called `check`; it was renamed because [`fluree validate`](validate.md)
already has a file mode, and `check` beside it read as a shorter `validate` on
a distinction nothing in `--help` could show. `check` still works as an
undocumented alias.

## Input

Every verb takes the same input handling.

```bash
fluree count dump.ttl            # a file
fluree count dump.nt.gz          # transparently decompressed
cat dump.ttl | fluree count      # stdin
fluree count - < dump.ttl        # stdin, named explicitly
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
fluree count dump.ttl --time | tee counts.txt
```

## Profiling

`--profile` reports where the time went, by phase. Note the `=`:
`--profile=json`, not `--profile json` — the flag's value must be attached,
because a space-separated optional value could not be told apart from the
input filename.

```console
$ fluree count corpus.ttl --profile
  ┌──────────┬──────────────┬──────────────┐
  │ phase    │           ms │       % wall │
  ├──────────┼──────────────┼──────────────┤
  │ read     │         2.24 │         0.53 │
  │ parse    │       422.99 │        99.46 │
  └──────────┴──────────────┴──────────────┘

  count corpus.ttl · turtle · 20.40 MiB · macos-aarch64
  wall 425.3ms (input → parse; excludes startup and fingerprinting) · 75μs unattributed
  960000 triples · 2.26M/s · 480001 grammar statements · peak RSS 39.36 MiB
  sink: below the measurement floor — under 96ns per call across 2400104 calls,
  which is where the clock's own 32ns/call stops being separable from it
  profiler cost 0.144% of wall (38289 clock reads @ 32ns/pair)
  UNTRUSTED sink: its extrapolated clock artifact is 18.1% of wall — the sink
  figure is not a baseline; read, decompress and parse are unaffected
```

That is a **release build** — a debug binary is roughly 20× slower and would
calibrate expectations badly. Four things in the report are worth
understanding.

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

**The profiler prices its own instrument, and gives two verdicts.** It
measures what a clock read costs on this host, then reports both what the
reads it actually took cost the run (`measured_overhead_pct` → `phases_trusted`)
and what the clock artifact carried by a *scaled* sink estimate would come to
(`estimator_artifact_pct` → `sink_trusted`). Either exceeding 2% of wall
prints an `UNTRUSTED` line naming that half. They are separate because the
second is false on essentially every `count` — a discard sink's artifact is a
large share of a fast parse — and one combined flag that is always false is a
flag nobody reads. **A regression gate should key on `phases_trusted`.**

**The sink estimate assumes a cooperating corpus.** The sampling schedule is a
deterministic function of the input, which is what makes two profiles of the
same corpus comparable — and also means someone who chooses the input can work
out which statements get timed and put the expensive work elsewhere. It
defends against the periodic structure that occurs naturally, not against a
corpus built to hide from it. `sink.relative_std_error_pct` bounds ordinary
sampling error; nothing here bounds a deliberate one.

`--profile=json` emits the same run as one JSON document on stderr, with
`schema` set to `fluree.rdf.profile.v1`:

```bash
fluree count dump.ttl --profile=json --no-hash 2> run.json
```

| Field | Meaning |
|---|---|
| `git_sha` | Commit the **binary was built from** — `git rev-parse` beside the executable, not in the working directory, so running it from another checkout cannot misattribute a baseline. `"unknown"` outside a checkout. |
| `host.host_class` | Comparability class; `FLUREE_BENCH_HOST_CLASS` or `{os}-{arch}`. Absolute timings may only be diffed between runs whose class matches — `{os}-{arch}` alone does not tell two cloud instance types apart. |
| `host.peak_rss_bytes` | Peak RSS, normalized across platforms. |
| `sink.body_ns` | Estimated per-event sink cost, or `null` when unresolvable. Never `0` for "unknown". |
| `sink.floor_ns_per_call` | The per-call cost the sink would have to exceed to be visible. |
| `sink.relative_std_error_pct` | Sampling error of the estimate; `null` under two samples. |
| `self_calibration.phases_trusted` | Whether the phase breakdown is baseline-grade. **Gate on this one.** |
| `self_calibration.sink_trusted` | Whether the sink estimate is baseline-grade. Usually false for `count`. |

The `sink` block is separate from `phases` because its most common honest
answer — "this could not be measured" — is not a duration, and an unresolved
sink gets no phase row at all rather than a zero one.

## Memory

Input is read into memory in one pass, so peak RSS scales with the input —
but not by a fixed multiple, because most of the excess is the parser's IRI
cache, and that grows with the number of *distinct* IRIs rather than with
bytes.

Release build, macOS/aarch64, 480k statements per corpus:

| Corpus | Input | Peak RSS | Ratio |
|---|---|---|---|
| `distinct.ttl` — every subject a fresh IRI | 22.2 MiB | 148.7 MiB | 6.7× |
| `reused.ttl` — 100 subjects, reused | 20.4 MiB | 39.3 MiB | 1.9× |

Both fixtures come from `scripts/rdf-rss-fixture.py`, so the figures
reproduce; expect them to move with IRI length and with corpus shape, which is
the point of quoting a range rather than a constant. At the 4 GiB input cap
this implies roughly 8–27 GB depending on shape, which is worth knowing before
pointing the tool at a large dump on a small machine. Streaming and chunked
reading land with the parallel conversion pipeline; until then, split large
inputs.

`--profile=json` reports `host.peak_rss_bytes` for the run in hand, normalized
across the Darwin-bytes / Linux-kilobytes split in `ru_maxrss`.

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
