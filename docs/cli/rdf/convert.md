# fluree rdf convert

Convert an RDF document from one syntax to another. No ledger, no `.fluree/`,
no connection — a file (or stdin) goes in and a file (or stdout) comes out.

```bash
fluree rdf convert [<FILE>] [--to <SYNTAX>] [-o <FILE>] [options]
```

```bash
fluree rdf convert dump.ttl --to nt > dump.nt
fluree rdf convert dump.ttl -o dump.jsonld
fluree rdf convert dump.nt.gz --to turtle --prefixes ctx.json
cat dump.ttl | fluree rdf convert --syntax turtle --to nquads
```

Conversion into the four text syntaxes is streaming: the parser emits into the
writer and bytes leave as they are produced. Nothing materializes the document,
so `convert big.ttl --to nt | head -5` costs five statements rather than a full
parse, and memory stays flat over a large dump.

**JSON-LD is the exception, and it is not a small one.** The writer is
document-at-once by construction — the format has no streaming form short of
NDJSON, which is deferred — so the whole graph is held in memory and nothing
reaches the output until the input is exhausted. Two consequences worth
planning around:

- **Memory scales with the document.** Measured on a 9.6 MiB Turtle corpus of
  200k statements, one distinct subject each: peak RSS 871 MiB for JSON-LD
  against 81 MiB for N-Triples — **~96× the input, against ~9×**. On a large
  dump that is the difference between a conversion and an OOM.

  Reproduce it with the corpus `scripts/rdf-rss-fixture.py` already generates
  for the [peak-RSS table](README.md#memory) — `distinct.ttl` is the same
  shape, scaled up — and read the figure back out of the profile:

  ```bash
  python3 scripts/rdf-rss-fixture.py /tmp
  for to in nt jsonld; do
    fluree rdf convert /tmp/distinct.ttl --to $to -o /tmp/out.$to \
      --profile=json --no-hash 2>&1 >/dev/null \
      | jq -r "\"$to: \(.host.peak_rss_bytes) bytes RSS for \(.corpus.bytes_decoded) in\""
  done
  ```

  On that 22 MiB fixture a release build reports `nt: 197 MiB … = 9x` and
  `jsonld: 2100 MiB … = 95x`. Expect the **ratio** to reproduce rather than
  the absolute numbers — peak RSS moves with allocator, platform and build
  profile, and every figure here is a release build on macOS/aarch64.
- **No early exit.** `convert big.ttl --to jsonld | head -5` parses the entire
  input before writing anything, so it costs a full conversion. The pipe trick
  only works for the streaming syntaxes.

This is a documented property of the format, not a defect to be fixed here —
the same call `riot` makes, and the same one RDF/XML will make when it lands.
See the writer module's own notes in `fluree-graph-format::writer`.

## Formats

| In | Out | Streaming |
|---|---|---|
| Turtle, N-Triples | Turtle | yes, O(1) in statements |
| Turtle, N-Triples | N-Triples, N-Quads, TriG | yes, O(1) in statements |
| Turtle, N-Triples | JSON-LD | **no — buffers the whole document** |

N-Triples is a subset of the Turtle grammar and is read by the same parser.
N-Quads and TriG **input** waits on the quad parsers; JSON-LD input goes
through `fluree insert`, not this path yet. RDF/XML, RDF/JSON and Jelly have
neither reader nor writer yet, and naming one produces a refusal that says so.

```console
$ fluree rdf convert dump.ttl --to rdfxml
error: cannot write rdfxml yet — the RDF/XML writer lands with the XML family
  help: writable today: turtle, ntriples, nquads, trig, jsonld
```

## Choosing the output syntax

First rule that answers wins:

1. `--to <SYNTAX>`
2. The extension of `-o <FILE>` (`out.ttl` → Turtle)
3. **N-Quads**, matching `riot` — the only syntax in the set that holds any
   dataset without loss and needs no context, which makes it the right thing
   to get when you did not say.

```console
$ fluree rdf convert dump.ttl
<http://example.org/alice> <http://example.org/name> "Alice" .
<http://example.org/alice> <http://example.org/knows> <http://example.org/bob> .
<http://example.org/bob> <http://example.org/name> "Bob"@en .
```

## Options

| Option | Description |
|--------|-------------|
| `--to <SYNTAX>` | Output syntax. Overrides the `-o` extension |
| `-o, --output <FILE>` | Write to a file instead of stdout |
| `--bnode-policy <POLICY>` | `relabel` (default) or `preserve` |
| `--prefixes <JSON\|PATH>` | Prefixes for compaction — inline JSON or a file. Namespaces must be absolute IRIs |
| `--continue-on-error` | Skip unparseable statements; report each and exit 1 |
| `--parallelism <N>` | Parse threads (global flag). `1` is the serial path exactly |
| `--pretty` | Buffered, regrouped Turtle. **Not implemented** |
| `--syntax <SYNTAX>` | Input syntax, overriding extension and sniffing |
| `--base <IRI>` | Base IRI for resolving relative references |
| `--time` | Print elapsed time and throughput to stderr |
| `--profile[=FORMAT]` | Per-phase timing breakdown (`human` or `json`) |

See [the `rdf` overview](README.md) for input handling, compression and syntax
resolution, which are shared by every verb.

## Fidelity

Turtle and TriG output is the streaming **blocks** tier: consecutive runs of
the same subject fold with `;` and of the same subject-and-predicate with `,`,
and document order is otherwise preserved. A subject that recurs later in the
document is *not* regrouped with its earlier self, `[ … ]` anonymous-node
syntax is not reconstructed, and an `rdf:first`/`rdf:rest` spine is not
re-collapsed into `( … )`. Those need the whole graph in memory; `riot` draws
the same line. `--pretty` is reserved for that tier and currently refuses,
rather than quietly giving you blocks-tier output under a flag that promised
something else.

Collections are read as the `rdf:first`/`rdf:rest` spine RDF says they are,
and an empty `()` as `rdf:nil` — not as the flattened list items Fluree's
ingest path stores. That is not configurable here: an indexed list item is a
storage shape with no RDF serialization, and every writer refuses one.

## Blank nodes

`--bnode-policy relabel` (the default) mints a fresh `_:b1`, `_:b2`, … for
every blank node, bijectively — one output label per distinct input label, so
no two nodes merge and none splits. This is what `riot` and Oxigraph do, and
it is what makes output deterministic.

Fluree's own `_:fdb-…` stable identifiers pass through verbatim under either
policy, so `fluree export | fluree rdf convert` keeps working; they are
addressable identifiers rather than incidental syntax.

`--bnode-policy preserve` emits the input's labels unchanged wherever they are
legal to emit. Labels that no RDF parser would have accepted — internal mints
from R2RML, JSON-LD or the IR itself — are relabelled into the reserved
`_:fdbw-` namespace regardless, because emitting one would produce a document
that either fails to parse or, worse, parses as something else.

That reservation has a consequence you can hit: under `preserve`, an input
that already contains a `_:fdbw-…` label is **refused**, because preserving it
could merge a user's node with one the writer minted. The error names the label
and the remedy:

```console
$ fluree rdf convert collide.ttl --to nt --bnode-policy preserve
error: the output is incomplete — the writer refused an event: blank-node label
  `fdbw-1` is inside the `_:fdbw-` namespace this writer reserves for anonymous
  nodes; preserving it could merge it with a minted node. Relabel instead of preserving.
  help: convert with --bnode-policy relabel, which renames every blank node and
  cannot collide
```

`relabel` — the default — has no such failure mode, because it renames
everything except the `_:fdb-` carve-out.

## Prefixes

Prefixes declared by the input are always carried into Turtle and TriG output.
`--prefixes` adds more, as inline JSON or a path to a JSON file; a JSON-LD
`@context` document works unchanged, with or without the `@context` wrapper.

```console
$ fluree rdf convert dump.nt --to turtle --prefixes '{"ex":"http://example.org/"}'
@prefix ex: <http://example.org/> .

ex:alice
    ex:name "Alice" ;
    ex:knows ex:bob .
ex:bob
    ex:name "Bob"@en .
```

For JSON-LD output the same map becomes the document's `@context`, so one flag
means one thing whichever syntax you asked for.

Every namespace must be an absolute IRI. A relative or malformed one is
refused before anything is written, rather than producing a document this
tool's own reader would reject:

```console
$ fluree rdf convert dump.ttl --to turtle --prefixes '{"ok":"not an iri"}'
error: --prefixes: namespace for 'ok' is not an absolute IRI: 'not an iri'
  help: a namespace needs a scheme, like "http://example.org/" — a relative or
  malformed one produces a document no RDF reader accepts
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Converted — and also a closed downstream pipe, which is how `\| head` ends |
| `1` | The input document did not parse |
| `2` | The invocation or destination was wrong: no such file, unwritable path, a syntax with no writer |

### What a failure does to `-o`

Two cases, and the difference is when the problem becomes knowable.

A refusal that needs **no input at all** — an output syntax with no writer, a
malformed `--prefixes` namespace, `--pretty` — is raised before the destination
is opened, so the file `-o` names is left exactly as it was. That is the only
promise available here, and it is deliberately narrow.

Anything discovered **mid-stream** — a parse error, or a writer refusing an
event — happens after `-o` was created and after bytes were written to it.
Those bytes cannot be recalled: `-o` has already been truncated and now holds a
prefix of the conversion. A streaming converter cannot honestly promise
otherwise; writing to a temporary file and renaming on success would trade
that away for double the disk and no streaming to the final path.

The error says which you got:

```console
$ fluree rdf convert broken.ttl --to nt -o out.nt
error: broken.ttl:3:16: unexpected character '?'
  wrote 1 statement(s) before the document stopped parsing — the output is a
  prefix of the conversion, not the whole of it
```

The same applies to a blank-node collision under `--bnode-policy preserve`:
the refusal arrives mid-parse, so `-o` is already partial when it does.

### Parallelism

`--parallelism <N>` (the global flag) parses across threads. `1` is the serial
path exactly, so the flag is never a correctness decision; `0` — the default —
uses as many threads as the host reports.

```bash
fluree rdf convert big.ttl --to nt -o big.nt --parallelism 8
```

The document is cut at statement boundaries, each worker parses its chunk and
writes its own bytes, and the fragments are concatenated in order. Every text
syntax participates; JSON-LD does not, because it is document-at-once and there
are no fragments to concatenate.

**Parallel and serial output are equivalent, not identical.** Blank-node labels
are assigned by a deterministic function of (label, chunk) so that workers need
no coordination, and that does not produce the same labels a single serial pass
does. What is guaranteed:

- the same input at the same `--parallelism` is **byte-identical across runs**
  — thread scheduling cannot reach the output;
- serial and parallel denote the **same graph**: same triples, same number of
  distinct blank nodes, and a blank node named in two chunks is still one node.

`riot` makes no cross-mode byte promise either. If you need byte-stability
across a change of thread count, pin `--parallelism`.

Two smaller consequences worth knowing. In Turtle and TriG output, prefixes are
declared once — by the first chunk — and a subject whose statements straddle a
chunk boundary is written as two subject blocks rather than one. Both are valid
blocks-tier output; the tier already declines to regroup a subject that recurs
later in a document.

Parallelism trades memory for threads: in-flight chunks are bounded, so peak
usage is roughly `(threads + queue) × chunk output size` rather than the whole
output.

`--profile` reports `threads_used` and a `parallel_reason`, so a run that fell
back to serial says why. It also records the machine's 1-minute load average
next to the core count, and prints a `LOADED` line when the average exceeds the
core count — a duration measured on a contended machine is not a measurement,
and this is the only way to tell after the fact.

### `--continue-on-error`

Skip the statements that do not parse and keep the rest:

```console
$ fluree rdf convert partly-broken.ttl --to nt --continue-on-error > out.nt
skipped: partly-broken.ttl:3:11: unexpected character '?'
skipped: partly-broken.ttl:5:11: unexpected character '?'
warning: 2 statement(s) skipped, 3 written → stdout
```

**It still exits 1.** Skipping is not success — riot's rule, and the one thing
a script must not do is read a partial conversion as a whole one. A clean
document under the flag exits 0 and says nothing.

A skipped statement contributes **nothing**, not even the part of itself that
had already been written. Turtle emits during descent, so
`ex:bad ex:p "one" ; ex:q "two" ; ex:r ??` has two triples in flight before the
failure is known; the flag turns on statement buffering so the rollback is
real. Each skip is reported with its position in the original file.

Recovery is serial: resync needs to see the document as one sequence of
statements, so `--continue-on-error` and `--parallelism` do not combine — the
former wins, and `--profile` reports why.

The recovery point is the next statement boundary after the error. A directive
that gets skipped over as part of a bad statement is therefore lost, and the
statements needing it fail in turn — each reported, none silent.

## Compressed output

Not yet. `-o out.nt.gz` is refused rather than writing plain N-Triples into a
file whose name promises gzip:

```bash
fluree rdf convert in.ttl --to nt | gzip > out.nt.gz
```

Compressed *input* decompresses transparently, which is why the asymmetry gets
an error instead of a silent surprise.

## Profiling

`--profile` works here as on the other verbs, with two extra phases. `write`
is time spent in real output I/O, measured at buffer-flush granularity;
`serialize` is the rest of the sink cost — formatting terms into bytes —
derived by subtracting `write` from the sink estimate. Both are absent when
the sink estimate does not clear its measurement floor, rather than being
invented from a number that is not there.

```console
$ fluree rdf convert big.ttl --to nt -o big.nt --profile
  ┌───────────┬──────────────┬──────────────┐
  │ phase     │           ms │       % wall │
  ├───────────┼──────────────┼──────────────┤
  │ read      │         3.71 │         0.06 │
  │ parse     │      5846.32 │        99.74 │
  │ serialize │      1562.50 │        26.66 │
  │ write     │        12.90 │         0.22 │
  └───────────┴──────────────┴──────────────┘
```

`serialize` and `write` are nested inside `parse` — the writer runs during the
parse, not after it — so the shares deliberately do not sum to 100%. The
`sink` row that `check` and `count` show is replaced by its decomposition
here; the total is still reported in the `sink` block of `--profile=json`.
