# fluree rdf check

Parse an RDF document and report what is wrong with it. No ledger is
involved: this is a parse and an exit code.

```bash
fluree rdf check [<FILE>] [options]
```

```bash
fluree rdf check dump.ttl
fluree rdf check dump.nt.gz
cat dump.ttl | fluree rdf check --syntax turtle
fluree rdf check dump.ttl --format json
```

## Output

A clean document says so and exits 0:

```console
$ fluree rdf check dump.ttl
ok: dump.ttl: no syntax errors
```

A broken one points at the problem and exits 1:

```console
$ fluree rdf check broken.ttl
error: broken.ttl:3:16: unexpected character '?'
  ex:bob ex:name ?? .
                 ^
```

Both go to **stderr**, which is where `riot` and [`fluree rdf count`](count.md)
put theirs — a `check` that wrote its diagnostics to stdout would be the one
verb in the group whose failure output disappears into a redirect. Only
`--format json` writes to stdout, because that one is a document a script
consumes rather than a message to a person.

Under `-q` the `ok:` line is suppressed and the exit code is the whole
answer — which is what a loop over ten thousand files wants.

## Options

| Option | Description |
|--------|-------------|
| `--format <fmt>` | `table` (default, human) or `json` |
| `--syntax <SYNTAX>` | Input syntax, overriding extension and sniffing |
| `--base <IRI>` | Base IRI for resolving relative references |
| `--time` | Print elapsed time and throughput to stderr |
| `--profile[=FORMAT]` | Per-phase timing breakdown (`human` or `json`) |
| `--no-hash` | Skip the corpus SHA-256 in `--profile` output |

See [the `rdf` overview](README.md) for input handling, syntax resolution,
and profiling, which are shared by every verb.

## JSON output

`--format json` writes one document to stdout:

```json
{
  "schema": "fluree.rdf.check.v1",
  "input": "broken.ttl",
  "syntax": "turtle",
  "ok": false,
  "grammar_statements": 2,
  "diagnostics": [
    {
      "severity": "error",
      "offset": 78,
      "line": 3,
      "column": 16,
      "message": "unexpected character '?'",
      "snippet": "ex:bob ex:name ?? ."
    }
  ]
}
```

`grammar_statements` counts what parsed before the failure, so it says where
in the document the trouble started, in statements rather than bytes. It is
Turtle's `statement ::= directive | triples '.'` — a `@prefix` line counts as
one, and a statement with a predicate-object list also counts as one while
emitting several triples. It is deliberately not called "statements", because
it is not the triple count.

`message` is always a single line. The lexer's own messages arrive as a
pre-rendered block with their own numbered excerpt, their own caret, and the
position repeated in the text; everything after the first line is dropped,
along with the trailing position clause, because `line` and `column` carry
that already.

Location fields are omitted, rather than null, for the errors that carry no
position (an undefined prefix, a relative IRI with no base).

`diagnostics` is always an array, empty on a clean document — a consumer can
read its length without first checking whether the key exists.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | The document parsed |
| `1` | The document did not parse; diagnostics are on stdout |
| `2` | The invocation was wrong: no such file, unreadable, unknown syntax |

## Current limits

The parser stops at the first error, so at most one diagnostic is reported
per run. `--continue-on-error`, which resynchronizes at the next statement
boundary and reports every problem in one pass, lands with the diagnostics
work in the conversion pipeline. It requires statement-scoped output
buffering to be correct — a Turtle statement emits triples during descent,
before its terminating `.` proves it well-formed — which is why it is not a
flag that could simply be added here.
