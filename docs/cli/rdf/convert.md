# fluree rdf convert

Convert a document from one RDF syntax to another.

**Not yet implemented.** No serializers have landed. The verb is registered
so the surface it will occupy is fixed — the flags below are final, and what
is missing is the writers.

```bash
fluree rdf convert [<FILE>] [--to <SYNTAX>] [-o <FILE>] [options]
```

Running it today reports what is missing rather than "not implemented":

```console
$ fluree rdf convert dump.ttl --to nquads
error: `fluree rdf convert` cannot write nquads yet — no serializers have landed.
  help: `fluree rdf check` and `fluree rdf count` work today; writers for
  turtle, ntriples, nquads, trig, and jsonld land with the conversion pipeline
```

## Options

| Option | Description |
|--------|-------------|
| `--to <SYNTAX>` | Output syntax. Defaults to `nquads` |
| `-o, --output <FILE>` | Write to a file instead of stdout |
| `--pretty` | Group and indent Turtle output |
| `--syntax <SYNTAX>` | Input syntax, overriding extension and sniffing |
| `--base <IRI>` | Base IRI for resolving relative references |
| `--time` | Print elapsed time and throughput to stderr |
| `--profile[=FORMAT]` | Per-phase timing breakdown (`human` or `json`) |

See [the `rdf` overview](README.md) for input handling and syntax
resolution, which are shared by every verb and work today.

## Decisions already made

**The default output syntax is N-Quads**, matching `riot`. It is the only
syntax in the set that holds any dataset without loss and needs no context,
which makes it the right thing to get when you did not say what you wanted.

**Binary output never goes to a terminal.** `--to jelly` with stdout attached
to a TTY is refused, with instructions to redirect or pass `-o` — the same
guard [`fluree export`](../export.md) applies to `.flpack` archives. This
check is already live: it runs before the not-yet-implemented refusal, so a
user learns about it either way.

**`--pretty` is a documented trade, not a formatting whim.** Default Turtle
output is streaming: consecutive statements about the same subject fold into
`;` and `,` groups, but a subject that reappears later in the document is not
regrouped with its earlier self, because doing so requires holding the whole
graph. `--pretty` opts into buffering and says so — the same distinction
`riot` exposes. It is rejected for syntaxes that have no pretty form.
