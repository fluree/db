# fluree doc read

Read one document with an HTTP reader (an extraction model reached over HTTP, configured as `[doc.reader]`) and print its reading: mentions linked to catalog records, entities, and claims, in the shape [`ingest`](ingest.md) would commit for that file, without a ledger.

```bash
fluree doc read <FILE> --ontology <FILE|LEDGER> [OPTIONS]
```

The file is parsed as [`parse`](parse.md) parses it, then read with the same requests `ingest` sends: cut into calls when it is long, every call carrying the ontology and the catalog, a call resent only when the reader says it has no capacity yet or is rate-limiting (a 503 or 429 with `Retry-After`). No ledger is written. Files as ontology and catalog need no project; a ledger as either does.

The reader is the `[doc.reader]` of the project when there is one, and of the global config otherwise. `FLUREE_DOC_READER_URL`, `_API_KEY`, `_AUTH_HEADER`, `_MODELS` and `_CACHE` override its fields one by one, and with no config at all they are the reader.

## Arguments

| Argument | Description |
|---|---|
| `<FILE>` | One document, of any type `ingest` reads. |

## Options

| Option | Description |
|---|---|
| `--ontology <FILE\|LEDGER>` | Ontology to read against. Required. |
| `--entities <FILE\|LEDGER[#CLASS]>` (alias `--catalog`) | Known entities to link mentions to. Repeatable; `#Class` scopes to one type. |
| `--model <ID>` | The extraction model, one a `[doc.reader]` lists in `models`. Default: `doc.model`. |
| `--cache <ontology\|catalog\|off>` | What the reader may keep for the next request that shares it. |
| `--format <FORMAT>` | `jsonld` (default), `turtle`, or `trig`: the structure graph in `<doc>` and the reading in `<doc>/reading`. |
| `-o, --output <FILE>` | Write to this file instead of stdout; opened before the read, so a bad path fails before anything is sent. |
| `--base-iri <IRI>`, `--doc-iri <IRI>` | The document IRI, as for `parse`. |
| `--no-escalate`, `--max-crops <N>` | Vision-model escalation during the parse, as for `parse`. |
| `--no-cache` | Neither read nor write the parse and reading caches: the document is read afresh. |

Only the reading goes to stdout, so it can be piped or inserted as it is. Stderr says which model read the file, the nodes received and the mentions, entities and relations among them, the calls made (and how many the cache answered), and what the reader billed. A file with no text is not sent.

## Offsets

`nif:beginIndex` and `nif:endIndex` index the text the parser made of the file, which `fluree doc parse --format text` prints, not the file's bytes. With `--format trig`, that text is the document's `nif:isString` in the `<doc>` graph.

## Examples

```bash
# The reading, into a file
fluree doc read invoice.pdf --ontology invoices.ttl --model reader-small > invoice.jsonld

# Linked to a catalog, as Turtle
fluree doc read memo.docx --ontology onto.ttl --catalog people.ttl --format turtle

# Structure and reading together, each in its own named graph
fluree doc read call.vtt --ontology onto.ttl --format trig -o call.trig

# The reading into a ledger, as it is
fluree doc read memo.md --ontology onto.ttl -o memo.jsonld
fluree insert --ledger memos -f memo.jsonld
```

See [`fluree doc read`](../../cli/doc.md#fluree-doc-read) in the CLI reference for the reader's request and answer.
