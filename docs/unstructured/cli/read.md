# fluree doc read

Read one document with an HTTP reader (an extraction model reached over HTTP, configured as `[doc.reader]`) and print its reading as the reader wrote it: a Fluree DB transaction holding the run, the facts in the ontology's terms, the claims behind them and the mentions linked to catalog records. It is what [`ingest`](ingest.md) commits for that file, without a ledger.

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
| `--include <PARTS>` | What the reading holds besides the facts: `claims`, `mentions`, `claims,mentions` (default) or `none`. |
| `--format <FORMAT>` | `jsonld` (default): the transaction as the reader wrote it. `trig`: the same as TriG, the run in `<#txn-meta>`. `turtle`: its statements without the run. |
| `-o, --output <FILE>` | Write to this file instead of stdout; opened before the read, so a bad path fails before anything is sent. |
| `--base-iri <IRI>`, `--doc-iri <IRI>` | The document IRI, as for `parse`. |
| `--no-escalate`, `--max-crops <N>` | Vision-model escalation during the parse, as for `parse`. |
| `--no-cache` | Neither read nor write the parse and reading caches: the document is read afresh. |

Only the reading goes to stdout, so it can be piped or inserted as it is: the run becomes the commit's metadata. Stderr says which model read the file, the facts, claims, mentions, new entities and linked records, the calls made (and how many the cache answered), what the reader billed, and for each call its run, the release that read it and the reader's ID for the call. A file with no text is not sent. A document too long for one call is read in several, each its own run and its own transaction: as `jsonld` they need `-o FILE.jsonld` and go to a file per call (`FILE-1.jsonld`, `FILE-2.jsonld`, …, each of which `fluree insert -f` takes), and without `-o` the read is refused before anything is sent; `trig` prints them one after another.

## Offsets

`nif:beginIndex` and `nif:endIndex` index the text the parser made of the file, which `fluree doc parse --format text` prints, not the file's bytes. The reading doesn't repeat that text; the run's `xt:textSha256` is its SHA-256.

## Examples

```bash
# The reading, into a file
fluree doc read invoice.pdf --ontology invoices.ttl --model reader-small > invoice.jsonld

# Linked to a catalog, as Turtle
fluree doc read memo.docx --ontology onto.ttl --catalog people.ttl --format turtle

# As TriG, the run in <#txn-meta>
fluree doc read call.vtt --ontology onto.ttl --format trig -o call.trig

# The facts alone
fluree doc read memo.md --ontology onto.ttl --include none

# The reading into a ledger, as it is
fluree doc read memo.md --ontology onto.ttl -o memo.jsonld
fluree insert --ledger memos -f memo.jsonld
```

See [`fluree doc read`](../../cli/doc.md#fluree-doc-read) in the CLI reference for the reader's request and answer.
