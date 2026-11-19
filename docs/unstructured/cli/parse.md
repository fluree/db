# fluree doc parse

Parse one document and print its DoCO structure graph: what [`ingest`](ingest.md) would write for that file, without chunks, embeddings or a ledger.

```bash
fluree doc parse <FILE> [OPTIONS]
```

No ledger is written, and no project is needed: it runs in any directory. Where there is a project, or a global Fluree config, its `[doc.vlm]` slot (`[doc.llm]` failing that) reads the pages and regions the deterministic pass could not, and the parse is cached there, as for `ingest`. `FLUREE_DOC_VLM_*` and `FLUREE_DOC_LLM_*` configure a model anywhere, with or without either. With none of these, the parse is deterministic and makes no network connection; `--no-escalate` rules out a model call whatever is configured.

## Arguments

| Argument | Description |
|---|---|
| `<FILE>` | One document, of any type `ingest` reads: PDF, Markdown and text, HTML, DOCX, PPTX, XLSX, WebVTT and SubRip transcripts, email (`.eml`, `.msg`), or an image (read only by a vision model). |

## Options

| Option | Description |
|---|---|
| `--format <FORMAT>` | `doco` (default): the structure graph as JSON-LD. `text`: the plain-text projection that `nif:beginIndex` / `nif:endIndex` index into. |
| `-o, --output <FILE>` | Write to this file instead of stdout. |
| `--base-iri <IRI>` | Prefix the file name is appended to, forming the document IRI (default `urn:fluree:doc:`), as `ingest` does for a file named on its command line. |
| `--doc-iri <IRI>` | The document IRI in full, in place of `--base-iri` and the file name. Every node is minted under it and stamped with it. |
| `--no-escalate` | Never call a vision model, whatever is configured. |
| `--max-crops <N>` | Most crops the document may send to the vision model (default `70`). Over it, the deterministic reading is printed and a warning says so. |
| `--no-cache` | Neither read nor write the parse and reading caches. |

Warnings go to stderr, so they never mix with the graph: pages nothing could read, crops over the cap, and an email's attachments, which the graph describes but does not read.

## Examples

```bash
# The graph, into a file
fluree doc parse report.pdf > report.jsonld

# The text the graph's offsets index into
fluree doc parse call.vtt --format text

# Under your own IRI, then into a ledger as it is
fluree doc parse memo.docx --doc-iri https://example.org/doc/memo -o memo.jsonld
fluree insert docs -f memo.jsonld
```

The output is the parsing engine's own JSON-LD, with its context, so it inserts as it is. What it holds is described in [What gets built](../concepts/what-gets-built.md#the-structure-graph).
