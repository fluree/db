# CLI reference

`fluree doc` has four subcommands.

| Subcommand | Description |
|---|---|
| [`ingest`](ingest.md) | Parse documents into a ledger and build the indexes over them |
| [`search`](search.md) | Search a ledger's chunks by meaning (vector) or by words (full-text) |
| [`parse`](parse.md) | Parse one document and print its DoCO graph, without writing to a ledger |
| [`read`](read.md) | Read one document with an HTTP reader and print its reading, without writing to a ledger |

Configuration lives in the `[doc]` table of the project's `config.toml`; see [Configuration](../reference/configuration.md).
