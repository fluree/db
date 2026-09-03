# Re-runs and caching

Running `fluree doc ingest` over the same folder again is meant to be the normal way to work: add a file, fix a scan, switch embedding models, upgrade the CLI, run it again.

## Unchanged documents are skipped

The document node records the file's SHA-256, the parser revision and the embedding model. When all three match, the document is reported as `unchanged` and nothing is parsed, embedded or written. `--force` overrides this.

## Changed documents are replaced, not diffed

A document whose bytes, parser or embedding model changed is re-extracted, and the previous extraction is retracted first: everything stamped with its `doc:sourceDocument`, and the document node itself. Then the new graph is inserted. The ledger holds exactly one extraction of each document at its head, and because Fluree is immutable the earlier one remains queryable at its commit — history without maintaining it. Element IRIs are minted in emission order and a re-extraction may shift them, which is why cross-references should target the document or a chunk's text rather than an element number.

## Two caches

Under `.fluree/cache/doc/`, keyed so that a re-run pays only for what actually changed:

- **Parse cache** — keyed on the file's content hash plus a fingerprint of everything that shapes the output: parser revision, document IRI, the vision model (if any) and the crop cap. A re-run over an unchanged folder parses nothing.
- **Reading cache** — keyed on the crop's pixels, the prompt and the model, not on the document. A parser upgrade re-routes pages, but a crop whose pixels did not change is answered without a model call. This is where the money is.

`--no-cache` bypasses both. Deleting the directory is always safe.

## Indexes follow

After the documents, the full-text index is created or synced incrementally. There is no vector index to keep in step: embeddings live on the chunks, so switching embedding models re-embeds the documents and search picks up the new vectors on the next query.
