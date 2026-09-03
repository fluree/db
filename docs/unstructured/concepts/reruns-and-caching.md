# Re-runs and caching

Running `fluree doc ingest` over the same folder again is meant to be the normal way to work: add a file, fix a scan, switch embedding models, upgrade the CLI, run it again.

## Unchanged documents are skipped

The document node records the file's SHA-256, the parser revision, the chunk sizes, the embedding model and, when extraction ran, a fingerprint of the ontology, the `--entities` sources, the language model, the guidance and the relation mode. When all of them match, the document is reported as `unchanged` and nothing is parsed, embedded, extracted or written. `--force` overrides this.

## Changed documents are replaced, not diffed

A document whose bytes, parser or embedding model changed is re-extracted, and the previous extraction is retracted first: everything stamped with its `doc:sourceDocument`, and the document node itself. Then the new graph is inserted. Entity nodes minted by extraction are shared between documents and are not retracted; an edge an earlier extraction asserted is kept only while some other relation still supports it. The ledger holds exactly one extraction of each document at its head, and because Fluree is immutable the earlier one remains queryable at its commit — history without maintaining it. Element IRIs are minted in emission order and a re-extraction may shift them, which is why cross-references should target the document or a chunk's text rather than an element number.

## Three caches

Under `.fluree/cache/doc/`, keyed so that a re-run pays only for what actually changed:

- **Parse cache** — keyed on the file's content hash plus a fingerprint of everything that shapes the output: parser revision, document IRI, the vision model (if any) and the crop cap. A re-run over an unchanged folder parses nothing.
- **Reading cache** — keyed on the crop's pixels, the prompt and the model, not on the document. A parser upgrade re-routes pages, but a crop whose pixels did not change is answered without a model call. This is where the money is.
- **Extraction cache** — keyed on the exact ask: the model, the ontology and guidance, the known entities found in the chunk, and the chunk's text. Changing the ontology invalidates exactly the language-model stage; a re-run into a fresh ledger with the same inputs makes no calls.

`--no-cache` bypasses all three. Deleting the directory is always safe.

## Indexes follow

After the documents, the ledger's own binary index is brought up to the new head. A CLI process never runs the background indexer a server would, and without this every later invocation, `doc search` included, would replay the commits just written into memory before answering: on a 66-page statement that was two seconds per command against a few milliseconds indexed. Then the full-text index is created or synced incrementally, and the vector index likewise. Switching embedding models changes the vector width; an index built for the old width is dropped and rebuilt rather than synced.
