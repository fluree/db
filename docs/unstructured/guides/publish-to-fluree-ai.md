# Publish to Fluree AI

The ledger `fluree doc ingest` builds is an ordinary ledger, so the remote-sync commands move it into a Fluree AI account.

## Register the remote in the project

`publish` and `push` look for the remote in the project's own `.fluree/config.toml`. If you registered and logged in from your home directory for `doc.remote`, register in the project too:

```bash
cd my-corpus
fluree remote add acct https://<your-stack>/v1/fluree
fluree auth login --remote acct
```

## Publish

```bash
fluree publish acct contracts
  ✓ Created remote ledger 'contracts:main'
  Pushing 3 commit(s)…✓ Published 'contracts:main' to 'acct' (3 commit(s), remote head t=3)
  → upstream set to 'acct/contracts:main'
```

That creates the ledger on the stack and pushes every commit: the structure graph, the chunks, the embeddings and the document nodes. Later runs push only what is new:

```bash
fluree doc ingest ./contracts -l contracts   # locally, as before
fluree push contracts
```

Query the hosted copy with the `remote/ledger` form:

```bash
fluree query acct/contracts -e '{"@context":{"doc":"https://ns.flur.ee/doc#"},
  "where":[{"@id":"?c","@type":"doc:Chunk","doc:text":"?t"}],"select":["?c","?t"],"limit":3}'
```

## What does not travel

The two indexes are graph sources on your machine, not commits, so they are not pushed.

- The full-text index can be rebuilt on the stack with the same indexing query the ingest used:

  ```bash
  fluree bm25 create --remote acct --name contracts-text --ledger contracts:main -e '{
    "@context": {"doc": "https://ns.flur.ee/doc#"},
    "where": [{"@id": "?c", "@type": "doc:Chunk"}],
    "select": {"?c": ["@id", "doc:text", "doc:headerPath"]}
  }'
  ```

- A vector index cannot yet be created over the network. On the stack, similarity over the published `doc:embedding` values is available through the inline `cosineSimilarity` function in a query, and through Fluree AI's own retrieval over the ledger.

## Parsing straight into a hosted ledger

Not supported today: `fluree doc ingest` runs in-process against local storage. Two workable paths:

1. Ingest locally and publish, as above. This keeps retract-on-rerun and the caches.
2. `fluree doc ingest … --out-dir ./tx` writes each document's transaction as JSON-LD; `fluree insert --remote acct <ledger> -f ./tx/<file>.jsonld` loads it into a hosted ledger. Treat it as a one-time load: the automatic retraction of a previous extraction does not apply.
