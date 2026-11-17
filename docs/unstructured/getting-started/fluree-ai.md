# Connect a Fluree AI account

A Fluree AI account holds model access. Connect the CLI to it once and `fluree doc` needs no model configuration at all: crop reading, embeddings and extraction are routed through your account's gateway to whatever provider sits behind it, whether keys your organisation configured or Fluree's own proxy.

The pipeline still runs on your machine and the ledger is still local. Only the model calls go to Fluree AI. See [Models and calls](../concepts/models-and-calls.md) for exactly what is sent.

## 1. Get an account

A Fluree AI stack has a URL of its own. Open it and sign in with your organisation's single sign-on, or use **Create a new account** on the sign-in page. If you were invited, the invitation carries the URL.

## 2. Register the stack as a remote

Use the stack URL plus `/v1/fluree`. The CLI discovers the stack's OpenID Connect configuration by itself:

```bash
fluree remote add acct https://<your-stack>/v1/fluree
  info: auto-discovered OIDC auth from server
  Run `fluree auth login --remote acct` to authenticate
```

## 3. Log in

```bash
fluree auth login --remote acct
```

Your browser opens at the stack's activation page with a device code. Approve it there; the CLI is waiting and stores the login next to the remote. `fluree auth status --remote acct` shows what it holds. Logins expire after a few hours and are refreshed automatically when a command needs them; when the refresh itself has lapsed, log in again.

Where you run these matters. The remote and its login are written to the nearest `.fluree/config.toml`. Run them inside a project to keep them per project, or from your home directory to put them in `~/.fluree/config.toml`, which `doc.remote` also consults — so one login can serve every project. Note that the sync commands (`publish`, `push`) look only in the project's own config; see [Publish to Fluree AI](../guides/publish-to-fluree-ai.md).

## 4. Name it for `fluree doc`

```bash
fluree config set doc.remote acct
fluree doc ingest ./contracts -l contracts
```

```
ingest 3 document(s) → contracts
  account    acct (Fluree AI gateway supplies unset model slots)
  parser     fluree-doc-parse 407daa0
  escalation auto (crops the parser cannot read)
  embedding  text-embedding-3-small
```

`doc.remote` fills every model slot you have not set yourself. Any slot you set explicitly still wins, so a local embedding model can be paired with the account's vision model:

```toml
[doc]
remote = "acct"

[doc.embedding]
url = "http://localhost:11434/v1"
model = "nomic-embed-text"
```

## What the account does and does not do

- **Does:** answer model calls — reading crops of pages the parser could not, embedding chunk text, and (when enabled) extracting entities — with the account's models and billing.
- **Does not:** receive your documents, store your ledger, or run the extraction. Nothing is uploaded except the crops and chunk text a model call needs, and nothing is kept.

To move a built ledger into your account, [publish it](../guides/publish-to-fluree-ai.md).
