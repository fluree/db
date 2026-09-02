# Install

Fluree Unstructured is part of the Fluree CLI. There is nothing else to install: the parsing engine is compiled in, and the caches and ledger live under your project.

```bash
curl -fsSL https://flur.ee/install.sh | sh     # or: cargo install fluree-db-cli
fluree --version
```

Then create a project directory. Everything `fluree doc` writes — the ledger, the indexes, the parse cache — lives under its `.fluree/`:

```bash
mkdir my-corpus && cd my-corpus
fluree init
```

The deterministic tier needs no model. To add one, see the [quickstart](quickstart.md) for a local endpoint or [connect a Fluree AI account](fluree-ai.md).
