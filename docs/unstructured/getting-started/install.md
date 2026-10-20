# Install

Fluree Unstructured is part of the Fluree CLI. There is nothing else to install: the parsing engine is compiled in, and the caches and ledger live under your project.

```bash
# Homebrew (macOS / Linux)
brew install fluree/tap/fluree

# or the shell installer (macOS / Linux)
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.sh | sh

fluree --version
```

On Windows, run `irm https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.ps1 | iex` in PowerShell. Pre-built binaries for every platform are on the [releases page](https://github.com/fluree/db/releases). The CLI is not published to crates.io, so `cargo install fluree-db-cli` does not work.

Then create a project directory. Everything `fluree doc` writes — the ledger, the indexes, the parse cache — lives under its `.fluree/`:

```bash
mkdir my-corpus && cd my-corpus
fluree init
```

The deterministic tier needs no model. To add one, see the [quickstart](quickstart.md) for a local endpoint or [connect a Fluree AI account](fluree-ai.md).
