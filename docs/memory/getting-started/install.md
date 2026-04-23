# Install Fluree

Fluree Memory ships as part of the `fluree` CLI. Install the binary once and you have both the database and the memory tooling.

## macOS / Linux (installer script)

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.sh | sh
```

## Homebrew (macOS / Linux)

```bash
brew install fluree/tap/fluree
```

## PowerShell (Windows)

Open PowerShell and run:

```powershell
irm https://github.com/fluree/db/releases/latest/download/fluree-db-cli-installer.ps1 | iex
```

Open a **new** PowerShell session and verify with `fluree --version`. The binary is unsigned, so Windows SmartScreen may prompt on first run — click **More info → Run anyway**.

## Pre-built binary

```bash
# Linux x86_64
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-x86_64-unknown-linux-gnu.tar.xz | tar xJ

# macOS aarch64
curl -L https://github.com/fluree/db/releases/latest/download/fluree-db-cli-aarch64-apple-darwin.tar.xz | tar xJ
```

## Build from source

If you have Rust installed:

```bash
git clone https://github.com/fluree/db
cd db
cargo install --path fluree-db-cli
```

## Verify

```bash
fluree --version
fluree memory --help
```

You should see a list of `memory` subcommands: `init`, `add`, `recall`, `update`, `forget`, `status`, `export`, `import`, `mcp-install`.

Next: [quickstart](quickstart.md).
