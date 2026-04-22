# Fluree Memory

Persistent, searchable memory for AI coding assistants — built for real work.

Fluree Memory gives tools like Claude Code, Cursor, and VS Code Copilot a long-term project brain. Facts, decisions, and constraints are captured as structured memories, stored in a local Fluree ledger you control, and retrieved via ranked recall — either by the agent through MCP or directly from the CLI.

Because memories live in plain-text TTL files under your project (`.fluree-memory/repo.ttl` for the team, `.fluree-memory/.local/user.ttl` for you), they can be committed to git and shared across the team the same way code is. No cloud service, no opaque database, no data leaving your machine. Open the file, read it, grep it, diff it, review it in a PR.

## Design philosophy

We initiallly built Fluree Memory for us, with a goal to increase the velocity of development with LLMs, work seamlessly in a git workflow, and to reduce token usage -- in that order. We ended with a simple knowledge organization model (it started out more complex), and leaned into the speed and power of our knowledge graph database. We found most memory systems are designed for benchmarks or demos -- they optimize for recall scores on synthetic tasks, ship your data to a hosted service, or bury context in a format only the tool can read - often running LLMs over git hooks or conversation turns that can burn more tokens than your actual coding session.

Fluree Memory has been refined by running it daily across real repositories — a 37-crate Rust workspace, multi-service TypeScript apps, real teams — and iterating on what actually gets used. The schema started with five memory kinds, four sensitivity levels, six sub-type fields, and bi-temporal validity. Usage data showed that 85% of memories were facts, "architecture" covered 81% of sub-types, and most optional fields were never set. So we simplified. Three kinds. Tags instead of sub-type taxonomies. Scope instead of a redundant sensitivity axis. Fewer decisions for the agent to make on every save means more saves actually happen.

The principles that came out of this:

- **Your repo, your data.** Memories are local Turtle (TTL) files. They live alongside your code, flow through your existing review and version control, and never leave your infrastructure. There is no hosted component, no account, no telemetry.
- **Visible and auditable.** Every memory is a block of Turtle you can read in any text editor. `git diff` shows exactly what changed. `git blame` shows who (or what) added it. No black boxes.
- **Simple enough to actually use.** Three kinds — `fact`, `decision`, `constraint` — cover the real-world space. If a model has to deliberate over a five-way kind taxonomy plus sub-types on every save, it won't save. A system that gets used at 80% fidelity beats one that's theoretically perfect but sits idle.
- **Recalled, not regurgitated.** Seach with metadata re-ranking (tags, branch affinity, recency) pulls what's relevant to the current task. The agent gets a handful of targeted memories, not a dump of everything that was ever stored.
- **Optimized for context tokens.** Terse output, scoring thresholds, and explicit instructions of pagination telling the LLM whats next with enough context it can decide if useful to fetch more.
- **Iterated from production.** The schema, the recall ranking, the tool descriptions — all of it has been refined based on real agent behavior across real codebases. Features that earned usage stay. Features that didn't get cut.

## Why

Every AI coding session starts from zero. The model doesn't remember what was tried last week, which library the team chose and why, or the ten subtle gotchas that live in someone's head. You either re-explain each time, stuff it all into a `CLAUDE.md` / `AGENTS.md` that bloats context, or ship agents that repeat mistakes.

Fluree Memory is:

- **Structured**, not a wall of markdown. Memories have a kind (`fact`, `decision`, `constraint`), tags, scope, optional severity, rationale, and artifact references.
- **Recalled on demand** via BM25 keyword-scored search over memory content, with metadata-based re-ranking (tags, refs, kind, branch, recency). The agent pulls only what's relevant to the current task, keeping context small.
- **Versioned** via git — `update` modifies in place (same ID, only changed fields); `git log -p` shows the full history. Use `fluree create <name> --memory` to import git history into a time-travel-capable Fluree ledger.
- **Scoped** per-repo or per-user, so team knowledge stays shareable and personal preferences stay yours.
- **Local-first**, stored in `.fluree-memory/` as TTL — no cloud dependency, you own the data.
- **Secret-aware** — content is scanned on write against a set of known credential patterns, and matches are redacted automatically.

## Start here

- **New?** → [Quickstart](getting-started/quickstart.md) — install, init, store a memory, recall it.
- **Using Claude Code?** → [Set up Claude Code](getting-started/claude-code.md)
- **Using Cursor?** → [Set up Cursor](getting-started/cursor.md)
- **Want to understand the model?** → [What is a memory?](concepts/what-is-a-memory.md)
- **Looking for a command?** → [CLI reference](cli/README.md)

## How it fits

Fluree Memory is a feature of [Fluree DB](../docs/README.md) — installing the `fluree` CLI gives you both. If you only care about the memory tooling, you can still install and use Fluree as a single binary and never touch the rest of the database features.
