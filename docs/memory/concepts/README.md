# Concepts

Short, self-contained explanations of the ideas behind Fluree Memory. Read these once; they'll save you time when you're reading CLI reference or wiring a new IDE.

- [What is a memory?](what-is-a-memory.md) — the three kinds (`fact`, `decision`, `constraint`) and when to use each.
- [Repo vs user memory](repo-vs-user.md) — how scope decides whether a memory ends up in `repo.ttl` (shared with the team) or `.local/user.ttl` (yours).
- [Updates and forgetting](supersession.md) — how `update` modifies memories in place and how history is tracked via git.
- [Recall and ranking](recall-and-ranking.md) — how BM25 scores results and how tag / kind filters narrow them.
- [MCP server](mcp.md) — the tools Fluree Memory exposes to AI agents.
- [Secrets and sensitivity](secrets-and-sensitivity.md) — automatic redaction and scope-based privacy.
