# Fluree DB (Rust) Documentation

This `docs/` directory is the documentation “source of truth” that lives alongside the code and evolves with it.

- Start with `SUMMARY.md` as the navigation spine.
- Add pages incrementally; the structure is designed to work well with future tooling (mdBook, Zola, Docusaurus, etc.).

## Suggested authoring conventions

- One topic per file.
- Prefer stable, capability-based names (e.g. `query/sparql.md`) over version- or implementation-specific names.
- Use relative links within `docs/` so the content renders well on GitHub and in doc-site generators later.

