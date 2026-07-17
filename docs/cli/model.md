# fluree model

Governance model tooling — declare what things are, who may do what, and how classes relate, as ordinary data in the target ledger.

A governance model has three facets:

| Facet | Subcommand | Compiles to |
|-------|------------|-------------|
| **Entity** (what things are) | `model entity` | SHACL node shapes |
| **Access** (who may do what) | `model access` | Thin class-targeted policies using write verbs |
| **Reasoning** (what follows) | `model class` | `rdfs:Class` / `rdfs:subClassOf` vocabulary |

The commands are **compilers to data**: they transform declared intent into ordinary JSON-LD transactions and queries against the target ledger. There is no bespoke server API behind them, so they work identically against local ledgers, `fluree-db-server`, and hosted stacks. There is no stored intent language and nothing to sync — the compiled artifacts are small, self-describing, and hand-editing them afterward is legitimate authorship, not drift.

## Usage

```bash
fluree model <FACET> <COMMAND> [OPTIONS]
```

All write commands accept `--dry-run` (print the compiled JSON-LD without transacting) and `--remote <r>` (run against a configured remote instead of the local ledger).

## fluree model access

Compile an access profile into policies on a dataset.

```bash
fluree model access enable <DATASET> --profile <read|write|intake> --entity <IRI> [OPTIONS]
fluree model access show <DATASET>
```

Profiles map intent onto the policy engine's write verbs (`f:create` / `f:update` / `f:delete`):

| Profile | Grants | Compiled policies |
|---------|--------|-------------------|
| `read` | View instances of the class | View policy (`f:onClass` + `f:allow`, or an `f:query` relationship gate) |
| `write` | Full instance ownership: view + create/update/delete | The view policy plus a write policy carrying `f:action [create, update, delete]` on the class |
| `intake` | Submit without read-back (the anonymous-form shape) | A create-only policy |

Verb semantics make the class grant exact: class targeting matches pre ∪ post state, and `rdf:type` writes match by the class they mint, so a Lead grant can never create a Contract. No property allow-list is needed — the property *surface* of a class belongs to its SHACL shape (`model entity define --closed`); validation owns "what a valid X looks like", policy owns "who may cause which state transitions".

### Options

| Option | Description |
|--------|-------------|
| `--profile <p>` | `read`, `write`, or `intake` |
| `--entity <iri>` | Entity class IRI (absolute) |
| `--property <iri>` | Narrow the **write** policy to a column set (`f:onProperty` conjunction; repeatable) — "may edit status of Leads, nothing else" |
| `--connected <path>` | Relationship gate (**read** profile only): a SPARQL property path from the requesting identity to the entity, e.g. `"^<https://example.org/owner>"` (I see what I own). Stored verbatim in the policy via the engine's `@path` context term |
| `--class-iri <iri>` | Policy class IRI override (default `{entity}/access/{profile}`) |
| `--space <id>` | Attach the policy class to this space's grant on the dataset (hosted stacks; requires `--remote`). Merges with existing grant classes, never clobbers |
| `--dry-run` | Print the compiled JSON-LD without transacting |

### Semantics of re-running

`enable` **replaces** the two policy nodes it owns (`{class}/view`, `{class}/write`) atomically: one transaction wildcard-deletes both ids and inserts the fresh compilation. A property from a previous run cannot linger (the policy loader gives `f:allow` precedence over `f:query`, so a stale `f:allow: true` would silently disable a newly added `--connected` gate), and profile switches on the same class are exact — switching `write` → `read` revokes `{class}/write`.

### Examples

```bash
# Apps may fully own Lead instances
fluree model access enable crm --profile write --entity https://example.org/Lead

# Column-narrowed write: may edit status of Leads, nothing else
fluree model access enable crm --profile write --entity https://example.org/Lead \
  --property https://example.org/status

# Relationship-gated read: I see Leads whose team I'm a member of
fluree model access enable crm --profile read --entity https://example.org/Lead \
  --connected "<https://example.org/memberOf>/^<https://example.org/team>"

# Hosted stack: transact the policies AND attach the class to a space grant
fluree model access enable crm --profile write --entity https://example.org/Lead \
  --space 01hx... --remote prod
```

## fluree model entity

Define an entity as a SHACL node shape — the single source of truth for the property surface of a class.

```bash
fluree model entity define <DATASET> --entity <IRI> --property "<spec>" [--property "<spec>"...] [OPTIONS]
fluree model entity show <DATASET>
```

Each `--property` spec is `"<iri> [type] [required] [in[v1,v2,...]]"` where type is one of `string`, `integer`, `decimal`, `boolean`, `date`, `datetime`, `iri` (omitted = untyped).

| Option | Description |
|--------|-------------|
| `--entity <iri>` | Entity class IRI (absolute) |
| `--property "<spec>"` | Property spec (repeatable, at least one) |
| `--label <text>` | Human label for the class |
| `--closed` | Closed shape: instances may carry ONLY the declared properties (`rdf:type` excepted). Recommended for app-writable entities |
| `--dry-run` | Print the compiled JSON-LD without transacting |

> **Enforcement note:** Fluree runs SHACL at transaction time once any shapes exist in a ledger (reject mode by default). Defining an entity is not just documentation — it activates validation for its class. Existing data is not retro-validated; `fluree validate` produces a full report.

Re-running `define` replaces the shape and its property shapes atomically — constraints from a previous definition (`sh:closed`, `sh:minCount`, `sh:in`, …) do not survive a re-run that dropped them. The entity class node itself is shared authorship with `model class define` and stays additive.

```bash
fluree model entity define crm --entity https://example.org/Lead --closed \
  --property "https://example.org/name string required" \
  --property "https://example.org/status string in[new,qualified,won,lost]" \
  --property "https://example.org/owner iri"
```

## fluree model class

Define classes and their `rdfs:subClassOf` hierarchy.

```bash
fluree model class define <DATASET> --class <IRI> [--subclass-of <IRI>...] [--label <text>] [OPTIONS]
fluree model class show <DATASET>
```

When RDFS entailment is enabled on a dataset, the engine follows the hierarchy in **both query and policy**: a view policy on `ex:Contact` covers `ex:Lead` if `ex:Lead rdfs:subClassOf ex:Contact`. A widened hierarchy widens every grant on the parent — which is why the hierarchy lives under governance tooling and not ad-hoc transacts.

Re-running `define` with `--subclass-of` replaces the parent set (list every parent each time). `show` lists domain classes only — policy classes minted by `model access` are excluded.

```bash
fluree model class define crm --class https://example.org/Lead \
  --subclass-of https://example.org/Contact --label "Lead"
```
