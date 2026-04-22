# Tutorial: Building a Knowledge Base with Fluree

This tutorial walks through a realistic scenario — building a team knowledge base — to show how Fluree's differentiating features work together. You'll use time travel, full-text search, branching, and access control in a single workflow.

**Time:** ~20 minutes
**Prerequisites:** Fluree installed and running (`fluree init && fluree server run`)

## Step 1: Create the ledger and add data

```bash
fluree create knowledge-base
fluree use knowledge-base
```

Insert some articles and team members:

```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .
@prefix f:      <https://ns.flur.ee/db#> .

ex:alice  a schema:Person ;
  schema:name "Alice Chen" ;
  ex:role     "engineer" ;
  ex:team     "platform" .

ex:bob  a schema:Person ;
  schema:name "Bob Martinez" ;
  ex:role     "engineer" ;
  ex:team     "platform" .

ex:carol  a schema:Person ;
  schema:name "Carol White" ;
  ex:role     "manager" ;
  ex:team     "platform" .

ex:doc1  a ex:Article ;
  schema:name    "Deployment Runbook" ;
  schema:author  ex:alice ;
  ex:team        "platform" ;
  ex:visibility  "internal" ;
  ex:content     "Step 1: Check the monitoring dashboard. Step 2: Run the database migration script. Step 3: Deploy the new container image using the CI pipeline."^^f:fullText .

ex:doc2  a ex:Article ;
  schema:name    "Onboarding Guide" ;
  schema:author  ex:bob ;
  ex:team        "platform" ;
  ex:visibility  "public" ;
  ex:content     "Welcome to the platform team. This guide covers setting up your development environment, accessing the database, and deploying your first service."^^f:fullText .

ex:doc3  a ex:Article ;
  schema:name    "Incident Response Playbook" ;
  schema:author  ex:carol ;
  ex:team        "platform" ;
  ex:visibility  "confidential" ;
  ex:content     "During a production incident, the on-call engineer should check database health, review recent deployments, and escalate if the service is not recovering within 15 minutes."^^f:fullText .
'
```

Verify the data is there:

```bash
fluree query --format table 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?title ?author_name ?visibility
WHERE {
  ?doc a ex:Article ;
       schema:name ?title ;
       schema:author ?author ;
       ex:visibility ?visibility .
  ?author schema:name ?author_name .
}
ORDER BY ?title'
```

```
┌─────────────────────────────┬───────────────┬──────────────┐
│ title                       │ author_name   │ visibility   │
├─────────────────────────────┼───────────────┼──────────────┤
│ Deployment Runbook          │ Alice Chen    │ internal     │
│ Incident Response Playbook  │ Carol White   │ confidential │
│ Onboarding Guide            │ Bob Martinez  │ public       │
└─────────────────────────────┴───────────────┴──────────────┘
```

This is transaction `t=1`. Remember this — we'll come back to it.

## Step 2: Full-text search

The article content was inserted with the `@fulltext` datatype, so it's automatically indexed for BM25 relevance scoring. Search for articles about deployments:

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc", "@type": "ex:Article",
      "ex:content": "?content",
      "schema:name": "?title"
    },
    ["bind", "?score", "(fulltext ?content \"database deployment\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "limit": 10
}'
```

Results are ranked by relevance — the deployment runbook and incident playbook both mention deployments and databases, while the onboarding guide has a weaker match.

You can combine search with graph filters. Find only **public** articles matching the search:

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc", "@type": "ex:Article",
      "ex:content": "?content",
      "schema:name": "?title",
      "ex:visibility": "public"
    },
    ["bind", "?score", "(fulltext ?content \"database deployment\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]]
}'
```

Search results participate in standard graph joins and filters — no separate search service needed.

## Step 3: Update data and use time travel

Let's update the deployment runbook with a new version:

```bash
fluree update 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>
PREFIX f: <https://ns.flur.ee/db#>

DELETE { ex:doc1 ex:content ?old }
INSERT { ex:doc1 ex:content "Step 1: Check the monitoring dashboard and verify all health checks pass. Step 2: Run the database migration script with --dry-run first. Step 3: Deploy the new container image. Step 4: Verify the deployment in staging before promoting to production."^^f:fullText }
WHERE  { ex:doc1 ex:content ?old }'
```

Now query the **current** version:

```bash
fluree query 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?content WHERE { ex:doc1 ex:content ?content }'
```

And query the **original** version using time travel:

```bash
fluree query --at 1 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?content WHERE { ex:doc1 ex:content ?content }'
```

The `--at 1` flag queries the data as it was after transaction 1 — before the update. Both versions coexist in the same ledger.

You can also see the full change history:

```bash
fluree history 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?content ?t ?op WHERE { ex:doc1 ex:content ?content }'
```

Each result includes `?t` (the transaction number) and `?op` (whether it was an assertion or retraction). You see the original content retracted and the new content asserted, with exact timestamps.

**Use cases this enables:**
- **Audit trails** — Who changed what, when?
- **Rollback** — See what the data looked like before a bad change
- **Compliance** — Prove what was known at a specific point in time
- **Debugging** — Compare current vs. historical state to find when a problem was introduced

## Step 4: Branch to experiment safely

Suppose you want to reorganize the knowledge base — maybe split articles into categories, or restructure ownership. You don't want to affect the production data while experimenting.

Create a branch:

```bash
fluree branch create reorganize
fluree use knowledge-base:reorganize
```

On the branch, add categories and reorganize:

```bash
fluree insert '
@prefix schema: <http://schema.org/> .
@prefix ex:     <http://example.org/> .

ex:doc1 ex:category "operations" .
ex:doc2 ex:category "onboarding" .
ex:doc3 ex:category "operations" .
'
```

```bash
fluree update 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

DELETE { ex:doc3 ex:visibility "confidential" }
INSERT { ex:doc3 ex:visibility "internal" }
WHERE  { ex:doc3 ex:visibility "confidential" }'
```

Verify the branch has the changes:

```bash
fluree query --format table 'PREFIX schema: <http://schema.org/>
PREFIX ex: <http://example.org/>

SELECT ?title ?category ?visibility
WHERE {
  ?doc a ex:Article ;
       schema:name ?title ;
       ex:category ?category ;
       ex:visibility ?visibility .
}
ORDER BY ?title'
```

The main branch is untouched:

```bash
fluree query --ledger knowledge-base:main 'PREFIX ex: <http://example.org/>
PREFIX schema: <http://schema.org/>

SELECT ?title ?visibility
WHERE {
  ?doc a ex:Article ; schema:name ?title ; ex:visibility ?visibility .
  OPTIONAL { ?doc ex:category ?cat }
  FILTER(!BOUND(?cat))
}
ORDER BY ?title'
```

No categories on main — the branch is fully isolated.

When you're happy with the changes, merge back:

```bash
fluree branch merge reorganize
fluree use knowledge-base:main
```

Now main has the categories and the visibility change. The branch can continue for future experiments or be dropped:

```bash
fluree branch drop reorganize
```

## Step 5: Add access control

Now let's add policies so that different users see different articles based on their role and team.

Insert policies into the ledger:

```bash
fluree insert '{
  "@context": {
    "f": "https://ns.flur.ee/db#",
    "ex": "http://example.org/",
    "schema": "http://schema.org/"
  },
  "@graph": [
    {
      "@id": "ex:policy-public-read",
      "@type": "f:Policy",
      "f:action": "query",
      "f:resource": { "ex:visibility": "public" },
      "f:allow": true
    },
    {
      "@id": "ex:policy-team-internal",
      "@type": "f:Policy",
      "f:subject": "?user",
      "f:action": "query",
      "f:resource": {
        "ex:visibility": "internal",
        "ex:team": "?team"
      },
      "f:condition": [
        { "@id": "?user", "ex:team": "?team" }
      ],
      "f:allow": true
    },
    {
      "@id": "ex:policy-manager-confidential",
      "@type": "f:Policy",
      "f:subject": "?user",
      "f:action": "query",
      "f:resource": {
        "ex:visibility": "confidential",
        "ex:team": "?team"
      },
      "f:condition": [
        { "@id": "?user", "ex:team": "?team", "ex:role": "manager" }
      ],
      "f:allow": true
    }
  ]
}'
```

These three policies create a layered access model:

1. **Public articles** — visible to everyone
2. **Internal articles** — visible only to members of the same team
3. **Confidential articles** — visible only to managers on the same team

Query as Alice (engineer, platform team):

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?title", "?visibility"],
  "where": [
    {"@id": "?doc", "@type": "ex:Article", "schema:name": "?title", "ex:visibility": "?visibility"}
  ],
  "opts": {"identity": "ex:alice"}
}'
```

Alice sees the public onboarding guide and the internal deployment runbook, but **not** the confidential incident playbook.

Query as Carol (manager, platform team):

```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?title", "?visibility"],
  "where": [
    {"@id": "?doc", "@type": "ex:Article", "schema:name": "?title", "ex:visibility": "?visibility"}
  ],
  "opts": {"identity": "ex:carol"}
}'
```

Carol sees all three articles, including the confidential one.

The same query, different results, based on who's asking — enforced by the database, not application code.

## Step 6: Combine everything

Now let's use all features together. Carol (manager) searches for articles about "database" in the knowledge base, with policies applied, and compares what she sees now vs. what existed before the reorganization:

**Current state, with policy:**
```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "select": ["?title", "?visibility", "?score"],
  "where": [
    {
      "@id": "?doc", "@type": "ex:Article",
      "ex:content": "?content",
      "schema:name": "?title",
      "ex:visibility": "?visibility"
    },
    ["bind", "?score", "(fulltext ?content \"database\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]],
  "opts": {"identity": "ex:carol"}
}'
```

**Historical state (before runbook was updated):**
```bash
fluree query '{
  "@context": {"schema": "http://schema.org/", "ex": "http://example.org/"},
  "from": "knowledge-base:main@t:1",
  "select": ["?title", "?score"],
  "where": [
    {
      "@id": "?doc", "@type": "ex:Article",
      "ex:content": "?content",
      "schema:name": "?title"
    },
    ["bind", "?score", "(fulltext ?content \"database\")"],
    ["filter", "(> ?score 0)"]
  ],
  "orderBy": [["desc", "?score"]]
}'
```

In a single database, you've combined:
- **Full-text search** — ranked by relevance
- **Access control** — Carol sees confidential articles, others wouldn't
- **Time travel** — compare current vs. historical content
- **Branching** — experimented with reorganization without risk

## What you've learned

| Feature | What it gave you |
|---|---|
| **Ledger** | A single place for all knowledge base data |
| **Full-text search** | BM25-ranked article discovery, integrated in queries |
| **Time travel** | Complete audit trail, historical comparison, rollback capability |
| **Branching** | Safe experimentation without affecting production |
| **Policies** | Automatic access control based on team and role |
| **SPARQL + JSON-LD** | Two query languages accessing the same engine |

## Next steps

- [Search Cookbook](../guides/cookbook-search.md) — Deeper guide to BM25 and vector search
- [Time Travel Cookbook](../guides/cookbook-time-travel.md) — Practical time-travel patterns
- [Branching Cookbook](../guides/cookbook-branching.md) — Branch/merge workflows
- [Policies Cookbook](../guides/cookbook-policies.md) — Access control patterns
- [SPARQL Reference](../query/sparql.md) — Full SPARQL 1.1 reference
- [JSON-LD Query](../query/jsonld-query.md) — Fluree's native query language
