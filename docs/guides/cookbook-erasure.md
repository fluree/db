# Cookbook: Permanent erasure

Permanent erasure destroys selected values: the commits that hold them are replaced by redacted copies, and the originals are deleted from the server's storage. A zero-knowledge proof shows each copy is the original commit less exactly the erased operations, so the rest of the ledger, its commit IDs and its history still verify.

Use it when data has to be gone: a right-to-erasure request, a credential that leaked into a record, a contract that requires destruction. If the value only needs to stop being readable, a [targeted removal](cookbook-retention.md#targeted-removal) is immediate, needs no prover and works on any storage. [Data lifecycle](../concepts/data-lifecycle.md) compares the options.

This guide walks one erasure from start to finish, then covers common variations. The [permanent erasure reference](../transactions/permanent-erasure.md) has every detail.

## Before you start

Erasure cannot be undone, and it is asynchronous. Check these first:

| Requirement | Why |
|---|---|
| File storage (`--storage-path`), on a single node or a Raft cluster whose members share it | Memory and S3 storage cannot hold an erasure; the request answers `501` |
| The [prover](../operations/erasure-prover.md) of the server's release, installed where the server finds it | It makes the proofs. Without it the request answers `501` |
| Background indexing on (the default) | An erasure publishes only once the affected commits are indexed |
| Admin access | `POST /erase` is admin-protected |
| Each affected commit has at most 50,000 operations | A larger commit cannot be proved; the request answers `422` |
| A host with memory and cores to spare, or a separate proving host | Proving a 1,000-operation commit takes about 4 minutes on 16 vCPUs and about 9.6 GB of memory |

And know what it will not do:

- It removes **values, not IRIs**. If the subject is `mailto:alice@example.org`, that IRI stays.
- It purges **this server's storage**. Backups, clones and exports made earlier keep the value until you replace them.
- On an **unsigned** commit, someone who can rebuild every other byte of the commit can confirm a guess at the erased value against the commit's ID. [Signed commits](../security/commit-signing.md) close that.

[Designing for erasure](../concepts/data-lifecycle.md#designing-for-erasure) lists what to set up ahead of time.

## Walkthrough: erase one person's email

### 1. Install the prover and check it

On the server's host:

```bash
fluree prover install
fluree prover status
```

```
✓ /usr/local/bin/fluree-prover: proves an image this release accepts
```

With Docker, run the image tagged `-erasure` (for example `fluree/server:latest-erasure`), which ships the prover. [Running the erasure prover](../operations/erasure-prover.md) covers sizing, isolation and clusters.

### 2. Confirm the server can erase

Start (or restart) the server, then read its discovery document:

```bash
curl -s http://localhost:8090/.well-known/fluree.json | jq .erasure
```

```json
{ "proving": true }
```

`false` means requests will be refused. The server's startup log says why: look for `permanent erasure available` or `permanent erasure unavailable`.

### 3. Find exactly what to erase

A request selects facts by subject, predicate and (optionally) value, using full IRIs. Look at what the ledger holds, including history, before you write the selectors:

```bash
fluree history https://example.org/user-8f3a -l people:main
```

```
┌────┬────┬───────────────────────────┬────────────────────┐
│ t  │ op │ predicate                 │ value              │
├────┼────┼───────────────────────────┼────────────────────┤
│ 7  │ +  │ https://example.org/email │ alice@old.example  │
│ 19 │ -  │ https://example.org/email │ alice@old.example  │
│ 19 │ +  │ https://example.org/email │ alice@example.org  │
└────┴────┴───────────────────────────┴────────────────────┘
```

Here the email has had two values. To erase both, select the property and leave the value out.

Do this step before the request, not after: accepting the request removes the history you are reading. If an earlier removal or erasure already covers this subject, the entity-wide form is refused with `410`, because it reads every property of the subject; name the property instead (`-p https://example.org/email`).

### 4. Send the request

```bash
curl -X POST http://localhost:8090/v1/fluree/erase/people:main \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin token>' \
  -d '{
    "matching": [
      { "subject": "https://example.org/user-8f3a",
        "predicate": "https://example.org/email" }
    ]
  }'
```

The request returns once three things are durable:

1. The values are **removed from every branch** that holds them: retracted if current, and refused in history. Readers stop seeing them now, not when the proofs finish.
2. An **authorization commit** records the request's ID, who made it and a hash of what it erases. It records no value.
3. A **job** is queued for the proofs.

```json
{
  "requestId": "erasure-3f1c…",
  "manifest": [
    { "commit": "bagaybqabciqa…", "slot": 4 },
    { "commit": "bagaybqabciqb…", "slot": 0 },
    { "commit": "bagaybqabciqb…", "slot": 1 },
    { "commit": "bagaybqabciqc…", "slot": 0 }
  ],
  "commits": 3,
  "authorizedT": 43,
  "stage": "proving",
  "removals": [{ "ledgerId": "people:main", "removal": { "requestId": "removal-…" } }]
}
```

`manifest` lists every operation that will be erased, by commit and position. `commits` is how many commits must be proved. It is usually more than you expect, because every operation that carries a value is erased: its assertion, the retraction that ended it, and the retraction the removal itself just made. Here that is the commit at `t` 7, the commit at `t` 19 (two operations), and the removal's own commit.

Sending the same request again returns the same job. It is safe to retry.

### 5. Follow the job

```bash
curl -s http://localhost:8090/v1/fluree/retention/people:main \
  -H 'Authorization: Bearer <token>' \
  | jq '.registration.erasures[] | {requestId, stage, retries, lastError, purge}'
```

A job moves through these stages:

| Stage | What is happening | Typical wait |
|---|---|---|
| `proving` | The prover redacts each affected commit, one at a time, and the server verifies each proof | Minutes per commit; see [Cost](../transactions/permanent-erasure.md#cost) |
| `indexing` | Every commit is proved. The server waits until each branch's index covers them, and asks for the builds it needs | Seconds to minutes |
| `published` | The replacements now stand for the originals. Every read of those commits, on any path, reads the replacement | Reached about a second after publication |
| `purging` | Indexes are rewritten without the erased values, then the original commit files, transaction documents and derived copies are deleted | At least 15 minutes: older indexes stay that long so running queries can finish. On a cluster, at least an hour |
| `purged` | Nothing this server's storage holds contains the erased values | Done |
| `failed` | A commit could not be proved. Nothing of the job was published | See [Troubleshooting](#troubleshooting) |

The requester, the affected commits and `lastError` are audit data. They appear only when the request carries a token with the `fluree.ledger.audit.all` or `fluree.ledger.audit.ledgers` [claim](../security/authentication.md#scope-claims), even on a server that does not require authentication. Any other reader sees stages, retries and timings.

Mint one with [`fluree token create`](../cli/token.md):

```bash
fluree token create --private-key @~/.fluree/key --read-ledger people:main --audit-ledger people:main
```

The server must trust the token's issuer. A server that runs without data authentication reads the token only for its audit grant, and still needs the issuer listed: start it with `--data-auth-trusted-issuer <did:key>`.

Treat `published` as "no reader can get the value from this server" and `purged` as "this server's disk no longer has it".

### 6. Check the result

Current state and history both refuse the value:

```bash
# No rows
fluree query people:main 'SELECT ?e WHERE { <https://example.org/user-8f3a> <https://example.org/email> ?e }'

# 410 err:db/HistoryUnavailable
fluree history https://example.org/user-8f3a -p https://example.org/email -l people:main
```

The affected commits still exist, with the same IDs and `t`, minus the erased operations:

```bash
fluree show t:7 -l people:main
```

A purged job reports what the purge could not check under `purge.unverified`. That is the memory of other processes reading the same storage: restart those.

### 7. Clean up what the purge leaves

- **IRIs.** If you erased every fact about a subject, its IRI is still in the index dictionaries. A full reindex after the purge builds an index without it: `fluree reindex people:main`.
- **Query peers.** Peers that follow the server's `/events` stream evict what they cached. Restart any other process that reads the storage, and peers on an earlier release.
- **Clones.** See [Carry an erasure to clones](#carry-an-erasure-to-clones).
- **Backups and archives.** An archive made before the erasure still holds the value. Make a new one (`fluree export --format ledger`), which carries each erased commit as its erasure and never as its original, and destroy the old ones.
- **Disk.** A purge deletes files. With [encryption at rest](../security/encryption.md) the freed blocks are unreadable; without it, the filesystem or SSD may keep them until they are overwritten.

## Variations

### Erase everything about a subject

A selector needs a subject and a predicate, so erasing a whole subject means one selector per property it has ever had. Ask the ledger's history for the list:

```bash
fluree query people:main --format json '{
  "from": "people:main@t:1",
  "to": "people:main@t:latest",
  "selectDistinct": ["?p"],
  "where": [
    { "@id": "https://example.org/user-8f3a", "?p": { "@value": "?v", "@t": "?t" } }
  ]
}' > predicates.json
```

This query reads every property of the subject, so the ledger refuses it (`410`) when an earlier removal or erasure already covers one of that subject's properties. There, list the subject's current properties with the same pattern and no `from`/`to` (a query at the head is always answered), and add the properties it used to have from your data model. A history query that names one predicate confirms each.

Turn the list into a request, review it, and send it:

```bash
jq '{ matching: [ flatten[] | { subject: "https://example.org/user-8f3a", predicate: . } ] }' \
  predicates.json > erase.json

cat erase.json   # read it before you send it

curl -X POST http://localhost:8090/v1/fluree/erase/people:main \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <admin token>' \
  -d @erase.json
```

Three things this does not cover:

- **Other subjects that point at this one.** `ex:order-77 ex:customer ex:user-8f3a` is a fact about the order. Add a selector for it (`"object": {"@id": "https://example.org/user-8f3a"}`) if the link itself must go.
- **Named graphs.** A selector without `graph` covers the default graph. Repeat the query and the selectors for each named graph, with `"graph": "<graph IRI>"`.
- **The IRI.** Reindex after the purge, as above. Namespace prefixes stay in the ledger's namespace table regardless.

### Erase one leaked value

Name the value, so other values of the property are left alone:

```json
{
  "matching": [
    { "subject": "https://example.org/ticket-204",
      "predicate": "https://example.org/notes",
      "object": "temp password is hunter2" }
  ]
}
```

A plain string selects an `xsd:string`. For another datatype or a language-tagged string, use a value object: `{"@value": "1984-02-11", "@type": "xsd:date"}` or `{"@value": "…", "@language": "fr"}`. For a reference, `{"@id": "<IRI>"}`.

### Erase what a removal already took out

If you made a [targeted removal](cookbook-retention.md#targeted-removal) earlier and now need the data destroyed, send the same selectors to `/erase`. Or name the operations from the removal's receipt:

```json
{
  "occurrences": [
    { "commit": "bafy…", "slot": 3 }
  ]
}
```

`matching` is usually the better choice. It reaches every branch that holds the fact, and commits a merge brought in. A named occurrence is that one operation, plus the one that bounds its span.

### A ledger with signed commits

By default the proof also verifies each erased commit's signatures, which takes about half as long again. A request can trade privacy for speed:

```json
{ "matching": [ … ], "signatures": "expose" }
```

With `expose` the proof publishes the signatures and the hash of the signed body, and the server checks them itself. Anyone who can rebuild the rest of that body can then test guesses at the erased value. Keep the default unless the erased values are high-entropy (a random token, not a postcode).

### On a Raft cluster

The request goes to any member and is forwarded to the leader. What changes:

- Every member must run a release with erasure, and each needs the prover installed. The leader hands each proof to another member, so a learner started with a prover makes a dedicated proving node.
- The first erasure a cluster accepts turns on its [garbage collection](../operations/raft-clusters.md#garbage-collection-and-permanent-erasure), permanently. After that the cluster refuses ref-based commit-head pushes, and a member on an earlier release cannot rejoin.
- The purge waits an hour after the erasure takes effect, then for every member to clean its own copies. An offline member holds the job at `purging`; `purge.blocked` names it.

Read [On a cluster](../transactions/permanent-erasure.md#on-a-cluster) before the first one.

### Carry an erasure to clones

A clone holds its own copy of the commits. It takes an erasure only from a remote it has been told to trust for that, because receiving one deletes history the clone already has:

```bash
# On the clone
fluree remote erasure-authority origin true
fluree pull
```

The pull checks every proof (no prover needed), makes the local ledger read as the erasure does, and deletes the originals from local storage before it exits. `fluree info` lists any erasure whose files are still there. A pull is needed even when there are no new commits.

A clone that never pulls keeps the value. So does a clone whose remote is not designated: its pull is refused once the remote's history holds an erasure.

In the other direction, `fluree push` carries the erasures a local copy holds to a single server on file storage, for a principal with the authority to erase there. See [Pushes](../transactions/permanent-erasure.md#pushes).

### From Rust

```rust
use fluree_db_api::retention::erasure::ErasureRequest;
use fluree_db_api::retention::FactSelector;

let receipt = fluree
    .erase_occurrences(
        "people:main",
        ErasureRequest {
            matching: vec![FactSelector {
                subject: "https://example.org/user-8f3a".into(),
                predicate: "https://example.org/email".into(),
                graph: None,
                object: None,
            }],
            ..Default::default()
        },
        Some("did:key:z6Mk…".into()), // recorded as the requester
    )
    .await?;

let status = fluree.retention_status("people:main").await?;
```

The instance must be file-backed, built with `FlureeBuilder::with_erasure(ErasureConfig::subprocess(prover_config))`, and must run the retention worker (`Fluree::start_maintenance` with retention options), which advances the job.

## Troubleshooting

| Symptom | Cause | What to do |
|---|---|---|
| `501` on the request | No prover of this release, erasure disabled (`--disable-erasure`), memory or S3 storage, or a query peer | Check `fluree prover status` and the startup log. On a cluster, every member must run a release with erasure |
| `422`, naming a commit | The commit has more than 50,000 operations, or is not a v4 SHA2-256 commit | That commit cannot be erased. The values are already removed from what the ledger serves, and stay removed |
| `409`, naming commits and slots | Another fact's object is a triple term that spells the erased fact (`rdf:reifies <<( … )>>`), which would keep the value readable | Add those occurrences to the request |
| `404` | Nothing in the ledger's history matches | Check the IRIs are full and exact, the value's datatype, and the graph |
| `proving` with `retries` climbing | The prover is missing, crashing, or being killed for memory | Read `lastError` (with an audit token). Give the prover more memory or its own host; see [Memory](../operations/erasure-prover.md#memory-and-cpu) |
| `proving` for hours | A large commit. About 3 hours for 50,000 operations on 16 vCPUs | Wait, or add cores. A step that passes `--erasure-prover-timeout-secs` three times fails the job |
| Stays at `indexing` | Indexing is disabled, or behind | Check `lastError` and indexing lag. Run `fluree index <ledger>` |
| Stays at `purging` with `purge.blocked` | A full-text or vector index over a dropped ledger reads an erased property; another process holds the storage; on a cluster, a member is offline | Follow the message. The pass retries an hour later |
| `lastError` never appears | The request's token has no audit claim | See [Follow the job](#5-follow-the-job) |
| `failed` | The prover refused a commit, or timed out three times | Fix the cause in `lastError`, then send the same request again: it resumes where it stopped |
| A historical query on unrelated data now answers `410` | It names neither its predicates nor one subject: `?s ?p ?o`, a graph crawl, a Cypher query, reasoning. Such a read is refused at any earlier point a removal covers, for every subject | Name the predicates in the query's patterns, or a constant subject. See [what still works in history](cookbook-retention.md#what-still-works-in-history-afterwards) |

## Related documentation

- [Permanent erasure](../transactions/permanent-erasure.md): the reference, including archives, transfers, status fields and limits
- [Running the erasure prover](../operations/erasure-prover.md): installing, sizing and isolating the prover
- [`fluree prover`](../cli/prover.md): the CLI command
- [Data lifecycle](../concepts/data-lifecycle.md): choosing between expiry, removal and erasure
- [Cookbook: expiring data and trimming history](cookbook-retention.md)
- [ZK redaction feasibility study](../design/zk-redaction-feasibility.md): how the proofs work and what they cost
