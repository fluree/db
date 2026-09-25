# Graph Store Protocol

Fluree implements the [W3C SPARQL 1.1 Graph Store HTTP Protocol](https://www.w3.org/TR/sparql11-http-rdf-update/): read, replace, add to, or remove one graph with plain HTTP verbs. RDF tools that speak the protocol (Apache Jena, RDF4J, rdflib and others) can use Fluree as a graph store without a Fluree-specific client.

## URL

```
/v1/fluree/data/{ledger}?graph={graph-iri}
/v1/fluree/data/{ledger}?default
```

`graph=` names a graph by its absolute IRI (percent-encode it). A bare `default` names the ledger's default graph. A request names exactly one of the two; neither or both is a `400`. The ledger's system graphs (`#txn-meta`, `#config`) cannot be addressed here.

This is the protocol's *indirect* graph identification. *Direct* identification, where the request URL is itself the graph IRI, is not supported: Fluree's graph IRIs are not URLs on the server.

## Methods

| Method | Does | Success |
|---|---|---|
| `GET` | Returns the graph's triples, the whole graph in one response | `200`; `404` if the graph does not exist |
| `HEAD` | Same status and `Content-Type` as `GET`, without building the graph | `200` / `404` |
| `PUT` | Replaces the graph's contents with the body | `201` if this created the graph, otherwise `200` |
| `POST` | Adds the body's triples to the graph | `201` if this created the graph, otherwise `200` |
| `DELETE` | Removes the graph (the default graph is emptied) | `200`; `404` if the graph does not exist |

`PUT`, `POST` and `DELETE` respond with the standard transaction response (`ledger`, `t`, `tx-id`, commit). Other statuses: `400` for a malformed request or body, `406` when `GET` can't produce any format in `Accept`, `415` for an unsupported body type, and the usual `401` / `403` for auth.

### PUT: replace

`PUT` is [graph sync](../transactions/sync.md): one commit holding only the difference between the graph's current contents and the body. Triples already there are not retracted and re-asserted, so replacing a large graph with a small change is a small commit, and an unchanged body commits nothing.

As the protocol specifies, an empty `PUT` body empties the graph. Unlike `/sync`, there is no `allowEmpty` opt-in: the method is the explicit replace.

### POST: add

`POST` inserts the body's triples into the graph and retracts nothing. Blank nodes are fresh on every request (an RDF merge), so posting the same document with `[ … ]` nodes twice adds two copies of those nodes. Use `PUT` when you want the graph to match a document. An empty `POST` body is a `400`: there is nothing to add.

### DELETE: remove

`DELETE` on a named graph is `DROP GRAPH`; on the default graph it is `CLEAR DEFAULT`.

### Graph existence

Fluree has no empty named graph: a named graph exists while it holds at least one triple. So after a `DELETE`, or a `PUT` with an empty body, `GET` on that graph is a `404`. The default graph always exists.

For `GET` and `HEAD`, existence is as the caller sees it: a named graph with no triple the caller may read is a `404`, the same answer as a graph that isn't there. Authentication runs first, so an unauthenticated read is a `401` whether or not the graph exists.

`GET` is not paged. It builds and serializes the whole graph before responding, so for a large graph, query it through `/query` with `LIMIT` / `OFFSET`, or export the ledger.

## Formats

`PUT` and `POST` accept:

| Content-Type | Body |
|---|---|
| `text/turtle` | Turtle |
| `application/n-triples` | N-Triples |
| `application/trig` | TriG: triples in `GRAPH` blocks naming the request's graph, or outside any block, but not both |
| `application/ld+json`, `application/json` | Insert-shaped JSON-LD |

The TriG rules are those of [sync](../transactions/sync.md#payload-formats): every block must name the request's graph, a block for another graph is a `400`, and a body targeting the default graph cannot contain blocks.

`GET` answers in the highest-weighted (`q`) format in `Accept` that it supports; equal weights keep the header's order:

| Accept | Response |
|---|---|
| `application/ld+json`, `application/json`, `*/*`, `application/*`, or no `Accept` | JSON-LD |
| `text/turtle` (or `text/*`) | Turtle |
| `application/n-triples` | N-Triples |
| `application/rdf+xml` | RDF/XML |

An `Accept` that names none of these is a `406`. The Turtle and N-Triples a `GET` returns are what `PUT` accepts, so a graph read as Turtle and put back unchanged commits nothing: blank nodes are written under their stored labels, which a write resolves back to the same nodes.

**Edge annotations are not returned.** `GET` returns a graph's triples, including an annotation's own triples (`ex:claim1 ex:confidence 0.9`), but not the link that ties the annotation to its edge, because `CONSTRUCT` does not serialize RDF 1.2 annotations yet. So a graph with annotations does not survive a `GET` followed by a `PUT` of the result: the `PUT` commits a change that removes those links. Keep the source file as the copy you `PUT`, and read annotations with a JSON-LD query (see [Edge annotations](../concepts/edge-annotations.md)).

## Auth and policy

`GET` runs a SPARQL `CONSTRUCT` of the graph through the query path, and `HEAD` an `ASK`, so read policy applies exactly as it does for `/query`: a restricted reader sees only what policy allows, and the `404` follows what they can see.

`PUT`, `POST` and `DELETE` need write access to the ledger, as `/insert` does. Modify policy applies to the staged changes. As with sync, the scan of a graph's current contents that `PUT` diffs against is not view-policy filtered.

## Clusters

Reads are served by whichever node receives them. Writes go through consensus: on a Raft follower they are forwarded to the leader, and on a peer they are forwarded to the transaction server.

## Examples

```bash
# Replace a graph with a Turtle file
curl -X PUT "http://localhost:8090/v1/fluree/data/mydb:main?graph=urn:example:tools" \
  -H "Content-Type: text/turtle" \
  --data-binary @tools.ttl

# Add triples to it
curl -X POST "http://localhost:8090/v1/fluree/data/mydb:main?graph=urn:example:tools" \
  -H "Content-Type: text/turtle" \
  --data-binary @more.ttl

# Read it back
curl "http://localhost:8090/v1/fluree/data/mydb:main?graph=urn:example:tools" \
  -H "Accept: text/turtle"

# Replace the default graph
curl -X PUT "http://localhost:8090/v1/fluree/data/mydb:main?default" \
  -H "Content-Type: text/turtle" \
  --data-binary @data.ttl

# Remove the graph
curl -X DELETE "http://localhost:8090/v1/fluree/data/mydb:main?graph=urn:example:tools"
```

## Related

- [Sync](../transactions/sync.md): the replace-by-difference operation behind `PUT`, with dry runs and the empty-payload guard
- [Turtle and TriG ingest](../transactions/turtle.md)
- [Datasets and named graphs](../concepts/datasets-and-named-graphs.md)
