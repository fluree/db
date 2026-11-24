# Lists

A list is an ordered set of values that a subject holds under one property: a JSON-LD `{"@list": [...]}`, or a Turtle or SPARQL `( … )` in object position.

```turtle
ex:recipe ex:steps ( "chop" "fry" "serve" ) .
```

```json
{"@id": "ex:recipe", "ex:steps": {"@list": ["chop", "fry", "serve"]}}
```

## How a list is stored

Fluree stores a list compactly. Each member is a value of the property that holds the list, and carries its place: which of the subject's lists on that property it belongs to, and its position there. No `rdf:first` / `rdf:rest` triples are stored for it.

So a query reads a list's members as values of the property, as JSON-LD does:

```sparql
SELECT ?step WHERE { ex:recipe ex:steps ?step }    # "chop", "fry", "serve"
```

The order is kept wherever a whole list is written out: a JSON-LD crawl returns `{"@list": [...]}`, and [export](../cli/export.md), the Graph Store `GET`, CONSTRUCT and DESCRIBE write the list in order. A solution of a SELECT carries no position. See [Lists in SPARQL](../query/sparql.md#lists) for what each pattern reads.

As RDF, a stored list is still the chain of cells a `( … )` stands for. A `p/rdf:rest*/rdf:first` walk reaches its members, the Graph Store `GET` reads the list as that chain, and N-Triples export, which has no `( … )`, writes it out.

## Writing lists

- JSON-LD `@list`, Turtle and SPARQL `( … )`, and a list written out as `rdf:first` / `rdf:rest` triples over the document's own blank nodes are all stored the same way.
- A subject can hold several lists on one property; each keeps its own members in order.
- Writing a list the subject and property already hold, with the same members in the same order, restates it rather than adding another.
- To replace a list, upsert it.
- Deleting a value deletes every entry of a list that holds it.
- A JSON-LD `delete` naming a `@list`, or a `( … )` in a SPARQL `DELETE WHERE`, deletes each list that has those members in that order, whole. See [Writing lists](../query/sparql.md#writing-lists).
- An empty list is the IRI `rdf:nil`, a value like any other, including as a member of a list.

## Lists of lists

A list can hold lists:

```turtle
ex:s ex:matrix ( ( 1 2 ) ( 3 4 ) 5 ) .
```

```json
{"@id": "ex:s", "ex:matrix": {"@list": [{"@list": [1, 2]}, {"@list": [3, 4]}, 5]}}
```

It is stored as one list. Every value in it, at any depth, is a value of the property, and its place also records its depth and where its inner lists begin, so nothing is stored for an inner list itself. As a result:

- `ex:s ex:matrix ?v` binds `1`, `2`, `3`, `4` and `5`.
- A JSON-LD crawl returns the nested `@list`, and export, the Graph Store `GET`, CONSTRUCT and DESCRIBE write the nested `( … )`.
- A `p/rdf:rest*/rdf:first` walk reaches the top-level items, as RDF has them: each inner list is a blank node, and `5`. One more `rdf:rest*/rdf:first` per level reaches the values a level deeper: `ex:s ex:matrix/rdf:rest*/rdf:first/rdf:rest*/rdf:first ?v` binds `1`, `2`, `3` and `4`. Write the steps out for each level; `(rdf:rest*/rdf:first)+` repeats a transitive step, which property paths do not support.
- Deleting a value keeps the lists around it: `( ( 1 ) ( 2 3 ) )` without `2` is `( ( 1 ) ( 3 ) )`. An inner list left with no value is gone.
- A nested list is restated only when its whole shape matches, and a JSON-LD `delete` or a SPARQL `DELETE WHERE` collection naming one matches its shape too: `( ( 1 2 ) 3 )` and `( 1 ( 2 3 ) )` are different lists.
- A view policy that hides some values reads the list as if they were deleted. A CONSTRUCT whose WHERE filters values out does the same.

## What is stored as triples instead

A chain is stored as the `rdf:first` / `rdf:rest` triples it spells, and read as those triples, when it is not a plain list of the document's own blank nodes:

- a cell that something else names, an annotation on a cell's triple included;
- a cell that carries triples other than its `rdf:first` and `rdf:rest`;
- a chain whose cells are not blank nodes, or that does not end in `rdf:nil`.

A bulk import (`fluree create --from`) also stores a list written out as `rdf:first` / `rdf:rest` triples as those triples. A `( … )` imports compactly, its inner lists included.

Data written before nested lists were stored compactly holds an inner list as its triples, with its head a member of the outer list. A JSON-LD crawl reads such data as the nested list. To store lists an earlier release wrote as triples compactly, see [Lists stored as triples](../operations/upgrading-from-4.2.md#lists-stored-as-triples).

## Limits

| | Limit |
|---|---|
| Members of one list | 2,147,483,648 (2³¹) |
| Lists one subject holds on one property | 2,147,483,648 (2³¹); past that, a further list is stored as its triples |
| Depth of a list of lists | 15 lists below the top-level list |
| Values in one list of lists | 8,388,608 (2²³) |

JSON-LD refuses a list of lists past either of its limits. From Turtle, N-Triples or SPARQL, such a list keeps its inner lists as triples.
