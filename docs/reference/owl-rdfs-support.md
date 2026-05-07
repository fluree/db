# OWL & RDFS Support Reference

This page lists every OWL and RDFS construct that Fluree's reasoning engine
supports. For conceptual background see
[Reasoning and inference](../concepts/reasoning.md); for query syntax see
[Query-time reasoning](../query/reasoning.md).

## Quick orientation

Fluree implements reasoning via two techniques:

- **Query rewriting** (RDFS and OWL 2 QL modes) — patterns are expanded at
  compile time; no facts are materialized.
- **Forward-chaining materialization** (OWL 2 RL mode) — derived facts are
  computed before query execution using the OWL 2 RL rule set.

The tables below indicate which technique handles each construct.

---

## RDFS constructs

These constructs are handled by **query rewriting** in RDFS mode (and also by
materialization in OWL 2 RL mode).

### rdfs:subClassOf

Declares that every instance of one class is also an instance of another.

```turtle
ex:Student  rdfs:subClassOf  ex:Person .
```

**Effect:** A query for `?x rdf:type ex:Person` also returns instances typed as
`ex:Student` (and any subclass of `Student`, transitively).

**JSON-LD transaction:**
```json
{"@id": "ex:Student", "rdfs:subClassOf": {"@id": "ex:Person"}}
```

### rdfs:subPropertyOf

Declares that one property is a specialization of another.

```turtle
ex:hasMother  rdfs:subPropertyOf  ex:hasParent .
```

**Effect:** A query for `?x ex:hasParent ?y` also returns triples asserted with
`ex:hasMother`.

**JSON-LD transaction:**
```json
{"@id": "ex:hasMother", "rdfs:subPropertyOf": {"@id": "ex:hasParent"}}
```

### rdfs:domain

Declares that the subject of a property is an instance of a class.

```turtle
ex:teaches  rdfs:domain  ex:Professor .
```

**Effect (OWL 2 QL / OWL 2 RL):** If `ex:alice ex:teaches ex:cs101`, then
`ex:alice rdf:type ex:Professor` is inferred.

**JSON-LD transaction:**
```json
{"@id": "ex:teaches", "rdfs:domain": {"@id": "ex:Professor"}}
```

### rdfs:range

Declares that the object of a property is an instance of a class.

```turtle
ex:teaches  rdfs:range  ex:Course .
```

**Effect (OWL 2 QL / OWL 2 RL):** If `ex:alice ex:teaches ex:cs101`, then
`ex:cs101 rdf:type ex:Course` is inferred.

**JSON-LD transaction:**
```json
{"@id": "ex:teaches", "rdfs:range": {"@id": "ex:Course"}}
```

> **Note:** Range inference applies to IRI-valued objects only. Literal values
> (strings, numbers, etc.) are not assigned a type via `rdfs:range`.

---

## OWL property constructs

These are handled by **materialization** in OWL 2 RL mode (some also by query
rewriting in OWL 2 QL mode, as noted).

### owl:inverseOf

Declares that two properties are inverses of each other.

```turtle
ex:hasMother  owl:inverseOf  ex:motherOf .
```

**Effect:** If `ex:alice ex:hasMother ex:carol`, then
`ex:carol ex:motherOf ex:alice` is inferred (and vice versa).

**Handled by:** OWL 2 QL (query rewriting) *and* OWL 2 RL (materialization).

**OWL 2 RL rule:** `prp-inv`

**JSON-LD transaction:**
```json
{"@id": "ex:hasMother", "owl:inverseOf": {"@id": "ex:motherOf"}}
```

### owl:SymmetricProperty

Declares that a property holds in both directions.

```turtle
ex:livesWith  a  owl:SymmetricProperty .
```

**Effect:** If `ex:alice ex:livesWith ex:bob`, then
`ex:bob ex:livesWith ex:alice` is inferred.

**OWL 2 RL rule:** `prp-symp`

**JSON-LD transaction:**
```json
{"@id": "ex:livesWith", "@type": "owl:SymmetricProperty"}
```

### owl:TransitiveProperty

Declares that a property chains through intermediate nodes.

```turtle
ex:hasAncestor  a  owl:TransitiveProperty .
```

**Effect:** If `ex:a ex:hasAncestor ex:b` and `ex:b ex:hasAncestor ex:c`, then
`ex:a ex:hasAncestor ex:c` is inferred.

**OWL 2 RL rule:** `prp-trp`

**JSON-LD transaction:**
```json
{"@id": "ex:hasAncestor", "@type": "owl:TransitiveProperty"}
```

### owl:FunctionalProperty

Declares that a property can have at most one value per subject.

```turtle
ex:hasBirthDate  a  owl:FunctionalProperty .
```

**Effect:** If `ex:alice ex:hasBirthDate ex:d1` and
`ex:alice ex:hasBirthDate ex:d2`, then `ex:d1 owl:sameAs ex:d2` is inferred.

**OWL 2 RL rule:** `prp-fp`

**JSON-LD transaction:**
```json
{"@id": "ex:hasBirthDate", "@type": "owl:FunctionalProperty"}
```

### owl:InverseFunctionalProperty

Declares that a property's object uniquely identifies the subject.

```turtle
ex:hasSSN  a  owl:InverseFunctionalProperty .
```

**Effect:** If `ex:alice ex:hasSSN "123"` and `ex:bob ex:hasSSN "123"`, then
`ex:alice owl:sameAs ex:bob` is inferred.

**OWL 2 RL rule:** `prp-ifp`

**JSON-LD transaction:**
```json
{"@id": "ex:hasSSN", "@type": "owl:InverseFunctionalProperty"}
```

### owl:equivalentProperty

Declares that two properties have identical extensions.

```turtle
ex:author  owl:equivalentProperty  ex:writtenBy .
```

**Effect:** Treated as mutual `rdfs:subPropertyOf` — queries and rules see both
properties' triples when either is used.

### owl:propertyChainAxiom

Declares that a chain of properties implies another property.

```turtle
ex:hasUncle  owl:propertyChainAxiom  ( ex:hasParent  ex:hasBrother ) .
```

**Effect:** If `ex:alice ex:hasParent ex:bob` and
`ex:bob ex:hasBrother ex:charlie`, then `ex:alice ex:hasUncle ex:charlie` is
inferred.

**OWL 2 RL rule:** `prp-spo2`

Chains can be of arbitrary length (2 or more properties) and can include
inverse elements:

```turtle
ex:hasNephew  owl:propertyChainAxiom  (
    [ owl:inverseOf ex:hasBrother ]
    ex:hasChild
) .
```

**JSON-LD transaction:**
```json
{
  "@id": "ex:hasUncle",
  "owl:propertyChainAxiom": {
    "@list": [{"@id": "ex:hasParent"}, {"@id": "ex:hasBrother"}]
  }
}
```

---

## OWL class constructs

### owl:equivalentClass

Declares that two classes have identical extensions.

```turtle
ex:Pupil  owl:equivalentClass  ex:Student .
```

**Effect:** Instances of either class are inferred to be instances of both.

**OWL 2 RL rule:** `cax-eqc`

### owl:hasKey

Declares a set of properties that uniquely identify instances of a class.

```turtle
ex:Person  owl:hasKey  ( ex:hasSSN ) .
```

**Effect:** If two `ex:Person` instances share the same `ex:hasSSN` value, they
are inferred to be `owl:sameAs`.

**OWL 2 RL rule:** `prp-key`

---

## OWL restrictions (class expressions)

OWL restrictions define classes based on property constraints. They are used with
OWL 2 RL materialization.

### owl:hasValue

Defines a class of all subjects that have a specific value for a property.

```turtle
ex:RedThings  a  owl:Restriction ;
    owl:onProperty  ex:color ;
    owl:hasValue     ex:Red .
```

**Effect (forward — cls-hv1):** If `?x rdf:type ex:RedThings`, then
`?x ex:color ex:Red` is inferred.

**Effect (backward — cls-hv2):** If `?x ex:color ex:Red`, then
`?x rdf:type ex:RedThings` is inferred.

> **Limitation:** Currently supports IRI-valued `hasValue` only. Literal values
> (strings, numbers) are not yet supported.

### owl:someValuesFrom

Defines a class of subjects that have at least one value of a given type for a
property.

```turtle
ex:Parent  a  owl:Restriction ;
    owl:onProperty      ex:hasChild ;
    owl:someValuesFrom  ex:Person .
```

**Effect (cls-svf1):** If `?x ex:hasChild ?y` and `?y rdf:type ex:Person`,
then `?x rdf:type ex:Parent` is inferred.

### owl:allValuesFrom

Defines a class where all values of a property belong to a given type.

```turtle
ex:VeganRestaurant  a  owl:Restriction ;
    owl:onProperty     ex:servesFood ;
    owl:allValuesFrom  ex:VeganDish .
```

**Effect (cls-avf):** If `?x rdf:type ex:VeganRestaurant` and
`?x ex:servesFood ?y`, then `?y rdf:type ex:VeganDish` is inferred.

### owl:maxCardinality (= 1)

When a restriction specifies `maxCardinality` of 1, it acts like a
context-specific functional property.

```turtle
ex:SingleChild  a  owl:Restriction ;
    owl:onProperty      ex:hasChild ;
    owl:maxCardinality  1 .
```

**Effect (cls-maxc2):** If `?x rdf:type ex:SingleChild`,
`?x ex:hasChild ?y1`, and `?x ex:hasChild ?y2`, then
`?y1 owl:sameAs ?y2` is inferred.

### owl:maxQualifiedCardinality (= 1)

Like `maxCardinality` but restricted to objects of a specific class.

```turtle
ex:MonogamousPerson  a  owl:Restriction ;
    owl:onProperty                  ex:marriedTo ;
    owl:maxQualifiedCardinality     1 ;
    owl:onClass                     ex:Person .
```

**Effect (cls-maxqc3/4):** If `?x` is typed as this restriction class, has two
`ex:marriedTo` values, and both are `ex:Person`, they are inferred to be
`owl:sameAs`.

---

## OWL class operations

### owl:intersectionOf

Defines a class as the intersection of member classes.

```turtle
ex:WorkingStudent  owl:intersectionOf  ( ex:Student  ex:Employee ) .
```

**Effect (forward — cls-int1):** If `?x rdf:type ex:Student` and
`?x rdf:type ex:Employee`, then `?x rdf:type ex:WorkingStudent` is inferred.

**Effect (backward — cls-int2):** If `?x rdf:type ex:WorkingStudent`, then
both `?x rdf:type ex:Student` and `?x rdf:type ex:Employee` are inferred.

### owl:unionOf

Defines a class as the union of member classes.

```turtle
ex:PersonOrOrg  owl:unionOf  ( ex:Person  ex:Organization ) .
```

**Effect (cls-uni):** If `?x rdf:type ex:Person` (or `ex:Organization`), then
`?x rdf:type ex:PersonOrOrg` is inferred.

### owl:oneOf

Defines an enumerated class — a fixed set of individuals.

```turtle
ex:PrimaryColor  owl:oneOf  ( ex:Red  ex:Blue  ex:Yellow ) .
```

**Effect (cls-oo):** `ex:Red`, `ex:Blue`, and `ex:Yellow` are each inferred to
be of type `ex:PrimaryColor`.

---

## owl:sameAs

`owl:sameAs` declares that two IRIs refer to the same real-world entity.

```turtle
ex:alice  owl:sameAs  ex:aliceSmith .
```

**Effect:** All facts about `ex:alice` and `ex:aliceSmith` are merged. Queries
for either IRI return the combined set of properties.

### How sameAs is produced

`owl:sameAs` can be asserted explicitly or inferred by these rules:

| Rule | Trigger |
|------|---------|
| `prp-fp` | Functional property with multiple objects |
| `prp-ifp` | Inverse functional property with multiple subjects |
| `prp-key` | owl:hasKey match across instances |
| `cls-maxc2` | maxCardinality = 1 violation |
| `cls-maxqc3/4` | maxQualifiedCardinality = 1 violation |

### Equivalence properties

`owl:sameAs` is handled as an equivalence relation:
- **Symmetric:** if `a sameAs b` then `b sameAs a`
- **Transitive:** if `a sameAs b` and `b sameAs c` then `a sameAs c`
- **Reflexive:** every resource is same-as itself (implicit)

The engine uses a union-find data structure to efficiently track equivalence
classes and select a canonical representative for each.

---

## OWL 2 RL rule index

For reference, the complete set of OWL 2 RL rules implemented by Fluree:

### Identity-producing rules (Phase B)

These rules produce `owl:sameAs` facts and run before other rules to ensure
proper canonicalization.

| Rule | Construct | Description |
|------|-----------|-------------|
| `prp-fp` | `owl:FunctionalProperty` | Same subject + different objects → sameAs |
| `prp-ifp` | `owl:InverseFunctionalProperty` | Same object + different subjects → sameAs |
| `prp-key` | `owl:hasKey` | Same class + matching key values → sameAs |
| `cls-maxc2` | `owl:maxCardinality = 1` | Over-cardinality → sameAs |
| `cls-maxqc3` | `owl:maxQualifiedCardinality = 1` | Qualified over-cardinality → sameAs |
| `cls-maxqc4` | `owl:maxQualifiedCardinality = 1` | Variant for `owl:Thing` |

### Non-identity rules (Phase C)

| Rule | Construct | Description |
|------|-----------|-------------|
| `prp-symp` | `owl:SymmetricProperty` | P(x,y) → P(y,x) |
| `prp-trp` | `owl:TransitiveProperty` | P(x,y) ∧ P(y,z) → P(x,z) |
| `prp-inv` | `owl:inverseOf` | P(x,y) → Q(y,x) |
| `prp-dom` | `rdfs:domain` | P(x,y) → type(x,C) |
| `prp-rng` | `rdfs:range` | P(x,y) → type(y,C) |
| `prp-spo1` | `rdfs:subPropertyOf` | P1(x,y) → P2(x,y) |
| `prp-spo2` | `owl:propertyChainAxiom` | Chain match → P(first,last) |
| `cax-sco` | `rdfs:subClassOf` | type(x,C1) → type(x,C2) |
| `cax-eqc` | `owl:equivalentClass` | type(x,C1) ↔ type(x,C2) |
| `cls-hv1` | `owl:hasValue` (backward) | type(x,C) → P(x,v) |
| `cls-hv2` | `owl:hasValue` (forward) | P(x,v) → type(x,C) |
| `cls-svf1` | `owl:someValuesFrom` | P(x,y) ∧ type(y,D) → type(x,C) |
| `cls-avf` | `owl:allValuesFrom` | type(x,C) ∧ P(x,y) → type(y,D) |
| `cls-int1` | `owl:intersectionOf` (forward) | All member types → intersection type |
| `cls-int2` | `owl:intersectionOf` (backward) | Intersection type → all member types |
| `cls-uni` | `owl:unionOf` | Any member type → union type |
| `cls-oo` | `owl:oneOf` | Listed individual → enumerated type |

---

## Known limitations

| Area | Limitation |
|------|-----------|
| **Literal hasValue** | `owl:hasValue` with literal values (strings, numbers) is not yet supported; only IRI-valued restrictions work. |
| **Negation** | `owl:complementOf` and negation-as-failure are not supported. OWL 2 RL is a positive-only fragment. |
| **Disjointness** | `owl:disjointWith` and `owl:AllDisjointClasses` do not trigger inconsistency detection. |
| **Cardinality > 1** | Only `maxCardinality = 1` and `maxQualifiedCardinality = 1` are implemented (these are the only identity-producing cardinalities in OWL 2 RL). |
| **Datatype reasoning** | No inference over datatypes (e.g., `xsd:integer` subtype of `xsd:decimal`). |

## Namespaces

For reference, the standard namespace prefixes:

| Prefix | URI |
|--------|-----|
| `rdf` | `http://www.w3.org/1999/02/22-rdf-syntax-ns#` |
| `rdfs` | `http://www.w3.org/2000/01/rdf-schema#` |
| `owl` | `http://www.w3.org/2002/07/owl#` |
| `xsd` | `http://www.w3.org/2001/XMLSchema#` |
